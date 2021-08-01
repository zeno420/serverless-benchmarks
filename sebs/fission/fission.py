import math
import os
import subprocess
import shutil
import time
import uuid
from typing import cast, Dict, List, Optional, Tuple, Type, Union  # noqa
from time import sleep

import docker
from sebs import config
from sebs import benchmark

from sebs.fission.storage import Storage
from sebs.fission.function import FissionFunction
from sebs.fission.config import FissionConfig
from sebs.fission.triggers import HTTPTrigger
from sebs.utils import execute
from sebs.benchmark import Benchmark
from sebs.cache import Cache
from sebs.config import SeBSConfig
from sebs.utils import LoggingHandlers
from sebs.faas.function import Function, ExecutionResult, Trigger
from sebs.faas.storage import PersistentStorage
from sebs.faas.system import System

#TODO: set context with namespace in kubeconfig, sebs.py will use it
#TODO: implement wait for func creation or update logic
#TODO: requirements.txt* beachten

class Fission(System):
    logs_client = None
    cached = False
    _config: FissionConfig


    @staticmethod
    def name():
        return "fission"

    @staticmethod
    def typename():
        return "Fission"

    @staticmethod
    def function_type() -> "Type[Function]":
        return FissionFunction

    @property
    def config(self) -> FissionConfig:
        return self._config

    """
        :param cache_client: Function cache instance
        :param config: Experiments config
        :param docker_client: Docker instance
    """

    def __init__(
        self,
        sebs_config: SeBSConfig,
        config: FissionConfig,
        cache_client: Cache,
        docker_client: docker.client,
        logger_handlers: LoggingHandlers,
    ):
        super().__init__(sebs_config, cache_client, docker_client)
        self.logging_handlers = logger_handlers
        self._config = config
        self.storage: Optional[Storage] = None

    """
        Create a client instance for cloud storage. When benchmark and buckets
        parameters are passed, then storage is initialized with required number
        of buckets. Buckets may be created or retrieved from cache.

        :param benchmark: benchmark name
        :param buckets: tuple of required input/output buckets
        :param replace_existing: replace existing files in cached buckets?
        :return: storage client
    """

    def get_storage(self, replace_existing: bool = False) -> PersistentStorage:
        #TODO:
        if not self.storage:
            self.storage = Storage(
                self.cache_client,
                replace_existing,
                self.config.resources._url,
                self.config.resources._access_key,
                self.config.resources._secret_key,
            )
            self.storage.logging_handlers = self.logging_handlers
        else:
            self.storage.replace_existing = replace_existing
        return self.storage

    """
        It would be sufficient to just pack the code and ship it as zip to AWS.
        However, to have a compatible function implementation across providers,
        we create a small module.
        Issue: relative imports in Python when using storage wrapper.
        Azure expects a relative import inside a module thus it's easier
        to always create a module.

        Structure:
        function
        - function.py
        - storage.py
        - resources
        handler.py

        benchmark: benchmark name
    """

    def package_code(self, directory: str, language_name: str, language_version: str, benchmark: str) -> Tuple[str, int]:
        # TODO: .js support


        CONFIG_FILES = {
            "python": ["handler.py", "requirements.txt", "requirements.txt.3.6", "requirements.txt.3.7"],
        }
        VERSION_REQUIREMENT_FILE = {
            "python": {
                "3.6": "requirements.txt.3.6",
                "3.7": "requirements.txt.3.7"
            }
        }

        package_config = CONFIG_FILES[language_name]
        requirement_files = VERSION_REQUIREMENT_FILE[language_name][language_version]

        function_dir = os.path.join(directory, "function")
        os.makedirs(function_dir)
        # move all files to 'function' except CONFIG_FILES
        for file in os.listdir(directory):

            if file in requirement_files:
                #FIXME: remove language specific
                with open(os.path.join(directory, 'requirements.txt'), 'a') as requirement_file:
                    with open(os.path.join(directory, file), 'r') as input_file:
                        requirement_file.write('\n')
                        requirement_file.write(input_file.read())

            if file not in package_config:
                file = os.path.join(directory, file)
                shutil.move(file, function_dir)

        build_file_name = os.path.join(directory, 'build.sh')

        with open(build_file_name, 'w') as build_file:
            build_file.write('''#!/bin/sh
pip3 install -r ${SRC_PKG}/requirements.txt -t ${SRC_PKG} && cp -r ${SRC_PKG} ${DEPLOY_PKG}
''')
        mode = os.stat(build_file_name).st_mode
        mode |= 0o111
        os.chmod(build_file_name, mode)

        # FIXME: use zipfile
        # create zip with handler.py and submodules
        execute("zip -qu -r9 {}.zip build.sh requirements.txt handler.py function/".format(benchmark), shell=True, cwd=directory)
        benchmark_archive = "{}.zip".format(os.path.join(directory, benchmark))
        self.logging.info("Created {} archive".format(benchmark_archive))

        # TODO: check max zip size, fission only supports to 1MB
        bytes_size = os.path.getsize(os.path.join(directory, benchmark_archive))
        mbytes = bytes_size / 1024.0 / 1024.0
        self.logging.info("Zip archive size {:2f} MB".format(mbytes))

        return directory, bytes_size
        # return os.path.join(directory, "{}.zip".format(benchmark)), bytes_size

    def create_function(self, code_package: Benchmark, func_name: str) -> "FissionFunction":

        package = code_package.code_location
        benchmark = code_package.benchmark
        language = code_package.language_name
        language_runtime = code_package.language_version
        code_size = code_package.code_size
        func_name = Fission.format_function_name(func_name)
        storage_client = self.get_storage()

        try:
            # subprocess will cause error if function is not present
            subprocess.check_output(['fission', 'function', 'get', '--name', func_name])

            self.logging.info(
                "Function {} exists on Fission, retrieve configuration.".format(func_name)
            )

            fission_function = FissionFunction(
                func_name,
                benchmark,
                code_package.hash
            )
            self.update_function(fission_function, code_package)
            fission_function.updated_code = True

        except subprocess.CalledProcessError:

            self.logging.info("Creating function {} from {}".format(func_name, package))

            code_config: Dict[str, Union[str, bytes]]
            package = code_package.code_location

            # create function
            subprocess.check_output(['fission', 'function', 'create', '--name', func_name, '--env', '{}'.format(language),
             '--src', "{}/{}.zip".format(package, benchmark), '--entrypoint', 'handler.handler',
             '--buildcmd', "./build.sh", '--executortype', 'newdeploy'])
            #FIXME: find better solution for waiting till really created func
            sleep(10)

            fission_function = FissionFunction(
                func_name,
                benchmark,
                code_package.hash
            )

        return fission_function

    def cached_function(self, function: Function):

        for trigger in function.triggers(Trigger.TriggerType.HTTP):
            trigger.logging_handlers = self.logging_handlers

    """
        Update function code and configuration on AWS.

        :param benchmark: benchmark name
        :param name: function name
        :param code_package: path to code package
        :param code_size: size of code package in bytes
        :param timeout: function timeout in seconds
        :param memory: memory limit for function
    """

    def update_function(self, function: Function, code_package: Benchmark):

        function = cast(FissionFunction, function)
        name = function.name
        benchmark = code_package.benchmark
        package = code_package.code_location

        subprocess.check_output(['fission', 'function', 'update', '--name', name, '--src', "{}/{}.zip".format(package, benchmark),
         '--buildcmd', "./build.sh"])
        #FIXME: find better solution for waiting till really updated func
        sleep(3)

        self.logging.info("Published new function code")

    @staticmethod
    def default_function_name(code_package: Benchmark) -> str:
        # Create function name
        func_name = "{}-{}-{}".format(
            code_package.benchmark,
            code_package.language_name,
            code_package.benchmark_config.memory,
        )
        return Fission.format_function_name(func_name)

    @staticmethod
    def format_function_name(func_name: str) -> str:
        # Fission wants alphabetic as first char and no dots or underscores
        func_name = func_name.replace(".", "-")
        func_name = func_name.replace("_", "-")
        if not func_name.startswith("sebs-"):
            func_name = "sebs-" + func_name
            func_name = func_name[0:10]
        return func_name

    def shutdown(self) -> None:
        super().shutdown()

    def download_metrics(
        self,
        function_name: str,
        start_time: int,
        end_time: int,
        requests: Dict[str, ExecutionResult],
        metrics: dict,
    ):
        pass

    def create_trigger(self, func: Function, trigger_type: Trigger.TriggerType) -> Trigger:

        function = cast(FissionFunction, func)

        if trigger_type == Trigger.TriggerType.HTTP:

            try:
                subprocess.check_output(['fission', 'httptrigger', 'update', '--function', function.name, '--name', "trigger-{}".format(function.name),
                '--url', 'sebs/{}'.format(function.name), '--method', 'GET', '--method', 'POST'])
            except subprocess.CalledProcessError:
                # create http trigger for function
                subprocess.check_output(['fission', 'httptrigger', 'create', '--function', function.name, '--name', "trigger-{}".format(function.name),
                '--url', 'sebs/{}'.format(function.name), '--method', 'GET', '--method', 'POST'])
                #FIXME: find better solution for waiting till really created trigger
                sleep(3)

            url = "http://{}/sebs/{}".format(self._config.resources._gateway_hostname, function.name)
            trigger = HTTPTrigger(url, self.config.resources._url_intern, self.config.resources._access_key, self.config.resources._secret_key)
            trigger.logging_handlers = self.logging_handlers
        else:
            raise RuntimeError("Not supported!")

        function.add_trigger(trigger)
        self.cache_client.update_function(function)
        return trigger

    def enforce_cold_start(self, functions: List[Function], code_package: Benchmark):
        raise NotImplementedError
