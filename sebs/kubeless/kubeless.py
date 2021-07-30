import math
import os
import subprocess
import shutil
import time
import uuid
from typing import cast, Dict, List, Optional, Tuple, Type, Union  # noqa

import docker
from sebs import config

from sebs.kubeless.storage import Storage
from sebs.kubeless.function import KubelessFunction
from sebs.kubeless.config import KubelessConfig
from sebs.utils import execute
from sebs.benchmark import Benchmark
from sebs.cache import Cache
from sebs.config import SeBSConfig
from sebs.utils import LoggingHandlers
from sebs.faas.function import Function, ExecutionResult, Trigger
from sebs.faas.storage import PersistentStorage
from sebs.faas.system import System

#TODO: namespace from kubeconfig

class Kubeless(System):
    logs_client = None
    cached = False
    _config: KubelessConfig

    @staticmethod
    def name():
        return "kubeless"

    @staticmethod
    def typename():
        return "Kubeless"

    @staticmethod
    def function_type() -> "Type[Function]":
        return KubelessFunction

    @property
    def config(self) -> KubelessConfig:
        return self._config

    """
        :param cache_client: Function cache instance
        :param config: Experiments config
        :param docker_client: Docker instance
    """

    def __init__(
        self,
        sebs_config: SeBSConfig,
        config: KubelessConfig,
        cache_client: Cache,
        docker_client: docker.client,
        logger_handlers: LoggingHandlers,
    ):
        super().__init__(sebs_config, cache_client, docker_client)
        self.logging_handlers = logger_handlers
        self._config = config
        self.storage: 


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
                config.minio_url,
                self.config.credentials.access_key,
                self.config.credentials.secret_key,
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

    def package_code(self, directory: str, language_name: str, benchmark: str) -> Tuple[str, int]:
        # TODO: .js support

        CONFIG_FILES = {
            "python": ["handler.py", "requirements.txt"],
        }
        package_config = CONFIG_FILES[language_name]
        function_dir = os.path.join(directory, "function")
        os.makedirs(function_dir)
        # move all files to 'function' except CONFIG_FILES
        for file in os.listdir(directory):
            if file not in package_config:
                file = os.path.join(directory, file)
                shutil.move(file, function_dir)

        # FIXME: use zipfile
        # create zip with handler.py and submodules
        execute("zip -qu -r9 {}.zip handler.py function/".format(benchmark), shell=True, cwd=directory)
        benchmark_archive = "{}.zip".format(os.path.join(directory, benchmark))
        self.logging.info("Created {} archive".format(benchmark_archive))

        # TODO: check max uzip size, kubeless only supports to 1MB
        bytes_size = os.path.getsize(os.path.join(directory, benchmark_archive))
        mbytes = bytes_size / 1024.0 / 1024.0
        self.logging.info("Zip archive size {:2f} MB".format(mbytes))

        return os.path.join(directory, "{}.zip".format(benchmark)), bytes_size

    def create_function(self, code_package: Benchmark, func_name: str) -> "KubelessFunction":

        package = code_package.code_location
        benchmark = code_package.benchmark
        language = code_package.language_name
        language_runtime = code_package.language_version
        timeout = code_package.benchmark_config.timeout
        memory = code_package.benchmark_config.memory
        code_size = code_package.code_size
        code_bucket: Optional[str] = None
        func_name = Kubeless.format_function_name(func_name)
        storage_client = self.get_storage()
        
        # we can either check for exception or use list_functions
        try:
            # subprocess will cause error if function is not present
            subprocess.check_output(['kubeless', 'function', 'list', func_name])

            self.logging.info(
                "Function {} exists on Kubeless, retrieve configuration.".format(func_name)
            )

            #TODO: correct when KubelessFunc is implemented
            kubeless_function = KubelessFunction(
                func_name,
                code_package.benchmark,
                ret["Configuration"]["FunctionArn"],
                code_package.hash,
                timeout,
                memory,
                language_runtime,
                self.config.resources.lambda_role(self.session),
            )
            self.update_function(kubeless_function, code_package)
            kubeless_function.updated_code = True

        except subprocess.CalledProcessError:

            self.logging.info("Creating function {} from {}".format(func_name, package))

            code_config: Dict[str, Union[str, bytes]]

            #TODO: check where in code_package correct info for subprocess is held
            subprocess.check_output(['kubeless', 'function', 'deploy', func_name , '--runtime', '{}{}'.format(language, language_runtime),
             '--from-file', code_package.code_location, '--handler', 'handler.handler', '--dependencies', code_package.code_package])
            
            #TODO: correct when KubelessFunc is implemented
            kubeless_function = KubelessFunction(
                func_name,
                code_package.benchmark,
                ret["FunctionArn"],
                code_package.hash,
                timeout,
                memory,
                language_runtime,
                self.config.resources.lambda_role(self.session),
                code_bucket,
            )

        #TODO: add correct http trigger
        # Add LibraryTrigger to a new function

        trigger = LibraryTrigger(func_name, self)
        trigger.logging_handlers = self.logging_handlers
        kubeless_function.add_trigger(trigger)

        return kubeless_function

    def cached_function(self, function: Function):
        #TODO:

        for trigger in function.triggers(Trigger.TriggerType.LIBRARY):
            trigger.logging_handlers = self.logging_handlers
            cast(LibraryTrigger, trigger).deployment_client = self
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

        function = cast(KubelessFunction, function)
        name = function.name
        code_size = code_package.code_size
        package = code_package.code_location
        subprocess.check_output(['kubeless', 'function', 'update', name, '--from-file', package, '--dependencies', code_package.code_package])
            
        self.logging.info("Published new function code")

    @staticmethod
    def default_function_name(code_package: Benchmark) -> str:
        # Create function name
        func_name = "{}-{}-{}".format(
            code_package.benchmark,
            code_package.language_name,
            code_package.benchmark_config.memory,
        )
        return Kubeless.format_function_name(func_name)

    @staticmethod
    def format_function_name(func_name: str) -> str:
        # Kubeless doesnt restrict function names (as far as i know)
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
        #TODO

        function = cast(KubelessFunction, func)

        if trigger_type == Trigger.TriggerType.HTTP:

            api_name = "{}-http-api".format(function.name)
            http_api = self.config.resources.http_api(api_name, function, self.session)
            # https://aws.amazon.com/blogs/compute/announcing-http-apis-for-amazon-api-gateway/
            # but this is wrong - source arn must be {api-arn}/*/*
            self.get_lambda_client().add_permission(
                FunctionName=function.name,
                StatementId=str(uuid.uuid1()),
                Action="lambda:InvokeFunction",
                Principal="apigateway.amazonaws.com",
                SourceArn=f"{http_api.arn}/*/*",
            )
            trigger = HTTPTrigger(http_api.endpoint, api_name)
            trigger.logging_handlers = self.logging_handlers
        elif trigger_type == Trigger.TriggerType.LIBRARY:
            # should already exist
            return func.triggers(Trigger.TriggerType.LIBRARY)[0]
        else:
            raise RuntimeError("Not supported!")

        function.add_trigger(trigger)
        self.cache_client.update_function(function)
        return trigger

    def enforce_cold_start(self, functions: List[Function], code_package: Benchmark):
        raise NotImplementedError
