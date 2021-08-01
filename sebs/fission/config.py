import json
import os
import time
from typing import cast, Dict, Optional

from sebs.cache import Cache
from sebs.faas.config import Config, Credentials, Resources
from sebs.utils import LoggingHandlers


class FissionCredentials(Credentials):

    def __init__(self):
        super().__init__()

    @staticmethod
    def typename() -> str:
        return "Fission.Credentials"

    @staticmethod
    def initialize(dct: dict) -> Credentials:
        return FissionCredentials()

    @staticmethod
    def deserialize(config: dict, cache: Cache, handlers: LoggingHandlers) -> Credentials:

        # FIXME: update return types of both functions to avoid cast
        # needs 3.7+  to support annotations
        cached_config = cache.get_config("fission")
        ret: FissionCredentials
        # Load cached values
        if cached_config and "credentials" in cached_config:
            ret = cast(FissionCredentials, FissionCredentials.initialize(cached_config["credentials"]))
            ret.logging_handlers = handlers
            ret.logging.info("Using cached credentials for Fission")
        else:
            # Check for new config
            if "credentials" in config:
                ret = cast(FissionCredentials, FissionCredentials.initialize(config["credentials"]))
            elif "KUBELESS_CONTEXT" in os.environ:
                ret = FissionCredentials()
            else:
                ret = FissionCredentials()
                ret.logging.info("Using set Kubernetes context.")
            ret.logging.info("No cached credentials for AWS found, initialize!")
            ret.logging_handlers = handlers
        return ret

    def update_cache(self, cache: Cache):
        pass

    def serialize(self) -> dict:
        out = {}
        return out


class FissionResources(Resources):

    _url: str
    _url_intern: str
    _access_key: str
    _secret_key: str
    _gateway_type: str
    _gateway_hostname: str

    def __init__(self, url: str, url_intern: str, access_key: str, secret_key: str, gateway_hostname: str, gateway_type: str):
        super().__init__()
        self._url = url
        self._url_intern = url_intern
        self._access_key = access_key
        self._secret_key = secret_key
        self._gateway_type = gateway_type
        self._gateway_hostname = gateway_hostname

    @staticmethod
    def typename() -> str:
        return "Fission.Resources"

    # FIXME: python3.7+ future annotatons
    @staticmethod
    def initialize(dct: dict) -> Resources:

        url = ""
        url_intern = ""
        access_key = ""
        secret_key = ""
        gateway_hostname = ""
        gateway_type = "nginx"

        if "storage" in dct:
            storage_dict = dct["storage"]
            url = storage_dict["url"] if "url" in storage_dict else ""
            url_intern = storage_dict["url_intern"] if "url_intern" in storage_dict else ""
            access_key = storage_dict["access_key"] if "access_key" in storage_dict else ""
            secret_key = storage_dict["secret_key"] if "secret_key" in storage_dict else ""

        if "ingress" in dct:
            ingress_dict = dct["ingress"]
            gateway_hostname = ingress_dict["hostname"] if "hostname" in ingress_dict else ""
            gateway_type = ingress_dict["type"] if "type" in ingress_dict else "nginx"

        ret = FissionResources(url, url_intern, access_key, secret_key, gateway_hostname, gateway_type)
        return ret

    def serialize(self) -> dict:
        out = {
            "storage": {
                "url": self._url,
                "url_intern": self._url_intern,
                "access_key": self._access_key,
                "secret_key": self._secret_key
            },
            "ingress": {
                "hostname": self._gateway_hostname,
                "type": self._gateway_type
            }
        }
        return out

    def update_cache(self, cache: Cache):
        cache.update_config(val=self._url, keys=["fission", "resources", "storage", "url"])
        cache.update_config(val=self._url, keys=["fission", "resources", "storage", "url_intern"])
        cache.update_config(val=self._access_key, keys=["fission", "resources", "storage", "access_key"])
        cache.update_config(val=self._secret_key, keys=["fission", "resources", "storage", "secret_key"])
        cache.update_config(val=self._gateway_hostname, keys=["fission", "resources", "ingress", "hostname"])
        cache.update_config(val=self._gateway_type, keys=["fission", "resources", "ingress", "type"])

    @staticmethod
    def deserialize(config: dict, cache: Cache, handlers: LoggingHandlers) -> Resources:

        cached_config = cache.get_config("fission")
        ret: FissionResources
        # Load cached values
        if cached_config and "resources" in cached_config:
            ret = cast(FissionResources, FissionResources.initialize(cached_config["resources"]))
            ret.logging_handlers = handlers
            ret.logging.info("Using cached resources for Fission")
        else:
            # Check for new config
            if "resources" in config:
                ret = cast(FissionResources, FissionResources.initialize(config["resources"]))
                ret.logging_handlers = handlers
                ret.logging.info("No cached resources for Fission found, using user configuration.")
            else:
                ret = FissionResources()
                ret.logging_handlers = handlers
                ret.logging.info("No resources for Fission found, initialize!")

        return ret


class FissionConfig(Config):
    def __init__(self, credentials: FissionCredentials, resources: FissionResources):
        super().__init__()
        self._credentials = credentials
        self._resources = resources
        self._region = "dummy"

    @staticmethod
    def typename() -> str:
        return "Fission.Config"

    @property
    def credentials(self) -> FissionCredentials:
        return self._credentials

    @property
    def resources(self) -> FissionResources:
        return self._resources

    # FIXME: use future annotations (see sebs/faas/system)
    @staticmethod
    def initialize(cfg: Config, dct: dict):
        pass

    @staticmethod
    def deserialize(config: dict, cache: Cache, handlers: LoggingHandlers) -> Config:

        cached_config = cache.get_config("fission")
        # FIXME: use future annotations (see sebs/faas/system)
        credentials = cast(FissionCredentials, FissionCredentials.deserialize(config, cache, handlers))
        resources = cast(FissionResources, FissionResources.deserialize(config, cache, handlers))
        config_obj = FissionConfig(credentials, resources)
        config_obj.logging_handlers = handlers
        # Load cached values
        if cached_config:
            config_obj.logging.info("Using cached config for Fission")
            FissionConfig.initialize(config_obj, cached_config)
        else:
            config_obj.logging.info("Using user-provided config for Fission")
            FissionConfig.initialize(config_obj, config)

        return config_obj

    """
        Update the contents of the user cache.
        The changes are directly written to the file system.

        Update values: region.
    """

    def update_cache(self, cache: Cache):
        self.credentials.update_cache(cache)
        self.resources.update_cache(cache)

    def serialize(self) -> dict:
        out = {
            "name": "fission",
            "credentials": self._credentials.serialize(),
            "resources": self._resources.serialize(),
        }
        return out
