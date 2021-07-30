import json
import os
import time
from typing import cast, Dict, Optional

from sebs.cache import Cache
from sebs.faas.config import Config, Credentials, Resources
from sebs.utils import LoggingHandlers


class KubelessCredentials(Credentials):

    def __init__():
        super().__init__()

    @staticmethod
    def typename() -> str:
        return "Kubeless.Credentials"

    @staticmethod
    def initialize(dct: dict) -> Credentials:
        return KubelessCredentials()

    @staticmethod
    def deserialize(config: dict, cache: Cache, handlers: LoggingHandlers) -> Credentials:

        # FIXME: update return types of both functions to avoid cast
        # needs 3.7+  to support annotations
        cached_config = cache.get_config("kubeless")
        ret: KubelessCredentials
        # Load cached values
        if cached_config and "credentials" in cached_config:
            ret = cast(KubelessCredentials, KubelessCredentials.initialize(cached_config["credentials"]))
            ret.logging_handlers = handlers
            ret.logging.info("Using cached credentials for Kubeless")
        else:
            # Check for new config
            if "credentials" in config:
                ret = cast(KubelessCredentials, KubelessCredentials.initialize(config["credentials"]))
            elif "KUBELESS_CONTEXT" in os.environ:
                ret = KubelessCredentials()
            else:
                ret = KubelessCredentials()
                ret.logging.info("Using set Kubernetes context.")
            ret.logging.info("No cached credentials for AWS found, initialize!")
            ret.logging_handlers = handlers
        return ret

    def update_cache(self, cache: Cache):
        pass

    def serialize(self) -> dict:
        out = {}
        return out


class KubelessResources(Resources):

    _url: str
    _access_key: str
    _secret_key: str

    def __init__(self, url: str, access_key: str, secret_key: str):
        super().__init__()
        self._url = url
        self._access_key = access_key
        self._secret_key = secret_key

    @staticmethod
    def typename() -> str:
        return "Kubeless.Resources"

    # FIXME: python3.7+ future annotatons
    @staticmethod
    def initialize(dct: dict) -> Resources:
        if "storage" not in dct:
            return
        storage_dict = dct["storage"]
        ret = KubelessResources(storage_dict["url"] if "url" in storage_dict else "",
          storage_dict["access_key"] if "access_key" in storage_dict else "",
          storage_dict["secret_key"] if "secret_key" in storage_dict else "")
        return ret

    def serialize(self) -> dict:
        out = {
            "storage": {
                "url": self._url,
                "access_key": self._access_key,
                "secret_key": self._secret_key
            } 
        }
        return out

    def update_cache(self, cache: Cache):
        cache.update_config(val=self._url, keys=["kubeless", "resources", "storage", "url"])
        cache.update_config(val=self._access_key, keys=["kubeless", "resources", "storage", "access_key"])
        cache.update_config(val=self._secret_key, keys=["kubeless", "resources", "storage", "secret_key"])        

    @staticmethod
    def deserialize(config: dict, cache: Cache, handlers: LoggingHandlers) -> Resources:

        cached_config = cache.get_config("kubeless")
        ret: KubelessResources
        # Load cached values
        if cached_config and "resources" in cached_config:
            ret = cast(KubelessResources, KubelessResources.initialize(cached_config["resources"]))
            ret.logging_handlers = handlers
            ret.logging.info("Using cached resources for Kubeless")
        else:
            # Check for new config
            if "resources" in config:
                ret = cast(KubelessResources, KubelessResources.initialize(config["resources"]))
                ret.logging_handlers = handlers
                ret.logging.info("No cached resources for Kubeless found, using user configuration.")
            else:
                ret = KubelessResources()
                ret.logging_handlers = handlers
                ret.logging.info("No resources for Kubeless found, initialize!")

        return ret


class KubelessConfig(Config):
    def __init__(self, credentials: KubelessCredentials, resources: KubelessResources):
        super().__init__()
        self._credentials = credentials
        self._resources = resources

    @staticmethod
    def typename() -> str:
        return "Kubeless.Config"

    @property
    def credentials(self) -> KubelessCredentials:
        return self._credentials

    @property
    def resources(self) -> KubelessResources:
        return self._resources

    # FIXME: use future annotations (see sebs/faas/system)
    @staticmethod
    def initialize(cfg: Config, dct: dict):
        pass

    @staticmethod
    def deserialize(config: dict, cache: Cache, handlers: LoggingHandlers) -> Config:

        cached_config = cache.get_config("kubeless")
        # FIXME: use future annotations (see sebs/faas/system)
        credentials = cast(KubelessCredentials, KubelessCredentials.deserialize(config, cache, handlers))
        resources = cast(KubelessResources, KubelessResources.deserialize(config, cache, handlers))
        config_obj = KubelessConfig(credentials, resources)
        config_obj.logging_handlers = handlers
        # Load cached values
        if cached_config:
            config_obj.logging.info("Using cached config for Kubeless")
            KubelessConfig.initialize(config_obj, cached_config)
        else:
            config_obj.logging.info("Using user-provided config for Kubeless")
            KubelessConfig.initialize(config_obj, config)

        resources.set_region(config_obj.region)
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
            "name": "kubeless",
            "credentials": self._credentials.serialize(),
            "resources": self._resources.serialize(),
        }
        return out
