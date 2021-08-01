from typing import Dict, Optional  # noqa

from sebs.faas.function import ExecutionResult, Trigger


class HTTPTrigger(Trigger):
    def __init__(self, url: str, storage_url: str, access_key: str, secret_key: str):
        super().__init__()
        self.url = url
        self.storage_url = storage_url
        self.access_key = access_key
        self.secret_key = secret_key

    @staticmethod
    def typename() -> str:
        return "Kubeless.HTTPTrigger"

    @staticmethod
    def trigger_type() -> Trigger.TriggerType:
        return Trigger.TriggerType.HTTP

    def sync_invoke(self, payload: dict) -> ExecutionResult:

        payload["minio_sebs_storage_url"] = self.storage_url
        payload["minio_sebs_storage_access_key"] = self.access_key
        payload["minio_sebs_storage_secret_key"] = self.secret_key

        self.logging.debug(f"Invoke function {self.url}")
        return self._http_invoke(payload, self.url)

    def async_invoke(self, payload: dict) -> ExecutionResult:
        import concurrent.futures

        pool = concurrent.futures.ThreadPoolExecutor()
        fut = pool.submit(self.sync_invoke, payload)
        return fut

    def serialize(self) -> dict:
        return {"type": "HTTP", "url": self.url, "storage_url": self.storage_url, "access_key": self.access_key, "secret_key": self.secret_key}

    @staticmethod
    def deserialize(obj: dict) -> Trigger:
        return HTTPTrigger(obj["url"], obj["storage_url"], obj["access_key"], obj["secret_key"])
