import json
import os
import secrets
import uuid
from typing import List, Optional

import minio

from sebs.cache import Cache
from ..faas.storage import PersistentStorage


#TODO: (de)serialize

class Storage(PersistentStorage):
    @staticmethod
    def typename() -> str:
        return "Kubeless.Minio"

    @staticmethod
    def deployment_name():
        return "kubeless"

    # the location does not matter
    MINIO_REGION = "dummy"

    def __init__(self, cache_client: Cache, replace_existing: bool, url, access_key, secret_key):
        super().__init__(self.MINIO_REGION, cache_client, replace_existing)
        self._url = url
        self._access_key = access_key
        self._secret_key = secret_key
        self.connection = minio.Minio(
            self._url, access_key=self._access_key, secret_key=self._secret_key, secure=False
        )

    def _create_bucket(self, name: str, buckets: List[str] = []):
        for bucket_name in buckets:
            if name in bucket_name:
                self.logging.info(
                    "Bucket {} for {} already exists, skipping.".format(bucket_name, name)
                )
                return bucket_name
        # minio has limit of bucket name to 16 characters
        bucket_name = "{}-{}".format(name, str(uuid.uuid4())[0:16])
        try:
            self.connection.make_bucket(bucket_name, location=self.MINIO_REGION)
            self.logging.info("Created bucket {}".format(bucket_name))
            return bucket_name
        except (
            minio.error.BucketAlreadyOwnedByYou,
            minio.error.BucketAlreadyExists,
            minio.error.ResponseError,
        ) as err:
            self.logging.error("Bucket creation failed!")
            # rethrow
            raise err

    def uploader_func(self, bucket_idx, file, filepath):
        try:
            self.connection.fput_object(self.input_buckets[bucket_idx], file, filepath)
        except minio.error.ResponseError as err:
            self.logging.error("Upload failed!")
            raise (err)

    def clean(self):
        for bucket in self.output_buckets:
            objects = self.connection.list_objects_v2(bucket)
            objects = [obj.object_name for obj in objects]
            for err in self.connection.remove_objects(bucket, objects):
                self.logging.error("Deletion Error: {}".format(err))

    def download_results(self, result_dir):
        result_dir = os.path.join(result_dir, "storage_output")
        for bucket in self.output_buckets:
            objects = self.connection.list_objects_v2(bucket)
            objects = [obj.object_name for obj in objects]
            for obj in objects:
                self.connection.fget_object(bucket, obj, os.path.join(result_dir, obj))

    def clean_bucket(self, bucket: str):
        delete_object_list = map(
            lambda x: minio.DeleteObject(x.object_name),
            self.connection.list_objects(bucket_name=bucket),
        )
        errors = self.connection.remove_objects(bucket, delete_object_list)
        for error in errors:
            self.logging.error("Error when deleting object from bucket {}: {}!", bucket, error)

    def correct_name(self, name: str) -> str:
        return name

    def download(self, bucket_name: str, key: str, filepath: str):
        raise NotImplementedError()

    def list_bucket(self, bucket_name: str):
        objects_list = self.connection.list_objects(bucket_name)
        objects: List[str]
        return [obj.object_name for obj in objects_list]

    def list_buckets(self, bucket_name: str) -> List[str]:
        buckets = self.connection.list_buckets()
        return [bucket.name for bucket in buckets if bucket_name in bucket.name]

    def upload(self, bucket_name: str, filepath: str, key: str):
        raise NotImplementedError()
