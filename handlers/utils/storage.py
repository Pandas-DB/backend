from typing import Protocol, List
import boto3
from .exceptions import StorageError


class StorageBackend(Protocol):
    def store(self, path: str, content: bytes) -> str: pass
    def get(self, path: str) -> bytes: pass
    def delete(self, path: str) -> None: pass
    def list(self, prefix: str) -> List[str]: pass
    def head(self, path: str) -> int: pass


class S3Storage(StorageBackend):
    def __init__(self, bucket: str):
        self.s3 = boto3.client('s3')
        self.bucket = bucket

    def store(self, path: str, content: bytes) -> str:
        self.s3.put_object(Bucket=self.bucket, Key=path, Body=content)
        return path

    def get(self, path: str) -> bytes:
        try:
            return self.s3.get_object(Bucket=self.bucket, Key=path)['Body'].read()
        except Exception as e:
            raise StorageError(f"Failed to get {path}: {str(e)}")

    def delete(self, path: str) -> None:
        try:
            self.s3.delete_object(Bucket=self.bucket, Key=path)
        except Exception as e:
            raise StorageError(f"Failed to delete {path}: {str(e)}")

    def list(self, prefix: str) -> List[str]:
        try:
            paginator = self.s3.get_paginator('list_objects_v2')
            objects = []
            for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
                if 'Contents' in page:
                    objects.extend([obj['Key'] for obj in page['Contents']])
            return objects
        except Exception as e:
            raise StorageError(f"Failed to list {prefix}: {str(e)}")

    def head(self, path: str) -> int:
        try:
            size = self.s3.head_object(Bucket=self.bucket, Key=path)['ContentLength']
            return size
        except Exception as e:
            raise StorageError(f"Failed to get size for {path}: {str(e)}")