from typing import Protocol, List, Dict, Tuple
import boto3
from .exceptions import StorageError
from concurrent.futures import ThreadPoolExecutor


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

    def get_batch_multithread(self, keys: List[str], max_workers: int = 100) -> Dict[str, bytes]:
        """
        Recursively retrieve multiple S3 objects in parallel.

        Args:
            keys: List of S3 keys to retrieve
            max_workers: Maximum number of concurrent threads for retrieval

        Returns:
            Dictionary mapping keys to their contents as bytes

        Raises:
            StorageError: If any retrieval fails
        """
        results: Dict[str, bytes] = {}
        failed_keys: List[Tuple[str, Exception]] = []

        def _get_single_object(key: str) -> None:
            try:
                content = self.get(key)
                results[key] = content
            except Exception as e:
                failed_keys.append((key, e))

        # Use ThreadPoolExecutor for parallel retrieval
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            executor.map(_get_single_object, keys)

        if failed_keys:
            error_messages = '\n'.join(f"- {key}: {str(error)}" for key, error in failed_keys)
            raise StorageError(f"Failed to retrieve some objects:\n{error_messages}")

        return results

    def delete_batch_multithread(self, keys: List[str], max_workers: int = 100) -> None:
        """
        Delete multiple S3 objects in parallel.

        Args:
            keys: List of S3 keys to delete
            max_workers: Maximum number of concurrent threads for deletion

        Raises:
            StorageError: If any deletion fails
        """
        failed_keys: List[Tuple[str, Exception]] = []

        def _delete_single_object(key: str) -> None:
            try:
                self.delete(key)
            except Exception as e:
                failed_keys.append((key, e))

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            executor.map(_delete_single_object, keys)

        if failed_keys:
            error_messages = '\n'.join(f"- {key}: {str(error)}" for key, error in failed_keys)
            raise StorageError(f"Failed to delete some objects:\n{error_messages}")

    def head_batch_multithread(self, keys: List[str], max_workers: int = 100) -> Dict[str, int]:
        """
        Get content lengths for multiple S3 objects in parallel.

        Args:
            keys: List of S3 keys to get sizes for
            max_workers: Maximum number of concurrent threads

        Returns:
            Dictionary mapping keys to their content lengths

        Raises:
            StorageError: If any head request fails
        """
        results: Dict[str, int] = {}
        failed_keys: List[Tuple[str, Exception]] = []

        def _head_single_object(key: str) -> None:
            try:
                size = self.head(key)
                results[key] = size
            except Exception as e:
                failed_keys.append((key, e))

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            executor.map(_head_single_object, keys)

        if failed_keys:
            error_messages = '\n'.join(f"- {key}: {str(error)}" for key, error in failed_keys)
            raise StorageError(f"Failed to get sizes for some objects:\n{error_messages}")

        return results

    def store_batch_multithread(
        self,
        items: Dict[str, bytes],
        max_workers: int = 100
    ) -> List[str]:
        """
        Store multiple objects in S3 in parallel.

        Args:
            items: Dictionary mapping keys to their contents as bytes
            max_workers: Maximum number of concurrent threads

        Returns:
            List of successfully stored keys

        Raises:
            StorageError: If any store operation fails
        """
        failed_keys: List[Tuple[str, Exception]] = []
        successful_keys: List[str] = []

        def _store_single_object(key: str) -> None:
            try:
                self.store(key, items[key])
                successful_keys.append(key)
            except Exception as e:
                failed_keys.append((key, e))

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            executor.map(_store_single_object, items.keys())

        if failed_keys:
            error_messages = '\n'.join(f"- {key}: {str(error)}" for key, error in failed_keys)
            raise StorageError(f"Failed to store some objects:\n{error_messages}")

        return successful_keys
