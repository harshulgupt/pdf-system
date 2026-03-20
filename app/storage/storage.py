"""
Storage abstraction — local filesystem now, S3 in production.
To swap: set STORAGE_BACKEND=s3 and provide AWS credentials.
The service layer calls only store_chunk() and retrieve_chunk().
"""
import os
import uuid
from abc import ABC, abstractmethod

STORAGE_DIR = os.getenv("STORAGE_DIR", "./chunk_storage")
os.makedirs(STORAGE_DIR, exist_ok=True)


class AbstractStorage(ABC):
    @abstractmethod
    def store_chunk(self, data: bytes, file_id: str, chunk_index: int) -> str: ...

    @abstractmethod
    def retrieve_chunk(self, path: str) -> bytes: ...


class LocalFileStorage(AbstractStorage):
    """Stores chunks as files on disk. In prod, swap for S3Storage."""

    def store_chunk(self, data: bytes, file_id: str, chunk_index: int) -> str:
        dir_path = os.path.join(STORAGE_DIR, file_id)
        os.makedirs(dir_path, exist_ok=True)
        path = os.path.join(dir_path, f"chunk_{chunk_index:06d}.bin")
        with open(path, "wb") as f:
            f.write(data)
        return path

    def retrieve_chunk(self, path: str) -> bytes:
        with open(path, "rb") as f:
            return f.read()


# ---------- S3 stub (uncomment + pip install boto3 to activate) ----------
# class S3Storage(AbstractStorage):
#     def __init__(self):
#         import boto3
#         self.s3 = boto3.client("s3")
#         self.bucket = os.getenv("S3_BUCKET")
#
#     def store_chunk(self, data, file_id, chunk_index):
#         key = f"{file_id}/chunk_{chunk_index:06d}.bin"
#         self.s3.put_object(Bucket=self.bucket, Key=key, Body=data)
#         return key
#
#     def retrieve_chunk(self, path):
#         obj = self.s3.get_object(Bucket=self.bucket, Key=path)
#         return obj["Body"].read()


def get_storage() -> AbstractStorage:
    backend = os.getenv("STORAGE_BACKEND", "local")
    if backend == "local":
        return LocalFileStorage()
    # elif backend == "s3": return S3Storage()
    raise ValueError(f"Unknown storage backend: {backend}")
