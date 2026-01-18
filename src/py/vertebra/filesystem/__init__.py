"""Filesystem clients backed by Bun IPC server."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from vertebra.filesystem.client import AsyncFileSystemClient, FileEntry, FileSystemClient

if TYPE_CHECKING:
    from vertebra.filesystem.fsspec import AsyncVertebraFileSystem, VertebraFileSystem


@dataclass(slots=True)
class LocalFileSystem:
    client: FileSystemClient

    def __init__(self) -> None:
        self.client = FileSystemClient(backend="local")

    def read_bytes(self, path: str) -> bytes:
        return self.client.read_bytes(path)

    def read_range(self, path: str, offset: int, length: int | None = None) -> bytes:
        return self.client.read_range(path, offset, length)

    def write_bytes(
        self,
        path: str,
        data: bytes,
        *,
        content_type: str | None = None,
        metadata: dict[str, str] | None = None,
    ) -> int:
        return self.client.write_bytes(path, data, content_type=content_type, metadata=metadata)

    def list(self, path: str) -> list[FileEntry]:
        return self.client.list(path)

    def read_file(self, path: str, local_path: str) -> int:
        return self.client.read_file(path, local_path)

    def write_file(
        self,
        path: str,
        local_path: str,
        *,
        content_type: str | None = None,
        metadata: dict[str, str] | None = None,
    ) -> int:
        return self.client.write_file(path, local_path, content_type=content_type, metadata=metadata)

    def stat(self, path: str) -> FileEntry:
        return self.client.stat(path)

    def exists(self, path: str) -> bool:
        return self.client.exists(path)

    def delete(self, path: str, recursive: bool = False) -> None:
        self.client.delete(path, recursive=recursive)

    def signed_url(
        self,
        path: str,
        *,
        method: str = "GET",
        expires_in: int = 3600,
        content_type: str | None = None,
        acl: str | None = None,
    ) -> str | None:
        return self.client.signed_url(
            path,
            method=method,
            expires_in=expires_in,
            content_type=content_type,
            acl=acl,
        )

    def mkdir(self, path: str) -> None:
        self.client.mkdir(path)

    def rmdir(self, path: str) -> None:
        self.client.rmdir(path, recursive=True)


@dataclass(slots=True)
class S3FileSystem:
    bucket: str
    client: FileSystemClient

    def __init__(self, bucket: str, *, options: dict[str, Any] | None = None) -> None:
        self.bucket = bucket
        self.client = FileSystemClient(backend="s3", bucket=bucket, options=options)

    def read_bytes(self, key: str) -> bytes:
        return self.client.read_bytes(key)

    def read_range(self, key: str, offset: int, length: int | None = None) -> bytes:
        return self.client.read_range(key, offset, length)

    def write_bytes(
        self,
        key: str,
        data: bytes,
        *,
        content_type: str | None = None,
        metadata: dict[str, str] | None = None,
    ) -> int:
        return self.client.write_bytes(key, data, content_type=content_type, metadata=metadata)

    def list(self, prefix: str = "") -> list[FileEntry]:
        return self.client.list(prefix)

    def read_file(self, key: str, local_path: str) -> int:
        return self.client.read_file(key, local_path)

    def write_file(
        self,
        key: str,
        local_path: str,
        *,
        content_type: str | None = None,
        metadata: dict[str, str] | None = None,
    ) -> int:
        return self.client.write_file(key, local_path, content_type=content_type, metadata=metadata)

    def stat(self, key: str) -> FileEntry:
        return self.client.stat(key)

    def exists(self, key: str) -> bool:
        return self.client.exists(key)

    def delete(self, key: str) -> None:
        self.client.delete(key)

    def signed_url(
        self,
        key: str,
        *,
        method: str = "GET",
        expires_in: int = 3600,
        content_type: str | None = None,
        acl: str | None = None,
    ) -> str | None:
        return self.client.signed_url(
            key,
            method=method,
            expires_in=expires_in,
            content_type=content_type,
            acl=acl,
        )


@dataclass(slots=True)
class GCSFileSystem:
    bucket: str
    client: FileSystemClient

    def __init__(self, bucket: str, *, options: dict[str, Any] | None = None) -> None:
        self.bucket = bucket
        self.client = FileSystemClient(backend="gcs", bucket=bucket, options=options)

    def read_bytes(self, key: str) -> bytes:
        return self.client.read_bytes(key)

    def read_range(self, key: str, offset: int, length: int | None = None) -> bytes:
        return self.client.read_range(key, offset, length)

    def write_bytes(
        self,
        key: str,
        data: bytes,
        *,
        content_type: str | None = None,
        metadata: dict[str, str] | None = None,
    ) -> int:
        return self.client.write_bytes(key, data, content_type=content_type, metadata=metadata)

    def list(self, prefix: str = "") -> list[FileEntry]:
        return self.client.list(prefix)

    def read_file(self, key: str, local_path: str) -> int:
        return self.client.read_file(key, local_path)

    def write_file(
        self,
        key: str,
        local_path: str,
        *,
        content_type: str | None = None,
        metadata: dict[str, str] | None = None,
    ) -> int:
        return self.client.write_file(key, local_path, content_type=content_type, metadata=metadata)

    def stat(self, key: str) -> FileEntry:
        return self.client.stat(key)

    def exists(self, key: str) -> bool:
        return self.client.exists(key)

    def delete(self, key: str) -> None:
        self.client.delete(key)

    def signed_url(
        self,
        key: str,
        *,
        method: str = "GET",
        expires_in: int = 3600,
        content_type: str | None = None,
        acl: str | None = None,
    ) -> str | None:
        return self.client.signed_url(
            key,
            method=method,
            expires_in=expires_in,
            content_type=content_type,
            acl=acl,
        )


class AsyncLocalFileSystem:
    def __init__(self) -> None:
        self.client = AsyncFileSystemClient(backend="local")

    async def read_bytes(self, path: str) -> bytes:
        return await self.client.read_bytes(path)

    async def read_range(self, path: str, offset: int, length: int | None = None) -> bytes:
        return await self.client.read_range(path, offset, length)

    async def write_bytes(
        self,
        path: str,
        data: bytes,
        *,
        content_type: str | None = None,
        metadata: dict[str, str] | None = None,
    ) -> int:
        return await self.client.write_bytes(path, data, content_type=content_type, metadata=metadata)

    async def list(self, path: str) -> list[FileEntry]:
        return await self.client.list(path)

    async def read_file(self, path: str, local_path: str) -> int:
        return await self.client.read_file(path, local_path)

    async def write_file(
        self,
        path: str,
        local_path: str,
        *,
        content_type: str | None = None,
        metadata: dict[str, str] | None = None,
    ) -> int:
        return await self.client.write_file(path, local_path, content_type=content_type, metadata=metadata)

    async def stat(self, path: str) -> FileEntry:
        return await self.client.stat(path)

    async def exists(self, path: str) -> bool:
        return await self.client.exists(path)

    async def delete(self, path: str, recursive: bool = False) -> None:
        await self.client.delete(path, recursive=recursive)

    async def signed_url(
        self,
        path: str,
        *,
        method: str = "GET",
        expires_in: int = 3600,
        content_type: str | None = None,
        acl: str | None = None,
    ) -> str | None:
        return await self.client.signed_url(
            path,
            method=method,
            expires_in=expires_in,
            content_type=content_type,
            acl=acl,
        )

    async def mkdir(self, path: str) -> None:
        await self.client.mkdir(path)

    async def rmdir(self, path: str) -> None:
        await self.client.rmdir(path, recursive=True)


class AsyncS3FileSystem:
    def __init__(self, bucket: str, *, options: dict[str, Any] | None = None) -> None:
        self.client = AsyncFileSystemClient(backend="s3", bucket=bucket, options=options)

    async def read_bytes(self, key: str) -> bytes:
        return await self.client.read_bytes(key)

    async def read_range(self, key: str, offset: int, length: int | None = None) -> bytes:
        return await self.client.read_range(key, offset, length)

    async def write_bytes(
        self,
        key: str,
        data: bytes,
        *,
        content_type: str | None = None,
        metadata: dict[str, str] | None = None,
    ) -> int:
        return await self.client.write_bytes(key, data, content_type=content_type, metadata=metadata)

    async def list(self, prefix: str = "") -> list[FileEntry]:
        return await self.client.list(prefix)

    async def read_file(self, key: str, local_path: str) -> int:
        return await self.client.read_file(key, local_path)

    async def write_file(
        self,
        key: str,
        local_path: str,
        *,
        content_type: str | None = None,
        metadata: dict[str, str] | None = None,
    ) -> int:
        return await self.client.write_file(key, local_path, content_type=content_type, metadata=metadata)

    async def stat(self, key: str) -> FileEntry:
        return await self.client.stat(key)

    async def exists(self, key: str) -> bool:
        return await self.client.exists(key)

    async def delete(self, key: str) -> None:
        await self.client.delete(key)

    async def signed_url(
        self,
        key: str,
        *,
        method: str = "GET",
        expires_in: int = 3600,
        content_type: str | None = None,
        acl: str | None = None,
    ) -> str | None:
        return await self.client.signed_url(
            key,
            method=method,
            expires_in=expires_in,
            content_type=content_type,
            acl=acl,
        )


class AsyncGCSFileSystem:
    def __init__(self, bucket: str, *, options: dict[str, Any] | None = None) -> None:
        self.client = AsyncFileSystemClient(backend="gcs", bucket=bucket, options=options)

    async def read_bytes(self, key: str) -> bytes:
        return await self.client.read_bytes(key)

    async def read_range(self, key: str, offset: int, length: int | None = None) -> bytes:
        return await self.client.read_range(key, offset, length)

    async def write_bytes(
        self,
        key: str,
        data: bytes,
        *,
        content_type: str | None = None,
        metadata: dict[str, str] | None = None,
    ) -> int:
        return await self.client.write_bytes(key, data, content_type=content_type, metadata=metadata)

    async def list(self, prefix: str = "") -> list[FileEntry]:
        return await self.client.list(prefix)

    async def read_file(self, key: str, local_path: str) -> int:
        return await self.client.read_file(key, local_path)

    async def write_file(
        self,
        key: str,
        local_path: str,
        *,
        content_type: str | None = None,
        metadata: dict[str, str] | None = None,
    ) -> int:
        return await self.client.write_file(key, local_path, content_type=content_type, metadata=metadata)

    async def stat(self, key: str) -> FileEntry:
        return await self.client.stat(key)

    async def exists(self, key: str) -> bool:
        return await self.client.exists(key)

    async def delete(self, key: str) -> None:
        await self.client.delete(key)

    async def signed_url(
        self,
        key: str,
        *,
        method: str = "GET",
        expires_in: int = 3600,
        content_type: str | None = None,
        acl: str | None = None,
    ) -> str | None:
        return await self.client.signed_url(
            key,
            method=method,
            expires_in=expires_in,
            content_type=content_type,
            acl=acl,
        )


__all__ = [
    "AsyncGCSFileSystem",
    "AsyncLocalFileSystem",
    "AsyncS3FileSystem",
    "AsyncVertebraFileSystem",
    "FileEntry",
    "GCSFileSystem",
    "LocalFileSystem",
    "S3FileSystem",
    "VertebraFileSystem",
]


def __getattr__(name: str) -> Any:
    if name in {"AsyncVertebraFileSystem", "VertebraFileSystem"}:
        from vertebra.filesystem.fsspec import AsyncVertebraFileSystem, VertebraFileSystem

        return {
            "AsyncVertebraFileSystem": AsyncVertebraFileSystem,
            "VertebraFileSystem": VertebraFileSystem,
        }[name]
    raise AttributeError(name)
