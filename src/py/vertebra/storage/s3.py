"""S3 client backed by Bun filesystem IPC."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from vertebra.filesystem import AsyncS3FileSystem, FileEntry, S3FileSystem


@dataclass(slots=True)
class S3Client:
    bucket: str
    _fs: S3FileSystem

    def __init__(self, bucket: str, *, options: dict[str, Any] | None = None) -> None:
        self.bucket = bucket
        self._fs = S3FileSystem(bucket, options=options)

    def upload_bytes(
        self, key: str, data: bytes, *, content_type: str | None = None, metadata: dict[str, str] | None = None
    ) -> int:
        return self._fs.write_bytes(key, data, content_type=content_type, metadata=metadata)

    def download_bytes(self, key: str) -> bytes:
        return self._fs.read_bytes(key)

    def delete(self, key: str) -> None:
        self._fs.delete(key)

    def list(self, prefix: str = "") -> list[FileEntry]:
        return self._fs.list(prefix)

    def stat(self, key: str) -> FileEntry:
        return self._fs.stat(key)

    def exists(self, key: str) -> bool:
        return self._fs.exists(key)

    def signed_url(
        self,
        key: str,
        *,
        method: str = "GET",
        expires_in: int = 3600,
        content_type: str | None = None,
        acl: str | None = None,
    ) -> str | None:
        return self._fs.signed_url(key, method=method, expires_in=expires_in, content_type=content_type, acl=acl)


class AsyncS3Client:
    def __init__(self, bucket: str, *, options: dict[str, Any] | None = None) -> None:
        self._fs = AsyncS3FileSystem(bucket, options=options)

    async def upload_bytes(
        self, key: str, data: bytes, *, content_type: str | None = None, metadata: dict[str, str] | None = None
    ) -> int:
        return await self._fs.write_bytes(key, data, content_type=content_type, metadata=metadata)

    async def download_bytes(self, key: str) -> bytes:
        return await self._fs.read_bytes(key)

    async def delete(self, key: str) -> None:
        await self._fs.delete(key)

    async def list(self, prefix: str = "") -> list[FileEntry]:
        return await self._fs.list(prefix)

    async def stat(self, key: str) -> FileEntry:
        return await self._fs.stat(key)

    async def exists(self, key: str) -> bool:
        return await self._fs.exists(key)

    async def signed_url(
        self,
        key: str,
        *,
        method: str = "GET",
        expires_in: int = 3600,
        content_type: str | None = None,
        acl: str | None = None,
    ) -> str | None:
        return await self._fs.signed_url(key, method=method, expires_in=expires_in, content_type=content_type, acl=acl)


__all__ = ("AsyncS3Client", "S3Client")
