"""Filesystem IPC client helpers."""

import asyncio
from collections.abc import Awaitable, Coroutine
from typing import Any, Literal, cast

import msgspec

from vertebra.common.exceptions import BrokerError, StorageError
from vertebra.resources.broker import AsyncBrokerClient, BrokerClientConfig

Backend = Literal["local", "s3", "gcs"]


class FileEntry(msgspec.Struct, frozen=True, gc=False):
    """Filesystem entry metadata returned from IPC operations."""

    path: str
    is_dir: bool
    size: int | None = None
    modified: str | None = None
    etag: str | None = None

    @classmethod
    def from_dict(cls, payload: "dict[str, Any]") -> "FileEntry":
        return cls(
            path=str(payload.get("path") or ""),
            is_dir=bool(payload.get("isDir") or payload.get("is_dir")),
            size=payload.get("size"),
            modified=payload.get("modified"),
            etag=payload.get("etag"),
        )


def _run_sync(coro: Awaitable[Any]) -> Any:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(cast("Coroutine[Any, Any, Any]", coro))
    msg = "Use AsyncFileSystemClient inside an event loop"
    raise RuntimeError(msg)


def _coerce_bytes(value: Any) -> bytes:
    if value is None:
        return b""
    if isinstance(value, bytes):
        return value
    if isinstance(value, bytearray):
        return bytes(value)
    if isinstance(value, memoryview):
        return value.tobytes()
    return bytes(value)


def _parse_cloud_path(path: str, default_bucket: str | None, scheme: str) -> "tuple[str, str]":
    scheme_prefix = f"{scheme}://"
    aliases = [scheme_prefix]
    if scheme == "gcs":
        aliases.append("gs://")
    for prefix in aliases:
        if path.startswith(prefix):
            without_scheme = path[len(prefix) :]
            parts = without_scheme.split("/", 1)
            if len(parts) == 1:
                bucket = parts[0]
                key = ""
            else:
                bucket, key = parts[0], parts[1]
            return bucket, key
    if not default_bucket:
        msg = "Bucket is required for cloud filesystem operations"
        raise StorageError(msg)
    return default_bucket, path.lstrip("/")


class SyncFileSystemClient:
    def __init__(
        self,
        backend: Backend = "local",
        *,
        pool: "str | None" = None,
        client_config: "BrokerClientConfig | None" = None,
        bucket: "str | None" = None,
        options: "dict[str, Any] | None" = None,
    ) -> None:
        self._backend = backend
        self._bucket = bucket
        self._options = options or {}
        self._pool = pool or f"storage-{backend}"
        self._client = AsyncBrokerClient(client_config or BrokerClientConfig())
        self._token: str | None = None

    def _ensure_token(self) -> str:
        if self._token is None:
            self._token = _run_sync(self._client.acquire(self._pool))
        return self._token

    def _build_params(self, path: str) -> "dict[str, Any]":
        if self._backend == "local":
            return {"backend": self._backend, "path": path}
        scheme = "s3" if self._backend == "s3" else "gcs"
        bucket, key = _parse_cloud_path(path, self._bucket, scheme)
        return {"backend": self._backend, "bucket": bucket, "key": key, "options": self._options}

    def _request(self, method: str, params: "dict[str, Any]") -> "dict[str, Any]":
        token = self._ensure_token()
        try:
            result = _run_sync(self._client.command(self._pool, token, method, [params]))
        except BrokerError as exc:
            raise StorageError(str(exc)) from exc
        if isinstance(result, dict):
            return result
        return {"result": result}

    def read_bytes(self, path: str) -> bytes:
        params = self._build_params(path)
        response = self._request("read", params)
        return _coerce_bytes(response.get("data"))

    def read_range(self, path: str, offset: int, length: int | None = None) -> bytes:
        params = self._build_params(path)
        range_payload = {"offset": int(offset)}
        if length is not None:
            range_payload["length"] = int(length)
        params["range"] = range_payload
        response = self._request("read_range", params)
        return _coerce_bytes(response.get("data"))

    def write_bytes(
        self, path: str, data: bytes, *, content_type: str | None = None, metadata: "dict[str, str] | None" = None
    ) -> int:
        params = self._build_params(path)
        params["data"] = data
        if content_type:
            params["contentType"] = content_type
        if metadata:
            params["metadata"] = metadata
        response = self._request("write", params)
        return int(response.get("bytes", len(data)))

    def list(self, path: str, *, prefix: str | None = None) -> "list[FileEntry]":
        params = self._build_params(path)
        if prefix:
            params["prefix"] = prefix
        response = self._request("list", params)
        entries = response.get("entries", []) if isinstance(response, dict) else []
        return [FileEntry.from_dict(entry) for entry in entries]

    def read_file(self, path: str, local_path: str) -> int:
        params = self._build_params(path)
        params["localPath"] = local_path
        response = self._request("read_file", params)
        return int(response.get("bytes", 0))

    def write_file(
        self, path: str, local_path: str, *, content_type: str | None = None, metadata: "dict[str, str] | None" = None
    ) -> int:
        params = self._build_params(path)
        params["localPath"] = local_path
        if content_type:
            params["contentType"] = content_type
        if metadata:
            params["metadata"] = metadata
        response = self._request("write_file", params)
        return int(response.get("bytes", 0))

    def stat(self, path: str) -> FileEntry:
        params = self._build_params(path)
        response = self._request("stat", params)
        return FileEntry.from_dict(response)

    def exists(self, path: str) -> bool:
        params = self._build_params(path)
        response = self._request("exists", params)
        return bool(response.get("exists"))

    def delete(self, path: str, *, recursive: bool = False) -> None:
        params = self._build_params(path)
        params["recursive"] = recursive
        self._request("delete", params)

    def signed_url(
        self,
        path: str,
        *,
        method: str = "GET",
        expires_in: int = 3600,
        content_type: str | None = None,
        acl: str | None = None,
    ) -> "str | None":
        params = self._build_params(path)
        params["method"] = method
        params["expiresIn"] = int(expires_in)
        if content_type:
            params["contentType"] = content_type
        if acl:
            params["acl"] = acl
        response = self._request("signed_url", params)
        if not response.get("supported", False):
            return None
        url = response.get("url")
        return str(url) if url else None

    def mkdir(self, path: str) -> None:
        params = self._build_params(path)
        self._request("mkdir", params)

    def rmdir(self, path: str, *, recursive: bool = False) -> None:
        params = self._build_params(path)
        params["recursive"] = recursive
        self._request("rmdir", params)

    def close(self) -> None:
        if self._token is not None:
            try:
                _run_sync(self._client.release(self._pool, self._token))
            finally:
                self._token = None
        _run_sync(self._client.close())


class AsyncFileSystemClient:
    def __init__(
        self,
        backend: Backend = "local",
        *,
        pool: "str | None" = None,
        client_config: "BrokerClientConfig | None" = None,
        bucket: "str | None" = None,
        options: "dict[str, Any] | None" = None,
    ) -> None:
        self._backend = backend
        self._bucket = bucket
        self._options = options or {}
        self._pool = pool or f"storage-{backend}"
        self._client = AsyncBrokerClient(client_config or BrokerClientConfig())
        self._token: str | None = None

    async def _ensure_token(self) -> str:
        if self._token is None:
            self._token = await self._client.acquire(self._pool)
        return self._token

    def _build_params(self, path: str) -> "dict[str, Any]":
        if self._backend == "local":
            return {"backend": self._backend, "path": path}
        scheme = "s3" if self._backend == "s3" else "gcs"
        bucket, key = _parse_cloud_path(path, self._bucket, scheme)
        return {"backend": self._backend, "bucket": bucket, "key": key, "options": self._options}

    async def _request(self, method: str, params: "dict[str, Any]") -> "dict[str, Any]":
        token = await self._ensure_token()
        try:
            result = await self._client.command(self._pool, token, method, [params])
        except BrokerError as exc:
            raise StorageError(str(exc)) from exc
        if isinstance(result, dict):
            return result
        return {"result": result}

    async def read_bytes(self, path: str) -> bytes:
        params = self._build_params(path)
        response = await self._request("read", params)
        return _coerce_bytes(response.get("data"))

    async def read_range(self, path: str, offset: int, length: int | None = None) -> bytes:
        params = self._build_params(path)
        range_payload = {"offset": int(offset)}
        if length is not None:
            range_payload["length"] = int(length)
        params["range"] = range_payload
        response = await self._request("read_range", params)
        return _coerce_bytes(response.get("data"))

    async def write_bytes(
        self, path: str, data: bytes, *, content_type: str | None = None, metadata: "dict[str, str] | None" = None
    ) -> int:
        params = self._build_params(path)
        params["data"] = data
        if content_type:
            params["contentType"] = content_type
        if metadata:
            params["metadata"] = metadata
        response = await self._request("write", params)
        return int(response.get("bytes", len(data)))

    async def list(self, path: str, *, prefix: str | None = None) -> "list[FileEntry]":
        params = self._build_params(path)
        if prefix:
            params["prefix"] = prefix
        response = await self._request("list", params)
        entries = response.get("entries", []) if isinstance(response, dict) else []
        return [FileEntry.from_dict(entry) for entry in entries]

    async def read_file(self, path: str, local_path: str) -> int:
        params = self._build_params(path)
        params["localPath"] = local_path
        response = await self._request("read_file", params)
        return int(response.get("bytes", 0))

    async def write_file(
        self, path: str, local_path: str, *, content_type: str | None = None, metadata: "dict[str, str] | None" = None
    ) -> int:
        params = self._build_params(path)
        params["localPath"] = local_path
        if content_type:
            params["contentType"] = content_type
        if metadata:
            params["metadata"] = metadata
        response = await self._request("write_file", params)
        return int(response.get("bytes", 0))

    async def stat(self, path: str) -> FileEntry:
        params = self._build_params(path)
        response = await self._request("stat", params)
        return FileEntry.from_dict(response)

    async def exists(self, path: str) -> bool:
        params = self._build_params(path)
        response = await self._request("exists", params)
        return bool(response.get("exists"))

    async def delete(self, path: str, *, recursive: bool = False) -> None:
        params = self._build_params(path)
        params["recursive"] = recursive
        await self._request("delete", params)

    async def signed_url(
        self,
        path: str,
        *,
        method: str = "GET",
        expires_in: int = 3600,
        content_type: str | None = None,
        acl: str | None = None,
    ) -> "str | None":
        params = self._build_params(path)
        params["method"] = method
        params["expiresIn"] = int(expires_in)
        if content_type:
            params["contentType"] = content_type
        if acl:
            params["acl"] = acl
        response = await self._request("signed_url", params)
        if not response.get("supported", False):
            return None
        url = response.get("url")
        return str(url) if url else None

    async def mkdir(self, path: str) -> None:
        params = self._build_params(path)
        await self._request("mkdir", params)

    async def rmdir(self, path: str, *, recursive: bool = False) -> None:
        params = self._build_params(path)
        params["recursive"] = recursive
        await self._request("rmdir", params)

    async def close(self) -> None:
        if self._token is not None:
            try:
                await self._client.release(self._pool, self._token)
            finally:
                self._token = None
        await self._client.close()


__all__ = ("AsyncFileSystemClient", "Backend", "FileEntry", "StorageError", "SyncFileSystemClient")
