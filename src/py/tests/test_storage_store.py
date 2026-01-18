from __future__ import annotations

import asyncio
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from vertebra.filesystem.client import FileEntry
from vertebra.storage.store import AsyncLocalStore, ByteRange, LocalStore, PutOptions

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator


class _LocalFsStub:
    def read_bytes(self, path: str) -> bytes:
        return Path(path).read_bytes()

    def read_range(self, path: str, offset: int, length: int | None = None) -> bytes:
        data = Path(path).read_bytes()
        if length is None:
            return data[offset:]
        return data[offset : offset + length]

    def write_bytes(
        self, path: str, data: bytes, *, content_type: str | None = None, metadata: dict[str, str] | None = None
    ) -> int:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        Path(path).write_bytes(data)
        return len(data)

    def list(self, path: str) -> list[FileEntry]:
        root = Path(path)
        if not root.exists():
            return []
        entries: list[FileEntry] = []
        for entry in root.iterdir():
            if entry.is_dir():
                entries.append(FileEntry(path=str(entry), is_dir=True))
            else:
                stat = entry.stat()
                entries.append(
                    FileEntry(
                        path=str(entry), is_dir=False, size=stat.st_size, modified=entry.stat().st_mtime.__str__()
                    )
                )
        return entries

    def stat(self, path: str) -> FileEntry:
        entry = Path(path)
        stat = entry.stat()
        return FileEntry(path=str(entry), is_dir=entry.is_dir(), size=stat.st_size)

    def exists(self, path: str) -> bool:
        return Path(path).exists()

    def delete(self, path: str, recursive: bool = False) -> None:
        target = Path(path)
        if target.is_dir():
            for child in target.rglob("*"):
                if child.is_file():
                    child.unlink()
            target.rmdir()
        elif target.exists():
            target.unlink()

    def mkdir(self, path: str) -> None:
        Path(path).mkdir(parents=True, exist_ok=True)

    def rmdir(self, path: str, recursive: bool = False) -> None:
        target = Path(path)
        if recursive and target.exists():
            for child in target.rglob("*"):
                if child.is_file():
                    child.unlink()
            for child in sorted(target.rglob("*"), reverse=True):
                if child.is_dir():
                    child.rmdir()
            target.rmdir()
        elif target.exists():
            target.rmdir()

    def read_file(self, path: str, local_path: str) -> int:
        src = Path(path)
        dst = Path(local_path)
        dst.parent.mkdir(parents=True, exist_ok=True)
        dst.write_bytes(src.read_bytes())
        return dst.stat().st_size

    def write_file(
        self, path: str, local_path: str, *, content_type: str | None = None, metadata: dict[str, str] | None = None
    ) -> int:
        src = Path(local_path)
        dst = Path(path)
        dst.parent.mkdir(parents=True, exist_ok=True)
        dst.write_bytes(src.read_bytes())
        return dst.stat().st_size

    def signed_url(
        self,
        path: str,
        *,
        method: str = "GET",
        expires_in: int = 3600,
        content_type: str | None = None,
        acl: str | None = None,
    ) -> str | None:
        return None


class _AsyncLocalFsStub(_LocalFsStub):
    async def read_bytes(self, path: str) -> bytes:  # type: ignore[override]
        return super().read_bytes(path)

    async def read_range(self, path: str, offset: int, length: int | None = None) -> bytes:  # type: ignore[override]
        return super().read_range(path, offset, length)

    async def write_bytes(  # type: ignore[override]
        self, path: str, data: bytes, *, content_type: str | None = None, metadata: dict[str, str] | None = None
    ) -> int:
        return super().write_bytes(path, data, content_type=content_type, metadata=metadata)

    async def list(self, path: str) -> list[FileEntry]:  # type: ignore[override]
        return super().list(path)

    async def stat(self, path: str) -> FileEntry:  # type: ignore[override]
        return super().stat(path)

    async def exists(self, path: str) -> bool:  # type: ignore[override]
        return super().exists(path)

    async def delete(self, path: str, recursive: bool = False) -> None:  # type: ignore[override]
        super().delete(path, recursive=recursive)

    async def mkdir(self, path: str) -> None:  # type: ignore[override]
        super().mkdir(path)

    async def rmdir(self, path: str, recursive: bool = False) -> None:  # type: ignore[override]
        super().rmdir(path, recursive=recursive)

    async def read_file(self, path: str, local_path: str) -> int:  # type: ignore[override]
        return super().read_file(path, local_path)

    async def write_file(  # type: ignore[override]
        self, path: str, local_path: str, *, content_type: str | None = None, metadata: dict[str, str] | None = None
    ) -> int:
        return super().write_file(path, local_path, content_type=content_type, metadata=metadata)

    async def signed_url(  # type: ignore[override]
        self,
        path: str,
        *,
        method: str = "GET",
        expires_in: int = 3600,
        content_type: str | None = None,
        acl: str | None = None,
    ) -> str | None:
        return None


def _chunk_iter(data: bytes, size: int) -> Iterator[bytes]:
    for i in range(0, len(data), size):
        yield data[i : i + size]


async def _async_chunk_iter(data: bytes, size: int) -> AsyncIterator[bytes]:
    for i in range(0, len(data), size):
        yield data[i : i + size]
        await asyncio.sleep(0)


def test_local_store_put_get_range(tmp_path: Path) -> None:
    store = LocalStore(root=str(tmp_path), client=_LocalFsStub())
    payload = b"abcdef"
    store.put("foo.bin", payload)

    assert store.get("foo.bin") == payload
    assert store.get_range("foo.bin", ByteRange(1, 3)) == b"bcd"
    assert store.get_ranges("foo.bin", [ByteRange(0, 1), ByteRange(2, 2)]) == [b"a", b"cd"]


def test_local_store_iter_chunks(tmp_path: Path) -> None:
    store = LocalStore(root=str(tmp_path), client=_LocalFsStub())
    payload = b"abcdef"
    store.put("chunks.bin", payload)
    chunks = list(store.iter_chunks("chunks.bin", chunk_size=2))
    assert chunks == [b"ab", b"cd", b"ef"]


def test_local_store_put_stream(tmp_path: Path) -> None:
    store = LocalStore(root=str(tmp_path), client=_LocalFsStub())
    payload = b"stream-data"
    result = store.put_stream("stream.bin", _chunk_iter(payload, 3), options=PutOptions(content_type="bin"))
    assert result.bytes == len(payload)
    assert store.get("stream.bin") == payload


def test_local_store_list_with_delimiter(tmp_path: Path) -> None:
    store = LocalStore(root=str(tmp_path), client=_LocalFsStub())
    store.put("dir/a.txt", b"a")
    store.put("dir/sub/b.txt", b"b")
    result = store.list_with_delimiter("dir", "/")
    assert "sub" in result.prefixes
    assert len(result.objects) == 2


def test_local_store_upload_download_path(tmp_path: Path) -> None:
    store = LocalStore(root=str(tmp_path), client=_LocalFsStub())
    source = tmp_path / "source.bin"
    source.write_bytes(b"payload")
    store.upload_from_path(str(source), "remote.bin")

    destination = tmp_path / "dest.bin"
    bytes_written = store.download_to_path("remote.bin", str(destination))
    assert bytes_written == len(b"payload")
    assert destination.read_bytes() == b"payload"


def test_local_store_signed_url(tmp_path: Path) -> None:
    store = LocalStore(root=str(tmp_path), client=_LocalFsStub())
    assert store.signed_url("foo.txt") is None


@pytest.mark.anyio
async def test_async_local_store_put_get(tmp_path: Path) -> None:
    store = AsyncLocalStore(root=str(tmp_path), client=_AsyncLocalFsStub())
    payload = b"async-data"
    await store.put("async.bin", payload)
    assert await store.get("async.bin") == payload


@pytest.mark.anyio
async def test_async_local_store_put_stream(tmp_path: Path) -> None:
    store = AsyncLocalStore(root=str(tmp_path), client=_AsyncLocalFsStub())
    payload = b"async-stream"
    result = await store.put_stream("async-stream.bin", _async_chunk_iter(payload, 4))
    assert result.bytes == len(payload)
    assert await store.get("async-stream.bin") == payload


@pytest.mark.anyio
async def test_async_local_store_upload_download(tmp_path: Path) -> None:
    store = AsyncLocalStore(root=str(tmp_path), client=_AsyncLocalFsStub())
    source = tmp_path / "async-source.bin"
    source.write_bytes(b"async")
    await store.upload_from_path(str(source), "remote.bin")
    destination = tmp_path / "async-dest.bin"
    bytes_written = await store.download_to_path("remote.bin", str(destination))
    assert bytes_written == len(b"async")
    assert destination.read_bytes() == b"async"


@pytest.mark.anyio
async def test_async_local_store_signed_url(tmp_path: Path) -> None:
    store = AsyncLocalStore(root=str(tmp_path), client=_AsyncLocalFsStub())
    assert await store.signed_url("foo.txt") is None
