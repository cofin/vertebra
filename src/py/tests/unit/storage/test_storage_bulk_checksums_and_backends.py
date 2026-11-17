"""Unit tests for vertebra.storage bulk transfers, multipart uploads, checksums, and backend wiring."""

import asyncio
import contextlib
import datetime as dt
import gc
import hashlib
import io
import os
import tempfile
import threading
import weakref
from collections.abc import AsyncIterator, Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, cast
from urllib.parse import parse_qs, urlsplit
from xml.etree import ElementTree

import pyarrow as pa
import pytest
from sqlspec.protocols import ObjectStoreProtocol
from sqlspec.storage.registry import StorageRegistry

import vertebra._storage as native_st
from vertebra.db.arrow import ArrowCursor
from vertebra.extensions.sqlspec.storage import ObjectStoreBackend, register_storage_alias
from vertebra.storage import (
    ArtifactReference,
    AsyncObjectStore,
    LocalConfig,
    MemoryConfig,
    ObjectMeta,
    PutResult,
    S3Config,
    SigningConfig,
    StreamingStorageResponse,
    SyncObjectStore,
)
from vertebra.storage._arrow import (
    write_batches_async,
    write_batches_sync,
)
from vertebra.typing import (
    ImproperConfigurationError,
    IntegrityError,
    VertebraError,
)


@contextlib.contextmanager
def _s3_multipart_completion_server() -> Iterator[tuple[S3Config, dict[int, bytes], list[bytes]]]:
    """Enforce S3's non-final part minimum on actual native HTTP uploads."""
    parts: dict[int, bytes] = {}
    completed: list[bytes] = []

    class CompletionHandler(BaseHTTPRequestHandler):
        def respond(self, status: int, body: bytes) -> None:
            self.send_response(status)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_POST(self) -> None:
            parsed = urlsplit(self.path)
            query = parse_qs(parsed.query, keep_blank_values=True)
            body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
            if parsed.path != "/codec-fixture/output":
                self.send_error(404)
            elif "uploads" in query:
                self.respond(
                    200,
                    b"<InitiateMultipartUploadResult><Bucket>codec-fixture</Bucket><Key>output</Key>"
                    b"<UploadId>fixture</UploadId></InitiateMultipartUploadResult>",
                )
            elif query.get("uploadId") == ["fixture"]:
                numbers = [int(part.findtext("PartNumber", "0")) for part in ElementTree.fromstring(body)]
                if any(len(parts[number]) < 5 * 1024 * 1024 for number in numbers[:-1]):
                    self.respond(
                        400, b"<Error><Code>EntityTooSmall</Code><Message>non-final part below 5 MiB</Message></Error>"
                    )
                    return
                completed.append(b"".join(parts[number] for number in numbers))
                self.respond(
                    200,
                    b"<CompleteMultipartUploadResult><Bucket>codec-fixture</Bucket><Key>output</Key>"
                    b'<ETag>"complete"</ETag></CompleteMultipartUploadResult>',
                )
            else:
                self.send_error(400)

        def do_PUT(self) -> None:
            query = parse_qs(urlsplit(self.path).query)
            number = int(query["partNumber"][0])
            parts[number] = self.rfile.read(int(self.headers["Content-Length"]))
            self.send_response(200)
            self.send_header("ETag", f'"part-{number}"')
            self.send_header("Content-Length", "0")
            self.end_headers()

        def do_DELETE(self) -> None:
            self.send_response(204)
            self.end_headers()

        def log_message(self, format: str, *args: object) -> None:
            _ = (format, args)

    server = ThreadingHTTPServer(("127.0.0.1", 0), CompletionHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield (
            S3Config(
                bucket="codec-fixture",
                region="us-east-1",
                endpoint=f"http://127.0.0.1:{server.server_port}",
                access_key_id="test",
                secret_access_key="test",
                allow_http=True,
            ),
            parts,
            completed,
        )
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


@pytest.mark.anyio
@pytest.mark.parametrize("file_format", ["arrow-ipc", "parquet"])
@pytest.mark.parametrize("async_write", [False, True])
@pytest.mark.parametrize("large", [False, True])
async def test_s3_codec_writers_complete_valid_multipart(file_format: str, async_write: bool, large: bool) -> None:
    """Native multipart coalescing makes real S3 codec output publish successfully."""
    pa = pytest.importorskip("pyarrow")
    table = pa.table({"payload": [b"x" * (9 * 1024 * 1024)]}) if large else pa.table({"id": range(250)})
    with _s3_multipart_completion_server() as (config, parts, completed):
        store: Any = AsyncObjectStore(config) if async_write else SyncObjectStore(config)
        operation = (
            store.write_parquet("output", table, compression="NONE", row_group_size=100)
            if file_format == "parquet"
            else store.write_arrow("output", table, batch_size=100)
        )
        result = await operation if async_write else operation
    assert isinstance(result, PutResult)
    assert result.path == "output"
    assert result.etag is not None
    assert result.etag.strip('"') == "complete"
    assert len(completed) == 1
    assert result.size == len(completed[0])
    sizes = [len(parts[number]) for number in sorted(parts)]
    assert all(size == 8 * 1024 * 1024 for size in sizes[:-1])
    assert 0 < sizes[-1] <= 8 * 1024 * 1024
    assert len(sizes) == (2 if large else 1)
    decoded = (
        pa.parquet.read_table(pa.BufferReader(completed[0]))
        if file_format == "parquet"
        else pa.ipc.open_stream(pa.BufferReader(completed[0])).read_all()
    )
    assert decoded.equals(table)


@pytest.mark.parametrize("scheme", ["memory", "file"])
def test_native_sync_bulk_file_io_conditional_copy_and_compose(tmp_path: Path, scheme: str) -> None:
    """Native SyncObjectStore round-trips files in bulk, enforces conditional copy, and composes objects."""
    url = "memory://bulk" if scheme == "memory" else f"file://{tmp_path / 'root'}"
    store = native_st.SyncObjectStore(url)
    sources: list[tuple[str, str]] = []
    for idx in range(5):
        local = tmp_path / f"up-{idx}.bin"
        local.write_bytes(bytes([idx]) * (idx + 1))
        sources.append((f"bulk/k{idx}.bin", str(local)))
    put_metas = store.put_files(sources, concurrency=2)
    assert [m.path for m in put_metas] == [key for key, _ in sources]
    downloads = [(key, str(tmp_path / "down" / f"d{idx}.bin")) for idx, (key, _) in enumerate(sources)]
    get_metas = store.get_files(downloads, concurrency=3)
    assert [m.size for m in get_metas] == [1, 2, 3, 4, 5]
    assert [Path(dst).read_bytes() for _, dst in downloads] == [bytes([i]) * (i + 1) for i in range(5)]
    single = store.get_file("bulk/k4.bin", str(tmp_path / "single.bin"))
    assert single.size == 5
    assert (tmp_path / "single.bin").read_bytes() == b"\x04" * 5
    with pytest.raises(FileExistsError):
        store.copy("bulk/k0.bin", "bulk/k1.bin", if_not_exists=True)
    assert store.get("bulk/k1.bin") == b"\x01\x01"
    store.copy("bulk/k0.bin", "bulk/k1.bin")
    assert store.get("bulk/k1.bin") == b"\x00"
    composed = store.compose(["bulk/k2.bin", "bulk/k3.bin"], "bulk/merged.bin", content_type="application/octet-stream")
    assert composed.size == 7
    assert store.get("bulk/merged.bin") == b"\x02" * 3 + b"\x03" * 4
    with pytest.raises(ValueError, match="concurrency"):
        store.get_files(downloads, concurrency=0)
    with pytest.raises(ValueError, match="concurrency"):
        store.put_files(sources, concurrency=-1)
    with pytest.raises(ValueError):
        store.compose([], "bulk/empty.bin")


@pytest.mark.anyio
async def test_native_async_bulk_file_io_conditional_copy_and_compose(tmp_path: Path) -> None:
    """Native AsyncObjectStore exposes awaitable bulk file I/O, conditional copy, and compose."""
    store = native_st.AsyncObjectStore("memory://bulk-async")
    sources: list[tuple[str, str]] = []
    for idx in range(3):
        local = tmp_path / f"a-{idx}.bin"
        local.write_bytes(f"part-{idx};".encode())
        sources.append((f"a/k{idx}", str(local)))
    put_metas = await store.put_files(sources, concurrency=2)
    assert [m.path for m in put_metas] == ["a/k0", "a/k1", "a/k2"]
    downloads = [(key, str(tmp_path / "out" / f"o{idx}")) for idx, (key, _) in enumerate(sources)]
    get_metas = await store.get_files(downloads)
    assert [m.size for m in get_metas] == [7, 7, 7]
    meta = await store.get_file("a/k1", str(tmp_path / "one.bin"))
    assert meta.size == 7
    assert (tmp_path / "one.bin").read_bytes() == b"part-1;"
    with pytest.raises(FileExistsError):
        await store.copy("a/k0", "a/k1", if_not_exists=True)
    await store.copy("a/k0", "a/fresh", if_not_exists=True)
    assert await store.get("a/fresh") == b"part-0;"
    composed = await store.compose(["a/k0", "a/k1", "a/k2"], "a/all")
    assert composed.size == 21
    assert await store.get("a/all") == b"part-0;part-1;part-2;"
    with pytest.raises(ValueError, match="concurrency"):
        await store.get_files(downloads, concurrency=0)


def _bulk_store(asynchronous: bool) -> Any:
    """Build a sync or async memory store; every store owns a private in-memory backend."""
    return AsyncObjectStore(MemoryConfig()) if asynchronous else SyncObjectStore(MemoryConfig())


async def _resolve(value: Any) -> Any:
    """Await coroutine results from async stores and pass sync results through."""
    return await value if asyncio.iscoroutine(value) else value


def _write_bulk_sources(root: Path) -> list[Path]:
    """Write five local files with distinct payloads for bulk transfer tests."""
    root.mkdir(parents=True, exist_ok=True)
    paths = []
    for idx in range(5):
        local = root / f"file-{idx}.bin"
        local.write_bytes(f"payload-{idx}-".encode() * (idx + 1))
        paths.append(local)
    return paths


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_upload_many_download_many_and_get_file_roundtrip(tmp_path: Path, asynchronous: bool) -> None:
    """upload_many writes prefixed keys, download_many restores basenames, and get_file streams one object."""
    store = _bulk_store(asynchronous)
    sources = _write_bulk_sources(tmp_path / "src")
    results = await _resolve(store.upload_many(sources, "bulk/run", concurrency=2))
    assert all(isinstance(result, PutResult) for result in results)
    assert [result.path for result in results] == [f"bulk/run/file-{idx}.bin" for idx in range(5)]
    assert [result.size for result in results] == [source.stat().st_size for source in sources]
    assert all(result.etag for result in results)

    keys = [result.path for result in results]
    downloaded = await _resolve(store.download_many(keys, tmp_path / "down", concurrency=3))
    assert downloaded == [tmp_path / "down" / f"file-{idx}.bin" for idx in range(5)]
    assert [path.read_bytes() for path in downloaded] == [source.read_bytes() for source in sources]

    meta = await _resolve(store.get_file("bulk/run/file-3.bin", tmp_path / "single.bin"))
    assert isinstance(meta, ObjectMeta)
    assert meta.path == "bulk/run/file-3.bin"
    assert meta.size == sources[3].stat().st_size
    assert (tmp_path / "single.bin").read_bytes() == sources[3].read_bytes()

    unprefixed = await _resolve(store.upload_many([sources[0]]))
    assert [result.path for result in unprefixed] == ["file-0.bin"]


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("explicit_destination", [False, True])
@pytest.mark.parametrize("fail", [False, True])
async def test_staged_download_removes_staged_files_on_exit(
    tmp_path: Path, asynchronous: bool, explicit_destination: bool, fail: bool
) -> None:
    """staged_download stages every key and deletes exactly the staged files on normal and exceptional exit."""
    store = _bulk_store(asynchronous)
    sources = _write_bulk_sources(tmp_path / "src")
    keys = [result.path for result in await _resolve(store.upload_many(sources, "staged"))]
    destination = tmp_path / "stage" if explicit_destination else None
    if destination is not None:
        destination.mkdir()
        (destination / "keep.txt").write_bytes(b"unrelated")
    staged: list[Path] = []

    async def stage() -> None:
        if asynchronous:
            async with store.staged_download(keys, destination, concurrency=2) as paths:
                staged.extend(paths)
                assert [path.read_bytes() for path in paths] == [source.read_bytes() for source in sources]
                if fail:
                    raise RuntimeError("consumer failed")
        else:
            with store.staged_download(keys, destination, concurrency=2) as paths:
                staged.extend(paths)
                assert [path.read_bytes() for path in paths] == [source.read_bytes() for source in sources]
                if fail:
                    raise RuntimeError("consumer failed")

    if fail:
        with pytest.raises(RuntimeError, match="consumer failed"):
            await stage()
    else:
        await stage()

    assert [path.name for path in staged] == [f"file-{idx}.bin" for idx in range(5)]
    assert not any(path.exists() for path in staged)
    if destination is None:
        assert not staged[0].parent.exists()
    else:
        assert staged[0].parent == destination
        assert sorted(child.name for child in destination.iterdir()) == ["keep.txt"]


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_staged_download_defaults_to_dev_shm_or_tempdir(asynchronous: bool) -> None:
    """Without a destination, staged files land in a unique directory under /dev/shm or the system temp dir."""
    store = _bulk_store(asynchronous)
    await _resolve(store.put("base/one.bin", b"one"))
    shm = Path("/dev/shm")
    expected_base = shm if shm.is_dir() and os.access(shm, os.W_OK) else Path(tempfile.gettempdir())
    if asynchronous:
        async with store.staged_download(["base/one.bin"]) as paths:
            staged = list(paths)
            assert staged[0].read_bytes() == b"one"
    else:
        with store.staged_download(["base/one.bin"]) as paths:
            staged = list(paths)
            assert staged[0].read_bytes() == b"one"
    assert staged[0].parent.parent == expected_base
    assert not staged[0].parent.exists()


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("fail_download", [False, True])
async def test_staged_download_keeps_preexisting_destination_files(
    tmp_path: Path, asynchronous: bool, fail_download: bool
) -> None:
    """staged_download into a caller-owned directory unlinks only files it created, after success or a failed download."""
    store = _bulk_store(asynchronous)
    await _resolve(store.put("staged/fresh.bin", b"fresh"))
    await _resolve(store.put("staged/shared.bin", b"downloaded"))
    destination = tmp_path / "stage"
    destination.mkdir()
    (destination / "shared.bin").write_bytes(b"caller")
    (destination / "ghost.bin").write_bytes(b"caller-ghost")
    keys = ["staged/fresh.bin", "staged/shared.bin", *(["absent/ghost.bin"] if fail_download else [])]

    async def stage() -> None:
        if asynchronous:
            async with store.staged_download(keys, destination) as paths:
                assert [path.read_bytes() for path in paths] == [b"fresh", b"downloaded"]
        else:
            with store.staged_download(keys, destination) as paths:
                assert [path.read_bytes() for path in paths] == [b"fresh", b"downloaded"]

    if fail_download:
        with pytest.raises(FileNotFoundError):
            await stage()
    else:
        await stage()
        assert (destination / "shared.bin").read_bytes() == b"downloaded"

    assert sorted(child.name for child in destination.iterdir()) == ["ghost.bin", "shared.bin"]
    assert (destination / "ghost.bin").read_bytes() == b"caller-ghost"


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("alias", ["a//b", "./a/./b", "/a//./b", " a/b "])
async def test_object_keys_collapse_empty_and_dot_segments(asynchronous: bool, alias: str) -> None:
    """Empty and `.` segments collapse before native validation, so every alias addresses the key `a/b`."""
    store = _bulk_store(asynchronous)
    put = await _resolve(store.put(alias, b"payload"))
    assert put.path == "a/b"
    assert await _resolve(store.get("a/b")) == b"payload"
    assert await _resolve(store.get(alias)) == b"payload"
    assert [meta.path for meta in await _resolve(store.list("a"))] == ["a/b"]


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("key", ["../a", "a/../b", "a//../b", "./a/.."])
async def test_object_keys_reject_parent_segments(asynchronous: bool, key: str) -> None:
    """A `..` segment is rejected even when surrounded by empty or `.` segments, and nothing is written."""
    store = _bulk_store(asynchronous)
    with pytest.raises(ValueError, match="parent segment"):
        await _resolve(store.put(key, b"payload"))
    assert await _resolve(store.list()) == []


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_bulk_transfers_reject_duplicates_before_io_and_accept_empty(tmp_path: Path, asynchronous: bool) -> None:
    """Duplicate basenames or keys raise ValueError before any transfer, and empty inputs return []."""
    store = _bulk_store(asynchronous)
    await _resolve(store.put("a/x.bin", b"a"))
    await _resolve(store.put("b/x.bin", b"b"))
    destination = tmp_path / "dupes"
    with pytest.raises(ValueError, match="x.bin"):
        await _resolve(store.download_many(["a/x.bin", "b/x.bin"], destination))
    assert not destination.exists()

    (tmp_path / "one").mkdir()
    (tmp_path / "two").mkdir()
    (tmp_path / "one" / "same.bin").write_bytes(b"1")
    (tmp_path / "two" / "same.bin").write_bytes(b"2")
    with pytest.raises(ValueError, match="same.bin"):
        await _resolve(store.upload_many([tmp_path / "one" / "same.bin", tmp_path / "two" / "same.bin"], "up"))
    assert await _resolve(store.list("up")) == []

    if asynchronous:
        with pytest.raises(ValueError, match="x.bin"):
            async with store.staged_download(["a/x.bin", "b/x.bin"], destination):
                pytest.fail("duplicate staged keys must not enter the context")
    else:
        with pytest.raises(ValueError, match="x.bin"), store.staged_download(["a/x.bin", "b/x.bin"], destination):
            pytest.fail("duplicate staged keys must not enter the context")
    assert not destination.exists()

    assert await _resolve(store.download_many([], destination)) == []
    assert await _resolve(store.upload_many([])) == []
    if asynchronous:
        async with store.staged_download([]) as paths:
            assert paths == []
    else:
        with store.staged_download([]) as paths:
            assert paths == []


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_copy_overwrite_false_raises_file_exists(asynchronous: bool) -> None:
    """copy(..., overwrite=False) refuses to replace an existing destination and keeps its payload."""
    store = _bulk_store(asynchronous)
    await _resolve(store.put("a", b"1"))
    await _resolve(store.put("b", b"2"))
    with pytest.raises(FileExistsError):
        await _resolve(store.copy("a", "b", overwrite=False))
    assert await _resolve(store.get("b")) == b"2"
    await _resolve(store.copy("a", "c", overwrite=False))
    assert await _resolve(store.get("c")) == b"1"
    await _resolve(store.copy("a", "b"))
    assert await _resolve(store.get("b")) == b"1"


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize(
    ("pattern", "expected"),
    [
        ("runs/2026/*.parquet", ["runs/2026/part-00.parquet", "runs/2026/part-01.parquet"]),
        ("runs/2026/part-0[1].parquet", ["runs/2026/part-01.parquet"]),
        ("runs/202?/notes.*", ["runs/2026/notes.txt"]),
        ("runs/2026/notes.txt", ["runs/2026/notes.txt"]),
        ("runs/2026/missing.txt", []),
        ("runs/2027/*.parquet", []),
    ],
)
async def test_glob_wildcards_literal_fast_path_and_missing(
    asynchronous: bool, pattern: str, expected: list[str]
) -> None:
    """glob filters listed keys with fnmatch, uses head for literal keys, and returns [] when nothing matches."""
    store = _bulk_store(asynchronous)
    for key in ("runs/2026/part-00.parquet", "runs/2026/part-01.parquet", "runs/2026/notes.txt", "runs/20x.txt"):
        await _resolve(store.put(key, key.encode()))
    matched = await _resolve(store.glob(pattern))
    assert all(isinstance(meta, ObjectMeta) for meta in matched)
    assert [meta.path for meta in matched] == expected
    assert [meta.size for meta in matched] == [len(path) for path in expected]


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_glob_strips_store_uri_prefix(tmp_path: Path, asynchronous: bool) -> None:
    """glob accepts patterns prefixed with the store URL and resolves them relative to the store root."""
    config = LocalConfig(root=tmp_path / f"glob-root-{asynchronous}")
    store: Any = AsyncObjectStore(config) if asynchronous else SyncObjectStore(config)
    await _resolve(store.put("logs/a.json", b"{}"))
    await _resolve(store.put("logs/b.txt", b"b"))
    matched = await _resolve(store.glob(f"{store.url}/logs/*.json"))
    assert [meta.path for meta in matched] == ["logs/a.json"]
    literal = await _resolve(store.glob(f"{store.url}/logs/b.txt"))
    assert [meta.path for meta in literal] == ["logs/b.txt"]


class _PlaceholderListingNative:
    """Delegate to a real native store while injecting a trailing-`/` directory placeholder into listings."""

    def __init__(self, native: Any, asynchronous: bool, placeholder: str) -> None:
        self.native = native
        self.asynchronous = asynchronous
        self.placeholder = ObjectMeta(path=placeholder, size=0)

    def __getattr__(self, name: str) -> Any:
        return getattr(self.native, name)

    def list(self, prefix: str | None) -> Any:
        listed = self.native.list(prefix)
        if self.asynchronous:

            async def resolve() -> Any:
                return [*(await listed), self.placeholder]

            return resolve()
        return [*listed, self.placeholder]


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_glob_skips_trailing_slash_directory_placeholders(asynchronous: bool) -> None:
    """glob never returns keys ending in `/` even when the pattern matches them."""
    base = SyncObjectStore(MemoryConfig())
    base.put("runs/2026/part-00.parquet", b"p")
    paired = base.to_async() if asynchronous else base
    native = _PlaceholderListingNative(paired._require_native(), asynchronous, "runs/2026/")
    store: Any = (
        AsyncObjectStore(MemoryConfig(), _native_instance=native)
        if asynchronous
        else SyncObjectStore(MemoryConfig(), _native_instance=native)
    )
    matched = await _resolve(store.glob("runs/*"))
    assert [meta.path for meta in matched] == ["runs/2026/part-00.parquet"]


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_compose_concatenates_sources_and_rejects_empty(asynchronous: bool) -> None:
    """compose writes the concatenation of its sources and rejects an empty source list before I/O."""
    store = _bulk_store(asynchronous)
    for idx, payload in enumerate((b"alpha-", b"beta-", b"gamma")):
        await _resolve(store.put(f"p{idx + 1}", payload))
    result = await _resolve(store.compose(["p1", "p2", "p3"], "merged.bin", content_type="application/octet-stream"))
    assert isinstance(result, PutResult)
    assert result.path == "merged.bin"
    assert result.size == len(b"alpha-beta-gamma")
    assert await _resolve(store.get("merged.bin")) == b"alpha-beta-gamma"
    with pytest.raises(ValueError, match="source"):
        await _resolve(store.compose([], "empty.bin"))
    assert not await _resolve(store.exists("empty.bin"))


_SELECTIVE_PAYLOAD = b"selective-expected-digest-payload-" * 128
_SELECTIVE_CRC32C = "14bf0fea"
_S3_MULTIPART_PAYLOAD = b"s3-multipart-block!!" * (450 * 1024)
_S3_MULTIPART_CRC32C = "744215fd"


def _sha256_and_md5(data: bytes) -> tuple[str, str]:
    """Return lowercase hexadecimal (sha256, md5) digests for `data`."""
    return hashlib.sha256(data).hexdigest(), hashlib.md5(data).hexdigest()


def _split_chunks(payload: bytes) -> list[bytes]:
    """Split `payload` into three slices for multipart and streaming upload helpers."""
    if not payload:
        return []
    one = len(payload) // 3
    two = (2 * len(payload)) // 3
    return [part for part in (payload[:one], payload[one:two], payload[two:]) if part]


async def _async_chunk_iter(chunks: list[bytes]) -> AsyncIterator[bytes]:
    """Yield `chunks` asynchronously."""
    for chunk in chunks:
        yield chunk


async def _write_with_method(
    store: Any,
    write_method: str,
    key: str,
    payload: bytes,
    tmp_path: Path,
    *,
    asynchronous: bool,
    part_size: int = 4096,
    **kwargs: Any,
) -> tuple[PutResult, Any]:
    """Invoke `write_method` on `store` and return `(put_result, writer_or_none)`."""
    if write_method == "put":
        return await _resolve(store.put(key, payload, **kwargs)), None
    if write_method == "put_stream":
        chunks = _split_chunks(payload)
        stream_input: Any = _async_chunk_iter(chunks) if asynchronous else iter(chunks)
        return await _resolve(store.put_stream(key, stream_input, **kwargs)), None
    if write_method == "put_multipart":
        parts: list[Any] = [memoryview(chunk) for chunk in _split_chunks(payload)]
        return await _resolve(store.put_multipart(key, parts, **kwargs)), None
    if write_method == "put_file":
        local_src = tmp_path / f"src-{key.replace('/', '_')}.bin"
        local_src.parent.mkdir(parents=True, exist_ok=True)
        local_src.write_bytes(payload)
        return await _resolve(store.put_file(key, local_src, **kwargs)), None
    writer = store.open_writer(key, part_size=part_size, **kwargs)
    slices = _split_chunks(payload)
    if asynchronous:
        async with writer:
            for piece in slices:
                await writer.write(piece)
    else:
        with writer:
            for piece in slices:
                writer.write(piece)
    assert isinstance(writer.result, PutResult)
    return writer.result, writer


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("write_method", ["put", "put_stream", "put_multipart", "put_file", "open_writer"])
async def test_write_default_checksum_false_returns_none_receipts(
    tmp_path: Path,
    asynchronous: bool,
    write_method: str,
) -> None:
    """Default checksum=False writes return None receipts and never promote user metadata to receipts."""
    store = _bulk_store(asynchronous)
    payload = b"default-checksum-disabled-payload"
    extra_kwargs: dict[str, Any] = (
        {"attributes": {"sha256": "0" * 64, "crc32c": "00000000", "md5": "0" * 32}}
        if write_method in {"put", "put_stream", "open_writer"}
        else {}
    )
    result, _ = await _write_with_method(
        store,
        write_method,
        "items/default.bin",
        payload,
        tmp_path,
        asynchronous=asynchronous,
        **extra_kwargs,
    )
    assert isinstance(result, PutResult)
    assert result.sha256 is None
    assert result.crc32c is None
    assert result.md5 is None
    head_meta = await _resolve(store.head("items/default.bin"))
    assert (head_meta.sha256, head_meta.crc32c, head_meta.md5) == (None, None, None)
    assert await _resolve(store.get("items/default.bin")) == payload


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("write_method", ["put", "put_stream", "put_multipart", "put_file", "open_writer"])
@pytest.mark.parametrize(
    ("payload", "expected_crc32c"),
    [
        pytest.param(b"", "00000000", id="empty"),
        pytest.param(b"123456789", "e3069283", id="rfc3720"),
        pytest.param(b"vertebra-integrity-chunk-" * 1024, "5fcfbeb2", id="multi-chunk"),
    ],
)
async def test_write_checksum_true_populates_sha256_crc32c_and_md5_receipts(
    tmp_path: Path,
    asynchronous: bool,
    write_method: str,
    payload: bytes,
    expected_crc32c: str,
) -> None:
    """checksum=True populates SHA-256, Castagnoli CRC32C, and MD5 receipts across sync/async write paths."""
    store = _bulk_store(asynchronous)
    expected_sha256, expected_md5 = _sha256_and_md5(payload)

    result, _ = await _write_with_method(
        store,
        write_method,
        "items/checked.bin",
        payload,
        tmp_path,
        asynchronous=asynchronous,
        part_size=4096,
        checksum=True,
    )
    assert isinstance(result, PutResult)
    assert result.size == len(payload)
    assert result.sha256 == expected_sha256
    assert result.crc32c == expected_crc32c
    assert result.md5 == expected_md5
    assert await _resolve(store.get("items/checked.bin")) == payload


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("write_method", ["put", "put_stream", "put_multipart", "put_file", "open_writer"])
@pytest.mark.parametrize("digest_field", ["expected_sha256", "expected_crc32c", "expected_md5"])
async def test_write_expected_digests_pass_and_selective_receipt_population(
    tmp_path: Path,
    asynchronous: bool,
    write_method: str,
    digest_field: str,
) -> None:
    """Matching expected_* digests succeed and populate only requested receipts when checksum=False."""
    store = _bulk_store(asynchronous)
    payload = _SELECTIVE_PAYLOAD
    exp_sha, exp_md5 = _sha256_and_md5(payload)
    digests = {"sha256": exp_sha, "crc32c": _SELECTIVE_CRC32C, "md5": exp_md5}
    attr_name = digest_field.removeprefix("expected_")

    result, _ = await _write_with_method(
        store,
        write_method,
        "items/selective.bin",
        payload,
        tmp_path,
        asynchronous=asynchronous,
        part_size=1024,
        **{digest_field: digests[attr_name]},
    )
    assert getattr(result, attr_name) == digests[attr_name]
    assert (result.sha256 is not None) == (attr_name == "sha256")
    assert (result.crc32c is not None) == (attr_name == "crc32c")
    assert (result.md5 is not None) == (attr_name == "md5")

    all_three, _ = await _write_with_method(
        store,
        write_method,
        "items/all-three.bin",
        payload,
        tmp_path,
        asynchronous=asynchronous,
        part_size=1024,
        checksum=True,
        expected_sha256=digests["sha256"],
        expected_crc32c=digests["crc32c"],
        expected_md5=digests["md5"],
    )
    assert all_three.sha256 == digests["sha256"]
    assert all_three.crc32c == digests["crc32c"]
    assert all_three.md5 == digests["md5"]


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("write_method", ["put", "put_stream", "put_multipart", "put_file", "open_writer"])
@pytest.mark.parametrize(
    ("digest_field", "wrong_hex"),
    [
        ("expected_sha256", "0" * 64),
        ("expected_crc32c", "ffffffff"),
        ("expected_md5", "0" * 32),
    ],
)
async def test_write_expected_digest_mismatch_raises_integrity_error_and_preserves_prior_bytes(
    tmp_path: Path,
    asynchronous: bool,
    write_method: str,
    digest_field: str,
    wrong_hex: str,
) -> None:
    """Mismatched expected_* digests raise IntegrityError before publish and preserve existing object bytes."""
    store = _bulk_store(asynchronous)
    await _resolve(store.put("items/guarded.bin", b"prior-intact-bytes"))

    if write_method == "open_writer":
        writer = store.open_writer("items/guarded.bin", part_size=4, **{digest_field: wrong_hex})
        with pytest.raises(IntegrityError) as exc_info:
            if asynchronous:
                async with writer:
                    await writer.write(b"corrupted-replacement")
            else:
                with writer:
                    writer.write(b"corrupted-replacement")
        assert writer.closed
        assert writer.result is None
    else:
        bad_kw: dict[str, Any] = {digest_field: wrong_hex}
        with pytest.raises(IntegrityError) as exc_info:
            await _write_with_method(
                store,
                write_method,
                "items/guarded.bin",
                b"corrupted-replacement",
                tmp_path,
                asynchronous=asynchronous,
                **bad_kw,
            )

    assert type(exc_info.value) is IntegrityError
    assert isinstance(exc_info.value, VertebraError)
    assert isinstance(exc_info.value, OSError)
    assert await _resolve(store.get("items/guarded.bin")) == b"prior-intact-bytes"

    with pytest.raises(IntegrityError) as fresh_exc:
        await _write_with_method(
            store,
            write_method,
            "items/never-committed.bin",
            b"corrupted-fresh",
            tmp_path,
            asynchronous=asynchronous,
            part_size=4,
            **{digest_field: wrong_hex},
        )
    assert type(fresh_exc.value) is IntegrityError
    assert not await _resolve(store.exists("items/never-committed.bin"))


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("write_method", ["put_stream", "put_file", "open_writer"])
async def test_s3_multipart_write_receipts_and_mismatch_abort(
    tmp_path: Path,
    asynchronous: bool,
    write_method: str,
) -> None:
    """Multi-part (>8 MiB) S3 uploads compute exact receipts and abort before completion on mismatch."""
    payload = _S3_MULTIPART_PAYLOAD
    assert len(payload) > 8 * 1024 * 1024
    exp_sha, exp_md5 = _sha256_and_md5(payload)
    digests = {"sha256": exp_sha, "crc32c": _S3_MULTIPART_CRC32C, "md5": exp_md5}

    with _s3_multipart_completion_server() as (config, parts, completed):
        store: Any = AsyncObjectStore(config) if asynchronous else SyncObjectStore(config)
        with pytest.raises(IntegrityError) as exc_info:
            await _write_with_method(
                store,
                write_method,
                "output",
                payload,
                tmp_path,
                asynchronous=asynchronous,
                part_size=8_388_608,
                expected_sha256="0" * 64,
            )
        assert type(exc_info.value) is IntegrityError
        assert completed == []
        assert len(parts) >= 1

    with _s3_multipart_completion_server() as (config, parts, completed):
        store = AsyncObjectStore(config) if asynchronous else SyncObjectStore(config)
        result, _ = await _write_with_method(
            store,
            write_method,
            "output",
            payload,
            tmp_path,
            asynchronous=asynchronous,
            part_size=8_388_608,
            checksum=True,
            expected_sha256=digests["sha256"],
            expected_crc32c=digests["crc32c"],
            expected_md5=digests["md5"],
        )
        assert len(completed) == 1
        assert completed[0] == payload
        assert len(parts) == 2
        assert result.sha256 == digests["sha256"]
        assert result.crc32c == digests["crc32c"]
        assert result.md5 == digests["md5"]


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_verified_get_and_get_file_pass_and_mismatch_preserves_destination(
    tmp_path: Path,
    asynchronous: bool,
) -> None:
    """Verified get and get_file enforce expected_sha256 and preserve local destinations on mismatch."""
    store = _bulk_store(asynchronous)
    payload = b"verified-download-payload-" * 512
    good_sha = hashlib.sha256(payload).hexdigest()
    bad_sha = "a" * 64
    await _resolve(store.put("downloads/blob.bin", payload))

    fetched = await _resolve(store.get("downloads/blob.bin", expected_sha256=good_sha))
    assert fetched == payload

    with pytest.raises(IntegrityError) as get_exc:
        await _resolve(store.get("downloads/blob.bin", expected_sha256=bad_sha))
    assert type(get_exc.value) is IntegrityError

    good_dst = tmp_path / "good.bin"
    meta = await _resolve(store.get_file("downloads/blob.bin", good_dst, expected_sha256=good_sha))
    assert isinstance(meta, ObjectMeta)
    assert meta.sha256 == good_sha
    assert good_dst.read_bytes() == payload

    existing_dst = tmp_path / "existing.bin"
    existing_dst.write_bytes(b"caller-existing-file")
    with pytest.raises(IntegrityError) as existing_exc:
        await _resolve(store.get_file("downloads/blob.bin", existing_dst, expected_sha256=bad_sha))
    assert type(existing_exc.value) is IntegrityError
    assert existing_dst.read_bytes() == b"caller-existing-file"
    assert not [item for item in tmp_path.iterdir() if item.name.endswith(".part")]

    fresh_dst = tmp_path / "fresh-absent.bin"
    with pytest.raises(IntegrityError) as fresh_exc:
        await _resolve(store.get_file("downloads/blob.bin", fresh_dst, expected_sha256=bad_sha))
    assert type(fresh_exc.value) is IntegrityError
    assert not fresh_dst.exists()
    assert not [item for item in tmp_path.iterdir() if item.name.endswith(".part")]


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_verified_get_stream_and_open_reader_pass_mismatch_and_early_close(
    asynchronous: bool,
) -> None:
    """Verified get_stream and open_reader verify SHA-256 on EOF and allow early close without raising."""
    store = _bulk_store(asynchronous)
    payload = b"0123456789abcdef" * 2048
    good_sha = hashlib.sha256(payload).hexdigest()
    bad_sha = "b" * 64
    await _resolve(store.put("streams/blob.bin", payload))

    if asynchronous:
        stream_chunks = [
            bytes(buf) async for buf in store.get_stream("streams/blob.bin", chunk_size=4096, expected_sha256=good_sha)
        ]
        assert b"".join(stream_chunks) == payload

        with pytest.raises(IntegrityError) as stream_exc:
            _ = [
                bytes(buf)
                async for buf in store.get_stream("streams/blob.bin", chunk_size=4096, expected_sha256=bad_sha)
            ]
        assert type(stream_exc.value) is IntegrityError

        early_agen = store.get_stream("streams/blob.bin", chunk_size=4096, expected_sha256=bad_sha)
        first = bytes(await anext(early_agen))
        assert first == payload[:4096]
        await early_agen.aclose()

        async with store.open_reader("streams/blob.bin", chunk_size=4096, expected_sha256=good_sha) as reader:
            assert await reader.read() == payload
            assert reader.seek(0) == 0
            head_part = await reader.read(5000)
            assert reader.seek(0, io.SEEK_CUR) == 5000
            with pytest.raises(io.UnsupportedOperation):
                reader.seek(4)
            with pytest.raises(io.UnsupportedOperation):
                await reader.read_at(0, 4)
            tail_part = await reader.read()
            assert head_part + tail_part == payload
            assert reader.seek(0) == 0
            ait = aiter(reader)
            first_iter = await anext(ait)
            rest_read = await reader.read()
            assert first_iter + rest_read == payload
            assert reader.seek(0) == 0
            iter_chunks = [chunk async for chunk in reader]
            assert b"".join(iter_chunks) == payload

        await store.put("streams/empty.bin", b"")
        empty_sha = hashlib.sha256(b"").hexdigest()
        async with store.open_reader("streams/empty.bin", expected_sha256=empty_sha) as empty_reader:
            assert await empty_reader.read() == b""
            assert empty_reader.seek(0) == 0
            assert await empty_reader.read() == b""

        with pytest.raises(IntegrityError) as read_exc:
            async with store.open_reader("streams/blob.bin", chunk_size=4096, expected_sha256=bad_sha) as bad_reader:
                _ = await bad_reader.read()
        assert type(read_exc.value) is IntegrityError

        with pytest.raises(IntegrityError) as iter_exc:
            async with store.open_reader(
                "streams/blob.bin", chunk_size=4096, expected_sha256=bad_sha
            ) as bad_iter_reader:
                _ = [chunk async for chunk in bad_iter_reader]
        assert type(iter_exc.value) is IntegrityError

        async with store.open_reader("streams/blob.bin", chunk_size=4096, expected_sha256=bad_sha) as early_reader:
            prefix = await early_reader.read(16)
            assert prefix == payload[:16]

        sync_closed_reader = store.open_reader("streams/blob.bin", chunk_size=4096, expected_sha256=bad_sha)
        assert await sync_closed_reader.read(16) == payload[:16]
        sync_closed_reader.close()
        assert sync_closed_reader.closed
    else:
        stream_chunks = [
            bytes(buf) for buf in store.get_stream("streams/blob.bin", chunk_size=4096, expected_sha256=good_sha)
        ]
        assert b"".join(stream_chunks) == payload

        with pytest.raises(IntegrityError) as stream_exc:
            _ = [bytes(buf) for buf in store.get_stream("streams/blob.bin", chunk_size=4096, expected_sha256=bad_sha)]
        assert type(stream_exc.value) is IntegrityError

        early_gen = store.get_stream("streams/blob.bin", chunk_size=4096, expected_sha256=bad_sha)
        first = bytes(next(early_gen))
        assert first == payload[:4096]
        early_gen.close()

        with store.open_reader("streams/blob.bin", chunk_size=4096, expected_sha256=good_sha) as reader:
            assert reader.read() == payload
            assert reader.seek(0) == 0
            head_part = reader.read(5000)
            assert reader.seek(0, io.SEEK_CUR) == 5000
            with pytest.raises(io.UnsupportedOperation):
                reader.seek(4)
            with pytest.raises(io.UnsupportedOperation):
                reader.read_at(0, 4)
            tail_part = reader.read()
            assert head_part + tail_part == payload
            assert reader.seek(0) == 0
            it = iter(reader)
            first_iter = next(it)
            rest_read = reader.read()
            assert first_iter + rest_read == payload
            assert reader.seek(0) == 0
            assert b"".join(reader) == payload

        store.put("streams/empty.bin", b"")
        empty_sha = hashlib.sha256(b"").hexdigest()
        with store.open_reader("streams/empty.bin", expected_sha256=empty_sha) as empty_reader:
            assert empty_reader.read() == b""
            assert empty_reader.seek(0) == 0
            assert empty_reader.read() == b""

        with (
            pytest.raises(IntegrityError) as read_exc,
            store.open_reader("streams/blob.bin", chunk_size=4096, expected_sha256=bad_sha) as bad_reader,
        ):
            _ = bad_reader.read()
        assert type(read_exc.value) is IntegrityError

        with (
            pytest.raises(IntegrityError) as iter_exc,
            store.open_reader("streams/blob.bin", chunk_size=4096, expected_sha256=bad_sha) as bad_iter_reader,
        ):
            _ = list(bad_iter_reader)
        assert type(iter_exc.value) is IntegrityError

        with store.open_reader("streams/blob.bin", chunk_size=4096, expected_sha256=bad_sha) as early_reader:
            prefix = early_reader.read(16)
            assert prefix == payload[:16]


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("chunked_reads", [False, True], ids=["exact-size", "split-exact-size"])
async def test_verified_open_reader_exact_length_reads_enforce_sha256(
    asynchronous: bool,
    chunked_reads: bool,
) -> None:
    """Exact-length sized reads reaching object EOF enforce expected_sha256 before returning bytes."""
    store = _bulk_store(asynchronous)
    payload = b"exact-size-verified-reader-" * 300
    good_sha = hashlib.sha256(payload).hexdigest()
    bad_sha = "c" * 64
    total = len(payload)
    half = total // 2
    await _resolve(store.put("streams/exact.bin", payload))

    if asynchronous:
        async with store.open_reader(
            "streams/exact.bin", size=total, chunk_size=2048, expected_sha256=good_sha
        ) as ok_reader:
            if chunked_reads:
                data = await ok_reader.read(half) + await ok_reader.read(total - half)
            else:
                data = await ok_reader.read(total)
            assert data == payload
            assert await ok_reader.read(16) == b""

        async with store.open_reader(
            "streams/exact.bin", size=total, chunk_size=2048, expected_sha256=bad_sha
        ) as bad_reader:
            if chunked_reads:
                assert await bad_reader.read(half) == payload[:half]
                with pytest.raises(IntegrityError):
                    await bad_reader.read(total - half)
            else:
                with pytest.raises(IntegrityError):
                    await bad_reader.read(total)
    else:
        with store.open_reader("streams/exact.bin", chunk_size=2048, expected_sha256=good_sha) as ok_reader:
            data = ok_reader.read(half) + ok_reader.read(total - half) if chunked_reads else ok_reader.read(total)
            assert data == payload
            assert ok_reader.read(16) == b""

        with store.open_reader("streams/exact.bin", chunk_size=2048, expected_sha256=bad_sha) as bad_reader:
            if chunked_reads:
                assert bad_reader.read(half) == payload[:half]
                with pytest.raises(IntegrityError):
                    bad_reader.read(total - half)
            else:
                with pytest.raises(IntegrityError):
                    bad_reader.read(total)


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize(
    ("operation", "bad_kwargs"),
    [
        ("put", {"expected_sha256": "A" * 64}),
        ("put", {"expected_sha256": "abcd"}),
        ("put", {"expected_sha256": f"{'0' * 63}g"}),
        ("put", {"expected_sha256": ""}),
        ("put", {"expected_crc32c": "E3069283"}),
        ("put", {"expected_crc32c": "e30692"}),
        ("put", {"expected_crc32c": "zzzzzzzz"}),
        ("put", {"expected_md5": "D" * 32}),
        ("put", {"expected_md5": "d41d8c"}),
        ("put", {"expected_md5": f"{'0' * 31}z"}),
        ("put_stream", {"expected_sha256": "A" * 64}),
        ("put_stream", {"expected_crc32c": "E3069283"}),
        ("put_stream", {"expected_md5": "D" * 32}),
        ("put_multipart", {"expected_sha256": "short"}),
        ("put_multipart", {"expected_crc32c": "short"}),
        ("put_multipart", {"expected_md5": "short"}),
        ("put_file", {"expected_sha256": "A" * 64}),
        ("put_file", {"expected_crc32c": "E3069283"}),
        ("put_file", {"expected_md5": "D" * 32}),
        ("open_writer", {"expected_sha256": "A" * 64}),
        ("open_writer", {"expected_crc32c": "E3069283"}),
        ("open_writer", {"expected_md5": "D" * 32}),
        ("get", {"expected_sha256": "A" * 64}),
        ("get", {"expected_sha256": "short"}),
        ("get_file", {"expected_sha256": "A" * 64}),
        ("get_stream", {"expected_sha256": "A" * 64}),
        ("open_reader", {"expected_sha256": "A" * 64}),
        ("open_reader", {"expected_sha256": ""}),
    ],
)
async def test_malformed_expected_digest_raises_value_error_before_io(
    tmp_path: Path,
    asynchronous: bool,
    operation: str,
    bad_kwargs: dict[str, str],
) -> None:
    """Malformed expected digest hex strings raise ValueError prior to filesystem or backend I/O."""
    store = _bulk_store(asynchronous)
    with pytest.raises(ValueError, match="lowercase hexadecimal"):
        if operation == "put":
            await _resolve(store.put("bad/key.bin", b"x", **bad_kwargs))
        elif operation == "put_stream":
            await _resolve(store.put_stream("bad/key.bin", [b"x"], **bad_kwargs))
        elif operation == "put_multipart":
            await _resolve(store.put_multipart("bad/key.bin", [b"x"], **bad_kwargs))
        elif operation == "put_file":
            await _resolve(store.put_file("bad/key.bin", tmp_path / "does-not-exist.bin", **bad_kwargs))
        elif operation == "open_writer":
            store.open_writer("bad/key.bin", **bad_kwargs)
        elif operation == "get":
            await _resolve(store.get("bad/missing.bin", **bad_kwargs))
        elif operation == "get_file":
            await _resolve(store.get_file("bad/missing.bin", tmp_path / "out.bin", **bad_kwargs))
        elif operation == "get_stream":
            if asynchronous:
                async for _ in store.get_stream("bad/missing.bin", **bad_kwargs):
                    pass
            else:
                for _ in store.get_stream("bad/missing.bin", **bad_kwargs):
                    pass
        elif operation == "open_reader":
            store.open_reader("bad/missing.bin", **bad_kwargs)


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("file_format", ["parquet", "arrow-ipc"])
async def test_write_batches_checksum_and_expected_digests(asynchronous: bool, file_format: str) -> None:
    """write_batches_sync and write_batches_async compute checksums and verify expected digests."""
    pa = pytest.importorskip("pyarrow")
    store = _bulk_store(asynchronous)
    table = pa.table({"id": [1, 2, 3, 4], "name": ["alpha", "beta", "gamma", "delta"]})
    key = f"arrow/integrity-{file_format}.bin"

    if asynchronous:
        res = await write_batches_async(
            cast(AsyncObjectStore, store), key, table, file_format=file_format, checksum=True
        )
    else:
        res = write_batches_sync(cast(SyncObjectStore, store), key, table, file_format=file_format, checksum=True)

    stored = bytes(await _resolve(store.get(key)))
    exp_sha, exp_md5 = _sha256_and_md5(stored)
    assert res.sha256 == exp_sha
    assert res.md5 == exp_md5
    assert res.crc32c is not None and len(res.crc32c) == 8
    exp_crc = res.crc32c
    assert (
        await _resolve(store.put(f"arrow/crc-{file_format}.bin", stored, expected_crc32c=exp_crc))
    ).crc32c == exp_crc

    verified_key = f"arrow/verified-{file_format}.bin"
    if asynchronous:
        verified = await write_batches_async(
            cast(AsyncObjectStore, store),
            verified_key,
            table,
            file_format=file_format,
            expected_sha256=exp_sha,
            expected_crc32c=exp_crc,
            expected_md5=exp_md5,
        )
    else:
        verified = write_batches_sync(
            cast(SyncObjectStore, store),
            verified_key,
            table,
            file_format=file_format,
            expected_sha256=exp_sha,
            expected_crc32c=exp_crc,
            expected_md5=exp_md5,
        )
    assert (verified.sha256, verified.crc32c, verified.md5) == (exp_sha, exp_crc, exp_md5)


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize(
    "bad_digest",
    [
        {"expected_sha256": "0" * 64},
        {"expected_crc32c": "00000000"},
        {"expected_md5": "0" * 32},
    ],
)
async def test_write_batches_expected_digest_mismatch_preserves_prior_object(
    asynchronous: bool, bad_digest: dict[str, str]
) -> None:
    """Mismatched expected digests abort write_batches uploads without clobbering existing bytes."""
    pa = pytest.importorskip("pyarrow")
    store = _bulk_store(asynchronous)
    key = "arrow/preserved.parquet"
    sentinel = b"prior-arrow-bytes"
    await _resolve(store.put(key, sentinel))
    table = pa.table({"id": [1, 2], "label": ["x", "y"]})

    bad_digest_kw: dict[str, Any] = dict(bad_digest)
    with pytest.raises(IntegrityError):
        if asynchronous:
            await write_batches_async(cast(AsyncObjectStore, store), key, table, file_format="parquet", **bad_digest_kw)
        else:
            write_batches_sync(cast(SyncObjectStore, store), key, table, file_format="parquet", **bad_digest_kw)

    assert bytes(await _resolve(store.get(key))) == sentinel


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_write_batches_midstream_reader_failure_aborts_upload(asynchronous: bool) -> None:
    """Mid-stream RecordBatchReader errors abort multipart upload and preserve prior bytes."""
    pa = pytest.importorskip("pyarrow")
    store = _bulk_store(asynchronous)
    key = "arrow/midstream-abort.parquet"
    sentinel = b"prior-valid-object"
    await _resolve(store.put(key, sentinel))

    schema = pa.schema([("id", pa.int64())])

    def failing_batches() -> Iterator[Any]:
        yield pa.record_batch([pa.array([1, 2, 3])], schema=schema)
        raise RuntimeError("midstream reader failure")

    reader = pa.RecordBatchReader.from_batches(schema, failing_batches())
    with pytest.raises(RuntimeError, match="midstream reader failure"):
        if asynchronous:
            await write_batches_async(cast(AsyncObjectStore, store), key, reader, file_format="parquet")
        else:
            write_batches_sync(cast(SyncObjectStore, store), key, reader, file_format="parquet")

    assert bytes(await _resolve(store.get(key))) == sentinel


def test_artifact_reference_and_metadata_gc_collection() -> None:
    """ArtifactReference is frozen/SQL-free and ObjectMeta/PutResult participate in cycle GC."""
    assert ObjectMeta.__struct_config__.gc is True
    assert PutResult.__struct_config__.gc is True

    ref = ArtifactReference(
        uri="memory://bucket/artifacts/output.json",
        version=3,
        content_type="application/json",
        size=128,
        updated=dt.datetime(2026, 11, 16, 0, 0, tzinfo=dt.UTC),
        signed_url="https://example.invalid/signed",
    )
    assert ref.uri == "memory://bucket/artifacts/output.json"
    assert ref.version == 3
    untyped_ref: Any = ref
    with pytest.raises(AttributeError):
        untyped_ref.version = 4

    class _CycleTarget:
        def __init__(self) -> None:
            self.owner: Any = None

    meta_target: Any = _CycleTarget()
    meta = ObjectMeta(path="a.bin", size=10, attributes={"target": meta_target})
    meta_target.owner = meta
    assert gc.is_tracked(meta) is True
    meta_ref = weakref.ref(meta_target)
    del meta, meta_target
    gc.collect()
    assert meta_ref() is None

    put_target: Any = _CycleTarget()
    put = PutResult(path="b.bin", size=20, attributes={"target": put_target})
    put_target.owner = put
    assert gc.is_tracked(put) is True
    put_ref = weakref.ref(put_target)
    del put, put_target
    gc.collect()
    assert put_ref() is None


def test_sqlspec_object_store_backend_validation_and_uri_resolution(tmp_path: Path) -> None:
    """Verify ObjectStoreBackend configuration validation, base_path scoping, and traversal guards."""
    with pytest.raises(ImproperConfigurationError, match="requires an explicit store"):
        ObjectStoreBackend("")

    prebuilt = SyncObjectStore(MemoryConfig())
    with pytest.raises(ImproperConfigurationError, match="apply only when building"):
        ObjectStoreBackend(
            store=prebuilt,
            signing=SigningConfig(provider="hmac", hmac_secret="secret"),
        )

    backend = ObjectStoreBackend(f"file://{tmp_path}", base_path="nested/sub/")
    assert backend.protocol == "file"
    assert backend.base_path == "nested/sub"
    assert backend.resolve_uri("a/b.txt") == f"{tmp_path}/nested/sub/a/b.txt"

    with pytest.raises(ValueError, match="Path traversal rejected"):
        backend.resolve_uri("../escape.txt")


@pytest.mark.parametrize("scheme", ["memory", "file"])
def test_sqlspec_object_store_backend_sync_and_async_parity(scheme: str, tmp_path: Path) -> None:
    """Verify sync and async ObjectStoreBackend CRUD, streaming, file open, and Arrow operations."""
    uri = "memory://" if scheme == "memory" else f"file://{tmp_path}"
    backend = ObjectStoreBackend(uri, base_path="root")

    backend.write_bytes_sync("dir/hello.bin", b"hello-bytes")
    assert backend.read_bytes_sync("dir/hello.bin") == b"hello-bytes"
    backend.write_text_sync("dir/note.txt", "hello-text")
    assert backend.read_text_sync("dir/note.txt") == "hello-text"
    assert backend.exists_sync("dir/hello.bin") is True
    assert backend.is_object_sync("dir/hello.bin") is True
    assert backend.is_path_sync("dir") is True

    meta = backend.get_metadata_sync("dir/hello.bin")
    assert meta["size"] == 11
    assert b"".join(backend.stream_read_sync("dir/hello.bin", chunk_size=4)) == b"hello-bytes"

    backend.put_stream_sync("dir/stream.bin", iter([b"abc", b"defg"]))
    assert b"".join(backend.get_stream_sync("dir/stream.bin", chunk_size=3)) == b"abcdefg"
    with backend.open_sync("dir/stream.bin") as reader:
        assert reader.read(4) == b"abcd"

    backend.copy_sync("dir/hello.bin", "dir/copied.bin")
    backend.move_sync("dir/copied.bin", "dir/moved.bin")
    assert backend.exists_sync("dir/copied.bin") is False
    assert backend.read_bytes_sync("dir/moved.bin") == b"hello-bytes"

    listed = sorted(backend.list_objects_sync("dir/"))
    assert "dir/moved.bin" in listed
    assert "dir/note.txt" in backend.glob_sync("dir/*.txt")
    page_items, cursor = backend.list_page_sync("dir/", limit=2)
    assert len(page_items) <= 2
    assert cursor is None or isinstance(cursor, str)

    table = pa.table({"id": [1, 2, 3], "label": ["a", "b", "c"]})
    backend.write_arrow_sync("arrow/data.parquet", table)
    roundtrip_parquet = backend.read_arrow_sync("arrow/data.parquet")
    assert roundtrip_parquet.num_rows == 3
    assert roundtrip_parquet.column("label").to_pylist() == ["a", "b", "c"]

    backend.write_arrow_sync("arrow/data.ipc", table, file_format="arrow")
    roundtrip_ipc = backend.read_arrow_sync("arrow/data.ipc", file_format="arrow")
    assert roundtrip_ipc.num_rows == 3

    cursor_arrow = ArrowCursor.from_rows([{"id": 10, "val": "x"}, {"id": 20, "val": "y"}], threshold_rows=1)
    backend.write_arrow_cursor_sync("arrow/cursor.parquet", cursor_arrow)
    assert backend.read_arrow_sync("arrow/cursor.parquet").num_rows == 2

    streamed_batches = list(backend.stream_arrow_sync("arrow/*.parquet"))
    assert sum(batch.num_rows for batch in streamed_batches) == 5

    response = backend.stream_response("dir/hello.bin", media_type="text/plain", filename="hello.bin")
    assert isinstance(response, StreamingStorageResponse)
    assert response.media_type == "text/plain"

    backend.delete_sync("dir/moved.bin")
    assert backend.exists_sync("dir/moved.bin") is False

    async def _run_async() -> None:
        await backend.write_bytes_async("async/a.bin", b"async-bytes")
        assert await backend.read_bytes_async("async/a.bin") == b"async-bytes"
        await backend.write_text_async("async/t.txt", "async-text")
        assert await backend.read_text_async("async/t.txt") == "async-text"
        assert await backend.exists_async("async/a.bin") is True
        async_meta = await backend.get_metadata_async("async/a.bin")
        assert async_meta["size"] == 11

        async def _chunks() -> AsyncIterator[bytes]:
            yield b"p1-"
            yield b"p2"

        await backend.put_stream_async("async/s.bin", _chunks())
        read_chunks = [chunk async for chunk in await backend.stream_read_async("async/s.bin", chunk_size=2)]
        assert b"".join(read_chunks) == b"p1-p2"
        stream_chunks = [chunk async for chunk in backend.get_stream_async("async/s.bin", chunk_size=3)]
        assert b"".join(stream_chunks) == b"p1-p2"

        async with backend.open_async("async/s.bin") as async_reader:
            assert await async_reader.read() == b"p1-p2"

        await backend.copy_async("async/a.bin", "async/c.bin")
        await backend.move_async("async/c.bin", "async/m.bin")
        assert await backend.exists_async("async/c.bin") is False
        assert await backend.exists_async("async/m.bin") is True
        async_listed = await backend.list_objects_async("async/")
        assert "async/m.bin" in async_listed
        assert "async/t.txt" in await backend.glob_async("async/*.txt")
        page_keys, _ = await backend.list_page_async("async/", limit=10)
        assert "async/t.txt" in page_keys

        await backend.write_arrow_async("async/tbl.parquet", table)
        async_tbl = await backend.read_arrow_async("async/tbl.parquet")
        assert async_tbl.num_rows == 3
        await backend.write_arrow_cursor_async(
            "async/cursor.parquet", ArrowCursor.from_rows([{"k": 1}], threshold_rows=1)
        )
        assert (await backend.read_arrow_async("async/cursor.parquet")).num_rows == 1
        arrow_batches = [batch async for batch in backend.stream_arrow_async("async/*.parquet")]
        assert sum(batch.num_rows for batch in arrow_batches) == 4
        await backend.delete_async("async/m.bin")
        assert await backend.exists_async("async/m.bin") is False

    asyncio.run(_run_async())


def test_sqlspec_object_store_backend_signing_and_registry_alias() -> None:
    """Verify ObjectStoreBackend presigning parity and StorageRegistry alias registration."""
    unsigned = ObjectStoreBackend("memory://")
    assert unsigned.supports_signing is False
    with pytest.raises(NotImplementedError):
        unsigned.sign_sync("reports/daily.csv")
    with pytest.raises(NotImplementedError):
        asyncio.run(unsigned.sign_async("reports/daily.csv"))

    s3_cfg = S3Config(
        bucket="my-bucket",
        region="us-east-1",
        access_key_id="AKIAIOSFODNN7EXAMPLE",
        secret_access_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    )
    signed_backend = ObjectStoreBackend(
        s3_cfg,
        signing=SigningConfig(
            provider="s3",
            bucket="my-bucket",
            region="us-east-1",
            access_key_id="AKIAIOSFODNN7EXAMPLE",
            secret_access_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        ),
    )
    assert signed_backend.supports_signing is True
    single_url = signed_backend.sign_sync("exports/data.parquet", expires_in=300)
    assert isinstance(single_url, str) and "X-Amz-Signature=" in single_url
    batch_urls = asyncio.run(
        signed_backend.sign_async(["exports/a.parquet", "exports/b.parquet"], expires_in=300, for_upload=True)
    )
    assert isinstance(batch_urls, list) and len(batch_urls) == 2

    registry = StorageRegistry()
    register_storage_alias("artifacts", "memory://", registry=registry, base_path="tenant-a")
    assert registry.is_alias_registered("artifacts") is True
    resolved = registry.get("artifacts")
    assert isinstance(resolved, ObjectStoreProtocol)

    store_alias_target = SyncObjectStore("memory://")
    register_storage_alias("store_artifacts", store_alias_target, registry=registry, base_path="tenant-b")
    assert registry.is_alias_registered("store_artifacts") is True
    assert isinstance(registry.get("store_artifacts"), ObjectStoreProtocol)

    direct_backend = ObjectStoreBackend("memory://", base_path="tenant-a")
    assert isinstance(direct_backend, ObjectStoreBackend)
    direct_backend.write_bytes_sync("run-1/out.bin", b"ok-alias")
    assert direct_backend.read_bytes_sync("run-1/out.bin") == b"ok-alias"
    assert direct_backend.resolve_uri("run-1/out.bin").startswith("memory://tenant-a/run-1/out.bin")
