"""Unit tests for vertebra.storage Arrow, Parquet, IPC streaming, and buffered readers/writers."""

import array
import asyncio
import contextlib
import gc
import gzip
import io
import sys
import threading
import warnings
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import pytest

import vertebra.storage as storage
from vertebra.db.arrow import ArrowCursor
from vertebra.storage import (
    AsyncObjectStore,
    AsyncReadableFile,
    AsyncWritableFile,
    MemoryConfig,
    PutResult,
    S3Config,
    SyncObjectStore,
    SyncWritableFile,
    _arrow,
)
from vertebra.storage import (
    IntegrityError as StorageIntegrityError,
)
from vertebra.storage._arrow import (
    iter_batches_async,
    read_table_async,
    read_table_sync,
    write_batches_async,
    write_batches_sync,
)
from vertebra.typing import (
    IntegrityError,
    MissingDependencyError,
    PreconditionError,
    reset_dependency_cache,
)


class _AsyncThreadEvent:
    """Thread-safe event bridging worker thread signaling to an asyncio waiter without polling."""

    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop
        self._thread_event = threading.Event()
        self._async_event = asyncio.Event()

    def set(self) -> None:
        self._thread_event.set()
        with contextlib.suppress(RuntimeError):
            self._loop.call_soon_threadsafe(self._async_event.set)

    def is_set(self) -> bool:
        return self._thread_event.is_set()

    async def wait_or_task(self, task: asyncio.Task[Any], timeout: float) -> None:
        waiter = asyncio.create_task(self._async_event.wait())
        try:
            async with asyncio.timeout(timeout):
                done, _ = await asyncio.wait({task, waiter}, return_when=asyncio.FIRST_COMPLETED)
            if task in done and not self.is_set():
                await task
        finally:
            if not waiter.done():
                waiter.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await waiter


@pytest.mark.parametrize("file_format", ["arrow-ipc", "parquet"])
@pytest.mark.anyio
async def test_storage_arrow_batch_readers(file_format: str) -> None:
    """Public sync and async readers decode exact batches and Parquet projections."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    table = pa.table({"id": range(250), "label": ["value"] * 250})
    sink = pa.BufferOutputStream()
    if file_format == "parquet":
        pq.write_table(table, sink, row_group_size=100)
    else:
        with pa.ipc.new_stream(sink, table.schema) as writer:
            writer.write_table(table, max_chunksize=100)
    store = SyncObjectStore("memory://")
    store.put("batches", sink.getvalue())
    if file_format == "parquet":
        sync_batches = list(store.iter_parquet_batches("batches", batch_size=100, columns=["id"]))
        async_batches = [
            batch async for batch in store.to_async().iter_parquet_batches("batches", batch_size=100, columns=["id"])
        ]
        expected = table.select(["id"])
    else:
        sync_batches = list(store.iter_arrow_batches("batches"))
        async_batches = [batch async for batch in store.to_async().iter_arrow_batches("batches")]
        expected = table
    assert [batch.num_rows for batch in sync_batches] == [100, 100, 50]
    assert pa.Table.from_batches(sync_batches).equals(expected)
    assert pa.Table.from_batches(async_batches).equals(expected)


@pytest.mark.anyio
@pytest.mark.parametrize("empty", [False, True])
async def test_storage_arrow_ipc_file_table_fallback(empty: bool) -> None:
    """Seekable IPC file reads preserve data and zero-row schemas."""
    pa = pytest.importorskip("pyarrow")
    table = pa.table({"id": pa.array([] if empty else [1, 2], type=pa.int64())})
    sink = pa.BufferOutputStream()
    with pa.ipc.new_file(sink, table.schema) as writer:
        writer.write_table(table)
    store = SyncObjectStore("memory://")
    store.put("file", sink.getvalue())
    assert read_table_sync(store, "file", file_format="arrow-ipc").equals(table)
    assert (await read_table_async(store.to_async(), "file", file_format="arrow-ipc")).equals(table)


@pytest.mark.anyio
async def test_storage_arrow_dictionary_stream_and_early_close(monkeypatch: pytest.MonkeyPatch) -> None:
    """One persistent IPC decoder retains dictionary state and closes native streams."""
    pa = pytest.importorskip("pyarrow")
    labels = pa.DictionaryArray.from_arrays(pa.array([0, 1]), pa.array(["one", "two"]))
    batch = pa.record_batch([labels], names=["label"])
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, batch.schema) as writer:
        writer.write_batch(batch)
        writer.write_batch(batch)
    store = SyncObjectStore("memory://")
    store.put("dictionary", sink.getvalue())
    streams: list[Any] = []
    original = AsyncObjectStore.get_stream

    def observe_stream(self: AsyncObjectStore, *args: Any, **kwargs: Any) -> Any:
        stream = original(self, *args, **kwargs)
        streams.append(stream)
        return stream

    monkeypatch.setattr(AsyncObjectStore, "get_stream", observe_stream)
    batches = store.to_async().iter_arrow_batches("dictionary")
    assert (await anext(batches)).column(0).to_pylist() == ["one", "two"]
    assert (await anext(batches)).column(0).to_pylist() == ["one", "two"]
    await batches.aclose()
    assert len(streams) == 1
    assert streams[0].ag_frame is None
    assert [item.column(0).to_pylist() for item in store.iter_arrow_batches("dictionary")] == [
        ["one", "two"],
        ["one", "two"],
    ]


@pytest.mark.anyio
async def test_storage_parquet_reader_native_range_demand(monkeypatch: pytest.MonkeyPatch) -> None:
    """Later rowgroups remain unread until the consumer asks for their batches."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    table = pa.table({"id": range(300), "blob": [f"{index}:" + "x" * 2048 for index in range(300)]})
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, row_group_size=100, compression=None, use_dictionary=False)
    payload = sink.getvalue()
    metadata = pq.ParquetFile(pa.BufferReader(payload)).metadata
    second_offset = metadata.row_group(1).column(0).data_page_offset
    store = SyncObjectStore("memory://")
    store.put("groups", payload)
    ranges: list[tuple[int, int]] = []
    handles: list[AsyncReadableFile] = []
    original_range = AsyncObjectStore.get_range
    original_open = AsyncObjectStore.open_reader

    async def observe_range(self: AsyncObjectStore, key: str, offset: int, length: int) -> bytes:
        ranges.append((offset, length))
        return await original_range(self, key, offset, length)

    def observe_open(self: AsyncObjectStore, *args: Any, **kwargs: Any) -> AsyncReadableFile:
        handle = original_open(self, *args, **kwargs)
        handles.append(handle)
        return handle

    def reject_materialization(*args: Any, **kwargs: Any) -> Any:
        pytest.fail("batch readers must use native streams or ranges")

    monkeypatch.setattr(AsyncObjectStore, "get_range", observe_range)
    monkeypatch.setattr(AsyncObjectStore, "open_reader", observe_open)
    monkeypatch.setattr(AsyncObjectStore, "get", reject_materialization)
    monkeypatch.setattr(AsyncObjectStore, "get_view", reject_materialization)
    monkeypatch.setattr(SyncObjectStore, "get_range", reject_materialization)
    batches = store.to_async().iter_parquet_batches("groups", batch_size=100)
    first = await anext(batches)
    assert first.equals(table.slice(0, 100).to_batches()[0])
    assert payload.size > 3 * 65536
    data_ranges = [(offset, length) for offset, length in ranges if offset != payload.size - 65536]
    assert data_ranges
    assert all(offset + length <= second_offset for offset, length in data_ranges)
    checkpoint = ranges.copy()
    await asyncio.sleep(0)
    assert ranges == checkpoint
    assert (await anext(batches)).equals(table.slice(100, 100).to_batches()[0])
    assert any(offset == second_offset for offset, _ in ranges)
    await batches.aclose()
    assert len(handles) == 1
    assert handles[0].closed


@pytest.mark.anyio
async def test_storage_parquet_explicit_prefetch_option(monkeypatch: pytest.MonkeyPatch) -> None:
    """The private SQLSpec seam forwards an explicit prefetch opt-in."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    table = pa.table({"id": [1, 2]})
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink)
    store = SyncObjectStore("memory://")
    store.put("prefetch", sink.getvalue())
    seen: list[bool] = []
    original = pq.ParquetFile

    def observe_options(*args: Any, **kwargs: Any) -> Any:
        seen.append(kwargs["pre_buffer"])
        return original(*args, **kwargs)

    monkeypatch.setattr(pq, "ParquetFile", observe_options)
    result = [
        batch
        async for batch in iter_batches_async(
            store.to_async(), "prefetch", file_format="parquet", options={"pre_buffer": True}
        )
    ]
    assert pa.Table.from_batches(result).equals(table)
    assert seen == [True]


@pytest.mark.anyio
@pytest.mark.parametrize("batch_size", [0, -1])
async def test_storage_parquet_rejects_batch_size_before_io(batch_size: int) -> None:
    """Invalid demand sizes fail before looking up a missing native object."""
    store = SyncObjectStore("memory://")
    with pytest.raises(ValueError, match="batch_size"):
        next(store.iter_parquet_batches("missing", batch_size=batch_size))
    with pytest.raises(ValueError, match="batch_size"):
        await anext(store.to_async().iter_parquet_batches("missing", batch_size=batch_size))


def _parquet_column_encodings(store: SyncObjectStore, key: str) -> dict[str, set[str]]:
    """Return the encodings recorded for each column of the first row group."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    row_group = pq.ParquetFile(pa.BufferReader(store.get(key))).metadata.row_group(0)
    return {
        row_group.column(index).path_in_schema: set(row_group.column(index).encodings)
        for index in range(row_group.num_columns)
    }


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize(
    ("use_dictionary", "use_byte_stream_split", "dictionary_columns", "split_columns"),
    [
        (False, ["metric"], set(), {"metric"}),
        (["tag"], False, {"tag"}, set()),
        ("tag", False, {"tag"}, set()),
        (False, "metric", set(), {"metric"}),
        (True, True, {"id", "tag", "metric"}, set()),
    ],
    ids=["split-list", "dictionary-list", "dictionary-str", "split-str", "all-columns"],
)
async def test_storage_parquet_encoding_options_reach_column_metadata(
    asynchronous: bool,
    use_dictionary: bool | str | list[str],
    use_byte_stream_split: bool | str | list[str],
    dictionary_columns: set[str],
    split_columns: set[str],
) -> None:
    """Dictionary and byte-stream-split choices are recorded per column in the written footer."""
    pa = pytest.importorskip("pyarrow")
    table = pa.table(
        {
            "id": pa.array(range(64), type=pa.int64()),
            "tag": [f"tag-{index % 4}" for index in range(64)],
            "metric": [index / 7 for index in range(64)],
        }
    )
    store = SyncObjectStore("memory://")
    if asynchronous:
        await store.to_async().write_parquet(
            "t.parquet", table, use_dictionary=use_dictionary, use_byte_stream_split=use_byte_stream_split
        )
        batches = [batch async for batch in store.to_async().iter_parquet_batches("t.parquet")]
    else:
        store.write_parquet(
            "t.parquet", table, use_dictionary=use_dictionary, use_byte_stream_split=use_byte_stream_split
        )
        batches = list(store.iter_parquet_batches("t.parquet"))
    encodings = _parquet_column_encodings(store, "t.parquet")
    assert {name for name, values in encodings.items() if "RLE_DICTIONARY" in values} == dictionary_columns
    assert {name for name, values in encodings.items() if "BYTE_STREAM_SPLIT" in values} == split_columns
    assert pa.Table.from_batches(batches).equals(table)


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_storage_parquet_options_override_explicit_keywords(asynchronous: bool) -> None:
    """Writer options replace compression and rowgroup defaults without duplicate keyword errors."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    table = pa.table({"id": pa.array(range(10), type=pa.int64())})
    store = SyncObjectStore("memory://")
    options = {"compression": "snappy", "row_group_size": 4, "use_dictionary": False}
    if asynchronous:
        await store.to_async().write_parquet("o.parquet", table, compression="zstd", options=options)
    else:
        store.write_parquet("o.parquet", table, compression="zstd", options=options)
    metadata = pq.ParquetFile(pa.BufferReader(store.get("o.parquet"))).metadata
    assert metadata.num_row_groups == 3
    assert metadata.row_group(0).column(0).compression == "SNAPPY"
    assert "RLE_DICTIONARY" not in metadata.row_group(0).column(0).encodings


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_storage_parquet_reader_options_select_row_groups(asynchronous: bool) -> None:
    """Reader options forward rowgroup selection to demand-paged Parquet iteration."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    table = pa.table({"id": pa.array(range(30), type=pa.int64())})
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, row_group_size=10)
    store = SyncObjectStore("memory://")
    store.put("groups.parquet", sink.getvalue())
    options = {"row_groups": [1], "use_threads": False}
    if asynchronous:
        batches = [batch async for batch in store.to_async().iter_parquet_batches("groups.parquet", options=options)]
    else:
        batches = list(store.iter_parquet_batches("groups.parquet", options=options))
    assert pa.Table.from_batches(batches).equals(table.slice(10, 10))


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_storage_arrow_ipc_compression_options_round_trip(asynchronous: bool) -> None:
    """IPC write options compress record batch bodies and read options decode them in bounded slices."""
    pa = pytest.importorskip("pyarrow")
    table = pa.table({"id": pa.array([7] * 20_000, type=pa.int64())})
    store = SyncObjectStore("memory://")
    read_options = {"use_threads": False}
    if asynchronous:
        async_store = store.to_async()
        await async_store.write_arrow("plain.arrow", table)
        await async_store.write_arrow("t.arrow", table, options={"compression": "zstd"})
        batches = [
            batch async for batch in async_store.iter_arrow_batches("t.arrow", batch_size=8_000, options=read_options)
        ]
    else:
        store.write_arrow("plain.arrow", table)
        store.write_arrow("t.arrow", table, options={"compression": "zstd"})
        batches = list(store.iter_arrow_batches("t.arrow", batch_size=8_000, options=read_options))
    assert store.head("t.arrow").size * 10 < store.head("plain.arrow").size
    assert [batch.num_rows for batch in batches] == [8_000, 8_000, 4_000]
    assert pa.Table.from_batches(batches).equals(table)


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.anyio
async def test_storage_arrow_ipc_options_reject_conflicting_write_options(asynchronous: bool) -> None:
    """A prebuilt IpcWriteOptions cannot be combined with loose IPC option keys."""
    pa = pytest.importorskip("pyarrow")
    table = pa.table({"id": [1]})
    store = SyncObjectStore("memory://")
    options = {"options": pa.ipc.IpcWriteOptions(), "compression": "zstd"}
    with pytest.raises(ValueError, match="IpcWriteOptions"):
        if asynchronous:
            await store.to_async().write_arrow("conflict.arrow", table, options=options)
        else:
            store.write_arrow("conflict.arrow", table, options=options)
    assert not store.exists("conflict.arrow")


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("file_format", ["arrow-ipc", "parquet"])
@pytest.mark.parametrize("wrapper", ["pyarrow-buffer", "memoryview", "bytes", "bytearray"])
async def test_storage_writers_accept_raw_ipc_buffers(asynchronous: bool, file_format: str, wrapper: str) -> None:
    """Serialized IPC streams passed as raw buffers decode directly without a buffer_view wrapper."""
    pa = pytest.importorskip("pyarrow")
    table = pa.table({"id": pa.array([1, 2, 3], type=pa.int64()), "tag": ["a", "b", "c"]})
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    ipc_buffer = sink.getvalue()
    payload: Any = {
        "pyarrow-buffer": ipc_buffer,
        "memoryview": memoryview(ipc_buffer),
        "bytes": ipc_buffer.to_pybytes(),
        "bytearray": bytearray(ipc_buffer.to_pybytes()),
    }[wrapper]
    store = SyncObjectStore("memory://")
    async_store = store.to_async()
    if file_format == "parquet":
        if asynchronous:
            await async_store.write_parquet("from_buf", payload)
        else:
            store.write_parquet("from_buf", payload)
        batches = list(store.iter_parquet_batches("from_buf"))
    else:
        if asynchronous:
            await async_store.write_arrow("from_buf", payload)
        else:
            store.write_arrow("from_buf", payload)
        batches = list(store.iter_arrow_batches("from_buf"))
    assert pa.Table.from_batches(batches).equals(table)


@pytest.mark.anyio
@pytest.mark.parametrize(("options", "expected"), [(None, True), ({"pre_buffer": False}, False)])
async def test_storage_parquet_table_reads_default_to_pre_buffer(
    options: dict[str, Any] | None, expected: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Materialized Parquet reads coalesce ranges by default while honoring an explicit override."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    table = pa.table({"id": [1, 2]})
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink)
    store = SyncObjectStore("memory://")
    store.put("table.parquet", sink.getvalue())
    seen: list[bool] = []
    original = pq.read_table

    def observe_read(*args: Any, **kwargs: Any) -> Any:
        seen.append(kwargs["pre_buffer"])
        return original(*args, **kwargs)

    monkeypatch.setattr(pq, "read_table", observe_read)
    assert _arrow.read_table_sync(store, "table.parquet", file_format="parquet", options=options).equals(table)
    assert (
        await _arrow.read_table_async(store.to_async(), "table.parquet", file_format="parquet", options=options)
    ).equals(table)
    assert seen == [expected, expected]


@pytest.mark.parametrize(
    ("chunks", "requests", "expected", "zero_copy"),
    [
        ([b"abcdefgh"], [3, 3, 3], [b"abc", b"def", b"gh"], [True, True, True]),
        ([b"abc", b"defg", b"hi"], [5, 4, 1], [b"abcde", b"fghi", b""], [False, False, False]),
        ([], [4], [b""], [False]),
    ],
    ids=["within-chunk", "spanning-chunks", "empty-stream"],
)
def test_storage_stream_file_read_buffer_slices_pending_chunk(
    chunks: list[bytes], requests: list[int], expected: list[bytes], zero_copy: list[bool]
) -> None:
    """`read_buffer` returns a view over the pending native chunk when it fits and one copy when it spans chunks."""
    pending = iter(chunks)
    stream = _arrow._StreamFile(lambda: next(pending, None))
    results = [stream.read_buffer(size) for size in requests]
    assert [bytes(result) for result in results] == expected
    assert [isinstance(result, memoryview) for result in results] == zero_copy
    assert stream.tell() == sum(len(chunk) for chunk in chunks)


@pytest.mark.anyio
@pytest.mark.parametrize("file_format", ["arrow-ipc", "parquet"])
async def test_storage_arrow_corruption_closes_sources(file_format: str, monkeypatch: pytest.MonkeyPatch) -> None:
    """Corrupt codecs raise their original errors and close real native sources."""
    pa = pytest.importorskip("pyarrow")
    store = SyncObjectStore("memory://")
    store.put("corrupt", b"truncated invalid codec")
    sources: list[Any] = []
    original = AsyncObjectStore.open_reader if file_format == "parquet" else AsyncObjectStore.get_stream

    def observe_source(self: AsyncObjectStore, *args: Any, **kwargs: Any) -> Any:
        source = original(self, *args, **kwargs)
        sources.append(source)
        return source

    monkeypatch.setattr(AsyncObjectStore, "open_reader" if file_format == "parquet" else "get_stream", observe_source)
    async_store = store.to_async()
    iterator = (
        async_store.iter_parquet_batches("corrupt")
        if file_format == "parquet"
        else async_store.iter_arrow_batches("corrupt")
    )
    with pytest.raises(pa.ArrowInvalid):
        await anext(iterator)
    assert len(sources) == 1
    assert sources[0].closed if file_format == "parquet" else sources[0].ag_frame is None
    sync_iterator = (
        store.iter_parquet_batches("corrupt") if file_format == "parquet" else store.iter_arrow_batches("corrupt")
    )
    with pytest.raises(pa.ArrowInvalid):
        next(sync_iterator)


@pytest.mark.anyio
@pytest.mark.parametrize("file_format", ["arrow-ipc", "parquet"])
@pytest.mark.parametrize("cancel", [False, True])
async def test_storage_arrow_native_network_barrier(
    file_format: str, cancel: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Real native network reads keep the loop responsive and unwind before close."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    table = pa.table({"id": [1, 2]})
    sink = pa.BufferOutputStream()
    if file_format == "parquet":
        pq.write_table(table, sink)
    else:
        with pa.ipc.new_stream(sink, table.schema) as writer:
            writer.write_table(table)
    payload = sink.getvalue().to_pybytes()
    entered = _AsyncThreadEvent(asyncio.get_running_loop())
    release = threading.Event()
    paths: list[str] = []

    class RangeHandler(BaseHTTPRequestHandler):
        def do_HEAD(self) -> None:
            paths.append(self.path)
            self.send_response(200)
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("ETag", '"fixture"')
            self.send_header("Last-Modified", "Mon, 01 Jan 2024 00:00:00 GMT")
            self.end_headers()

        def do_GET(self) -> None:
            paths.append(self.path)
            entered.set()
            if not release.wait(10):
                return
            range_header = self.headers.get("Range")
            if range_header:
                start, end = range_header.removeprefix("bytes=").split("-", 1)
                offset = int(start)
                limit = int(end) + 1 if end else len(payload)
                body = payload[offset:limit]
                self.send_response(206)
                self.send_header("Content-Range", f"bytes {offset}-{limit - 1}/{len(payload)}")
            else:
                body = payload
                self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("ETag", '"fixture"')
            self.end_headers()
            with contextlib.suppress(BrokenPipeError, ConnectionResetError):
                self.wfile.write(body)

        def log_message(self, format: str, *args: object) -> None:
            _ = (format, args)

    server = ThreadingHTTPServer(("127.0.0.1", 0), RangeHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    sources: list[Any] = []
    original = AsyncObjectStore.open_reader if file_format == "parquet" else AsyncObjectStore.get_stream

    def observe_source(self: AsyncObjectStore, *args: Any, **kwargs: Any) -> Any:
        source = original(self, *args, **kwargs)
        sources.append(source)
        return source

    monkeypatch.setattr(AsyncObjectStore, "open_reader" if file_format == "parquet" else "get_stream", observe_source)
    store = AsyncObjectStore(
        S3Config(
            bucket="codec-fixture",
            region="us-east-1",
            endpoint=f"http://127.0.0.1:{server.server_port}",
            access_key_id="test",
            secret_access_key="test",
            allow_http=True,
        )
    )
    batches = (
        store.iter_parquet_batches("data.parquet")
        if file_format == "parquet"
        else store.iter_arrow_batches("data.parquet")
    )
    before = asyncio.all_tasks()

    async def next_batch() -> Any:
        return await anext(batches)

    task: asyncio.Task[Any] = asyncio.create_task(next_batch())
    try:
        await entered.wait_or_task(task, 5)
        assert await asyncio.create_task(asyncio.sleep(0, result="responsive")) == "responsive"
        if cancel:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(task, 2)
        else:
            release.set()
            assert (await asyncio.wait_for(task, 5)).equals(table.to_batches()[0])
            await batches.aclose()
        assert len(sources) == 1
        assert sources[0].closed if file_format == "parquet" else sources[0].ag_frame is None
        assert all(path == "/codec-fixture/data.parquet" for path in paths)
        assert not (asyncio.all_tasks() - before)
    finally:
        release.set()
        if not task.done():
            task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        await batches.aclose()
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


@pytest.mark.anyio
@pytest.mark.parametrize("file_format", ["arrow-ipc", "parquet"])
async def test_storage_arrow_schema_only_batches(file_format: str) -> None:
    """Schema-only streams yield no batches without losing full-Table schema."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    table = pa.table({"id": pa.array([], type=pa.int64())})
    sink = pa.BufferOutputStream()
    if file_format == "parquet":
        pq.write_table(table, sink)
    else:
        with pa.ipc.new_stream(sink, table.schema):
            pass
    store = SyncObjectStore("memory://")
    store.put("empty", sink.getvalue())
    sync_batches = (
        store.iter_parquet_batches("empty") if file_format == "parquet" else store.iter_arrow_batches("empty")
    )
    assert list(sync_batches) == []
    async_store = store.to_async()
    async_batches = (
        async_store.iter_parquet_batches("empty")
        if file_format == "parquet"
        else async_store.iter_arrow_batches("empty")
    )
    assert [batch async for batch in async_batches] == []
    assert read_table_sync(store, "empty", file_format=file_format).equals(table)
    assert (await read_table_async(async_store, "empty", file_format=file_format)).equals(table)


@pytest.mark.anyio
async def test_storage_arrow_missing_codec_before_io(monkeypatch: pytest.MonkeyPatch) -> None:
    """Optional Arrow dependencies fail explicitly before native reads begin."""
    store = SyncObjectStore("memory://")
    with monkeypatch.context() as context:
        context.setitem(sys.modules, "pyarrow", None)
        reset_dependency_cache()
        with pytest.raises(MissingDependencyError, match="pyarrow"):
            next(store.iter_arrow_batches("missing"))
        with pytest.raises(MissingDependencyError, match="pyarrow"):
            await anext(store.to_async().iter_arrow_batches("missing"))
    reset_dependency_cache()


@pytest.mark.anyio
@pytest.mark.parametrize("file_format", ["arrow-ipc", "parquet"])
@pytest.mark.parametrize("key", ["sync", "async"])
async def test_storage_arrow_writers_roundtrip(file_format: str, key: str) -> None:
    """Public writers return native metadata and preserve batches and rowgroups for each writer path."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    table = pa.table({"id": range(250)})
    store = SyncObjectStore("memory://")
    if file_format == "parquet":
        result = store.write_parquet("sync", table, row_group_size=100)
        async_result = await store.to_async().write_parquet("async", table, row_group_size=100)
    else:
        result = store.write_arrow("sync", table, batch_size=100)
        async_result = await store.to_async().write_arrow("async", table, batch_size=100)
    assert isinstance(result, PutResult)
    assert isinstance(async_result, PutResult)
    assert result.size > 0
    assert async_result.size == result.size
    payload = pa.BufferReader(store.get(key))
    if file_format == "parquet":
        reader = pq.ParquetFile(payload)
        assert [reader.metadata.row_group(i).num_rows for i in range(3)] == [100, 100, 50]
        assert reader.read().equals(table)
    else:
        with pa.ipc.open_stream(payload) as reader:
            batches = list(reader)
        assert [batch.num_rows for batch in batches] == [100, 100, 50]
        assert pa.Table.from_batches(batches).equals(table)


class _ObservedArrowUpload:
    """Record completion while delegating every operation to a real native upload."""

    def __init__(self, native: Any, events: list[tuple[str, int]], asynchronous: bool) -> None:
        self.native = native
        self.events = events
        self.asynchronous = asynchronous

    def write_part(self, chunk: Any) -> Any:
        self.events.append(("start", len(chunk)))
        result = self.native.write_part(chunk)
        if self.asynchronous:

            async def complete() -> None:
                await result
                self.events.append(("part", len(chunk)))

            return complete()
        self.events.append(("part", len(chunk)))
        return result

    def complete(self) -> Any:
        self.events.append(("complete", 0))
        return self.native.complete()

    def abort(self) -> Any:
        self.events.append(("abort", 0))
        return self.native.abort()


class _ObservedArrowStore:
    """Observe native multipart sessions without replacing the public store helper."""

    def __init__(self, native: Any, events: list[tuple[str, int]], asynchronous: bool) -> None:
        self.native = native
        self.events = events
        self.asynchronous = asynchronous

    def __getattr__(self, name: str) -> Any:
        return getattr(self.native, name)

    def create_multipart(self, *args: Any, **kwargs: Any) -> Any:
        upload = self.native.create_multipart(*args, **kwargs)
        self.events.append(("create", 0))
        if self.asynchronous:

            async def complete() -> Any:
                return _ObservedArrowUpload(await upload, self.events, True)

            return complete()
        return _ObservedArrowUpload(upload, self.events, False)


@pytest.mark.anyio
@pytest.mark.parametrize("file_format", ["arrow-ipc", "parquet"])
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("fail", [False, True])
async def test_storage_arrow_writer_demand_and_abort(file_format: str, asynchronous: bool, fail: bool) -> None:
    """The first batch finishes native writes before the producer advances or fails."""
    pa = pytest.importorskip("pyarrow")
    batch = pa.record_batch({"id": [1, 2]})
    events: list[tuple[str, int]] = []
    closed: list[bool] = []
    base = SyncObjectStore("memory://")
    paired = base.to_async() if asynchronous else base
    native = _ObservedArrowStore(paired._require_native(), events, asynchronous)

    def batches() -> Iterator[Any]:
        try:
            yield batch
            assert sum(size for kind, size in events if kind == "part") > 20
            if fail:
                raise ValueError("producer failed")
            yield batch
        finally:
            closed.append(True)

    async def write() -> Any:
        if asynchronous:
            async_store = AsyncObjectStore("memory://", _native_instance=native)
            return await (
                async_store.write_parquet("output", batches())
                if file_format == "parquet"
                else async_store.write_arrow("output", batches())
            )
        sync_store = SyncObjectStore("memory://", _native_instance=native)
        return (
            sync_store.write_parquet("output", batches())
            if file_format == "parquet"
            else sync_store.write_arrow("output", batches())
        )

    if fail:
        with pytest.raises(ValueError, match="producer failed"):
            await write()
        assert ("abort", 0) in events
        assert ("complete", 0) not in events
        assert not base.exists("output")
    else:
        assert isinstance(await write(), PutResult)
        iterator = (
            base.iter_parquet_batches("output") if file_format == "parquet" else base.iter_arrow_batches("output")
        )
        assert pa.Table.from_batches(list(iterator)).equals(pa.Table.from_batches([batch, batch]))
    assert closed == [True]


@pytest.mark.anyio
@pytest.mark.parametrize("file_format", ["arrow-ipc", "parquet"])
@pytest.mark.parametrize("input_kind", ["empty-table", "batch", "cursor", "empty-iterable"])
@pytest.mark.parametrize("key", ["sync", "async"])
async def test_storage_arrow_writer_input_contracts(
    file_format: str, input_kind: str, key: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Tables, batches, cursors and explicit empty schemas retain Arrow values through each writer path."""
    pa = pytest.importorskip("pyarrow")
    table = pa.table({"id": pa.array([] if input_kind.startswith("empty") else [1, 2], type=pa.int64())})
    cursor = ArrowCursor.from_rows([{"id": 1}, {"id": 2}]) if input_kind == "cursor" else None
    if cursor is not None:

        def reject_rows(*args: Any, **kwargs: Any) -> Any:
            pytest.fail("Arrow writers must decode the cursor buffer without Python rows")

        monkeypatch.setattr(ArrowCursor, "to_pylist", reject_rows)
        monkeypatch.setattr(ArrowCursor, "to_arrow_table", reject_rows)
    data = (
        cursor
        if cursor is not None
        else table.to_batches()[0]
        if input_kind == "batch"
        else iter(())
        if input_kind == "empty-iterable"
        else table
    )
    store = SyncObjectStore("memory://")
    try:
        write_batches_sync(store, "sync", data, file_format=file_format, schema=table.schema, batch_size=1)
        await write_batches_async(
            store.to_async(), "async", data, file_format=file_format, schema=table.schema, batch_size=1
        )
        batches = store.iter_parquet_batches(key) if file_format == "parquet" else store.iter_arrow_batches(key)
        assert pa.Table.from_batches(list(batches), schema=table.schema).equals(table)
    finally:
        if cursor is not None:
            cursor.close()


@pytest.mark.anyio
@pytest.mark.parametrize("file_format", ["arrow-ipc", "parquet"])
@pytest.mark.parametrize("case", ["zero-size", "empty", "schema-mismatch", "bad-option", "missing-codec"])
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_storage_arrow_writer_rejections(
    file_format: str, case: str, asynchronous: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Invalid inputs preserve their errors and never commit an object."""
    pa = pytest.importorskip("pyarrow")
    batch = pa.record_batch({"id": [1]})
    store = SyncObjectStore("memory://")
    options = {"unsupported_codec_option": True} if case == "bad-option" else None
    data = [] if case == "empty" else [batch, pa.record_batch({"other": [2]})] if case == "schema-mismatch" else [batch]
    error = MissingDependencyError if case == "missing-codec" else TypeError if case == "bad-option" else ValueError
    with monkeypatch.context() as context:
        if case == "missing-codec":
            context.setitem(sys.modules, "pyarrow", None)
            reset_dependency_cache()
        with pytest.raises(error):
            if asynchronous:
                await write_batches_async(
                    store.to_async(),
                    "missing",
                    iter(data),
                    file_format=file_format,
                    batch_size=0 if case == "zero-size" else 2,
                    options=options,
                )
            else:
                write_batches_sync(
                    store,
                    "missing",
                    iter(data),
                    file_format=file_format,
                    batch_size=0 if case == "zero-size" else 2,
                    options=options,
                )
    reset_dependency_cache()
    assert not store.exists("missing")


@pytest.mark.anyio
@pytest.mark.parametrize("file_format", ["arrow-ipc", "parquet"])
async def test_storage_arrow_writer_native_network_cancellation(file_format: str) -> None:
    """Cancel an actual native multipart request and observe abort before returning."""
    pa = pytest.importorskip("pyarrow")
    batch = pa.record_batch({"payload": [b"x" * (9 * 1024 * 1024)]})
    entered = _AsyncThreadEvent(asyncio.get_running_loop())
    release = threading.Event()
    aborted = threading.Event()
    requests: list[tuple[str, str]] = []
    closed: list[bool] = []

    class MultipartHandler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            requests.append(("POST", self.path))
            self.rfile.read(int(self.headers.get("Content-Length", "0")))
            if "uploads" in self.path:
                body = b"<InitiateMultipartUploadResult><Bucket>codec-fixture</Bucket><Key>output</Key><UploadId>fixture</UploadId></InitiateMultipartUploadResult>"
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            else:
                self.send_error(500, "unexpected completion")

        def do_PUT(self) -> None:
            requests.append(("PUT", self.path))
            self.rfile.read(int(self.headers["Content-Length"]))
            entered.set()
            release.wait(10)
            self.send_response(200)
            self.send_header("ETag", '"part"')
            self.send_header("Content-Length", "0")
            self.end_headers()

        def do_DELETE(self) -> None:
            requests.append(("DELETE", self.path))
            aborted.set()
            self.send_response(204)
            self.end_headers()

        def log_message(self, format: str, *args: object) -> None:
            _ = (format, args)

    server = ThreadingHTTPServer(("127.0.0.1", 0), MultipartHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    config = S3Config(
        bucket="codec-fixture",
        region="us-east-1",
        endpoint=f"http://127.0.0.1:{server.server_port}",
        access_key_id="test",
        secret_access_key="test",
        allow_http=True,
    )
    native_store = AsyncObjectStore(config)
    events: list[tuple[str, int]] = []
    store = AsyncObjectStore(config, _native_instance=_ObservedArrowStore(native_store._require_native(), events, True))

    def batches() -> Iterator[Any]:
        try:
            yield batch
            yield batch
        finally:
            closed.append(True)

    before = asyncio.all_tasks()
    operation = (
        store.write_parquet("output", batches(), compression="NONE")
        if file_format == "parquet"
        else store.write_arrow("output", batches())
    )
    task = asyncio.create_task(operation)
    try:
        await entered.wait_or_task(task, 10)
        assert await asyncio.create_task(asyncio.sleep(0, result="responsive")) == "responsive"
        assert sum(kind == "start" for kind, _ in events) > sum(kind == "part" for kind, _ in events)
        assert ("complete", 0) not in events
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 5)
        assert aborted.is_set()
        assert closed == [True]
        assert any(method == "DELETE" and "uploadId=fixture" in path for method, path in requests)
        assert all("uploads" in path for method, path in requests if method == "POST")
        assert not (asyncio.all_tasks() - before)
    finally:
        release.set()
        if not task.done():
            task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


@pytest.mark.anyio
@pytest.mark.parametrize("file_format", ["arrow-ipc", "parquet"])
async def test_storage_arrow_writer_dictionary_state(file_format: str) -> None:
    """Persistent writers preserve dictionary batches and native media metadata."""
    pa = pytest.importorskip("pyarrow")
    labels = pa.DictionaryArray.from_arrays(pa.array([0, 1]), pa.array(["one", "two"]))
    batch = pa.record_batch([labels], names=["label"])
    store = SyncObjectStore("memory://")
    async_store = store.to_async()
    if file_format == "parquet":
        await async_store.write_parquet("dictionary", iter([batch, batch]), row_group_size=2)
        result = list(store.iter_parquet_batches("dictionary", batch_size=2))
    else:
        await async_store.write_arrow("dictionary", iter([batch, batch]), batch_size=2)
        result = list(store.iter_arrow_batches("dictionary"))
    assert [item.column(0).to_pylist() for item in result] == [["one", "two"], ["one", "two"]]
    expected = "application/vnd.apache.parquet" if file_format == "parquet" else "application/vnd.apache.arrow.stream"
    assert store.head("dictionary").content_type == expected


@pytest.mark.anyio
@pytest.mark.parametrize("file_format", ["arrow-ipc", "parquet"])
@pytest.mark.parametrize("cancellations", [1, 2, 3])
async def test_storage_arrow_writer_waits_for_cancelled_codec_worker(file_format: str, cancellations: int) -> None:
    """Cancellation retains the sink and producer until the active codec step exits."""
    pa = pytest.importorskip("pyarrow")
    batch = pa.record_batch({"id": [1]})
    entered = _AsyncThreadEvent(asyncio.get_running_loop())
    release = threading.Event()
    closed: list[bool] = []
    store = SyncObjectStore("memory://")

    def batches() -> Iterator[Any]:
        try:
            yield batch
            entered.set()
            if not release.wait(10):
                raise TimeoutError("codec barrier was not released")
            yield batch
        finally:
            closed.append(True)

    async_store = store.to_async()
    operation = (
        async_store.write_parquet("cancelled", batches())
        if file_format == "parquet"
        else async_store.write_arrow("cancelled", batches())
    )
    task = asyncio.create_task(operation)
    try:
        await entered.wait_or_task(task, 5)
        for _ in range(cancellations):
            task.cancel()
            await asyncio.sleep(0)
            await asyncio.sleep(0)
        assert not task.done()
        assert closed == []
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 5)
        assert closed == [True]
        assert not store.exists("cancelled")
    finally:
        release.set()
        if not task.done():
            task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


class _RecordingUpload:
    """Record part lengths while delegating every multipart operation to a real native upload."""

    def __init__(self, native: Any, parts: list[int], payloads: list[Any]) -> None:
        self.native = native
        self.parts = parts
        self.payloads = payloads

    def write_part(self, chunk: Any) -> Any:
        self.parts.append(memoryview(chunk).nbytes)
        self.payloads.append(chunk)
        return self.native.write_part(chunk)

    def complete(self) -> Any:
        return self.native.complete()

    def abort(self) -> Any:
        return self.native.abort()


class _RecordingNative:
    """Record payload objects and multipart parts forwarded to a real native store."""

    def __init__(self, native: Any, asynchronous: bool) -> None:
        self.native = native
        self.asynchronous = asynchronous
        self.payloads: list[Any] = []
        self.parts: list[int] = []
        self.uploads = 0

    def __getattr__(self, name: str) -> Any:
        return getattr(self.native, name)

    def put(self, key: str, data: Any, **kwargs: Any) -> Any:
        self.payloads.append(data)
        return self.native.put(key, data, **kwargs)

    def create_multipart(self, *args: Any, **kwargs: Any) -> Any:
        upload = self.native.create_multipart(*args, **kwargs)
        self.uploads += 1
        if self.asynchronous:

            async def resolve() -> Any:
                return _RecordingUpload(await upload, self.parts, self.payloads)

            return resolve()
        return _RecordingUpload(upload, self.parts, self.payloads)


def _recording_store(asynchronous: bool) -> tuple[Any, SyncObjectStore, _RecordingNative]:
    """Build a store whose native backend records forwarded payloads, plus a sync verifier.

    The recording native wraps the verifier's own backend, or the handle derived from it through
    `to_async()`, so the verifier reads exactly what the recording store wrote.
    """
    base = SyncObjectStore(MemoryConfig())
    paired = base.to_async() if asynchronous else base
    native = _RecordingNative(paired._require_native(), asynchronous)
    store: Any = (
        AsyncObjectStore(MemoryConfig(), _native_instance=native)
        if asynchronous
        else SyncObjectStore(MemoryConfig(), _native_instance=native)
    )
    return store, base, native


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_put_forwards_buffers_without_memoryview_wrapping(asynchronous: bool) -> None:
    """put and put_multipart hand caller buffers straight to native so bytes stay zero-copy."""
    store, base, native = _recording_store(asynchronous)
    payload = b"immutable-payload"
    mutable = bytearray(b"mutable-payload")
    parts = [b"part-a:", bytearray(b"part-b")]
    typed = array.array("I", [1, 2, 3])

    if asynchronous:
        first = await store.put("imm.bin", payload)
        await store.put("mut.bin", mutable)
        mutable.clear()
        multi = await store.put_multipart("multi.bin", parts)
        typed_meta = await store.put("typed.bin", typed)
    else:
        first = store.put("imm.bin", payload)
        store.put("mut.bin", mutable)
        mutable.clear()
        multi = store.put_multipart("multi.bin", parts)
        typed_meta = store.put("typed.bin", typed)

    assert native.payloads[0] is payload
    assert native.payloads[2] is parts[0]
    assert native.payloads[3] is parts[1]
    assert not any(isinstance(item, memoryview) for item in native.payloads[:4])
    assert first.size == len(payload)
    assert first.etag
    assert multi.size == 13
    assert typed_meta.size == typed.itemsize * 3
    assert mutable == bytearray()
    assert base.get("mut.bin") == b"mutable-payload"
    assert base.get("multi.bin") == b"part-a:part-b"
    assert base.get("typed.bin") == typed.tobytes()


_READ_AHEAD_OBJECT = bytes(range(256)) * 4096


@pytest.mark.parametrize(("read_ahead", "after_reads", "after_seek"), [(4_194_304, 1, 1), (0, 16, 17)])
def test_sync_reader_read_ahead_coalesces_range_calls(
    monkeypatch: pytest.MonkeyPatch, read_ahead: int, after_reads: int, after_seek: int
) -> None:
    """Sixteen 4 KiB reads plus a backward seek share one cached window unless read-ahead is disabled."""
    store = SyncObjectStore(MemoryConfig())
    store.put("blob.bin", _READ_AHEAD_OBJECT)
    calls: list[tuple[int, int]] = []
    original = SyncObjectStore.get_range

    def spy(self: SyncObjectStore, path: str, offset: int, length: int) -> bytes:
        calls.append((offset, length))
        return original(self, path, offset, length)

    monkeypatch.setattr(SyncObjectStore, "get_range", spy)
    with store.open_reader("blob.bin", read_ahead=read_ahead) as reader:
        chunks = [reader.read(4096) for _ in range(16)]
        assert b"".join(chunks) == _READ_AHEAD_OBJECT[:65536]
        assert len(calls) == after_reads
        assert reader.seek(1024) == 1024
        assert reader.read(4096) == _READ_AHEAD_OBJECT[1024:5120]
        assert reader.tell() == 5120
    assert len(calls) == after_seek
    if read_ahead:
        assert calls == [(0, len(_READ_AHEAD_OBJECT))]


@pytest.mark.anyio
@pytest.mark.parametrize(("read_ahead", "after_reads", "after_seek"), [(4_194_304, 1, 1), (0, 16, 17)])
async def test_async_reader_read_ahead_coalesces_range_calls(
    monkeypatch: pytest.MonkeyPatch, read_ahead: int, after_reads: int, after_seek: int
) -> None:
    """AsyncReadableFile serves sub-window reads and backward seeks from one cached range."""
    store = AsyncObjectStore(MemoryConfig())
    await store.put("blob.bin", _READ_AHEAD_OBJECT)
    calls: list[tuple[int, int]] = []
    original = AsyncObjectStore.get_range

    async def spy(self: AsyncObjectStore, path: str, offset: int, length: int) -> bytes:
        calls.append((offset, length))
        return await original(self, path, offset, length)

    monkeypatch.setattr(AsyncObjectStore, "get_range", spy)
    async with store.open_reader("blob.bin", read_ahead=read_ahead) as reader:
        chunks = [await reader.read(4096) for _ in range(16)]
        assert b"".join(chunks) == _READ_AHEAD_OBJECT[:65536]
        assert len(calls) == after_reads
        assert reader.seek(1024) == 1024
        assert await reader.read(4096) == _READ_AHEAD_OBJECT[1024:5120]
    assert len(calls) == after_seek
    if read_ahead:
        assert calls == [(0, len(_READ_AHEAD_OBJECT))]


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_reader_bypasses_cache_for_large_reads_and_refills_on_miss(
    monkeypatch: pytest.MonkeyPatch, asynchronous: bool
) -> None:
    """Reads at least read_ahead long bypass the cache and window misses refetch bounded by EOF."""
    calls: list[tuple[int, int]] = []
    data = bytes(range(256)) * 16
    if asynchronous:
        async_store = AsyncObjectStore(MemoryConfig(namespace="bypass-async"))
        await async_store.put("blob.bin", data)
        async_original = AsyncObjectStore.get_range

        async def async_spy(self: AsyncObjectStore, path: str, offset: int, length: int) -> bytes:
            calls.append((offset, length))
            return await async_original(self, path, offset, length)

        monkeypatch.setattr(AsyncObjectStore, "get_range", async_spy)
        async_reader = async_store.open_reader("blob.bin", size=len(data), read_ahead=1024)
        assert await async_reader.read(1024) == data[:1024]
        assert await async_reader.read(100) == data[1024:1124]
        assert await async_reader.read(50) == data[1124:1174]
        async_reader.seek(len(data) - 10)
        assert await async_reader.read(100) == data[-10:]
    else:
        store = SyncObjectStore(MemoryConfig(namespace="bypass-sync"))
        store.put("blob.bin", data)
        original = SyncObjectStore.get_range

        def spy(self: SyncObjectStore, path: str, offset: int, length: int) -> bytes:
            calls.append((offset, length))
            return original(self, path, offset, length)

        monkeypatch.setattr(SyncObjectStore, "get_range", spy)
        reader = store.open_reader("blob.bin", read_ahead=1024)
        assert reader.read(1024) == data[:1024]
        assert reader.read(100) == data[1024:1124]
        assert reader.read(50) == data[1124:1174]
        reader.seek(len(data) - 10)
        assert reader.read(100) == data[-10:]
    assert calls == [(0, 1024), (1024, 1024), (len(data) - 10, 10)]


@pytest.mark.parametrize("asynchronous", [False, True])
def test_open_reader_rejects_negative_read_ahead(asynchronous: bool) -> None:
    """read_ahead below zero raises ValueError on both reader flavors."""
    store: Any = AsyncObjectStore("memory://") if asynchronous else SyncObjectStore("memory://")
    with pytest.raises(ValueError, match="read_ahead"):
        store.open_reader("missing.bin", size=10, read_ahead=-1)


_WRITER_CHUNK = 1_048_576
_WRITER_TOTAL = 20 * _WRITER_CHUNK


def _writer_chunk(index: int) -> bytes:
    """Return a deterministic 1 MiB chunk distinct per index."""
    return bytes([index % 251]) * _WRITER_CHUNK


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_open_writer_streams_multipart_parts_from_reusable_bytearray(asynchronous: bool) -> None:
    """A 20 MiB stream flushes 8 MiB parts, completes with an etag, and never pins the caller's bytearray."""
    store, base, native = _recording_store(asynchronous)
    scratch = bytearray()
    if asynchronous:
        async with store.open_writer("big.bin", part_size=8_388_608, content_type="application/octet-stream") as wf:
            assert isinstance(wf, AsyncWritableFile)
            assert native.uploads == 0
            for index in range(20):
                scratch += _writer_chunk(index)
                assert await wf.write(scratch) == _WRITER_CHUNK
                scratch.clear()
            assert wf.tell() == _WRITER_TOTAL
    else:
        with store.open_writer("big.bin", part_size=8_388_608, content_type="application/octet-stream") as wf:
            assert isinstance(wf, SyncWritableFile)
            assert native.uploads == 0
            for index in range(20):
                scratch += _writer_chunk(index)
                assert wf.write(scratch) == _WRITER_CHUNK
                scratch.clear()
            assert wf.tell() == _WRITER_TOTAL
    assert wf.closed
    assert isinstance(wf.result, PutResult)
    assert wf.result.etag
    assert wf.result.size == _WRITER_TOTAL
    assert native.uploads == 1
    assert native.parts == [8_388_608, 8_388_608, 4_194_304]
    assert base.get("big.bin") == b"".join(_writer_chunk(index) for index in range(20))
    assert base.head("big.bin").content_type == "application/octet-stream"


def test_sync_writer_supports_gzip_and_buffered_writer() -> None:
    """SyncWritableFile is a RawIOBase usable by gzip.GzipFile and io.BufferedWriter."""
    store = SyncObjectStore(MemoryConfig(namespace="writer-stdlib"))
    payload = b"vertebra-streaming-writer\n" * 50_000

    with store.open_writer("data.gz", part_size=65536) as wf, gzip.GzipFile(fileobj=wf, mode="wb") as gz:
        gz.write(payload)
    assert gzip.decompress(store.get("data.gz")) == payload

    raw = store.open_writer("buffered.bin", part_size=65536)
    with io.BufferedWriter(raw, buffer_size=4096) as buffered:
        buffered.write(payload)
    assert raw.closed
    assert raw.result is not None
    assert raw.result.size == len(payload)
    assert store.get("buffered.bin") == payload


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("part_size", [8_388_608, 4])
async def test_open_writer_aborts_on_exception(asynchronous: bool, part_size: int) -> None:
    """An exception inside the writer context propagates and leaves no object behind."""
    store, base, native = _recording_store(asynchronous)
    wf = store.open_writer("aborted.bin", part_size=part_size)
    with pytest.raises(ValueError, match="boom"):
        if asynchronous:
            async with wf:
                await wf.write(b"partial-payload")
                raise ValueError("boom")
        else:
            with wf:
                wf.write(b"partial-payload")
                raise ValueError("boom")
    assert wf.closed
    assert wf.result is None
    assert not base.exists("aborted.bin")
    assert native.parts == ([] if part_size > 4 else [4, 4, 4])


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_open_writer_explicit_abort_and_closed_writes(asynchronous: bool) -> None:
    """abort() discards the upload and later writes fail as closed-file errors."""
    store, base, _native = _recording_store(asynchronous)
    wf = store.open_writer("dropped.bin", part_size=4)
    if asynchronous:
        await wf.write(b"0123456789")
        await wf.abort()
        with pytest.raises(ValueError, match="closed"):
            await wf.write(b"more")
        await wf.aclose()
    else:
        wf.write(b"0123456789")
        wf.abort()
        with pytest.raises(ValueError, match="closed"):
            wf.write(b"more")
        wf.close()
    assert wf.closed
    assert wf.result is None
    assert not base.exists("dropped.bin")


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_open_writer_zero_bytes_creates_empty_object(asynchronous: bool) -> None:
    """Closing a writer that received no bytes completes a valid zero-byte object."""
    store, base, _native = _recording_store(asynchronous)
    if asynchronous:
        async with store.open_writer("empty.bin") as wf:
            assert await wf.write(b"") == 0
    else:
        with store.open_writer("empty.bin") as wf:
            assert wf.write(b"") == 0
    assert isinstance(wf.result, PutResult)
    assert wf.result.size == 0
    assert base.get("empty.bin") == b""


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_open_writer_if_not_exists_preserves_existing_object(asynchronous: bool) -> None:
    """if_not_exists rejects completion over an existing key and keeps the original payload."""
    store, base, _native = _recording_store(asynchronous)
    base.put("taken.bin", b"original")
    wf = store.open_writer("taken.bin", if_not_exists=True)
    with pytest.raises(FileExistsError):
        if asynchronous:
            async with wf:
                await wf.write(b"replacement")
        else:
            with wf:
                wf.write(b"replacement")
    assert wf.closed
    assert wf.result is None
    assert base.get("taken.bin") == b"original"


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("part_size", [0, -1])
def test_open_writer_rejects_non_positive_part_size(asynchronous: bool, part_size: int) -> None:
    """part_size must be positive on both writer flavors."""
    store: Any = AsyncObjectStore("memory://") if asynchronous else SyncObjectStore("memory://")
    with pytest.raises(ValueError, match="part_size"):
        store.open_writer("bad.bin", part_size=part_size)


class _FailingUpload:
    """Multipart upload double whose part writes raise a configured exception and record every call."""

    def __init__(self, error: BaseException, asynchronous: bool) -> None:
        self.error = error
        self.asynchronous = asynchronous
        self.calls: list[str] = []

    def write_part(self, chunk: Any) -> Any:
        self.calls.append("write_part")
        if self.asynchronous:
            return self._raise_async()
        raise self.error

    def complete(self) -> Any:
        self.calls.append("complete")
        return self.resolve(None)

    def abort(self) -> Any:
        self.calls.append("abort")
        return self.resolve(None)

    def resolve(self, value: Any) -> Any:
        if not self.asynchronous:
            return value

        async def resolved() -> Any:
            return value

        return resolved()

    async def _raise_async(self) -> None:
        raise self.error


class _FailingNative:
    """Native store double that hands out a single failing multipart upload."""

    def __init__(self, upload: _FailingUpload) -> None:
        self.upload = upload

    def create_multipart(self, *args: Any, **kwargs: Any) -> Any:
        return self.upload.resolve(self.upload)


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("asynchronous", "error"),
    [
        pytest.param(False, OSError("part upload failed"), id="sync-error"),
        pytest.param(True, OSError("part upload failed"), id="async-error"),
        pytest.param(True, asyncio.CancelledError(), id="async-cancelled"),
    ],
)
async def test_writer_part_failure_aborts_and_blocks_later_commit(asynchronous: bool, error: BaseException) -> None:
    """A failed or cancelled part write aborts the upload so a later close cannot commit a gapped object."""
    base = SyncObjectStore(MemoryConfig())
    upload = _FailingUpload(error, asynchronous)
    native = _FailingNative(upload)
    store: Any = (
        AsyncObjectStore(MemoryConfig(), _native_instance=native)
        if asynchronous
        else SyncObjectStore(MemoryConfig(), _native_instance=native)
    )
    wf = store.open_writer("failed.bin", part_size=4)
    with pytest.raises(type(error)):
        if asynchronous:
            await wf.write(b"0123456789")
        else:
            wf.write(b"0123456789")
    assert wf.closed
    if asynchronous:
        assert await wf.aclose() is None
    else:
        wf.close()
    assert upload.calls == ["write_part", "abort"]
    assert wf.result is None
    assert not base.exists("failed.bin")


@pytest.mark.anyio
async def test_async_writer_flush_and_aclose_return_put_result() -> None:
    """flush() keeps the tail pending, and aclose() returns the stored PutResult on every call."""
    store = AsyncObjectStore(MemoryConfig(namespace="writer-aclose-result"))
    wf = store.open_writer("done.bin", part_size=4)
    await wf.write(b"abcdef")
    await wf.flush()
    assert wf.tell() == 6
    assert wf.result is None
    result = await wf.aclose()
    assert isinstance(result, PutResult)
    assert result is wf.result
    assert result.size == 6
    assert await wf.aclose() is result
    with pytest.raises(ValueError, match="closed"):
        await wf.flush()
    assert await store.get("done.bin") == b"abcdef"


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_put_stream_normalizes_typed_buffer_chunks(asynchronous: bool) -> None:
    """put_stream uploads typed array chunks as raw bytes, matching put."""
    store, base, native = _recording_store(asynchronous)
    chunks = [array.array("I", [1, 2, 3]), array.array("I", [4, 5])]
    if asynchronous:
        meta = await store.put_stream("typed.bin", chunks)
    else:
        meta = store.put_stream("typed.bin", chunks)
    expected = b"".join(chunk.tobytes() for chunk in chunks)
    assert meta.size == len(expected)
    assert native.parts == [len(chunk.tobytes()) for chunk in chunks]
    assert base.get("typed.bin") == expected


@pytest.mark.anyio
async def test_async_writer_collected_open_emits_resource_warning() -> None:
    """Collecting an open AsyncWritableFile warns about the uncompleted multipart upload."""
    store = AsyncObjectStore(MemoryConfig(namespace="writer-unclosed"))
    wf = store.open_writer("leaked.bin", part_size=4)
    await wf.write(b"0123456789")
    with pytest.warns(ResourceWarning, match="leaked.bin"):
        del wf
        gc.collect()
    assert not await store.exists("leaked.bin")


@pytest.mark.anyio
@pytest.mark.parametrize("finish", ["aclose", "abort"])
async def test_async_writer_collected_after_finish_is_silent(finish: str) -> None:
    """A completed or aborted AsyncWritableFile is collected without a ResourceWarning."""
    store = AsyncObjectStore(MemoryConfig())
    wf = store.open_writer("finished.bin", part_size=4)
    await wf.write(b"0123456789")
    await getattr(wf, finish)()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        del wf
        gc.collect()
    assert not [item for item in caught if issubclass(item.category, ResourceWarning)]


def test_storage_exports_writers_and_precondition_error() -> None:
    """vertebra.storage re-exports the streaming writers, IntegrityError, and PreconditionError in a sorted __all__."""
    assert storage.IntegrityError is IntegrityError
    assert StorageIntegrityError is IntegrityError
    assert storage.PreconditionError is PreconditionError
    assert storage.SyncWritableFile is SyncWritableFile
    assert storage.AsyncWritableFile is AsyncWritableFile
    assert {"AsyncWritableFile", "IntegrityError", "PreconditionError", "SyncWritableFile"} <= set(storage.__all__)
    assert list(storage.__all__) == sorted(storage.__all__)
    assert issubclass(SyncWritableFile, io.RawIOBase)


def _make_concurrent_store(backend: str, tmp_path: Path) -> SyncObjectStore:
    """Create a sync object store backed by either a local directory or an in-memory store."""
    if backend == "file":
        return SyncObjectStore.from_local(tmp_path)
    return SyncObjectStore("memory://")


def _assert_no_staging_files(root: Path) -> None:
    """Assert that no temporary local staging files remain under root."""
    leftover = [path for path in root.rglob("*") if "#" in path.name]
    assert leftover == []


@pytest.mark.parametrize("backend", ["file", "memory"])
@pytest.mark.parametrize("mode", ["if_not_exists", "if_none_match"])
def test_put_stream_create_only_concurrent_writers(backend: str, mode: str, tmp_path: Path) -> None:
    """Concurrent create-only put_stream writers produce one winner, N-1 FileExistsError, and no staging residue."""
    store = _make_concurrent_store(backend, tmp_path)
    worker_count = 8
    barrier = threading.Barrier(worker_count)

    def chunks(idx: int) -> Iterator[bytes]:
        yield f"w{idx}-head-".encode()
        barrier.wait(timeout=10.0)
        yield f"w{idx}-tail".encode()

    def run_worker(idx: int) -> tuple[int, PutResult | None, Exception | None]:
        try:
            result = store.put_stream(
                "nested/race-stream.bin",
                chunks(idx),
                if_not_exists=(mode == "if_not_exists"),
                if_none_match="*" if mode == "if_none_match" else None,
            )
        except Exception as exc:
            return (idx, None, exc)
        else:
            return (idx, result, None)

    with ThreadPoolExecutor(max_workers=worker_count) as pool:
        outcomes = list(pool.map(run_worker, range(worker_count)))

    winners = [(idx, res) for idx, res, exc in outcomes if exc is None and res is not None]
    losers = [exc for _idx, _res, exc in outcomes if exc is not None]
    assert len(winners) == 1
    assert len(losers) == worker_count - 1
    assert all(type(exc) is FileExistsError for exc in losers)
    winner_idx, winner_result = winners[0]
    assert isinstance(winner_result, PutResult)
    expected_payload = f"w{winner_idx}-head-w{winner_idx}-tail".encode()
    assert store.get("nested/race-stream.bin") == expected_payload
    assert winner_result.size == len(expected_payload)
    if backend == "file":
        _assert_no_staging_files(tmp_path)


@pytest.mark.parametrize("backend", ["file", "memory"])
def test_open_writer_create_only_concurrent_writers(backend: str, tmp_path: Path) -> None:
    """Concurrent create-only open_writer sessions produce one winner, clean aborted losers, and no staging residue."""
    store = _make_concurrent_store(backend, tmp_path)
    worker_count = 8
    barrier = threading.Barrier(worker_count)

    def run_worker(idx: int) -> tuple[int, SyncWritableFile, Exception | None]:
        wf = store.open_writer("nested/race-writer.bin", part_size=4, if_not_exists=True)
        try:
            with wf:
                wf.write(f"w{idx:02d}-part1-".encode())
                barrier.wait(timeout=10.0)
                wf.write(f"w{idx:02d}-tail".encode())
        except Exception as exc:
            return (idx, wf, exc)
        else:
            return (idx, wf, None)

    with ThreadPoolExecutor(max_workers=worker_count) as pool:
        outcomes = list(pool.map(run_worker, range(worker_count)))

    winners = [(idx, wf) for idx, wf, exc in outcomes if exc is None]
    losers = [(wf, exc) for _idx, wf, exc in outcomes if exc is not None]
    assert len(winners) == 1
    assert len(losers) == worker_count - 1
    for loser_wf, exc in losers:
        assert type(exc) is FileExistsError
        assert loser_wf.closed is True
        assert loser_wf.result is None
    winner_idx, winner_wf = winners[0]
    assert winner_wf.closed is True
    assert isinstance(winner_wf.result, PutResult)
    expected_payload = f"w{winner_idx:02d}-part1-w{winner_idx:02d}-tail".encode()
    assert store.get("nested/race-writer.bin") == expected_payload
    assert winner_wf.result.size == len(expected_payload)
    if backend == "file":
        _assert_no_staging_files(tmp_path)


@pytest.mark.parametrize("backend", ["file", "memory"])
@pytest.mark.parametrize("file_format", ["parquet", "arrow-ipc"])
def test_sqlspec_write_arrow_sync_overwrite_false_concurrent_writers(
    backend: str, file_format: str, tmp_path: Path
) -> None:
    """Concurrent SQLSpec write_arrow_sync with overwrite=False yields one winner and N-1 FileExistsError."""
    pa = pytest.importorskip("pyarrow")
    pytest.importorskip("sqlspec")
    from vertebra.extensions.sqlspec.storage import ObjectStoreBackend as SQLSpecObjectStoreBackend

    store = _make_concurrent_store(backend, tmp_path)
    backend_store = SQLSpecObjectStoreBackend(store=store)
    worker_count = 6
    barrier = threading.Barrier(worker_count)
    ext = "parquet" if file_format == "parquet" else "arrow"
    key = f"nested/race-sync.{ext}"
    tables = [pa.table({"worker": [idx, idx], "tag": [f"w{idx}", f"w{idx}"]}) for idx in range(worker_count)]

    def run_worker(idx: int) -> tuple[int, Exception | None]:
        barrier.wait(timeout=10.0)
        try:
            backend_store.write_arrow_sync(key, tables[idx], overwrite=False, file_format=file_format)
        except Exception as exc:
            return (idx, exc)
        else:
            return (idx, None)

    with ThreadPoolExecutor(max_workers=worker_count) as pool:
        outcomes = list(pool.map(run_worker, range(worker_count)))

    winners = [idx for idx, exc in outcomes if exc is None]
    losers = [exc for _idx, exc in outcomes if exc is not None]
    assert len(winners) == 1
    assert len(losers) == worker_count - 1
    assert all(type(exc) is FileExistsError for exc in losers)
    stored = backend_store.read_arrow_sync(key, file_format=file_format)
    assert stored.equals(tables[winners[0]])
    if backend == "file":
        _assert_no_staging_files(tmp_path)


@pytest.mark.anyio
@pytest.mark.parametrize("backend", ["file", "memory"])
@pytest.mark.parametrize("file_format", ["parquet", "arrow-ipc"])
async def test_sqlspec_write_arrow_async_overwrite_false_concurrent_writers(
    backend: str, file_format: str, tmp_path: Path
) -> None:
    """Concurrent SQLSpec write_arrow_async with overwrite=False yields one winner and N-1 FileExistsError."""
    pa = pytest.importorskip("pyarrow")
    pytest.importorskip("sqlspec")
    from vertebra.extensions.sqlspec.storage import ObjectStoreBackend as SQLSpecObjectStoreBackend

    store = _make_concurrent_store(backend, tmp_path)
    backend_store = SQLSpecObjectStoreBackend(store=store)
    worker_count = 6
    barrier = asyncio.Barrier(worker_count)
    ext = "parquet" if file_format == "parquet" else "arrow"
    key = f"nested/race-async.{ext}"
    tables = [pa.table({"worker": [idx, idx], "tag": [f"w{idx}", f"w{idx}"]}) for idx in range(worker_count)]

    async def run_worker(idx: int) -> int:
        await asyncio.wait_for(barrier.wait(), timeout=10.0)
        await backend_store.write_arrow_async(key, tables[idx], overwrite=False, file_format=file_format)
        return idx

    outcomes = await asyncio.gather(*(run_worker(idx) for idx in range(worker_count)), return_exceptions=True)

    winners = [item for item in outcomes if isinstance(item, int)]
    losers = [item for item in outcomes if isinstance(item, BaseException)]
    assert len(winners) == 1
    assert len(losers) == worker_count - 1
    assert all(type(exc) is FileExistsError for exc in losers)
    stored = await backend_store.read_arrow_async(key, file_format=file_format)
    assert stored.equals(tables[winners[0]])
    if backend == "file":
        _assert_no_staging_files(tmp_path)
