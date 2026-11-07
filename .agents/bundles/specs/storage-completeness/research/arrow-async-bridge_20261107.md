---
type: Research
title: Bounded PyArrow and Native Async I/O Bridge
description: Ground synchronous PyArrow callbacks, sparse Parquet reads, per-batch worker jobs, and cancellation ownership before implementation.
status: draft
created_at: '2026-11-07T12:00:00Z'
updated_at: '2026-11-07T12:00:00Z'
generated: {by: flow/0.28.0, at: '2026-11-07T12:00:00Z'}
sources:
  - {id: facade, resource: 'src/py/vertebra/storage/_store.py::AsyncObjectStore', title: Native async range and multipart facade}
  - {id: cursor, resource: 'src/py/vertebra/db/arrow.py::ArrowCursor', title: Current ArrowCursor signatures}
  - {id: parquet, resource: 'https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ParquetFile.html', title: PyArrow ParquetFile metadata and batch APIs}
  - {id: ipc, resource: 'https://arrow.apache.org/docs/python/generated/pyarrow.ipc.RecordBatchStreamReader.html', title: PyArrow native IPC stream/dictionary reader}
  - {id: asyncio, resource: 'https://docs.python.org/3/library/asyncio-task.html#asyncio.run_coroutine_threadsafe', title: Owning-loop coroutine scheduling from workers}
tags: [storage, arrow, parquet, streaming, async]
---

# Bounded PyArrow and Native Async I/O Bridge

<!-- truth: start -->
PyArrow codecs expect synchronous file callbacks; passing an async reader to them cannot make callbacks await native I/O. [^parquet] [^ipc]
Private per-batch worker jobs bridge callbacks onto the owning event loop; the public storage facade contains no asyncio.to_thread. [^facade] [^asyncio]
The bridge owns readers/uploads and cancellation cleanup, while the supplied AsyncObjectStore remains caller-owned. [^facade]
<!-- truth: end -->

## Source-Grounded Probe

The worktree's Python3.12 environment supplies PyArrow26.0.1. A throwaway in-memory probe serializes250rows (`id`,`payload`) with row_group_size100. The encoded file is2820bytes and footer metadata994bytes, with three groups100/100/50. `pq.read_metadata(pa.BufferReader(b"PAR1" + footer_and_trailer))` retains original file column offsets.

A seekable sparse reader containing only the4byte header, exact footer/trailer and the selected `id` column's compressed range successfully supports `pq.ParquetFile(reader, metadata=metadata, pre_buffer=False).read_row_group(1, columns=["id"])`. The result contains100rows beginning IDs100 and101. Column range starts at the minimum valid dictionary/data page offset and ends after total_compressed_size. The probe confirms PyArrow can decode against original sparse offsets; no custom Parquet/FlatBuffer parser is required. [^parquet]

Local signatures confirm `ipc.read_record_batch(obj,schema,dictionary_memo=None)` requires populated dictionary state; manually framing async IPC would introduce a broader custom protocol. Keep the native RecordBatchStreamReader and its dictionary/replacement semantics inside the bounded synchronous worker boundary. [^ipc]

## Chosen Deep Boundary

`storage/_arrow.py` owns codecs, per-reader/per-writer state and worker jobs. `AsyncRangeBridge` captures the explicit owning loop and AsyncObjectStore; synchronous worker read callbacks schedule native get_range on that loop with run_coroutine_threadsafe. Cache capacity is8MiB. A required consumer boundary may copy requested range bytes; there is no whole encoded-input-file prefetch, mmap spool or batch-list materialization. [^facade] [^asyncio]

Each async iterator request submits exactly one next-batch worker job. On return the worker is free; a paused consumer retains reader state without reserving a blocked worker thread. Async writers submit one schema/batch/close job at a time; callback uploads await actual native multipart completion and enforce backpressure. Iterator/read conveniences can materialize requested output Table data, which is separate from preloading encoded input.

Cancellation stops new I/O/batches, closes local callback state, tracks/cancels pending native futures and awaits the worker plus reader/abort cleanup before returning. A foreign event loop fails explicitly before I/O. Store ownership stays with the caller; helpers close only readers/uploads they create. Root architecture adjudication selects this boundary; ch17–ch19 worksheets specify implementation and deterministic negative/lifetime tests.

## Rejected Shapes

A synchronous PyArrow reader called on the event loop can block native network callbacks. Direct AsyncReadableFile callbacks do not satisfy PyArrow's synchronous contract. A permanently blocked worker/producer queue leaks capacity when a consumer pauses. Hand-written Arrow/Parquet parsing broadens scope and dictionary risk. Whole-file BufferOutputStream/list conversion loses bounded streaming.

[^facade]: `src/py/vertebra/storage/_store.py::SyncObjectStore`, `AsyncObjectStore`, native multipart and readable-file helpers.
[^cursor]: `src/py/vertebra/db/arrow.py::ArrowCursor.from_rows(rows, columns=None, *, threshold_rows=...)` and `to_arrow_table`.
[^parquet]: [PyArrow ParquetFile](https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ParquetFile.html) and the local PyArrow26.0.1 sparse-reader probe.
[^ipc]: [PyArrow RecordBatchStreamReader](https://arrow.apache.org/docs/python/generated/pyarrow.ipc.RecordBatchStreamReader.html) and local API signatures.
[^asyncio]: [asyncio cross-thread coroutine scheduling](https://docs.python.org/3/library/asyncio-task.html#asyncio.run_coroutine_threadsafe).
