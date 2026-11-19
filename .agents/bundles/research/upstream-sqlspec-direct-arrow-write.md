---
type: Research
title: Upstream SQLSpec Direct Arrow Write Dispatch
description: Shipped SQLSpec >= 0.72.2 DirectStreamWriteProtocol pipeline dispatch and Vertebra streaming Parquet, Arrow IPC, CSV, overwrite=False, and microbenchmark support.
status: stable
tags:
  - storage
  - arrow
  - sqlspec
  - upstream
generated:
  by: flow-learn
  at: "2026-11-13T16:11:00Z"
verified:
  - by: agent:flow-executor
    at: "2026-11-19T00:25:00Z"
    commit: 6f76f8a7
  - by: agent:flow-learn
    at: "2026-11-13T16:11:00Z"
    commit: 61f0cd0e
sources:
  - id: sqlspec-storage
    resource: src/py/vertebra/extensions/sqlspec/storage.py::ObjectStoreBackend
    title: Vertebra SQLSpec ObjectStoreBackend DirectStreamWriteProtocol and direct Arrow writers
  - id: py-arrow
    resource: src/py/vertebra/storage/_arrow.py::write_batches_sync
    title: Multipart Parquet, Arrow IPC, and CSV batch writer
  - id: bench-micro
    resource: tools/benchmarks/micro.py::benchmark_arrow_storage_pipeline
    title: Direct streaming vs buffered Arrow storage microbenchmark
  - id: tests-sqlspec-storage
    resource: src/py/tests/unit/extensions/test_data_storage_integrations.py
    title: SQLSpec Arrow pipeline format, overwrite=False, cancellation, and benchmark tests
---

# Upstream SQLSpec Direct Arrow Write Dispatch

<!-- truth: start -->
- Upstream `sqlspec>=0.72.2` ships `DirectStreamWriteProtocol` (`supports_direct_arrow_write(file_format, *, overwrite=True)`, `open_write_stream_sync`, `open_write_stream_async`) and dispatches through `supports_direct_stream_write(backend)` in `SyncStoragePipeline.write_arrow` and `AsyncStoragePipeline.write_arrow` [^sqlspec-storage].
- When `supports_direct_arrow_write` returns `True`, SQLSpec streams Arrow tables and batch iterators through `encode_arrow_to_sink` and `encode_arrow_to_async_sink` without allocating a whole-table byte payload [^sqlspec-storage].
- `vertebra.extensions.sqlspec.storage.ObjectStoreBackend` implements `DirectStreamWriteProtocol` for `parquet`, `arrow-ipc` (`arrow`, `ipc`, `feather`), and `csv`, forwarding `overwrite=False` as conditional `if_not_exists=True` multipart uploads [^sqlspec-storage] [^py-arrow].
- Direct backend methods (`write_arrow_sync`, `write_arrow_async`, `write_arrow_cursor_sync`, `write_arrow_cursor_async`) delegate to `write_batches_sync` and `write_batches_async`, preserving `overwrite=False` and digest verification options [^sqlspec-storage] [^py-arrow].
- `tools.benchmarks.micro.benchmark_arrow_storage_pipeline` compares `SyncStoragePipeline.write_arrow` streaming sinks against buffered `encode_arrow_payload` plus `write_bytes_sync`, reporting operations/sec and `tracemalloc` peak KiB [^bench-micro] [^tests-sqlspec-storage].
<!-- truth: end -->

## Shipped Upstream Dispatch (`sqlspec>=0.72.2`)

Upstream `sqlspec>=0.72.2` replaces whole-table Arrow byte buffering on streaming-capable backends with `DirectStreamWriteProtocol`. Before allocating a `pyarrow.BufferOutputStream`, `SyncStoragePipeline.write_arrow` and `AsyncStoragePipeline.write_arrow` evaluate `supports_direct_stream_write(backend)` and `backend.supports_direct_arrow_write(format_choice, overwrite=overwrite)` [^sqlspec-storage]:

1. When `True`, the pipeline opens `backend.open_write_stream_sync(path, overwrite=overwrite)` or `backend.open_write_stream_async(path, overwrite=overwrite)` and streams encoded chunks via `encode_arrow_to_sink` or `encode_arrow_to_async_sink`.
2. `write_arrow_batches` and `write_arrow_batches_async` use the same streaming byte sink for `RecordBatchReader` and batch-iterator sources, aborting the upload on mid-stream errors or task cancellation [^tests-sqlspec-storage].
3. When `False`, the pipeline falls back to `encode_arrow_payload` (`_encode_arrow_source_to_bytes`) followed by byte-oriented `write_bytes_sync` or `write_bytes_async`.

## Vertebra Format, Conditional Write, and Benchmark Support

`ObjectStoreBackend` implements `supports_direct_arrow_write(file_format, *, overwrite=True)` returning `True` across `parquet`, `arrow`, `ipc`, `arrow-ipc`, `feather`, and `csv` [^sqlspec-storage]. `open_write_stream_sync` and `open_write_stream_async` return `SyncWritableFile` and `AsyncWritableFile` handles with `if_not_exists=True` whenever `overwrite=False`, raising `FileExistsError` on destination conflicts without clobbering existing objects [^sqlspec-storage] [^tests-sqlspec-storage].

Direct backend calls (`write_arrow_sync`, `write_arrow_async`, `write_arrow_cursor_sync`, `write_arrow_cursor_async`) and `vertebra.storage._arrow` (`normalize_arrow_format`, `write_batches_*`, `iter_batches_*`, `read_table_*`) also support `csv` (`text/csv`, `pyarrow.csv`), `overwrite=False` (`if_not_exists=True`), and `__arrow_c_stream__` sources [^py-arrow]. `benchmark_arrow_storage_pipeline` in `tools/benchmarks/micro.py` measures steady-state throughput and `tracemalloc` peak KiB for direct streaming versus buffered `encode_arrow_payload` writes over local storage, skipping when optional `pyarrow`, `sqlspec`, or storage extras are absent [^bench-micro] [^tests-sqlspec-storage].

[^sqlspec-storage]: `src/py/vertebra/extensions/sqlspec/storage.py::ObjectStoreBackend` — Vertebra SQLSpec `ObjectStoreBackend` `DirectStreamWriteProtocol` and direct Arrow writers.
[^py-arrow]: `src/py/vertebra/storage/_arrow.py::write_batches_sync` — Multipart Parquet, Arrow IPC, and CSV batch writer.
[^bench-micro]: `tools/benchmarks/micro.py::benchmark_arrow_storage_pipeline` — Direct streaming vs buffered Arrow storage microbenchmark.
[^tests-sqlspec-storage]: `src/py/tests/unit/extensions/test_data_storage_integrations.py` — SQLSpec Arrow pipeline format, `overwrite=False`, cancellation, and benchmark tests.
