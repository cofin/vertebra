---
type: Research
title: Upstream SQLSpec Direct Arrow Write Dispatch
description: Contract proposal for SQLSpec pipeline dispatch on supports_direct_arrow_write to stream Arrow tables without full payload buffering.
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
  - by: agent:flow-learn
    at: "2026-11-13T16:11:00Z"
    commit: 61f0cd0e
sources:
  - id: sqlspec-storage
    resource: src/py/vertebra/extensions/sqlspec/storage.py::ObjectStoreBackend
    title: Vertebra SQLSpec ObjectStoreBackend capability flag and direct Arrow writers
  - id: py-arrow
    resource: src/py/vertebra/storage/_arrow.py::write_batches_sync
    title: Bounded multipart Arrow and Parquet batch writer
---

# Upstream SQLSpec Direct Arrow Write Dispatch

<!-- truth: start -->
- `vertebra.extensions.sqlspec.storage.ObjectStoreBackend` declares `supports_direct_arrow_write = True` and implements `write_arrow_sync` and `write_arrow_async` over bounded multipart batch uploads [^sqlspec-storage] [^py-arrow].
- Upstream `sqlspec` `0.71.1` `SyncStoragePipeline.write_arrow` and `AsyncStoragePipeline.write_arrow` serialize full `pyarrow.Table` payloads in memory before calling `write_bytes_*`; inspecting `supports_direct_arrow_write` upstream eliminates intermediate buffering [^sqlspec-storage].
<!-- truth: end -->

## Problem Statement

Vertebra `vertebra.extensions.sqlspec.storage.ObjectStoreBackend` exposes `supports_direct_arrow_write = True` alongside `write_arrow_sync` and `write_arrow_async`, which stream Arrow record batches into native multipart storage uploads via `write_batches_sync` and `write_batches_async` [^sqlspec-storage] [^py-arrow]. As of SQLSpec `0.71.1`, upstream `SyncStoragePipeline.write_arrow` and `AsyncStoragePipeline.write_arrow` still serialize the full `pyarrow.Table` into an in-memory buffer and invoke byte-oriented `write_bytes_*` rather than checking for direct Arrow write capability on the backend [^sqlspec-storage].

## Proposed Upstream Pipeline Dispatch

Upstream `SyncStoragePipeline.write_arrow` and `AsyncStoragePipeline.write_arrow` should inspect `getattr(backend, "supports_direct_arrow_write", False)` before allocating an intermediate Parquet or Arrow IPC byte payload:

1. Check `bool(getattr(backend, "supports_direct_arrow_write", False))` on the configured storage backend.
2. When `True`, forward the target path (`destination`), `table`, `format_hint`, `compression`, `write_options`, and integrity options (`checksum`, `expected_sha256`, `expected_crc32c`, `expected_md5`, or backend-supported kwargs) directly to `backend.write_arrow_sync(...)` or `await backend.write_arrow_async(...)` [^sqlspec-storage].
3. When `False` (or when `supports_direct_arrow_write` is absent), preserve existing in-memory Arrow serialization and byte upload fallback behavior for non-streaming backends.

## Backward Compatibility and Integration Notes

Because Vertebra `ObjectStoreBackend` already maps `format_hint`, `compression`, `write_options`, and integrity kwargs into `_prepare_arrow_write` and delegates directly to `write_batches_sync` / `write_batches_async`, shipping this dispatch check upstream eliminates whole-table memory spikes without changing caller signatures in Vertebra or existing SQLSpec storage pipelines [^sqlspec-storage] [^py-arrow].

[^sqlspec-storage]: `src/py/vertebra/extensions/sqlspec/storage.py::ObjectStoreBackend` — Vertebra SQLSpec `ObjectStoreBackend` capability flag and direct Arrow writers.
[^py-arrow]: `src/py/vertebra/storage/_arrow.py::write_batches_sync` — Bounded multipart Arrow and Parquet batch writer.
