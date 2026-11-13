---
type: Spec
flow_id: storage-integrity-and-streaming
chapter_id: ch02-streaming-arrow-writes-and-integrations
title: "Chapter 02: Streaming Arrow Multipart Writes, Ecosystem Adapters, and Checksum Benchmark"
description: "Threads content-integrity digests and bounded-memory Arrow/Parquet multipart streaming across vertebra.storage, SQLSpec, Advanced Alchemy, and Django storage backends, and adds the put_checksum Criterion benchmark."
status: stable
state: planned
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
depends_on_chapters:
  - ch01-integrity-receipts-and-verified-reads
plan_commit: 18baadcd
updated_at: "2026-11-12T23:55:00Z"
generated:
  by: flow-plan
  at: "2026-11-12T23:55:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T23:55:00Z"
    commit: 18baadcd
tags:
  - storage
  - arrow
  - sqlspec
  - benchmark
sources:
  - id: py-arrow
    resource: src/py/vertebra/storage/_arrow.py::write_batches_sync
    title: "Incremental Arrow and Parquet multipart upload codecs"
  - id: sqlspec-backend
    resource: src/py/vertebra/extensions/sqlspec/storage.py::ObjectStoreBackend
    title: "SQLSpec ObjectStoreBackend write_arrow_sync/async and write_bytes_sync/async"
  - id: aa-backend
    resource: src/py/vertebra/extensions/alchemy/storage.py::ObjectStoreBackend
    title: "Advanced Alchemy ObjectStoreBackend save_object and save_object_async"
  - id: django-storage
    resource: src/py/vertebra/extensions/django/storage.py::DjangoStorage
    title: "Django Storage _save integration"
  - id: core-cargo
    resource: src/rs/vertebra-core/Cargo.toml
    title: "Criterion workspace dev-dependency and bench target precedent"
---

# Chapter 02: Streaming Arrow Multipart Writes, Ecosystem Adapters, and Checksum Benchmark

<!-- truth: start -->
- **Baseline (`18baadcd`)**: `write_batches_sync` and `write_batches_async` do not forward `checksum` or `expected_*` digests [^py-arrow]; SQLSpec `ObjectStoreBackend` lacks `supports_direct_arrow_write: ClassVar[bool] = True` ([#100](https://github.com/cofin/vertebra/issues/100)) [^sqlspec-backend]; Advanced Alchemy and Django adapters omit digest forwarding and streaming Arrow save helpers [^aa-backend] [^django-storage]; and `vertebra-storage` has no Criterion checksum benchmark [^core-cargo].
- **Target Architecture**: `write_batches_sync/async` stream Arrow/Parquet chunks through `put_stream` with optional `checksum`/`expected_*` receipts; SQLSpec `ObjectStoreBackend` advertises `supports_direct_arrow_write = True`, normalizes pipeline write options, and aborts multipart uploads on mid-stream codec failure; Advanced Alchemy adds `save_arrow`/`save_arrow_async` + digest forwarding and Django `_save` honors `content.{sha256,crc32c,md5}`; and `benches/put_checksum.rs` proves zero-cost default writes.
- **Scope & Boundaries**: Covers `R4` and the benchmark half of `R2` across `storage/_arrow.py`, `extensions/{sqlspec,alchemy,django}/storage.py`, and `vertebra-storage/benches/` (`3` compact tasks). Zero `.venv` paths are referenced; upstream SQLSpec pipeline changes remain tracked in `ch03:t03`.
<!-- truth: end -->

## 1. Specification, Context & Provenance

Although `vertebra.storage._arrow` streams Arrow IPC and Parquet batches incrementally via `put_stream` [^py-arrow], SQLSpec pipelines require an explicit backend capability marker (`supports_direct_arrow_write: ClassVar[bool] = True`) and normalized `compression` / `format_hint` / `write_options` keyword handling to invoke `write_arrow_sync` / `write_arrow_async` directly ([#100](https://github.com/cofin/vertebra/issues/100)) [^sqlspec-backend]. Likewise, Advanced Alchemy and Django storage adapters must forward expected digests from `ch01` without ever calling `asyncio.to_thread` on the async storage plane (`PYO3-01`) [^aa-backend] [^django-storage]. Guidance: [object-storage.md](../../../../knowledge/domains/storage/object-storage.md) and [sqlspec-alchemy-and-dialects.md](../../../../knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md).

## 2. Requirements, Concrete Contracts & Architecture

```python
# src/py/vertebra/storage/_arrow.py
def write_batches_sync(
    store: SyncObjectStore,
    path: str,
    data: Any,
    *,
    format: str | None = None,
    checksum: bool = False,
    expected_sha256: str | None = None,
    expected_crc32c: str | None = None,
    expected_md5: str | None = None,
    **codec_options: Any,
) -> PutResult: ...

# src/py/vertebra/extensions/sqlspec/storage.py
class ObjectStoreBackend:
    supports_direct_arrow_write: ClassVar[bool] = True

# src/py/vertebra/extensions/alchemy/storage.py
class ObjectStoreBackend:
    def save_arrow(self, path: str, data: Any, *, format: str | None = None, checksum: bool = False, expected_sha256: str | None = None, **codec_options: Any) -> PutResult: ...
    async def save_arrow_async(self, path: str, data: Any, *, format: str | None = None, checksum: bool = False, expected_sha256: str | None = None, **codec_options: Any) -> PutResult: ...
```

- **Streaming & Digest Contracts**:
  - `write_batches_sync` / `write_batches_async`: pass `checksum`, `expected_sha256`, `expected_crc32c`, and `expected_md5` into `store.put_stream(...)` and return `PutResult`; any exception raised by a `RecordBatchReader` or codec mid-stream aborts the underlying multipart session and leaves prior objects untouched.
  - SQLSpec `ObjectStoreBackend`: set `supports_direct_arrow_write: ClassVar[bool] = True`; map `compression`, `format_hint`, and `write_options` in `_normalize_arrow_write_options`; pass `checksum` and `expected_{sha256,crc32c,md5}` through `_write_options` on `write_bytes_sync/async` and `write_arrow_sync/async`, and pass `expected_sha256` on `read_bytes_sync/async`.
  - Advanced Alchemy `ObjectStoreBackend` & `DjangoStorage`: forward `checksum` / `expected_{sha256,crc32c,md5}` on `save_object` / `save_object_async`, expose streaming `save_arrow` / `save_arrow_async` over native async/sync stores (zero `to_thread`), and forward `getattr(content, "sha256", None)` (`crc32c`, `md5`) in `DjangoStorage._save`, surfacing `vertebra.typing.IntegrityError` unmodified.
  - Criterion Benchmark (`benches/put_checksum.rs`): groups `put_memory_default/{1MiB,64MiB}` and `put_memory_checksum/{1MiB,64MiB}` with `Throughput::Bytes`.

## 3. Documentation & OKF Knowledge Plan

Arrow streaming, digest forwarding, and the upstream SQLSpec hook note are documented in `ch03` (`t02`–`t03`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Chapter / Task | Primary Files | Verification |
| --- | --- | --- | --- |
| `R4` (`_arrow.py` digest pass-through + SQLSpec `supports_direct_arrow_write` & abort safety) | `t01` | `src/py/vertebra/{storage/_arrow,extensions/sqlspec/storage}.py` | `pytest test_storage_store.py test_data_storage_integrations.py -k "arrow or sha256"` |
| `R4` (Advanced Alchemy `save_arrow*` + digest forwarding & Django `content.sha256`) | `t02` | `src/py/vertebra/extensions/{alchemy,django}/storage.py` | `pytest test_data_storage_integrations.py -k "alchemy or django or sha256"` |
| `R2` (Criterion `put_checksum` benchmark) | `t03` | `src/rs/vertebra-storage/{Cargo.toml,benches/put_checksum.rs}` | `cargo test -p vertebra-storage --benches` |

## 5. Implementation Plan

- [ ] [t01-sqlspec-direct-arrow-and-stream-safety.md](tasks/t01-sqlspec-direct-arrow-and-stream-safety.md) — Thread digest options through `vertebra.storage._arrow`, set `supports_direct_arrow_write = True` on SQLSpec `ObjectStoreBackend`, and prove mid-stream abort safety and bounded memory.
- [ ] [t02-alchemy-and-django-arrow-and-digest-integrations.md](tasks/t02-alchemy-and-django-arrow-and-digest-integrations.md) — Add streaming `save_arrow`/`save_arrow_async` and digest forwarding to Advanced Alchemy `ObjectStoreBackend` and `DjangoStorage._save`.
- [ ] [t03-storage-checksum-criterion-benchmark.md](tasks/t03-storage-checksum-criterion-benchmark.md) — Add Criterion `put_checksum` benchmark comparing default vs checksum `1 MiB` and `64 MiB` memory writes.

## 6. Current State & Remaining Plan Snapshot

- **Fast Chapter Gate (`< 25s`)**:
  `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-storage --benches && uv run --no-sync ruff check src/py/vertebra/storage/_arrow.py src/py/vertebra/extensions/{sqlspec,alchemy,django}/storage.py && uv run --no-sync mypy src/py/vertebra/storage/_arrow.py src/py/vertebra/extensions/{sqlspec,alchemy,django}/storage.py && uv run --no-sync pytest src/py/tests/unit/extensions/test_data_storage_integrations.py -q`

[^py-arrow]: `src/py/vertebra/storage/_arrow.py::write_batches_sync` — Incremental Arrow and Parquet multipart upload codecs.
[^sqlspec-backend]: `src/py/vertebra/extensions/sqlspec/storage.py::ObjectStoreBackend` — SQLSpec `ObjectStoreBackend` `write_arrow_sync`/`async` and `write_bytes_sync`/`async`.
[^aa-backend]: `src/py/vertebra/extensions/alchemy/storage.py::ObjectStoreBackend` — Advanced Alchemy `ObjectStoreBackend` `save_object` and `save_object_async`.
[^django-storage]: `src/py/vertebra/extensions/django/storage.py::DjangoStorage` — Django `Storage` `_save` integration.
[^core-cargo]: `src/rs/vertebra-core/Cargo.toml` — Criterion workspace dev-dependency and bench target precedent.
