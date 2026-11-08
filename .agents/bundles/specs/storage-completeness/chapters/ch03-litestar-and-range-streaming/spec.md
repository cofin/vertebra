---
type: Spec
parent_flow: storage-completeness
chapter_id: ch03-litestar-and-range-streaming
title: "Chapter 03: HTTP Range Streaming and Litestar Store/Static Integration"
description: Implement RFC 9110 single-range 206/416 StreamingStorageResponse and Litestar AsyncObjectStore Store plus static-files filesystem adapter.
status: stable
state: active
planning_status: complete
depends_on_chapters:
  - ch01-native-engine-and-auth
replan_trigger: null
current_task: null
plan_revision: 8
plan_commit: 222cbf99
state_revision: 1
last_verified_checkpoint: 99317bed609e62a73d8b0036d444c1268ac2eb1d
last_reviewed_commit: null
created_at: "2026-11-07T04:15:00Z"
updated_at: "2026-11-08T02:25:35Z"
generated:
  by: flow-setup
  at: "2026-11-07T04:15:00Z"
verified:
  - by: agent:flow-setup
    at: "2026-11-07T04:15:00Z"
    commit: cdb978d3
sources:
  - id: streaming-py
    resource: src/py/vertebra/storage/_streaming.py::StreamingStorageResponse
    title: Zero-copy HTTP streaming response over SyncObjectStore and AsyncObjectStore
  - id: litestar-store
    resource: src/py/vertebra/extensions/litestar/store.py::MemoryStore
    title: Litestar Store adapter module hosting key-value and session stores
  - id: litestar-init
    resource: src/py/vertebra/extensions/litestar/__init__.py
    title: Litestar extension public exports
tags:
  - storage
  - litestar
  - http
  - streaming
  - zero-copy
---

# Chapter 03: HTTP Range Streaming and Litestar Store/Static Integration

<!-- truth: start -->
- **RFC 9110 Single-Range Semantics**: `StreamingStorageResponse` parses `Range: bytes=<start>-<end>`, `bytes=<start>-`, and `bytes=-<suffix>`, returning `200 OK` without a `Range` header, `206 Partial Content` with `Content-Range: bytes <start>-<end>/<total>` and `Accept-Ranges: bytes` on valid single ranges, and `416 Range Not Satisfiable` with `Content-Range: bytes */<total>` and empty body on out-of-bounds, inverted, or multi-range byte specs [^streaming-py].
- **Non-Blocking Async Fastpath**: `StreamingStorageResponse` streams via `AsyncObjectStore.get_stream` / `get_range` without blocking the event loop on `SyncObjectStore` calls [^streaming-py].
- **Litestar Store & Static Adapter**: `ObjectStoreLitestarStore` implements `litestar.stores.base.Store` (`get`, `set`, `delete`, `delete_all`, `exists`, `expires_in`) over `AsyncObjectStore` with persistent versioned TTL envelopes plus `vertebra-expires-at-ms` attributes, and `ObjectStoreFileSystemAdapter` implements Litestar's async static-file reader contract (`info`, `open`) [^litestar-store] [^litestar-init].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

`src/py/vertebra/storage/_streaming.py::StreamingStorageResponse` lacks RFC 9110 `Range` header parsing and `416 Range Not Satisfiable` handling and currently requires explicit integer offsets or blocks when given an `AsyncObjectStore` on `to_fastpath_tuple` [^streaming-py]. Meanwhile, `src/py/vertebra/extensions/litestar/store.py` provides `MemoryStore` and `ResponseCacheStore` but no `AsyncObjectStore`-backed `Store` or static-files adapter for serving assets from GCS/S3/R2/Azure/OCI [^litestar-store]. Chapter 03 completes HTTP byte-range streaming and Litestar storage integration.

### 1.2 Non-Goals

- Multipart `multipart/byteranges` responses (multi-range headers return `416` or fall back per single-range contract).
- Distributed pub/sub channels in `extensions/litestar/channels.py` (owned by `queues-tasks-and-channels`).

### 1.3 Upstream Baseline & Code Analysis

Parent roadmap: [spec.md](../../spec.md); depends on [ch01-native-engine-and-auth](../ch01-native-engine-and-auth/spec.md). Skills: [python-framework](../../../../../skills/python-framework/SKILL.md) (plus external `litestar` plugin skill). Knowledge: [litestar.md](../../../../knowledge/domains/extensions/litestar.md).

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Chapter Requirements

- **R7.1 (`ch03:t01`)**: Add `ByteRange` / `parse_byte_range(range_header: str | None, total_size: int)` and `StreamingStorageResponse.from_sync_store(...) / from_async_store(..., range_header: str | None = None)` in `src/py/vertebra/storage/_streaming.py`, supporting `200 OK`, `206 Partial Content` (`Content-Range: bytes <start>-<end>/<total>`, `Accept-Ranges: bytes`, exact `Content-Length`), and `416 Range Not Satisfiable` (`Content-Range: bytes */<total>`, `Content-Length: 0`, empty body), plus async `to_async_fastpath_tuple` / `aiter_chunks` over `AsyncObjectStore.get_stream` and `get_range`.
- **R7.2 (`ch03:t02`)**: Add `ObjectStoreLitestarStore(_StoreBase)` (`set`, `get`, `delete`, `delete_all`, `exists`, `expires_in`, `with_namespace`) and `ObjectStoreFileSystemAdapter` (`info`, `open`) over `AsyncObjectStore` in `src/py/vertebra/extensions/litestar/store.py` and export both in `src/py/vertebra/extensions/litestar/__init__.py`.


The Store uses the fixed private `vertebra-store/` prefix, hierarchical validated namespace segments, and one UTF-8 hex-encoded segment per logical key. Parent bulk deletion includes descendants and preserves sibling prefixes and unrelated objects. Native listed paths and continuation cursors are unquoted before reuse so escaped `%7E` retains logical `~` key identity across deletion pages. Every write carries the versioned TTL envelope; malformed/unsupported envelopes raise `ValueError`. Renewal rewrites only live expiring entries; missing, expired, and immortal entries return `None` from `expires_in`, while live entries return nonnegative whole seconds.

Mount the filesystem adapter with `create_static_files_router(path="/assets", directories=["/"], file_system=adapter, resolve_symlinks=False)`. It maps virtual-root paths beneath its configured base prefix, rejects parent segments, resolves directories via bounded listing, and reuses `AsyncObjectStore.open_reader` (async read/context manager, synchronous seek/tell). Static GET/HEAD acceptance is separate from Range acceptance: a test handler wraps t01 `aiter_chunks()` in Litestar `Stream`, preserving response status and headers and explicitly converting chunks to bytes. Both adapters export through TYPE_CHECKING imports, `__all__`, and `_EXPORT_MAP`; no new response bridge API is added.

### 2.2 Edge-Case & Failure-Mode Matrix

| Input | Expected Behavior |
| --- | --- |
| `Range: bytes=0-15` on 100-byte object | `206 Partial Content`, `Content-Range: bytes 0-15/100`, `Content-Length: 16`, 16-byte slice. |
| `Range: bytes=-10` on 100-byte object | `206 Partial Content`, `Content-Range: bytes 90-99/100`, `Content-Length: 10`, trailing 10 bytes. |
| `Range: bytes=100-120` or `bytes=50-20` or `bytes=-0` on 100-byte object | `416 Range Not Satisfiable`, `Content-Range: bytes */100`, `Content-Length: 0`, body `b""`. |
| Expired key in `ObjectStoreLitestarStore.get(key)` | Deletes expired object lazily, returns `None`, and `expires_in(key)` returns `None`; live subsecond expiry returns `0`. Renewal leaves immortal entries unchanged. |
| `ObjectStoreLitestarStore(None)` | Raises `ImproperConfigurationError`. |

### 2.3 Alternatives Comparison

| Decision | Chosen Seam | Rejected Alternative |
| --- | --- | --- |
| TTL persistence in `ObjectStoreLitestarStore` | Always write `b"VTBSTORE\x01"` plus centralized MessagePack `(expires_at_ms, payload)`; also write `vertebra-expires-at-ms` attributes. The validated envelope remains authoritative when backends strip metadata | Maintaining an in-process dict of TTLs — loses expiration across multi-worker processes and restarts. |

## 3. Documentation & OKF Knowledge Plan

Progress: `[2/2 files documented in ch03]`. `.agents/bundles/knowledge/domains/extensions/litestar.md` documents the implemented Store, virtual filesystem, lazy exports, and explicit Litestar Stream response boundary. `.agents/bundles/knowledge/domains/storage/object-storage.md` documents the completed Range/native streaming and object-store consumer contracts. The storage knowledge target is already included in the root documentation manifest. Consumer guides remain owned by `ch05:t02`.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test Gate |
| --- | --- | --- | --- |
| R7.1 | [t01](tasks/t01-range-streaming-response.md) | `behavior_tdd` | `src/py/tests/unit/server/test_zero_copy_buffer.py`, `src/py/tests/unit/storage/test_storage_store.py` |
| R7.2 | [t02](tasks/t02-litestar-store-and-static-adapter.md) | `behavior_tdd` | `src/py/tests/unit/extensions/test_litestar_plugin.py` |

## 5. Implementation Plan

- [x] [a0888371a54253624c77c4b817875f9878f54b28] storage-completeness:ch03:t01 — Implement RFC 9110 Range/206/416 StreamingStorageResponse and async chunk/fastpath delivery (tasks/t01-range-streaming-response.md)
- [x] [99317bed609e62a73d8b0036d444c1268ac2eb1d] storage-completeness:ch03:t02 — Implement ObjectStoreLitestarStore and ObjectStoreFileSystemAdapter in extensions.litestar (tasks/t02-litestar-store-and-static-adapter.md)

**Chapter Recovery Checkpoint:** `uv run --no-sync pytest src/py/tests/unit/server/test_zero_copy_buffer.py src/py/tests/unit/extensions/test_litestar_plugin.py -q && uv run --no-sync ruff check src/py/vertebra/storage/_streaming.py src/py/vertebra/extensions/litestar` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Verified Checkpoint**: Range/FastPath consumers pass 51 canonical cases at `a0888371`; Litestar source `99317bed609e62a73d8b0036d444c1268ac2eb1d` passes 21 canonical and 634 extension/storage cases, focused MyPy, Ruff, and format checks. Chapter state is `active` pending reviews.
- **Remaining Tasks in Chapter**: None (`2/2` tasks complete). Chapter remains `active` until independent correctness review and fresh quality review pass at the implemented source checkpoint.
- **Active Invariants**: Zero blocking `SyncObjectStore` I/O inside async Litestar handlers; `StorageBuffer` views preserved end-to-end.

[^streaming-py]: `src/py/vertebra/storage/_streaming.py::StreamingStorageResponse` — Zero-copy storage HTTP response builder.
[^litestar-store]: `src/py/vertebra/extensions/litestar/store.py::MemoryStore` — Litestar `Store` implementations.
[^litestar-init]: `src/py/vertebra/extensions/litestar/__init__.py` — Litestar extension package exports.
