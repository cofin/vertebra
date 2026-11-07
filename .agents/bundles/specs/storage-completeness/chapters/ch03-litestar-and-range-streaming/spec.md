---
type: Spec
parent_flow: storage-completeness
chapter_id: ch03-litestar-and-range-streaming
title: 'Chapter 03: HTTP Range Streaming and Litestar Store/Static Integration'
description: Implement RFC 9110 single-range 206/416 StreamingStorageResponse and Litestar AsyncObjectStore Store plus static-files
  filesystem adapter.
status: draft
state: planned
planning_status: complete
depends_on_chapters:
- ch01-native-engine-and-auth
replan_trigger: null
current_task: storage-completeness:ch03:t01
plan_revision: 4
plan_commit: null
state_revision: 0
last_verified_checkpoint: null
last_reviewed_commit: null
created_at: '2026-11-07T04:15:00Z'
updated_at: '2026-11-07T04:15:00Z'
generated:
  by: flow-setup
  at: '2026-11-07T04:15:00Z'
verified:
- by: agent:flow-setup
  at: '2026-11-07T04:15:00Z'
  commit: cdb978d3
sources:
- id: streaming-py
  resource: src/py/vertebra/storage/_streaming.py::StreamingStorageResponse
  title: Zero-copy HTTP streaming response over SyncObjectStore and AsyncObjectStore
- id: litestar-store
  resource: src/py/vertebra/extensions/litestar/store.py::ValkeyStore
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
- **Litestar Store & Static Adapter**: `ObjectStoreLitestarStore` implements `litestar.stores.base.Store` (`get`, `set`, `delete`, `delete_all`, `exists`, `expires_in`) over `AsyncObjectStore` with TTL attribute metadata, and `ObjectStoreFileSystemAdapter` implements Litestar's async static-file reader contract (`info`, `open`) [^litestar-store] [^litestar-init].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

`src/py/vertebra/storage/_streaming.py::StreamingStorageResponse` lacks RFC 9110 `Range` header parsing and `416 Range Not Satisfiable` handling and currently requires explicit integer offsets or blocks when given an `AsyncObjectStore` on `to_fastpath_tuple` [^streaming-py]. Meanwhile, `src/py/vertebra/extensions/litestar/store.py` provides `MemoryStore` and `ValkeyStore` but no `AsyncObjectStore`-backed `Store` or static-files adapter for serving assets from GCS/S3/R2/Azure/OCI [^litestar-store]. Chapter 03 completes HTTP byte-range streaming and Litestar storage integration.

### 1.2 Non-Goals

- Multipart `multipart/byteranges` responses (multi-range headers return `416` or fall back per single-range contract).
- Distributed pub/sub channels in `extensions/litestar/channels.py` (owned by `queues-tasks-and-channels`).

### 1.3 Upstream Baseline & Code Analysis

Parent roadmap: [spec.md](../../spec.md); depends on [ch01-native-engine-and-auth](../ch01-native-engine-and-auth/spec.md). Skills: [python-framework](../../../../../skills/python-framework/SKILL.md) (plus external `litestar` plugin skill). Knowledge: [litestar.md](../../../../knowledge/domains/extensions/litestar.md).

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Chapter Requirements

- **R7.1 (`ch03:t01`)**: Add `ByteRangeSpec` / `parse_byte_range_header(range_header: str | None, total_size: int)` and `StreamingStorageResponse.from_request(..., range_header: str | None = None)` in `src/py/vertebra/storage/_streaming.py`, supporting `200 OK`, `206 Partial Content` (`Content-Range: bytes <start>-<end>/<total>`, `Accept-Ranges: bytes`, exact `Content-Length`), and `416 Range Not Satisfiable` (`Content-Range: bytes */<total>`, `Content-Length: 0`, empty body), plus async `resolve_fastpath_tuple` / `aiter_chunks` over `AsyncObjectStore.get_stream` and `get_range`.
- **R7.2 (`ch03:t02`)**: Add `ObjectStoreLitestarStore(_StoreBase)` (`set`, `get`, `delete`, `delete_all`, `exists`, `expires_in`, `with_namespace`) and `ObjectStoreFileSystemAdapter` (`info`, `open`) over `AsyncObjectStore` in `src/py/vertebra/extensions/litestar/store.py` and export both in `src/py/vertebra/extensions/litestar/__init__.py`.

### 2.2 Edge-Case & Failure-Mode Matrix

| Input | Expected Behavior |
| --- | --- |
| `Range: bytes=0-15` on 100-byte object | `206 Partial Content`, `Content-Range: bytes 0-15/100`, `Content-Length: 16`, 16-byte slice. |
| `Range: bytes=-10` on 100-byte object | `206 Partial Content`, `Content-Range: bytes 90-99/100`, `Content-Length: 10`, trailing 10 bytes. |
| `Range: bytes=100-120` or `bytes=50-20` or `bytes=-0` on 100-byte object | `416 Range Not Satisfiable`, `Content-Range: bytes */100`, `Content-Length: 0`, body `b""`. |
| Expired key in `ObjectStoreLitestarStore.get(key)` | Deletes expired object lazily, returns `None`, and `expires_in(key)` returns `0` or `None` per Litestar `Store` semantics. |
| `ObjectStoreLitestarStore(None)` | Raises `ImproperConfigurationError`. |

### 2.3 Alternatives Comparison

| Decision | Chosen Seam | Rejected Alternative |
| --- | --- | --- |
| TTL persistence in `ObjectStoreLitestarStore` | Store `vertebra-expires-at` epoch-ms in object `attributes` (from `ch01:t01`) with deterministic envelope fallback if backend strips custom metadata | Maintaining an in-process dict of TTLs — loses expiration across multi-worker processes and restarts. |

## 3. Documentation & OKF Knowledge Plan

Progress: `[0/0 files documented in ch03]` (documented in `ch04:t03` across `docs/usage/storage.md` and `.agents/bundles/knowledge/domains/storage/object-storage.md`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test Gate |
| --- | --- | --- | --- |
| R7.1 | [t01](tasks/t01-range-streaming-response.md) | `behavior_tdd` | `src/py/tests/unit/server/test_zero_copy_buffer.py`, `src/py/tests/unit/storage/test_storage_store.py` |
| R7.2 | [t02](tasks/t02-litestar-store-and-static-adapter.md) | `behavior_tdd` | `src/py/tests/unit/extensions/test_litestar_plugin.py` |

## 5. Implementation Plan

- [!] storage-completeness:ch03:t01 — Implement RFC 9110 Range/206/416 StreamingStorageResponse and async chunk/fastpath delivery (tasks/t01-range-streaming-response.md)
- [!] storage-completeness:ch03:t02 — Implement ObjectStoreLitestarStore and ObjectStoreFileSystemAdapter in extensions.litestar (tasks/t02-litestar-store-and-static-adapter.md)

**Chapter Recovery Checkpoint:** `uv run --no-sync pytest src/py/tests/unit/server/test_zero_copy_buffer.py src/py/tests/unit/extensions/test_litestar_plugin.py -q && uv run --no-sync ruff check src/py/vertebra/storage/_streaming.py src/py/vertebra/extensions/litestar` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Verified Baseline**: `HEAD @ cdb978d3`; `depends_on_chapters: [ch01-native-engine-and-auth]`, `state: planned`.
- **Remaining Tasks in Chapter**: `t01`, `t02` (`2/2` pending).
- **Active Invariants**: Zero blocking `SyncObjectStore` I/O inside async Litestar handlers; `StorageBuffer` views preserved end-to-end.

[^streaming-py]: `src/py/vertebra/storage/_streaming.py::StreamingStorageResponse` — Zero-copy storage HTTP response builder.
[^litestar-store]: `src/py/vertebra/extensions/litestar/store.py::ValkeyStore` — Litestar `Store` implementations.
[^litestar-init]: `src/py/vertebra/extensions/litestar/__init__.py` — Litestar extension package exports.
