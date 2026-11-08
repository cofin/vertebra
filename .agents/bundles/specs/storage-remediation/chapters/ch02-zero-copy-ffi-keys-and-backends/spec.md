---
type: Spec
flow_id: storage-remediation
parent_flow: storage-remediation
chapter_id: ch02-zero-copy-ffi-keys-and-backends
title: "Chapter 02: Zero-Copy FFI, Raw Object Keys, and Cloud/OCI Backends"
description: Define PreconditionError and map conditional failures, wrap immutable PyBytes zero-copy via PyBackedBytes while copying mutable buffers synchronously, check get_range overflow and zero-length bounds, preserve raw object keys via StorePath::parse, wire S3 virtual_hosted_style, R2 custom_domain, HTTP headers, file:// conditional metadata, and Litestar raw keys, and upgrade OciObjectStore to stream, conditionally write, and paginate.
status: stable
state: active
planning_status: complete
depends_on_chapters:
  - ch01-native-gcs-plane-and-signing
replan_trigger: null
current_task: "storage-remediation:ch02:t04"
plan_revision: 1
plan_commit: fd571e3a
state_revision: 5
last_verified_checkpoint: 8eb43fb2
last_reviewed_commit: null
created_at: "2026-11-08T20:55:00Z"
updated_at: "2026-11-08T23:48:07Z"
generated:
  by: flow/0.28.0
  at: "2026-11-08T20:55:00Z"
verified: []
sources:
  - id: py-typing
    resource: src/py/vertebra/_typing.py::VertebraError
    title: Internal exception and typing definitions re-exported by vertebra.typing
  - id: py-typing-facade
    resource: src/py/vertebra/typing.py::VertebraError
    title: Public vertebra.typing facade
  - id: storage-lib-rs
    resource: src/rs/vertebra-storage/src/lib.rs::extract_buffer_bytes
    title: PyO3 buffer extraction, error mapping, and signing bindings
  - id: storage-store-rs
    resource: src/rs/vertebra-storage/src/store.rs::ObjectStoreEngine
    title: ObjectStoreEngine key parsing, range checks, file:// metadata, and S3/R2/HTTP builders
  - id: storage-signing-rs
    resource: src/rs/vertebra-storage/src/signing.rs::S3SignConfig
    title: S3 SigV4 path-style and virtual-hosted-style URL presigning
  - id: storage-oci-rs
    resource: src/rs/vertebra-storage/src/oci.rs::OciObjectStore
    title: Native OCI Object Storage REST implementation
  - id: storage-pyi
    resource: src/py/vertebra/_storage.pyi::py_sign_s3_v4_url
    title: Type stubs for vertebra._storage
  - id: litestar-store
    resource: src/py/vertebra/extensions/litestar/store.py::ObjectStoreLitestarStore
    title: Litestar key-value store and filesystem adapter over ObjectStore
tags:
  - storage
  - ffi
  - s3
  - oci
  - preconditions
---

# Chapter 02: Zero-Copy FFI, Raw Object Keys, and Cloud/OCI Backends

<!-- truth: start -->
- **`PreconditionError` & Zero-Copy Immutable FFI (`FFI-05`)**: `vertebra.typing` exports `PreconditionError(VertebraError, OSError)` mapped from `StorageError::Precondition` and `object_store::Error::Precondition`; `extract_buffer_bytes` wraps `StorageBuffer` and immutable `PyBytes` zero-copy (`Bytes::from_owner(PyBackedBytes::from(...))`) while copying mutable `PyBuffer<u8>` (`bytearray`, `memoryview`, NumPy) synchronously before dropping `PyBuffer` so caller `bytearray.clear()` never hits `BufferError` [^py-typing] [^py-typing-facade] [^storage-lib-rs].
- **Checked `get_range`, Raw `StorePath::parse` Keys, `file://` Conditional Metadata & Cloud Builders**: `ObjectStoreEngine::get_range` validates `offset.checked_add(length)` and returns empty `Bytes` after `head()` when `length == 0`; `StorePath::parse` preserves raw `~` and `%` sequences verbatim across `store.rs`, `oci.rs`, and `litestar/store.py`; `file://` conditional fallback preserves `content_type` and `attributes`; `BackendConfig::S3` wires `session_token` and `virtual_hosted_style` (paired with `S3SignConfig.virtual_hosted_style`); and `BackendConfig::Http` forwards custom `headers` [^storage-store-rs] [^storage-signing-rs] [^litestar-store].
- **Streaming, Conditional & Paginated `OciObjectStore`**: `OciObjectStore` streams `get_opts` payloads via `resp.bytes_stream()`, honors `GetOptions` (`range`, `if_match`, `if_none_match`, `head`), `PutOptions` (`Create` -> `if-none-match: *`, `Update` -> `if-match`), and `CopyOptions` (`Create` -> `if-none-match: *`), and paginates `list` and `list_with_delimiter` (`delimiter=/` + `nextStartWith`) [^storage-oci-rs].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

`extract_buffer_bytes` in `src/rs/vertebra-storage/src/lib.rs` copies every immutable `bytes` payload via `buf.to_vec(py)` instead of wrapping `PyBytes` in `PyBackedBytes`, while `to_py_err` maps HTTP `412` precondition mismatches to `PermissionError` (`EACCES`) instead of a dedicated `PreconditionError` [^storage-lib-rs]. In `store.rs`, `get_range` performs unchecked `offset + length` addition (`u64` overflow panic across FFI in debug builds and wrapping range corruption in release) and emits invalid `Range: bytes=offset-(offset-1)` when `length == 0`; `StorePath::from` percent-encodes `~` and `%`, forcing `urllib.parse.unquote` workarounds in `litestar/store.py`; `S3` `session_token` / `virtual_hosted_style` and `Http` custom `headers` are unwired; and `OciObjectStore` buffers full GET responses in RAM while ignoring `GetOptions`, `PutOptions`, `CopyOptions`, and `nextStartWith` pagination [^storage-store-rs] [^storage-oci-rs] [^litestar-store].

**Non-Goals**:
- Bulk file transfers, `open_writer`, `glob`, and server-side `compose` (Chapter 03).
- Python `_store.py` direct non-`memoryview` buffer passthrough and `S3Config` / `R2Config` / `PreconditionError` re-exports (Chapter 03 `t03` and `t04`).

### 1.2 Upstream Execution Baseline

- Depends on `ch01-native-gcs-plane-and-signing`; parent spec [`../../spec.md`](../../spec.md).

### 1.3 Code Analysis & Seam Summary

| Area | Files Examined (`path/to/file::Symbol`) | Public Seam, Dependency Category & Integration |
| --- | --- | --- |
| Typing & FFI Buffers | `src/py/vertebra/_typing.py::VertebraError` [^py-typing], `src/py/vertebra/typing.py::VertebraError` [^py-typing-facade], `src/rs/vertebra-storage/src/lib.rs::extract_buffer_bytes` [^storage-lib-rs] | In-process exception hierarchy and PyO3 zero-copy / mutable-copy FFI boundary. |
| Store Core & S3 Signing | `src/rs/vertebra-storage/src/store.rs::ObjectStoreEngine` [^storage-store-rs], `src/rs/vertebra-storage/src/signing.rs::S3SignConfig` [^storage-signing-rs], `src/py/vertebra/_storage.pyi::py_sign_s3_v4_url` [^storage-pyi], `src/py/vertebra/extensions/litestar/store.py::ObjectStoreLitestarStore` [^litestar-store] | Local/remote object store engine, SigV4 URL signer, and Litestar KV store seam. |
| OCI Object Store | `src/rs/vertebra-storage/src/oci.rs::OciObjectStore` [^storage-oci-rs] | Remote-owned HTTP boundary verified against loopback OCI REST test servers. |

### 1.4 Relevant OKF Knowledge Chapters & Skills

- [`object-storage.md`](../../../../knowledge/domains/storage/object-storage.md), [`ffi-and-gil-discipline.md`](../../../../knowledge/patterns/rust/ffi-and-gil-discipline.md), [`pyo3-bridge`](../../../../../skills/pyo3-bridge/SKILL.md).

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Functional Requirements

- **R2.1 (`t01`)**: Define `PreconditionError(VertebraError, OSError)` in `src/py/vertebra/_typing.py` and re-export in `src/py/vertebra/typing.py`; map `StorageError::Precondition` and `object_store::Error::Precondition` in `to_py_err` to `vertebra.typing.PreconditionError`; update `extract_buffer_bytes` in `src/rs/vertebra-storage/src/lib.rs` to extract `StorageBuffer` (`data.clone()`), `PyBytes` (`Bytes::from_owner(PyBackedBytes::from(py_bytes.clone()))`), and contiguous `PyBuffer<u8>` (`Bytes::from(buf.to_vec(py)?)` before `buf` drops).
- **R2.2 (`t02`)**: Replace `StorePath::from` with `StorePath::parse` in `validate_object_path` and `validate_optional_prefix` (`store.rs`), add `checked_add` and `length == 0` handling to `ObjectStoreEngine::get_range`, preserve `content_type` and `attributes` in `file://` conditional put fallback, wire `S3` `session_token` + `virtual_hosted_style` and `Http` `headers` (`http_header.<name>` / JSON `headers`) in `store.rs`, and add `virtual_hosted_style: bool` to `S3SignConfig` in `signing.rs`.
- **R2.3 (`t03`)**: Expose `virtual_hosted_style: bool = False` on `py_sign_s3_v4_url` and `py_verify_s3_v4_url` in `src/rs/vertebra-storage/src/lib.rs` and `src/py/vertebra/_storage.pyi`, and remove `urllib.parse.unquote` from `src/py/vertebra/extensions/litestar/store.py`.
- **R2.4 (`t04`)**: Upgrade `OciObjectStore` (`src/rs/vertebra-storage/src/oci.rs`) to parse keys via `StorePath::parse`, stream `get_opts` bodies via `resp.bytes_stream()`, honor `GetOptions` (`range`, `if_match`, `if_none_match`, `head`), `PutOptions` (`Create` -> `if-none-match: *`, `Update` -> `if-match`), and `CopyOptions` (`Create` -> `if-none-match: *`), and paginate `list` and `list_with_delimiter` (`delimiter=/` + `nextStartWith`).

### 2.2 Full Public Type, Struct, Schema & Signature Contracts

- `class PreconditionError(VertebraError, OSError): """Raised when a conditional storage or resource precondition fails."""`
- `S3SignConfig { pub bucket: String, pub region: String, pub access_key_id: String, pub secret_access_key: String, pub endpoint: Option<String>, pub virtual_hosted_style: bool }`
- `py_sign_s3_v4_url(..., endpoint: str | None = None, virtual_hosted_style: bool = False) -> str`
- `py_verify_s3_v4_url(..., endpoint: str | None = None, virtual_hosted_style: bool = False) -> bool`

### 2.3 Caller -> Seam -> Collaborator Integration Map

- `PySyncObjectStore::put` / `PyAsyncObjectStore::put` -> `extract_buffer_bytes` (`PyBackedBytes` zero-copy on `bytes`, immediate `to_vec` copy on `bytearray`) -> `ObjectStoreEngine::put_opts` -> `to_py_err` (`PreconditionError` on `412`).
- `ObjectStoreLitestarStore` -> `ObjectStore.list` -> `StorePath::parse` -> exact raw key returned without `unquote`.

### 2.4 Non-Functional Constraints & Edge-Case / Failure-Mode Matrix

| Scenario / Boundary | Trigger / Input | Expected Behavior / Error Contract | Invariant Preserved |
| --- | --- | --- | --- |
| Mutable `bytearray` cleared immediately after `put` | `b = bytearray(b"xyz"); store.put("k", b); b.clear()` | `b.clear()` succeeds with zero `BufferError`; stored object remains `b"xyz"` | Safe caller buffer mutation (`FFI-05`). |
| Non-contiguous `memoryview` | `memoryview(b"abcdef")[::2]` | Raises `BufferError("storage input buffer must be C-contiguous")` | Rejects strided buffers cleanly (`FFI-05`). |
| `get_range` overflow or `length == 0` | `get_range("k", 10, u64::MAX)` or `get_range("k", 0, 0)` | Overflow raises `ValueError`; `length == 0` checks `head("k")` (`FileNotFoundError` if missing) and returns `b""` | Zero arithmetic panic (`FFI-06`). |
| Raw keys with `~` and `%25` | `put("v1~draft/100%25.txt", b"ok")` + `list()` | `head` and `list` report exact key `"v1~draft/100%25.txt"` | Lossless key round-trip across Litestar and native stores. |
| OCI `412` / multi-page `list` | `PutMode::Create` on existing OCI key or `nextStartWith` across 3 pages | `412` maps to `object_store::Error::Precondition` (`PreconditionError`); `list` yields all objects across pages | Full `ObjectStore` contract parity on OCI. |

### 2.5 Acceptance Scenarios (Given / When / Then)

1. **Given** an existing object in `SyncObjectStore("memory://ns")`, **When** `put("k", b"v2", if_match="wrong-etag")` is called, **Then** `PreconditionError` (subclass of `VertebraError` and `OSError`, not `PermissionError`) is raised.
2. **Given** a loopback OCI server with 2 pages of objects, conditional `if-match` / `if-none-match` enforcement, and a streamed GET body, **When** `OciObjectStore` runs `put_opts`, `get_opts` (with range and conditions), `copy_if_not_exists`, `list`, and `list_with_delimiter`, **Then** every operation streams and paginates accurately.

### 2.6 Alternatives Considered

- **Chosen**: Restrict `PyBackedBytes` zero-copy ownership to immutable `PyBytes` (`StorageBuffer` + `PyBytes`), copying mutable `PyBuffer<u8>` synchronously while holding the GIL before dropping `PyBuffer`.
- **Rejected**: Holding `PyBuffer<u8>` open across `py.detach` / Tokio tasks for `bytearray`, which locks the Python buffer export count (`BufferError` on `bytearray.clear()`) or risks data races if mutated concurrently.

## 3. Documentation & OKF Knowledge Plan

- **Progress**: `[0/0 files documented in ch02]` (consolidated in Chapter 06 `ch06:t01` and `ch06:t02`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test |
| --- | --- | --- | --- |
| R2.1 | `ch02 / t01` | `behavior_tdd` | `src/py/tests/unit/storage/test_storage_store.py` |
| R2.2 | `ch02 / t02` | `behavior_tdd` | `src/rs/vertebra-storage/tests/storage_engine_tests.rs` |
| R2.3 | `ch02 / t03` | `behavior_tdd` | `src/py/tests/unit/storage/test_signing.py`, `src/py/tests/unit/extensions/test_litestar_plugin.py` |
| R2.4 | `ch02 / t04` | `behavior_tdd` | `src/rs/vertebra-storage/tests/storage_engine_tests.rs` |

## 5. Implementation Plan

- [x] [d889b958] storage-remediation:ch02:t01 — Define `PreconditionError` in `vertebra.typing`, map `412` preconditions in `to_py_err`, and use zero-copy `PyBackedBytes` for immutable `PyBytes` (tasks/t01-precondition-error-and-zero-copy-ffi.md)
- [x] [fb8dde11] storage-remediation:ch02:t02 — Check `get_range` bounds, preserve raw keys with `StorePath::parse`, preserve `file://` conditional metadata, and wire S3/HTTP options in Rust (tasks/t02-checked-ranges-raw-keys-and-backend-builders.md)
- [x] [8eb43fb2] storage-remediation:ch02:t03 — Wire S3 `virtual_hosted_style` PyO3 bindings and stubs and remove Litestar key `unquote` workaround (tasks/t03-s3-virtual-hosted-bindings-and-litestar-keys.md)
- [ ] storage-remediation:ch02:t04 — Upgrade `OciObjectStore` to stream `get_opts`, honor conditional/copy headers, and paginate `list` and `list_with_delimiter` (tasks/t04-streaming-conditional-paginated-oci.md)
- **Chapter / Phase Recovery Checkpoint**: `PATH="$PWD/.venv/bin:$PATH" PYO3_PYTHON="$PWD/.venv/bin/python" CARGO_TARGET_DIR=/tmp/vtb-cargo-target cargo test --manifest-path src/rs/Cargo.toml -p vertebra-storage && uv run --no-sync pytest src/py/tests/unit/storage/test_storage_store.py src/py/tests/unit/storage/test_signing.py src/py/tests/unit/extensions/test_litestar_plugin.py -q`

## 6. Current State & Remaining Plan Snapshot

- **Verified Baseline (`HEAD`)**: `8eb43fb2` — `3/4` tasks completed in `ch02`.
- **Remaining Tasks in Chapter**: `t04`.
- **Remaining Chapters**: `ch02`–`ch06`.

[^py-typing]: `src/py/vertebra/_typing.py::VertebraError` — Internal exception and typing definitions re-exported by `vertebra.typing`.
[^py-typing-facade]: `src/py/vertebra/typing.py::VertebraError` — Public `vertebra.typing` facade.
[^storage-lib-rs]: `src/rs/vertebra-storage/src/lib.rs::extract_buffer_bytes` — PyO3 buffer extraction, error mapping, and signing bindings.
[^storage-store-rs]: `src/rs/vertebra-storage/src/store.rs::ObjectStoreEngine` — `ObjectStoreEngine` key parsing, range checks, `file://` metadata, and S3/R2/HTTP builders.
[^storage-signing-rs]: `src/rs/vertebra-storage/src/signing.rs::S3SignConfig` — S3 SigV4 path-style and virtual-hosted-style URL presigning.
[^storage-oci-rs]: `src/rs/vertebra-storage/src/oci.rs::OciObjectStore` — Native OCI Object Storage REST implementation.
[^storage-pyi]: `src/py/vertebra/_storage.pyi::py_sign_s3_v4_url` — Type stubs for `vertebra._storage`.
[^litestar-store]: `src/py/vertebra/extensions/litestar/store.py::ObjectStoreLitestarStore` — Litestar key-value store and filesystem adapter over `ObjectStore`.
