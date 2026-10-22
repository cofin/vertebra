---
type: Guide
title: "Object Storage, Presigning & Upload Tracking"
description: "The `vertebra-storage` Rust crate (`vertebra._storage`) and the `vertebra.storage` Python facade for multicloud object I/O, presigned URLs, transactional uploads, and range reads."
status: stable
tags: [domains, storage, object-store, signing]
sources:
  - resource: src/rs/vertebra-storage/Cargo.toml
  - resource: src/rs/vertebra-storage/src/lib.rs
  - resource: src/rs/vertebra-storage/src/store.rs
  - resource: src/rs/vertebra-storage/src/oci.rs
  - resource: src/rs/vertebra-storage/src/signing.rs
  - resource: src/rs/vertebra-storage/tests/storage_engine_tests.rs
  - resource: src/py/vertebra/storage/__init__.py
  - resource: src/py/vertebra/storage/_store.py
  - resource: src/py/vertebra/storage/registry.py
  - resource: src/py/vertebra/storage/pipeline.py
  - resource: src/py/vertebra/storage/arrow.py
  - resource: src/py/tests/integration/storage/conftest.py
---

# Object Storage, Presigning & Upload Tracking

Object storage has two layers. [`vertebra-storage`](../../../../../src/rs/vertebra-storage/src/lib.rs) is a PyO3 crate built as the `_storage` extension (`#[pymodule(gil_used = false)]`) on top of `object_store 0.13`. [`vertebra.storage`](../../../../../src/py/vertebra/storage/__init__.py) is the public Python API. It loads `vertebra._storage` lazily and falls back to pure-Python code when the extension is missing.

`make dev` and `make build` build the crate with `maturin develop -m src/rs/vertebra-storage/Cargo.toml`. The workspace `cargo build` in the `Makefile` excludes it.

## 1. Rust Engine (`store.rs`, `oci.rs`, `lib.rs`)

| Symbol | Source | Responsibility |
|---|---|---|
| [`BackendConfig`](../../../../../src/rs/vertebra-storage/src/store.rs) | `store.rs` | Variants `Memory`, `Local`, `S3`, `Gcs`, `Azure`, `Oci`, `R2`, `Http`. |
| [`ObjectStoreEngine`](../../../../../src/rs/vertebra-storage/src/store.rs) | `store.rs` | Wraps `Arc<dyn ObjectStore>`. `from_url` maps schemes `memory`, `file`, `s3`/`b2`, `r2`, `oci`, `gs`/`gcs`, `az`/`azure`/`abfs`, `http`/`https` to a backend. `b2` resolves to `https://s3.{region}.backblazeb2.com`. |
| `STORAGE_RUNTIME`, `runtime_handle`, `block_on_storage` | `store.rs` | Private 2-worker Tokio runtime. Sync calls use `block_in_place` when already on a multi-thread runtime. |
| [`StorageError`](../../../../../src/rs/vertebra-storage/src/store.rs) | `store.rs` | `ObjectStore`, `Io`, `InvalidUrl`, `Signing`. |
| [`OciObjectStore`](../../../../../src/rs/vertebra-storage/src/oci.rs) | `oci.rs` | Native `ObjectStore` impl over the OCI Object Storage REST API (`/n/{ns}/b/{bucket}/o/{name}`, pre-authenticated `par_url`, multipart via `OciMultipartUpload`). Defaults to `https://objectstorage.{region}.oraclecloud.com`. |
| `OciCavageCredentials`, `sign_oci_cavage_request` | `oci.rs` | RSA-SHA256 `draft-cavage-http-signatures-08` request signing (`Authorization`, `Date`, `Host`, `x-content-sha256`). |
| `chunk_uniform_parts`, `R2_UNIFORM_PART_SIZE` | `oci.rs` | Splits a payload into equal 8 MiB parts. `ObjectStoreEngine::put_multipart` re-chunks multi-part uploads when the scheme is `r2`, because R2 requires uniform non-trailing part sizes. |
| `NativeObjectStore` | `lib.rs` | Python class with sync and `*_async` methods for `put`, `put_file`, `get`, `get_buffer`, `get_range`, `head`, `exists`, `delete`, `copy`, `list`, plus sync `put_multipart`. |
| `StorageBuffer` | `lib.rs` | Frozen `Bytes` holder that implements the buffer protocol, so `memoryview(buf)` does not copy. |
| `RustTaskAwaitable` | `lib.rs` | `spawn_rust_task` creates an asyncio future on the running loop, runs the Rust future on the storage runtime, and resolves it through `call_soon_threadsafe`. Async calls do not go through `asyncio.to_thread`. |

Module functions exported by `_storage`: `py_sign_hmac_url`, `py_verify_hmac_url`, `py_sign_s3_v4_url`, `py_verify_s3_v4_url`, `py_sign_gcs_v4_iam_url`, `py_sign_oci_cavage_headers`, `py_sign_azure_sas_url`, `py_chunk_r2_uniform_parts`.

## 2. Signing (`signing.rs`)

| Symbol | Scheme |
|---|---|
| `HmacSigner` (`sign_url`, `verify_url`) | Vertebra HMAC-SHA256 URLs with `X-Vtb-Expires`, `X-Vtb-Method`, `X-Vtb-Signature`. |
| `S3SignConfig`, `sign_s3_v4_url`, `verify_s3_v4_url` | AWS SigV4 query presigning (`UNSIGNED-PAYLOAD`). |
| `GcsSignConfig`, `sign_gcs_v4_with_iam_endpoint` | GCS V4 signing through the IAM Credentials `signBlob` endpoint. |
| `AzureSasConfig`, `sign_azure_sas_url` | Azure Blob service SAS (`sv=2020-12-06`). |

Verification uses `constant_time_eq`.

## 3. Python Facade (`_store.py`)

Provider configs are `msgspec.Struct(frozen=True, gc=False, kw_only=True)` with a `to_url_and_options()` method: `LocalConfig`, `S3Config`, `GcsConfig`, `AzureConfig`, `OciConfig`, `R2Config`, `HttpConfig`, `MemoryConfig`. `StorageConfig` is the PEP 695 union of all of them.

- `ObjectStore(url, *, options, signing)` builds a `NativeObjectStore` for supported schemes. Constructors: `from_config`, `from_local`, `from_s3`, `from_gcs`, `from_azure`, `from_oci`, `from_r2`, `from_http`, `from_memory`.
- Methods accept any `Buffer` and return `PutResult` / `ObjectMeta` structs. `get_view` / `get_view_async` return a `memoryview` over `StorageBuffer` without copying.
- `presign_get`, `presign_put`, `verify_presigned_url` dispatch on `SigningConfig.provider` (`"hmac"`, `"s3"`, `"gcs"`). Verification supports HMAC and S3 only. GCS signing without the extension calls `signBlob` with `urllib`, and bypasses proxies for loopback IAM endpoints.
- `SigningConfig.sign_oci_headers` (static) requires the native extension and raises `RuntimeError` without it. Its `timestamp` parameter defaults to the fixed value `1_700_000_000`, so callers must pass the current time for real requests.
- `SigningConfig.sign_azure_sas` (static) uses the native function when available and has a pure-Python HMAC fallback.
- `R2Config.chunk_uniform_parts` (static) uses `py_chunk_r2_uniform_parts` when available. The Python fallback enforces a 5 MiB minimum part size. `R2Config.resolved_endpoint` builds `https://{account_id}[.{jurisdiction}].r2.cloudflarestorage.com`.
- `SyncStore` and `AsyncStore` are thin explicit-mode facades over one `ObjectStore`.

Key handling: `_normalize_key` strips leading `/` and rejects `..` segments. `_resolve_local` rejects paths that escape a `file://` root.

## 4. Registry, Upload Tracking, Range Reads

| Symbol | Source | Responsibility |
|---|---|---|
| [`StorageRegistry`](../../../../../src/py/vertebra/storage/registry.py) | `registry.py` | `RLock`-guarded alias and URI cache (`register_alias`, `register_store`, `resolve_store`, `get`, `aliases`, `clear`). A target with a `to_store()` method is materialized on registration. Calls that pass per-call `options`/`signing` to an alias are not cached. |
| `get_default_storage_registry` | `registry.py` | Returns the process-wide `_DEFAULT_REGISTRY`. |
| [`UploadTracker`](../../../../../src/py/vertebra/storage/pipeline.py) / `StagedUpload` | `pipeline.py` | Stages uploads and deferred deletes around a database transaction: `before_flush` writes staged objects, `after_commit` runs deferred deletes, `after_rollback` deletes the objects written in that transaction. Async variants mirror each hook. `attach_sqlalchemy_session` registers the three SQLAlchemy session events. `async with tracker:` rolls back on exception. Delete failures are suppressed. |
| [`ReadableFile`](../../../../../src/py/vertebra/storage/arrow.py) | `arrow.py` | Read-only seekable `io.RawIOBase` backed by `get_range`, with `read_at` / `read_at_async` that leave the cursor alone. It lets PyArrow or Polars read Parquet footers and row groups without downloading the whole object. |

`UploadTracker` is used by the alchemy storage extension ([sqlspec-alchemy-and-dialects](../extensions/sqlspec-alchemy-and-dialects.md)). `StorageRegistry`, `get_default_storage_registry`, and `ReadableFile` have no callers inside `src/py/vertebra` outside the storage package.

## 5. Emulator Fixtures

There is no emulator preset class in the library. [`src/py/tests/integration/storage/conftest.py`](../../../../../src/py/tests/integration/storage/conftest.py) defines Floci emulator fixtures (`floci_s3_config` on port 4566, `floci_azure_config` on 4577, `floci_gcs_config` on 4588, `floci_r2_config`, plus local, memory, and OCI configs). Endpoints can be overridden with `FLOCI_*_ENDPOINT` environment variables. The directory contains fixtures only, no test modules. Unit coverage lives in `src/py/tests/unit/storage/` and `src/rs/vertebra-storage/tests/storage_engine_tests.rs`.

## 6. Gotchas

- `ObjectStore.__init__` catches any exception from `NativeObjectStore(...)` and sets `_native = None`. With no native store and no `file://` root, reads and writes go to the in-process `_memory` dict. A misconfigured `s3://` store therefore keeps working in memory without raising an error.
- Without the native extension, every `*_async` method calls its blocking sync counterpart.
- `put` without a native backend synthesizes the ETag as the first 16 hex characters of SHA-256.
