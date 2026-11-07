---
type: Guide
title: "Object Storage, Presigning & Upload Tracking"
description: "The vertebra-storage Rust crate (vertebra._storage) and vertebra.storage Python facade for multicloud object I/O, presigned URLs, SigV4 headers, R2 chunking, and range reads."
status: stable
tags:
  - domains
  - storage
  - object-store
  - signing
generated:
  by: flow/0.26.0
  at: "2026-11-04T04:33:00Z"
verified:
  - by: agent:flow-setup
    at: "2026-11-07T03:55:00Z"
    commit: cdb978d3
  - by: agent:flow-setup
    at: "2026-11-04T04:33:00Z"
    commit: f741701
sources:
  - id: src-vertebra-core-src-gcp-auth-rs
    resource: src/rs/vertebra-core/src/gcp_auth.rs
    title: "Rust implementation module (src/rs/vertebra-core/src/gcp_auth.rs)"
  - id: src-vertebra-py-src-gcp-auth-rs
    resource: src/rs/vertebra-py/src/gcp_auth.rs
    title: "PyO3 GCP token cache bindings in vertebra._vertebra"
  - id: src-vertebra-storage-cargo-toml
    resource: src/rs/vertebra-storage/Cargo.toml
    title: "Rust crate manifest (src/rs/vertebra-storage/Cargo.toml)"
  - id: src-vertebra-storage-src-lib-rs
    resource: src/rs/vertebra-storage/src/lib.rs
    title: "Rust implementation module (src/rs/vertebra-storage/src/lib.rs)"
  - id: src-storage-pyi
    resource: src/py/vertebra/_storage.pyi
    title: "Python type stub for vertebra._storage"
  - id: src-vertebra-storage-src-store-rs
    resource: src/rs/vertebra-storage/src/store.rs
    title: "Rust implementation module (src/rs/vertebra-storage/src/store.rs)"
  - id: src-vertebra-storage-src-gcp-auth-rs
    resource: src/rs/vertebra-storage/src/gcp_auth.rs
    title: "Rust implementation module (src/rs/vertebra-storage/src/gcp_auth.rs)"
  - id: src-vertebra-storage-src-oci-rs
    resource: src/rs/vertebra-storage/src/oci.rs
    title: "Rust implementation module (src/rs/vertebra-storage/src/oci.rs)"
  - id: src-vertebra-storage-src-r2-rs
    resource: src/rs/vertebra-storage/src/r2.rs
    title: "Cloudflare R2 uniform multipart chunking (src/rs/vertebra-storage/src/r2.rs)"
  - id: src-vertebra-storage-src-signing-rs
    resource: src/rs/vertebra-storage/src/signing.rs
    title: "Rust implementation module (src/rs/vertebra-storage/src/signing.rs)"
  - id: src-vertebra-storage-tests-storage-engine-tests-rs
    resource: src/rs/vertebra-storage/tests/storage_engine_tests.rs
    title: "Rust implementation module (src/rs/vertebra-storage/tests/storage_engine_tests.rs)"
  - id: src-storage-init-py
    resource: src/py/vertebra/storage/__init__.py
    title: "Python implementation module (src/py/vertebra/storage/__init__.py)"
  - id: src-storage-store-py
    resource: src/py/vertebra/storage/_store.py
    title: "Python implementation module (src/py/vertebra/storage/_store.py)"
  - id: src-storage-streaming-py
    resource: src/py/vertebra/storage/_streaming.py
    title: "Python implementation module (src/py/vertebra/storage/_streaming.py)"
  - id: src-extensions-alchemy-storage-py
    resource: src/py/vertebra/extensions/alchemy/storage.py
    title: "Python implementation module (src/py/vertebra/extensions/alchemy/storage.py)"
  - id: src-extensions-sqlspec-storage-py
    resource: src/py/vertebra/extensions/sqlspec/storage.py
    title: "Python implementation module (src/py/vertebra/extensions/sqlspec/storage.py)"
  - id: src-tests-integration-storage-conftest-py
    resource: src/py/tests/integration/storage/conftest.py
    title: "Python implementation module (src/py/tests/integration/storage/conftest.py)"
---

# Object Storage, Presigning & Upload Tracking

<!-- truth: start -->
- **Rust Extension & GIL-Free Async Execution (`vertebra-storage`, `vertebra.storage`)**: [`vertebra-storage`](../../../../../src/rs/vertebra-storage/src/lib.rs)[^src-vertebra-storage-src-lib-rs] (typed in [`_storage.pyi`](../../../../../src/py/vertebra/_storage.pyi)[^src-storage-pyi]) builds `vertebra._storage` (`#[pymodule(gil_used = false)]`) over `object_store 0.13`. `SyncObjectStore` methods release the GIL via `py.detach()`, while `AsyncObjectStore` methods and `py_sign_gcs_v4_iam_url_async` return a `RustTaskAwaitable` (`vertebra._storage.RustTaskAwaitable`) backed by a per-loop `AsyncCompletionQueue` and `vertebra_core::notify::Notifier` (`eventfd` on Linux, pipe elsewhere) registered via `loop.add_reader`, completing on `STORAGE_RUNTIME` with zero Tokio `Python::attach` calls.
- **Shared GCP ADC Credential Caching & Local/IAM Signing (`vertebra_core::gcp_auth`, `signing.rs`)**: [`vertebra_core::gcp_auth::GcpTokenCache`](../../../../../src/rs/vertebra-core/src/gcp_auth.rs)[^src-vertebra-core-src-gcp-auth-rs] stores `std::sync::RwLock<Option<Arc<GcpTokenSnapshot>>>` across `Static`, `AuthorizedUser`, `ServiceAccount` (local PKCS#8 RSA-SHA256 JWT assertion), `MetadataServer`, `ImpersonatedServiceAccount`, and `GcloudCli` sources with `ProxyPolicy`, `quota_project_id` (`x-goog-user-project`), and single-flight `tokio::sync::Mutex` refresh; `SyncGcpTokenCache` and `AsyncGcpTokenCache` PyO3 classes are registered in both `vertebra-storage/src/lib.rs` and [`vertebra-py/src/gcp_auth.rs`](../../../../../src/rs/vertebra-py/src/gcp_auth.rs)[^src-vertebra-py-src-gcp-auth-rs]. `signing.rs` provides GCS V4 local PKCS#8 and IAM `signBlob` signing (`py_sign_gcs_v4_iam_url`, `py_sign_gcs_v4_iam_url_async`), AWS SigV4 URLs and headers (`py_sign_aws_sigv4_headers`, `AwsSigV4HeaderConfig`), OCI Cavage, Azure SAS, and HMAC signing.
<!-- truth: end -->

Object storage has two layers. [`vertebra-storage`](../../../../../src/rs/vertebra-storage/src/lib.rs) is a PyO3 crate built as the `_storage` extension (`#[pymodule(gil_used = false)]`) on top of `object_store 0.13`. [`vertebra.storage`](../../../../../src/py/vertebra/storage/__init__.py)[^src-storage-init-py] is the public Python API. It loads `vertebra._storage` lazily and falls back to pure-Python code when the extension is missing.

`make dev` and `make build` build the crate with `maturin develop -m src/rs/vertebra-storage/Cargo.toml`. The workspace `cargo build` in the `Makefile` excludes it.

## 1. Rust Engine (`store.rs`, `oci.rs`, `r2.rs`, `lib.rs`)

| Symbol | Source | Responsibility |
|---|---|---|
| [`BackendConfig`](../../../../../src/rs/vertebra-storage/src/store.rs)[^src-vertebra-storage-src-store-rs] | `store.rs` | Variants `Memory`, `Local`, `S3`, `Gcs`, `Azure`, `Oci`, `R2`, `Http`. |
| [`ObjectStoreEngine`](../../../../../src/rs/vertebra-storage/src/store.rs) | `store.rs` | Wraps `Arc<dyn object_store::ObjectStore>`. `from_url` maps schemes `memory`, `file`, `s3`/`b2`, `r2`, `oci`, `gs`/`gcs`, `az`/`azure`/`abfs`, `http`/`https` to a backend. `b2` resolves to `https://s3.{region}.backblazeb2.com`. |
| `STORAGE_RUNTIME`, `runtime_handle`, `block_on_storage` | `store.rs` | Private 2-worker Tokio runtime. Sync calls use `block_in_place` when already on a multi-thread runtime. |
| [`StorageError`](../../../../../src/rs/vertebra-storage/src/store.rs) | `store.rs` | `ObjectStore`, `Io`, `InvalidUrl`, `Signing`. |
| [`GcpTokenCache`](../../../../../src/rs/vertebra-core/src/gcp_auth.rs), [`GcpTokenSnapshot`](../../../../../src/rs/vertebra-storage/src/gcp_auth.rs)[^src-vertebra-storage-src-gcp-auth-rs], `SyncGcpTokenCache` / `AsyncGcpTokenCache`, `RefreshingGcpCredentialProvider` | `vertebra_core::gcp_auth`, `gcp_auth.rs`, `lib.rs` | `std::sync::RwLock<Option<Arc<GcpTokenSnapshot>>>` ADC Bearer token cache with `300s` proactive refresh skew (`DEFAULT_REFRESH_SKEW_MS = 300_000`), single-flight `tokio::sync::Mutex<()>` refresh deduplication, generation-checked `invalidate(failed_generation)`, `ProxyPolicy`, and `GcpAuthSource::{Static, AuthorizedUser, ServiceAccount, MetadataServer, ImpersonatedServiceAccount, GcloudCli}`. PyO3 `SyncGcpTokenCache` and `AsyncGcpTokenCache` classes are registered in both `vertebra-storage/src/lib.rs` and `vertebra-py/src/gcp_auth.rs`. Wired into `GoogleCloudStorageBuilder::with_credentials(...)` when no explicit `service_account_path` or `service_account_key` is supplied. |
| [`OciObjectStore`](../../../../../src/rs/vertebra-storage/src/oci.rs)[^src-vertebra-storage-src-oci-rs] | `oci.rs` | Native `ObjectStore` impl over the OCI Object Storage REST API (`/n/{ns}/b/{bucket}/o/{name}`, pre-authenticated `par_url`, multipart via `OciMultipartUpload`). Defaults to `https://objectstorage.{region}.oraclecloud.com`. |
| `OciCavageCredentials`, `sign_oci_cavage_request` | `oci.rs` | RSA-SHA256 `draft-cavage-http-signatures-08` request signing (`Authorization`, `Date`, `Host`, `x-content-sha256`). |
| [`chunk_uniform_parts`, `R2_UNIFORM_PART_SIZE`](../../../../../src/rs/vertebra-storage/src/r2.rs)[^src-vertebra-storage-src-r2-rs] | `r2.rs` | Splits a payload into uniform `8 MiB` parts (`R2_UNIFORM_PART_SIZE`). `ObjectStoreEngine::put_multipart` re-chunks multi-part uploads when the scheme is `r2`, because Cloudflare R2 requires uniform non-trailing part sizes. |
| `SyncObjectStore` / `AsyncObjectStore` / `RustTaskAwaitable` | `lib.rs` | Python classes with sync (`py.detach`) and async (`RustTaskAwaitable`) methods for `put`, `put_file`, `put_multipart`, `get`, `get_buffer`, `get_range`, `head`, `exists`, `delete`, `copy`, and `list`. |
| `StorageBuffer` | `lib.rs` | Frozen `Bytes` holder that implements the buffer protocol, so `memoryview(buf)` does not copy. |
| `RustCompletionRing` / `spawn_rust_task` | `lib.rs` | `spawn_rust_task` returns `RustTaskAwaitable` (`vertebra._storage.RustTaskAwaitable`), registers a per-loop `AsyncCompletionQueue` + `vertebra_core::notify::Notifier` (`eventfd`/pipe) once via `loop.add_reader`, runs the Rust future on `STORAGE_RUNTIME` without acquiring the GIL on Tokio worker threads, and drains ready completions in a single batch callback on the Python event loop thread. |

Module functions exported by `_storage`: `py_sign_hmac_url`, `py_verify_hmac_url`, `py_sign_s3_v4_url`, `py_verify_s3_v4_url`, `py_sign_aws_sigv4_headers`, `py_sign_gcs_v4_iam_url`, `py_sign_gcs_v4_iam_url_async`, `py_sign_oci_cavage_headers`, `py_sign_azure_sas_url`, `py_chunk_r2_uniform_parts`.

## 2. Signing (`signing.rs`)

| Symbol | Scheme |
|---|---|
| `HmacSigner` (`sign_url`, `verify_url`) | Vertebra HMAC-SHA256 URLs with `X-Vtb-Expires`, `X-Vtb-Method`, `X-Vtb-Signature`. |
| `S3SignConfig`, `sign_s3_v4_url`, `verify_s3_v4_url` | AWS SigV4 query presigning (`UNSIGNED-PAYLOAD`). |
| `AwsSigV4HeaderConfig`, `sign_aws_sigv4_headers` (`py_sign_aws_sigv4_headers`) | AWS SigV4 request header signing (`Authorization`, `x-amz-date`, `x-amz-content-sha256`, optional `x-amz-security-token`). |
| `GcsSignConfig`, `sign_gcs_v4_with_iam_endpoint` (`py_sign_gcs_v4_iam_url`, `py_sign_gcs_v4_iam_url_async`) | GCS V4 signing via local PKCS#8 RSA-SHA256 key or sync/async (`RustTaskAwaitable`) IAM Credentials `signBlob` calls. |
| `AzureSasConfig`, `sign_azure_sas_url` | Azure Blob service SAS (`sv=2020-12-06`). |

Verification uses `constant_time_eq`.

## 3. Python Facade (`_store.py`)

Provider configs are `msgspec.Struct(frozen=True, gc=False, kw_only=True)` with a `to_url_and_options()` method: `LocalConfig`, `S3Config`, `GCSConfig`, `AzureConfig`, `OCIConfig`, `R2Config`, `HTTPConfig`, `MemoryConfig`. `StorageConfig` is the PEP 695 union of all of them.

- `SyncObjectStore(url, *, options, signing)` and `AsyncObjectStore(url, *, options, signing)` build native stores for supported schemes. Constructors: `from_config`, `from_local`, `from_s3`, `from_gcs`, `from_azure`, `from_oci`, `from_r2`, `from_http`, `from_memory`.
- Methods accept any `Buffer` and return `PutResult` / `ObjectMeta` structs. `SyncObjectStore.get_view` and `AsyncObjectStore.get_view` return a `memoryview` over `StorageBuffer` without copying.
- `presign_get`, `presign_put`, `verify_presigned_url` dispatch on [`PresignedUrlSigner`](../../../../../src/py/vertebra/storage/_store.py)[^src-storage-store-py] (`provider`: `"hmac"`, `"s3"`, `"gcs"`). Verification supports HMAC and S3 only. GCS signing without the extension calls `signBlob` with `urllib`, and bypasses proxies for loopback IAM endpoints.
- `PresignedUrlSigner.sign_oci_headers` (static) requires the native extension and raises `RuntimeError` without it.
- `PresignedUrlSigner.sign_azure_sas` (static) uses the native function when available and has a pure-Python HMAC fallback.
- `R2Config.chunk_uniform_parts` (static) uses `py_chunk_r2_uniform_parts` when available. The Python fallback enforces a 5 MiB minimum part size. `R2Config.resolved_endpoint` builds `https://{account_id}[.{jurisdiction}].r2.cloudflarestorage.com`.
## 4. Range Reads & Ecosystem Adapters

| Symbol | Source | Responsibility |
|---|---|---|
| [`SyncReadableFile` / `AsyncReadableFile`](../../../../../src/py/vertebra/storage/_store.py) | `_store.py` | Read-only seekable `io.RawIOBase` backed by `get_range`, with `SyncReadableFile.read_at` and `AsyncReadableFile.read_at` that leave the cursor alone. It lets PyArrow or Polars read Parquet footers and row groups without downloading the whole object. |
| [`StreamingStorageResponse`](../../../../../src/py/vertebra/storage/_streaming.py)[^src-storage-streaming-py] | `_streaming.py` | Frozen `msgspec.Struct` descriptor for streaming objects, HTTP `206 Partial Content` byte ranges (`Content-Range`), and `307 Temporary Redirect` presigned URL handoffs (`X-Vertebra-Signed-Url`) directly through `FastPath` across native, Litestar, FastAPI, Starlette, Flask, and Django handlers. |
| [`ObjectStoreBackend`](../../../../../src/py/vertebra/extensions/alchemy/storage.py)[^src-extensions-alchemy-storage-py] | `extensions/alchemy/storage.py` | Drop-in `advanced_alchemy.types.file_object.StorageBackend` implementation wrapping `SyncObjectStore` / `AsyncObjectStore` with sync and async file attachments, content streaming, and presigned URLs. |
| [`ObjectStoreBackend`](../../../../../src/py/vertebra/extensions/sqlspec/storage.py)[^src-extensions-sqlspec-storage-py] | `extensions/sqlspec/storage.py` | Drop-in `sqlspec.protocols.ObjectStoreProtocol` implementation wrapping `SyncObjectStore` / `AsyncObjectStore` with buffer, streaming, range, and `VTBARROW` `ArrowCursor` (`read_arrow_shm`, `write_arrow_shm`) operations. |

Vertebra delegates storage registry and multi-store management to upstream ecosystems (`advanced_alchemy.types.file_object.StorageRegistry` and `sqlspec.storage.registry.StorageRegistry`), eliminating redundant internal abstractions.

## 5. Emulator Fixtures

There is no emulator preset class in the library. [`src/py/tests/integration/storage/conftest.py`](../../../../../src/py/tests/integration/storage/conftest.py)[^src-tests-integration-storage-conftest-py] defines Floci emulator fixtures (`floci_s3_config` on port 4566, `floci_azure_config` on 4577, `floci_gcs_config` on 4588, `floci_r2_config`, plus local, memory, and OCI configs). Endpoints can be overridden with `FLOCI_*_ENDPOINT` environment variables. The directory contains fixtures only, no test modules. Unit coverage lives in `src/py/tests/unit/storage/` and `src/rs/vertebra-storage/tests/storage_engine_tests.rs`.

## 6. Gotchas

- `_BaseObjectStore.__init__` catches any exception from `vertebra._storage.SyncObjectStore(...)` / `AsyncObjectStore(...)` and sets `_native = None`. With no native store and no `file://` root, reads and writes go to the in-process `_memory` dict. A misconfigured `s3://` store therefore keeps working in memory without raising an error.
- Without the native extension, `AsyncObjectStore` methods execute pure-Python local and in-memory fallbacks directly.
- `put` without a native backend synthesizes the ETag as the first 16 hex characters of SHA-256.

Provenance sources: [^src-vertebra-storage-cargo-toml], [^src-vertebra-storage-src-signing-rs], [^src-vertebra-storage-tests-storage-engine-tests-rs].

[^src-vertebra-core-src-gcp-auth-rs]: `src/rs/vertebra-core/src/gcp_auth.rs` — Rust implementation module (src/rs/vertebra-core/src/gcp_auth.rs).
[^src-vertebra-py-src-gcp-auth-rs]: `src/rs/vertebra-py/src/gcp_auth.rs` — PyO3 GCP token cache bindings in vertebra._vertebra.
[^src-vertebra-storage-cargo-toml]: `src/rs/vertebra-storage/Cargo.toml` — Rust crate manifest (src/rs/vertebra-storage/Cargo.toml).
[^src-vertebra-storage-src-lib-rs]: `src/rs/vertebra-storage/src/lib.rs` — Rust implementation module (src/rs/vertebra-storage/src/lib.rs).
[^src-storage-pyi]: `src/py/vertebra/_storage.pyi` — Python type stub for vertebra._storage.
[^src-vertebra-storage-src-store-rs]: `src/rs/vertebra-storage/src/store.rs` — Rust implementation module (src/rs/vertebra-storage/src/store.rs).
[^src-vertebra-storage-src-gcp-auth-rs]: `src/rs/vertebra-storage/src/gcp_auth.rs` — Rust implementation module (src/rs/vertebra-storage/src/gcp_auth.rs).
[^src-vertebra-storage-src-oci-rs]: `src/rs/vertebra-storage/src/oci.rs` — Rust implementation module (src/rs/vertebra-storage/src/oci.rs).
[^src-vertebra-storage-src-r2-rs]: `src/rs/vertebra-storage/src/r2.rs` — Cloudflare R2 uniform multipart chunking (src/rs/vertebra-storage/src/r2.rs).
[^src-vertebra-storage-src-signing-rs]: `src/rs/vertebra-storage/src/signing.rs` — Rust implementation module (src/rs/vertebra-storage/src/signing.rs).
[^src-vertebra-storage-tests-storage-engine-tests-rs]: `src/rs/vertebra-storage/tests/storage_engine_tests.rs` — Rust implementation module (src/rs/vertebra-storage/tests/storage_engine_tests.rs).
[^src-storage-init-py]: `src/py/vertebra/storage/__init__.py` — Python implementation module (src/py/vertebra/storage/__init__.py).
[^src-storage-store-py]: `src/py/vertebra/storage/_store.py` — Python implementation module (src/py/vertebra/storage/_store.py).
[^src-storage-streaming-py]: `src/py/vertebra/storage/_streaming.py` — Python implementation module (src/py/vertebra/storage/_streaming.py).
[^src-extensions-alchemy-storage-py]: `src/py/vertebra/extensions/alchemy/storage.py` — Python implementation module (src/py/vertebra/extensions/alchemy/storage.py).
[^src-extensions-sqlspec-storage-py]: `src/py/vertebra/extensions/sqlspec/storage.py` — Python implementation module (src/py/vertebra/extensions/sqlspec/storage.py).
[^src-tests-integration-storage-conftest-py]: `src/py/tests/integration/storage/conftest.py` — Python implementation module (src/py/tests/integration/storage/conftest.py).
