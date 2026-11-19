---
type: Gotcha
title: Object Storage Implementation Limitations
description: Verified caveats, deviations, and unsupported paths in the native object storage plane that callers and maintainers must account for.
status: stable
tags:
- storage
- limitations
- s3
- r2
- oci
- gcs
generated:
  by: flow/0.28.0
  at: "2026-11-09T00:44:30Z"
verified:
- by: agent:flow-executor
  at: "2026-11-17T01:28:00Z"
  commit: 5a833832
- by: agent:flow-setup
  at: "2026-11-12T23:30:00Z"
  commit: 18baadcd
- by: agent:flow-sync
  at: "2026-11-12T01:15:00Z"
  commit: c8722c76
- by: agent:flow-implement
  at: "2026-11-09T02:24:00Z"
  commit: 067fa901
sources:
- id: py-store
  resource: src/py/vertebra/storage/_store.py::SyncObjectStore
  title: Python SyncObjectStore and AsyncObjectStore facades
- id: storage-lib-rs
  resource: src/rs/vertebra-storage/src/lib.rs::extract_buffer_bytes
  title: PyO3 buffer extraction for vertebra._storage
- id: storage-store-rs
  resource: src/rs/vertebra-storage/src/store.rs::ObjectStoreEngine
  title: Rust ObjectStoreEngine file I/O, fsync, copy, rename, and compose
- id: storage-oci-rs
  resource: src/rs/vertebra-storage/src/oci.rs::OciObjectStore
  title: Native OCI Object Storage REST backend and PAR verification
- id: storage-signing-rs
  resource: src/rs/vertebra-storage/src/signing.rs::verify_gcs_v4_url
  title: Native S3/R2 SigV4, GCS V4, Azure SAS, and HMAC URL verification
- id: storage-cargo
  resource: src/rs/vertebra-storage/Cargo.toml
  title: object_store dependency pin and enabled backends
- id: ext-django-storage
  resource: src/py/vertebra/extensions/django/storage.py::DjangoStorage
  title: Django storage backend integration
- id: ext-sqlspec-storage
  resource: src/py/vertebra/extensions/sqlspec/storage.py::ObjectStoreBackend
  title: SQLSpec storage backend and direct Arrow write support
- id: py-arrow-codec
  resource: src/py/vertebra/storage/_arrow.py
  title: Demand-driven Parquet, Arrow IPC, and CSV storage codecs
- id: bench-micro
  resource: tools/benchmarks/micro.py::benchmark_arrow_storage_pipeline
  title: Direct streaming vs buffered Arrow storage microbenchmark
---

# Object Storage Implementation Limitations

<!-- truth: start -->
- `R2Config.custom_domain` is forwarded as a store option, but presigned URLs always target `https://<account_id>.r2.cloudflarestorage.com` because Cloudflare only honors presigned URLs on the S3 API domain ([Cloudflare R2 presigned URLs](https://developers.cloudflare.com/r2/api/s3/presigned-urls/)) [^py-store].
- Native `list(prefix)` matches whole path segments, so `glob()` lists the literal text before the first wildcard truncated to its last `/` and filters the full pattern with `fnmatch.fnmatchcase` [^py-store].
- Native `put` normalizes away trailing `/`, so directory placeholder keys cannot be created through `vertebra.storage`; `glob()` still skips placeholder keys returned by external writers [^py-store].
- Native `extract_buffer_bytes` accepts only byte-format buffers (`B`, `b`, `c`); the Python facade casts typed buffers (`pyarrow.Buffer`, `array.array`, NumPy) to a flat `B` view before crossing FFI, while `bytes` and `StorageBuffer` transfer zero-copy [^storage-lib-rs] [^py-store].
- `ObjectStoreEngine::get_file` flushes and calls `sync_all()` (`fsync`) on the `.part` file before atomic rename, plus parent directory `sync_all()` on Unix (`#[cfg(unix)]`); non-Unix hosts do not issue parent-directory `fsync` [^storage-store-rs].
- Cancellation cleanup (`PartFileGuard`, `CreatedFilesGuard`, `MultipartUploadSession` `Drop`, `ComposeTemporariesGuard`) runs when an aborted Tokio task drops its future; a `tokio::fs` open or rename already running on the blocking pool can still complete after the guard runs, so a `.part` or target file can survive that narrow window [^storage-store-rs].
- `OciObjectStore::copy_opts` fails closed with `Generic` when `copyObject` returns `202` without an `opc-work-request-id` header because completion cannot be confirmed [^storage-oci-rs].
- Create-only copy and rename use conditional multipart completion on S3/B2 and the `cf-copy-destination-if-none-match: *` copy header on R2. Providers that reject these primitives surface their backend error; there is no racy read-then-overwrite fallback. Destination conflicts raise `FileExistsError` and preserve the source and destination bytes [^storage-store-rs] [^storage-cargo].
- Provider atomic-rename support (`BackendConfig::supports_atomic_rename` / `supports_atomic_rename` property): `LocalFileSystem` (`file`/`local`), `InMemory` (`memory`), and `OciObjectStore` (`oci`) report `True` and execute atomic rename natively [^storage-store-rs]. `AmazonS3`, `R2`, `GoogleCloudStorage`, `MicrosoftAzure`, and `HttpStore` report `False` and fall back to non-atomic `copy` + `delete` unless `atomic_only=True` is passed, which fails closed with `InvalidInput` / `ValueError` [^storage-store-rs] [^py-store].
- Pooled storage HTTP clients apply a `300 s` total request timeout that also bounds streamed response bodies, so single very large downloads on slow links must use `get_range` windows or `open_reader` [^storage-oci-rs] [^storage-store-rs].
- `SyncObjectStore.verify_presigned_url` / `AsyncObjectStore.verify_presigned_url` verify HMAC-SHA256, S3/R2 SigV4, GCS V4, Azure SAS, and OCI PAR presigned URLs when `SigningConfig` is configured [^py-store] [^storage-signing-rs]. GCS V4 requires `verify_key_pem`, `private_key_pem`, or HMAC credentials; Azure SAS requires `azure_access_key`; OCI PAR validates endpoint, bucket, namespace, object path, token, and caller-supplied `expires_at` [^storage-signing-rs] [^storage-oci-rs].
- `DjangoStorage.get_created_time` and `DjangoStorage.get_accessed_time` raise `NotImplementedError` because creation and access timestamps are not mapped separately from `get_modified_time` [^ext-django-storage].
- `VerifyingStream` delivers streamed `get_stream` and `open_reader` chunks progressively and raises `IntegrityError` on the final EOF poll when `expected_sha256` mismatches; callers needing all-or-nothing atomicity must use `get_file` or verify before consuming chunks [^py-store].
- `ObjectStoreBackend.supports_direct_arrow_write`, `vertebra.storage._arrow`, and `select_to_storage` stream `parquet`, `arrow-ipc`, and `csv` without full-table buffering; `csv` batch streaming uses single-threaded `pyarrow.csv.open_csv` (inferring `null` on empty header-only files without `column_types`/`arrow_schema`), and `overwrite=False` uses conditional multipart completion [^ext-sqlspec-storage] [^py-arrow-codec].
- On `memory://` (`object_store::memory::InMemory`), multipart uploads retain zero-copy `PyBackedBytes` parts until `complete()`, so `tracemalloc` reflects cumulative parts; `file://` and cloud backends drop each part after `write_part` (`benchmark_arrow_storage_pipeline` therefore targets `file://`) [^storage-lib-rs] [^bench-micro].
- Cloud credential fail-closed boundaries: `ObjectStoreEngine` rejects incomplete Azure Entra triples (`tenant_id`, `client_id`, `client_secret`), malformed Azure `sas_token` strings, and partial OCI Cavage credentials at construction (`ImproperConfigurationError`), while Azure Entra OAuth2 token exchange stays lazy until the first request [^storage-store-rs] [^py-store]. Explicit GCS credential options (`service_account_path`, `service_account_key`, `application_credentials_path`, `application_credentials`) fail closed during auto token-cache construction, whereas `skip_signature=True`, `access_token`, and `target_service_account` delegate validation to `GoogleCloudStorageBuilder::build()` [^py-store] [^storage-store-rs].
- `if_not_exists=True` (SQLSpec `overwrite=False`) on `file://` stores checks for the object and then writes. Concurrent writers to one new key can each succeed, so use a single writer per key when the target must not be replaced. SQLSpec Arrow readers and writers raise `ValueError` when `file_format` and `format_hint` name different formats [^py-store] [^ext-sqlspec-storage].
<!-- truth: end -->

## Caller Guidance

- Serve public R2 objects through the custom domain with unsigned URLs or Cloudflare access rules; use presigned URLs only for the S3 API host.
- Prefer `open_reader(..., read_ahead=...)` or bounded `get_range` loops for multi-gigabyte objects on high-latency links.
- Parquet batch readers in `vertebra.storage._arrow` pass `read_ahead=0` to `open_reader` so row-group demand paging stays bounded; keep that default when adding new demand-paged codecs.
- Check `store.supports_atomic_rename` or pass `atomic_only=True` when callers require single-step move atomicity; use `overwrite=False` for create-only copy and rename only with providers supporting the configured conditional primitive and handle backend refusal explicitly.

## Open Limitations

Non-Unix parent-directory `fsync` omission in `get_file`, cancellation cleanup pool windows, `300 s` total client request timeouts, non-atomic `copy` + `delete` fallback on S3/R2/GCS/Azure/HTTP when `atomic_only=False`, OCI PAR lacking URL-embedded expiry/signatures, and `DjangoStorage` created/accessed timestamps remain open limitations. The R2 presigned-host rule, whole-segment `list(prefix)` matching, progressive `VerifyingStream` EOF checks, and trailing-`/` normalization follow provider and `object_store` semantics and are documented behavior rather than engine defects.

[^py-store]: `src/py/vertebra/storage/_store.py::SyncObjectStore` — Python `SyncObjectStore` and `AsyncObjectStore` facades.
[^storage-lib-rs]: `src/rs/vertebra-storage/src/lib.rs::extract_buffer_bytes` — PyO3 buffer extraction for `vertebra._storage`.
[^storage-store-rs]: `src/rs/vertebra-storage/src/store.rs::ObjectStoreEngine` — Rust `ObjectStoreEngine` file I/O, `fsync`, rename, and compose.
[^storage-oci-rs]: `src/rs/vertebra-storage/src/oci.rs::OciObjectStore` — Native OCI Object Storage REST backend and PAR verification.
[^storage-signing-rs]: `src/rs/vertebra-storage/src/signing.rs::verify_gcs_v4_url` — Native S3/R2 SigV4, GCS V4, Azure SAS, and HMAC URL verification.
[^storage-cargo]: `src/rs/vertebra-storage/Cargo.toml` — `object_store = "0.13.2"` dependency with the `aws`, `gcp`, `azure`, and `http` features.
[^ext-django-storage]: `src/py/vertebra/extensions/django/storage.py::DjangoStorage` — Django storage backend integration.
[^ext-sqlspec-storage]: `src/py/vertebra/extensions/sqlspec/storage.py::ObjectStoreBackend` — SQLSpec storage backend and direct Arrow write support.
[^py-arrow-codec]: `src/py/vertebra/storage/_arrow.py` — Demand-driven Parquet, Arrow IPC, and CSV storage codecs.
[^bench-micro]: `tools/benchmarks/micro.py::benchmark_arrow_storage_pipeline` — Direct streaming vs buffered Arrow storage microbenchmark.
