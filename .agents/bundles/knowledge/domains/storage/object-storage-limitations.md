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
  title: Rust ObjectStoreEngine file I/O, copy, rename, and compose
- id: storage-oci-rs
  resource: src/rs/vertebra-storage/src/oci.rs::OciObjectStore
  title: Native OCI Object Storage REST backend
- id: storage-cargo
  resource: src/rs/vertebra-storage/Cargo.toml
  title: object_store dependency pin and enabled backends
- id: ext-django-storage
  resource: src/py/vertebra/extensions/django/storage.py::DjangoStorage
  title: Django storage backend integration
- id: ext-sqlspec-storage
  resource: src/py/vertebra/extensions/sqlspec/storage.py::ObjectStoreBackend
  title: SQLSpec storage backend and direct Arrow write support
---

# Object Storage Implementation Limitations

<!-- truth: start -->
- `R2Config.custom_domain` is forwarded as a store option, but presigned URLs always target `https://<account_id>.r2.cloudflarestorage.com` because Cloudflare only honors presigned URLs on the S3 API domain ([Cloudflare R2 presigned URLs](https://developers.cloudflare.com/r2/api/s3/presigned-urls/)) [^py-store].
- Native `list(prefix)` matches whole path segments, so `glob()` lists the literal text before the first wildcard truncated to its last `/` and filters the full pattern with `fnmatch.fnmatchcase` [^py-store].
- Native `put` normalizes away trailing `/`, so directory placeholder keys cannot be created through `vertebra.storage`; `glob()` still skips placeholder keys returned by external writers [^py-store].
- Native `extract_buffer_bytes` accepts only byte-format buffers (`B`, `b`, `c`); the Python facade casts typed buffers (`pyarrow.Buffer`, `array.array`, NumPy) to a flat `B` view before crossing FFI, while `bytes` and `StorageBuffer` transfer zero-copy [^storage-lib-rs] [^py-store].
- `ObjectStoreEngine::get_file` flushes and atomically renames the `.part` file but does not call `fsync`; durability across power loss depends on the filesystem [^storage-store-rs].
- Cancellation cleanup (`PartFileGuard`, `CreatedFilesGuard`, `MultipartUploadSession` `Drop`, `ComposeTemporariesGuard`) runs when an aborted Tokio task drops its future; a `tokio::fs` open or rename already running on the blocking pool can still complete after the guard runs, so a `.part` or target file can survive that narrow window [^storage-store-rs].
- `OciObjectStore::copy_opts` fails closed with `Generic` when `copyObject` returns `202` without an `opc-work-request-id` header because completion cannot be confirmed [^storage-oci-rs].
- Create-only copy and rename use conditional multipart completion on S3/B2 and the `cf-copy-destination-if-none-match: *` copy header on R2. Providers that reject these primitives surface their backend error; there is no racy read-then-overwrite fallback. Destination conflicts raise `FileExistsError` and preserve the source and destination bytes [^storage-store-rs] [^storage-cargo].
- Cloud rename copies to the destination before deleting the source, so it is not atomic. A delete failure can leave both keys present [^storage-store-rs].
- Pooled storage HTTP clients apply a `300 s` total request timeout that also bounds streamed response bodies, so single very large downloads on slow links must use `get_range` windows or `open_reader` [^storage-oci-rs] [^storage-store-rs].
- `_BaseObjectStore.verify_presigned_url` checks HMAC and SigV4 signatures only and raises `NotImplementedError` on `gcs`, `azure`, and `oci` [^py-store].
- `DjangoStorage.get_created_time` and `DjangoStorage.get_accessed_time` raise `NotImplementedError` because creation and access timestamps are not mapped separately from `get_modified_time` [^ext-django-storage].
- `VerifyingStream` delivers streamed `get_stream` and `open_reader` chunks progressively and raises `IntegrityError` on the final EOF poll when `expected_sha256` mismatches; callers needing all-or-nothing atomicity must use `get_file` or verify before consuming chunks [^py-store].
- SQLSpec `<= 0.71.1` `SyncStoragePipeline.write_arrow` and `AsyncStoragePipeline.write_arrow` materialize full Arrow payloads before invoking the backend until upstream SQLSpec dispatches on `ObjectStoreBackend.supports_direct_arrow_write` [^ext-sqlspec-storage].
<!-- truth: end -->

## Caller Guidance

- Serve public R2 objects through the custom domain with unsigned URLs or Cloudflare access rules; use presigned URLs only for the S3 API host.
- Prefer `open_reader(..., read_ahead=...)` or bounded `get_range` loops for multi-gigabyte objects on high-latency links.
- Parquet batch readers in `vertebra.storage._arrow` pass `read_ahead=0` to `open_reader` so row-group demand paging stays bounded; keep that default when adding new demand-paged codecs.
- Use `overwrite=False` for create-only copy and rename only with providers supporting the configured conditional primitive. Handle backend refusal explicitly; a successful cloud rename does not provide an atomic move.

## Open Limitations

`get_file` durability (`fsync`), cancellation cleanup pool windows, `300 s` total client request timeouts, `verify_presigned_url` support on `gcs` / `azure` / `oci`, `DjangoStorage` created/accessed timestamps, and upstream SQLSpec `<= 0.71.1` pipeline buffering are open limitations. The R2 presigned-host rule, whole-segment `list(prefix)` matching, progressive `VerifyingStream` EOF checks, and trailing-`/` normalization follow provider and `object_store` semantics and are documented behavior rather than engine defects.

[^py-store]: `src/py/vertebra/storage/_store.py::SyncObjectStore` — Python `SyncObjectStore` and `AsyncObjectStore` facades.
[^storage-lib-rs]: `src/rs/vertebra-storage/src/lib.rs::extract_buffer_bytes` — PyO3 buffer extraction for `vertebra._storage`.
[^storage-store-rs]: `src/rs/vertebra-storage/src/store.rs::ObjectStoreEngine` — Rust `ObjectStoreEngine` file I/O and compose.
[^storage-oci-rs]: `src/rs/vertebra-storage/src/oci.rs::OciObjectStore` — Native OCI Object Storage REST backend.
[^storage-cargo]: `src/rs/vertebra-storage/Cargo.toml` — `object_store = "0.13.2"` dependency with the `aws`, `gcp`, `azure`, and `http` features.
[^ext-django-storage]: `src/py/vertebra/extensions/django/storage.py::DjangoStorage` — Django storage backend integration.
[^ext-sqlspec-storage]: `src/py/vertebra/extensions/sqlspec/storage.py::ObjectStoreBackend` — SQLSpec storage backend and direct Arrow write support.
