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
---

# Object Storage Implementation Limitations

<!-- truth: start -->
- `R2Config.custom_domain` is forwarded as a store option, but presigned URLs always target `https://<account_id>.r2.cloudflarestorage.com` because Cloudflare only honors presigned URLs on the S3 API domain ([Cloudflare R2 presigned URLs](https://developers.cloudflare.com/r2/api/s3/presigned-urls/)) [^py-store].
- Native `list(prefix)` matches whole path segments, so `glob()` lists the literal text before the first wildcard truncated to its last `/` and filters the full pattern with `fnmatch.fnmatchcase` [^py-store].
- Native `put` normalizes away trailing `/`, so directory placeholder keys cannot be created through `vertebra.storage`; `glob()` still skips placeholder keys returned by external writers [^py-store].
- Native `extract_buffer_bytes` accepts only byte-format buffers (`B`, `b`, `c`); the Python facade casts typed buffers (`pyarrow.Buffer`, `array.array`, NumPy) to a flat `B` view before crossing FFI, while `bytes` and `StorageBuffer` stay zero-copy [^storage-lib-rs] [^py-store].
- `ObjectStoreEngine::get_file` flushes and atomically renames the `.part` file but does not call `fsync`; durability across power loss depends on the filesystem [^storage-store-rs].
- Cancellation cleanup (`PartFileGuard`, `CreatedFilesGuard`, `MultipartUploadSession` `Drop`, `ComposeTemporariesGuard`) runs when an aborted Tokio task drops its future; a `tokio::fs` open or rename already running on the blocking pool can still complete after the guard runs, so a `.part` or target file can survive that narrow window [^storage-store-rs].
- `OciObjectStore::copy_opts` fails closed with `Generic` when `copyObject` returns `202` without an `opc-work-request-id` header because completion cannot be confirmed [^storage-oci-rs].
- Create-only `copy(..., overwrite=False)` and `rename(..., overwrite=False)` are not guaranteed on `s3`/`b2` and `r2`: `ObjectStoreEngine::copy` and `ObjectStoreEngine::rename` call `object_store` `copy_if_not_exists` / `rename_if_not_exists`, and the `AmazonS3Builder` stores are built without an `S3CopyIfNotExists` strategy, so `object_store 0.13.2` returns `NotSupported` (surfaced as a generic storage error, not `FileExistsError`) instead of performing a conditional copy. Only the `file` scheme falls back to a create-only put of the source bytes on `NotImplemented` [^storage-store-rs] [^storage-cargo].
- Pooled storage HTTP clients apply a `300 s` total request timeout that also bounds streamed response bodies, so single very large downloads on slow links must use `get_range` windows or `open_reader` [^storage-oci-rs] [^storage-store-rs].
<!-- truth: end -->

## Caller Guidance

- Serve public R2 objects through the custom domain with unsigned URLs or Cloudflare access rules; use presigned URLs only for the S3 API host.
- Prefer `open_reader(..., read_ahead=...)` or bounded `get_range` loops for multi-gigabyte objects on high-latency links.
- Parquet batch readers in `vertebra.storage._arrow` pass `read_ahead=0` to `open_reader` so row-group demand paging stays bounded; keep that default when adding new demand-paged codecs.
- On S3-compatible backends, do not rely on `overwrite=False` for mutual exclusion; use `put(..., if_not_exists=True)` only where the backend supports conditional puts, or coordinate writers outside the store.

## Planned Fixes

The `storage-integrity-and-streaming` flow owns the create-only `copy` and `rename` gap on S3-compatible backends and the integrity and streaming caveats on this page (`get_file` durability, cancellation cleanup windows, and the total request timeout on streamed bodies). The R2 presigned-host rule, whole-segment `list(prefix)` matching, and trailing-`/` normalization follow provider and `object_store` semantics and are documented behavior rather than defects.

[^py-store]: `src/py/vertebra/storage/_store.py::SyncObjectStore` — Python `SyncObjectStore` and `AsyncObjectStore` facades.
[^storage-lib-rs]: `src/rs/vertebra-storage/src/lib.rs::extract_buffer_bytes` — PyO3 buffer extraction for `vertebra._storage`.
[^storage-store-rs]: `src/rs/vertebra-storage/src/store.rs::ObjectStoreEngine` — Rust `ObjectStoreEngine` file I/O and compose.
[^storage-oci-rs]: `src/rs/vertebra-storage/src/oci.rs::OciObjectStore` — Native OCI Object Storage REST backend.
[^storage-cargo]: `src/rs/vertebra-storage/Cargo.toml` — `object_store = "0.13.2"` dependency with the `aws`, `gcp`, `azure`, and `http` features.
