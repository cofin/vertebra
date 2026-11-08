---
type: Task
id: "storage-remediation:ch02:t04"
chapter_id: ch02-zero-copy-ffi-keys-and-backends
title: Upgrade OciObjectStore to stream get_opts, honor conditional and copy headers, and paginate list and list_with_delimiter
description: Stream OCI GET bodies via resp.bytes_stream(), enforce GetOptions, PutOptions, and CopyOptions HTTP headers and 304/412 mappings, parse list keys with StorePath::parse, and paginate list and list_with_delimiter using nextStartWith and delimiter=/.
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "storage-remediation:ch02:t02"
read_set:
  - src/rs/vertebra-storage/src/oci.rs
  - src/rs/vertebra-storage/tests/storage_engine_tests.rs
files:
  - src/rs/vertebra-storage/src/oci.rs
tests:
  - src/rs/vertebra-storage/tests/storage_engine_tests.rs
estimated_loc: 235
plan_revision: 1
plan_commit: fd571e3a
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-08T20:55:00Z"
updated_at: "2026-11-08T20:55:00Z"
commit: null
generated:
  by: flow/0.28.0
  at: "2026-11-08T20:55:00Z"
verified: []
sources:
  - id: storage-oci-rs
    resource: src/rs/vertebra-storage/src/oci.rs::OciObjectStore
    title: Native OCI Object Storage REST implementation
tags:
  - storage
  - oci
  - streaming
  - pagination
  - tdd
---

# Task t04: Streaming, Conditional, and Paginated `OciObjectStore`

## Objective & Scope Boundary

Upgrade `src/rs/vertebra-storage/src/oci.rs` [^storage-oci-rs] (`1` file) to full `object_store::ObjectStore` streaming, conditional, and pagination parity:
1. Replace `resp.bytes().await` in `get_opts` with `GetResultPayload::Stream(resp.bytes_stream().map_err(...).boxed())`, and support `GetOptions` (`options.head` -> `HEAD` request, `options.range` -> RFC 9110 `Range` header + `206` range bounds, `options.if_match` / `options.if_none_match` / `options.if_modified_since` / `options.if_unmodified_since`, mapping `304` -> `Error::NotModified` and `412` -> `Error::Precondition`).
2. Honor `PutOptions` in `put_opts`: `PutMode::Create` -> `if-none-match: *`, `PutMode::Update(v)` -> `if-match: <e_tag or version>`, `opts.content_type` -> `content-type`, and `opts.attributes` -> `opc-meta-*` headers, mapping HTTP `412` to `object_store::Error::AlreadyExists` (for `Create`) or `object_store::Error::Precondition` (for `Update`).
3. Honor `CopyOptions` (`CopyMode::Create` -> `destinationObjectIfNoneMatchETag: "*"` in the `/actions/copyObject` JSON body, mapping `412` -> `Error::AlreadyExists`).
4. Paginate `list` (`nextStartWith` loop until `None`, parsing keys via `StorePath::parse`) and implement `list_with_delimiter` using OCI `delimiter=/` + `prefixes` across pages.

Out of scope: GCS/S3 backends (`ch01:t02`, `ch02:t02`).

## Bounded Read Manifest

- `src/rs/vertebra-storage/src/oci.rs` — `OciObjectStore` trait methods and Cavage signer integration [^storage-oci-rs].
- `src/rs/vertebra-storage/tests/storage_engine_tests.rs` — Loopback OCI test harness.

## Symbol & Signature Contracts

- `impl ObjectStore for OciObjectStore`: `put_opts`, `get_opts`, `copy_opts`, `list`, `list_with_delimiter` (`object_store 0.13` trait methods).

## Edge Cases & Error Semantics

- `PutMode::Update` when neither `e_tag` nor `version` is provided returns `object_store::Error::NotImplemented` / `Precondition` rather than silently overwriting unconditionally.
- `list_with_delimiter(Some("dir"))` normalizes prefix to `"dir/"` for OCI `prefix=` query parameter and deduplicates `prefixes` across paginated responses.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/rs/vertebra-storage/tests/storage_engine_tests.rs`.
- **Worked Test Matrix**:

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Streaming GET & Range/Conditional `get_opts` | Loopback OCI server serving chunked body, `206 Partial Content` on `Range: bytes=2-5`, and `412` on `if-match` mismatch | `get_stream` yields chunks incrementally; `get_range` returns exact 4 bytes; `if-match` mismatch returns `StorageError::Precondition`. |
| Conditional `put_opts` & `copy_if_not_exists` | `put_opts` with `if_not_exists=true` (sends `if-none-match: *`) and `if_match="etag-1"` (sends `if-match: etag-1`); `copy_if_not_exists` | Sends expected headers and maps `412` to `AlreadyExists` / `Precondition`. |
| Multi-page `list` & `list_with_delimiter` | Loopback OCI server returning page 1 (`nextStartWith: "k2"`, `prefixes: ["sub/"]`) and page 2 (`nextStartWith: null`) | `list` returns all objects across both pages with `~`/`%25` keys intact; `list_with_delimiter` returns objects + `["sub"]`. |

## Verification

```bash
PATH="$PWD/.venv/bin:$PATH" PYO3_PYTHON="$PWD/.venv/bin/python" CARGO_TARGET_DIR=/tmp/vtb-cargo-target cargo test --manifest-path src/rs/Cargo.toml -p vertebra-storage oci
```

## Acceptance Criteria

- [ ] `OciObjectStore` streams `get_opts`, honors `GetOptions`, `PutOptions`, and `CopyOptions`, and maps `304`/`412` accurately (R2.4).
- [ ] `OciObjectStore::list` and `list_with_delimiter` paginate `nextStartWith` to exhaustion and preserve raw keys via `StorePath::parse` (R2.4).

## Notes & Discoveries

- [2026-11-08 20:55] Reuse `build_http_client_options` / pooled `reqwest::Client` settings in `OciObjectStore` so OCI connections pool identically to GCS/S3.

[^storage-oci-rs]: `src/rs/vertebra-storage/src/oci.rs::OciObjectStore` — Native OCI Object Storage REST implementation.
