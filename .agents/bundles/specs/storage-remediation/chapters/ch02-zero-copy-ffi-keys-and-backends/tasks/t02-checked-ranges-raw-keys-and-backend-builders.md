---
type: Task
id: "storage-remediation:ch02:t02"
chapter_id: ch02-zero-copy-ffi-keys-and-backends
title: Check get_range bounds, preserve raw keys with StorePath::parse, preserve file:// conditional metadata, and wire S3/HTTP options in Rust
description: Replace StorePath::from with StorePath::parse in store.rs, guard get_range overflow and zero-length reads, preserve content_type and attributes on file:// conditional puts, forward S3 session_token/virtual_hosted_style and Http headers, and add virtual_hosted_style to S3SignConfig.
status: stable
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "storage-remediation:ch02:t01"
read_set:
  - src/rs/vertebra-storage/src/store.rs
  - src/rs/vertebra-storage/src/signing.rs
  - src/rs/vertebra-storage/src/lib.rs
  - src/rs/vertebra-storage/tests/storage_engine_tests.rs
files:
  - src/rs/vertebra-storage/src/store.rs
  - src/rs/vertebra-storage/src/signing.rs
  - src/rs/vertebra-storage/src/lib.rs
tests:
  - src/rs/vertebra-storage/tests/storage_engine_tests.rs
estimated_loc: 180
plan_revision: 1
plan_commit: fd571e3a
state_revision: 2
claimed_by: executor
claimed_at: "2026-11-08T23:30:00Z"
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: "2026-11-08T23:39:30Z"
last_verified_commit: fb8dde11
verification_evidence: "cargo test -p vertebra-storage (28 passed) && cargo clippy -p vertebra-storage --all-targets -- -D warnings && make review-check"
created_at: "2026-11-08T20:55:00Z"
updated_at: "2026-11-08T23:39:45Z"
commit: fb8dde11
generated:
  by: flow/0.28.0
  at: "2026-11-08T20:55:00Z"
verified: []
sources:
  - id: storage-store-rs
    resource: src/rs/vertebra-storage/src/store.rs::ObjectStoreEngine
    title: ObjectStoreEngine key parsing, range reads, and backend builders
  - id: storage-signing-rs
    resource: src/rs/vertebra-storage/src/signing.rs::S3SignConfig
    title: S3 SigV4 URL presigning and verification
tags:
  - storage
  - s3
  - keys
  - ranges
  - tdd
---

# Task t02: Checked `get_range`, Raw `StorePath::parse` Keys, `file://` Conditional Metadata, and S3/HTTP Options

## Objective & Scope Boundary

Update `src/rs/vertebra-storage/src/store.rs` [^storage-store-rs] and `src/rs/vertebra-storage/src/signing.rs` [^storage-signing-rs] (`2` files):
1. Replace `StorePath::from(trimmed)` with `StorePath::parse(trimmed).map_err(|e| StorageError::InvalidInput(format!("invalid object path '{path}': {e}")))` in `validate_object_path` and `validate_optional_prefix` so `~` and `%25` are preserved verbatim without double percent-encoding.
2. In `ObjectStoreEngine::get_range(path, offset, length)`: check `let end = offset.checked_add(length).ok_or_else(|| StorageError::InvalidInput("byte range offset + length overflow".into()))?;`; when `length == 0`, call `self.head(path).await?; return Ok(Bytes::new());`.
3. Ensure `file://` conditional put fallback (`NotImplemented` branch in `put_opts`) preserves `content_type` and `attributes` on returned `StoredObjectMeta` and writes sidecar metadata if applicable or returns consistent `StoredObjectMeta`.
4. Wire `session_token` (`builder.with_token(tok)`) and `virtual_hosted_style` (`builder.with_virtual_hosted_style_request(true)`) in `BackendConfig::S3`, wire custom `headers` (`headers` JSON object or `header.<name>` options -> `HeaderMap` on `ClientOptions::with_default_headers`) in `BackendConfig::Http`, and add `pub virtual_hosted_style: bool` to `S3SignConfig` (`sign_s3_v4_url` and `verify_s3_v4_url`).

Out of scope: PyO3 `py_sign_s3_v4_url` / `py_verify_s3_v4_url` arguments (`t03`).

## Bounded Read Manifest

- `src/rs/vertebra-storage/src/store.rs` — `validate_object_path`, `validate_optional_prefix`, `BackendConfig`, `ObjectStoreEngine::put_opts`, `ObjectStoreEngine::get_range` [^storage-store-rs].
- `src/rs/vertebra-storage/src/signing.rs` — `S3SignConfig`, `sign_s3_v4_url`, `verify_s3_v4_url` [^storage-signing-rs].
- `src/rs/vertebra-storage/tests/storage_engine_tests.rs` — Rust integration tests.

## Symbol & Signature Contracts

- `S3SignConfig { pub bucket: String, pub region: String, pub access_key_id: String, pub secret_access_key: String, pub endpoint: Option<String>, pub virtual_hosted_style: bool }`
- `BackendConfig::S3 { ..., session_token: Option<String>, virtual_hosted_style: bool, ... }`
- `BackendConfig::Http { url: String, allow_http: bool, headers: HashMap<String, String> }`

## Edge Cases & Error Semantics

- `get_range("missing.bin", 0, 0)` returns `StorageError::ObjectStore(NotFound)` (`FileNotFoundError`), whereas `get_range("exists.bin", 0, 0)` returns `Ok(Bytes::new())` with zero invalid `bytes=0--1` HTTP request.
- `get_range("exists.bin", 1, u64::MAX)` returns `Err(StorageError::InvalidInput(...))`.
- Virtual-hosted S3 signing for `bucket = "assets"`, `region = "us-east-1"`, `endpoint = None` signs `host:assets.s3.us-east-1.amazonaws.com` and path `/<encoded_key>`; with custom `endpoint = Some("https://s3.example.com:9000")` and `virtual_hosted_style = true`, signs `host:assets.s3.example.com:9000` and path `/<encoded_key>`.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/rs/vertebra-storage/tests/storage_engine_tests.rs`.
- **Worked Test Matrix**:

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| `get_range` overflow & `length == 0` | `engine.get_range("k", 5, u64::MAX)` and `engine.get_range("k", 5, 0)` | Overflow returns `Err(StorageError::InvalidInput(_))`; `length == 0` returns `Ok(b"")` when `"k"` exists and `NotFound` when missing. |
| Raw key with `~` and `%25` | `engine.put("a~b/c%25d.txt", b"1")` + `engine.list(None)` | Listed `meta.path == "a~b/c%25d.txt"`. |
| S3 virtual-hosted SigV4 & session token + HTTP custom headers | Loopback HTTP server inspecting `Host`, `x-amz-security-token`, and custom `X-Api-Key` header | Virtual-hosted URL verifies cleanly; `S3` sends `x-amz-security-token`; `Http` sends `x-api-key`. |

## Verification

```bash
PATH="$PWD/.venv/bin:$PATH" PYO3_PYTHON="$PWD/.venv/bin/python" CARGO_TARGET_DIR=/tmp/vtb-cargo-target cargo test --manifest-path src/rs/Cargo.toml -p vertebra-storage
```

## Acceptance Criteria

- [x] `get_range` guards `u64` overflow and `length == 0`, and `StorePath::parse` preserves `~` and `%` keys without double-encoding (R2.2).
- [x] `S3` `session_token` + `virtual_hosted_style` (engine + `S3SignConfig`) and `Http` custom `headers` pass loopback HTTP tests (R2.2).

## Notes & Discoveries

- [2026-11-08 20:55] Initialize `virtual_hosted_style: false` temporarily in `lib.rs` `S3SignConfig` literals if needed for compilation until `t03` exposes the Python keyword argument.

[^storage-store-rs]: `src/rs/vertebra-storage/src/store.rs::ObjectStoreEngine` — `ObjectStoreEngine` key parsing, range reads, and backend builders.
[^storage-signing-rs]: `src/rs/vertebra-storage/src/signing.rs::S3SignConfig` — S3 SigV4 URL presigning and verification.
