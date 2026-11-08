---
type: Task
id: "storage-remediation:ch02:t03"
chapter_id: ch02-zero-copy-ffi-keys-and-backends
title: Wire S3 virtual_hosted_style PyO3 bindings and stubs and remove Litestar key unquote workaround
description: Expose virtual_hosted_style on py_sign_s3_v4_url and py_verify_s3_v4_url in lib.rs and _storage.pyi, and remove urllib.parse.unquote from extensions/litestar/store.py now that StorePath::parse preserves raw keys.
status: stable
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "storage-remediation:ch02:t02"
read_set:
  - src/rs/vertebra-storage/src/lib.rs
  - src/py/vertebra/_storage.pyi
  - src/py/vertebra/extensions/litestar/store.py
  - src/py/tests/unit/storage/test_signing.py
  - src/py/tests/unit/extensions/test_litestar_plugin.py
files:
  - src/rs/vertebra-storage/src/lib.rs
  - src/py/vertebra/_storage.pyi
  - src/py/vertebra/extensions/litestar/store.py
tests:
  - src/py/tests/unit/storage/test_signing.py
  - src/py/tests/unit/extensions/test_litestar_plugin.py
estimated_loc: 85
plan_revision: 1
plan_commit: fd571e3a
state_revision: 2
claimed_by: executor
claimed_at: "2026-11-08T23:43:30Z"
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: "2026-11-08T23:48:07Z"
last_verified_commit: 8eb43fb2
verification_evidence: "mypy.stubtest vertebra._storage passed; pytest test_signing.py + test_litestar_plugin.py (54 passed); ruff, mypy, clippy, rustfmt, review-check passed"
created_at: "2026-11-08T20:55:00Z"
updated_at: "2026-11-08T23:48:07Z"
commit: 8eb43fb2
generated:
  by: flow/0.28.0
  at: "2026-11-08T20:55:00Z"
verified:
  - by: executor
    at: "2026-11-08T23:48:07Z"
    commit: 8eb43fb2
sources:
  - id: storage-lib-rs
    resource: src/rs/vertebra-storage/src/lib.rs::py_sign_s3_v4_url
    title: PyO3 S3 SigV4 signing and verification bindings
  - id: storage-pyi
    resource: src/py/vertebra/_storage.pyi::py_sign_s3_v4_url
    title: Type stubs for vertebra._storage
  - id: litestar-store
    resource: src/py/vertebra/extensions/litestar/store.py::ObjectStoreLitestarStore
    title: Litestar store over ObjectStore
tags:
  - storage
  - s3
  - litestar
  - ffi
  - tdd
---

# Task t03: Wire S3 `virtual_hosted_style` PyO3 Bindings & Remove Litestar `unquote` Workaround

## Objective & Scope Boundary

Expose `virtual_hosted_style: bool = False` on `py_sign_s3_v4_url` and `py_verify_s3_v4_url` in `src/rs/vertebra-storage/src/lib.rs` [^storage-lib-rs] and `src/py/vertebra/_storage.pyi` [^storage-pyi], and remove `from urllib.parse import unquote` and all `unquote(...)` calls from `src/py/vertebra/extensions/litestar/store.py` [^litestar-store] so keys containing `%25` or `~` round-trip without corruption.

Out of scope: Python `S3Config` / `SigningConfig` `virtual_hosted_style` forwarding in `_store.py` (`ch03:t03`).

## Bounded Read Manifest

- `src/rs/vertebra-storage/src/lib.rs` — `py_sign_s3_v4_url`, `py_verify_s3_v4_url` [^storage-lib-rs].
- `src/py/vertebra/_storage.pyi` — `py_sign_s3_v4_url`, `py_verify_s3_v4_url` [^storage-pyi].
- `src/py/vertebra/extensions/litestar/store.py` — `ObjectStoreLitestarStore` and filesystem adapter key listings [^litestar-store].
- `src/py/tests/unit/storage/test_signing.py` — S3 SigV4 signing tests.
- `src/py/tests/unit/extensions/test_litestar_plugin.py` — Litestar store key round-trip tests.

## Symbol & Signature Contracts

- `py_sign_s3_v4_url(..., endpoint: str | None = None, virtual_hosted_style: bool = False) -> str`
- `py_verify_s3_v4_url(..., endpoint: str | None = None, virtual_hosted_style: bool = False) -> bool`

## Edge Cases & Error Semantics

- Litestar store keys containing literal `%25` (e.g. `"rate%25limit"`) and `~` (e.g. `"session~abc"`) must survive `set`, `get`, `exists`, and `delete_all` without turning `%25` into `%`.

## Public-Seam Test Blueprint

- **Canonical test files**: `src/py/tests/unit/storage/test_signing.py` and `src/py/tests/unit/extensions/test_litestar_plugin.py`.
- **Worked Test Matrix**:

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Virtual-hosted S3 SigV4 sign & verify | `py_sign_s3_v4_url("my-bkt", "us-east-1", "AKIA...", "secret", "GET", "a/b.txt", 900, 1700000000, None, True)` | Starts with `https://my-bkt.s3.us-east-1.amazonaws.com/a/b.txt?` and `py_verify_s3_v4_url(..., True)` returns `True`. |
| Litestar store `%25` & `~` keys | `await store.set("user~1%25a", b"ok")`; `await store.get("user~1%25a")`; `await store.delete_all()` | Returns `b"ok"` and `delete_all()` deletes `"user~1%25a"` cleanly without `FileNotFoundError`. |

## Verification

```bash
PATH="$PWD/.venv/bin:$PATH" PYO3_PYTHON="$PWD/.venv/bin/python" CARGO_TARGET_DIR=/tmp/vtb-cargo-target uv run --offline --no-sync maturin develop --uv --offline --manifest-path src/rs/vertebra-storage/Cargo.toml
uv run --no-sync python -m mypy.stubtest vertebra._storage --allowlist tools/stubtest/allowlist.txt
uv run --no-sync ruff check src/py/vertebra/extensions/litestar/store.py src/py/tests/unit/storage/test_signing.py src/py/tests/unit/extensions/test_litestar_plugin.py
uv run --no-sync pytest src/py/tests/unit/storage/test_signing.py src/py/tests/unit/extensions/test_litestar_plugin.py -q
```

## Acceptance Criteria

- [x] [8eb43fb2] `py_sign_s3_v4_url` and `py_verify_s3_v4_url` support `virtual_hosted_style` and pass `mypy.stubtest vertebra._storage` (R2.3).
- [x] [8eb43fb2] `extensions/litestar/store.py` contains zero `unquote` calls and preserves `%25` and `~` keys end-to-end (R2.3).

## Notes & Discoveries

- [2026-11-08 20:55] Keep `virtual_hosted_style` defaulted to `False` at the low-level `py_sign_s3_v4_url` binding for path-style custom endpoints while `S3Config` forwards its boolean setting in `ch03:t03`.

[^storage-lib-rs]: `src/rs/vertebra-storage/src/lib.rs::py_sign_s3_v4_url` — PyO3 S3 SigV4 signing and verification bindings.
[^storage-pyi]: `src/py/vertebra/_storage.pyi::py_sign_s3_v4_url` — Type stubs for `vertebra._storage`.
[^litestar-store]: `src/py/vertebra/extensions/litestar/store.py::ObjectStoreLitestarStore` — Litestar store over `ObjectStore`.
