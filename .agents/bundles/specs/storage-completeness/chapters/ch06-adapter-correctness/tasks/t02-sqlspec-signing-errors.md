---
type: Task
id: storage-completeness:ch06:t02
chapter_id: ch06-adapter-correctness
title: Propagate configured signing validation failures
description: Propagate configured signing validation failures
status: stable
state: completed
priority: P1
verification_strategy: regression_tdd
depends_on: []
read_set:
- src/py/vertebra/extensions/sqlspec/storage.py
- src/py/tests/unit/extensions/test_data_storage_integrations.py
- src/py/vertebra/storage/_store.py
- src/py/tests/unit/storage/test_signing.py
files:
- src/py/vertebra/extensions/sqlspec/storage.py
tests:
- src/py/tests/unit/extensions/test_data_storage_integrations.py
estimated_loc: 75
plan_revision: 4
plan_commit: 88d5068031ba82078f2f7c4999cdf3cfd77c66be
state_revision: 0
claimed_by: agent:sqlspec_signing_repair
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: Complete chapter documentation sync and independent reviews.
last_verified_at: "2026-11-07T22:31:17Z"
last_verified_commit: 45b068e2
verification_evidence: >-
  Red: flock /tmp/vertebra-flow-shm-tests.lock uv run --no-sync pytest
  src/py/tests/unit/extensions/test_data_storage_integrations.py -q -k
  'signing_preserves or signing_unsupported or signing_retains' --disable-warnings
  --tb=short exited 1 (8 failed, 6 passed) before production edits. Native exact
  IAM prefix correction is independently approved. Green: flock
  /tmp/vertebra-flow-shm-tests.lock uv run --no-sync pytest
  src/py/tests/unit/extensions/test_data_storage_integrations.py -q
  --disable-warnings --tb=short exited 0 (37 passed). Ruff check and format --check
  on the two owned Python files exited 0; source Mypy exited 0 (1 file, no issues);
  git diff --check exited 0. Current rebuilt native artifacts were used unchanged;
  no Rust edit or rebuild is needed. Production diff is 12 added/14 removed lines;
  public signatures are unchanged. Signed source 45b068e2 is verified. Combined current-source storage/Django/data integration checkpoint exits 0 with 88 passed.
created_at: '2026-11-07T12:00:00Z'
updated_at: '2026-11-07T12:00:00Z'
commit: 45b068e2
sources:
- id: implementation
  resource: src/py/vertebra/extensions/sqlspec/storage.py
  title: Owned implementation seam
tags:
- storage
- repair
---

# Propagate configured signing validation failures

## Objective & Scope Boundary

Propagate configured signing validation failures. Execute one bounded worksheet and create one signed local conventional commit after verification. Read the existing functional baseline at `5fe477fa`; a reopened ch01 is not an execution prerequisite. Own only the listed production and canonical test files. [^implementation]

## Bounded Read Manifest

- `src/py/vertebra/extensions/sqlspec/storage.py` — owned implementation or canonical regression surface.
- `src/py/tests/unit/extensions/test_data_storage_integrations.py` — owned implementation or canonical regression surface.
- `src/py/vertebra/storage/_store.py` — collaborating contract.
- `src/py/tests/unit/storage/test_signing.py` — existing local IAM responder/native signing fixture pattern.

## Symbol & Signature Contracts

`ObjectStoreBackend._sign_single` and `_sign_single_async` check the configured store's public supports_signing capability and raise NotImplementedError only when signing is unavailable. Remove broad RuntimeError/ValueError conversion. Delegate supported presign_get/presign_put validation unchanged so invalid expiry, credentials or provider operation errors retain their exact type. Keep scalar/list return shape and for_upload HTTP method. No error-message substring classification.

Caller → seam → collaborator: public sync/async object-store or adapter operation → owned symbols above → existing native engine/provider or upstream framework contract. Preserve public APIs outside this task.

## Edge Cases & Error Semantics

The matrix below specifies observable failure behavior. Preserve the original error when cleanup also fails; retain ownership until abort or cleanup completes. No whole-object Vec/list/spool, size-cap fallback, Python lock, SDK fork, auth reimplementation or silent alias is permitted.

## Public-Seam Test Blueprint

Use the listed canonical test files and actual public store/framework calls. Do not assert source text or merely duplicate a helper implementation.

| Case | Input | Observable result |
| --- | --- | --- |
| Configured validation | MemoryConfig with explicit HMAC signing; sign_sync/sign_async("key", expires_in=0) | ValueError propagates through GET and upload paths |
| Unsupported backend | Unsigned explicit MemoryConfig; scalar and list sign calls | NotImplementedError with backend context |
| Successful configured HMAC | Positive expiry and for_upload=True/False | Valid signed GET/PUT URL; scalar/list shape retained |
| Configured native provider failure | A real `SyncObjectStore("memory://", signing=SigningConfig(provider="gcs", bucket="unit", service_account_email="signer@example.test", access_token="mock-oauth-token", iam_endpoint=local_server))`; local IAM responder returns HTTP 503 and body b"signing-unavailable" | Public `sign_sync` and `sign_async`, each with `for_upload=False/True`, preserve exact `RuntimeError` type (not its `NotImplementedError` subclass), with the unchanged native message exactly "signing error: IAM signBlob returned HTTP 503 Service Unavailable"; native signing does not include the responder body. No external credentials or mocked SUT/store methods. |

## Verification

Record the failing regression before implementation. Use this task's reserved target directory in each native command: `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target"` and `PYO3_PYTHON="$PWD/.venv/bin/python3.12"`. Run `cargo fmt --manifest-path src/rs/Cargo.toml --all --check` and `cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-storage --all-targets -- -D warnings` for native changes. Run `make dev` with those same environment values before Python native regressions and stubtest. Execute `uv run --no-sync pytest src/py/tests/unit/extensions/test_data_storage_integrations.py -q`. For Python ownership run `uv run --no-sync ruff check src/py/vertebra/extensions/sqlspec/storage.py src/py/tests/unit/extensions/test_data_storage_integrations.py` and `uv run --no-sync mypy src/py/vertebra/extensions/sqlspec/storage.py`; omit these two commands for Rust-only tasks. Record exact commands and exit status in verification_evidence.

## Acceptance Criteria

- [x] Every matrix row passes at its public seam with existing behavior preserved.
- [x] Only owned files change; bounds stay at read_set <= 6, production files <= 3 and estimated implementation LOC <= 250.
- [x] User-facing usage and native guarantees are documented by ch04:t03 and ch09; this task records its observable contract in the canonical regression tests.
- [x] A signed local conventional commit and verification evidence are recorded; no hosted action is performed.

## Notes & Discoveries

Configured signing delegates directly after checking the corresponding native facade's `supports_signing` capability. HMAC validation `ValueError` and real IAM HTTP 503 `RuntimeError` retain exact native types and messages through sync and async GET/upload calls. Unsupported scalar/list operations retain backend context, and successful HMAC scalar/list results verify the HTTP method and qualified key through the real store verifier. The local responder consumes actual native requests and shuts down deterministically. Production uses no exception-message classification, public signature changes, or native source changes.

The focused pre-edit regression selection reports eight failures and six passing characterization cases. The full canonical module passes all 37 tests after the repair; Ruff, formatting, Mypy and diff checks pass. Source signing and task completion remain owned by the parent coordinator.

[^implementation]: `src/py/vertebra/extensions/sqlspec/storage.py` — Current repository source for this task.
