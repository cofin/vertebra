---
type: Task
id: storage-completeness:ch06:t02
chapter_id: ch06-adapter-correctness
title: Propagate configured signing validation failures
description: Propagate configured signing validation failures
status: stable
state: in_progress
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
next_step: Execute the independently approved native-message-preserving repair.
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: '2026-11-07T12:00:00Z'
updated_at: '2026-11-07T12:00:00Z'
commit: null
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

- [ ] Every matrix row passes at its public seam with existing behavior preserved.
- [ ] Only owned files change; bounds stay at read_set <= 6, production files <= 3 and estimated implementation LOC <= 250.
- [ ] User-facing usage and native guarantees are documented by ch04:t03 and ch09; this task records its observable contract in the canonical regression tests.
- [ ] A signed local conventional commit and verification evidence are recorded; no hosted action is performed.

## Notes & Discoveries

Red: the focused public-seam selection reports 8 failed and 6 passed. Native IAM errors observed through the current adapter have cause text `signing error: IAM signBlob returned HTTP 503 Service Unavailable`; the worksheet incorrectly requires the same message without the native `signing error: ` prefix. The exact prefix is retained by the independently approved bounded correction; the same public failure cases now proceed to implementation. Any implementation contract contradiction routes through `/flow:revise storage-completeness` before production edits.

[^implementation]: `src/py/vertebra/extensions/sqlspec/storage.py` — Current repository source for this task.
