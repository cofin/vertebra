---
type: Task
id: storage-completeness:ch07:t01
chapter_id: ch07-remove-r2-python
title: Remove R2 from framework backend classifiers
description: Remove R2 from framework backend classifiers
status: stable
state: completed
priority: P1
verification_strategy: regression_tdd
depends_on: []
read_set:
- src/py/vertebra/extensions/alchemy/storage.py
- src/py/vertebra/extensions/sqlspec/storage.py
- src/py/vertebra/extensions/starlette/storage.py
- src/py/tests/unit/extensions/test_data_storage_integrations.py
- src/py/vertebra/storage/_store.py
files:
- src/py/vertebra/extensions/alchemy/storage.py
- src/py/vertebra/extensions/sqlspec/storage.py
- src/py/vertebra/extensions/starlette/storage.py
tests:
- src/py/tests/unit/extensions/test_data_storage_integrations.py
estimated_loc: 55
plan_revision: 6
plan_commit: 43c2e3fe
state_revision: 0
claimed_by: agent:python-framework-specialist
claimed_at: '2026-11-07T22:20:00Z'
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: '2026-11-07T22:37:24Z'
last_verified_commit: 4486f6fe
verification_evidence:
  source_checkpoint: Signed classifier and canonical-test commit 4486f6fe
  red: 'flock /tmp/vertebra-flow-shm-tests.lock uv run --no-sync pytest src/py/tests/unit/extensions/test_data_storage_integrations.py -q -k framework_storage: exit 1; 4 failed (DID NOT RAISE expected TypeError/ImproperConfigurationError), 4 passed, 37 deselected before production edits at 0f4b1895'
  native_build: 'UV_NO_SYNC=1 CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" make dev: exit 0; all five debug PyO3 extensions rebuilt/installed; Rust source unchanged'
  stubtest: 'uv run --no-sync python -m mypy.stubtest vertebra._storage: exit 0; no issues found in 1 module'
  ruff: 'uv run --no-sync ruff check over three owned adapter modules and canonical test: exit 0'
  format: 'uv run --no-sync ruff format --check over three owned adapter modules and canonical test: exit 0; 4 files already formatted'
  mypy: 'uv run --no-sync mypy over three owned adapter modules: exit 0; no issues found in 3 source files'
  green: 'flock /tmp/vertebra-flow-shm-tests.lock uv run --no-sync pytest src/py/tests/unit/extensions/test_data_storage_integrations.py -q: exit 0; 45 passed, 12 upstream SQLAlchemy deprecation warnings'
  diff_check: 'git diff --check: exit 0'
  platform_limits: none
created_at: '2026-11-07T12:00:00Z'
updated_at: '2026-11-07T22:37:24Z'
commit: 4486f6fe
sources:
- id: implementation
  resource: src/py/vertebra/extensions/alchemy/storage.py
  title: Owned implementation seam
tags:
- storage
- repair
---

# Remove R2 from framework backend classifiers

## Objective & Scope Boundary

Remove R2 from framework backend classifiers. Execute one bounded worksheet and create one signed local conventional commit after verification. Read the existing functional baseline at `5fe477fa`; a reopened ch01 is not an execution prerequisite. Own only the listed production and canonical test files. [^implementation]

## Bounded Read Manifest

- `src/py/vertebra/extensions/alchemy/storage.py` — owned implementation or canonical regression surface.
- `src/py/vertebra/extensions/sqlspec/storage.py` — owned implementation or canonical regression surface.
- `src/py/vertebra/extensions/starlette/storage.py` — owned implementation or canonical regression surface.
- `src/py/tests/unit/extensions/test_data_storage_integrations.py` — owned implementation or canonical regression surface.
- `src/py/vertebra/storage/_store.py` — collaborating contract.

## Symbol & Signature Contracts

Remove R2Config imports and entries in `_STORAGE_CONFIGS` from Advanced Alchemy, SQLSpec and Starlette. Remove r2:// advertised URI support in their docstrings. Keep S3Config with arbitrary explicit custom endpoints accepted; no provider hostname blacklist, alias or Cloudflare compatibility shim. This task precedes core Python API withdrawal so its imports stay valid at every commit.

Caller → seam → collaborator: public sync/async object-store or adapter operation → owned symbols above → existing native engine/provider or upstream framework contract. Preserve public APIs outside this task.

## Edge Cases & Error Semantics

The matrix below specifies observable failure behavior. Preserve the original error when cleanup also fails; retain ownership until abort or cleanup completes. No whole-object Vec/list/spool, size-cap fallback, Python lock, SDK fork, auth reimplementation or silent alias is permitted.

## Public-Seam Test Blueprint

Use the listed canonical test files and actual public store/framework calls. Do not assert source text or merely duplicate a helper implementation.

| Case | Input | Observable result |
| --- | --- | --- |
| Supported custom endpoint | Construct each adapter using S3Config(bucket="unit", endpoint="http://127.0.0.1:9000", allow_http=True, access_key_id="test-only-key", secret_access_key="test-only-secret") | Normal S3 classification retained |
| Removed first-class classifier | While the core class still exists at this frontier, pass explicit R2Config(bucket="unit", account_id="acct") to Alchemy ObjectStoreBackend, SQLSpec ObjectStoreBackend, and both Starlette registry registration APIs | Alchemy and both Starlette registries raise TypeError; SQLSpec preserves its existing ImproperConfigurationError for unsupported target types, all before native construction; valid custom-S3 config remains accepted by each corresponding API. URI rejection belongs to successor t02/t03, not this task. |

## Verification

Record the failing public regression before implementation and the passing result after. Execute each command below in this worktree; the storage target directory is reserved and shared SHM tests use the host lock.

- `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" make dev`
- `uv run --no-sync python -m mypy.stubtest vertebra._storage`
- `uv run --no-sync ruff check src/py/vertebra/extensions/alchemy/storage.py src/py/vertebra/extensions/sqlspec/storage.py src/py/vertebra/extensions/starlette/storage.py src/py/tests/unit/extensions/test_data_storage_integrations.py`
- `uv run --no-sync mypy src/py/vertebra/extensions/alchemy/storage.py src/py/vertebra/extensions/sqlspec/storage.py src/py/vertebra/extensions/starlette/storage.py`
- `uv run --no-sync pytest src/py/tests/unit/extensions/test_data_storage_integrations.py -q`

Record exact exit statuses, native build checkpoint and any platform limitation in verification_evidence. Review closure uses these results, never planning-only structural validation.

## Acceptance Criteria

- [x] Every matrix row passes at its public seam with existing behavior preserved.
- [x] Only owned files change; bounds stay at read_set <= 6, production files <= 3 and estimated implementation LOC <= 250.
- [x] User-facing usage and native guarantees are documented by ch04:t03 and ch09; this task records its observable contract in the canonical regression tests.
- [x] A signed local conventional commit and verification evidence are recorded; no hosted action is performed.

## Notes & Discoveries

Alchemy, SQLSpec and Starlette use the supported configuration types and preserve generic S3 endpoint behavior. The current canonical tests exercise URI rejection after public R2 configuration withdrawal; the temporary classifier case is represented by its recorded Red/Green receipt. Signed source and verification fields are complete.

[^implementation]: `src/py/vertebra/extensions/alchemy/storage.py` — Current repository source for this task.
