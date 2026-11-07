---
type: Task
id: storage-completeness:ch07:t02
chapter_id: ch07-remove-r2-python
title: Delete public R2Config and from_r2 convenience APIs
description: Delete public R2Config and from_r2 convenience APIs
status: stable
state: completed
priority: P1
verification_strategy: regression_tdd
depends_on:
- storage-completeness:ch07:t01
read_set:
- src/py/tests/unit/extensions/test_data_storage_integrations.py
- src/py/vertebra/storage/_store.py
- src/py/vertebra/storage/__init__.py
- src/py/tests/unit/storage/test_multicloud_storage.py
- src/py/tests/unit/storage/test_signing.py
- src/py/tests/integration/storage/conftest.py
files:
- src/py/vertebra/storage/_store.py
- src/py/vertebra/storage/__init__.py
tests:
- src/py/tests/unit/extensions/test_data_storage_integrations.py
- src/py/tests/unit/storage/test_multicloud_storage.py
- src/py/tests/unit/storage/test_signing.py
- src/py/tests/integration/storage/conftest.py
estimated_loc: 130
plan_revision: 5
plan_commit: 69a77955
state_revision: 0
claimed_by: agent:r2-public-api-withdrawal
claimed_at: '2026-11-07T22:42:52Z'
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: '2026-11-07T22:42:52Z'
last_verified_commit: 75f18e3b
verification_evidence: |
  Source checkpoint 767163e4 plus the owned uncommitted t02 diff. Red: the new
  public withdrawal and unsupported-URI cases exit 1 (7 failed, 56 deselected),
  exposing retained R2 symbols, native construction and framework acceptance.
  Green: the declared four-path pytest lane exits 0 (69 passed, 12 existing
  Advanced Alchemy/SQLAlchemy deprecation warnings, 5.51s). The final native
  constructor observer delegates to the real constructor and records zero calls
  for both rejected facade URIs. Generic S3 endpoint signing and all supported
  provider coverage pass through the real native extension.
  Native: CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target"
  PYO3_PYTHON="$PWD/.venv/bin/python3.12" UV_NO_SYNC=1 make dev exits 0;
  all five CPython 3.12 extensions are freshly built and installed at 767163e4.
  uv run --no-sync flock /tmp/vertebra-flow-shm-tests.lock python -m
  mypy.stubtest vertebra._storage exits 0 (1 module), retaining native R2
  helper/stub parity until ch08. Focused Mypy exits 0 (2 source files);
  Ruff check, Ruff format --check (6 files), and git diff --check exit 0.
  Runtime checks use uv run --no-sync and the shared SHM flock. No platform
  limitation or hosted action applies. Logs: /tmp/vertebra-ch07-t02-red.log,
  /tmp/vertebra-ch07-t02-green.log, /tmp/vertebra-ch07-t02-dev.log,
  /tmp/vertebra-ch07-t02-stubtest.log, /tmp/vertebra-ch07-t02-mypy.log.
created_at: '2026-11-07T12:00:00Z'
updated_at: '2026-11-07T12:00:00Z'
commit: 75f18e3b
sources:
- id: implementation
  resource: src/py/vertebra/storage/_store.py
  title: Owned implementation seam
tags:
- storage
- repair
---

# Delete public R2Config and from_r2 convenience APIs

## Objective & Scope Boundary

Delete public R2Config and from_r2 convenience APIs. Execute one bounded worksheet and create one signed local conventional commit after verification. Read the existing functional baseline at `5fe477fa`; a reopened ch01 is not an execution prerequisite. Own only the listed production and canonical test files. [^implementation]

## Bounded Read Manifest

- `src/py/tests/unit/extensions/test_data_storage_integrations.py` — migrate the preceding classifier regression when its temporary R2Config input disappears.

- `src/py/vertebra/storage/_store.py` — owned implementation or canonical regression surface.
- `src/py/vertebra/storage/__init__.py` — owned implementation or canonical regression surface.
- `src/py/tests/unit/storage/test_multicloud_storage.py` — owned implementation or canonical regression surface.
- `src/py/tests/unit/storage/test_signing.py` — owned implementation or canonical regression surface.
- `src/py/tests/integration/storage/conftest.py` — owned implementation or canonical regression surface.

## Symbol & Signature Contracts

Migrate t01's temporary R2Config classifier regression in `src/py/tests/unit/extensions/test_data_storage_integrations.py` when deleting the class: retain generic S3 endpoint acceptance and replace construction of the removed configuration with public symbol withdrawal and explicit unsupported-scheme coverage. Run that canonical module in this task so class deletion never leaves a broken test import or constructor.

Delete R2Config, its `chunk_uniform_parts` helper, from_r2 constructor, config union/validation members, signing configuration branch, supported-scheme membership, and public __all__/package exports. Reject `r2://` in facade URI construction as unsupported with ValueError before native construction. Convert relevant canonical signing/config examples to generic S3 custom endpoints or OCI 8 MiB coverage; delete R2-only fixture and its consumers without skipping unrelated provider acceptance. Generic S3 custom endpoints and S3 SigV4 remain fully supported. No alias or deprecation shim remains. Native helper is retained until ch08 withdrawal, keeping stubtest parity between commits.

Caller → seam → collaborator: public sync/async object-store or adapter operation → owned symbols above → existing native engine/provider or upstream framework contract. Preserve public APIs outside this task.

## Edge Cases & Error Semantics

The matrix below specifies observable failure behavior. Preserve the original error when cleanup also fails; retain ownership until abort or cleanup completes. No whole-object Vec/list/spool, size-cap fallback, Python lock, SDK fork, auth reimplementation or silent alias is permitted.

## Public-Seam Test Blueprint

Use the listed canonical test files and actual public store/framework calls. Do not assert source text or merely duplicate a helper implementation.

| Case | Input | Observable result |
| --- | --- | --- |
| Public withdrawal | import vertebra.storage; inspect R2Config/from_r2 availability | R2-specific symbols absent; supported provider exports still resolve |
| Explicit unsupported URI | SyncObjectStore/AsyncObjectStore("r2://assets") | Clear unsupported-scheme ValueError; no native helper invocation |
| Custom S3 endpoint | S3Config(bucket="assets", endpoint="https://custom.example", region="auto") | Accepted as ordinary S3 configuration/signing; no hostname blacklist |

## Verification

Record the failing public regression before implementation and the passing result after. Execute each command below in this worktree; the storage target directory is reserved and shared SHM tests use the host lock.

- `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" make dev`
- `uv run --no-sync python -m mypy.stubtest vertebra._storage`
- `uv run --no-sync ruff check src/py/vertebra/storage/_store.py src/py/vertebra/storage/__init__.py src/py/tests/unit/storage/test_multicloud_storage.py src/py/tests/unit/storage/test_signing.py src/py/tests/integration/storage/conftest.py src/py/tests/unit/extensions/test_data_storage_integrations.py`
- `uv run --no-sync mypy src/py/vertebra/storage/_store.py src/py/vertebra/storage/__init__.py`
- `uv run --no-sync pytest src/py/tests/unit/storage/test_multicloud_storage.py src/py/tests/unit/storage/test_signing.py src/py/tests/integration/storage/conftest.py src/py/tests/unit/extensions/test_data_storage_integrations.py -q`

Record exact exit statuses, native build checkpoint and any platform limitation in verification_evidence. Review closure uses these results, never planning-only structural validation.

## Acceptance Criteria

- [x] Every matrix row passes at its public seam with existing behavior preserved.
- [x] Only owned files change; bounds stay at read_set <= 6, production files <= 3 and estimated implementation LOC <= 250.
- [x] User-facing usage and native guarantees are documented by ch04:t03 and ch09; this task records its observable contract in the canonical regression tests.
- [x] A signed local conventional commit and verification evidence are recorded; no hosted action is performed.

## Notes & Discoveries

Revision 4 is independently approved by the fresh consolidated ch07/ch08 plan review. Any implementation contract contradiction routes through `/flow:revise storage-completeness` before production edits.

The facade removes R2Config, from_r2, uniform chunk convenience, signing inference and exports without introducing an alias or endpoint restriction. Existing unsupported-scheme validation rejects r2 before native construction; its ImproperConfigurationError is a ValueError subclass. The t01 framework regression now exercises removed URI rejection, and generic custom S3 endpoints retain SigV4 signing. The unused R2 emulator fixture has no consumers. The native helper and stub stay owned by ch08. Implementation and verification are complete; the coordinator owns the signed local commit and final state/checklist synchronization.

[^implementation]: `src/py/vertebra/storage/_store.py` — Current repository source for this task.
