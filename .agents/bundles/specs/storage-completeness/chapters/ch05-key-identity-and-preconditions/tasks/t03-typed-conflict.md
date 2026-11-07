---
type: Task
id: storage-completeness:ch05:t03
chapter_id: ch05-key-identity-and-preconditions
title: Export lazy typed PreconditionError for native CAS conflicts
description: Export lazy typed PreconditionError for native CAS conflicts
status: stable
state: completed
priority: P1
verification_strategy: regression_tdd
depends_on: []
read_set:
- src/rs/vertebra-storage/src/lib.rs
- src/py/vertebra/_storage.pyi
- src/py/vertebra/storage/__init__.py
- src/py/tests/unit/storage/test_storage_store.py
- src/rs/vertebra-storage/src/store.rs
files:
- src/rs/vertebra-storage/src/lib.rs
- src/py/vertebra/_storage.pyi
- src/py/vertebra/storage/__init__.py
tests:
- src/py/tests/unit/storage/test_storage_store.py
- src/rs/vertebra-storage/src/lib.rs
estimated_loc: 110
plan_revision: 4
plan_commit: 5fe477fa4f6e08bab9278dba3906e7c556ceb3a4
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: "2026-11-07T22:04:34Z"
last_verified_commit: 682aaaff
verification_evidence: "Red: 2 public typed-error/optional-import failures. Green: Rust converter unit covers six variants; full native engine 23 passed; Python storage 18 passed; make dev rebuilt all five extensions; all five mypy.stubtest modules passed; raw percent/Unicode key runtime probe passed; Mypy, Ruff, Clippy, formatting and diff checks passed."
created_at: '2026-11-07T12:00:00Z'
updated_at: "2026-11-07T22:04:34Z"
commit: 682aaaff
sources:
- id: implementation
  resource: src/rs/vertebra-storage/src/lib.rs
  title: Owned implementation seam
tags:
- storage
- repair
---

# Export lazy typed PreconditionError for native CAS conflicts

## Objective & Scope Boundary

Export lazy typed PreconditionError for native CAS conflicts. Execute one bounded worksheet and create one signed local conventional commit after verification. Read the existing functional baseline at `5fe477fa`; a reopened ch01 is not an execution prerequisite. Own only the listed production and canonical test files. [^implementation]

## Bounded Read Manifest

- `src/rs/vertebra-storage/src/lib.rs` — owned implementation or canonical regression surface.
- `src/py/vertebra/_storage.pyi` — owned implementation or canonical regression surface.
- `src/py/vertebra/storage/__init__.py` — owned implementation or canonical regression surface.
- `src/py/tests/unit/storage/test_storage_store.py` — owned implementation or canonical regression surface.
- `src/rs/vertebra-storage/src/store.rs` — collaborating contract.

## Symbol & Signature Contracts

Use PyO3 create_exception! to define `_storage.PreconditionError(PermissionError)`. Both `StorageError::Precondition` and nested `StorageError::ObjectStore(object_store::Error::Precondition)` map to this exact class in `lib.rs::to_py_err`. Register the exception in `_storage`; declare it in `_storage.pyi`. Expose `vertebra.storage.PreconditionError` through module-level lazy __getattr__ and __all__; add a `TYPE_CHECKING`-only `from vertebra._storage import PreconditionError as PreconditionError` so static consumers resolve the exact native class without eager runtime import; importing vertebra.storage while the optional native wheel is absent continues to succeed, and requesting the exception then uses the existing MissingDependencyError contract. Keep genuine IO PermissionDenied and wrapped object_store::Error::PermissionDenied as built-in PermissionError, not the CAS subtype; provider authorization failures must remain distinct from optimistic conflicts. Add owned `lib.rs` cfg(test) unit `test_precondition_error_mapping` after Python::initialize() inside Python::attach(...); construct BOTH StorageError::Precondition and nested object_store::Error::Precondition and assert exact native PreconditionError type, construct IO PermissionDenied and wrapped SDK PermissionDenied and assert exact built-in PermissionError for both, and both AlreadyExists forms assert exact FileExistsError. Create-only conflicts remain FileExistsError. Consumers such as Litestar renewal may catch only this type.

Caller → seam → collaborator: public sync/async object-store or adapter operation → owned symbols above → existing native engine/provider or upstream framework contract. Preserve public APIs outside this task.

## Edge Cases & Error Semantics

The matrix below specifies observable failure behavior. Preserve the original error when cleanup also fails; retain ownership until abort or cleanup completes. No whole-object Vec/list/spool, size-cap fallback, Python lock, SDK fork, auth reimplementation or silent alias is permitted.

## Public-Seam Test Blueprint

Use the listed canonical test files and actual public store/framework calls. Do not assert source text or merely duplicate a helper implementation.

| Case | Input | Observable result |
| --- | --- | --- |
| Update mismatch | Put v1; write with wrong if_match through sync and async APIs | Raises public PreconditionError and isinstance(error, PermissionError) |
| Permission distinction | Rust cfg(test) unit in owned lib.rs constructs StorageError::Io(PermissionDenied), calls to_py_err under Python::initialize(); Python::attach(...), and checks exact built-in type | PermissionError with type unequal to PreconditionError |
| Optional import | Fresh subprocess sets sys.modules["vertebra._storage"] = None BEFORE first import vertebra.storage, then imports storage without accessing exception | Import succeeds; store construction and requested native exception fail with existing optional dependency error |

| Rust complete mapping | Owned lib.rs cfg(test) maps both CAS error forms, both AlreadyExists forms and genuine IO and wrapped SDK PermissionDenied | Exact PreconditionError for both CAS forms, exact FileExistsError for create conflicts, exact built-in PermissionError for permission failure. |

## Verification

Record the failing regression before implementation. Use this task's reserved target directory in each native command: `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target"` and `PYO3_PYTHON="$PWD/.venv/bin/python3.12"`. Run `cargo fmt --manifest-path src/rs/Cargo.toml --all --check` and `cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-storage --all-targets -- -D warnings` for native changes. Run `make dev` with those same environment values before Python native regressions and stubtest. Execute `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" flock /tmp/vertebra-flow-shm-tests.lock uv run --no-sync cargo test --manifest-path src/rs/Cargo.toml -p vertebra-storage --lib test_precondition_error_mapping; uv run --no-sync pytest src/py/tests/unit/storage/test_storage_store.py -q; uv run --no-sync python -m mypy.stubtest vertebra._storage`. For Python ownership run `uv run --no-sync ruff check src/py/vertebra/_storage.pyi src/py/vertebra/storage/__init__.py src/py/tests/unit/storage/test_storage_store.py` and `uv run --no-sync mypy src/py/vertebra/_storage.pyi src/py/vertebra/storage/__init__.py`; omit these two commands for Rust-only tasks. Record exact commands and exit status in verification_evidence.

## Acceptance Criteria

- [x] Every matrix row passes at its public seam with existing behavior preserved.
- [x] Only owned files change; bounds stay at read_set <= 6, production files <= 3 and estimated implementation LOC <= 250.
- [x] User-facing usage and native guarantees are documented by ch04:t03 and ch09; this task records its observable contract in the canonical regression tests.
- [x] A signed local conventional commit and verification evidence are recorded; no hosted action is performed.

## Notes & Discoveries

Revision 4 is independently approved. Both native precondition forms map to the exact public class; genuine permission failures and create-only collisions retain their distinct built-in types. The optional-wheel subprocess and all five extension stub checks pass. Chapter correctness and fresh quality reviews remain pending.

[^implementation]: `src/rs/vertebra-storage/src/lib.rs` — Current repository source for this task.
