---
type: Task
id: storage-completeness:ch05:t01
chapter_id: ch05-key-identity-and-preconditions
title: Preserve native raw key and prefix identity
description: Preserve native raw key and prefix identity
status: stable
state: completed
priority: P1
verification_strategy: regression_tdd
depends_on: []
read_set:
- src/rs/vertebra-storage/src/store.rs
- src/rs/vertebra-storage/src/oci.rs
- src/rs/vertebra-storage/tests/storage_engine_tests.rs
files:
- src/rs/vertebra-storage/src/store.rs
- src/rs/vertebra-storage/src/oci.rs
tests:
- src/rs/vertebra-storage/tests/storage_engine_tests.rs
estimated_loc: 100
plan_revision: 4
plan_commit: 5fe477fa4f6e08bab9278dba3906e7c556ceb3a4
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: "2026-11-07T21:49:51Z"
last_verified_commit: c90285cf
verification_evidence: "4 regressions failed before fix, then 4 passed; full native storage_engine_tests 23 passed; cargo clippy all-targets -D warnings, cargo fmt --all --check, git diff --check passed. Installed Python extension rebuild remains required before Python consumer checks."
created_at: '2026-11-07T12:00:00Z'
updated_at: "2026-11-07T21:49:51Z"
commit: c90285cf
sources:
- id: implementation
  resource: src/rs/vertebra-storage/src/store.rs
  title: Owned implementation seam
tags:
- storage
- repair
---

# Preserve native raw key and prefix identity

## Objective & Scope Boundary

Preserve native raw key and prefix identity. Execute one bounded worksheet and create one signed local conventional commit after verification. Read the existing functional baseline at `5fe477fa`; a reopened ch01 is not an execution prerequisite. Own only the listed production and canonical test files. [^implementation]

## Bounded Read Manifest

- `src/rs/vertebra-storage/src/store.rs` — owned implementation or canonical regression surface.
- `src/rs/vertebra-storage/src/oci.rs` — owned implementation or canonical regression surface.
- `src/rs/vertebra-storage/tests/storage_engine_tests.rs` — owned implementation or canonical regression surface.

## Symbol & Signature Contracts

`store.rs::validate_object_path` validates raw text before `StorePath::parse`: an object key is nonempty, has no leading/trailing `/`, no repeated `/`, no literal `.` segment and no outer whitespace. Preserve the existing `reject_traversal_segments` guard on slash/backslash separated literal `..`, `%2e%2e`, `.%2e`, `%2e.` (case insensitive). Reject backslash characters rather than interpreting them as separators. Unicode, literal `%`, `%2F` and `%252F` remain raw key text and are never URL-decoded. `Path::parse` itself strips a boundary slash, so the explicit grammar must precede it; never use `Path::from` on raw keys.

`validate_optional_prefix(None)`, `Some("")` and `Some("/")` return None (all keys), preserving existing root-prefix behavior. A relative prefix with exactly one trailing slash (e.g. `"logs/"`) is valid and strips that single suffix before `Path::parse`; a leading slash except the root sentinel, repeated separators, outer whitespace, backslash, dot or guarded parent segments reject with InvalidInput. Listing `logs/` remains valid for list, list_page and list_with_delimiter. No object-key trailing-slash normalization is allowed.

OCI listing builds returned paths with `StorePath::parse(obj.name)` after the same raw-object grammar and maps invalid foreign paths to a visible storage error. Metadata keys are SDK raw identities; never URL-unquote them. Foreign SDK/local files with valid raw names remain readable through their exact listed key. Preserve percent/Unicode identity across put/get/head/list/paging/delimiter/delete/copy/rename, including validating both operands before any mutation. The owned Rust canonical tests must name raw-key, prefix-policy and nonmutating-invalid-operation cases explicitly.

Caller → seam → collaborator: public sync/async object-store or adapter operation → owned symbols above → existing native engine/provider or upstream framework contract. Preserve public APIs outside this task.

## Edge Cases & Error Semantics

The matrix below specifies observable failure behavior. Preserve the original error when cleanup also fails; retain ownership until abort or cleanup completes. No whole-object Vec/list/spool, size-cap fallback, Python lock, SDK fork, auth reimplementation or silent alias is permitted.

## Public-Seam Test Blueprint

Use the listed canonical test files and actual public store/framework calls. Do not assert source text or merely duplicate a helper implementation.

| Case | Input | Observable result |
| --- | --- | --- |
| Percent aliases | Store distinct literal%2Fkey and literal%252Fkey with b"first"/b"second" | List returns both exact keys; get(meta.path) returns matching payload, never the other object |
| Foreign keys | Write SDK/local raw Unicode/é%, nested/a and literal%2Fkey | Vertebra head/list/get retains exact identity |
| Prefix identity | List prefix literal%2F versus literal%252F | Each matches only corresponding raw prefix; delimiter names retain percent text |

| Prefix compatibility | Prefixes `None`, `""`, `"/"`, `"logs/"`; object keys `"/x"`, `"x/"`, `"x//y"`, `"."`, `"a/../b"`, `"a/%2E%2E/b"`, `"a\\b"` | Root sentinels list all; logs/ works in all list APIs; every invalid object/prefix form returns InvalidInput. |
| Nonmutating invalid operands | Existing source/destination; copy or rename with valid source + invalid destination, then invalid source + valid destination | Error occurs before effects; both objects retain exact bytes. |

## Verification

Record the failing regression before implementation. Use this task's reserved target directory in each native command: `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target"` and `PYO3_PYTHON="$PWD/.venv/bin/python3.12"`. Run `cargo fmt --manifest-path src/rs/Cargo.toml --all --check` and `cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-storage --all-targets -- -D warnings` for native changes. Run `make dev` with those same environment values before Python native regressions and stubtest. Execute `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" flock /tmp/vertebra-flow-shm-tests.lock uv run --no-sync cargo test --manifest-path src/rs/Cargo.toml -p vertebra-storage --test storage_engine_tests`. For Python ownership run `uv run --no-sync ruff check src/rs/vertebra-storage/src/store.rs src/rs/vertebra-storage/src/oci.rs src/rs/vertebra-storage/tests/storage_engine_tests.rs` and `uv run --no-sync mypy `; omit these two commands for Rust-only tasks. Record exact commands and exit status in verification_evidence.

## Acceptance Criteria

- [x] Every matrix row passes at its public seam with existing behavior preserved.
- [x] Only owned files change; bounds stay at read_set <= 6, production files <= 3 and estimated implementation LOC <= 250.
- [x] User-facing usage and native guarantees are documented by ch04:t03 and ch09; this task records its observable contract in the canonical regression tests.
- [x] A signed local conventional commit and verification evidence are recorded; no hosted action is performed.

## Notes & Discoveries

Native regression baseline: four raw-key tests fail against the prior implementation; all four pass with raw parsing. The complete Rust engine suite passes 23 tests, including embedded Python. Run Cargo through `uv run --no-sync` so embedded Python finds the isolated environment; `PYO3_PYTHON` alone selects the compilation interpreter. Python consumer verification still requires a native rebuild.

[^implementation]: `src/rs/vertebra-storage/src/store.rs` — Current repository source for this task.
