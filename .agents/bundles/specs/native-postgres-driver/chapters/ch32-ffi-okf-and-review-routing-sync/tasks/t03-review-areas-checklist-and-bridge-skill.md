---
type: Task
id: "native-postgres-driver:ch32:t03"
chapter_id: ch32-ffi-okf-and-review-routing-sync
title: Synchronize review areas.toml, checklist.md, and review-pyo3-bridge skill routing
description: Preserve vertebra-postgres globs and add a database-and-postgres shard, PostgreSQL OKF knowledge entries, and focused commands in areas.toml, update FFI-09 and PERF-06 in checklist.md, and update review-pyo3-bridge/SKILL.md so stub lists match the Makefile lint stubtest list, validating with make review-check and make review-route BASE=c8722c76.
status: draft
state: pending
priority: P1
verification_strategy: static_validation
depends_on:
  - "native-postgres-driver:ch32:t02"
read_set:
  - .agents/skills/review-core/references/areas.toml
  - .agents/skills/review-core/references/checklist.md
  - .agents/skills/review-pyo3-bridge/SKILL.md
  - tools/scripts/review_setup.py
  - src/rs/vertebra-postgres/src/lib.rs
  - src/py/vertebra/_postgres.pyi
files:
  - .agents/skills/review-core/references/areas.toml
  - .agents/skills/review-core/references/checklist.md
  - .agents/skills/review-pyo3-bridge/SKILL.md
tests: []
estimated_loc: 95
plan_revision: 7
plan_commit: c8722c76
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-10T14:45:00Z"
updated_at: "2026-11-12T01:30:00Z"
commit: null
generated:
  by: flow-revise
  at: "2026-11-12T01:30:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T01:30:00Z"
    commit: c8722c76
sources:
  - id: review-areas
    resource: .agents/skills/review-core/references/areas.toml
    title: Review area path globs, shards, knowledge files, and focused commands
  - id: review-checklist
    resource: .agents/skills/review-core/references/checklist.md
    title: Shared review checklist across RT, HTTP, FFI, PY, TS, PERF, TEST, and DOC
  - id: skill-review-pyo3
    resource: .agents/skills/review-pyo3-bridge/SKILL.md
    title: Read-only PyO3 native extension bridge review skill
  - id: review-script
    resource: tools/scripts/review_setup.py
    title: Automated review capability validator and git-diff area router
  - id: pg-lib-rs
    resource: src/rs/vertebra-postgres/src/lib.rs
    title: Native vertebra-postgres crate routed to runtime-ipc and pyo3-bridge
  - id: pg-pyi
    resource: src/py/vertebra/_postgres.pyi
    title: Native vertebra._postgres type stub verified under FFI-09
tags:
  - postgres
  - review
  - pyo3
  - routing
  - checklist
---

# Task t03: Synchronize Review `areas.toml`, `checklist.md`, and `review-pyo3-bridge` Skill Routing

## Objective & Scope Boundary

Synchronize `.agents/skills/review-core/references/areas.toml`, `.agents/skills/review-core/references/checklist.md`, and `.agents/skills/review-pyo3-bridge/SKILL.md` so review validation (`make review-check`) and branch diff routing (`make review-route BASE=c8722c76`) cover `vertebra-postgres`, `_postgres.pyi`, and the PostgreSQL OKF chapters [^review-areas] [^review-checklist] [^skill-review-pyo3] [^review-script] [^pg-lib-rs] [^pg-pyi]. Stub and crate enumerations are reconciled against the `Makefile` `lint` stubtest list at execution time, and no file states a fixed extension count:
1. `.agents/skills/review-core/references/areas.toml`:
   - Preserve the existing `"src/rs/vertebra-postgres/**"` entries in `[areas.runtime-ipc].paths` and `[areas.pyo3-bridge].paths` and `"src/py/vertebra/_postgres.pyi"` in `[areas.pyo3-bridge].paths` (present at `c8722c76`), and add a `[[areas.runtime-ipc.shards]]` entry `name = "database-and-postgres"` covering `src/rs/vertebra-db/**`, `src/rs/vertebra-postgres/**`, and `src/rs/vertebra-storage/**` so large branch diffs shard protocol/database Rust files beside `rings` and `platform-and-transport`.
   - Add `".agents/bundles/knowledge/domains/data/postgres-native-driver.md"` (created in `ch31:t01`) and `".agents/bundles/knowledge/domains/data/postgres-limitations.md"` (created in `ch22:t04`) to `[areas.runtime-ipc].knowledge` and `[areas.pyo3-bridge].knowledge`, and add `"cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres"` to both areas' read-only `focused_commands` [^review-areas] [^pg-lib-rs] [^pg-pyi].
2. `.agents/skills/review-core/references/checklist.md`:
   - Rewrite `FFI-09` so its `{...}.pyi` brace list and `uv run --no-sync python -m mypy.stubtest ...` module list match the `Makefile` `lint` stubtest list (adding `_postgres.pyi` / `vertebra._postgres`; `FFI-09` names `{_vertebra,_bundler,_storage,_bigquery,_spanner,_duckdb}.pyi` at `c8722c76`) and replace "across all 6 PyO3 extensions" with "across every PyO3 extension".
   - Update `PERF-06` so benchmark-only PostgreSQL reference packages (`[dependency-groups].postgres-bench`, passed by `make postgres-bench*`) are recognized beside `[dependency-groups].bench` (`make bench*`) [^review-checklist].
3. `.agents/skills/review-pyo3-bridge/SKILL.md` (`54` lines at `c8722c76` -> `<= 200` lines):
   - Update YAML `description`, `<!-- truth: start -->` (`Crates & Stubs` and `Stubs`), `Review Process` item `5`, and read-only `Focused Commands` (`cargo test ... -p vertebra-postgres` and `mypy.stubtest ... vertebra._postgres`) so crate, stub, and stubtest lists match the `Makefile` `lint` stubtest list (including `vertebra-postgres` and `src/py/vertebra/_postgres.pyi`) without introducing any command matching `reviewer_forbidden_commands` (`make dev`, `make rs-test`, `maturin develop`, etc.) [^skill-review-pyo3] [^review-script].

Out of scope: Modifying `tools/scripts/review_setup.py`, `.agents/agents/*.md`, or running the final full-branch review (`ch33:t01`).

## Bounded Read Manifest

- [.agents/skills/review-core/references/areas.toml](../../../../../../../.agents/skills/review-core/references/areas.toml) — Area path globs, shards, knowledge references, and `reviewer_forbidden_commands` [^review-areas].
- [.agents/skills/review-core/references/checklist.md](../../../../../../../.agents/skills/review-core/references/checklist.md) — Shared checklist items `FFI-01`..`FFI-10` and `PERF-01`..`PERF-09` [^review-checklist].
- [.agents/skills/review-pyo3-bridge/SKILL.md](../../../../../../../.agents/skills/review-pyo3-bridge/SKILL.md) — Current `review-pyo3-bridge` skill [^skill-review-pyo3].
- [tools/scripts/review_setup.py](../../../../../../../tools/scripts/review_setup.py) — `validate_manifest`, `validate_skills`, and `route_files` logic [^review-script].
- [src/rs/vertebra-postgres/src/lib.rs](../../../../../../../src/rs/vertebra-postgres/src/lib.rs) — Delivered `vertebra-postgres` crate root [^pg-lib-rs].
- [src/py/vertebra/_postgres.pyi](../../../../../../../src/py/vertebra/_postgres.pyi) — Delivered `vertebra._postgres` stub verified under `FFI-09` [^pg-pyi].

## Symbol & Signature Contracts

- Every `knowledge` file listed in `areas.toml` exists on disk (`review_setup.py::validate_manifest`), every checklist code in `checklist.md` keeps sequential numbering (`FFI-01`..`FFI-10`, `PERF-01`..`PERF-09`, and every other prefix with zero gaps), and `review-pyo3-bridge/SKILL.md` stays `<= 200` lines with zero `reviewer_forbidden_commands` matches in fenced shell blocks [^review-script].
- `make review-route BASE=c8722c76` routes `src/rs/vertebra-postgres/**` to both `runtime-ipc` (shard `database-and-postgres`) and `pyo3-bridge`, `src/py/vertebra/_postgres.pyi` to `pyo3-bridge`, `src/py/vertebra/db/adapters/postgres/**` to `python-framework` (`framework-and-extensions`), `benches/` + `tools/benchmarks/postgres.py` to `performance`, test suites to `tests`, and docs/OKF/skills to `docs-okf` with zero meaningful PostgreSQL files left unrouted.

## Edge Cases & Error Semantics

- Never place `make dev`, `make lint`, `make rs-test`, `make py-test`, or `maturin develop` inside `review-pyo3-bridge/SKILL.md` or `areas.toml` `focused_commands`: `review_setup.py` scans reviewer skills against `reviewer_forbidden_commands` and fails `make review-check` on any mutating/rebuild command.
- Keep the module order in the `FFI-09` brace list identical to the `Makefile` `lint` stubtest list so future driver flows append in one place, and verify `checklist.md` retains continuous `FFI-01`..`FFI-10` and `PERF-01`..`PERF-09` sequences (`validate_checklist` rejects skipped IDs).

## Public-Seam Test Blueprint

- **Canonical validation gates**: `make review-check` and `make review-route BASE=c8722c76` (`tests: []`).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Review manifest, checklist sequence, & skill validation | `make review-check` | Exits `0` (`review-check: ok`); `manifest`, `checklist`, `skills`, `agents`, `bundles`, `privacy`, and `python_imports` all pass. |
| Full branch PostgreSQL routing from the merged wheel baseline | `make review-route BASE=c8722c76` | Exits `0` and emits JSON listing `runtime-ipc` (with `database-and-postgres` shard matching `src/rs/vertebra-postgres/**`), `pyo3-bridge` (matching `src/rs/vertebra-postgres/**` and `src/py/vertebra/_postgres.pyi`), `python-framework`, `performance`, `tests`, and `docs-okf`. |
| `FFI-09` stub parity with the `Makefile` | `git grep -n "FFI-09" -- .agents/skills/review-core/references/checklist.md` compared against the `Makefile` `lint` stubtest list | The brace list and `mypy.stubtest` module list contain every stubtest module including `_postgres` / `vertebra._postgres` and no count word. |
| Reviewer read-only command discipline | Inspect `review-pyo3-bridge/SKILL.md` `Focused Commands` | Uses read-only `cargo test --manifest-path src/rs/Cargo.toml ... -p vertebra-postgres` and `uv run --no-sync python -m mypy.stubtest ... vertebra._postgres`, staying `<= 200` lines. |

## Verification

- Review manifest, skill, & OKF validator: `make review-check`
- Complete-driver diff routing check: `make review-route BASE=c8722c76`
- Stub parity check: `git grep -n "FFI-09" -- .agents/skills/review-core/references/checklist.md`

## Acceptance Criteria

- [ ] `areas.toml`, `checklist.md` (`FFI-09`, `PERF-06`), and `review-pyo3-bridge/SKILL.md` (`<= 200L`) cover `vertebra-postgres` / `_postgres.pyi` in lists matching the `Makefile` `lint` stubtest list, the PostgreSQL OKF knowledge chapters, and `[dependency-groups].postgres-bench` (R1, R8, R27).
- [ ] `make review-check` and `make review-route BASE=c8722c76` exit `0`, completing `[33/33 files documented]` across `ch30`–`ch32` (R8, R27).

## Notes & Discoveries

- Root non-code files such as `Makefile`, `pyproject.toml`, and `.github/workflows/*.yml` appear under `unmatched` in `make review-route BASE=c8722c76`; verify that zero `src/rs/vertebra-postgres/**`, `src/py/vertebra/**`, `src/py/tests/**`, `tools/benchmarks/**`, `docs/**`, or `.agents/**` files fall into `unmatched`.

[^review-areas]: `.agents/skills/review-core/references/areas.toml` — Review area path globs, shards, knowledge files, and focused commands.
[^review-checklist]: `.agents/skills/review-core/references/checklist.md` — Shared review checklist across `RT`, `HTTP`, `FFI`, `PY`, `TS`, `PERF`, `TEST`, and `DOC`.
[^skill-review-pyo3]: `.agents/skills/review-pyo3-bridge/SKILL.md` — Read-only PyO3 native extension bridge review skill.
[^review-script]: `tools/scripts/review_setup.py` — Automated review capability validator and git-diff area router.
[^pg-lib-rs]: `src/rs/vertebra-postgres/src/lib.rs` — Native `vertebra-postgres` crate routed to `runtime-ipc` and `pyo3-bridge`.
[^pg-pyi]: `src/py/vertebra/_postgres.pyi` — Native `vertebra._postgres` type stub verified under `FFI-09`.
