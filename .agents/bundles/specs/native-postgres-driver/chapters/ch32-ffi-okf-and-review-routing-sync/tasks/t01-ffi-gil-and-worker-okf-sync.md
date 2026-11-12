---
type: Task
id: "native-postgres-driver:ch32:t01"
chapter_id: ch32-ffi-okf-and-review-routing-sync
title: Synchronize FFI/GIL pattern, PyO3 runtime gotchas, and worker batch-dispatch OKF chapters
description: Update ffi-and-gil-discipline.md, pyo3-and-python-runtime.md, and trimmed pyo3-ffi-and-batch-dispatch.md (212L -> <= 200L) so extension enumerations match the Makefile lint stubtest list including _postgres, document cross-cdylib primitive-kwarg and VTBARROW rules, and PendingDeliveryGuard async drop restoration.
status: draft
state: pending
priority: P1
verification_strategy: static_validation
depends_on:
  - "native-postgres-driver:ch31:t04"
read_set:
  - .agents/bundles/knowledge/patterns/rust/ffi-and-gil-discipline.md
  - .agents/bundles/knowledge/gotchas/pyo3-and-python-runtime.md
  - .agents/bundles/knowledge/architecture/worker/pyo3-ffi-and-batch-dispatch.md
  - src/rs/vertebra-postgres/src/lib.rs
  - src/py/vertebra/_postgres.pyi
  - src/rs/vertebra-db/src/py_async.rs
files:
  - .agents/bundles/knowledge/patterns/rust/ffi-and-gil-discipline.md
  - .agents/bundles/knowledge/gotchas/pyo3-and-python-runtime.md
  - .agents/bundles/knowledge/architecture/worker/pyo3-ffi-and-batch-dispatch.md
tests: []
estimated_loc: 175
plan_revision: 8
plan_commit: 5eb75b2a
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
updated_at: "2026-11-12T04:16:55Z"
commit: null
generated:
  by: flow-revise
  at: "2026-11-12T01:30:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T01:30:00Z"
    commit: c8722c76
sources:
  - id: okf-ffi-pattern
    resource: .agents/bundles/knowledge/patterns/rust/ffi-and-gil-discipline.md
    title: PyO3 0.30.1 FFI and GIL discipline pattern chapter
  - id: okf-ffi-gotcha
    resource: .agents/bundles/knowledge/gotchas/pyo3-and-python-runtime.md
    title: PyO3 FFI, embedded globals, and Python runtime gotchas chapter
  - id: okf-worker-ffi
    resource: .agents/bundles/knowledge/architecture/worker/pyo3-ffi-and-batch-dispatch.md
    title: Worker PyO3 FFI surface and database extension boundary chapter
  - id: pg-lib-rs
    resource: src/rs/vertebra-postgres/src/lib.rs
    title: Standalone vertebra._postgres (gil_used = false) extension root
  - id: pg-pyi
    resource: src/py/vertebra/_postgres.pyi
    title: Typed stub declarations for vertebra._postgres
  - id: db-py-async
    resource: src/rs/vertebra-db/src/py_async.rs::spawn_rust_db_task_with_attr
    title: Setup-abort-safe completion queue and cross-extension ShmCapsule helpers
tags:
  - postgres
  - pyo3
  - ffi
  - gil
  - okf
---

# Task t01: Synchronize FFI/GIL Pattern, PyO3 Runtime Gotchas, and Worker Batch-Dispatch OKF Chapters

## Objective & Scope Boundary

Update `.agents/bundles/knowledge/patterns/rust/ffi-and-gil-discipline.md` (`154L` at `c8722c76` -> `<= 200L`), `.agents/bundles/knowledge/gotchas/pyo3-and-python-runtime.md` (`159L` -> `<= 200L`), and `.agents/bundles/knowledge/architecture/worker/pyo3-ffi-and-batch-dispatch.md` (`212L` -> `<= 200L`) in present tense for the `vertebra._postgres` PyO3 boundary [^okf-ffi-pattern] [^okf-ffi-gotcha] [^okf-worker-ffi] [^pg-lib-rs] [^pg-pyi] [^db-py-async]. All three chapters describe "five" extensions at `c8722c76`; every extension and crate enumeration is rewritten as a plain list matching the `Makefile` `lint` stubtest list (which includes `_postgres` and the sibling driver modules) at execution time, with no count word:
1. `ffi-and-gil-discipline.md` (`<= 200` lines):
   - Update the truth block, lead inventory, `sources:`, and `Cross-extension ownership` so the `#[pymodule(gil_used = false)]` extension enumeration includes `_postgres` (`src/rs/vertebra-postgres/src/lib.rs`), each exporting `FREE_THREADED` and `GIL_USED = False` with `.pyi` stub parity (`_postgres.pyi`) [^okf-ffi-pattern] [^pg-lib-rs] [^pg-pyi].
   - Document three PostgreSQL FFI patterns beside BigQuery/Spanner: owned primitive-kwarg constructors (never passing Rust `Arc<GcpTokenCache>` or sibling `PyRef` structs across `cdylib` files), unregistered `ShmRegion` + named `vertebra.ShmCapsule` single-claim `ArrowCursor` handoff with a distinct `spawn_rust_db_task_with_attr` drainer name and setup-failure `AbortHandle` cleanup, and `PendingDeliveryGuard` restoration when an in-flight async notification/replication `next()` future is dropped on cancellation after popping an item from a Rust channel [^db-py-async].
2. `pyo3-and-python-runtime.md` (`<= 200` lines):
   - Update the truth block and Sections `1`, `2`, `5`, and `Cross-extension Arrow capsules and release` so the extension enumeration includes `vertebra-postgres` (built with `-m src/rs/vertebra-postgres/Cargo.toml`), and add `[dependency-groups].postgres-bench` isolation beside `[dependency-groups].bench`, unified `$n` SQLSpec binds, and the `PendingDeliveryGuard` cancellation drop-loss pitfall on `tokio::sync::mpsc::Receiver::recv` bridges (`async_fn` cancelled between `recv()` wakeup and Python delivery) [^okf-ffi-gotcha].
3. `pyo3-ffi-and-batch-dispatch.md` (`212` lines — over budget -> `<= 200` lines):
   - Remove blank separator lines in the footer footnote block and tighten repetitive method descriptions so the file drops below `200` lines while adding `src/py/vertebra/_postgres.pyi` and `src/rs/vertebra-postgres/src/lib.rs` to the extension inventory and expanding `Database extension boundary` to include `_postgres` (runtime `vertebra._vertebra` exception/`ArrowCursor` resolution, an independent completion drainer, and producer-owned `vertebra.ShmCapsule` transfer) [^okf-worker-ffi] [^pg-pyi] [^db-py-async].

Out of scope: Modifying Rust/Python source files, `packaging-and-pgo.md` / `ADR-004` / `pyo3-bridge/SKILL.md` (`ch32:t02`), or review routing/checklist files (`ch32:t03`).

## Bounded Read Manifest

- [.agents/bundles/knowledge/patterns/rust/ffi-and-gil-discipline.md](../../../../../../../.agents/bundles/knowledge/patterns/rust/ffi-and-gil-discipline.md) — Current `154`-line PyO3 FFI pattern chapter [^okf-ffi-pattern].
- [.agents/bundles/knowledge/gotchas/pyo3-and-python-runtime.md](../../../../../../../.agents/bundles/knowledge/gotchas/pyo3-and-python-runtime.md) — Current `159`-line PyO3 and runtime gotchas chapter [^okf-ffi-gotcha].
- [.agents/bundles/knowledge/architecture/worker/pyo3-ffi-and-batch-dispatch.md](../../../../../../../.agents/bundles/knowledge/architecture/worker/pyo3-ffi-and-batch-dispatch.md) — Current `212`-line worker FFI and DB boundary chapter [^okf-worker-ffi].
- [src/rs/vertebra-postgres/src/lib.rs](../../../../../../../src/rs/vertebra-postgres/src/lib.rs) — Delivered `vertebra._postgres` crate exports and notification/replication modules [^pg-lib-rs].
- [src/py/vertebra/_postgres.pyi](../../../../../../../src/py/vertebra/_postgres.pyi) — Delivered `vertebra._postgres` `Awaitable[...]` and class stub contract [^pg-pyi].
- [src/rs/vertebra-db/src/py_async.rs](../../../../../../../src/rs/vertebra-db/src/py_async.rs) — `spawn_rust_db_task_with_attr` setup-abort guard and `construct_py_arrow_cursor` [^db-py-async].

## Symbol & Signature Contracts

- Every chapter stays `<= 200` lines with `<= 40`-line truth blocks, multi-line YAML frontmatter, updated `verified[0].commit` ancestor SHAs, zero standalone `---` body rules, and 1:1 `sources:` inline and footer footnote parity (including `src/rs/vertebra-postgres/src/lib.rs` and `src/py/vertebra/_postgres.pyi` where cited).
- Preserve existing anchor headings (`#cross-extension-ownership`, `#cross-extension-arrow-capsules-and-release`, `#database-extension-boundary`) referenced by `ADR-004` and sibling chapters so cross-chapter `#anchor` links stay intact.

## Edge Cases & Error Semantics

- Keep `generate_cli_docs.py` `ensure_native_stub_module` guidance in `pyo3-and-python-runtime.md` accurate: `--only-group doc` stubs only core `vertebra._vertebra` and `vertebra._bundler`, while `vertebra.db.adapters.postgres` loads `vertebra._postgres` lazily via `ensure_module` only when a live connection/pool opens.
- `pyo3-ffi-and-batch-dispatch.md` starts `12` lines over `200`: verify `wc -l` after adding `src-postgres-pyi` / `src-vertebra-postgres-src-lib-rs` `sources:` entries and footnotes so frontmatter + footnote growth stays `<= 200` lines.

## Public-Seam Test Blueprint

- **Canonical validation gates**: `make review-check` and `wc -l` (`tests: []`).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Line ceiling across FFI chapters | `wc -l .agents/bundles/knowledge/patterns/rust/ffi-and-gil-discipline.md .agents/bundles/knowledge/gotchas/pyo3-and-python-runtime.md .agents/bundles/knowledge/architecture/worker/pyo3-ffi-and-batch-dispatch.md` | All three chapters are `<= 200` lines with truth blocks `<= 40` lines. |
| OKF bundle, link, & provenance validation | `make review-check` | Exits `0`; every `sources:` resource resolves on disk, 1:1 inline/footer footnotes match, and cross-chapter relative links resolve. |
| Stale count audit | `git grep -n -i -E "(five|six|all [0-9]+) (pyo3 )?extensions" -- .agents/bundles/knowledge/patterns/rust/ffi-and-gil-discipline.md .agents/bundles/knowledge/gotchas/pyo3-and-python-runtime.md .agents/bundles/knowledge/architecture/worker/pyo3-ffi-and-batch-dispatch.md` | No output. |
| `_postgres` & `PendingDeliveryGuard` coverage | `git grep -n "_postgres\|PendingDeliveryGuard" -- .agents/bundles/knowledge/patterns/rust/ffi-and-gil-discipline.md .agents/bundles/knowledge/gotchas/pyo3-and-python-runtime.md .agents/bundles/knowledge/architecture/worker/pyo3-ffi-and-batch-dispatch.md` | Each file names `_postgres`; `ffi-and-gil-discipline.md` and `pyo3-and-python-runtime.md` also name `PendingDeliveryGuard`. |

## Verification

- Line-budget check: `wc -l .agents/bundles/knowledge/patterns/rust/ffi-and-gil-discipline.md .agents/bundles/knowledge/gotchas/pyo3-and-python-runtime.md .agents/bundles/knowledge/architecture/worker/pyo3-ffi-and-batch-dispatch.md`
- Stale count audit: `git grep -n -i -E "(five|six|all [0-9]+) (pyo3 )?extensions" -- .agents/bundles/knowledge/patterns/rust/ffi-and-gil-discipline.md .agents/bundles/knowledge/gotchas/pyo3-and-python-runtime.md .agents/bundles/knowledge/architecture/worker/pyo3-ffi-and-batch-dispatch.md` (expects no output)
- OKF & link check: `make review-check`

## Acceptance Criteria

- [ ] `ffi-and-gil-discipline.md`, `pyo3-and-python-runtime.md`, and `pyo3-ffi-and-batch-dispatch.md` sit `<= 200` lines each, enumerate extensions matching the `Makefile` `lint` stubtest list including `_postgres`, and document cross-`cdylib` primitive kwargs / `VTBARROW` ownership and `PendingDeliveryGuard` drop restoration (R1, R7, R20, R27).
- [ ] `make review-check` exits `0` (R27).

## Notes & Discoveries

- Inspect the delivered notification and replication Rust modules from `ch21` and `ch36` before citing a `resource: path::PendingDeliveryGuard` anchor in `ffi-and-gil-discipline.md`.

[^okf-ffi-pattern]: `.agents/bundles/knowledge/patterns/rust/ffi-and-gil-discipline.md` — PyO3 0.30.1 FFI and GIL discipline pattern chapter.
[^okf-ffi-gotcha]: `.agents/bundles/knowledge/gotchas/pyo3-and-python-runtime.md` — PyO3 FFI, embedded globals, and Python runtime gotchas chapter.
[^okf-worker-ffi]: `.agents/bundles/knowledge/architecture/worker/pyo3-ffi-and-batch-dispatch.md` — Worker PyO3 FFI surface and database extension boundary chapter.
[^pg-lib-rs]: `src/rs/vertebra-postgres/src/lib.rs` — Standalone `vertebra._postgres` (`gil_used = false`) extension root.
[^pg-pyi]: `src/py/vertebra/_postgres.pyi` — Typed stub declarations for `vertebra._postgres`.
[^db-py-async]: `src/rs/vertebra-db/src/py_async.rs::spawn_rust_db_task_with_attr` — Setup-abort-safe completion queue and cross-extension `ShmCapsule` helpers.
