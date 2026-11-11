---
type: Task
id: "native-postgres-driver:ch32:t01"
chapter_id: ch32-ffi-okf-and-review-routing-sync
title: Synchronize FFI/GIL pattern, PyO3 runtime gotchas, and worker batch-dispatch OKF chapters
description: Update ffi-and-gil-discipline.md, pyo3-and-python-runtime.md, and trimmed pyo3-ffi-and-batch-dispatch.md (212L -> <= 200L) for 6 gil_used = false extensions including _postgres, cross-cdylib primitive-kwarg and VTBARROW rules, and PendingDeliveryGuard async drop restoration.
status: stable
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
plan_revision: 6
plan_commit: 8e9b324e
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
updated_at: "2026-11-10T14:45:00Z"
commit: null
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

Update `.agents/bundles/knowledge/patterns/rust/ffi-and-gil-discipline.md` (`154L -> <= 200L`), `.agents/bundles/knowledge/gotchas/pyo3-and-python-runtime.md` (`159L -> <= 200L`), and `.agents/bundles/knowledge/architecture/worker/pyo3-ffi-and-batch-dispatch.md` (`212L -> <= 200L`) in strict present tense for the six-extension PyO3 boundary [^okf-ffi-pattern] [^okf-ffi-gotcha] [^okf-worker-ffi] [^pg-lib-rs] [^pg-pyi] [^db-py-async]:
1. `ffi-and-gil-discipline.md` (`<= 200` lines):
   - Update truth block, lead inventory, `sources:`, and `Cross-extension ownership` from five to six `#[pymodule(gil_used = false)]` PyO3 extensions (`_vertebra`, `_bundler`, `_storage`, `_bigquery`, `_spanner`, `_postgres` in `src/rs/vertebra-postgres/src/lib.rs`) exporting `FREE_THREADED` and `GIL_USED = False` with full `.pyi` stub parity (`_postgres.pyi`) [^okf-ffi-pattern] [^pg-lib-rs] [^pg-pyi].
   - Document three PostgreSQL FFI patterns alongside BigQuery/Spanner: owned primitive-kwarg constructors (never passing Rust `Arc<GcpTokenCache>` or sibling `PyRef` structs across `cdylib` files), unregistered `ShmRegion` + named `vertebra.ShmCapsule` single-claim `ArrowCursor` handoff with unique `spawn_rust_db_task_with_attr` drainer names and setup-failure `AbortHandle` cleanup, and `PendingDeliveryGuard` restoration when an in-flight async notification/replication `next()` future is dropped on cancellation after popping an item from a Rust channel [^db-py-async].
2. `pyo3-and-python-runtime.md` (`<= 200` lines):
   - Update truth block and Sections `1`, `2`, `5`, and `Cross-extension Arrow capsules and release` to six `gil_used = false` extensions (`vertebra-{bundler,storage,bigquery,spanner,postgres}` using `-m <Cargo.toml>`), `[dependency-groups].postgres-bench` isolation alongside `[dependency-groups].bench`, unified `$n` SQLSpec binds, and the `PendingDeliveryGuard` cancellation drop-loss pitfall on `tokio::sync::mpsc::Receiver::recv` bridges (`async_fn` cancelled between `recv()` wakeup and Python delivery) [^okf-ffi-gotcha].
3. `pyo3-ffi-and-batch-dispatch.md` (`212` lines baseline — over budget -> trim and synchronize strictly `<= 200` lines):
   - Remove blank separator lines in the footer footnote block and tighten repetitive method descriptions so the file drops below `200` lines while adding `src/py/vertebra/_postgres.pyi` and `src/rs/vertebra-postgres/src/lib.rs` to the six-extension inventory and expanding `Database extension boundary` to `_bigquery`, `_spanner`, and `_postgres` (runtime `vertebra._vertebra` exception/`ArrowCursor` resolution, independent completion drainers, and producer-owned `vertebra.ShmCapsule` transfer) [^okf-worker-ffi] [^pg-pyi] [^db-py-async].

Out of scope: Modifying Rust/Python source files, `packaging-and-pgo.md` / `ADR-004` / `pyo3-bridge/SKILL.md` (`ch32:t02`), or review routing/checklist files (`ch32:t03`).

## Bounded Read Manifest

- [.agents/bundles/knowledge/patterns/rust/ffi-and-gil-discipline.md](../../../../../../../.agents/bundles/knowledge/patterns/rust/ffi-and-gil-discipline.md) — Current `154`-line PyO3 FFI pattern chapter [^okf-ffi-pattern].
- [.agents/bundles/knowledge/gotchas/pyo3-and-python-runtime.md](../../../../../../../.agents/bundles/knowledge/gotchas/pyo3-and-python-runtime.md) — Current `159`-line PyO3 and runtime gotchas chapter [^okf-ffi-gotcha].
- [.agents/bundles/knowledge/architecture/worker/pyo3-ffi-and-batch-dispatch.md](../../../../../../../.agents/bundles/knowledge/architecture/worker/pyo3-ffi-and-batch-dispatch.md) — Current `212`-line worker FFI and DB boundary chapter [^okf-worker-ffi].
- [src/rs/vertebra-postgres/src/lib.rs](../../../../../../../src/rs/vertebra-postgres/src/lib.rs) — Delivered `vertebra._postgres` crate exports and notification/replication modules [^pg-lib-rs].
- [src/py/vertebra/_postgres.pyi](../../../../../../../src/py/vertebra/_postgres.pyi) — Delivered `vertebra._postgres` `Awaitable[...]` and class stub contract [^pg-pyi].
- [src/rs/vertebra-db/src/py_async.rs](../../../../../../../src/rs/vertebra-db/src/py_async.rs) — `spawn_rust_db_task_with_attr` setup-abort guard and `construct_py_arrow_cursor` [^db-py-async].

## Symbol & Signature Contracts

- Every chapter stays strictly `<= 200` lines with `<= 40`-line truth blocks, multi-line YAML frontmatter, updated `verified[0].commit` ancestor SHAs, zero standalone `---` body rules, and 1:1 `sources:` inline and footer footnote parity (including `src/rs/vertebra-postgres/src/lib.rs` and `src/py/vertebra/_postgres.pyi` where cited).
- Preserve existing anchor headings (`#cross-extension-ownership`, `#cross-extension-arrow-capsules-and-release`, `#database-extension-boundary`) referenced by `ADR-004` and sibling chapters so cross-chapter `#anchor` links stay intact.

## Edge Cases & Error Semantics

- Keep `generate_cli_docs.py` `ensure_native_stub_module` guidance in `pyo3-and-python-runtime.md` accurate: `--only-group doc` continues stubbing only core `vertebra._vertebra` and `vertebra._bundler` while `vertebra.db.adapters.postgres` loads `vertebra._postgres` lazily via `ensure_module` only when a live connection/pool opens.
- `pyo3-ffi-and-batch-dispatch.md` starts `12` lines over `200`: verify `wc -l` after adding `src-postgres-pyi` / `src-vertebra-postgres-src-lib-rs` `sources:` entries and footnotes so frontmatter + footnote growth stays strictly `<= 200` lines.

## Public-Seam Test Blueprint

- **Canonical validation gates**: `make review-check` and `wc -l` (`tests: []`).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| `DOC-08` line ceiling across FFI chapters | `wc -l .agents/bundles/knowledge/patterns/rust/ffi-and-gil-discipline.md .agents/bundles/knowledge/gotchas/pyo3-and-python-runtime.md .agents/bundles/knowledge/architecture/worker/pyo3-ffi-and-batch-dispatch.md` | All three chapters are strictly `<= 200` lines with truth blocks `<= 40` lines. |
| OKF bundle, link, & provenance validation | `make review-check` | Exits `0`; every `sources:` resource resolves on disk, 1:1 inline/footer footnotes match, and cross-chapter relative links resolve cleanly. |
| Six-extension & `PendingDeliveryGuard` audit | Inspect all three files | Zero residual `"five PyO3 extensions"` statements; documents `_postgres`, primitive-kwarg cross-`cdylib` isolation, `VTBARROW` `ShmCapsule`, and `PendingDeliveryGuard` async drop restoration. |

## Verification

- Line-budget check: `wc -l .agents/bundles/knowledge/patterns/rust/ffi-and-gil-discipline.md .agents/bundles/knowledge/gotchas/pyo3-and-python-runtime.md .agents/bundles/knowledge/architecture/worker/pyo3-ffi-and-batch-dispatch.md`
- OKF & link check: `make review-check`

## Acceptance Criteria

- [ ] `ffi-and-gil-discipline.md`, `pyo3-and-python-runtime.md`, and `pyo3-ffi-and-batch-dispatch.md` sit strictly `<= 200` lines each and accurately document all six `gil_used = false` extensions (`_postgres`), cross-`cdylib` primitive-kwargs / `VTBARROW` ownership, and `PendingDeliveryGuard` drop restoration (R1, R7, R20, R27).
- [ ] `make review-check` exits `0` cleanly (R27).

## Notes & Discoveries

- Inspect the delivered notification and replication Rust modules from `ch21` and `ch23` if citing the exact `PendingDeliveryGuard` `resource: path::PendingDeliveryGuard` anchor in `ffi-and-gil-discipline.md`.

[^okf-ffi-pattern]: `.agents/bundles/knowledge/patterns/rust/ffi-and-gil-discipline.md` — PyO3 0.30.1 FFI and GIL discipline pattern chapter.
[^okf-ffi-gotcha]: `.agents/bundles/knowledge/gotchas/pyo3-and-python-runtime.md` — PyO3 FFI, embedded globals, and Python runtime gotchas chapter.
[^okf-worker-ffi]: `.agents/bundles/knowledge/architecture/worker/pyo3-ffi-and-batch-dispatch.md` — Worker PyO3 FFI surface and database extension boundary chapter.
[^pg-lib-rs]: `src/rs/vertebra-postgres/src/lib.rs` — Standalone `vertebra._postgres` (`gil_used = false`) extension root.
[^pg-pyi]: `src/py/vertebra/_postgres.pyi` — Typed stub declarations for `vertebra._postgres`.
[^db-py-async]: `src/rs/vertebra-db/src/py_async.rs::spawn_rust_db_task_with_attr` — Setup-abort-safe completion queue and cross-extension `ShmCapsule` helpers.
