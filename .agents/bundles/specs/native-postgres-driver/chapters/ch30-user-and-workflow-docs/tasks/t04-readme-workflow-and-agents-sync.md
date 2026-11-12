---
type: Task
id: "native-postgres-driver:ch30:t04"
chapter_id: ch30-user-and-workflow-docs
title: Synchronize README, workflow knowledge chapter, and AGENTS instructions within line budgets
description: Add vertebra-postgres and _postgres to README.md, workflow.md (trimmed to <= 200 lines), and AGENTS.md (<= 100 lines) crate and extension enumerations, plus [dependency-groups].postgres-bench and postgres-* Make targets.
status: draft
state: pending
priority: P1
verification_strategy: documentation_validation
depends_on:
  - "native-postgres-driver:ch30:t03"
read_set:
  - README.md
  - .agents/bundles/knowledge/workflow.md
  - AGENTS.md
  - Makefile
  - pyproject.toml
  - src/rs/vertebra-postgres/src/lib.rs
files:
  - README.md
  - .agents/bundles/knowledge/workflow.md
  - AGENTS.md
tests: []
estimated_loc: 150
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
created_at: "2026-11-05T18:25:07Z"
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
  - id: repo-readme
    resource: README.md
    title: Public repository overview, layout tree, and Make command table
  - id: okf-workflow
    resource: .agents/bundles/knowledge/workflow.md
    title: Canonical Makefile commands, polyglot rebuild cycle, and Flow workflow chapter
  - id: repo-agents
    resource: AGENTS.md
    title: Root repository agent instructions and quick verification commands
  - id: root-makefile
    resource: Makefile
    title: Canonical polyglot dev, test, wheel, docs, review, and postgres-* targets
  - id: root-pyproject
    resource: pyproject.toml
    title: Root [postgres] optional extra and [dependency-groups].postgres-bench configuration
  - id: pg-lib-rs
    resource: src/rs/vertebra-postgres/src/lib.rs
    title: Native vertebra._postgres (gil_used = false) PyO3 extension crate
tags:
  - postgres
  - documentation
  - workflow
  - okf
  - makefile
---

# Task t04: Synchronize README, Workflow Knowledge Chapter, and `AGENTS.md` Within Line Budgets

## Objective & Scope Boundary

Update `README.md`, `.agents/bundles/knowledge/workflow.md`, and `AGENTS.md` in present tense to register `src/rs/vertebra-postgres` (`vertebra._postgres`, `vertebra-core[postgres]`), `[dependency-groups].postgres-bench`, and all `postgres-*` Make gates within line budgets, keeping every documented command matched to a real `Makefile` target (checklist rule `DOC-08`) [^repo-readme] [^okf-workflow] [^repo-agents] [^root-makefile] [^root-pyproject] [^pg-lib-rs]. Sibling driver flows edit the same enumerations, so every crate list is reconciled against `src/rs/*/Cargo.toml`, every extension list against the `Makefile` `lint` stubtest list, and no file states a fixed crate or extension count:
1. `README.md`:
   - Add `vertebra-postgres/  # PostgreSQL Protocol 3.0 client, pool, COPY, VTBARROW, LISTEN/NOTIFY, pgoutput v1 (_postgres)` to the `Project Layout` tree under `src/rs/` and `vertebra-postgres` (`vertebra._postgres`, `vertebra-core[postgres]`: Protocol 3.0 client/pool, SCRAM-SHA-256-PLUS, Cloud SQL/AlloyDB mTLS+IAM, single-`Sync` `pipeline()`, binary `COPY`, `VTBARROW`, `LISTEN`/`NOTIFY`, and `pgoutput` v1 logical replication) to the architecture summary and `Database & Cloud Connectivity` bullet, with zero external SQL/driver library comparisons.
   - Add `make postgres-test`, `make postgres-wheel-check`, `make postgres-bench`, and `make postgres-bench-check` to the `Development` Make command table [^repo-readme] [^root-makefile].
2. `.agents/bundles/knowledge/workflow.md` (`203` lines at `c8722c76` -> strictly `<= 200` lines, `<= 195` target):
   - Add `_postgres` / `vertebra-postgres` to the truth-block command catalog, the `make dev` / `make build` comments, `Polyglot Rebuild & Stale-Binary Gotchas`, and the `cargo build --workspace` exclude description, replacing count phrases such as "all six PyO3 extensions" and "all 6 extensions" with plain enumerations that match the `Makefile` `lint` stubtest list.
   - Add `make postgres-test` (`VERTEBRA_CONFORMANCE_DOCKER=1` and `VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS=1`), `make postgres-wheel-check`, `make postgres-bench`, and `make postgres-bench-check` to the command catalog, document `[dependency-groups].postgres-bench` (`python_version == '3.12'` reference PostgreSQL benchmark comparators) beside `[dependency-groups].bench` in `Dependency-Group Isolation`, state that `make build-pgo` profiles every crate discovered by `tools/pgo/rust_pgo.py::discover_extension_manifests` (including `vertebra-postgres`), and keep the existing `make review-route` entry (`BASE` defaults to `origin/main`) [^okf-workflow] [^root-makefile] [^root-pyproject] [^pg-lib-rs].
   - Consolidate verbose multi-line bullets to land `<= 200` lines, maintaining a `<= 40`-line truth block, multi-line YAML frontmatter, updated `verified[0]` entry, zero standalone `---` body rules, and 1:1 `sources:`-to-footnote parity.
3. `AGENTS.md` (`66` lines at `c8722c76` -> strictly `<= 100` lines):
   - Preserve the `<!-- project-customization: start -->` / `<!-- project-customization: end -->` region; add `vertebra-postgres` to the top-level crate list, add `_postgres` to the `make dev` comment enumeration, replace "all 6 extensions" in the `make py-test` and `make build` comments with wording that matches the `Makefile` (for example "every PyO3 extension stub"), and add `make postgres-test`, `make postgres-wheel-check`, `make postgres-bench`, and `make postgres-bench-check` to `Quick Verification Commands` [^repo-agents] [^root-makefile].

Out of scope: Modifying `Makefile`, `pyproject.toml`, `docs/` guides (completed in `ch30:t01`–`t03`), or `.agents/bundles/knowledge/domains/**` / `testing/**` / `product/**` (`ch31`).

## Bounded Read Manifest

- [README.md](../../../../../../../README.md) — Current public overview, layout tree, and Make target table [^repo-readme].
- [.agents/bundles/knowledge/workflow.md](../../../../../../../.agents/bundles/knowledge/workflow.md) — Current `203`-line workflow OKF chapter [^okf-workflow].
- [AGENTS.md](../../../../../../../AGENTS.md) — Current `66`-line root agent instructions [^repo-agents].
- [Makefile](../../../../../../../Makefile) — Exact `.PHONY` target names, `lint` stubtest module list, and environment flags for `dev`, `py-test`, `build-pgo`, `review-route`, and `postgres-*` [^root-makefile].
- [pyproject.toml](../../../../../../../pyproject.toml) — `[project.optional-dependencies].postgres` and `[dependency-groups].postgres-bench` definitions [^root-pyproject].
- [src/rs/vertebra-postgres/src/lib.rs](../../../../../../../src/rs/vertebra-postgres/src/lib.rs) — `#[pymodule(gil_used = false)]` `_postgres` module root [^pg-lib-rs].

## Symbol & Signature Contracts

- Every Make target documented across `README.md`, `workflow.md`, and `AGENTS.md` matches a real target in `Makefile` (`DOC-08`) [^root-makefile].
- Every extension enumeration in the three files matches the `Makefile` `lint` stubtest list, and every crate enumeration matches the directories holding `src/rs/*/Cargo.toml`.
- `.agents/bundles/knowledge/workflow.md` validates under `make review-check` (`<= 200` lines, `<= 40`-line truth block, valid `verified[0].commit` ancestor SHA, every `sources:` entry cited inline and in footer definitions).
- `AGENTS.md` stays `<= 100` lines and retains its exact `project-customization` HTML comment markers.

## Edge Cases & Error Semantics

- `workflow.md` starts `3` lines over the `200`-line ceiling: compact wordy prose in Sections `1`–`3` before adding `postgres-*` entries so the total drops to `<= 195`.
- Distinguish `[dependency-groups].bench` (HTTP/ASGI server comparator packages) from `[dependency-groups].postgres-bench` (`python_version == '3.12'` PostgreSQL comparator drivers used exclusively by `make postgres-bench*`), matching `pyproject.toml` from `ch29` [^root-pyproject].
- Never write a pinned SHA into `make review-route` documentation; the spec-level `BASE=c8722c76` routing applies only to this flow's final review.

## Public-Seam Test Blueprint

- **Canonical validation gates**: `make docs-check` and `make review-check` plus `wc -l` (`tests: []`).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Line budgets | `wc -l AGENTS.md .agents/bundles/knowledge/workflow.md` | `AGENTS.md <= 100` lines and `workflow.md <= 200` lines. |
| Stale count phrases | `git grep -n -i -E "all (six|6) (pyo3 )?extensions|five pyo3|nine rust crates" -- README.md AGENTS.md .agents/bundles/knowledge/workflow.md` | No output. |
| Enumeration parity | `git grep -n "_postgres\|vertebra-postgres" -- README.md AGENTS.md .agents/bundles/knowledge/workflow.md` compared against the `Makefile` `lint` stubtest list and `src/rs/*/Cargo.toml` | `_postgres` and `vertebra-postgres` appear in every extension and crate enumeration, and each enumeration matches its source list. |
| Make target & OKF schema parity | `make review-check` | Exits `0`; every documented Make target exists in `Makefile`, `workflow.md` frontmatter/truth-block/footnotes/links validate, and privacy checks pass on `README.md`, `AGENTS.md`, and `workflow.md`. |
| End-to-end chapter docs verification | `make docs-check` | Exits `0`; the Zensical site builds across all `12` files updated in `ch30` and responds HTTP `200`. |

## Verification

- Line-budget check: `wc -l AGENTS.md .agents/bundles/knowledge/workflow.md`
- Stale count audit: `git grep -n -i -E "all (six|6) (pyo3 )?extensions|five pyo3|nine rust crates" -- README.md AGENTS.md .agents/bundles/knowledge/workflow.md` (expects no output)
- Full documentation & review gate: `make docs-check && make review-check`

## Acceptance Criteria

- [ ] `README.md`, `.agents/bundles/knowledge/workflow.md` (`<= 200` lines), and `AGENTS.md` (`<= 100` lines) document `vertebra-postgres` (`_postgres`) in enumerations that match the `Makefile` and `src/rs/*/Cargo.toml`, `[dependency-groups].postgres-bench`, and `postgres-test` / `postgres-wheel-check` / `postgres-bench` / `postgres-bench-check` in present tense (R1, R27).
- [ ] `make docs-check` and `make review-check` exit `0` across all `12` files owned by `ch30` (R27).

## Notes & Discoveries

- When updating `verified[0].commit` in `workflow.md`, record the current branch `HEAD` commit prior to committing `ch30:t04` so `review_setup.py` validates `git merge-base --is-ancestor <sha> HEAD`.

[^repo-readme]: `README.md` — Public repository overview, layout tree, and Make command table.
[^okf-workflow]: `.agents/bundles/knowledge/workflow.md` — Canonical Makefile commands, polyglot rebuild cycle, and Flow workflow chapter.
[^repo-agents]: `AGENTS.md` — Root repository agent instructions and quick verification commands.
[^root-makefile]: `Makefile` — Canonical polyglot `dev`, `test`, `wheel`, `docs`, `review`, and `postgres-*` targets.
[^root-pyproject]: `pyproject.toml` — Root `[postgres]` optional extra and `[dependency-groups].postgres-bench` configuration.
[^pg-lib-rs]: `src/rs/vertebra-postgres/src/lib.rs` — Native `vertebra._postgres` (`gil_used = false`) PyO3 extension crate.
