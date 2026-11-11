---
type: Task
id: "native-postgres-driver:ch30:t04"
chapter_id: ch30-user-and-workflow-docs
title: Synchronize README, workflow knowledge chapter, and AGENTS instructions within line budgets
description: Update README.md, trim and synchronize .agents/bundles/knowledge/workflow.md (strictly <= 200 lines), and update AGENTS.md (strictly <= 100 lines) for 9 Rust crates, 6 PyO3 extensions, [dependency-groups].postgres-bench, and postgres-* Make targets.
status: stable
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
created_at: "2026-11-05T18:25:07Z"
updated_at: "2026-11-10T14:45:00Z"
commit: null
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

Update `README.md`, `.agents/bundles/knowledge/workflow.md`, and `AGENTS.md` in present tense to register `src/rs/vertebra-postgres` (`vertebra._postgres`, `vertebra-core[postgres]`), `[dependency-groups].postgres-bench`, and all `postgres-*` Make gates while enforcing strict `DOC-08` line budgets [^repo-readme] [^okf-workflow] [^repo-agents] [^root-makefile] [^root-pyproject] [^pg-lib-rs]:
1. `README.md`:
   - Update the architecture summary, `Database & Cloud Connectivity` bullet, and `Project Layout` tree (`src/rs/`) to list all nine Rust crates including `vertebra-postgres` (`vertebra._postgres`, `vertebra-core[postgres]`: Protocol 3.0 client/pool, SCRAM-SHA-256-PLUS, Cloud SQL/AlloyDB mTLS+IAM, single-`Sync` `pipeline()`, binary `COPY`, `VTBARROW`, `LISTEN`/`NOTIFY`, and `pgoutput` v1 logical replication) with zero external SQL/driver library comparisons.
   - Add `make postgres-test`, `make postgres-wheel-check`, `make postgres-bench`, and `make postgres-bench-check` to the `Development` Make command table and update `make dev` / `make lint` descriptions for six compiled PyO3 extensions [^repo-readme] [^root-makefile].
2. `.agents/bundles/knowledge/workflow.md` (`198` lines baseline -> trim and synchronize strictly `<= 200` lines):
   - Update truth block, command catalog, `Polyglot Rebuild & Stale-Binary Gotchas`, and `Dependency-Group Isolation` for nine Rust workspace crates (`vertebra-postgres`), six `#[pymodule(gil_used = false)]` extensions (`_vertebra`, `_bundler`, `_storage`, `_bigquery`, `_spanner`, `_postgres`), six-module `mypy.stubtest`, `[dependency-groups].postgres-bench` (`python_version == '3.12'` markers for reference PostgreSQL benchmark comparators alongside `[dependency-groups].bench`), `make postgres-test` (`VERTEBRA_CONFORMANCE_DOCKER=1` and `VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS=1`), `make postgres-wheel-check`, `make postgres-bench`, `make postgres-bench-check`, core-5 `make build-pgo` vs release-LTO `vertebra-postgres` `maturin` compilation, and `make review-route BASE=213967be` [^okf-workflow] [^root-makefile] [^root-pyproject] [^pg-lib-rs].
   - Consolidate verbose multi-line bullets so `wc -l .agents/bundles/knowledge/workflow.md` stays strictly `<= 200`, maintaining a `<= 40`-line truth block, multi-line YAML frontmatter, updated `verified[0]` entry, zero standalone `---` body rules, and 1:1 `sources:`-to-footnote parity.
3. `AGENTS.md` (`63` lines baseline -> synchronize strictly `<= 100` lines):
   - Preserve the `<!-- project-customization: start -->` / `<!-- project-customization: end -->` region intact; add `vertebra-postgres` (`_postgres`) to the top-level crate list and `make dev` / `make py-test` comments, and add `make postgres-test`, `make postgres-wheel-check`, `make postgres-bench`, and `make postgres-bench-check` to `Quick Verification Commands` [^repo-agents] [^root-makefile].

Out of scope: Modifying `Makefile`, `pyproject.toml`, `docs/` guides (completed in `ch30:t01`–`t03`), or `.agents/bundles/knowledge/domains/**` / `testing/**` / `product/**` (`ch31`).

## Bounded Read Manifest

- [README.md](../../../../../../../README.md) — Current public overview, layout tree, and Make target table [^repo-readme].
- [.agents/bundles/knowledge/workflow.md](../../../../../../../.agents/bundles/knowledge/workflow.md) — Current `198`-line workflow OKF chapter [^okf-workflow].
- [AGENTS.md](../../../../../../../AGENTS.md) — Current `63`-line root agent instructions [^repo-agents].
- [Makefile](../../../../../../../Makefile) — Exact `.PHONY` target names and environment flags for `dev`, `py-test`, `build-pgo`, `review-route`, and `postgres-*` [^root-makefile].
- [pyproject.toml](../../../../../../../pyproject.toml) — `[project.optional-dependencies].postgres` and `[dependency-groups].postgres-bench` definitions [^root-pyproject].
- [src/rs/vertebra-postgres/src/lib.rs](../../../../../../../src/rs/vertebra-postgres/src/lib.rs) — `#[pymodule(gil_used = false)]` `_postgres` module root [^pg-lib-rs].

## Symbol & Signature Contracts

- Every Make target documented across `README.md`, `workflow.md`, and `AGENTS.md` matches a real target in `Makefile` (`DOC-08`) [^root-makefile].
- `.agents/bundles/knowledge/workflow.md` validates under `make review-check` (`<= 200` lines, `<= 40`-line truth block, valid `verified[0].commit` ancestor SHA, every `sources:` entry cited inline and in footer definitions).
- `AGENTS.md` stays `<= 100` lines and retains its exact `project-customization` HTML comment markers.

## Edge Cases & Error Semantics

- `workflow.md` starts at `198` lines (`2` lines below the `200`-line `DOC-08` ceiling): compact wordy prose in Sections `1`–`3` before adding `postgres-*` entries so total line count drops comfortably below `200` (`<= 195` target).
- Distinguish `[dependency-groups].bench` (HTTP/ASGI server comparator packages) from `[dependency-groups].postgres-bench` (`python_version == '3.12'` PostgreSQL comparator drivers used exclusively by `make postgres-bench*`), matching `pyproject.toml` from `ch29` [^root-pyproject].

## Public-Seam Test Blueprint

- **Canonical validation gates**: `make docs-check` and `make review-check` plus `wc -l` (`tests: []`).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| `DOC-08` line budgets | `wc -l AGENTS.md .agents/bundles/knowledge/workflow.md` | `AGENTS.md <= 100` lines and `workflow.md <= 200` lines. |
| Make target & OKF schema parity | `make review-check` | Exits `0`; every documented Make target exists in `Makefile`, `workflow.md` frontmatter/truth-block/footnotes/links validate cleanly, and privacy checks pass on `README.md`, `AGENTS.md`, and `workflow.md`. |
| End-to-end chapter docs verification | `make docs-check` | Exits `0`; full Zensical site builds cleanly across all `12` files updated in `ch30` and responds HTTP `200`. |

## Verification

- Line-budget check: `wc -l AGENTS.md .agents/bundles/knowledge/workflow.md`
- Full documentation & review gate: `make docs-check && make review-check`

## Acceptance Criteria

- [ ] `README.md`, `.agents/bundles/knowledge/workflow.md` (`<= 200` lines), and `AGENTS.md` (`<= 100` lines) accurately document `vertebra-postgres` (`_postgres`, `6` extensions, `9` crates), `[dependency-groups].postgres-bench`, and `postgres-test` / `postgres-wheel-check` / `postgres-bench` / `postgres-bench-check` in present tense (R1, R27).
- [ ] `make docs-check` and `make review-check` exit `0` across all `12` files owned by `ch30` (R27).

## Notes & Discoveries

- When updating `verified[0].commit` in `workflow.md`, record the current branch `HEAD` commit prior to committing `ch30:t04` so `review_setup.py` validates `git merge-base --is-ancestor <sha> HEAD` cleanly.

[^repo-readme]: `README.md` — Public repository overview, layout tree, and Make command table.
[^okf-workflow]: `.agents/bundles/knowledge/workflow.md` — Canonical Makefile commands, polyglot rebuild cycle, and Flow workflow chapter.
[^repo-agents]: `AGENTS.md` — Root repository agent instructions and quick verification commands.
[^root-makefile]: `Makefile` — Canonical polyglot `dev`, `test`, `wheel`, `docs`, `review`, and `postgres-*` targets.
[^root-pyproject]: `pyproject.toml` — Root `[postgres]` optional extra and `[dependency-groups].postgres-bench` configuration.
[^pg-lib-rs]: `src/rs/vertebra-postgres/src/lib.rs` — Native `vertebra._postgres` (`gil_used = false`) PyO3 extension crate.
