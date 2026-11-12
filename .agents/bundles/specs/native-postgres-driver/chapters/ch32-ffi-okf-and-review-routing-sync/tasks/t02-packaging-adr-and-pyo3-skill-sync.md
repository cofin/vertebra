---
type: Task
id: "native-postgres-driver:ch32:t02"
chapter_id: ch32-ffi-okf-and-review-routing-sync
title: Synchronize packaging-and-pgo, ADR-004, and pyo3-bridge engineering skill for vertebra-postgres
description: Update packaging-and-pgo.md, adr-004-optional-driver-wheels-and-shm-ownership.md, and .agents/skills/pyo3-bridge/SKILL.md (all <= 200L) for vertebra-postgres optional wheel isolation, discovery-based PGO coverage, VTBARROW ownership, and stubtest guidance matching the Makefile lint stubtest list.
status: draft
state: pending
priority: P1
verification_strategy: static_validation
depends_on:
  - "native-postgres-driver:ch32:t01"
read_set:
  - .agents/bundles/knowledge/domains/frontend/packaging-and-pgo.md
  - .agents/bundles/knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md
  - .agents/skills/pyo3-bridge/SKILL.md
  - src/rs/vertebra-postgres/pyproject.toml
  - tools/pgo/rust_pgo.py
  - .github/workflows/release.yaml
files:
  - .agents/bundles/knowledge/domains/frontend/packaging-and-pgo.md
  - .agents/bundles/knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md
  - .agents/skills/pyo3-bridge/SKILL.md
tests: []
estimated_loc: 145
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
  - id: okf-packaging
    resource: .agents/bundles/knowledge/domains/frontend/packaging-and-pgo.md
    title: Standalone packaging, release wheel matrix, and PGO pipeline chapter
  - id: okf-adr-004
    resource: .agents/bundles/knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md
    title: ADR-004 optional driver wheels and shared-memory ownership decision
  - id: skill-pyo3
    resource: .agents/skills/pyo3-bridge/SKILL.md
    title: Project-local PyO3 native bridge engineering skill
  - id: pg-wheel-pyproject
    resource: src/rs/vertebra-postgres/pyproject.toml
    title: Standalone vertebra-postgres Maturin wheel/sdist manifest
  - id: pgo-discovery
    resource: tools/pgo/rust_pgo.py::discover_extension_manifests
    title: Rust PGO extension discovery from crate-local maturin pyproject.toml files
  - id: release-workflow
    resource: .github/workflows/release.yaml
    title: Multi-platform cibuildwheel and sdist release workflow
tags:
  - postgres
  - packaging
  - adr
  - pyo3
  - skills
---

# Task t02: Synchronize `packaging-and-pgo`, `ADR-004`, and `pyo3-bridge` Engineering Skill for `vertebra-postgres`

## Objective & Scope Boundary

Update `.agents/bundles/knowledge/domains/frontend/packaging-and-pgo.md` (`119L` at `c8722c76` -> `<= 200L`), `.agents/bundles/knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md` (`89L` -> `<= 200L`), and `.agents/skills/pyo3-bridge/SKILL.md` (`51L` -> `<= 200L`) in present tense [^okf-packaging] [^okf-adr-004] [^skill-pyo3] [^pg-wheel-pyproject] [^pgo-discovery] [^release-workflow]. Companion wheel lists are reconciled against `src/rs/*/pyproject.toml`, and extension/stub lists against the `Makefile` `lint` stubtest list, at execution time; no file states a fixed count:
1. `packaging-and-pgo.md` (`<= 200` lines):
   - Add `src/rs/vertebra-postgres/pyproject.toml` to `sources:`, replace "the four companion extension manifests" in `Release Wheel Matrix` with an enumeration matching `src/rs/*/pyproject.toml` (including `vertebra-postgres`), add `vertebra-postgres` and the `[postgres]` extra to `Optional extension distributions`, and document `make postgres-wheel-check` (`tools/scripts/verify_postgres_wheel.py`) outside-checkout wheel/sdist file-ownership and uninstall isolation verification [^okf-packaging] [^pg-wheel-pyproject] [^release-workflow].
   - Keep `Two-Phase Rust PGO Pipeline` as written: it already describes `tools/pgo/rust_pgo.py::discover_extension_manifests` (root-configured `vertebra-py` plus every crate with a crate-local maturin `pyproject.toml`), which includes `vertebra-postgres`; add `vertebra-postgres` there only if the paragraph enumerates discovered crates [^pgo-discovery].
2. `adr-004-optional-driver-wheels-and-shm-ownership.md` (`<= 200` lines):
   - Update the truth block, `Context`, `Decision`, and `Consequences` to include `vertebra-postgres` (`vertebra._postgres`, `vertebra-core[postgres]`: `postgres-protocol`, `rustls`, RSA/X.509 cloud discovery isolated from HTTP-only installs) beside `vertebra-bigquery` and `vertebra-spanner`, reinforcing owned primitive-kwarg constructors across `cdylib` boundaries, canonical `DatabaseError`/`GoogleCloudAuthError`/`ArrowCursor` ownership in `vertebra._vertebra`, distinct completion drainer attributes, and producer-owned `vertebra.ShmCapsule` `VTBARROW` cleanup [^okf-adr-004] [^pg-wheel-pyproject].
3. `.agents/skills/pyo3-bridge/SKILL.md` (`<= 200` lines):
   - Update YAML `description`, `<!-- truth: start -->` (`Crates & Stubs`, `Verify`), intro paragraph, and `Verification` bash snippet so the crate list, `src/py/vertebra/{...}.pyi` stub brace list, and `mypy.stubtest` module list match the `Makefile` `lint` stubtest list (including `vertebra-postgres`, `_postgres.pyi`, and `vertebra._postgres`), plus a concise rule under `Core Implementation Rules` on cross-`cdylib` primitive kwargs, `vertebra.ShmCapsule` ownership, and cancellation-safe async stream guards [^skill-pyo3].

Out of scope: Modifying `tools/pgo/`, `.github/workflows/release.yaml`, or review routing/checklist files (`ch32:t03`).

## Bounded Read Manifest

- [.agents/bundles/knowledge/domains/frontend/packaging-and-pgo.md](../../../../../../../.agents/bundles/knowledge/domains/frontend/packaging-and-pgo.md) — Current `119`-line packaging, release wheel, and discovery-based PGO chapter [^okf-packaging].
- [.agents/bundles/knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md](../../../../../../../.agents/bundles/knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md) — Current `89`-line ADR-004 optional driver wheel decision [^okf-adr-004].
- [.agents/skills/pyo3-bridge/SKILL.md](../../../../../../../.agents/skills/pyo3-bridge/SKILL.md) — Current `51`-line `pyo3-bridge` engineering skill [^skill-pyo3].
- [src/rs/vertebra-postgres/pyproject.toml](../../../../../../../src/rs/vertebra-postgres/pyproject.toml) — `vertebra-postgres` wheel/sdist include/exclude contract [^pg-wheel-pyproject].
- [tools/pgo/rust_pgo.py](../../../../../../../tools/pgo/rust_pgo.py) — `discover_extension_manifests` and `EXTENSION_MANIFESTS` [^pgo-discovery].
- [.github/workflows/release.yaml](../../../../../../../.github/workflows/release.yaml) — `vertebra-postgres` `cibuildwheel` and sdist matrix entry and wheel smoke test [^release-workflow].

## Symbol & Signature Contracts

- `packaging-and-pgo.md` and `adr-004-optional-driver-wheels-and-shm-ownership.md` carry valid OKF v0.2 multi-line YAML frontmatter, `<= 40`-line truth blocks, updated `verified[0].commit` ancestor SHAs, 1:1 `sources:` <-> footnote parity, and `<= 200` total lines.
- `.agents/skills/pyo3-bridge/SKILL.md` keeps single-line `name` and `description` frontmatter, a `<= 40`-line truth block, resolving relative Markdown links, and `<= 200` total lines.
- No file cites `src/py/vertebra/build/packaging/pgo.py` (deleted) or describes a static PGO manifest list.

## Edge Cases & Error Semantics

- Do not rewrite `ADR-004` as a changelog; present `vertebra-bigquery`, `vertebra-spanner`, `vertebra-postgres`, and the other optional driver wheels in present tense as the repository's optional native database wheel architecture (checklist rule `DOC-05`).
- Keep `.agents/skills/pyo3-bridge/SKILL.md` `description` single-line so skill parsers load it.

## Public-Seam Test Blueprint

- **Canonical validation gates**: `make review-check` and `wc -l` (`tests: []`).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Line budgets across OKF & engineering skill | `wc -l .agents/bundles/knowledge/domains/frontend/packaging-and-pgo.md .agents/bundles/knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md .agents/skills/pyo3-bridge/SKILL.md` | All three files are `<= 200` lines with truth blocks `<= 40` lines. |
| Skill, OKF, link, & privacy validation | `make review-check` | Exits `0`; `skills`, `bundles`, and `privacy` validators pass with zero broken links or missing `sources:` paths. |
| Stale PGO & count audit | `git grep -n -i -E "pgo\.py|core-5|core five|four companion|(five|six|all [0-9]+) (pyo3 )?extensions" -- .agents/bundles/knowledge/domains/frontend/packaging-and-pgo.md .agents/bundles/knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md .agents/skills/pyo3-bridge/SKILL.md` | No output. |
| Inventory parity | Inspect the three files against `src/rs/*/pyproject.toml` and the `Makefile` `lint` stubtest list | `vertebra-postgres` appears in every companion wheel list; `vertebra._postgres` appears in the `pyo3-bridge` `mypy.stubtest` command; each list matches its source. |

## Verification

- Line-budget check: `wc -l .agents/bundles/knowledge/domains/frontend/packaging-and-pgo.md .agents/bundles/knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md .agents/skills/pyo3-bridge/SKILL.md`
- Stale PGO & count audit: `git grep -n -i -E "pgo\.py|core-5|core five|four companion|(five|six|all [0-9]+) (pyo3 )?extensions" -- .agents/bundles/knowledge/domains/frontend/packaging-and-pgo.md .agents/bundles/knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md .agents/skills/pyo3-bridge/SKILL.md` (expects no output)
- Review capability & OKF check: `make review-check`

## Acceptance Criteria

- [ ] `packaging-and-pgo.md`, `adr-004-optional-driver-wheels-and-shm-ownership.md`, and `.agents/skills/pyo3-bridge/SKILL.md` (`<= 200L` each) document `vertebra-postgres` optional wheel isolation, `VTBARROW` `ShmCapsule` ownership, discovery-based PGO that includes `vertebra-postgres`, and `pyo3-bridge` crate/stub/stubtest lists matching the `Makefile` (R1, R20, R26, R27).
- [ ] `make review-check` exits `0` (R27).

## Notes & Discoveries

- `pyo3-bridge/SKILL.md` is an implementation skill (not a read-only `review-*` skill), so retaining `make dev` followed by the `mypy.stubtest` command in its `Verification` block is valid under `review_setup.py`.

[^okf-packaging]: `.agents/bundles/knowledge/domains/frontend/packaging-and-pgo.md` — Standalone packaging, release wheel matrix, and PGO pipeline chapter.
[^okf-adr-004]: `.agents/bundles/knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md` — ADR-004 optional driver wheels and shared-memory ownership decision.
[^skill-pyo3]: `.agents/skills/pyo3-bridge/SKILL.md` — Project-local PyO3 native bridge engineering skill.
[^pg-wheel-pyproject]: `src/rs/vertebra-postgres/pyproject.toml` — Standalone `vertebra-postgres` Maturin wheel/sdist manifest.
[^pgo-discovery]: `tools/pgo/rust_pgo.py::discover_extension_manifests` — Rust PGO extension discovery from crate-local maturin `pyproject.toml` files.
[^release-workflow]: `.github/workflows/release.yaml` — Multi-platform `cibuildwheel` and sdist release workflow.
