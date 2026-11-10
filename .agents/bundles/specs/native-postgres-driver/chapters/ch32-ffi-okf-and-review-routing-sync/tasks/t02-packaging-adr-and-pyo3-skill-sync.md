---
type: Task
id: "native-postgres-driver:ch32:t02"
chapter_id: ch32-ffi-okf-and-review-routing-sync
title: Synchronize packaging-and-pgo, ADR-004, and pyo3-bridge engineering skill for vertebra-postgres
description: Update packaging-and-pgo.md, adr-004-optional-driver-wheels-and-shm-ownership.md, and .agents/skills/pyo3-bridge/SKILL.md (all <= 200L) for vertebra-postgres optional wheel isolation, core-5 PGO vs release-LTO compilation, VTBARROW ownership, and 6-extension stubtest guidance.
status: stable
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
  - src/py/vertebra/build/packaging/pgo.py
  - .github/workflows/release.yaml
files:
  - .agents/bundles/knowledge/domains/frontend/packaging-and-pgo.md
  - .agents/bundles/knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md
  - .agents/skills/pyo3-bridge/SKILL.md
tests: []
estimated_loc: 145
plan_revision: 6
plan_commit: ef86844f
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
  - id: pgo-py
    resource: src/py/vertebra/build/packaging/pgo.py::EXTENSION_MANIFESTS
    title: Rust PGO extension manifest list
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

Update `.agents/bundles/knowledge/domains/frontend/packaging-and-pgo.md` (`122L -> <= 200L`), `.agents/bundles/knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md` (`89L -> <= 200L`), and `.agents/skills/pyo3-bridge/SKILL.md` (`51L -> <= 200L`) in strict present tense [^okf-packaging] [^okf-adr-004] [^skill-pyo3] [^pg-wheel-pyproject] [^pgo-py] [^release-workflow]:
1. `packaging-and-pgo.md` (`<= 200` lines):
   - Add `src/rs/vertebra-postgres/pyproject.toml` to `sources:` and update `Release Wheel Matrix` and `Optional extension distributions` from four to five companion manifests (`vertebra-bundler`, `vertebra-storage`, `vertebra-bigquery`, `vertebra-spanner`, `vertebra-postgres` via `[postgres]`), including `make postgres-wheel-check` (`tools/scripts/verify_postgres_wheel.py`) outside-checkout wheel/sdist file-ownership and uninstall isolation verification [^okf-packaging] [^pg-wheel-pyproject] [^release-workflow].
   - Keep `Two-Phase Rust PGO Pipeline` exact against `pgo.py::EXTENSION_MANIFESTS`: `make build-pgo` profiles `vertebra-http` and the five manifests in `EXTENSION_MANIFESTS`, whereas `vertebra-postgres` builds with workspace `[profile.release]` (`lto = true`, `codegen-units = 1`, `opt-level = 3`, `panic = "abort"`) via `maturin` during `make build` and `release.yaml` [^pgo-py].
2. `adr-004-optional-driver-wheels-and-shm-ownership.md` (`<= 200` lines):
   - Update truth block, `Context`, `Decision`, and `Consequences` to include `vertebra-postgres` (`vertebra._postgres`, `vertebra-core[postgres]`: `postgres-protocol`, `rustls`, RSA/X.509 cloud discovery isolated from HTTP-only installs) alongside `vertebra-bigquery` and `vertebra-spanner`, reinforcing owned primitive-kwarg constructors across `cdylib` boundaries, canonical `DatabaseError`/`GoogleCloudAuthError`/`ArrowCursor` ownership in `vertebra._vertebra`, distinct completion drainer attributes, and producer-owned `vertebra.ShmCapsule` `VTBARROW` cleanup [^okf-adr-004] [^pg-wheel-pyproject].
3. `.agents/skills/pyo3-bridge/SKILL.md` (`<= 200` lines):
   - Update YAML `description`, `<!-- truth: start -->` (`Crates & Stubs`, `Verify`), intro paragraph, and `Verification` bash snippet to cover all six PyO3 crates (`vertebra-py`, `vertebra-bundler`, `vertebra-storage`, `vertebra-bigquery`, `vertebra-postgres`, `vertebra-spanner`) and all six `.pyi` stubs (`{_vertebra,_bundler,_storage,_bigquery,_postgres,_spanner}.pyi`, including `vertebra._postgres` in `mypy.stubtest`), plus a concise rule under `Core Implementation Rules` on cross-`cdylib` primitive-kwargs, `vertebra.ShmCapsule` ownership, and cancellation-safe async stream guards [^skill-pyo3].

Out of scope: Modifying `src/py/vertebra/build/packaging/pgo.py`, `.github/workflows/release.yaml`, or review routing/checklist files (`ch32:t03`).

## Bounded Read Manifest

- [.agents/bundles/knowledge/domains/frontend/packaging-and-pgo.md](../../../../../../../.agents/bundles/knowledge/domains/frontend/packaging-and-pgo.md) — Current `122`-line packaging, release wheel, and PGO chapter [^okf-packaging].
- [.agents/bundles/knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md](../../../../../../../.agents/bundles/knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md) — Current `89`-line ADR-004 optional driver wheel decision [^okf-adr-004].
- [.agents/skills/pyo3-bridge/SKILL.md](../../../../../../../.agents/skills/pyo3-bridge/SKILL.md) — Current `51`-line `pyo3-bridge` engineering skill [^skill-pyo3].
- [src/rs/vertebra-postgres/pyproject.toml](../../../../../../../src/rs/vertebra-postgres/pyproject.toml) — `vertebra-postgres` wheel/sdist include/exclude contract [^pg-wheel-pyproject].
- [src/py/vertebra/build/packaging/pgo.py](../../../../../../../src/py/vertebra/build/packaging/pgo.py) — `EXTENSION_MANIFESTS` core-5 PGO coverage [^pgo-py].
- [.github/workflows/release.yaml](../../../../../../../.github/workflows/release.yaml) — Delivered `vertebra-postgres` `cibuildwheel` and sdist matrix from `ch29` [^release-workflow].

## Symbol & Signature Contracts

- `packaging-and-pgo.md` and `adr-004-optional-driver-wheels-and-shm-ownership.md` carry valid OKF v0.2 multi-line YAML frontmatter, `<= 40`-line truth blocks, updated `verified[0].commit` ancestor SHAs, 1:1 `sources:` <-> footnote parity, and `<= 200` total lines.
- `.agents/skills/pyo3-bridge/SKILL.md` keeps single-line `name` and `description` frontmatter, `<= 40`-line truth block, resolving relative Markdown links, and `<= 200` total lines.

## Edge Cases & Error Semantics

- Do not rewrite `ADR-004` as a changelog; present `vertebra-bigquery`, `vertebra-spanner`, and `vertebra-postgres` cohesively in present tense as the repository's optional native database wheel architecture (`DOC-05`).
- Check `.agents/skills/index.md` only if needed for reading; keep `.agents/skills/pyo3-bridge/SKILL.md` description crisp and single-line so skill parsers load it cleanly.

## Public-Seam Test Blueprint

- **Canonical validation gates**: `make review-check` and `wc -l` (`tests: []`).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Line budgets across OKF & engineering skill | `wc -l .agents/bundles/knowledge/domains/frontend/packaging-and-pgo.md .agents/bundles/knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md .agents/skills/pyo3-bridge/SKILL.md` | All three files sit strictly `<= 200` lines with truth blocks `<= 40` lines. |
| Skill, OKF, link, & privacy validation | `make review-check` | Exits `0`; `skills`, `bundles`, and `privacy` validators pass with zero broken links or missing `sources:` paths. |
| Wheel & PGO accuracy audit | Inspect `packaging-and-pgo.md`, `ADR-004`, and `pyo3-bridge/SKILL.md` | Lists five optional companion wheels including `vertebra-postgres`, six `mypy.stubtest` modules including `vertebra._postgres`, and exact core-5 `EXTENSION_MANIFESTS` PGO vs release-LTO `_postgres` boundary. |

## Verification

- Line-budget check: `wc -l .agents/bundles/knowledge/domains/frontend/packaging-and-pgo.md .agents/bundles/knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md .agents/skills/pyo3-bridge/SKILL.md`
- Review capability & OKF check: `make review-check`

## Acceptance Criteria

- [ ] `packaging-and-pgo.md`, `adr-004-optional-driver-wheels-and-shm-ownership.md`, and `.agents/skills/pyo3-bridge/SKILL.md` (`<= 200L` each) accurately document `vertebra-postgres` optional wheel isolation, `VTBARROW` `ShmCapsule` ownership, core-5 PGO vs release-LTO builds, and six-extension `pyo3-bridge` workflows (R1, R20, R26, R27).
- [ ] `make review-check` exits `0` cleanly (R27).

## Notes & Discoveries

- Note that `pyo3-bridge/SKILL.md` is an implementation skill (not a read-only `review-*` skill), so retaining `make dev` followed by six-module `mypy.stubtest` in its `Verification` block is valid and expected by `review_setup.py`.

[^okf-packaging]: `.agents/bundles/knowledge/domains/frontend/packaging-and-pgo.md` — Standalone packaging, release wheel matrix, and PGO pipeline chapter.
[^okf-adr-004]: `.agents/bundles/knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md` — ADR-004 optional driver wheels and shared-memory ownership decision.
[^skill-pyo3]: `.agents/skills/pyo3-bridge/SKILL.md` — Project-local PyO3 native bridge engineering skill.
[^pg-wheel-pyproject]: `src/rs/vertebra-postgres/pyproject.toml` — Standalone `vertebra-postgres` Maturin wheel/sdist manifest.
[^pgo-py]: `src/py/vertebra/build/packaging/pgo.py::EXTENSION_MANIFESTS` — Rust PGO extension manifest list.
[^release-workflow]: `.github/workflows/release.yaml` — Multi-platform `cibuildwheel` and sdist release workflow.
