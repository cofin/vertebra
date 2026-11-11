---
type: Task
id: "native-postgres-driver:ch30:t02"
chapter_id: ch30-user-and-workflow-docs
title: Update native stubs reference, reference index, and installation guide for vertebra._postgres
description: Document all 6 compiled gil_used = false PyO3 modules including vertebra._postgres in native-stubs.md and reference/index.md within verify_docs_artifacts.py TOC and HTML limits, and add [postgres] installation tabs and free-threaded coverage in installation.md.
status: stable
state: pending
priority: P1
verification_strategy: documentation_validation
depends_on:
  - "native-postgres-driver:ch30:t01"
read_set:
  - docs/reference/native-stubs.md
  - docs/reference/index.md
  - docs/getting-started/installation.md
  - src/py/vertebra/_postgres.pyi
  - tools/docs/verify_docs_artifacts.py
  - pyproject.toml
files:
  - docs/reference/native-stubs.md
  - docs/reference/index.md
  - docs/getting-started/installation.md
tests: []
estimated_loc: 140
plan_revision: 6
plan_commit: 043d8444
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
  - id: doc-stubs
    resource: docs/reference/native-stubs.md
    title: Native PyO3 extension type stubs reference page
  - id: doc-ref-index
    resource: docs/reference/index.md
    title: API reference landing page and module overview table
  - id: doc-install
    resource: docs/getting-started/installation.md
    title: Getting-started installation, optional extras, and free-threaded Python guide
  - id: pg-pyi
    resource: src/py/vertebra/_postgres.pyi::SyncPostgresClient
    title: Typed stub exports for vertebra._postgres
  - id: docs-verifier
    resource: tools/docs/verify_docs_artifacts.py::verify_site_artifacts
    title: Secondary TOC (<= 14) and reference HTML (<= 120,000 bytes) verifier
  - id: root-pyproject
    resource: pyproject.toml
    title: Root optional extras including vertebra-core[postgres]
tags:
  - postgres
  - documentation
  - stubs
  - installation
  - zensical
---

# Task t02: Update Native Stubs Reference, Reference Index, and Installation Guide for `vertebra._postgres`

## Objective & Scope Boundary

Synchronize `docs/reference/native-stubs.md`, `docs/reference/index.md`, and `docs/getting-started/installation.md` for all six `#[pymodule(gil_used = false)]` PyO3 extensions including `vertebra._postgres` (`src/rs/vertebra-postgres`, `vertebra-core[postgres]`) [^doc-stubs] [^doc-ref-index] [^doc-install] [^pg-pyi] [^docs-verifier] [^root-pyproject]:
1. `docs/reference/native-stubs.md`:
   - Update the intro prose and `Compiled Modules` heading/table (`six` PyO3 extensions) to add `vertebra._postgres` (`src/rs/vertebra-postgres`, `src/py/vertebra/_postgres.pyi`, `vertebra-core[postgres]`) with summary coverage of Protocol 3.0 clients, physical pools, portals, binary `COPY`, `VTBARROW`, `LISTEN`/`NOTIFY`, and `pgoutput` v1 logical replication.
   - Add one `## vertebra._postgres — PostgreSQL Protocol 3.0 Client, Pool, COPY, Arrow & Replication` section rendering every public class exported in `src/py/vertebra/_postgres.pyi` with `heading_level: 4` and `members: false` (plus a compact capability summary list/table of key methods rather than repeating verbose `25`-kwarg `__new__` / `validate_configuration` signatures across clients and pools) so the page's secondary TOC rises from `12` to `13 <= 14` (`H2+H3`) and `site/reference/native-stubs/index.html` remains strictly `<= 120,000` bytes [^pg-pyi] [^docs-verifier].
2. `docs/reference/index.md`:
   - Update the `Native Extensions (.pyi)` table row to name all six compiled modules (`vertebra._vertebra`, `vertebra._bundler`, `vertebra._storage`, `vertebra._bigquery`, `vertebra._spanner`, `vertebra._postgres`) and update the `Database Capabilities` row to reference `PostgresChangeSource` and `PostgresEmbeddingPushdown` [^doc-ref-index].
3. `docs/getting-started/installation.md`:
   - Add `[postgres]` install snippets across all four linked `pymdownx.tabbed` blocks (`=== "uv"`, `=== "pip"`, `=== "pdm"`, `=== "poetry"`), add a `postgres` row (`vertebra-postgres` native PostgreSQL Protocol 3.0 client, pool, binary `COPY`, `VTBARROW`, `LISTEN`/`NOTIFY`, and `pgoutput` v1 replication) to the `Optional Extras` table without mentioning external Python PostgreSQL libraries, and add `vertebra._postgres` (`vertebra-postgres`) alongside the existing five extensions in `Free-Threaded Python (3.14t & 3.15t)` (including the `python3.15t -X gil=0` import check snippet without inline `#` comments) [^doc-install] [^root-pyproject].

Out of scope: Modifying `src/py/vertebra/_postgres.pyi`, `database-capabilities.md` (`ch30:t01`), cloud/packaging/queue guides (`ch30:t03`), or `README.md` / `workflow.md` / `AGENTS.md` (`ch30:t04`).

## Bounded Read Manifest

- [docs/reference/native-stubs.md](../../../../../../../docs/reference/native-stubs.md) — Current five-module summary table and `12`-item `H2+H3` TOC layout [^doc-stubs].
- [docs/reference/index.md](../../../../../../../docs/reference/index.md) — Current API reference module catalog [^doc-ref-index].
- [docs/getting-started/installation.md](../../../../../../../docs/getting-started/installation.md) — Current tabbed installer commands, optional extras table, and free-threaded verification guide [^doc-install].
- [src/py/vertebra/_postgres.pyi](../../../../../../../src/py/vertebra/_postgres.pyi) — Complete list of `vertebra._postgres` public stub classes (`__all__`) [^pg-pyi].
- [tools/docs/verify_docs_artifacts.py](../../../../../../../tools/docs/verify_docs_artifacts.py) — `max_toc_items = 14` and `max_reference_bytes = 120_000` thresholds [^docs-verifier].
- [pyproject.toml](../../../../../../../pyproject.toml) — `[project.optional-dependencies].postgres` definition [^root-pyproject].

## Symbol & Signature Contracts

- Every `::: vertebra._postgres.<ClassName>` directive in `docs/reference/native-stubs.md` matches an exported class in `src/py/vertebra/_postgres.pyi::__all__` and sets `heading_level: 4`, `members: false`, `show_root_heading: true`, and `show_source: false` so Griffe resolves `.pyi` stub docstrings cleanly during `--only-group doc` builds.
- Every installation snippet in `docs/getting-started/installation.md` uses linked `=== "uv"`, `=== "pip"`, `=== "pdm"`, and `=== "poetry"` blocks (`pdm add "vertebra-core[postgres]"` and `poetry add -E postgres vertebra-core` matching repository convention).

## Edge Cases & Error Semantics

- `docs/reference/native-stubs.md` already contains `4` `H2` headings and `8` `heading_level: 3` class blocks (`12` TOC items); adding even two `heading_level: 3` blocks under `## vertebra._postgres` pushes `H2+H3` to `15` and fails `verify_docs_artifacts.py`. All `vertebra._postgres` class directives MUST use `heading_level: 4`.
- In the `Free-Threaded Python (3.14t & 3.15t)` snippet of `installation.md`, note that importing optional companion modules (`_postgres`, `_bigquery`, `_spanner`, `_storage`, `_bundler`) applies when the matching optional extras are installed, keeping the core-only quickstart accurate.

## Public-Seam Test Blueprint

- **Canonical validation gates**: `make docs`, `make docs-check`, and `make review-check` (`tests: []`).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| `native-stubs.md` Griffe `.pyi` resolution & byte/TOC limits | `make docs` inspecting `site/reference/native-stubs/index.html` | Exits `0`; every `vertebra._postgres.*` directive resolves cleanly, secondary TOC count equals `13` (`<= 14`), and HTML size is `<= 120,000` bytes. |
| Reference index & installation tabs | Inspect `docs/reference/index.md` and `docs/getting-started/installation.md` + `make docs-check` | Six native modules listed consistently, `[postgres]` appears across all four linked `uv`/`pip`/`pdm`/`poetry` tabs and `Optional Extras`, `vertebra._postgres` included in `3.14t`/`3.15t` coverage, zero external driver names, and live `zensical serve` returns HTTP `200`. |
| Bundle & privacy check | `make review-check` | Exits `0` with zero broken relative links or forbidden patterns. |

## Verification

- Build & live-serve docs check: `make docs && make docs-check`
- Review & privacy check: `make review-check`

## Acceptance Criteria

- [ ] `docs/reference/native-stubs.md` and `docs/reference/index.md` document all six `gil_used = false` native modules including `vertebra._postgres` (`src/rs/vertebra-postgres`) within `H2+H3 <= 14` and `<= 120,000` HTML bytes (R1, R26, R27).
- [ ] `docs/getting-started/installation.md` documents `vertebra-core[postgres]` across linked `uv`/`pip`/`pdm`/`poetry` tabs, `Optional Extras`, and `Free-Threaded Python (3.14t & 3.15t)`, with `make docs-check` and `make review-check` exiting `0` (R26, R27).

## Notes & Discoveries

- `src/py/vertebra/_postgres.pyi` expands in `ch20`–`ch23` with COPY, Arrow, notification, and replication handles; read the completed `_postgres.pyi::__all__` at execution time so every exported stub class is listed under `## vertebra._postgres` at `heading_level: 4`.

[^doc-stubs]: `docs/reference/native-stubs.md` — Native PyO3 extension type stubs reference page.
[^doc-ref-index]: `docs/reference/index.md` — API reference landing page and module overview table.
[^doc-install]: `docs/getting-started/installation.md` — Getting-started installation, optional extras, and free-threaded Python guide.
[^pg-pyi]: `src/py/vertebra/_postgres.pyi::SyncPostgresClient` — Typed stub exports for `vertebra._postgres`.
[^docs-verifier]: `tools/docs/verify_docs_artifacts.py::verify_site_artifacts` — Secondary TOC (`<= 14`) and reference HTML (`<= 120,000` bytes) verifier.
[^root-pyproject]: `pyproject.toml` — Root optional extras including `vertebra-core[postgres]`.
