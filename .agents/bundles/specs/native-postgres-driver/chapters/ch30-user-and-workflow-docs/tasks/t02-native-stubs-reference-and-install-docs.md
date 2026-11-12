---
type: Task
id: "native-postgres-driver:ch30:t02"
chapter_id: ch30-user-and-workflow-docs
title: Update native stubs reference, reference index, and installation guide for vertebra._postgres
description: Add vertebra._postgres to the compiled-module enumerations in native-stubs.md and reference/index.md within verify_docs_artifacts.py TOC and HTML limits, and add [postgres] installation tabs and free-threaded coverage in installation.md.
status: draft
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

Add `vertebra._postgres` (`src/rs/vertebra-postgres`, `vertebra-core[postgres]`) to the existing compiled-module enumerations in `docs/reference/native-stubs.md`, `docs/reference/index.md`, and `docs/getting-started/installation.md` [^doc-stubs] [^doc-ref-index] [^doc-install] [^pg-pyi] [^docs-verifier] [^root-pyproject]. Sibling driver flows edit the same enumerations, so every list is reconciled against the `Makefile` `lint` stubtest list at execution time and no page states a fixed module count:
1. `docs/reference/native-stubs.md`:
   - Add `_postgres` to the page title and intro `.pyi` enumeration, and add a `vertebra._postgres` row (`SyncPostgresClient`, `AsyncPostgresClient`, `SyncPostgresPool`, `AsyncPostgresPool`, `AsyncPostgresListener`, `AsyncPostgresReplication`; backing crate `vertebra-postgres` with Protocol 3.0 clients, physical pools, portals, binary `COPY`, `VTBARROW`, `LISTEN`/`NOTIFY`, and `pgoutput` v1 logical replication) to the summary table.
   - Add one `## vertebra._postgres — PostgreSQL Protocol 3.0 Client, Pool, COPY, Arrow & Replication` section rendering every public class exported in `src/py/vertebra/_postgres.pyi::__all__` with `heading_level: 4` and `members: false` (plus a compact capability summary list of key methods rather than repeating verbose `25`-kwarg `__new__` / `validate_configuration` signatures across clients and pools) so the secondary TOC rises by exactly one `H2` (`12 -> 13` at `c8722c76`, always `<= 14`) and `site/reference/native-stubs/index.html` stays `<= 120,000` bytes [^pg-pyi] [^docs-verifier].
2. `docs/reference/index.md`:
   - Add `_postgres` to the intro stub enumeration and the `PyO3 Native Stubs` table row (both list `_vertebra`, `_bundler`, `_storage`, `_bigquery`, `_spanner` at `c8722c76`; complete them so they match the `Makefile` `lint` stubtest list), and update the `Database Capabilities` row to reference `PostgresChangeSource` and `PostgresEmbeddingPushdown` [^doc-ref-index].
3. `docs/getting-started/installation.md`:
   - Add `[postgres]` install snippets across all four linked `pymdownx.tabbed` blocks (`=== "uv"`, `=== "pip"`, `=== "pdm"`, `=== "poetry"`), add a `postgres` row (`uv add "vertebra-core[postgres]"`, standalone native Rust `vertebra-postgres` (`vertebra._postgres`) Protocol 3.0 client, pool, binary `COPY`, `VTBARROW`, `LISTEN`/`NOTIFY`, and `pgoutput` v1 replication) to the `Optional Extras` table without mentioning external Python PostgreSQL libraries, and add `vertebra._postgres` to the `Free-Threaded Python (3.14t & 3.15t)` native-extension enumeration (including the `python3.15t -X gil=0` import check snippet without inline `#` comments) [^doc-install] [^root-pyproject].

Out of scope: Modifying `src/py/vertebra/_postgres.pyi`, `database-capabilities.md` (`ch30:t01`), cloud/packaging/queue guides (`ch30:t03`), or `README.md` / `workflow.md` / `AGENTS.md` (`ch30:t04`).

## Bounded Read Manifest

- [docs/reference/native-stubs.md](../../../../../../../docs/reference/native-stubs.md) — Current summary table and `12`-item `H2+H3` TOC layout (`5` `H2` + `7` `heading_level: 3` blocks at `c8722c76`) [^doc-stubs].
- [docs/reference/index.md](../../../../../../../docs/reference/index.md) — Current API reference module catalog [^doc-ref-index].
- [docs/getting-started/installation.md](../../../../../../../docs/getting-started/installation.md) — Current tabbed installer commands, optional extras table, and free-threaded verification guide [^doc-install].
- [src/py/vertebra/_postgres.pyi](../../../../../../../src/py/vertebra/_postgres.pyi) — Complete list of `vertebra._postgres` public stub classes (`__all__`) [^pg-pyi].
- [tools/docs/verify_docs_artifacts.py](../../../../../../../tools/docs/verify_docs_artifacts.py) — `max_toc_items = 14` and `max_reference_bytes = 120_000` thresholds [^docs-verifier].
- [pyproject.toml](../../../../../../../pyproject.toml) — `[project.optional-dependencies].postgres` definition [^root-pyproject].

## Symbol & Signature Contracts

- Every `::: vertebra._postgres.<ClassName>` directive in `docs/reference/native-stubs.md` matches an exported class in `src/py/vertebra/_postgres.pyi::__all__` and sets `heading_level: 4`, `members: false`, `show_root_heading: true`, and `show_source: false` so Griffe resolves `.pyi` stub docstrings during `--only-group doc` builds.
- Every installation snippet in `docs/getting-started/installation.md` uses linked `=== "uv"`, `=== "pip"`, `=== "pdm"`, and `=== "poetry"` blocks (`pdm add "vertebra-core[postgres]"` and `poetry add -E postgres vertebra-core` matching repository convention).
- The module enumerations in all three files contain every module in the `Makefile` `lint` stubtest list and nothing else.

## Edge Cases & Error Semantics

- `docs/reference/native-stubs.md` carries `12` TOC items at `c8722c76`; adding even two `heading_level: 3` blocks under `## vertebra._postgres` pushes `H2+H3` to `15` and fails `verify_docs_artifacts.py`. All `vertebra._postgres` class directives use `heading_level: 4`; re-count `H2` + `heading_level: 3` blocks before editing in case sibling flows added sections.
- In the `Free-Threaded Python (3.14t & 3.15t)` snippet of `installation.md`, state that importing optional companion modules (including `_postgres`) applies when the matching optional extras are installed, keeping the core-only quickstart accurate.

## Public-Seam Test Blueprint

- **Canonical validation gates**: `make docs`, `make docs-check`, and `make review-check` (`tests: []`).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| `native-stubs.md` Griffe `.pyi` resolution & byte/TOC limits | `make docs` inspecting `site/reference/native-stubs/index.html` | Exits `0`; every `vertebra._postgres.*` directive resolves, secondary TOC count is one higher than before the edit and `<= 14`, and HTML size is `<= 120,000` bytes. |
| Enumeration parity with the `Makefile` | `git grep -n "_postgres" -- docs/reference/native-stubs.md docs/reference/index.md docs/getting-started/installation.md` compared against the `Makefile` `lint` stubtest list | `_postgres` appears in the native-stubs title, intro, and table, both `reference/index.md` enumerations, and the installation free-threaded list; each enumeration matches the stubtest list exactly. |
| Installation tabs & external names | Inspect `docs/getting-started/installation.md` + `make docs-check` | `[postgres]` appears across all four linked `uv`/`pip`/`pdm`/`poetry` tabs and `Optional Extras`, zero external driver names, and live `zensical serve` returns HTTP `200`. |
| Bundle & privacy check | `make review-check` | Exits `0` with zero broken relative links or forbidden patterns. |

## Verification

- Build & live-serve docs check: `make docs && make docs-check`
- Enumeration check: `git grep -n "_postgres" -- docs/reference/native-stubs.md docs/reference/index.md docs/getting-started/installation.md`
- Review & privacy check: `make review-check`

## Acceptance Criteria

- [ ] `docs/reference/native-stubs.md` and `docs/reference/index.md` list `vertebra._postgres` (`src/rs/vertebra-postgres`) in enumerations that match the `Makefile` `lint` stubtest list, within `H2+H3 <= 14` and `<= 120,000` HTML bytes (R1, R26, R27).
- [ ] `docs/getting-started/installation.md` documents `vertebra-core[postgres]` across linked `uv`/`pip`/`pdm`/`poetry` tabs, `Optional Extras`, and `Free-Threaded Python (3.14t & 3.15t)`, with `make docs-check` and `make review-check` exiting `0` (R26, R27).

## Notes & Discoveries

- `src/py/vertebra/_postgres.pyi` expands in `ch22`–`ch23` with replication handles; read the completed `_postgres.pyi::__all__` at execution time so every exported stub class is listed under `## vertebra._postgres` at `heading_level: 4`.

[^doc-stubs]: `docs/reference/native-stubs.md` — Native PyO3 extension type stubs reference page.
[^doc-ref-index]: `docs/reference/index.md` — API reference landing page and module overview table.
[^doc-install]: `docs/getting-started/installation.md` — Getting-started installation, optional extras, and free-threaded Python guide.
[^pg-pyi]: `src/py/vertebra/_postgres.pyi::SyncPostgresClient` — Typed stub exports for `vertebra._postgres`.
[^docs-verifier]: `tools/docs/verify_docs_artifacts.py::verify_site_artifacts` — Secondary TOC (`<= 14`) and reference HTML (`<= 120,000` bytes) verifier.
[^root-pyproject]: `pyproject.toml` — Root optional extras including `vertebra-core[postgres]`.
