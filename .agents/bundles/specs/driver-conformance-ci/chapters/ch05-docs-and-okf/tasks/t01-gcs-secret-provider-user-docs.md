---
type: Task
id: "driver-conformance-ci:ch05:t01"
chapter_id: ch05-docs-and-okf
title: Document GCS bearer secret providers and DuckDB rotation in user docs
description: Add a DuckDB httpfs section to the storage auth guide, a GCS ADC example and rotation semantics to the analytics guide, and reference entries for both providers.
status: draft
state: pending
priority: P2
verification_strategy: documentation_validation
depends_on:
  - "driver-conformance-ci:ch04:t02"
read_set:
  - docs/usage/storage-auth-and-signing.md
  - docs/usage/analytics-and-cdc.md
  - docs/reference/storage.md
  - src/py/vertebra/storage/_secrets.py
  - src/py/vertebra/db/adapters/duckdb/config.py
files:
  - docs/usage/storage-auth-and-signing.md
  - docs/usage/analytics-and-cdc.md
  - docs/reference/storage.md
tests: []
estimated_loc: 110
plan_revision: 1
plan_commit: null
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-12T00:30:00Z"
updated_at: "2026-11-12T00:30:00Z"
commit: null
sources:
  - id: providers
    resource: src/py/vertebra/storage/_secrets.py::SyncGcsBearerSecretProvider
    title: Shipped GCS bearer secret providers
  - id: secret-config
    resource: src/py/vertebra/db/adapters/duckdb/config.py::DuckDBSecretConfig
    title: DuckDB secret validation rules
tags:
  - documentation
  - duckdb
  - gcs
---

# Task t01: Document GCS Bearer Secret Providers and DuckDB Rotation in User Docs

## Objective & Scope Boundary

1. `docs/usage/storage-auth-and-signing.md`: add a `### DuckDB httpfs secrets` subsection at the end of `## Google Cloud credentials and token caches` showing `SyncGcsBearerSecretProvider.from_config(config)` and sharing one `SyncGcpTokenCache` between object stores and DuckDB (`SyncGcsBearerSecretProvider(cache)`), linking to the analytics guide [^providers].
2. `docs/usage/analytics-and-cdc.md`: under `## Zero-Copy Arrow SHM (ArrowCursor), DuckDB & DataFusion`, add a GCS ADC example (`DuckDBConfig(extensions=("httpfs",), secrets=(DuckDBSecretConfig(name="gcs_lake", secret_type="gcs", value_provider=SyncGcsBearerSecretProvider.from_config(GCSConfig(bucket="lake")), scope="gs://lake/"),))`) and extend the `DuckDBSecretConfig` bullet with: rotation triggers (generation change, `needs_refresh()`, `refresh_interval_seconds`, `refresh_secrets()`), `(HTTP 401)` / `(HTTP 403)` invalidation without retry, `AsyncGcsBearerSecretProvider` requiring `AsyncDuckDBDriver`, and the `persistent=True` / interval validation errors [^secret-config].
3. `docs/reference/storage.md`: add `## GCS bearer secret providers` after `## GCS token caches` with one prose paragraph and `::: vertebra.storage._secrets.SyncGcsBearerSecretProvider` / `::: vertebra.storage._secrets.AsyncGcsBearerSecretProvider` blocks using the page's existing options (`heading_level: 4`, `show_bases: false`).

Out of scope: other docs (`t02`), OKF (`t03`, `t04`).

## Bounded Read Manifest

- `docs/usage/storage-auth-and-signing.md` — token cache section.
- `docs/usage/analytics-and-cdc.md` — DuckDB section and secrets bullet.
- `docs/reference/storage.md` — `## GCS token caches` and mkdocstrings option style.
- `src/py/vertebra/storage/_secrets.py` — provider API [^providers].
- `src/py/vertebra/db/adapters/duckdb/config.py` — validation messages [^secret-config].

## Symbol & Signature Contracts

- Examples import from public paths only: `from vertebra.storage import GCSConfig, SyncGcsBearerSecretProvider` and `from vertebra.db.adapters.duckdb import DuckDBConfig, DuckDBSecretConfig, SyncDuckDBDriver`.
- Examples declare structs inline, no `frozen=True, gc=False` boilerplate, no single-use intermediates beyond `config`/`provider`.

## Edge Cases & Error Semantics

- Examples never show `persistent=True` with a provider.
- Wording states that `required=False` secrets skip provider failures and that the default `required=True` raises.

## Public-Seam Test Blueprint

- **Gate**: `documentation_validation`.

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Site build | `make docs` | Exit `0`; reference HTML contains `SyncGcsBearerSecretProvider` and `AsyncGcsBearerSecretProvider` anchors |
| Names exist | `uv run --no-sync python -c "from vertebra.storage import SyncGcsBearerSecretProvider, AsyncGcsBearerSecretProvider, GCSConfig; from vertebra.db.adapters.duckdb import DuckDBConfig, DuckDBSecretConfig"` | Exit `0` |
| Example constructs | Run the analytics example up to `DuckDBConfig(...)` with `GCSConfig(bucket="lake", access_token="t")` | No exception (no network needed) |

## Verification

- `make docs`
- `uv run --no-sync python -c "from vertebra.storage import SyncGcsBearerSecretProvider, AsyncGcsBearerSecretProvider"`
- `git grep -n "SyncGcsBearerSecretProvider" -- docs/usage/storage-auth-and-signing.md docs/usage/analytics-and-cdc.md docs/reference/storage.md` lists all three files.

## Acceptance Criteria

- [ ] Storage auth guide, analytics guide, and storage reference document both providers and rotation semantics (R15).
- [ ] `make docs` exits `0` (R15).

## Notes & Discoveries

- [2026-11-12 00:30] `docs/index.md` already claims DuckDB `httpfs` ADC integration; it becomes accurate with ch02 and needs no edit.

[^providers]: `src/py/vertebra/storage/_secrets.py::SyncGcsBearerSecretProvider` — Shipped providers.
[^secret-config]: `src/py/vertebra/db/adapters/duckdb/config.py::DuckDBSecretConfig` — Validation rules.
