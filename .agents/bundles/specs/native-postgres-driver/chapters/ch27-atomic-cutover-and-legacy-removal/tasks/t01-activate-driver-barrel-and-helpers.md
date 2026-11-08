---
type: Task
id: "native-postgres-driver:ch27:t01"
chapter_id: ch27-atomic-cutover-and-legacy-removal
title: Activate permanent native PostgreSQL driver barrel, package exports, and numeric embedding pushdown
description: Switch vertebra.db.adapters.postgres.driver and package __init__ to export the permanent _sync, _async, types, events, and change_stream contracts atomically, update PostgresEmbeddingPushdown to numeric binds with verified Google-style docstrings, and migrate core driver unit tests to canonical public imports.
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch26:t04"
read_set:
  - src/py/vertebra/db/adapters/postgres/driver.py
  - src/py/vertebra/db/adapters/postgres/__init__.py
  - src/py/vertebra/db/adapters/postgres/embeddings.py
  - src/py/vertebra/db/adapters/postgres/config.py
  - src/py/tests/unit/db/test_postgres_driver.py
  - .agents/bundles/specs/native-postgres-driver/contracts.md
files:
  - src/py/vertebra/db/adapters/postgres/driver.py
  - src/py/vertebra/db/adapters/postgres/__init__.py
  - src/py/vertebra/db/adapters/postgres/embeddings.py
tests:
  - src/py/tests/unit/db/test_postgres_driver.py
  - src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py
  - src/py/tests/unit/db/test_postgres_native_adapters.py
  - src/py/tests/unit/db/test_postgres_native_adapter_pipeline.py
  - src/py/tests/unit/db/test_postgres_native_adapter_arrow.py
estimated_loc: 230
plan_revision: 4
plan_commit: dd432f18
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
updated_at: "2026-11-07T18:00:00Z"
commit: null
sources:
  - id: pg-driver-py
    resource: src/py/vertebra/db/adapters/postgres/driver.py::SyncPostgreSQLDriver
    title: Public PostgreSQL driver barrel module
  - id: pg-init-py
    resource: src/py/vertebra/db/adapters/postgres/__init__.py
    title: Lazy package export map for vertebra.db.adapters.postgres
  - id: pg-embeddings-py
    resource: src/py/vertebra/db/adapters/postgres/embeddings.py::PostgresEmbeddingPushdown
    title: AlloyDB SQL embedding pushdown and pgvector similarity search helper
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Frozen public contracts for native PostgreSQL activation
tags:
  - postgres
  - cutover
  - driver
  - embeddings
---

# Task t01: Activate Permanent Native PostgreSQL Driver Barrel, Package Exports, and Numeric Embedding Pushdown

## Objective & Scope Boundary

Atomically switch `src/py/vertebra/db/adapters/postgres/driver.py` and `src/py/vertebra/db/adapters/postgres/__init__.py` to select the completed `._sync.SyncPostgreSQLDriver` and `._async.AsyncPostgreSQLDriver` classes together with exact module-owned lazy exports [^pg-driver-py] [^pg-init-py], update `PostgresEmbeddingPushdown` in `embeddings.py` to emit unconditional `$1`/`$2` numeric binds across both native drivers with complete Google-style docstrings [^pg-embeddings-py], and migrate core adapter/pipeline/Arrow/dataplane unit tests from direct `._sync`/`._async` imports and legacy SQLite/mock ports to canonical public imports over `postgres_ports.py` [^pg-contracts].

Out of scope: `extensions/sqlspec/adapters.py` and `extensions/sqlalchemy/dialects.py` activation (`ch27:t02`), and physical deletion of private `_LegacyAsyncPostgreSQLDriver` (`ch27:t03`) and `_LegacySyncPostgreSQLDriver` (`ch27:t04`).

## Bounded Read Manifest

- [src/py/vertebra/db/adapters/postgres/driver.py](../../../../../../../src/py/vertebra/db/adapters/postgres/driver.py) — Existing `SyncPostgreSQLDriver` and `AsyncPostgreSQLDriver` definitions to rename private while importing `._sync` and `._async` [^pg-driver-py].
- [src/py/vertebra/db/adapters/postgres/__init__.py](../../../../../../../src/py/vertebra/db/adapters/postgres/__init__.py) — `__all__`, `__lazy_modules__`, and `_EXPORT_MAP` package exports [^pg-init-py].
- [src/py/vertebra/db/adapters/postgres/embeddings.py](../../../../../../../src/py/vertebra/db/adapters/postgres/embeddings.py) — `PostgresEmbeddingPushdown.embed_documents`, `similarity_search`, and `_uses_dollar_binds` [^pg-embeddings-py].
- [src/py/vertebra/db/adapters/postgres/config.py](../../../../../../../src/py/vertebra/db/adapters/postgres/config.py) — `PostgreSQLConfig`, `DatabaseRegistry` resolution, and native primitive option projection.
- [src/py/tests/unit/db/test_postgres_driver.py](../../../../../../../src/py/tests/unit/db/test_postgres_driver.py) — Existing legacy port tests (`_SQLiteAsyncPGPool`, `MockConnectionPool`, `executor=...`) to migrate to shared native ports; `driver-fail-loud` already replaces its `memory://` cases.
- [.agents/bundles/specs/native-postgres-driver/contracts.md](../../../contracts.md) — Final activation, value record, and constructor invariants [^pg-contracts].

## Symbol & Signature Contracts

- `src/py/vertebra/db/adapters/postgres/driver.py`:
  - Rename existing legacy classes in-place to `_LegacySyncPostgreSQLDriver` and `_LegacyAsyncPostgreSQLDriver` (updating `_LegacyAsyncPostgreSQLDriver.__init__` and `decode_binary_jsonb` static call to reference `_LegacySyncPostgreSQLDriver` so the module stays statically valid until `t03`/`t04` delete them).
  - Import `from vertebra.db.adapters.postgres._async import AsyncPostgreSQLDriver` and `from vertebra.db.adapters.postgres._sync import SyncPostgreSQLDriver` at top-level grouped imports, keeping `__all__ = ("AsyncPostgreSQLDriver", "SyncPostgreSQLDriver")`. Zero runtime flags, environment toggles, or fallback branches select `_Legacy*`.
  - Maintain complete Google-style module and public symbol docstrings across `driver.py` and `embeddings.py` (including PEP 257 attribute docstrings on any `msgspec.Struct` fields) verified by `tools.docs.scan_public_docstrings` so `ch30:t01`'s `make docs` Griffe build succeeds without touching Python files.
- `src/py/vertebra/db/adapters/postgres/__init__.py`:
  - Keep `"AsyncPostgreSQLDriver": ("vertebra.db.adapters.postgres.driver", "AsyncPostgreSQLDriver")` and `"SyncPostgreSQLDriver": ("vertebra.db.adapters.postgres.driver", "SyncPostgreSQLDriver")` in `_EXPORT_MAP`.
  - Map `PostgresArrowResult` and `PostgresReplicationIdentity` (alongside `PostgresColumn`, `PostgresQueryResult`, `PgInterval`, `PgArray`, `PgComposite`, `PgTemporal`, `PgJson`, `PgBitString`, `PgVector`, `PgParameter`, `PostgresErrorInfo`, `PostgresPipelineOperation`, and `PostgresPipelineOutcome`) to `vertebra.db.adapters.postgres.types` (keeping `decode_pg_arrow_stream` and `encode_pg_arrow_stream` mapped to `vertebra.db.adapters.postgres.arrow`).
  - Map `PostgresNotificationDriver` and `PostgresReplicationDriver` to `vertebra.db.adapters.postgres.events`.
  - Map `PostgresChangeConfig`, `PostgresChangeSource`, `PostgresCheckpoint`, `PostgresCheckpointStore`, `MemoryPostgresCheckpointStore`, `PostgresCheckpointConflictError`, and `PostgresReplicationGapError` to `vertebra.db.adapters.postgres.change_stream`.
- `src/py/vertebra/db/adapters/postgres/embeddings.py`:
  - Remove `PostgresEmbeddingPushdown._uses_dollar_binds()` and emit `("$1", "$2")` unconditionally in `embed_documents(...)` (`SELECT embedding($1, t.txt)::vector FROM unnest($2::text[]) WITH ORDINALITY AS t(txt, idx) ORDER BY t.idx`) and `similarity_search(...)` (`SELECT *, <col> <op> $1::vector AS distance FROM <table> ORDER BY distance ASC LIMIT $2`) for both `SyncPostgreSQLDriver` and `AsyncPostgreSQLDriver`.
  - Extract decoded `PgVector` (`row_val.values`), `VectorView`, binary `bytes`, or text pgvector payloads cleanly in `_extract_vector` while preserving `VectorBatchView.from_f32_rows` normalization.

## Edge Cases & Error Semantics

- Between `ch27:t01` and `ch27:t02`, pre-`t02` SQLSpec/SQLAlchemy sync callers that still emit `%s` pyformat placeholders continue passing because `SyncPostgreSQLDriver._prepare_native_query` lowers `%s` to `$N` via `PostgreSQLCore.prepare_native_query`.
- Calling `SyncPostgreSQLDriver` or `AsyncPostgreSQLDriver` when `vertebra._postgres` is uninstalled raises `MissingDependencyError` naming `vertebra-core[postgres]` (`extra="postgres"`).
- Legacy test-only constructor kwargs (`executor=...`, `async_executor=...`) raise `TypeError`, and `PostgreSQLConfig(dsn="memory://")` keeps failing closed with `ImproperConfigurationError` (the `driver-fail-loud` guard) on the permanent drivers.
- `PostgresEmbeddingPushdown.embed_documents` raises `DataError` when the returned row count disagrees with `len(texts)` and `ValueError` on empty `texts` or nonpositive `dimensions`.

## Public-Seam Test Blueprint

- **Canonical test files**: `src/py/tests/unit/db/test_postgres_driver.py`, `src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py`, `src/py/tests/unit/db/test_postgres_native_adapters.py`, `src/py/tests/unit/db/test_postgres_native_adapter_pipeline.py`, `src/py/tests/unit/db/test_postgres_native_adapter_arrow.py`.
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Canonical class & `_EXPORT_MAP` resolution | `from vertebra.db.adapters.postgres import SyncPostgreSQLDriver, AsyncPostgreSQLDriver, PostgresArrowResult, PostgresReplicationIdentity, PostgresChangeSource, PostgresCheckpointStore` | Exact `._sync` / `._async` driver classes, `PostgresArrowResult` & `PostgresReplicationIdentity` from `types`, and `PostgresChange*` / `PostgresCheckpoint*` from `change_stream`; zero `_Legacy*` in `__all__`. |
| Missing wheel diagnostic | Mask `vertebra._postgres` absent and invoke `SyncPostgreSQLDriver(PostgreSQLConfig(dsn="postgresql://u:p@localhost/db")).execute("SELECT 1")` | Raises `MissingDependencyError` containing `"vertebra-core[postgres]"`. |
| Sync & async embedding pushdown numeric SQL | `PostgresEmbeddingPushdown(sync_driver, model="gemini-embedding-001", dimensions=3).embed_documents(["alpha", "beta"])` and `similarity_search([1.0, 0.0, 0.0], table="docs")` | Both sync and async drivers receive `$1` and `$2` numeric placeholders (`("gemini-embedding-001", ["alpha", "beta"])`) and return normalized `VectorBatchView` (`len == 2`) and ranked rows. |
| Native port migration & docstring scan | Execute DDL (`0`), DML counts, failed `BEGIN` retry, transaction rollback, and `PgVector` / `JSONB` queries + run `tools.docs.scan_public_docstrings` | All assertions pass over shared `postgres_ports.py` native doubles (`active_leases == 0`), zero legacy mock pools remain, and public/field docstrings pass cleanly. |

## Verification

- Initial Red baseline: `make dev && uv run --no-sync pytest src/py/tests/unit/db/test_postgres_driver.py src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py -q`
- Final Green + docstring + lint + typecheck gate: `make dev && PYTHONPATH="src/py:." uv run --no-sync python -m tools.docs.scan_public_docstrings --check-leaks --check-overloads --check-typeddicts src/py/vertebra/db/adapters/postgres && uv run --no-sync pytest src/py/tests/unit/db/test_postgres_driver.py src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py src/py/tests/unit/db/test_postgres_native_adapters.py src/py/tests/unit/db/test_postgres_native_adapter_pipeline.py src/py/tests/unit/db/test_postgres_native_adapter_arrow.py -q && uv run --no-sync ruff check src/py/vertebra/db/adapters/postgres src/py/tests/unit/db && uv run --no-sync mypy src/py/vertebra/db/adapters/postgres`

## Acceptance Criteria

- [ ] `driver.py` and `postgres/__init__.py` export the permanent `._sync.SyncPostgreSQLDriver` and `._async.AsyncPostgreSQLDriver` classes plus exact `types`, `events`, and `change_stream` lazy targets with zero legacy fallback selection (R24).
- [ ] `PostgresEmbeddingPushdown` emits `$1`/`$2` numeric binds across both sync and async PostgreSQL drivers, and `driver.py` + `embeddings.py` pass `tools.docs.scan_public_docstrings` (R24).
- [ ] `test_postgres_driver.py`, `test_pg_dataplane_and_datafusion.py`, and native adapter unit suites import canonical public paths and pass over shared native ports (R24).

## Notes & Discoveries

- [2026-11-07 16:45] Preserve `PostgreSQLCore` CPU helpers (`strip_jsonb_header`, `encode_jsonb`, `encode_pgcopy_rows`, `decode_pgcopy_rows`) and non-PostgreSQL cases in `test_pg_dataplane_and_datafusion.py`.

[^pg-driver-py]: `src/py/vertebra/db/adapters/postgres/driver.py::SyncPostgreSQLDriver` — Public PostgreSQL driver barrel module.
[^pg-init-py]: `src/py/vertebra/db/adapters/postgres/__init__.py` — Lazy package export map for `vertebra.db.adapters.postgres`.
[^pg-embeddings-py]: `src/py/vertebra/db/adapters/postgres/embeddings.py::PostgresEmbeddingPushdown` — AlloyDB SQL embedding pushdown and `pgvector` similarity search helper.
[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Frozen public contracts for native PostgreSQL activation.
