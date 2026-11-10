---
type: Task
id: "native-postgres-driver:ch28:t04"
chapter_id: ch28-live-conformance-and-service-acceptance
title: Verify live binary COPY, VTBARROW, notifications, pgvector pushdown, and PgBouncer pooling
description: Execute end-to-end live feature acceptance across binary COPY, native VTBARROW selection/streaming/import, LISTEN/NOTIFY rollback suppression, pgvector 0.8.1 PgVector/PgBitString/PostgresEmbeddingPushdown, and PgBouncer transaction pooling.
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch28:t03"
read_set:
  - src/py/tests/integration/conftest.py
  - src/py/vertebra/db/adapters/postgres/embeddings.py
  - src/py/vertebra/db/adapters/postgres/notify.py
  - src/py/vertebra/db/arrow.py
  - .agents/bundles/specs/native-postgres-driver/contracts.md
files:
  - src/py/tests/integration/conftest.py
tests:
  - src/py/tests/integration/db/test_postgres_features.py
estimated_loc: 245
plan_revision: 5
plan_commit: fd571e3a
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
updated_at: "2026-11-08T02:05:22Z"
commit: null
sources:
  - id: int-conftest
    resource: src/py/tests/integration/conftest.py::vtb_postgres_config
    title: Integration service fixtures for vanilla PG, pgvector 0.8.1, and PgBouncer 1.26.0
  - id: pg-embeddings
    resource: src/py/vertebra/db/adapters/postgres/embeddings.py::PostgresEmbeddingPushdown
    title: SQL embedding pushdown and pgvector similarity search helper
  - id: pg-notify
    resource: src/py/vertebra/db/adapters/postgres/notify.py::PostgresNotificationSource
    title: PostgreSQL notification source and publication transport
  - id: db-arrow
    resource: src/py/vertebra/db/arrow.py::ArrowCursor
    title: Public ArrowCursor wrapper over inline IPC and VTBARROW ShmCapsule
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Arrow, COPY, PgVector, PgBitString, event hook, and pooler contracts
tags:
  - postgres
  - copy
  - arrow
  - pgvector
  - pgbouncer
  - notifications
---

# Task t04: Verify Live Binary `COPY`, `VTBARROW`, Notifications, `pgvector` Pushdown, and `PgBouncer` Pooling

## Objective & Scope Boundary

Join the cut-over Python PostgreSQL adapters, `vertebra._postgres` native Rust engine, and live `ch28:t02`–`t03` Docker services in `src/py/tests/integration/db/test_postgres_features.py` (with any shared fixture wiring in `src/py/tests/integration/conftest.py`) [^int-conftest] [^pg-embeddings] [^pg-notify] [^db-arrow] [^pg-contracts]:
1. Live binary `COPY` in/out and native `ArrowCursor` (`select_to_arrow`, `stream_arrow_batches`, `load_from_arrow`) round-trip across `NUMERIC`/`Decimal`, temporal, `JSONB`, dimensioned arrays, and `NULL`s without Python tuple row materialization, plus early generator close and `asyncio` stream cancellation recovery.
2. Live `PostgresNotificationSource` `LISTEN`/`NOTIFY` delivery, async transaction rollback suppression, and `direct_dsn` bypass when `PostgreSQLConfig` targets transaction-mode `PgBouncer` (plus `direct_dsn` verification for `vertebra.db.adapters.postgres.change_stream.PostgresChangeSource` without duplicating `ch28:t02` WAL tests).
3. Live `pgvector` 0.8.1 (`PG14` and `PG18`) `PgVector` (`vector`, `halfvec`) and `PgBitString` (`bit`, `varbit`) binds/results, dimension/NaN/Inf negative errors, `NULL`s, cold/warm OID fallback, and `PostgresEmbeddingPushdown` against a test-owned SQL `embedding(text, text) RETURNS vector` function.
4. Live `PgBouncer` transaction-mode (`max_prepared_statements=0` and `200`) and session/direct acceptance: disabled cache on `0`, named statement reuse on `200` + `protocol_level_pooler_cache=True`, transaction stickiness, and stale plan invalidation on DDL schema change.

Out of scope: Re-implementing protocol-level unit fixtures or duplicating `ch28:t02`'s five-version logical replication matrix.

## Bounded Read Manifest

- [src/py/tests/integration/conftest.py](../../../../../../../src/py/tests/integration/conftest.py) — `pgvector` 0.8.1, `PgBouncer` 1.26.0, and logical PostgreSQL fixtures from `ch28:t01`–`t03` [^int-conftest].
- [src/py/vertebra/db/adapters/postgres/embeddings.py](../../../../../../../src/py/vertebra/db/adapters/postgres/embeddings.py) — `PostgresEmbeddingPushdown` SQL expressions and similarity search [^pg-embeddings].
- [src/py/vertebra/db/adapters/postgres/notify.py](../../../../../../../src/py/vertebra/db/adapters/postgres/notify.py) — `PostgresNotificationSource` publish and subscribe streams [^pg-notify].
- [src/py/vertebra/db/arrow.py](../../../../../../../src/py/vertebra/db/arrow.py) — `ArrowCursor`, `ARROW_SHM_THRESHOLD_ROWS`, and `ShmCapsule` views [^db-arrow].
- [.agents/bundles/specs/native-postgres-driver/contracts.md](../../../contracts.md) — `PgVector`, `PgBitString`, `PostgresArrowResult`, `direct_dsn`, and pooler cache contracts [^pg-contracts].

## Symbol & Signature Contracts

- `src/py/tests/integration/conftest.py`:
  - Expose composite fixture helper(s) pairing `PgBouncer` `proxy_config` (`pooler_mode="transaction"`, `direct_dsn=direct_config.dsn`) with the backing server's logical/direct endpoint so `test_postgres_features.py` exercises pooled queries and `direct_dsn` event sessions against the same live database.
- `src/py/tests/integration/db/test_postgres_features.py`:
  - Exercise public `SyncPostgreSQLDriver` and `AsyncPostgreSQLDriver` (`select_to_arrow`, `stream_arrow_batches`, `load_from_arrow`, `transaction`, `pipeline`), `PostgresNotificationSource`, `PostgresChangeSource` (`vertebra.db.adapters.postgres.change_stream`, `direct_dsn` preflight/open), and `PostgresEmbeddingPushdown`.

## Edge Cases & Error Semantics

- Breaking out of `stream_arrow_batches` early (`break` / `aclose()`) or cancelling an active async Arrow portal stream recovers or retires the pinned session cleanly so the next pool acquisition executes `SELECT 1` without hanging or leaking pool capacity.
- `PgVector` dimension outside `1..16000`, `NaN`/`Inf`, or `halfvec` overflow to infinity raises `ValueError` / `DataError` before or at native bind without corrupting the session; `PgBitString(b"\xa0", 3)` round-trips `bit(3)` / `varbit` (`101`) losslessly.
- Opening `PostgresNotificationSource` or `PostgresChangeSource` on a `pooler_mode="transaction"` driver without `direct_dsn` fails closed before network dial (`ImproperConfigurationError` / `InterfaceError`), whereas configuring `direct_dsn` connects directly to the backing PostgreSQL server and delivers live events produced by pooled transactions.
- Publishing via `await source.publish(...)` inside a rolled-back `AsyncPostgreSQLDriver.transaction()` delivers zero notifications to active listeners.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/integration/db/test_postgres_features.py`.
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Live binary `COPY` & `VTBARROW` round-trip + early stream close | `load_from_arrow` -> `select_to_arrow` (below & above `ARROW_SHM_THRESHOLD_ROWS`) -> `stream_arrow_batches` early `break` across `Decimal`, `TIMESTAMPTZ`, `JSONB`, arrays, `NULL` | Exact row counts, lossless schema/values, inline vs `ShmCapsule` threshold honored, and clean lease recovery after early stream close. |
| Live `LISTEN`/`NOTIFY` + rollback suppression + `direct_dsn` | Subscribe via `PostgresNotificationSource` on transaction-pooled driver with `direct_dsn`; publish in rolled-back vs committed async tx | Committed payload arrives once in order; rolled-back payload is suppressed; missing `direct_dsn` in transaction mode fails closed. |
| Live `pgvector` 0.8.1 (`PG14`/`PG18`) + `PostgresEmbeddingPushdown` | Bind/decode `PgVector(..., "vector")`, `PgVector(..., "halfvec")`, `PgBitString(b"\xa0", 3)`, and call `embed_documents` / `similarity_search` against test SQL `embedding()` | Exact binary32/binary16/bit values, `NULL`s, invalid dimension rejection, and cosine/euclidean/dot `similarity_search` ordering. |
| Live `PgBouncer` `max_prepared_statements=0` vs `200` + DDL invalidation | Run repeated parameterized queries, `transaction()`, `pipeline()`, and `ALTER TABLE` across `PgBouncer` transaction mode (`0` and `200`) | `0` runs safely without prepared statement collisions; `200` reuses named statements and invalidates stale cached plans cleanly after DDL. |

## Verification

- Initial Red baseline: `make dev && VERTEBRA_CONFORMANCE_DOCKER=1 uv run --no-sync pytest src/py/tests/integration/db/test_postgres_features.py -q -rs`
- Final Green + Rust + lint gate: `make dev && VERTEBRA_CONFORMANCE_DOCKER=1 uv run --no-sync pytest src/py/tests/integration/db/test_postgres_features.py -q -rs && cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres && uv run --no-sync ruff check src/py/tests/integration/conftest.py src/py/tests/integration/db/test_postgres_features.py`

## Acceptance Criteria

- [ ] Live binary `COPY`, `VTBARROW` `ArrowCursor`, portal early-close/cancellation, and `LISTEN`/`NOTIFY` rollback suppression pass against live PostgreSQL containers without skips (R16, R19, R20, R21, R25).
- [ ] Digest-pinned `pgvector` 0.8.1 (`PG14`/`PG18`) `PgVector`/`PgBitString`/`PostgresEmbeddingPushdown` and `PgBouncer` 1.26.0 transaction-pooler (`0` and `200`) + `direct_dsn` acceptance pass end-to-end (R15, R25).

## Notes & Discoveries

- [2026-11-07 16:45] Test-owned SQL `embedding(model text, input text) RETURNS vector` validates `PostgresEmbeddingPushdown` bind ordering, vector decoding, and cosine/L2/inner-product search locally without claiming live Vertex/AlloyDB ML inference.

[^int-conftest]: `src/py/tests/integration/conftest.py::vtb_postgres_config` — Integration service fixtures for vanilla PG, `pgvector` 0.8.1, and `PgBouncer` 1.26.0.
[^pg-embeddings]: `src/py/vertebra/db/adapters/postgres/embeddings.py::PostgresEmbeddingPushdown` — SQL embedding pushdown and `pgvector` similarity search helper.
[^pg-notify]: `src/py/vertebra/db/adapters/postgres/notify.py::PostgresNotificationSource` — PostgreSQL notification source and publication transport.
[^db-arrow]: `src/py/vertebra/db/arrow.py::ArrowCursor` — Public `ArrowCursor` wrapper over inline IPC and `VTBARROW` `ShmCapsule`.
[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Arrow, `COPY`, `PgVector`, `PgBitString`, event hook, and pooler contracts.
