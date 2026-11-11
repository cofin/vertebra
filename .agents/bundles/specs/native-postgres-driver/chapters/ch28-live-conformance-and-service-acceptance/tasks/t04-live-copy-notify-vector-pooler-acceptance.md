---
type: Task
id: "native-postgres-driver:ch28:t04"
chapter_id: ch28-live-conformance-and-service-acceptance
title: Verify live binary COPY, VTBARROW, notifications, pgvector pushdown, and PgBouncer pooling
description: Execute parameterized live feature acceptance across binary COPY, /dev/shm-verified VTBARROW selection/streaming/import, LISTEN/NOTIFY rollback suppression, pgvector 0.8.1 PgVector/PgBitString/PostgresEmbeddingPushdown, and PgBouncer transaction pooling.
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
plan_revision: 6
plan_commit: bf53faf3
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
verified:
  - by: agent:plan-generator
    at: "2026-11-10T14:45:00Z"
    commit: bf53faf3
sources:
  - id: int-conftest
    resource: src/py/tests/integration/conftest.py::vtb_postgres_config
    title: VERTEBRA_CONFORMANCE_DOCKER-gated fixtures for PG, pgvector 0.8.1, and SCRAM PgBouncer 1.26.0
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

Join the cut-over Python PostgreSQL adapters, `vertebra._postgres` native Rust engine, and `VERTEBRA_CONFORMANCE_DOCKER == "1"`-gated live `ch28:t02`–`t03` Docker services in `src/py/tests/integration/db/test_postgres_features.py` (plus composite fixture helpers in `src/py/tests/integration/conftest.py`) using `@pytest.mark.parametrize` and parameterized fixtures exclusively (zero `for` loops over test cases, operators, modes, or sync/async backends inside pytest test bodies) [^int-conftest] [^pg-embeddings] [^pg-notify] [^db-arrow] [^pg-contracts]:
1. Live binary `COPY` in/out and native `ArrowCursor` (`select_to_arrow`, `stream_arrow_batches`, `load_from_arrow`) round-trip across `NUMERIC`/`Decimal`, temporal, `JSONB`, dimensioned arrays, and `NULL`s without Python tuple materialization, plus early generator close / `asyncio.Task.cancel()` recovery and explicit `/dev/shm` snapshot assertions confirming zero lingering test-owned SHM segments.
2. Live `PostgresNotificationSource` `LISTEN`/`NOTIFY` delivery, async transaction rollback suppression, sync-transaction `InterfaceError` refusal, and `direct_dsn` bypass when `PostgreSQLConfig` targets transaction-mode `PgBouncer` (plus `direct_dsn` verification for `PostgresChangeSource`).
3. Live `pgvector` 0.8.1 (`PG14` and `PG18`) `PgVector` (`vector`, `halfvec`) and `PgBitString` (`bit`, `varbit`) binds/results, dimension/NaN/Inf negative errors, `NULL`s, cold/warm OID fallback, and `PostgresEmbeddingPushdown` against a test-owned SQL `embedding(text, text) RETURNS vector` function.
4. Live `PgBouncer` transaction-mode (`max_prepared_statements=0` and `200`) and session/direct acceptance: disabled cache on `0`, named statement reuse on `200` + `protocol_level_pooler_cache=True`, transaction stickiness, and stale plan invalidation after `ALTER TABLE`.

Out of scope: Re-implementing protocol-level unit fixtures or duplicating `ch28:t02`'s five-version logical replication matrix.

## Bounded Read Manifest

- [src/py/tests/integration/conftest.py](../../../../../../../src/py/tests/integration/conftest.py) — `require_live_postgres_docker`, `pgvector` 0.8.1, SCRAM `PgBouncer` 1.26.0, and logical PostgreSQL fixtures from `ch28:t01`–`t03` [^int-conftest].
- [src/py/vertebra/db/adapters/postgres/embeddings.py](../../../../../../../src/py/vertebra/db/adapters/postgres/embeddings.py) — `PostgresEmbeddingPushdown` SQL expressions and similarity search [^pg-embeddings].
- [src/py/vertebra/db/adapters/postgres/notify.py](../../../../../../../src/py/vertebra/db/adapters/postgres/notify.py) — `PostgresNotificationSource` publish and subscribe streams [^pg-notify].
- [src/py/vertebra/db/arrow.py](../../../../../../../src/py/vertebra/db/arrow.py) — `ArrowCursor`, `ARROW_SHM_THRESHOLD_ROWS`, and `ShmCapsule` views [^db-arrow].
- [.agents/bundles/specs/native-postgres-driver/contracts.md](../../../contracts.md) — `PgVector`, `PgBitString`, `PostgresArrowResult`, `direct_dsn`, and pooler cache contracts [^pg-contracts].

## Symbol & Signature Contracts

- `src/py/tests/integration/conftest.py`:
  - Expose `VERTEBRA_CONFORMANCE_DOCKER == "1"`-gated composite fixture pairing SCRAM `PgBouncer` `proxy_config` (`pooler_mode="transaction"`, `direct_dsn=direct_config.dsn`) with the backing logical/direct endpoint, plus a `/dev/shm` snapshot helper recording Vertebra SHM segment names matching the current process (`os.getpid()`) / `vtb*` / `vertebra*` before and after Arrow tests (no-op returning `frozenset()` on non-Linux hosts lacking `/dev/shm`).
- `src/py/tests/integration/db/test_postgres_features.py`:
  - Exercise public `SyncPostgreSQLDriver` and `AsyncPostgreSQLDriver` (`select_to_arrow(..., threshold_rows=..., max_result_bytes=...)`, `stream_arrow_batches(..., batch_size=..., columns=...)`, `load_from_arrow(table_name, data, columns=..., schema_name=...)`, `transaction`, `pipeline`), `PostgresNotificationSource`, `PostgresChangeSource` (`direct_dsn` preflight/open), and `PostgresEmbeddingPushdown` using `@pytest.mark.parametrize` for sync/async drivers, invalid `PgVector` inputs, positive `similarity_search` distance metrics `("cosine", "euclidean", "dot")`, negative `pytest.raises(ValueError)` rejection on invalid metric strings such as `"l2"`, and `PgBouncer` modes.

## Edge Cases & Error Semantics

- Breaking out of `stream_arrow_batches` early (`break` / `aclose()`), cancelling an active async Arrow portal stream (`task.cancel()`), or closing/GC-releasing SHM-backed `ArrowCursor` objects recovers or retires the pinned session cleanly (`SELECT 1` succeeds immediately on the next acquire) AND leaves `snapshot_shm_after - snapshot_shm_before == frozenset()` (zero lingering test-owned `/dev/shm` segments after cursor closure and `gc.collect()`).
- `PgVector` dimension outside `1..16000`, `NaN`/`Inf`, or `halfvec` overflow to infinity raises `ValueError` / `DataError` without corrupting the session; `PgBitString(b"\xa0", 3)` round-trips `bit(3)` / `varbit` (`101`) losslessly.
- Opening `PostgresNotificationSource` or `PostgresChangeSource` on a `pooler_mode="transaction"` driver without `direct_dsn` raises `ImproperConfigurationError` before any network dial, whereas setting `direct_dsn` streams live committed events produced through the `PgBouncer` transaction pool while rolled-back async transactions deliver zero notifications.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/integration/db/test_postgres_features.py`.
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Live binary `COPY`, `VTBARROW`, early portal close, & `/dev/shm` hygiene | Parameterized sync/async `load_from_arrow` -> `select_to_arrow` (inline & SHM `threshold_rows`) -> `stream_arrow_batches` full drain, early `break`/`aclose()`, and `task.cancel()` with `/dev/shm` before/after snapshots | Exact row counts & lossless `Decimal`/`TIMESTAMPTZ`/`JSONB`/array/`NULL` values, clean post-close `SELECT 1`, and `0` lingering test-owned `/dev/shm` segments after cursor close + `gc.collect()`. |
| Live `LISTEN`/`NOTIFY` + rollback suppression + `direct_dsn` | Subscribe via `PostgresNotificationSource` on transaction-pooled driver with `direct_dsn`; publish in rolled-back vs committed async tx; verify `PostgresChangeSource` over `direct_dsn` | Committed payload arrives once in order; rolled-back payload is suppressed; missing `direct_dsn` raises `ImproperConfigurationError` preflight. |
| Parameterized `pgvector` 0.8.1 (`PG14`/`PG18`) + `PostgresEmbeddingPushdown` | Bind/decode `PgVector(..., "vector")`, `PgVector(..., "halfvec")`, `PgBitString(b"\xa0", 3)`, invalid dim/`NaN`/`Inf`, negative invalid metric strings (`"l2"` raising `ValueError`), and parameterized `("cosine", "euclidean", "dot")` `similarity_search` against SQL `embedding()` | Exact binary32/binary16/bit values and `NULL`s, deterministic `ValueError`/`DataError` rejection with healthy session afterward, and exact ranked similarity ordering across `("cosine", "euclidean", "dot")` on sync & async drivers. |
| Parameterized `PgBouncer` `max_prepared_statements=0` vs `200` + DDL invalidation | Repeated parameterized queries, `transaction()`, `pipeline()`, and `ALTER TABLE ... TYPE` across `PgBouncer` transaction mode (`0` and `200`) | `0` avoids prepared-statement collisions across backend rotation; `200` reuses named statements and invalidates cached plans cleanly after DDL. |

## Verification

- Offline safety check: `env -u VERTEBRA_CONFORMANCE_DOCKER -u VERTEBRA_CONFORMANCE_POSTGRES_DSN -u VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS uv run --no-sync pytest src/py/tests/integration/db/test_postgres_features.py -q`
- Final strict Chapter 28 live Green + Rust + lint gate: `make dev && VERTEBRA_CONFORMANCE_DOCKER=1 VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS=1 uv run --no-sync pytest src/py/tests/integration/db/test_driver_conformance.py -k postgres src/py/tests/integration/db/test_postgres_native.py src/py/tests/integration/db/test_postgres_replication.py src/py/tests/integration/db/test_postgres_services.py src/py/tests/integration/db/test_postgres_features.py -q -rs && PYTHONPATH="$(pwd)/src/py" cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --locked && uv run --no-sync ruff check src/py/tests/integration`

## Acceptance Criteria

- [ ] Parameterized live binary `COPY`, `VTBARROW` `ArrowCursor`, portal early-close/cancellation with `/dev/shm` zero-leak verification, and `LISTEN`/`NOTIFY` rollback suppression pass with `0` skips/xfails under `VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS=1` (R16, R19, R20, R21, R25).
- [ ] Multi-arch `pgvector` 0.8.1 (`PG14`/`PG18`) `PgVector`/`PgBitString`/`PostgresEmbeddingPushdown` and SCRAM `PgBouncer` 1.26.0 transaction-pooler (`0` and `200`) + `direct_dsn` acceptance pass end-to-end with zero test-body `for` loops (R15, R25).

## Notes & Discoveries

- Test-owned SQL `embedding(model text, input text) RETURNS vector` validates `PostgresEmbeddingPushdown` bind ordering, vector decoding, and parameterized distance rankings locally without external ML inference calls.

[^int-conftest]: `src/py/tests/integration/conftest.py::vtb_postgres_config` — `VERTEBRA_CONFORMANCE_DOCKER`-gated fixtures for PG, `pgvector` 0.8.1, and SCRAM `PgBouncer` 1.26.0.
[^pg-embeddings]: `src/py/vertebra/db/adapters/postgres/embeddings.py::PostgresEmbeddingPushdown` — SQL embedding pushdown and `pgvector` similarity search helper.
[^pg-notify]: `src/py/vertebra/db/adapters/postgres/notify.py::PostgresNotificationSource` — PostgreSQL notification source and publication transport.
[^db-arrow]: `src/py/vertebra/db/arrow.py::ArrowCursor` — Public `ArrowCursor` wrapper over inline IPC and `VTBARROW` `ShmCapsule`.
[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Arrow, `COPY`, `PgVector`, `PgBitString`, event hook, and pooler contracts.
