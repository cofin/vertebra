---
type: Spec
flow_id: native-postgres-driver
title: Standalone Rust PostgreSQL Wheel (vertebra-postgres)
description: Standalone PyO3 crate src/rs/vertebra-postgres (vertebra._postgres + vertebra-core[postgres]) implementing a full Rust PostgreSQL wire client with authentication, TLS, extended protocol, binary codecs, statement cache, COPY, live LISTEN/NOTIFY, single-Sync pipeline mode, cancellation, and VTBARROW ShmCapsule results, replacing psycopg and asyncpg in the Python driver.
state: draft
parent_prd: native-data-platform
research:
  - ../native-data-platform/research/rust-driver-crates_20261103.md
  - ../integration-remediation/research/audit-findings_20261103.md
plan_revision: 0
plan_commit: null
state_revision: 0
current_task: null
last_verified_checkpoint: null
created_at: 2026-11-03T04:05:00Z
updated_at: 2026-11-04T01:15:00Z
tags: [rust, postgres, driver, wheel, pipeline]
---

# Flow Charter: Standalone Rust PostgreSQL Wheel (`vertebra-postgres`)

## Approach

See [crate research](../native-data-platform/research/rust-driver-crates_20261103.md).

- **Base.** `postgres-protocol` 0.6.12 supplies frontend/backend message codecs and SASL (including `SCRAM-SHA-256-PLUS`) under a Vertebra-owned connection state machine in `src/rs/vertebra-postgres` built on `vertebra_db::connection` (`Connection`, `CancelHandle`, `TransportStream`, `install_rustls_ring_provider`), `vertebra_db::postgres` pure-CPU frame helpers, `vertebra_db::py_async` (`spawn_rust_db_task_with_attr(..., "_vertebra_postgres_completion_drainer")` over `AsyncCompletionQueue` + `Notifier`), and `SyncCompletionQueue` for synchronous worker release.
- **TLS.** `tokio-rustls` (`ring` + `webpki-root-certs` + custom CA/mTLS via `vertebra_db::connection`).
- **Test oracle.** `tokio-postgres` 0.7.18 as a `dev-dependency` reference implementation. (`tokio-postgres::Client` appends one `Sync` per statement and cannot emit single-`Sync` multi-statement pipelines.)
- **Packaging (`vertebra._postgres` + `vertebra-core[postgres]`).** Packaged as its own workspace PyO3 `cdylib` + `rlib` crate `src/rs/vertebra-postgres` (`#[pymodule(gil_used = false)]`, `src/py/vertebra/_postgres.pyi` verified by `mypy.stubtest`, `[project.optional-dependencies].postgres`, `[tool.uv.sources]`, `[[tool.bumpversion.files]]`, `uv lock --offline`, and `areas.toml` registration), handing off `VTBARROW` SHM cursors to `vertebra._vertebra.ArrowCursor` via `ShmCapsule` with zero copies.

## Scope

1. **Crate scaffold, connection & authentication (`src/rs/vertebra-postgres/`).**
   - `SyncPostgresClient` and `AsyncPostgresClient` in `vertebra._postgres`; startup message (`encode_startup_message`), cleartext/MD5/`SCRAM-SHA-256`/`SCRAM-SHA-256-PLUS` authentication, `sslmode` TLS negotiation (`disable`, `prefer`, `require`, `verify-ca`, `verify-full`), Cloud SQL (`:3307`) and AlloyDB (`:5433`) dialers via `vertebra_db::connection` / `vertebra_db::dialer`, IAM OAuth2 token passwords via `SyncGcpTokenCache` / `AsyncGcpTokenCache`, `ParameterStatus`, `BackendKeyData`, and TCP keepalive.
2. **Query protocols.**
   - Simple query and Extended Query protocol (`Parse`/`Bind`/`Describe`/`Execute`/`Sync`), portal row limits for chunked streaming, multi-result sets, and `ErrorResponse`/`NoticeResponse` decoding mapped onto `DatabaseError` subclasses (`vertebra._vertebra`) with `sqlstate`.
3. **Binary codecs.**
   - Binary send/receive for `bool`, `int2`/`int4`/`int8`, `float4`/`float8`, `numeric` (`Decimal`), `text`/`varchar`, `bytea`, `uuid`, `date`/`time`/`timestamp`/`timestamptz`/`interval`, `json`/`jsonb` (`\x01` version prefix), arrays, composites, and `pgvector` (`vector`/`halfvec`/`bit`), with text fallback for unknown OIDs and `msgspec` hydration in Python.
4. **Statements and pooling.**
   - Named prepared statements with `PoolerMode` awareness (`session` vs. PgBouncer `transaction` mode) and Rust connection pooling exported from `vertebra-postgres` (`rlib`) for `broker-pool-sharing`.
5. **Bulk & Arrow.**
   - Binary `COPY FROM STDIN` / `COPY TO STDOUT`, `load_from_arrow`, and `select_to_arrow` / `stream_arrow_batches` writing directly into `ShmArrowStreamWriter` and returning `vertebra._vertebra.ArrowCursor` via `ShmCapsule`.
6. **Notifications & logical replication.**
   - Wire live asynchronous `NotificationResponse` (`LISTEN` / `NOTIFY`) push frames from the Rust connection into `vertebra/db/adapters/postgres/notify.py::PostgresNotificationSource`, and add `PostgresChangeSource` implementing `AsyncChangeSource`.
7. **Pipelining.**
   - Implement `SyncPipelineCapable.pipeline()` (`SyncPipeline`) and `AsyncPipelineCapable.pipeline()` (`AsyncPipeline`) queuing `Parse`/`Bind`/`Execute` frames across N statements with a single trailing `Sync` and one socket flush, demultiplexing ordered results and per-statement errors.
8. **Cancellation.**
   - PostgreSQL `CancelRequest` frame using `BackendKeyData` (`process_id`, `secret_key`) wired to `driver.cancel()` and `_DbTaskCanceller` asyncio task cancellation.
9. **Python driver cutover.**
   - Cut `vertebra/db/adapters/postgres/driver.py` (`SyncPostgreSQLDriver`, `AsyncPostgreSQLDriver`) over to `ensure_module("vertebra._postgres", package="vertebra-core[postgres]", extra="postgres")`, removing `psycopg_pool`, `asyncpg` duck-typing, `memory://`, `_in_memory_rows`, and the `postgres` `xfail` marker in `test_driver_conformance.py`.

## Public Seams

`vertebra._postgres` (`SyncPostgresClient`, `AsyncPostgresClient`), `SyncPostgreSQLDriver`, `AsyncPostgreSQLDriver`, `driver.pipeline()` (`SyncPipelineCapable` / `AsyncPipelineCapable`), `PostgresNotificationSource`, and `PostgresChangeSource` (`AsyncChangeSource`).

## Dependencies

`db-layout-and-contract` (`refactor/sync-async-names @ 08ac03a`) and `integration-remediation/db-engine-primitives`.

## Acceptance Boundary

- `src/py/tests/integration/db/test_driver_conformance.py` passes natively (without `xfail`) against PostgreSQL 14–18 via `pytest-databases`.
- Golden wire fixtures cover startup, `SCRAM-SHA-256`, extended query, binary `COPY`, and single-`Sync` pipelining (verifying one socket flush per pipeline batch).
- `mypy.stubtest vertebra._postgres`, `make rs-bench`, `make bench-check`, `make lint-all`, `make rs-test`, and `make py-test` pass.

## Draft Task Outline

- t01 Scaffold `src/rs/vertebra-postgres` (`vertebra._postgres`, `_postgres.pyi`, `[postgres]` extra + `uv.lock`), connection state machine, `SCRAM-SHA-256`/`PLUS` auth, `sslmode` TLS, and Cloud SQL / AlloyDB dialers.
- t02 Simple and Extended Query protocol + `SQLSTATE` `DatabaseError` mapping across `SyncPostgresClient` and `AsyncPostgresClient`.
- t03 Binary scalar, array, JSONB, and `pgvector` codecs.
- t04 Prepared statement cache and PgBouncer transaction-pooler modes.
- t05 Binary `COPY` and direct `DataRow` -> `VTBARROW` `ShmCapsule` `ArrowCursor` streaming.
- t06 Live `LISTEN`/`NOTIFY` wiring into `PostgresNotificationSource` + `PostgresChangeSource` (`AsyncChangeSource`).
- t07 Single-`Sync` `pipeline()` (`SyncPipelineCapable` / `AsyncPipelineCapable`) and `CancelRequest` cancellation.
- t08 Python `SyncPostgreSQLDriver`/`AsyncPostgreSQLDriver` cutover + conformance activation.
