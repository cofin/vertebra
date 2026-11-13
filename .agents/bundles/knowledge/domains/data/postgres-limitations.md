---
type: Gotcha
title: PostgreSQL Native Driver Limitations
description: The native PostgreSQL wheel is available while public drivers and consumer guards retain legacy PostgreSQL dependencies.
status: stable
tags: [postgres, limitations, driver]
generated:
  by: flow-implement
  at: "2026-11-12T02:22:54Z"
verified:
  - by: agent:executor
    at: "2026-11-13T02:42:00Z"
    commit: ecf184e4
sources:
  - id: pg-stream
    resource: src/rs/vertebra-postgres/src/replication/stream.rs::ReplicationSession
    title: Bounded native session transport and explicit delivery publication
  - id: pg-supervisor
    resource: src/rs/vertebra-postgres/src/replication/supervisor.rs::ReplicationSupervisor
    title: Multi-generation verified reconnect supervisor and delivery leases
  - id: pg-feedback
    resource: src/rs/vertebra-postgres/src/replication/feedback.rs::ReplicationFeedback
    title: Native payload codecs and bounded published-Commit acknowledgements
  - id: pg-driver-py
    resource: src/py/vertebra/db/adapters/postgres/driver.py::SyncPostgreSQLDriver
    title: Public PostgreSQL drivers over psycopg_pool and asyncio.to_thread
  - id: pg-queue-py
    resource: src/py/vertebra/framework/queues/postgres.py::PostgreSQLQueueBackend
    title: Caller-supplied queue executor and psycopg dependency guard
  - id: pg-dialer-cloudsql
    resource: src/py/vertebra/db/dialer/cloudsql.py::CloudSQLConfig
    title: Cloud SQL dialer configuration and legacy connection helpers
  - id: pg-dialer-alloydb
    resource: src/py/vertebra/db/dialer/alloydb.py::AlloyDBConfig
    title: AlloyDB dialer configuration and legacy connection helpers
  - id: pg-driver-tests
    resource: src/py/tests/unit/db/test_postgres_driver.py::test_asyncpg_pool_transaction_and_affected_rows
    title: SQLite-backed asyncpg and psycopg pool test doubles
  - id: pg-pipeline-py
    resource: src/py/vertebra/db/adapters/postgres/pipeline.py::SyncPostgresPipeline
    title: Native PostgreSQL pipeline facade awaiting driver integration
  - id: pg-arrow-py
    resource: src/py/vertebra/db/adapters/postgres/arrow.py::load_arrow_sync
    title: Native Arrow COPY helper awaiting driver integration
  - id: pg-wheel
    resource: src/rs/vertebra-postgres/src/lib.rs
    title: Standalone native PostgreSQL extension registration
---

# PostgreSQL Native Driver Limitations

<!-- truth: start -->
- `vertebra._postgres` is available through `vertebra-core[postgres]`. `make dev` and `make build` build the extension; `make postgres-test` and `make postgres-wheel-check` define its verification lanes. Public PostgreSQL drivers do not use the native wheel yet [^pg-wheel] [^pg-driver-py].
- `SyncPostgreSQLDriver` uses `psycopg_pool.ConnectionPool`; its missing-dependency guard names `psycopg[binary,pool]`. `AsyncPostgreSQLDriver` offloads sync-driver operations with `asyncio.to_thread` [^pg-driver-py].
- `PostgreSQLQueueBackend` probes `psycopg` before raising `ImproperConfigurationError` when `executor=` is absent [^pg-queue-py].
- Cloud SQL and AlloyDB dialers retain `create_psycopg_connect_func` and `create_asyncpg_connect_func` [^pg-dialer-cloudsql] [^pg-dialer-alloydb].
- The PostgreSQL `pipeline.py` and `arrow.py` helpers have only unit-test importers. Public drivers do not expose native pipelines or Arrow COPY import through these helpers [^pg-pipeline-py] [^pg-arrow-py] [^pg-driver-py].
- `test_postgres_driver.py` exercises the async driver with a SQLite-backed asyncpg pool double and the sync driver with psycopg pool mocks [^pg-driver-tests].
- The title and body of [PR #104](https://github.com/cofin/vertebra/pull/104) describe a public-driver cutover absent from the merged source at `c8722c76`; the native PostgreSQL plan assigns that cutover to chapters `ch04`–`ch05` [^pg-driver-py].
- Native Rust feedback primitives decode borrowed XLogData/keepalive payloads, encode status payloads, and compute bounded heartbeat intervals. `ReplicationFeedback` separates received, published, and acknowledged Commit positions, caps unacknowledged publications at 1024, and rejects invalid updates without mutation. `ReplicationSession` and `ReplicationSupervisor` (`connect_logical_verified`) provide bounded Rust streaming and identity-verified multi-generation reconnect supervision, while Python `AsyncPostgresReplication` (`ch02`) and `PostgresChangeSource` (`ch03`) are not yet exposed [^pg-feedback] [^pg-stream] [^pg-supervisor].
<!-- truth: end -->

## Caller Guidance

- Install `vertebra-core[postgres]` for raw `SyncPostgresClient`, `AsyncPostgresClient`, and native pool use.
- Install `psycopg[binary,pool]` for the public PostgreSQL drivers until the native cutover completes.
- Pass an `executor=` to `PostgreSQLQueueBackend`; installing the native wheel does not provide a queue executor.
- Treat the native wheel and public driver surfaces separately when selecting dependency and conformance gates.

## Remaining Integration

`ReplicationSession` owns a bounded socket pump and publication leases. Dropping a lease restores its record ahead of the FIFO; only publishing a Commit makes its exact end LSN acknowledgeable. Heartbeat writes continue under read backpressure. Explicit shutdown and cancellation fence outstanding leases; graceful shutdown uses one absolute deadline for final feedback, CopyDone and backend drain. Natural clean EOF permits already decoded records to drain and publish [^pg-stream].

`ReplicationSupervisor` and `connect_logical_verified` supervise multi-generation reconnect recovery from `acknowledged_lsn`, while cancellation-safe Python replication handles (`AsyncPostgresReplication`) remain in `ch02` and `PostgresChangeSource` remains in `ch03`. Feedback receipt alone never advances flush/apply positions. The standalone `ReplicationFeedback` accepts caller-supplied decoded Commit positions; the session supplies those boundaries from pgoutput, never server `wal_end` [^pg-feedback] [^pg-stream] [^pg-supervisor].

Chapter `ch04` builds permanent native drivers and connects compiler, pipeline, Arrow, and event-hook seams. Chapter `ch05` switches public consumers, replaces legacy driver test doubles, removes psycopg/asyncpg dialer helpers, changes the queue dependency guard, and removes these split-state caveats. Runtime caveats for the completed native driver belong to chapter `ch09`.

[^pg-driver-py]: `src/py/vertebra/db/adapters/postgres/driver.py::SyncPostgreSQLDriver` — Public PostgreSQL drivers over psycopg_pool and asyncio.to_thread.
[^pg-queue-py]: `src/py/vertebra/framework/queues/postgres.py::PostgreSQLQueueBackend` — Caller-supplied queue executor and psycopg dependency guard.
[^pg-dialer-cloudsql]: `src/py/vertebra/db/dialer/cloudsql.py::CloudSQLConfig` — Cloud SQL dialer configuration and legacy connection helpers.
[^pg-dialer-alloydb]: `src/py/vertebra/db/dialer/alloydb.py::AlloyDBConfig` — AlloyDB dialer configuration and legacy connection helpers.
[^pg-driver-tests]: `src/py/tests/unit/db/test_postgres_driver.py::test_asyncpg_pool_transaction_and_affected_rows` — SQLite-backed asyncpg and psycopg pool test doubles.
[^pg-pipeline-py]: `src/py/vertebra/db/adapters/postgres/pipeline.py::SyncPostgresPipeline` — Native PostgreSQL pipeline facade awaiting driver integration.
[^pg-arrow-py]: `src/py/vertebra/db/adapters/postgres/arrow.py::load_arrow_sync` — Native Arrow COPY helper awaiting driver integration.
[^pg-wheel]: `src/rs/vertebra-postgres/src/lib.rs` — Standalone native PostgreSQL extension registration.

[^pg-feedback]: `src/rs/vertebra-postgres/src/replication/feedback.rs::ReplicationFeedback` — Native codecs, intervals, and bounded acknowledgement state.

[^pg-stream]: `src/rs/vertebra-postgres/src/replication/stream.rs::ReplicationSession` — Bounded native session transport and explicit delivery publication.
[^pg-supervisor]: `src/rs/vertebra-postgres/src/replication/supervisor.rs::ReplicationSupervisor` — Multi-generation verified reconnect supervisor and delivery leases.
