---
type: Spec
flow_id: native-postgres-driver
title: "Complete Standalone Native PostgreSQL Driver"
description: "Complete verified replication reconnect supervision, Python replication handles, durable PostgresChangeSource, permanent native Python drivers, atomic psycopg/asyncpg removal and GCP dialer cleanup, PG14-18 conformance, benchmarks, and docs over the merged vertebra-postgres wheel."
status: stable
state: active
planning_quality: standard
planning_degraded_reason: null
has_chapters: true
chapters:
  - ch01-replication-supervision
  - ch02-replication-python-bridge
  - ch03-cdc-options-and-bind-compiler
  - ch04-permanent-native-postgres-drivers
  - ch05-atomic-cutover-and-legacy-removal
  - ch06-live-conformance-and-service-acceptance
  - ch07-wheel-ci-and-benchmarks
  - ch08-user-and-reference-docs
  - ch09-data-packaging-okf-and-routing-sync
current_chapter: ch09-data-packaging-okf-and-routing-sync
current_task: "native-postgres-driver:ch09:t01"
parent_prd: native-data-platform
research:
  - research/protocol-and-execution_20261105.md
  - research/cloud-material_20261105.md
plan_revision: 9
plan_commit: 18baadcd
state_revision: 30
last_verified_checkpoint: 0eabc0a2
last_reviewed_chapter: ch08-user-and-reference-docs
last_reviewed_commit: 0eabc0a2
created_at: "2026-11-03T04:05:00Z"
updated_at: "2026-11-14T01:12:30Z"
generated:
  by: flow-plan
  at: "2026-11-13T00:25:00Z"
verified:
  - by: agent:code-reviewer
    at: "2026-11-14T01:12:01Z"
    commit: 0eabc0a2
  - by: agent:quality-reviewer
    at: "2026-11-14T01:06:30Z"
    commit: 0eabc0a2
tags:
  - rust
  - postgres
  - driver
  - wheel
  - pipeline
  - replication
  - cutover
sources:
  - id: pg-native-okf
    resource: .agents/bundles/knowledge/domains/data/postgres-native-driver.md
    title: "Merged standalone vertebra-postgres wheel and bounded ReplicationSession baseline"
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: "Canonical interface, codec, replication, and cutover contracts"
  - id: pg-stream-rs
    resource: "src/rs/vertebra-postgres/src/replication/stream.rs::ReplicationSession"
    title: "Bounded single-generation CopyBoth pump and cancel-safe delivery leases"
  - id: py-async-rs
    resource: "src/rs/vertebra-db/src/py_async.rs::spawn_rust_db_task_with_attr"
    title: "Per-extension AsyncCompletionQueue drainer in vertebra-db"
  - id: pg-driver-py
    resource: "src/py/vertebra/db/adapters/postgres/driver.py::SyncPostgreSQLDriver"
    title: "Legacy psycopg_pool PostgreSQL driver targeted for atomic cutover"
  - id: pg-queue-py
    resource: "src/py/vertebra/framework/queues/postgres.py::PostgreSQLQueueBackend"
    title: "PostgreSQL queue backend missing-executor dependency guard"
  - id: dialer-cloudsql
    resource: "src/py/vertebra/db/dialer/cloudsql.py::CloudSQLConfig"
    title: "Python Cloud SQL dialer configuration and TLS/ephemeral cert helpers"
  - id: dialer-alloydb
    resource: "src/py/vertebra/db/dialer/alloydb.py::AlloyDBConfig"
    title: "Python AlloyDB dialer configuration and client cert helpers"
  - id: sqlspec-py
    resource: "src/py/vertebra/extensions/sqlspec/adapters.py::BaseSyncDriver"
    title: "SQLSpec statement compilation and native pipeline dispatch"
  - id: sqlalchemy-py
    resource: src/py/vertebra/extensions/sqlalchemy/dialects.py
    title: "SQLAlchemy +vtb/+vertebra PostgreSQL dialect parameter style"
---

# Specification: Complete Standalone Native PostgreSQL Driver

<!-- truth: start -->
- **Merged Baseline (`c8722c76` + `18baadcd`)**: `src/rs/vertebra-postgres` (`vertebra._postgres`, `vertebra-core[postgres]`) ships on `main` with Protocol `3.0` framing, SCRAM-SHA-256-PLUS, Cloud SQL and AlloyDB mTLS + IAM discovery, lossless binary/text codecs, LIFO physical pools, single-`Sync` pipelines, `CancelRequest`, binary `COPY`, `VTBARROW` `ArrowCursor`s, `AsyncPostgresListener`, incremental `PgOutputDecoder` transaction envelopes, commit-fenced `ReplicationFeedback`, and single-generation `ReplicationSession` pumps (documented in [`postgres-native-driver.md`](../../knowledge/domains/data/postgres-native-driver.md)) [^pg-native-okf] [^pg-stream-rs].
- **Remaining Split Runtime State**: Public `SyncPostgreSQLDriver` and `AsyncPostgreSQLDriver` still execute via `psycopg_pool` and `asyncio.to_thread`, `PostgreSQLQueueBackend` probes `psycopg` when `executor=` is omitted, `pipeline.py` and `arrow.py` lack production driver wiring, and `cloudsql.py` / `alloydb.py` retain legacy `psycopg`/`asyncpg`/`asyncmy` helpers plus `CERT_NONE`/fake-RSA/swallowed-exception defects [^pg-driver-py] [^pg-queue-py] [^dialer-cloudsql] [^dialer-alloydb].
- **Consolidated Delivery (`ch01`–`ch09`, `32` `compact` Tasks)**: Chapters `ch01`–`ch02` finish verified `ReplicationSupervisor` reconnect and cancel-safe Python `AsyncPostgresReplication`; `ch03`–`ch05` build `PostgresChangeSource`, `options.py`, `binds.py`, permanent `_sync.py`/`_async.py` drivers, and execute one atomic cutover across SQLSpec (`ParameterStyle.NUMERIC`), SQLAlchemy (`numeric_dollar`), queues, and GCP dialers; `ch06`–`ch09` land PG14–18 conformance, wheel/benchmark gates, Zensical docs, and `<= 200L` OKF sync [^pg-contracts] [^sqlspec-py] [^sqlalchemy-py].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Merged Baseline & Problem Statement
Squash commits `c8722c76` ([PR #104](https://github.com/cofin/vertebra/pull/104)) and `18baadcd` ([PR #118](https://github.com/cofin/vertebra/pull/118)) delivered the standalone `vertebra-postgres` crate through single-generation `ReplicationSession`s (recorded in [`postgres-native-driver.md`](../../knowledge/domains/data/postgres-native-driver.md)) [^pg-native-okf]. Detailed interface and wire contracts live in [`contracts.md`](contracts.md) [^pg-contracts], with protocol and cloud research in [`research/protocol-and-execution_20261105.md`](research/protocol-and-execution_20261105.md) and [`research/cloud-material_20261105.md`](research/cloud-material_20261105.md).

Nine cohesive chapters (`32` tasks) remain to finish logical replication supervision, Python handles, durable CDC change streams, permanent native Python drivers, atomic removal of `psycopg`/`asyncpg` and GCP Python dialer defects, live PostgreSQL `14`–`18` conformance, wheel/benchmark verification, and user/OKF documentation.

### 1.2 Non-Goals
- Retaining `psycopg`, `psycopg_pool`, `asyncpg`, or `asyncmy` runtime fallbacks, `_Legacy*` aliases, or staging flags after `ch05`.
- Implicitly creating or dropping replication slots/publications or advancing `confirmed_flush_lsn` ahead of durable checkpoint persistence.
- Engine-specific PostgreSQL imports or SQL literals in generic `src/py/vertebra/db/*.py` or `src/py/vertebra/db/driver/*.py`.
- Native `SyncPostgresQueueStore`/`AsyncPostgresQueueStore` SQL executors (owned by [`queues-tasks-and-channels`](../queues-tasks-and-channels/spec.md)) or `SyncNotificationSource`/`AsyncNotificationSource` protocol split (owned by [`native-redis-driver`](../native-redis-driver/spec.md) `ch05`).

## 2. Requirements, Concrete Contracts & Architecture

- **`R1` Verified Replication Supervision & Cancel-Safe Python Handle (`ch01`–`ch02`)**: `ReplicationSupervisor` fences stale leases across generations, verifies server `system_identifier`, database OID, timeline, slot, and publication fingerprint via `connect_logical_verified` (`confirmed_flush_lsn` floor, bounded jittered `ReconnectPolicy` on transient `SQLSTATE 55006` / transport loss), and exposes frozen `AsyncPostgresReplication` in `vertebra._postgres` (`14` `.pyi` classes) over `spawn_rust_db_task_with_attr_and_future(...) -> PyResult<RustDbTaskAwaitable>` [^pg-stream-rs] [^py-async-rs] [^pg-contracts].
- **`R2` Durable `PostgresChangeSource`, DSN/Cloud Options & Lexer Bind Compiler (`ch03`)**: `PostgresChangeSource` (`change_stream.py`) coordinates single-flight shielded CAS checkpointing (`PostgresCheckpointStore`, `MemoryPostgresCheckpointStore`) strictly before wire `acknowledge(lsn)`, `EventSessionRegistry` tracks dedicated listeners/streams for clean driver close, `options.py` normalizes DSN/libpq/percent-decoded Unix socket/multi-host/pooler/cloud material options, and `binds.py` compiles `%s`/`%(name)s`/`$N` in one lexer pass preserving quotes, `E'...'`, `$tag$`, comments, `::`, and `?`/`?|`/`?&`.
- **`R3` Permanent Native Drivers & Atomic Consumer/Dialer Cutover (`ch04`–`ch05`)**: `_sync.py::SyncPostgreSQLDriver` and `_async.py::AsyncPostgreSQLDriver` integrate pools, `SyncCodecPlan`, shielded transactions/savepoints, `copy_from`/`copy_to`, `pipeline()`, `ArrowCursor` (`select_to_arrow`, `stream_arrow_batches`, `load_from_arrow`), and dedicated event hooks. `ch05` switches `driver.py` to a 12-line barrel, updates SQLSpec (`ParameterStyle.NUMERIC`) and SQLAlchemy (`numeric_dollar`) directly without staging switches, updates `embeddings.py` (`$1`/`$2`) and `PostgreSQLQueueBackend` (`vertebra._postgres` guard), and deletes `create_psycopg_connect_func`/`create_asyncpg_connect_func`/`create_asyncmy_creator` from `cloudsql.py` and `alloydb.py` while fixing async cert fetches, fake RSA fallback, `CERT_NONE`, and swallowed token exceptions [^pg-driver-py] [^pg-queue-py] [^dialer-cloudsql] [^dialer-alloydb].
- **`R4` Live PG14–18 Conformance, Wheel Isolation & Benchmarks (`ch06`–`ch07`)**: Parameterized PG `14`–`18` conformance with `VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS=1`, live `wal_level=logical` replication and `pgvector`/`PgBouncer` suites, two-probe `verify_postgres_wheel.py`, free-threaded 8-thread pool tests, `[dependency-groups].postgres-bench`, and Criterion `benches/postgres_native.rs` (keeping chapter gates `< 60s` offline and running Docker/wheel/bench suites in `/flow:finish` and CI).
- **`R5` Zensical Documentation & `<= 200L` OKF Sync (`ch08`–`ch09`)**: Synchronize user/reference docs (`databases.md`, `analytics-and-cdc.md`, `database-capabilities.md`, `native-stubs.md`, `cloud-dialers.md`, `extensions.md`, `README.md`, `AGENTS.md`) and OKF chapters (`database-drivers-and-dialers.md`, `postgres-native-driver.md`, `postgres-limitations.md`, `database-driver-limitations.md`, `query-pipeline-cdc-and-tenancy.md`, `tech-stack.md`, `product.md`, `adr-004`, and `areas.toml`).

## 3. Documentation & OKF Knowledge Plan

- **Code-Adjacent OKF Sync**: `ch02:t04` updates PyO3 FFI OKF chapters; `ch05:t04` updates `sqlspec-alchemy-and-dialects.md`, `distributed-queues-and-email.md`, and removes split-state caveats in `database-driver-limitations.md`; `ch07:t04` trims and updates `polyglot-test-matrix.md` (`<= 200L`), `benchmarks-and-free-threaded.md`, and `workflow.md`.
- **Final Docs & Data OKF Sync (`ch08`–`ch09`)**: `ch08` updates all Zensical guides/references (`make docs` clean); `ch09` trims `database-drivers-and-dialers.md` (`260L -> <= 200L`), finalizes `postgres-native-driver.md` and `postgres-limitations.md` (`<= 200L`), and syncs `tech-stack.md`, `product.md`, `packaging-and-pgo.md`, `adr-004`, and `areas.toml`.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Chapters & Tasks | Primary Files | Verification |
| --- | --- | --- | --- |
| `R1` Verified reconnect & Python replication handle | `ch01` (`t01`–`t02`), `ch02` (`t01`–`t04`) | `src/rs/vertebra-postgres/src/**`, `src/rs/vertebra-db/src/py_async.rs`, `src/py/vertebra/_postgres.pyi` | `cargo test -p vertebra-postgres`, `stubtest`, `test_postgres_native_replication.py` |
| `R2` `PostgresChangeSource`, `options.py` & `binds.py` | `ch03` (`t01`–`t04`) | `src/py/vertebra/db/adapters/postgres/events.py`, `change_stream.py`, `options.py`, `binds.py` | `test_postgres_change_stream.py`, `test_postgres_options_and_binds.py`, `mypy` |
| `R3` Permanent drivers & atomic cutover + dialer fixes | `ch04` (`t01`–`t03`), `ch05` (`t01`–`t04`) | `src/py/vertebra/db/adapters/postgres/_sync.py`, `_async.py`, `driver.py`, `extensions/**`, `db/dialer/**`, `framework/queues/postgres.py` | Unit adapter/compiler/dialer/queue suites + `git grep` absence of `psycopg\|asyncpg` |
| `R4` PG14–18 conformance, wheel check & benchmarks | `ch06` (`t01`–`t03`), `ch07` (`t01`–`t04`) | `src/py/tests/integration/**`, `tools/scripts/verify_postgres_wheel.py`, `tools/benchmarks/postgres.py`, `benches/postgres_native.rs`, `Makefile`, `ci.yml` | `< 60s` offline unit/stub gates + `/flow:finish` Docker/wheel/bench lanes |
| `R5` User docs, OKF `<= 200L` & review routing | `ch08` (`t01`–`t04`), `ch09` (`t01`–`t04`) | `docs/**`, `README.md`, `AGENTS.md`, `.agents/bundles/knowledge/**`, `.agents/bundles/product/**`, `.agents/skills/review-core/references/areas.toml` | `make docs`, `make review-check`, `make review-route BASE=18baadcd` |

## 5. Implementation Plan

- [x] [9ec32183] [ch01-replication-supervision](chapters/ch01-replication-supervision/spec.md) — Generation-fenced `ReplicationSupervisor`, bounded control/event channels, monotonic LSN acks, and identity-verified `connect_logical_verified` resume.
- [x] [b67aa83a] [ch02-replication-python-bridge](chapters/ch02-replication-python-bridge/spec.md) — Cancel-safe `spawn_rust_db_task_with_attr_and_future`, frozen `AsyncPostgresReplication`, 14-class `_postgres.pyi`, and PyO3 FFI OKF sync.
- [x] [65577e7a] [ch03-cdc-options-and-bind-compiler](chapters/ch03-cdc-options-and-bind-compiler/spec.md) — `PostgresChangeSource` CAS checkpoint-before-ack coordinator, `EventSessionRegistry`, libpq/cloud `options.py`, and single-pass lexer `binds.py`.
- [x] [76ad8f3a] [ch04-permanent-native-postgres-drivers](chapters/ch04-permanent-native-postgres-drivers/spec.md) — Permanent `SyncPostgreSQLDriver` (`_sync.py`) and `AsyncPostgreSQLDriver` (`_async.py`) wiring native pools, pipelines, `VTBARROW`, and event hooks.
- [x] [780217cf] [ch05-atomic-cutover-and-legacy-removal](chapters/ch05-atomic-cutover-and-legacy-removal/spec.md) — Atomic SQLSpec (`NUMERIC`), SQLAlchemy (`numeric_dollar`), 12-line `driver.py` barrel, `PostgreSQLQueueBackend` guard, and `cloudsql.py`/`alloydb.py` legacy removal + TLS/async fixes.
- [x] [7a7b5180] [ch06-live-conformance-and-service-acceptance](chapters/ch06-live-conformance-and-service-acceptance/spec.md) — PostgreSQL 14–18 conformance matrix with zero-skip hook plus live `pgoutput` replication and `PGCOPY`/`VTBARROW`/`LISTEN`/`pgvector`/`PgBouncer` suites.
- [x] [af023efd] [ch07-wheel-ci-and-benchmarks](chapters/ch07-wheel-ci-and-benchmarks/spec.md) — `verify_postgres_wheel.py`, CI/release smoke lanes, free-threaded 8-thread pool proofs, `postgres-bench` group, Criterion benches, and testing OKF sync.
- [x] [0eabc0a2] [ch08-user-and-reference-docs](chapters/ch08-user-and-reference-docs/spec.md) — Zensical user guides (`databases.md`, `analytics-and-cdc.md`, `cloud-dialers.md`, `extensions.md`), `native-stubs.md`, `README.md`, and `AGENTS.md`.
- [~] [ch09-data-packaging-okf-and-routing-sync](chapters/ch09-data-packaging-okf-and-routing-sync/spec.md) — Final `database-drivers-and-dialers.md` (`<= 200L`), `postgres-native-driver.md`, `postgres-limitations.md`, `tech-stack.md`, `adr-004`, and `areas.toml` sync.

## 6. Current State & Remaining Plan Snapshot

- **Active Frontier**: Chapters `ch01`–`ch08` (`28/32` tasks) are completed and reviewed; `ch09-data-packaging-okf-and-routing-sync` is the active chapter (`4` open `compact` tasks remain).
- **Downstream Unlocks**: Completing `ch05` unblocks [`broker-pool-sharing`](../broker-pool-sharing/spec.md), [`driver-conformance-ci`](../driver-conformance-ci/spec.md), [`native-redis-driver`](../native-redis-driver/spec.md), [`native-adapter-organization`](../native-adapter-organization/spec.md), [`sqlspec-adapter-parity`](../sqlspec-adapter-parity/spec.md), and [`queues-tasks-and-channels`](../queues-tasks-and-channels/spec.md).
- **`/flow:finish` Full Gate**: `make lint-all && make rs-test && make py-test && make postgres-wheel-check && make postgres-test && make postgres-bench-check && make docs-check && make review-check`.

[^pg-native-okf]: `.agents/bundles/knowledge/domains/data/postgres-native-driver.md` — Merged standalone vertebra-postgres wheel and bounded ReplicationSession baseline.
[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Canonical interface, codec, replication, and cutover contracts.
[^pg-stream-rs]: `src/rs/vertebra-postgres/src/replication/stream.rs::ReplicationSession` — Bounded single-generation CopyBoth pump and cancel-safe delivery leases.
[^py-async-rs]: `src/rs/vertebra-db/src/py_async.rs::spawn_rust_db_task_with_attr` — Per-extension AsyncCompletionQueue drainer in vertebra-db.
[^pg-driver-py]: `src/py/vertebra/db/adapters/postgres/driver.py::SyncPostgreSQLDriver` — Legacy psycopg_pool PostgreSQL driver targeted for atomic cutover.
[^pg-queue-py]: `src/py/vertebra/framework/queues/postgres.py::PostgreSQLQueueBackend` — PostgreSQL queue backend missing-executor dependency guard.
[^dialer-cloudsql]: `src/py/vertebra/db/dialer/cloudsql.py::CloudSQLConfig` — Python Cloud SQL dialer configuration and TLS/ephemeral cert helpers.
[^dialer-alloydb]: `src/py/vertebra/db/dialer/alloydb.py::AlloyDBConfig` — Python AlloyDB dialer configuration and client cert helpers.
[^sqlspec-py]: `src/py/vertebra/extensions/sqlspec/adapters.py::BaseSyncDriver` — SQLSpec statement compilation and native pipeline dispatch.
[^sqlalchemy-py]: `src/py/vertebra/extensions/sqlalchemy/dialects.py` — SQLAlchemy +vtb/+vertebra PostgreSQL dialect parameter style.
