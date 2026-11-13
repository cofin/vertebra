# Chapters: Complete Standalone Native PostgreSQL Driver (`native-postgres-driver`)

| Chapter | State | Tasks | Summary & Fast Chapter Gate |
| --- | --- | --- | --- |
| [`ch01-replication-supervision`](ch01-replication-supervision/spec.md) | `planned` | `2` | Generation-fenced `ReplicationSupervisor`, bounded control/event channels, monotonic LSN acks, and identity-verified `connect_logical_verified` resume. |
| [`ch02-replication-python-bridge`](ch02-replication-python-bridge/spec.md) | `planned` | `4` | Cancel-safe `spawn_rust_db_task_with_attr_and_future`, frozen `AsyncPostgresReplication`, 14-class `_postgres.pyi`, and PyO3 FFI OKF sync. |
| [`ch03-cdc-options-and-bind-compiler`](ch03-cdc-options-and-bind-compiler/spec.md) | `planned` | `4` | `PostgresChangeSource` CAS checkpoint-before-ack coordinator, `EventSessionRegistry`, libpq/cloud `options.py`, and single-pass lexer `binds.py`. |
| [`ch04-permanent-native-postgres-drivers`](ch04-permanent-native-postgres-drivers/spec.md) | `planned` | `3` | Permanent `SyncPostgreSQLDriver` (`_sync.py`) and `AsyncPostgreSQLDriver` (`_async.py`) wiring native pools, pipelines, `VTBARROW`, and event hooks. |
| [`ch05-atomic-cutover-and-legacy-removal`](ch05-atomic-cutover-and-legacy-removal/spec.md) | `planned` | `4` | Atomic SQLSpec (`NUMERIC`), SQLAlchemy (`numeric_dollar`), 12-line `driver.py` barrel, `PostgreSQLQueueBackend` guard, and `cloudsql.py`/`alloydb.py` legacy removal + TLS/async fixes. |
| [`ch06-live-conformance-and-service-acceptance`](ch06-live-conformance-and-service-acceptance/spec.md) | `planned` | `3` | PostgreSQL 14–18 conformance matrix with zero-skip hook plus live `pgoutput` replication and `PGCOPY`/`VTBARROW`/`LISTEN`/`pgvector`/`PgBouncer` suites. |
| [`ch07-wheel-ci-and-benchmarks`](ch07-wheel-ci-and-benchmarks/spec.md) | `planned` | `4` | `verify_postgres_wheel.py`, CI/release smoke lanes, free-threaded 8-thread pool proofs, `postgres-bench` group, Criterion benches, and testing OKF sync. |
| [`ch08-user-and-reference-docs`](ch08-user-and-reference-docs/spec.md) | `planned` | `4` | Zensical user guides (`databases.md`, `analytics-and-cdc.md`, `cloud-dialers.md`, `extensions.md`), `native-stubs.md`, `README.md`, and `AGENTS.md`. |
| [`ch09-data-packaging-okf-and-routing-sync`](ch09-data-packaging-okf-and-routing-sync/spec.md) | `planned` | `4` | Final `database-drivers-and-dialers.md` (`<= 200L`), `postgres-native-driver.md`, `postgres-limitations.md`, `tech-stack.md`, `adr-004`, and `areas.toml` sync. |
