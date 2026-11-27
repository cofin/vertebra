# Planner Notes: ch04-data-benchmarks-and-verification

## Files Written

- `chapters/ch04-data-benchmarks-and-verification/spec.md`
- `chapters/ch04-data-benchmarks-and-verification/tasks/t01-empirical-tokio-and-wsgi-tuning.md`
- `chapters/ch04-data-benchmarks-and-verification/tasks/t02-pluggable-native-stdlib-db-drivers.md`
- `chapters/ch04-data-benchmarks-and-verification/tasks/t03-sqlspec-and-advanced-alchemy-db-drivers.md`
- `chapters/ch04-data-benchmarks-and-verification/tasks/t04-advanced-alchemy-standalone-comparator.md`
- `chapters/ch04-data-benchmarks-and-verification/tasks/t05-http-data-targets-and-offline-tests.md`
- `chapters/ch04-data-benchmarks-and-verification/tasks/t06-empirical-release-benchmark-gate.md`
- `chapters/ch04-data-benchmarks-and-verification/tasks/t07-okf-sync-runtime-ipc-http.md`
- `chapters/ch04-data-benchmarks-and-verification/tasks/t08-okf-sync-worker-python-benchmarks.md`
- `chapters/ch04-data-benchmarks-and-verification/planner-notes.md`

## Task Summaries

- `t01` (`src/rs/vertebra-http/src/config.rs`, `src/rs/vertebra-py/src/wsgi/pool.rs`): Apply empirically verified 1w GIL Embedded Tokio default `available_cpus.clamp(1, 2)`, raise `WsgiExecutor` `DEFAULT_STALL_THRESHOLD_US` to `2_000`, raise lazy `default_threads` to `cpus * 12` clamped `64..128`, and cut inline `now_ns()` calls from 2 to 1 per item.
- `t02` (`tools/benchmarks/apps/_data_backend.py`, `tools/benchmarks/apps/_db_drivers.py`): Extend `DataBackendConfig` with `db_adapter` and `db_adapter_flavor`, introduce `WorldDriver`, decouple async/native Postgres I/O from `_db_lock`, and implement `native` (`SyncSQLiteDriver`/`AsyncSQLiteDriver`, `SyncPostgreSQLDriver`/`AsyncPostgreSQLDriver` with `PostgreSQLConfig(min_size=1, max_size=pool_size)`) and `stdlib` drivers with idempotent 10,000-row seeding.
- `t03` (`tools/benchmarks/apps/_db_orm_drivers.py`): Implement `sqlspec` (Vertebra adapters and upstream `sqlspec`) and `advanced_alchemy` (`+vtb`/`+vtb_async` with `BrokerPool`/`AsyncBrokerPool`, plus vanilla SQLAlchemy) `WorldDriver` backends for SQLite and PostgreSQL.
- `t04` (`tools/benchmarks/adapters/sql.py`): Add `capture_advanced_alchemy_report`, register four `COMPARATOR_PAIRS` entries for SQLite and PostgreSQL sync/async, and wire `--engine advanced_alchemy --aa-engine sqlite|postgres` CLI dispatch.
- `t05` (`tools/benchmarks/targets.py`, `src/py/tests/unit/tools/test_e2e_benchmarks.py`): Guard `apply_shm_broker_dsn` for Postgres-only URLs, register ten SQLSpec/Advanced Alchemy/stdlib HTTP data targets on ports `8160`–`8169` with `Target.adapter` metadata, and add offline unit tests.
- `t06` (verification only): Run `make build`, `make lint-all`, `make rs-test`, live HTTP/1.1 Granian comparison, SQLite full-stack/standalone suites, and Docker `postgres:17-alpine` full-stack/standalone suites.
- `t07` (3 OKF chapters): Sync `topologies-and-embedding.md`, `tiered-rings-and-batch-bridge.md`, and `batching-dispatch-and-backpressure.md` in present-tense OKF v0.2 format.
- `t08` (3 OKF chapters): Sync `asgi-and-wsgi-runners.md`, `hot-path-and-async-dispatch.md`, and `benchmarks-and-free-threaded.md` in present-tense OKF v0.2 format.

## Root Spec Issues

- Resolved in `plan_revision: 3`: updated root `spec.md` R1 (`2,000 µs` stall threshold, `cpus * 12` clamped `64..128` lazy threads), R3 (`available_cpus.clamp(1, 2)` Tokio threads for 1w GIL Embedded), Postgres driver symbol names (`SyncPostgreSQLDriver`/`AsyncPostgreSQLDriver`), `AsyncBrokerPool`, and `ch04:t07`/`t08` OKF doc task mappings.

## Open Questions

None.
