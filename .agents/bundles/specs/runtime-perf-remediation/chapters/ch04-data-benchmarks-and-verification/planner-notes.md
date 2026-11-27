# Planner Notes: ch04-data-benchmarks-and-verification

## Files Written

- `chapters/ch04-data-benchmarks-and-verification/spec.md`
- `chapters/ch04-data-benchmarks-and-verification/tasks/t01-pluggable-native-stdlib-db-drivers.md`
- `chapters/ch04-data-benchmarks-and-verification/tasks/t02-sqlspec-and-advanced-alchemy-db-drivers.md`
- `chapters/ch04-data-benchmarks-and-verification/tasks/t03-advanced-alchemy-standalone-comparator.md`
- `chapters/ch04-data-benchmarks-and-verification/tasks/t04-http-data-targets-and-offline-tests.md`
- `chapters/ch04-data-benchmarks-and-verification/tasks/t05-empirical-release-benchmark-gate.md`
- `chapters/ch04-data-benchmarks-and-verification/tasks/t06-okf-sync-runtime-ipc-http.md`
- `chapters/ch04-data-benchmarks-and-verification/tasks/t07-okf-sync-worker-python-benchmarks.md`
- `chapters/ch04-data-benchmarks-and-verification/planner-notes.md`

## Task Summaries

- `t01` (`tools/benchmarks/apps/_data_backend.py`, `tools/benchmarks/apps/_db_drivers.py`): Extend `DataBackendConfig` with `db_adapter` and `db_adapter_flavor`, introduce the `WorldDriver` protocol, decouple async/native Postgres I/O from `_db_lock`, and implement `native` and `stdlib` SQLite/PostgreSQL drivers with idempotent 10,000-row seeding.
- `t02` (`tools/benchmarks/apps/_db_orm_drivers.py`): Implement `sqlspec` (Vertebra adapters and upstream `sqlspec`) and `advanced_alchemy` (`+vtb`/`+vtb_async` with `BrokerPool`, plus vanilla SQLAlchemy) `WorldDriver` backends for SQLite and PostgreSQL.
- `t03` (`tools/benchmarks/adapters/sql.py`): Add `capture_advanced_alchemy_report`, register four `COMPARATOR_PAIRS` entries for SQLite and PostgreSQL sync/async, and wire `--engine advanced_alchemy --aa-engine sqlite|postgres` CLI dispatch.
- `t04` (`tools/benchmarks/targets.py`, `src/py/tests/unit/tools/test_e2e_benchmarks.py`): Guard `apply_shm_broker_dsn` for Postgres-only URLs, register ten SQLSpec/Advanced Alchemy/stdlib HTTP data targets on ports `8160`–`8169` with `Target.adapter` metadata, and add offline unit tests.
- `t05` (verification only): Run `make build`, `make lint-all`, `make rs-test`, live HTTP/1.1 Granian comparison plus Embedded 1w `VERTEBRA_TOKIO_THREADS=1` vs `2` A/B check, SQLite full-stack/standalone suites, and Docker `postgres:17-alpine` full-stack/standalone suites.
- `t06` (3 OKF chapters): Sync `topologies-and-embedding.md`, `tiered-rings-and-batch-bridge.md`, and `batching-dispatch-and-backpressure.md` in present-tense OKF v0.2 format.
- `t07` (3 OKF chapters): Sync `asgi-and-wsgi-runners.md`, `hot-path-and-async-dispatch.md`, and `benchmarks-and-free-threaded.md` in present-tense OKF v0.2 format.

## Root Spec Issues

- `DataBackendConfig` in root `spec.md` Section 2.2 shows `db_adapter: str = "native"` without a separate flavor selector; `ch04` adds an additive default field `db_adapter_flavor: str = "vertebra"` (`VTB_BENCH_DB_ADAPTER_FLAVOR=vertebra|upstream`) so baseline Granian/Uvicorn servers can select upstream `sqlspec` and vanilla `advanced_alchemy` engines while keeping `db_adapter` within the four root-specified values.
- `tools/benchmarks/targets.py::apply_shm_broker_dsn` currently appends `shm_broker=true` to any non-empty `VTB_BENCH_DB_DSN`; `t04` guards it to PostgreSQL URLs only so SQLite file paths are not corrupted when SHM broker targets run against SQLite.

## Open Questions

- Empirical trigger (resolved in `t05`): Embedded 1w `VERTEBRA_TOKIO_THREADS=1` versus `2` throughput and P99 latency after `ch02` and `ch03` land.
- Empirical trigger (resolved in `t05`): `WsgiExecutor` stall threshold (`250 µs` default) under live `hello` and `io_10ms` load.
- Empirical trigger (resolved in `t05`): Whether live SQLSpec, Advanced Alchemy, or PostgreSQL SHM broker HTTP runs surface any adapter-side serialization or pooling bottleneck requiring a follow-up revision before OKF sync.
