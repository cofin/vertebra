---
type: Spec
parent_flow: runtime-perf-remediation
chapter_id: ch04-data-benchmarks-and-verification
title: Empirical Tuning, Data Benchmarks, Verification, and OKF Sync
description: Apply empirically validated Embedded Tokio and WSGI pool defaults, wire SQLite and PostgreSQL native, SQLSpec, and Advanced Alchemy benchmarks, run live verification gates, and sync OKF knowledge chapters.
status: stable
state: planned
planning_quality: standard
planning_degraded_reason: null
planning_status: complete
depends_on_chapters:
  - ch01-wsgi-adaptive-pool
  - ch02-embedded-bridge-and-batching
  - ch03-ingress-and-worker-hotpaths
replan_trigger: null
plan_revision: 3
plan_commit: "9dfbd408"
current_task: null
last_verified_checkpoint: null
last_reviewed_commit: null
generated:
  by: flow/0.26.0
  at: "2026-11-27T16:10:00Z"
verified:
  - by: user:cofin
    at: "2026-11-27T16:25:21Z"
    commit: "9dfbd408"
sources:
  - id: src-root-spec
    resource: .agents/bundles/specs/runtime-perf-remediation/spec.md
    title: Root flow specification and acceptance criteria
  - id: src-http-config
    resource: src/rs/vertebra-http/src/config.rs::ServerConfig::apply_runtime_thread_env
    title: Embedded Tokio thread default and RequestBatcher shard sizing
  - id: src-wsgi-pool
    resource: src/rs/vertebra-py/src/wsgi/pool.rs::WsgiExecutor
    title: Adaptive Rust WSGI executor stall threshold and lazy thread pool sizing
  - id: src-bench-backend
    resource: tools/benchmarks/apps/_data_backend.py::DataBackend
    title: Shared benchmark database, cache, and storage backend
  - id: src-bench-sql
    resource: tools/benchmarks/adapters/sql.py::capture_sqlspec_report
    title: Standalone SQL, SQLAlchemy, and SQLSpec adapter comparator suite
  - id: src-bench-targets
    resource: tools/benchmarks/targets.py::ALL_TARGETS
    title: HTTP, data, framework, and Granian benchmark target definitions
  - id: src-bench-fixtures
    resource: tools/benchmarks/fixtures.py::FixtureLauncher
    title: Tmpfs-backed local Docker benchmark fixtures including postgres:17-alpine
  - id: src-test-e2e-bench
    resource: src/py/tests/unit/tools/test_e2e_benchmarks.py
    title: Offline end-to-end tests for benchmark store, drivers, adapters, and CLI matrix
tags:
  - benchmarks
  - sqlite
  - postgres
  - sqlspec
  - advanced-alchemy
  - verification
  - okf
---

# Empirical Tuning, Data Benchmarks, Verification, and OKF Sync

<!-- truth: start -->
- Update 1-worker GIL Embedded default Tokio threads in `ServerConfig::apply_runtime_thread_env` to `available_cpus.clamp(1, 2)` and tune `WsgiExecutor` defaults (`DEFAULT_STALL_THRESHOLD_US = 2_000`, `default_threads = cpus * 12` clamped `64..128`, single `now_ns()` per inline item) based on release A/B probe results.
- Extend `DataBackendConfig` with `db_adapter` (`native|sqlspec|advanced_alchemy|stdlib`) and `db_adapter_flavor` (`vertebra|upstream`), keeping `db_backend="memory"` as default.
- Implement real SQLite and PostgreSQL drivers across native (`SyncSQLiteDriver`/`AsyncSQLiteDriver`, `SyncPostgreSQLDriver`/`AsyncPostgreSQLDriver`), SQLSpec, Advanced Alchemy (`+vtb`/`+vtb_async` with `BrokerPool`/`AsyncBrokerPool`), and stdlib/vanilla baselines with idempotent 10,000-row `world` seeding and non-serialized async execution.
- Add `capture_advanced_alchemy_report`, four `COMPARATOR_PAIRS` entries, and `--engine advanced_alchemy --aa-engine sqlite|postgres` CLI dispatch to `tools/benchmarks/adapters/sql.py`.
- Register ten SQLSpec, Advanced Alchemy, and stdlib HTTP data targets on ports `8160`–`8169` in `tools/benchmarks/targets.py` with explicit `Target.adapter` metadata and offline pytest coverage.
- Run release `make build`, `make lint-all`, `make rs-test`, live Granian vs Vertebra HTTP/1.1, SQLite, and Docker PostgreSQL benchmark matrices, then sync all six OKF knowledge chapters.
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Chapter Scope & Baseline

Chapter `ch04-data-benchmarks-and-verification` applies the empirically validated runtime defaults from post-`ch03` release probing, delivers Requirement R7 from the root specification, and closes the flow with live verification and OKF documentation sync [^src-root-spec].

Release A/B probing on commit `9dfbd408` resolved both runtime tuning triggers before finalizing this chapter:

| Target / Variant | `plaintext` | `json` | `echo` | `io_10ms` |
| :--- | ---: | ---: | ---: | ---: |
| `granian-asgi` (1w baseline) | 73.9k | 70.8k | 34.6k | — |
| `granian-rsgi` (1w baseline) | 93.5k | 94.1k | 58.1k | — |
| `vtb-embedded-asgi` (`tokio=1` vs `tokio=2` vs `tokio=4`) | 107.7k / **144.2k** / 105.2k | 101.6k / **143.6k** / 94.2k | 81.9k / **114.7k** / 64.7k | — |
| `vtb-embedded-rsgi` (`tokio=1` vs `tokio=2`) | 104.1k / **177.6k** | 98.3k / **179.1k** | 91.5k / **140.8k** | — |
| `vtb-embedded-native` (`tokio=1` vs `tokio=2`) | 120.6k / **169.7k** | 97.0k / **170.6k** | 87.8k / **149.3k** | — |
| `vtb-embedded-wsgi` (`250µs/48t` vs `2ms/128t`) | 94.0k / **194.7k** | — | — | 4.7k / **12.2k** |
| `vtb-shm-wsgi-4w` (`250µs` vs `2ms`) | 102.4k / **235.9k** | — | — | 12.2k / **12.2k** |

Two targeted adjustments lock in those gains across all targets:

1. `ServerConfig::apply_runtime_thread_env` sets 1w GIL Embedded default `worker_threads = available_cpus.clamp(1, 2)` (`multi_thread(2)` on `>= 2`-core hosts), gaining +34%–+82% over `tokio=1`, beating Granian 1w by 1.9×–3.3×, and avoiding the ASGI `/echo` regression seen at `tokio=3/4` while keeping `EmbeddedSupervisor` child processes at `1` Tokio thread [^src-http-config].
2. `WsgiExecutor` raises `DEFAULT_STALL_THRESHOLD_US` from `250` to `2_000` (eliminating false-positive concurrent fallback on zero-I/O `/hello`), increases `default_threads` from `cpus * 4` clamped `32..128` to `cpus * 12` clamped `64..128` (eliminating the 48-thread ceiling on 1w `io_10ms`), and reuses `item_end` as the next `item_start` so `now_ns()` runs once per item instead of twice [^src-wsgi-pool].

On the benchmark harness side, `DataBackend` currently supports only in-memory rows and a mutex-guarded `sqlite3.Connection`, standalone `adapters/sql.py` omits an Advanced Alchemy comparator, and `targets.py` lacks dedicated SQLSpec, Advanced Alchemy, and stdlib HTTP data targets [^src-bench-backend] [^src-bench-sql] [^src-bench-targets].

Local Docker fixture support provides `postgres:17-alpine` on tmpfs via `FixtureLauncher` [^src-bench-fixtures].

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Functional Requirements

- **Empirical runtime defaults**: Apply `available_cpus.clamp(1, 2)` for 1w GIL Embedded Tokio threads in `ServerConfig::apply_runtime_thread_env`, and tune `WsgiExecutor` (`DEFAULT_STALL_THRESHOLD_US = 2_000`, `default_threads = cpus * 12` clamped `64..128`, single `now_ns()` per inline item) [^src-http-config] [^src-wsgi-pool].
- **Pluggable database adapters**: Extend `DataBackendConfig` with `db_adapter: str = "native"` (`VTB_BENCH_DB_ADAPTER`) and `db_adapter_flavor: str = "vertebra"` (`VTB_BENCH_DB_ADAPTER_FLAVOR`), keeping `db_backend="memory"` default [^src-bench-backend].
- **Real SQLite & PostgreSQL drivers**: Support `db_backend` in `("memory", "sqlite", "postgres")` across `native`, `sqlspec`, `advanced_alchemy`, and `stdlib` adapters without holding `self._db_lock` across async or pooled Postgres I/O.
- **Idempotent seeding**: Seed the 10,000-row `world` table idempotently on both SQLite and PostgreSQL without multi-worker startup races.
- **Standalone Advanced Alchemy comparator**: Add `capture_advanced_alchemy_report` and `--engine advanced_alchemy --aa-engine sqlite|postgres` in `tools/benchmarks/adapters/sql.py` with four registered `COMPARATOR_PAIRS` entries [^src-bench-sql].
- **HTTP data targets & offline tests**: Add ten SQLSpec, Advanced Alchemy, and stdlib data targets in `tools/benchmarks/targets.py` with explicit `Target.adapter` metadata and offline unit coverage in `test_e2e_benchmarks.py` [^src-bench-targets] [^src-test-e2e-bench].
- **Empirical verification & OKF sync**: Execute `make build`, `make lint-all`, `make rs-test`, and the three live benchmark suites, then update all six OKF chapters listed in root `spec.md` Section 3 [^src-root-spec].

### 2.2 Concrete Contracts

```rust
// src/rs/vertebra-http/src/config.rs::ServerConfig::apply_runtime_thread_env
// When VERTEBRA_TOKIO_THREADS is unset and topology == Topology::Embedded && !free_threaded:
self.worker_threads = available_cpus.clamp(1, 2);

// src/rs/vertebra-py/src/wsgi/pool.rs
const DEFAULT_STALL_THRESHOLD_US: u64 = 2_000;
let default_threads = cpus.saturating_mul(12).clamp(64, 128);
```

```python
# tools/benchmarks/apps/_data_backend.py
VALID_DB_BACKENDS: Final[frozenset[str]] = frozenset({"memory", "sqlite", "postgres"})
VALID_DB_ADAPTERS: Final[frozenset[str]] = frozenset({"native", "sqlspec", "advanced_alchemy", "stdlib"})
VALID_DB_ADAPTER_FLAVORS: Final[frozenset[str]] = frozenset({"vertebra", "upstream"})

class DataBackendConfig(msgspec.Struct, frozen=True, gc=False):
    db_backend: str = "memory"
    db_adapter: str = "native"
    db_adapter_flavor: str = "vertebra"
    db_dsn: str = ""
    cache_backend: str = "memory"
    cache_dsn: str = ""
    storage_backend: str = "memory"
    storage_endpoint: str = ""
    storage_bucket: str = "vertebra-bench"
    storage_root: str = ""
    workers: int = 1
    broker_mode: str = "none"
    total_pool_budget: int = 40
    strict_free_threading: bool = False
```

```python
# tools/benchmarks/apps/_db_drivers.py
class WorldDriver(Protocol):
    def fetch_world_sync(self, row_id: int) -> WorldRow: ...
    def fetch_worlds_sync(self, row_ids: list[int]) -> list[WorldRow]: ...
    def update_worlds_sync(self, updates: list[tuple[int, int]]) -> list[WorldRow]: ...
    async def fetch_world_async(self, row_id: int) -> WorldRow: ...
    async def fetch_worlds_async(self, row_ids: list[int]) -> list[WorldRow]: ...
    async def update_worlds_async(self, updates: list[tuple[int, int]]) -> list[WorldRow]: ...
    def close(self) -> None: ...
    async def aclose(self) -> None: ...

def build_world_driver(config: DataBackendConfig, *, pool_size: int) -> WorldDriver: ...
```

```python
# tools/benchmarks/apps/_db_orm_drivers.py
def build_orm_world_driver(config: DataBackendConfig, *, pool_size: int) -> WorldDriver: ...
```

```python
# tools/benchmarks/adapters/sql.py
type AdvancedAlchemyEngine = Literal["sqlite", "postgres"]

async def capture_advanced_alchemy_report(
    engine: AdvancedAlchemyEngine,
    dsn: str,
    *,
    iterations: int = 100,
    warmup: int = 10,
    include_external: bool = True,
    strict_free_threading: bool = False,
) -> AdapterBenchmarkReport: ...
```

### 2.3 Adapter Matrix & Target Wiring

| Adapter | Flavor | SQLite Implementation | PostgreSQL Implementation |
| :--- | :--- | :--- | :--- |
| `native` | `vertebra` | `SyncSQLiteDriver` / `AsyncSQLiteDriver` (`SQLiteConfig`) | `SyncPostgreSQLDriver` / `AsyncPostgreSQLDriver` (`PostgreSQLConfig(min_size=1, max_size=pool_size, broker=...)`) |
| `sqlspec` | `vertebra` | `SQLiteSyncConfig` / `SQLiteAsyncConfig` (`share_pool=(broker_mode=="shm_broker")`) | `PostgresSyncConfig` / `PostgresAsyncConfig` (`min_size=1`, `max_size=pool_size`, `share_pool=(broker_mode=="shm_broker")`) |
| `sqlspec` | `upstream` | `sqlspec.adapters.sqlite.SqliteConfig` / `sqlspec.adapters.aiosqlite.AiosqliteConfig` | `sqlspec.adapters.psycopg.PsycopgSyncConfig` / `sqlspec.adapters.asyncpg.AsyncpgConfig` |
| `advanced_alchemy` | `vertebra` | `sqlite+vtb` / `sqlite+vtb_async` repository services | `postgresql+vtb` / `postgresql+vtb_async` (`BrokerPool`/`AsyncBrokerPool` bound with `SyncSHMBrokerTransport`/`AsyncSHMBrokerTransport` in `shm_broker`) |
| `advanced_alchemy` | `upstream` | `sqlite` / `sqlite+aiosqlite` repository services | `postgresql+psycopg` / `postgresql+asyncpg` repository services |
| `stdlib` | `vertebra` / `upstream` | `sqlite3` / `aiosqlite` | `psycopg` (or `psycopg_pool`) / `asyncpg` |

Standalone `COMPARATOR_PAIRS` additions in `tools/benchmarks/adapters/sql.py`:

- `"vertebra-aa-sqlite-sync": ("vanilla-aa-sqlite-sync",)`
- `"vertebra-aa-sqlite-async": ("vanilla-aa-sqlite-async",)`
- `"vertebra-aa-postgres-sync": ("vanilla-aa-postgres-sync",)`
- `"vertebra-aa-postgres-async": ("vanilla-aa-postgres-async",)`

New HTTP data targets in `tools/benchmarks/targets.py` (ports `8160`–`8169`):

- `vtb-data-sqlspec-native-4w` (`adapter="sqlspec"`), `vtb-data-sqlspec-shm-4w-broker` (`adapter="sqlspec"`, `broker_mode="shm_broker"`).
- `vtb-data-aa-native-4w` (`adapter="advanced_alchemy"`), `vtb-data-aa-shm-4w-broker` (`adapter="advanced_alchemy"`, `broker_mode="shm_broker"`).
- `granian-asgi-data-stdlib` (`adapter="stdlib"`), `granian-asgi-data-sqlspec` (`adapter="sqlspec-upstream"`), `granian-asgi-data-aa` (`adapter="advanced_alchemy-vanilla"`).
- `uvicorn-httptools-data-stdlib` (`adapter="stdlib"`), `uvicorn-httptools-data-sqlspec` (`adapter="sqlspec-upstream"`), `uvicorn-httptools-data-aa` (`adapter="advanced_alchemy-vanilla"`).

### 2.4 Edge Cases & Failure Modes

- **Missing Postgres DSN**: Raise `ValueError` with setup guidance referencing `VTB_BENCH_DB_DSN` and `tools.benchmarks.fixtures` (`postgres:17-alpine`).
- **Optional external Postgres packages (`psycopg`, `asyncpg`)**: Both live in the `postgres-bench` dependency group rather than the base `.venv`. Import via `import_optional`; standalone comparators skip missing external drivers gracefully, while explicit `stdlib`/`upstream` Postgres HTTP driver selection raises `RuntimeError` instructing `uv sync --group postgres-bench`.
- **SQLite SHM broker DSN safety**: `apply_shm_broker_dsn` must only append `shm_broker=true` to PostgreSQL URLs, never raw SQLite file paths.
- **SQLite `:memory:` isolation**: Seed `world` idempotently per connection for `:memory:` targets and enable WAL + `busy_timeout=5000` for file-backed SQLite.
- **No mutex serialization on async/native Postgres**: Generate row IDs via thread-local `random.Random` and never hold `DataBackend._db_lock` across database I/O.
- **Lazy loop-bound async pool initialization**: Benchmark apps construct `DataBackend` at module import time before worker event loops start. Initialize async drivers and pools (`AsyncSQLiteDriver`, `AsyncPostgreSQLDriver`, `aiosqlite`, `asyncpg`, async SQLSpec/SQLAlchemy pools) lazily on the first `async def` call using per-loop synchronization while keeping table seeding synchronous.
- **SHM broker transport wrapping**: `DatabaseRegistry.default_broker()` returns a native `LeaseBroker`, whereas `BrokerPool`/`AsyncBrokerPool` expect `SyncSHMBrokerTransport`/`AsyncSHMBrokerTransport`, and `BrokerHostHook` sets `VERTEBRA_BROKER_SHM`. Attach via `BrokerConfig(shm_name=os.environ["VERTEBRA_BROKER_SHM"], role="worker")` when present (otherwise wrap `DatabaseRegistry.default_broker()`), and pass `broker=` / `share_pool=` explicitly according to `broker_mode == "shm_broker"`.
- **PostgreSQL Docker initdb readiness**: After `FixtureLauncher.start()`, poll for SQL readiness (`SELECT 1`) before launching benchmark servers because `postgres:17-alpine` restarts once after `initdb`.

## 3. Documentation & OKF Knowledge Plan

- `[0/6 files documented]`
  - `.agents/bundles/knowledge/architecture/runtime/topologies-and-embedding.md` (`t07`) — 1w GIL Embedded `available_cpus.clamp(1, 2)` Tokio thread default, consumer-aligned `RequestBatcher` sharding, cgroup-aware `EmbeddedSupervisor`.
  - `.agents/bundles/knowledge/architecture/ipc/tiered-rings-and-batch-bridge.md` (`t07`) — `BatchResponseSink`, `InProcessRequest` `PathAndQuery`, `submit_batch_flushed`.
  - `.agents/bundles/knowledge/architecture/http/batching-dispatch-and-backpressure.md` (`t07`) — Lazy `IdleTimeoutStream`, connection-pinned shards, batch-locked `BridgeCorrelation`, `BackpressureController` fastpath, reusable `ShmDispatcher` scratch buffers.
  - `.agents/bundles/knowledge/architecture/worker/asgi-and-wsgi-runners.md` (`t08`) — Adaptive `WsgiExecutor` (`2,000 µs` stall threshold, `cpus * 12` clamped `64..128` lazy threads), `WsgiServerConfig`, lazy `RsgiHttpProtocol`, `AsgiScope` bitmask drop.
  - `.agents/bundles/knowledge/patterns/python/hot-path-and-async-dispatch.md` (`t08`) — Split `_on_notify_readable`/`_drain_batches`, `native_only` mixed sync/async fast path, direct `Request::body`, 1-arg extractors.
  - `.agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md` (`t08`) — SQLite/PostgreSQL native, SQLSpec, and Advanced Alchemy full-stack and standalone comparators plus empirical results.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Tasks | Verification Strategy |
| :--- | :--- | :--- |
| R1, R3 (Empirical Tokio 2-thread & WSGI stall/pool tuning) | `t01` | `regression_tdd` |
| R7 (Pluggable SQLite & Postgres `native`/`stdlib` backends) | `t02` | `behavior_tdd` |
| R7 (Pluggable SQLite & Postgres `sqlspec`/`advanced_alchemy` backends) | `t03` | `behavior_tdd` |
| R7 (Standalone Advanced Alchemy comparator & CLI) | `t04` | `behavior_tdd` |
| R7 (HTTP data targets & offline unit tests) | `t05` | `regression_tdd` |
| R7 (Release build & live Granian/SQLite/PostgreSQL benchmark gate) | `t06` | `integration_acceptance` |
| R7 (OKF knowledge sync across 6 chapters) | `t07`, `t08` | `documentation_validation` |

## 5. Implementation Plan

- [ ] t01-empirical-tokio-and-wsgi-tuning — Apply empirically verified 1w Embedded Tokio `clamp(1, 2)` and `WsgiExecutor` `2,000 µs` / `64..128` thread defaults (tasks/t01-empirical-tokio-and-wsgi-tuning.md)
- [ ] t02-pluggable-native-stdlib-db-drivers — Extend `DataBackendConfig` and implement `native` and `stdlib` SQLite/PostgreSQL `WorldDriver` backends (tasks/t02-pluggable-native-stdlib-db-drivers.md)
- [ ] t03-sqlspec-and-advanced-alchemy-db-drivers — Implement `sqlspec` and `advanced_alchemy` SQLite/PostgreSQL `WorldDriver` adapters (tasks/t03-sqlspec-and-advanced-alchemy-db-drivers.md)
- [ ] t04-advanced-alchemy-standalone-comparator — Add `capture_advanced_alchemy_report`, `COMPARATOR_PAIRS`, and CLI dispatch to `tools/benchmarks/adapters/sql.py` (tasks/t04-advanced-alchemy-standalone-comparator.md)
- [ ] t05-http-data-targets-and-offline-tests — Register SQLSpec, Advanced Alchemy, and stdlib HTTP data targets and offline pytest coverage (tasks/t05-http-data-targets-and-offline-tests.md)
- [ ] t06-empirical-release-benchmark-gate — Build release binaries and run live Granian, SQLite, and PostgreSQL benchmark matrices (tasks/t06-empirical-release-benchmark-gate.md)
- [ ] t07-okf-sync-runtime-ipc-http — Update runtime, IPC, and HTTP OKF knowledge chapters (tasks/t07-okf-sync-runtime-ipc-http.md)
- [ ] t08-okf-sync-worker-python-benchmarks — Update worker, Python hot-path, and benchmark OKF knowledge chapters (tasks/t08-okf-sync-worker-python-benchmarks.md)

### Fast Chapter Gate (`< 60s`)

```bash
export PYO3_PYTHON="$(realpath .venv/bin/python)"
export LD_LIBRARY_PATH="$("$PYO3_PYTHON" -c 'import sysconfig; print(sysconfig.get_config_var("LIBDIR") or "")')"
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-http --lib config::tests
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-py wsgi::pool::tests -- --test-threads=8
UV_OFFLINE=1 make dev-ext EXT=core
uv run --no-sync pytest src/py/tests/unit/server/test_wsgi_adaptive_pool.py src/py/tests/unit/tools/test_data_apps_native.py src/py/tests/unit/tools/test_e2e_benchmarks.py src/py/tests/unit/tools/test_adapter_benchmarks_sql.py src/py/tests/unit/tools/test_matrix_targets_and_scenarios.py src/py/tests/unit/tools/test_cli_matrix_filters.py src/py/tests/unit/test_bench_infrastructure.py
uv run --no-sync ruff check tools/benchmarks/ src/py/tests/unit/tools/test_e2e_benchmarks.py src/py/tests/unit/tools/test_data_apps_native.py
uv run --no-sync mypy tools/benchmarks/apps/_data_backend.py tools/benchmarks/apps/_db_drivers.py tools/benchmarks/apps/_db_orm_drivers.py tools/benchmarks/adapters/sql.py tools/benchmarks/targets.py
```

## 6. Current State & Remaining Plan Snapshot

- **Chapter State**: `planned` (`planning_status: complete`, `plan_revision: 3`, approved and ready for execution).
- **Remaining Tasks**: `t01`, `t02`, `t03`, `t04`, `t05`, `t06`, `t07`, `t08`.
- **Empirical Findings Resolved**: `t01` locks in `available_cpus.clamp(1, 2)` for 1w GIL Embedded Tokio and `2,000 µs` / `64..128` lazy threads for `WsgiExecutor` based on live release A/B runs on `9dfbd408`.

[^src-root-spec]: `.agents/bundles/specs/runtime-perf-remediation/spec.md` — Root flow specification and acceptance criteria.
[^src-http-config]: `src/rs/vertebra-http/src/config.rs::ServerConfig::apply_runtime_thread_env` — Embedded Tokio thread default and RequestBatcher shard sizing.
[^src-wsgi-pool]: `src/rs/vertebra-py/src/wsgi/pool.rs::WsgiExecutor` — Adaptive Rust WSGI executor stall threshold and lazy thread pool sizing.
[^src-bench-backend]: `tools/benchmarks/apps/_data_backend.py::DataBackend` — Shared benchmark database, cache, and storage backend.
[^src-bench-sql]: `tools/benchmarks/adapters/sql.py::capture_sqlspec_report` — Standalone SQL, SQLAlchemy, and SQLSpec adapter comparator suite.
[^src-bench-targets]: `tools/benchmarks/targets.py::ALL_TARGETS` — HTTP, data, framework, and Granian benchmark target definitions.
[^src-bench-fixtures]: `tools/benchmarks/fixtures.py::FixtureLauncher` — Tmpfs-backed local Docker benchmark fixtures including postgres:17-alpine.
[^src-test-e2e-bench]: `src/py/tests/unit/tools/test_e2e_benchmarks.py` — Offline end-to-end tests for benchmark store, drivers, adapters, and CLI matrix.
