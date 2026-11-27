---
type: Spec
parent_flow: runtime-perf-remediation
chapter_id: ch04-data-benchmarks-and-verification
title: Data Benchmarks, Empirical Verification, and OKF Sync
description: Wire real SQLite and PostgreSQL backends across native, SQLSpec, and Advanced Alchemy adapters, add standalone comparators, execute live verification benchmarks, and sync OKF knowledge chapters.
status: stable
state: planned
planning_quality: standard
planning_degraded_reason: null
planning_status: empirical_dependent
depends_on_chapters:
  - ch01-wsgi-adaptive-pool
  - ch02-embedded-bridge-and-batching
  - ch03-ingress-and-worker-hotpaths
replan_trigger: "Re-evaluate Embedded 1w VERTEBRA_TOKIO_THREADS (1 vs 2), WsgiExecutor stall threshold, and any SQLSpec/Advanced Alchemy or Postgres hot-path bottlenecks surfaced by live benchmark runs."
plan_revision: 2
plan_commit: "86527cf1"
current_task: null
last_verified_checkpoint: null
last_reviewed_commit: null
generated:
  by: flow/0.26.0
  at: "2026-11-27T01:38:00Z"
verified:
  - by: user:cofin
    at: "2026-11-27T01:58:43Z"
    commit: "86527cf1"
sources:
  - id: src-root-spec
    resource: .agents/bundles/specs/runtime-perf-remediation/spec.md
    title: Root flow specification and R7 acceptance criteria
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

# Data Benchmarks, Empirical Verification, and OKF Sync

<!-- truth: start -->
- Extend `DataBackendConfig` with `db_adapter` (`native|sqlspec|advanced_alchemy|stdlib`) and `db_adapter_flavor` (`vertebra|upstream`), keeping `db_backend="memory"` as default.
- Implement real SQLite and PostgreSQL drivers across native, SQLSpec, Advanced Alchemy (`+vtb`/`+vtb_async` with `BrokerPool`), and stdlib/vanilla baselines with idempotent 10,000-row `world` seeding and non-serialized async Postgres execution.
- Add `capture_advanced_alchemy_report`, `COMPARATOR_PAIRS` entries, and `--engine advanced_alchemy --aa-engine sqlite|postgres` CLI dispatch to `tools/benchmarks/adapters/sql.py`.
- Register SQLSpec, Advanced Alchemy, and stdlib HTTP data targets in `tools/benchmarks/targets.py` with clean `Target.adapter` tags and offline pytest coverage.
- Run release `make build`, live Granian vs Vertebra HTTP/1.1 + Tokio 1 vs 2 A/B, SQLite, and Docker PostgreSQL benchmark matrices, then sync all six OKF knowledge chapters.
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Chapter Scope & Baseline

Chapter `ch04-data-benchmarks-and-verification` delivers Requirement R7 from the root specification and closes the flow with live verification and OKF documentation sync [^src-root-spec].

Today `DataBackend` only supports in-memory rows and a single mutex-guarded `sqlite3.Connection`, and its async methods delegate synchronously to `*_sync` [^src-bench-backend].

Standalone SQL comparators cover SQLite, PostgreSQL, SQLAlchemy `+vtb`, and SQLSpec, but omit an Advanced Alchemy repository comparator [^src-bench-sql].

HTTP data targets in `targets.py` leave `Target.adapter` unset (`"none"`) and lack dedicated SQLSpec, Advanced Alchemy, and stdlib baseline targets [^src-bench-targets].

Local Docker fixture support already provides `postgres:17-alpine` on tmpfs via `FixtureLauncher` [^src-bench-fixtures].

### 1.2 Empirical Dependence

This chapter is marked `empirical_dependent` on `ch01`, `ch02`, and `ch03`.

Live release benchmark measurements in `t05` must evaluate three triggers before final OKF publication:

- Embedded 1-worker `VERTEBRA_TOKIO_THREADS=1` versus `2` throughput and P99 latency.
- `WsgiExecutor` stall threshold behavior on `hello` and `io_10ms`.
- Any SQLSpec, Advanced Alchemy, or PostgreSQL hot-path bottlenecks surfaced under live load.

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Functional Requirements

- **Pluggable database adapters**: Extend `DataBackendConfig` with `db_adapter: str = "native"` (`VTB_BENCH_DB_ADAPTER`) and `db_adapter_flavor: str = "vertebra"` (`VTB_BENCH_DB_ADAPTER_FLAVOR`), keeping `db_backend="memory"` default [^src-bench-backend].
- **Real SQLite & PostgreSQL drivers**: Support `db_backend` in `("memory", "sqlite", "postgres")` across `native`, `sqlspec`, `advanced_alchemy`, and `stdlib` adapters without holding `self._db_lock` across async or pooled Postgres I/O.
- **Idempotent seeding**: Seed the 10,000-row `world` table idempotently on both SQLite and PostgreSQL without multi-worker startup races.
- **Standalone Advanced Alchemy comparator**: Add `capture_advanced_alchemy_report` and `--engine advanced_alchemy --aa-engine sqlite|postgres` in `tools/benchmarks/adapters/sql.py` with four registered `COMPARATOR_PAIRS` entries [^src-bench-sql].
- **HTTP data targets & offline tests**: Add SQLSpec, Advanced Alchemy, and stdlib data targets in `tools/benchmarks/targets.py` with explicit `Target.adapter` metadata and offline unit coverage in `test_e2e_benchmarks.py` [^src-bench-targets] [^src-test-e2e-bench].
- **Empirical verification & OKF sync**: Execute `make build` and the three live benchmark suites, then update all six OKF chapters listed in root `spec.md` Section 3 [^src-root-spec].

### 2.2 Concrete Contracts

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
| `native` | `vertebra` | `SyncSQLiteDriver` / `AsyncSQLiteDriver` | `SyncPostgresDriver` / `AsyncPostgresDriver` (`pool_size`, `shm_broker=true`) |
| `sqlspec` | `vertebra` | `SQLiteSyncConfig` / `SQLiteAsyncConfig` | `PostgresSyncConfig` / `PostgresAsyncConfig` |
| `sqlspec` | `upstream` | `sqlspec.adapters.sqlite.SqliteConfig` | `sqlspec.adapters.asyncpg.AsyncpgConfig` |
| `advanced_alchemy` | `vertebra` | `sqlite+vtb` / `sqlite+vtb_async` repository services | `postgresql+vtb` / `postgresql+vtb_async` (+ `BrokerPool` in `shm_broker`) |
| `advanced_alchemy` | `upstream` | `sqlite` / `sqlite+aiosqlite` repository services | `postgresql+psycopg` / `postgresql+asyncpg` repository services |
| `stdlib` | `vertebra` / `upstream` | `sqlite3` / `aiosqlite` | `psycopg` / `asyncpg` |

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
- **SQLite SHM broker DSN safety**: `apply_shm_broker_dsn` must only append `shm_broker=true` to PostgreSQL URLs, never raw SQLite file paths.
- **SQLite `:memory:` isolation**: Seed `world` idempotently per connection for `:memory:` targets and enable WAL + `busy_timeout=5000` for file-backed SQLite.
- **No mutex serialization on async/native Postgres**: Generate row IDs via thread-local `random.Random` and never hold `DataBackend._db_lock` across database I/O.
- **PostgreSQL Docker initdb readiness**: After `FixtureLauncher.start()`, poll for SQL readiness (`SELECT 1`) before launching benchmark servers because `postgres:17-alpine` restarts once after `initdb`.

## 3. Documentation & OKF Knowledge Plan

- `[0/6 files documented]`
  - `.agents/bundles/knowledge/architecture/runtime/topologies-and-embedding.md` (`t06`) — 1w GIL Embedded Tokio thread default, consumer-aligned `RequestBatcher` sharding, cgroup-aware `EmbeddedSupervisor`.
  - `.agents/bundles/knowledge/architecture/ipc/tiered-rings-and-batch-bridge.md` (`t06`) — `BatchResponseSink`, `InProcessRequest` `PathAndQuery`, `submit_batch_flushed`.
  - `.agents/bundles/knowledge/architecture/http/batching-dispatch-and-backpressure.md` (`t06`) — Lazy `IdleTimeoutStream`, connection-pinned shards, batch-locked `BridgeCorrelation`, `BackpressureController` fastpath, reusable `ShmDispatcher` scratch buffers.
  - `.agents/bundles/knowledge/architecture/worker/asgi-and-wsgi-runners.md` (`t07`) — Adaptive `WsgiExecutor`, `WsgiServerConfig`, lazy `RsgiHttpProtocol`, `AsgiScope` bitmask drop.
  - `.agents/bundles/knowledge/patterns/python/hot-path-and-async-dispatch.md` (`t07`) — Split `_on_notify_readable`/`_drain_batches`, `native_only` mixed sync/async fast path, direct `Request::body`, 1-arg extractors.
  - `.agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md` (`t07`) — SQLite/PostgreSQL native, SQLSpec, and Advanced Alchemy full-stack and standalone comparators plus empirical results.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Tasks | Verification Strategy |
| :--- | :--- | :--- |
| R7 (Pluggable SQLite & Postgres `native`/`stdlib` backends) | `t01` | `behavior_tdd` |
| R7 (Pluggable SQLite & Postgres `sqlspec`/`advanced_alchemy` backends) | `t02` | `behavior_tdd` |
| R7 (Standalone Advanced Alchemy comparator & CLI) | `t03` | `behavior_tdd` |
| R7 (HTTP data targets & offline unit tests) | `t04` | `regression_tdd` |
| R7 (Release build & live Granian/SQLite/PostgreSQL benchmark gate) | `t05` | `integration_acceptance` |
| R7 (OKF knowledge sync across 6 chapters) | `t06`, `t07` | `documentation_validation` |

## 5. Implementation Plan

- [ ] t01-pluggable-native-stdlib-db-drivers — Extend `DataBackendConfig` and implement `native` and `stdlib` SQLite/PostgreSQL `WorldDriver` backends (tasks/t01-pluggable-native-stdlib-db-drivers.md)
- [ ] t02-sqlspec-and-advanced-alchemy-db-drivers — Implement `sqlspec` and `advanced_alchemy` SQLite/PostgreSQL `WorldDriver` adapters (tasks/t02-sqlspec-and-advanced-alchemy-db-drivers.md)
- [ ] t03-advanced-alchemy-standalone-comparator — Add `capture_advanced_alchemy_report`, `COMPARATOR_PAIRS`, and CLI dispatch to `tools/benchmarks/adapters/sql.py` (tasks/t03-advanced-alchemy-standalone-comparator.md)
- [ ] t04-http-data-targets-and-offline-tests — Register SQLSpec, Advanced Alchemy, and stdlib HTTP data targets and offline pytest coverage (tasks/t04-http-data-targets-and-offline-tests.md)
- [ ] t05-empirical-release-benchmark-gate — Build release binaries and run live Granian, Tokio A/B, SQLite, and PostgreSQL benchmark matrices (tasks/t05-empirical-release-benchmark-gate.md)
- [ ] t06-okf-sync-runtime-ipc-http — Update runtime, IPC, and HTTP OKF knowledge chapters (tasks/t06-okf-sync-runtime-ipc-http.md)
- [ ] t07-okf-sync-worker-python-benchmarks — Update worker, Python hot-path, and benchmark OKF knowledge chapters (tasks/t07-okf-sync-worker-python-benchmarks.md)

### Fast Chapter Gate (`< 60s`)

```bash
uv run --no-sync pytest src/py/tests/unit/tools/test_e2e_benchmarks.py src/py/tests/unit/tools/test_adapter_benchmarks_sql.py src/py/tests/unit/test_bench_infrastructure.py
uv run --no-sync ruff check tools/benchmarks/ src/py/tests/unit/tools/test_e2e_benchmarks.py
uv run --no-sync mypy tools/benchmarks/apps/_data_backend.py tools/benchmarks/apps/_db_drivers.py tools/benchmarks/apps/_db_orm_drivers.py tools/benchmarks/adapters/sql.py tools/benchmarks/targets.py
```

## 6. Current State & Remaining Plan Snapshot

- **Chapter State**: `planned` (`empirical_dependent`, approved; awaiting `ch01`–`ch03` completion).
- **Remaining Tasks**: `t01`, `t02`, `t03`, `t04`, `t05`, `t06`, `t07`.
- **Replan Trigger**: Re-evaluate Embedded 1w `VERTEBRA_TOKIO_THREADS` (`1` vs `2`), `WsgiExecutor` stall threshold, and any SQLSpec, Advanced Alchemy, or PostgreSQL bottlenecks observed in `t05`.

[^src-root-spec]: `.agents/bundles/specs/runtime-perf-remediation/spec.md` — Root flow specification and R7 acceptance criteria.
[^src-bench-backend]: `tools/benchmarks/apps/_data_backend.py::DataBackend` — Shared benchmark database, cache, and storage backend.
[^src-bench-sql]: `tools/benchmarks/adapters/sql.py::capture_sqlspec_report` — Standalone SQL, SQLAlchemy, and SQLSpec adapter comparator suite.
[^src-bench-targets]: `tools/benchmarks/targets.py::ALL_TARGETS` — HTTP, data, framework, and Granian benchmark target definitions.
[^src-bench-fixtures]: `tools/benchmarks/fixtures.py::FixtureLauncher` — Tmpfs-backed local Docker benchmark fixtures including postgres:17-alpine.
[^src-test-e2e-bench]: `src/py/tests/unit/tools/test_e2e_benchmarks.py` — Offline end-to-end tests for benchmark store, drivers, adapters, and CLI matrix.
