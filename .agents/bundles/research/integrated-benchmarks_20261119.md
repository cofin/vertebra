---
type: Research
title: "Integrated Adapter & Full-Stack Protocol Benchmarking"
description: "Architecture and methodology for comparing Vertebra native database, cache, and storage adapters against third-party Python drivers standalone and under Native, RSGI, ASGI, WSGI, and gRPC web load across Embedded, SHM, and free-threaded topologies."
status: stable
tags:
  - research
  - benchmarks
  - adapters
  - protocols
  - free-threaded
  - shm-broker
sources:
  - id: bench-targets
    resource: tools/benchmarks/targets.py::ALL_TARGETS
    title: "Benchmark target registry, vtb launch commands, and free-threaded env helpers"
  - id: bench-runner
    resource: tools/benchmarks/runner.py::OhaRunner
    title: "OhaRunner load execution and ServerRunner subprocess lifecycle"
  - id: bench-scenarios
    resource: tools/benchmarks/scenarios.py::SCENARIOS
    title: "HTTP benchmark scenario registry and presets"
  - id: bench-schema
    resource: tools/benchmarks/sql/schema.sql
    title: "DuckDB benchmark_runs and benchmark_results schema"
  - id: bench-postgres
    resource: tools/benchmarks/postgres.py::capture_live_report
    title: "PostgreSQL standalone comparative benchmark capture and threshold comparator"
  - id: bench-baseline
    resource: tools/benchmarks/baseline.py::BaselineRecord
    title: "Benchmark regression baseline record and threshold checks"
  - id: server-worker
    resource: src/py/vertebra/server/worker.py::_instantiate_mount_runners
    title: "Worker runner dispatch across Native, ASGI, RSGI, and WSGI interfaces"
  - id: http-config
    resource: src/rs/vertebra-http/src/config.rs::ServerConfig
    title: "Rust HTTP ingress config, SO_REUSEPORT, Shm thread clamping, and VERTEBRA_H2C"
  - id: framework-app
    resource: src/py/vertebra/framework/_app.py::App._register_procedure_and_mount
    title: "Native unary gRPC POST route mounting over HTTP/2"
  - id: shm-broker
    resource: src/rs/vertebra-core/src/broker.rs::ShmLeaseTable
    title: "POSIX shared-memory connection lease table and statement cache"
  - id: broker-host
    resource: src/py/vertebra/extensions/broker/host.py::BrokerHost
    title: "Cross-process broker host and engine capsule supervision"
  - id: db-registry
    resource: src/py/vertebra/db/registry.py::SCHEME_REGISTRY
    title: "Database and cache adapter scheme registry and shared pool lookup"
  - id: object-store
    resource: src/py/vertebra/storage/_store.py::SyncObjectStore
    title: "Native multicloud object storage client"
  - id: test-fixtures
    resource: src/py/tests/conftest.py::pytest_plugins
    title: "Registered pytest-databases Docker service plugins"
  - id: pyproject
    resource: pyproject.toml
    title: "Dependency groups, editable companion crates, and Python version markers"
  - id: okf-bench
    resource: .agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md
    title: "Benchmark Suite and Free-Threaded Python 3.14t/3.15t OKF chapter"
  - id: okf-broker
    resource: .agents/bundles/knowledge/domains/data/broker-and-shm-leases.md
    title: "Shared-Memory Connection Broker and Statement Cache OKF chapter"
  - id: okf-dialects
    resource: .agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md
    title: "SQLSpec, Advanced Alchemy, and SQLAlchemy Dialect Extensions OKF chapter"
  - id: tefb
    resource: https://www.techempower.com/benchmarks/
    title: "TechEmpower Framework Benchmarks database and caching workload specifications"
  - id: pep703
    resource: https://peps.python.org/pep-0703/
    title: "PEP 703 Making the Global Interpreter Lock Optional in CPython"
---

# Integrated Adapter & Full-Stack Protocol Benchmarking

<!-- truth: start -->
- `tools/benchmarks/` currently runs 8 static/echo/sleep HTTP/1.1 scenarios against 17 targets (`5` embedded, `7` shm, `1` free-threaded, `4` baseline) using closed-loop `OhaRunner`. Results land in a 2-table DuckDB store (`benchmark_runs`, `benchmark_results`) without adapter, protocol, transport, or broker columns.[^bench-targets][^bench-runner][^bench-scenarios][^bench-schema]
- `tools/benchmarks/postgres.py` is the only standalone comparative adapter benchmark (`schema_version=1`, `min_throughput_ratio=0.90`, `max_latency_ratio=1.25`); the other 10 database/cache adapters, `ObjectStore`, 36 `+vtb` SQLAlchemy dialects, and 8 SQLSpec engines have no comparative harness.[^bench-postgres][^db-registry][^object-store][^okf-dialects]
- Vertebra's server supports Native, RSGI, ASGI, WSGI, and HTTP/2 gRPC (`VERTEBRA_H2C=1`), plus Embedded (`SO_REUSEPORT`), SHM multi-worker, and Free-Threaded `3.14t`/`3.15t` topologies, but benchmark targets omit Vertebra RSGI, gRPC, SHM-broker, and data-backed targets.[^server-worker][^http-config][^framework-app][^bench-targets]
- `ShmLeaseTable` and `BrokerHost` coordinate cross-worker connection pooling and statement caching for 6 SQL engines (PostgreSQL, MySQL, SQLite, Oracle, MSSQL, DuckDB), excluding Redis/Valkey and `ObjectStore`; only PostgreSQL pins per-slot server transactions.[^shm-broker][^broker-host][^okf-broker]
- `pyproject.toml` pins `duckdb`, `asyncpg`, `psycopg[binary]`, and several SQLAlchemy extras to `python_version < '3.14'`. Free-threaded `3.14t`/`3.15t` benchmarks must run the harness from `.venv`, launch `src/rs/target-ft/release/vertebra-server` via `_ft_env()`, and assert `sys._is_gil_enabled() is False`.[^pyproject][^bench-targets][^okf-bench]
- Integration tests already wire `pytest-databases` Docker plugins (PostgreSQL, MySQL, MariaDB, Oracle, MSSQL, Redis, Valkey, RustFS, Floci AWS/GCP/Azure/OCI, Azurite `azure_blob`), providing local `tmpfs`-backed fixtures without custom container plumbing.[^test-fixtures]
<!-- truth: end -->

## Question & Scope

How should Vertebra design and run integrated benchmarks that:

1. Compare Vertebra's native database, Redis/Valkey, object storage, SQLAlchemy `+vtb`, and SQLSpec adapters against third-party Python adapters both standalone and under web load.
2. Measure full-stack performance when running database, Redis/Valkey, and object storage connections through Native, RSGI, ASGI, WSGI, and gRPC across Embedded, SHM (per-worker pools vs SHM connection broker), and free-threaded `3.14t`/`3.15t` topologies.

## Verified Findings

| Claim | Evidence | Source |
| --- | --- | --- |
| Existing HTTP suite covers static/echo/sleep HTTP/1.1 only | `8` scenarios, `17` targets (`5` embedded, `7` shm, `1` free-threaded, `4` baseline), closed-loop `oha` without `-q`, 2-table DuckDB schema lacking adapter/protocol/broker columns | `tools/benchmarks/targets.py::ALL_TARGETS`[^bench-targets], `tools/benchmarks/runner.py::OhaRunner`[^bench-runner], `tools/benchmarks/sql/schema.sql`[^bench-schema] |
| Server supports 5 interfaces and 3 topologies not fully represented in targets | Native, RSGI (`RsgiAdapter`/`drive_rsgi_batch`), ASGI, WSGI, and unary gRPC over h2c (`VERTEBRA_H2C=1`), plus Embedded (`SO_REUSEPORT`), SHM (`worker_threads.clamp(1, 4)`), and `3.14t`/`3.15t`; `ALL_TARGETS` omits Vertebra RSGI, gRPC, SHM-broker, and data apps | `src/py/vertebra/server/worker.py::_instantiate_mount_runners`[^server-worker], `src/rs/vertebra-http/src/config.rs::ServerConfig`[^http-config], `src/py/vertebra/framework/_app.py::App._register_procedure_and_mount`[^framework-app] |
| Comparative adapter benchmarking exists only for PostgreSQL | `capture_live_report` / `compare_reports` compares `SyncPostgreSQLDriver`/`AsyncPostgreSQLDriver` against `psycopg`/`asyncpg` (`0.90` throughput / `1.25` latency bounds); 10 other DB/cache adapters, `ObjectStore`, 36 `+vtb` dialects, and 8 SQLSpec engines lack comparators | `tools/benchmarks/postgres.py::capture_live_report`[^bench-postgres], `src/py/vertebra/db/registry.py::SCHEME_REGISTRY`[^db-registry], `src/py/vertebra/storage/_store.py::SyncObjectStore`[^object-store], `sqlspec-alchemy-and-dialects.md`[^okf-dialects] |
| SHM broker spans 6 SQL engines and is unwired in benchmarks | `ShmLeaseTable` (`BROKER_ABI_VERSION=2`, `MAX_BROKER_SLOTS=4096`, `MAX_POOL_PARTITIONS=64`, 224 B statement slots) + `BrokerHost` cover PG, MySQL, SQLite, Oracle, MSSQL, DuckDB; Redis/Valkey and `ObjectStore` do not use the broker; only PG pins per-slot server transactions | `src/rs/vertebra-core/src/broker.rs::ShmLeaseTable`[^shm-broker], `src/py/vertebra/extensions/broker/host.py::BrokerHost`[^broker-host], `broker-and-shm-leases.md`[^okf-broker] |
| Free-threaded `3.14t`/`3.15t` requires `.venv` / `.venv-ft` isolation and GIL checks | `pyproject.toml` pins `duckdb`, `asyncpg`, `psycopg[binary]` to `<3.14`; harness runs from `.venv` and launches `target-ft/release/vertebra-server` via `_ft_env()`; 13 PyO3 modules set `gil_used=false`; third-party comparators can silently re-enable the GIL unless `sys._is_gil_enabled() is False` is asserted | `pyproject.toml`[^pyproject], `tools/benchmarks/targets.py::ALL_TARGETS`[^bench-targets], `benchmarks-and-free-threaded.md`[^okf-bench], PEP 703[^pep703] |
| Local container infrastructure already exists in tests | `pytest-databases` plugins (PG, MySQL, MariaDB, Oracle, MSSQL, Redis, Valkey, RustFS, Floci AWS/GCP/Azure/OCI, Azurite `azure_blob`) are registered; `/usr/bin/docker` is present on host, while `oha`, `ghz`, `h2load`, and `k6` are not currently on `PATH` | `src/py/tests/conftest.py::pytest_plugins`[^test-fixtures] |

## Recommended Benchmarking Architecture

Structure the suite in two complementary layers so driver overhead, protocol gateway overhead, and topology/broker scaling can be attributed cleanly instead of conflated in a single number.

### Layer 1: Standalone Adapter Comparators

Generalize `tools/benchmarks/postgres.py` (`schema_version=1`, ratio checks, reproducible JSON output) into a modular suite under `tools/benchmarks/adapters/` with isolated `[dependency-groups].bench-comparators` in `pyproject.toml` (keeping `<3.14` markers where upstream wheels lack free-threaded builds and never adding `--group duckdb` to `Makefile`).

| Engine / Layer | Vertebra Surface | Third-Party Comparators | Core Operations |
| --- | --- | --- | --- |
| PostgreSQL | `Sync`/`AsyncPostgreSQLDriver`, `+vtb` SQLAlchemy, SQLSpec | `asyncpg`, `psycopg[binary]` (sync + async) | Point `SELECT`, parameterized multi-row `SELECT`, `INSERT`/`UPDATE`, batch/`COPY`, Arrow export, concurrent pool contention |
| MySQL / MariaDB | `Sync`/`AsyncMySQLDriver` | `aiomysql` (async), `pymysql` / `mysqlclient` (sync) | Point `SELECT`, multi-row `SELECT`, batch writes, concurrent pool |
| SQLite / DuckDB | `Sync`/`AsyncSQLiteDriver`, `Sync`/`AsyncDuckDBDriver` | `sqlite3` / `aiosqlite`, `duckdb` | Local file vs `:memory:` (`pool_key() -> None`), concurrent reads, analytical Arrow scan |
| Oracle / MSSQL | `Sync`/`AsyncOracleDriver`, `Sync`/`AsyncMSSQLDriver` | `oracledb` (thin sync/async), `pymssql` / `pyodbc` | Point `SELECT`, array bind, concurrent pool |
| Redis / Valkey | `Sync`/`AsyncRedisDriver`, `Sync`/`AsyncValkeyDriver` | `redis-py` (`redis.asyncio` + sync, with/without `hiredis`) | `GET`/`SET`, `MGET`/pipeline (`10`/`50` ops), Pub/Sub fanout, high-thread `3.14t` scaling (`queue.Queue` contention check) |
| Object Storage | `Sync`/`AsyncObjectStore` (`S3Config`, `AzureConfig`, `GCSConfig`, `R2Config`, `OCIConfig`, `LocalConfig`, `MemoryConfig`) | `obstore`, `boto3` / `aiobotocore`, `azure-storage-blob` | Small (`4 KiB`) and medium (`1 MiB`) `GET`/`PUT`, streaming range read against RustFS (S3), Floci (AWS/GCP/Azure/OCI), Azurite (`azure_blob`), and Local/Memory |
| ORM / Query Layers | SQLAlchemy `+vtb` dialects + `BrokerPool`, SQLSpec adapters | Vanilla SQLAlchemy (`asyncpg`/`psycopg`), vanilla SQLSpec | Identical queries measuring dialect/driver overhead vs pool sharing |

Stratify non-blocking event-loop drivers (`asyncpg`, `aiomysql`, Vertebra native async drivers) separately from thread-pooled blocking drivers (`pymysql`, `pymssql`, `pyodbc`, `aiosqlite`), which exhibit 2x to 5x p99 tail latency inflation under web concurrency due to GIL or thread-pool saturation.

### Layer 2: Full-Stack Protocol x Topology x Data Matrix

Use standardized TechEmpower-style data routes plus an object-storage route so every interface executes identical work with matched database pool caps and worker counts.[^tefb]

| Route / Workload | Operations per Request | What It Isolates |
| --- | --- | --- |
| `/json` (baseline) | Static serialization only | Pure protocol gateway and serialization floor |
| `/db` | 1 random row `SELECT` by primary key (`1..10000`) | Driver round-trip + protocol handoff overhead |
| `/queries?queries=N` (`N=1,5,10,20`) | `N` sequential row `SELECT`s | Event-loop wakeup cost, `PyIter_Send` stepping, and driver latency amplification |
| `/updates?queries=N` | `N` reads + `N` primary-key `UPDATE`s | Transaction / pool checkout pressure and SHM broker lease contention |
| `/cached-queries?count=N` (`N=10,100`) | Redis/Valkey `GET`/`MGET` (optional fallback) | RESP3 codec and cache pool scaling without SQL overhead |
| `/object/{key}` | `4 KiB` / `64 KiB` `GET` and `PUT` against RustFS, Floci, or Azurite | Streaming body handoff and zero-copy buffer behavior |

Run these routes across the full protocol and topology matrix:

1. **Protocols**: Vertebra Native, Vertebra RSGI, Vertebra ASGI, Vertebra WSGI, and gRPC (distinguishing unary `App` procedures over `VERTEBRA_H2C=1` from `GrpcServiceRegistry` ASGI mounts, since native unary routes currently bypass HTTP/2 trailer emission).[^framework-app][^http-config]
2. **External Server & Framework Baselines**:
   - Raw gateway servers: `granian-rsgi`, `granian-asgi`, and `uvicorn-httptools` running the exact same app logic and third-party drivers (`asyncpg`, `psycopg`, `redis-py`).
   - Full web frameworks: **Django-Bolt** (`django-bolt` Rust/Actix + PyO3 + msgspec `runbolt`), **Spikard** (Rust-backed), **Litestar** (on `vtb`, `granian`, `uvicorn`), **FastAPI** (on `vtb`, `granian`, `uvicorn`), **Django** (standard ASGI/WSGI on `vtb` vs `django-bolt`), and **Flask** (on `vtb` WSGI).
3. **Topologies**:
   - **Embedded** (`1w` and `4w` via `SO_REUSEPORT`) with per-worker pools.
   - **SHM multi-worker** (`4w`) comparing **per-worker pools** vs **`?shm_broker=true` (`BrokerHost` + `ShmLeaseTable`)** at an identical total server connection budget (for example, `4` workers x `10` per-worker connections = `40` total vs a single `40`-slot SHM broker pool).
   - **Free-Threaded (`3.14t`/`3.15t`)** (`1t`, `4t`, `8t` workers) with `VERTEBRA_STRICT_FREE_THREADING=1` and a startup assertion that `sys._is_gil_enabled() is False`.

### Load Generation & Measurement Discipline

- **Avoid coordinated omission**: `OhaRunner` currently passes `--latency-correction` without `-q`, measuring closed-loop saturation throughput only.[^bench-runner] Pair closed-loop sweeps (`c=32, 128, 256`) for peak RPS with open-loop constant-arrival-rate runs for honest p95/p99/p99.9 latency. Use `oha -q <rps>` (`50%` and `80%` of peak RPS), `k6` `constant-arrival-rate`, `h2load -c -m` (HTTP/2), and `ghz --rps` (gRPC).
- **Reproducible local containers**: Reuse the `pytest-databases` Docker plugins (using **RustFS** instead of MinIO for S3, plus **Floci** for AWS/GCP/Azure/OCI emulation and **Azurite** `azure_blob` for Azure Blob) with `tmpfs` data mounts, explicit `mem_limit` for Oracle/MSSQL (`>= 512m-1g`), and `--shm-size` (`>= 64m-256m`) for PostgreSQL/AlloyDB Omni.
- **DuckDB Schema Extension**: Extend `tools/benchmarks/sql/schema.sql` (`benchmark_results`) with `adapter`, `protocol`, `transport`, `broker_mode`, `load_mode`, `target_rps`, `pool_size`, and `gil_enabled` columns so `tools.benchmarks report` and `compare` can slice by any dimension while keeping existing HTTP runs backward-compatible.[^bench-schema][^bench-baseline]

## Proposed Implementation Jobs

The research decomposes into 6 separate-file jobs for `/bb:spec`:

1. **`bench-schema-and-store`** (`~220` LOC): `tools/benchmarks/sql/{schema,insert,queries}.sql`, `tools/benchmarks/store.py`, `tools/benchmarks/reports.py` — add matrix columns (`adapter`, `protocol`, `transport`, `broker_mode`, `load_mode`, `gil_enabled`) and matrix reporting.
2. **`bench-load-drivers`** (`~260` LOC): `tools/benchmarks/runner.py`, `tools/benchmarks/drivers.py` — add open-loop `oha -q`, `h2load`, `ghz`, and `k6` load runners plus `BrokerHost` / fixture lifecycle hooks in `ServerRunner`.
3. **`bench-standalone-adapters`** (`~280` LOC): `tools/benchmarks/adapters/{__init__,sql,cache,storage}.py` — generalize `tools/benchmarks/postgres.py` across SQL, Redis/Valkey, ObjectStore, SQLAlchemy `+vtb`, and SQLSpec comparators.
4. **`bench-e2e-workload-apps`** (`~280` LOC): `tools/benchmarks/apps/data_{native,asgi,rsgi,wsgi,grpc}.py` — standardized `/db`, `/queries`, `/updates`, `/cached-queries`, and `/object/{key}` apps with pluggable Vertebra and third-party adapters.
5. **`bench-matrix-targets-and-cli`** (`~240` LOC): `tools/benchmarks/{scenarios,targets,cli}.py` — register Vertebra RSGI, gRPC (`VERTEBRA_H2C=1`), SHM-broker, data-backed, and multi-interface `3.14t`/`3.15t` targets with CLI matrix filters.
6. **`bench-fixtures-deps-and-tests`** (`~200` LOC): `tools/benchmarks/fixtures.py`, `pyproject.toml`, `src/py/tests/unit/tools/test_e2e_benchmarks.py` — isolated `[dependency-groups].bench-comparators`, `tmpfs` Docker fixture launcher, and offline unit tests for parsers, schemas, and threshold checks.

## Skipped Subtrees & Open Risks

- **Skipped subtrees**: Per-engine adapter constructors under `src/py/vertebra/db/adapters/{bigquery,datafusion,duckdb,mssql,mysql,oracle,redis,spanner,sqlite,valkey}`, framework extensions under `src/py/vertebra/extensions/{alchemy,litestar,sqlalchemy,sqlspec}`, and `src/py/vertebra/cache` were not read line-by-line; implementation jobs should inspect exact constructor and pool options for each adapter before coding.
- **Load-generator CLI provisioning**: Install `oha`, `ghz`, `h2load`, and `k6` on demand via a `Makefile` target (`make bench-tools`) when absent from `PATH`, keeping offline unit tests mocked at the `subprocess.run` boundary and retaining the Docker command builder as an optional fallback.
- **Free-threaded third-party comparator support**: Verify which third-party drivers (`redis`, `pymysql`, `aiomysql`, `aiosqlite`, `oracledb`, `pymssql`, `pyodbc`, `boto3`, `obstore`) build on `3.14t`/`3.15t` and declare `Py_MOD_GIL_NOT_USED` vs requiring `python_version < '3.14'`.
- **gRPC trailer behavior**: Native unary `App` procedures currently bypass HTTP/2 trailer emission; confirm whether gRPC benchmarks should target `GrpcServiceRegistry` ASGI mounts first or wait for native h2c trailer support.
- **Runtime and cloud quota guardrails**: Rebuild or clean stale `src/rs/target/` binaries because `find_server_binary` picks the newest `st_mtime_ns`. Never share SQLAlchemy `Compiled` objects across processes, and preserve `:memory:` `pool_key() -> None` isolation. Bound live cloud pools below Cloud SQL connection limits, Memorystore Valkey client limits, and GCS per-object write limits.

[^bench-targets]: `tools/benchmarks/targets.py::ALL_TARGETS`
[^bench-runner]: `tools/benchmarks/runner.py::OhaRunner`
[^bench-scenarios]: `tools/benchmarks/scenarios.py::SCENARIOS`
[^bench-schema]: `tools/benchmarks/sql/schema.sql`
[^bench-postgres]: `tools/benchmarks/postgres.py::capture_live_report`
[^bench-baseline]: `tools/benchmarks/baseline.py::BaselineRecord`
[^server-worker]: `src/py/vertebra/server/worker.py::_instantiate_mount_runners`
[^http-config]: `src/rs/vertebra-http/src/config.rs::ServerConfig`
[^framework-app]: `src/py/vertebra/framework/_app.py::App._register_procedure_and_mount`
[^shm-broker]: `src/rs/vertebra-core/src/broker.rs::ShmLeaseTable`
[^broker-host]: `src/py/vertebra/extensions/broker/host.py::BrokerHost`
[^db-registry]: `src/py/vertebra/db/registry.py::SCHEME_REGISTRY`
[^object-store]: `src/py/vertebra/storage/_store.py::SyncObjectStore`
[^test-fixtures]: `src/py/tests/conftest.py::pytest_plugins`
[^pyproject]: `pyproject.toml`
[^okf-bench]: `.agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md`
[^okf-broker]: `.agents/bundles/knowledge/domains/data/broker-and-shm-leases.md`
[^okf-dialects]: `.agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md`
[^tefb]: `https://www.techempower.com/benchmarks/`
[^pep703]: `https://peps.python.org/pep-0703/`
