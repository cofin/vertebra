<div align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/assets/banner-inline-dark.svg">
    <source media="(prefers-color-scheme: light)" srcset="docs/assets/banner-inline-light.svg">
    <img alt="Vertebra" src="docs/assets/banner-inline-light.svg" width="520">
  </picture>
  <p><strong>One runtime for Python and TypeScript — backend, frontend, web, gRPC, workers, and standalone apps.</strong></p>
</div>

---

Vertebra is one unified runtime for Python and TypeScript across backend, frontend, web, gRPC, workers, and standalone apps:

- **Python Framework & Server**: Type-safe APIs (`App`, `Controller`, `Router`, `Route`, `Procedure`, `ProcedureRouter`, `Provide`, `Inject`, `QueueService`, Inertia v3, SSR, and MCP/A2A agent protocols) plus native, ASGI, WSGI, and RSGI support.
- **Worker Runtime (`vertebra-core`, `vertebra-py`)**: Runs Python in-process or across multiple worker processes over shared memory.
- **HTTP Server & Bundler (`vertebra-http`, `vertebra-bundler`)**: HTTP/1.1, HTTP/2, HTTP/3, gRPC, WebSockets, SSE, TLS, compression, static files, JavaScript bundling, and QuickJS SSR.
- **Storage, Databases & TypeScript Client (`vertebra-storage`, `vertebra-db`, `vertebra-postgres`, `vertebra-mysql`, `vertebra-mssql`, `vertebra-duckdb`, `vertebra-oracle`, `vertebra-bigquery`, `vertebra-spanner`, `@vertebra/client`)**: Multicloud object storage (S3, GCS, Azure, R2, OCI, HTTP, local), native database drivers (PostgreSQL, MySQL, SQLite, Oracle 26ai, SQL Server, DuckDB, Google BigQuery, Google Cloud Spanner, Redis, Valkey, DataFusion), native `driver.pipeline()` batching (`SyncPipelineCapable`, `AsyncPipelineCapable`), Cloud SQL/AlloyDB dialers, and a typed TypeScript client for React, Vue, Svelte, Angular, HTMX, and Vite.

## Quickstart

```bash
uv add vertebra-core
# Optional native wheels:
# bigquery, bundler, duckdb, mssql, mysql, oracle, postgres, spanner, storage
uv add "vertebra-core[postgres]"
```

## CLI Usage (`vtb`)

```bash
vtb run myapp:app --host 127.0.0.1 --port 8000 --workers 4
vtb dev myapp:app --port 8000
vtb js build
vtb js generate-types
vtb init my-project
vtb build --standalone
```

## Common Make Targets

```bash
make dev         # Build Rust crates and install debug Python extensions
make build       # Build TS client, release Rust binaries, and wheel
make test        # Run all tests (Rust + Python + TypeScript)
make rs-test     # Run Rust workspace tests (tls, compression, and gcp features)
make py-test     # Run Python test suite
make oracle-test # Run Oracle 26ai Rust tests, stubtest, and Python unit tests
make oracle-wheel-check # Verify vertebra-oracle wheel isolation
make postgres-test # Run vertebra-postgres Rust tests and _postgres stubtest
make postgres-wheel-check # Verify vertebra-postgres wheel and isolation
make postgres-bench-check # Run Criterion tests and comparator unit suite
make ts-test     # Run TypeScript client tests (Vitest)
make lint        # Format and lint Python, Rust, and TypeScript
make check       # Run full lint, type checks, all tests, and coverage
make docs        # Build Zensical site, postbuild, and verify artifacts
make docs-serve  # Serve Zensical documentation locally with live reload
make bench       # Run benchmark suite
```

## Repository Layout

```text
src/
  py/
    vertebra/          # Python framework, server, CLI, agent, storage, db
    tests/             # Python unit and FFI test suite
  rs/
    vertebra-core/     # Shared-memory rings and worker IPC
    vertebra-py/       # PyO3 native extension (_vertebra)
    vertebra-http/     # HTTP/1.1, HTTP/2, HTTP/3, gRPC, WebSocket, and TLS server
    vertebra-bundler/  # JavaScript bundler and QuickJS SSR (_bundler)
    vertebra-storage/  # Cloud and local object storage (_storage)
    vertebra-db/       # DB wire codecs, cloud dialers, and Arrow SHM framing
    vertebra-bigquery/ # BigQuery REST v2 and Storage Read/Write (_bigquery)
    vertebra-duckdb/   # Embedded DuckDB 2.0 and VTBARROW SHM (_duckdb)
    vertebra-mssql/    # SQL Server TDS 7.4/8.0 and BulkLoad (_mssql)
    vertebra-mysql/    # MySQL/MariaDB binary protocol (_mysql)
    vertebra-oracle/   # Oracle 26ai Thin client, DRCP pool, and Arrow SHM (_oracle)
    vertebra-postgres/ # PostgreSQL 3.0, PGCOPY, pgoutput, Arrow SHM (_postgres)
    vertebra-spanner/  # Spanner v1 gRPC and DatabaseAdmin (_spanner)
  ts/
    vertebra-client/   # @vertebra/client TypeScript SDK
```

## Execution Modes

Vertebra automatically picks the right worker mode from `--workers` and your Python build (or you can set `--topology embedded` or `--topology shm` yourself):

### 1. Embedded Mode
- **When it runs**: Automatically for a single worker (`--workers 0` or `1`) or on free-threaded Python (`3.14t`/`3.15t`), or when you pass `--topology embedded`.
- **How it works**: The Rust HTTP server and Python app run inside the same OS process with no cross-process overhead.

### 2. Shared-Memory Mode (`shm`)
- **When it runs**: Automatically when `--workers > 1` on standard GIL-enabled Python on Linux/macOS, or when you pass `--topology shm`.
- **How it works**: The Rust HTTP server spawns separate Python worker processes and passes requests and responses through shared memory.
