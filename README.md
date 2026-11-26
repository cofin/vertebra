<div align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://cofin.github.io/vertebra/assets/banner-inline-dark.svg">
    <source media="(prefers-color-scheme: light)" srcset="https://cofin.github.io/vertebra/assets/banner-inline-light.svg">
    <img alt="Vertebra" src="https://cofin.github.io/vertebra/assets/banner-inline-light.svg" width="520">
  </picture>
  <p><strong>One runtime for Python and TypeScript — backend, frontend, web, gRPC, workers, and standalone apps.</strong></p>
  <p>
    <a href="https://cofin.github.io/vertebra/">Documentation</a> ·
    <a href="https://cofin.github.io/vertebra/getting-started/quickstart/">Quickstart</a> ·
    <a href="https://cofin.github.io/vertebra/usage/">Usage Guides</a> ·
    <a href="https://cofin.github.io/vertebra/changelog/">Changelog</a>
  </p>
</div>

Vertebra combines a Rust HTTP server, a typed Python web framework, native database and object storage drivers, a frontend bundler, and a bundled TypeScript client into one package.

## Install

```bash
uv add vertebra-core
```

Or with `pip`:

```bash
pip install vertebra-core
```

Add optional extras for databases, object storage, frontend bundling, or framework integrations as needed:

```bash
uv add "vertebra-core[postgres]"
uv add "vertebra-core[storage,bundler]"
```

Available native extras: `bigquery`, `bundler`, `duckdb`, `mssql`, `mysql`, `oracle`, `postgres`, `redis`, `spanner`, `sqlite`, `storage`, `valkey`.

## Quickstart

Create `app.py`:

```python
import msgspec
from vertebra import App


class Item(msgspec.Struct):
    name: str
    quantity: int = 1


app = App()


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/items")
def create_item(body: Item) -> Item:
    return body
```

Run the development server with live reload:

```bash
vtb dev app:app --port 8000
```

Run in production across multiple workers:

```bash
vtb run app:app --host 0.0.0.0 --port 8000 --workers 4
```

## Features

### Web Framework & HTTP Server
- **Typed Python APIs**: Build endpoints with `App`, `Controller`, `Router`, `Procedure`, dependency injection (`Provide`/`Inject`), and `msgspec.Struct` validation.
- **Modern Protocols**: Native HTTP/1.1, HTTP/2, HTTP/3 (QUIC), TLS, compression, static files, WebSockets, Server-Sent Events, and gRPC.
- **Bring Your Own Framework**: Run native Vertebra apps or existing ASGI, WSGI, and RSGI apps (Litestar, Starlette, FastAPI, Flask, Django) with `vtb run`.
- **Automatic Worker Modes**: Runs in-process for a single worker or free-threaded Python (`3.14t`/`3.15t`), and scales across shared-memory worker processes when `--workers > 1` on standard Python (`--topology embedded|shm`).

### Databases & Caching
- **Native Sync & Async Drivers**: PostgreSQL, MySQL, SQLite, Oracle 26ai, SQL Server, DuckDB, BigQuery, Spanner, Redis, and Valkey.
- **High-Throughput Data Access**: Statement pipelining, zero-copy Arrow results, DataFusion queries, change feeds, and Cloud SQL/AlloyDB dialers.
- **Ecosystem Integrations**: Drop-in support for SQLSpec, SQLAlchemy, Advanced Alchemy, and Django database backends.

### Object Storage
- **One API Across Clouds**: Sync and async access to S3, GCS, Azure Blob, Cloudflare R2, OCI, HTTP, and local files via `vertebra-core[storage]`.
- **Large File Support**: Streaming reads/writes, multipart uploads, bulk transfers, HTTP Range responses, and presigned URLs.

### Frontend, Bundling & TypeScript Client
- **Built-In Bundling & SSR**: Bundle TypeScript/JSX, run server-side rendering, and integrate with Inertia.js or HTMX.
- **Zero-NPM Client SDK**: `@vertebra/client` ships inside the wheel with adapters for React, Vue, Svelte, Angular, HTMX, and Vite.
- **End-to-End Types**: Generate TypeScript, Zod, Valibot, OpenAPI, and Protobuf schemas directly from Python routes.

### Queues, Live State & AI Agents
- **Background Workers**: Distributed task queues, cron schedules, and dedicated worker processes via `vtb workers run`.
- **Real-Time Sync**: Delta-based live state rooms over WebSockets.
- **Agent Protocols**: Built-in Model Context Protocol (MCP) tools, resources, and prompts alongside Agent-to-Agent (A2A) services.

### Standalone Packaging
- Package applications into standalone directories (`vtb build --standalone`) or single-file executables (`vtb build --onefile`).

## CLI Commands

```bash
vtb init my-project          # Scaffold a new project
vtb dev app:app              # Run with live reload
vtb run app:app --workers 4  # Run the production server
vtb workers run              # Run background queue workers
vtb js build                 # Bundle frontend assets
vtb js generate-types        # Generate TypeScript/Zod types from routes
vtb build --standalone       # Build a standalone distribution
```

## Documentation

Full guides, configuration options, and API references are at [cofin.github.io/vertebra](https://cofin.github.io/vertebra/).
