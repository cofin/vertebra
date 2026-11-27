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

> [!WARNING]
> Vertebra is experimental software. APIs and behavior may change without notice, and this project may be archived at any time.

Vertebra combines a Rust HTTP server, a typed Python web framework, database and object storage drivers, a frontend bundler, and a TypeScript client in one package.

## Install

```bash
uv add vertebra-core
```

Or with `pip`:

```bash
pip install vertebra-core
```

Add optional extras for databases, object storage, frontend bundling, or framework integrations:

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
- **Typed Python APIs**: Define routes, controllers, RPC procedures, dependency injection, and `msgspec.Struct` request/response models.
- **Protocols**: HTTP/1.1, HTTP/2, HTTP/3, TLS, compression, static files, WebSockets, Server-Sent Events, and gRPC.
- **Existing frameworks**: Run native Vertebra apps or existing ASGI, WSGI, and RSGI apps (Litestar, Starlette, FastAPI, Flask, Django) with `vtb run`.
- **Worker scaling**: Runs in-process by default, scales across shared-memory worker processes on standard Python, and scales across threads on free-threaded Python.

### Databases & Caching
- **Sync & async drivers**: PostgreSQL, MySQL, SQLite, Oracle, SQL Server, DuckDB, BigQuery, Spanner, Redis, and Valkey.
- **Pipelines & analytics**: Statement pipelining, Arrow results, DataFusion queries, change feeds, and Cloud SQL, AlloyDB, AWS RDS, and Azure Entra dialers.
- **ORM & SQL integrations**: Drop-in support for SQLSpec, SQLAlchemy, Advanced Alchemy, and Django database backends.

### Object Storage
- **One API across clouds**: Sync and async access to S3, GCS, Azure Blob, Cloudflare R2, OCI, HTTP, and local files via `vertebra-core[storage]`.
- **Large files & signing**: Streaming uploads and downloads, multipart transfers, HTTP Range responses, Parquet range reads, and presigned URLs.

### Frontend, Bundling & TypeScript Client
- **Bundling & SSR**: Built-in TypeScript/JSX bundler, QuickJS server-side rendering, Inertia, and HTMX helpers.
- **Bundled TypeScript client**: `@vertebra/client` ships inside the Python wheel with adapters for React, Vue, Svelte, Angular, HTMX, and Vite — no npm required.
- **Schema generation**: Generate TypeScript, Zod, Valibot, OpenAPI, and Protobuf schemas from Python routes.

### Queues, Live State & AI Agents
- **Background workers**: Distributed task queues, cron schedules, and dedicated worker processes.
- **Live state**: Delta-based real-time state rooms over WebSockets and cross-node pub/sub channels.
- **Agent protocols**: Built-in Model Context Protocol (MCP) tools, resources, and prompts alongside Agent-to-Agent (A2A) services and vector embeddings.

### Standalone Packaging
- Package applications into standalone directories or single-file executables with `vtb build`.

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
