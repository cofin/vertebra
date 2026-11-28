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

Vertebra combines a Rust HTTP server, a typed Python web framework, database and object storage drivers, a frontend bundler, and a TypeScript client in one package. Serve existing ASGI, WSGI, or RSGI apps (Litestar, FastAPI, Starlette, Flask, Django) directly and adopt compiled routing, Rust middleware offload, native drivers, and shared-memory worker pools gradually.

## Install

```bash
uv add vertebra-core
```

Add optional extras (such as `[postgres]`, `[storage]`, or `[litestar]`) as needed — see the [installation guide](https://cofin.github.io/vertebra/getting-started/installation/) for the full list.

## Run an Existing App

Serve an existing Litestar, FastAPI, Starlette, Flask, or Django app directly:

```bash
vtb run myapp:app --host 0.0.0.0 --port 8000 --workers 4
```

Upgrade gradually by enabling framework plugins for compiled routing and Rust middleware offload (CORS, compression, static files), or mount existing and native apps side by side to migrate route by route:

```bash
vtb run api:app --mount /legacy=legacy_app:app --workers 4
```

## Quickstart

Create a native `app.py`:

```python
import msgspec
from vertebra import App, get, post


class Item(msgspec.Struct):
    name: str
    quantity: int = 1


@get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@post("/items")
async def create_item(body: Item) -> Item:
    return body


app = App(route_handlers=[health, create_item])
```

Run the development server with live reload, or run across multiple workers in production:

```bash
vtb dev app:app --port 8000
vtb run app:app --host 0.0.0.0 --port 8000 --workers 4
```

## Features

### Web Framework & HTTP Server
- **Run existing apps & upgrade gradually**: Serve existing Litestar, Starlette, FastAPI, Flask, or Django apps via ASGI, WSGI, or RSGI with `vtb run`, attach framework plugins for compiled routing and Rust middleware offload, or mount native and existing apps together with `--mount` and `App.mount`.
- **Cooperative multiprocessing & scaling**: Runs in-process by default, scales across cooperative worker processes over shared memory on standard Python — sharing bounded database connection pools through the shared-memory broker, compiled statement caches, pub/sub rings, and local embedding batches across workers — and scales across threads on free-threaded Python (`3.14t`/`3.15t`).
- **Typed Python APIs**: Define routes, controllers, RPC procedures, dependency injection, and `msgspec.Struct` request/response models.
- **Protocols**: HTTP/1.1, HTTP/2, HTTP/3, TLS, compression, static files, WebSockets, Server-Sent Events, and gRPC.

### Databases & Caching
- **Sync & async drivers**: PostgreSQL, MySQL, SQLite, Oracle, SQL Server, DuckDB, BigQuery, Spanner, Redis, and Valkey.
- **Shared-memory connection broker**: Share bounded connection pools, compiled statement caches, and crash-safe lease reclamation across worker processes (`vtb broker serve`, `?broker=true`, and SQLSpec `Proxy*Config`).
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
vtb run app:app --workers 4  # Run a native or ASGI/WSGI/RSGI app
vtb broker serve             # Run the shared-memory DB connection broker
vtb workers run              # Run background queue workers
vtb js build                 # Bundle frontend assets
vtb js generate-types        # Generate TypeScript/Zod types from routes
vtb build --standalone       # Build a standalone distribution
```

## Documentation

Full guides, configuration options, and API references are at [cofin.github.io/vertebra](https://cofin.github.io/vertebra/).
