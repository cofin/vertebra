---
title: Vertebra
hide:
  - navigation
  - toc
---

<h1 id="vertebra" class="vertebra-sr-only">Vertebra</h1>

<div class="vertebra-hero">
  <div class="vertebra-hero-lockup">
    <img class="vertebra-hero-logo" src="assets/logo.svg" alt="Vertebra Logo" width="72" height="72" />
    <div class="vertebra-hero-divider" aria-hidden="true"></div>
    <span class="vertebra-brand-text">Vertebra</span>
  </div>
  <p class="vertebra-hero-lead">
    One runtime for Python and TypeScript — backend, frontend, web, gRPC, workers, and standalone apps.
  </p>
  <div class="vertebra-hero-actions">
    <a class="vertebra-cta vertebra-cta--primary" href="getting-started/quickstart/">Get Started</a>
    <a class="vertebra-cta" href="architecture/">Architecture</a>
    <a class="vertebra-cta" href="reference/">API Reference</a>
  </div>
</div>

## Install

=== "uv"
    ```bash
    uv add vertebra-core
    ```
=== "pip"
    ```bash
    pip install vertebra-core
    ```
=== "pdm"
    ```bash
    pdm add vertebra-core
    ```
=== "poetry"
    ```bash
    poetry add vertebra-core
    ```

The `@vertebra/client` TypeScript SDK ships inside the Python wheel and is served directly by Vertebra, so you do not need npm to use it.

## First Application

Define handlers in `app.py` and pass them to `App`.

```python
from vertebra import App, get


@get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


app = App(route_handlers=[health])
```

Run the server:

```bash
vtb run app:app --host 127.0.0.1 --port 8000
```

## Explore Vertebra

| Section | What You Will Find |
| --- | --- |
| **[Getting Started](getting-started/index.md)** | Installation, quickstart, configuration, CLI workflows, runnable examples, and changelog. |
| **[Usage](usage/index.md)** | Guides for routing, workers, databases, object storage, frontend and SSR, and AI agents. |
| **[Architecture](architecture/index.md)** | How the HTTP server, shared-memory transport, worker lanes, and bundler work together. |
| **[Reference](reference/index.md)** | API documentation for Python packages, native extensions, the TypeScript client, and the CLI. |
| **[Examples](examples/index.md)** | Runnable Python examples covering routing, queues, live state, storage, databases, and agents. |

## Core Capabilities

| Area | What's included |
| --- | --- |
| **HTTP server** | HTTP/1.1, HTTP/2, HTTP/3, gRPC, WebSockets, SSE, TLS, compression, and static files in Rust. |
| **Existing apps & gradual adoption** | Serve existing Litestar, FastAPI, Starlette, Flask, and Django apps via ASGI, WSGI, or RSGI with `vtb run`, offload routing and middleware to Rust, and mount apps side by side with `--mount`. |
| **Cooperative multiprocessing** | Runs in-process by default, scales across worker processes over shared memory with shared connection broker pools, statement caches, and pub/sub rings on standard Python, and scales across threads on free-threaded Python. |
| **Databases & analytics** | Sync and async drivers for PostgreSQL, MySQL, SQLite, Oracle, SQL Server, DuckDB, BigQuery, Spanner, Redis, and Valkey, with pipelining, cloud IAM dialers, change feeds, and Arrow/DataFusion support. |
| **Object storage** | One sync and async API for S3, GCS, Azure Blob, Cloudflare R2, OCI, HTTP, local files, and in-memory stores, with streaming, presigned URLs, and Parquet range reads. |
| **Frontend & SSR** | Built-in TypeScript/JSX bundler, QuickJS server-side rendering, Inertia, HTMX, and a bundled TypeScript client with schema codegen. |
| **AI & agents** | Built-in MCP and A2A servers, a stdio bridge, tool workflows, and vector embeddings. |

## Runtime Overview

![Vertebra runtime architecture](assets/diagrams/runtime-architecture-light.svg#only-light)
![Vertebra runtime architecture](assets/diagrams/runtime-architecture-dark.svg#only-dark)
