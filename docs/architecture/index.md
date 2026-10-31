# Architecture Overview

Vertebra is organized into **Rust crates** (`src/rs/`), **Python framework and worker packages** (`src/py/vertebra/`), and the **TypeScript client SDK** (`src/ts/vertebra-client/`).

## Crate & Package Map

```mermaid
flowchart TD
    subgraph Rust["Rust Workspace (src/rs/)"]
        Core["vertebra-core (Shared Memory & Worker Transport)"]
        Http["vertebra-http (HTTP/1.1, H2, H3, gRPC, WS, SSE, Backpressure)"]
        Py["vertebra-py (_vertebra PyO3 Extension)"]
        Bundler["vertebra-bundler (_bundler Rolldown/Oxc + QuickJS SSR)"]
        Storage["vertebra-storage (_storage Cloud & Local Object Storage)"]
        Db["vertebra-db (Postgres, MySQL, SQLite Pools & Wire Codecs)"]
    end

    subgraph Python["Python 3.12–3.15 & 3.14t/3.15t (src/py/vertebra/)"]
        Server["vertebra.server (Workers, Lanes, Event Loops, ASGI/WSGI/RSGI)"]
        Framework["vertebra.framework (App, Controller, Router, DI, Queues, Inertia, SSR)"]
        Agent["vertebra.agent (MCP, A2A, SpecTree & Workflows)"]
    end

    subgraph TS["TypeScript SDK (src/ts/vertebra-client/)"]
        Client["@vertebra/client (HTTP, SSE, WS, LiveState, Apex, Wasm, Arrow, MCP/A2A)"]
    end

    Http --> Core
    Py --> Core
    Py --> Http
    Py --> Db
    Server --> Py
    Framework --> Server
    Framework --> Bundler
    Framework --> Storage
    Client --> Http
```

## Architecture Chapters

| Chapter | Focus Area |
| --- | --- |
| **[Runtime & Shared-Memory IPC](runtime-and-ipc.md)** | How `vertebra-core` moves requests and responses between Rust and Python workers in-process (`BatchBridge`) or across shared memory (`SpscRing`, `MpmcRing`, `TieredRingSet`). |
| **[HTTP/1.1, HTTP/2, HTTP/3 & gRPC Ingress](http-ingress.md)** | How `vertebra-http` batches requests, serves static assets, offloads CORS and compression, frames gRPC/WebSockets/SSE, and sheds excess load (`BackpressureController`). |
| **[PyO3 Bridge & Worker Lanes](pyo3-and-workers.md)** | How `vertebra-py` (`_vertebra`), `vertebra-db`, and `vertebra-storage` bridge Rust and Python, release the GIL during I/O, and isolate work across `fast`, `async`, `cpu`, and `blocking` lanes. |
| **[Bundler, QuickJS SSR & TypeScript SDK](bundler-and-client.md)** | How `vertebra-bundler` (`_bundler`) compiles frontend assets with Rolldown/Oxc, renders SSR pages in QuickJS, and pairs with `@vertebra/client`. |
