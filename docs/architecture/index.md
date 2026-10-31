# Architecture Overview

Vertebra is a polyglot runtime divided into clear responsibilities across **Rust 2024 crates** (`src/rs/`), **Python framework and worker runners** (`src/py/vertebra/`), and the **TypeScript client SDK** (`src/ts/vertebra-client/`).

## Crate & Package Map

```mermaid
flowchart TD
    subgraph Rust["Rust 2024 Workspace (src/rs/)"]
        Core["vertebra-core (SHM Rings, Futex/Ulock, BatchBridge)"]
        Http["vertebra-http (HTTP/1.1, H2, H3, gRPC, WS, SSE, Backpressure)"]
        Py["vertebra-py (_vertebra PyO3 Extension)"]
        Bundler["vertebra-bundler (_bundler Rolldown/Oxc + QuickJS SSR)"]
        Storage["vertebra-storage (_storage object_store Bindings)"]
        Db["vertebra-db (Postgres, MySQL, SQLite Pools & Wire Codecs)"]
    end

    subgraph Python["Python 3.12–3.15 & 3.14t/3.15t (src/py/vertebra/)"]
        Server["vertebra.server (Worker, Lanes, EventLoopPool, ASGI/WSGI/RSGI)"]
        Framework["vertebra.framework (App, Router, DTO, DI, Queues, Inertia, SSR)"]
        Agent["vertebra.agent (MCP, A2A, Google ADK)"]
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
| **[Runtime & Shared-Memory IPC](runtime-and-ipc.md)** | `vertebra-core` memory layout (`ShmHeader`), cache-line-padded SPSC and MPMC rings, tiered response rings (`TieredRingSet`), `futex`/`ulock` handshakes, and `BatchBridge`. |
| **[HTTP/1.1, HTTP/2, HTTP/3 & gRPC Ingress](http-ingress.md)** | `vertebra-http` request batching (`RequestBatcher`), radix routing, `BackpressureController` admission control and circuit breaking, and multi-protocol framing. |
| **[PyO3 Bridge & Worker Lanes](pyo3-and-workers.md)** | `vertebra-py` (`_vertebra`) zero-copy `RequestBatch`, `gil_used = false` free-threaded `3.14t`/`3.15t` execution, parallelism strategies, and worker lane isolation. |
| **[Bundler, QuickJS SSR & TypeScript SDK](bundler-and-client.md)** | `vertebra-bundler` (`_bundler`) embedded Rolldown/Oxc compilation, pooled QuickJS server-side rendering, and `@vertebra/client` subpath architecture. |
