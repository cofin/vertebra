# Architecture Overview

Vertebra is split into Rust crates (`src/rs/`), Python packages (`src/py/vertebra/`), and the `@vertebra/client` TypeScript SDK (`src/ts/vertebra-client/`).

## Architecture Chapters

| Chapter | Focus Area |
| --- | --- |
| **[Runtime & Shared-Memory IPC](runtime-and-ipc.md)** | How `vertebra-core` moves requests and responses between Rust and Python workers in-process or across shared-memory ring buffers. |
| **[HTTP/1.1, HTTP/2, HTTP/3 & gRPC Ingress](http-ingress.md)** | How `vertebra-http` batches requests, serves static assets, offloads CORS and compression, frames gRPC, WebSockets, and SSE, and sheds excess load. |
| **[PyO3 Bridge & Worker Lanes](pyo3-and-workers.md)** | How `vertebra-py`, `vertebra-db`, and `vertebra-storage` bridge Rust and Python, release the GIL during I/O, and isolate work across worker lanes. |
| **[Bundler, QuickJS SSR & TypeScript SDK](bundler-and-client.md)** | How `vertebra-bundler` compiles frontend assets with Rolldown and Oxc, renders SSR pages in QuickJS, and pairs with `@vertebra/client`. |
