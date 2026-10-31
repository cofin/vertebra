# Changelog

All notable changes to Vertebra are documented on this page.

## 0.1.0

Initial release of Vertebra (`vertebra-core` on PyPI and `@vertebra/client` on npm).

### Added

- **Rust HTTP Ingress & Shared-Memory IPC (`vertebra-core`, `vertebra-http`)**:
  - Hyper HTTP/1.1 and HTTP/2 server, ALPN TLS, optional HTTP/3 over QUIC, gRPC wire framing, WebSocket, and Server-Sent Events (SSE).
  - Lock-free SPSC and MPMC shared-memory ring buffers (`ShmHeader`, `SpscRing`, `MpmcRing`, `TieredRingSet`) and embedded `BatchBridge` with adaptive `futex`/`ulock` and `eventfd` signaling.
  - Opt-in `BackpressureController` with bounded in-flight permits, queue timeouts, and sliding-window circuit breaking.
- **Python Application Framework & Multi-Protocol Runners (`vertebra.framework`, `vertebra.server`)**:
  - `App`, `Router`, `Controller`, `RouteDef`, `Request`, `Response`, `DTO`, `Container`/`Provide` dependency injection, `Guard`/`SecurityConfig`, `OpenAPIConfig`, `LiveState`, `QueueService`/`WorkerRunner`/`CronScheduler`, and `TestClient`/`AsyncTestClient`.
  - Multi-protocol worker execution supporting native `RequestBatch` handlers, ASGI 3.0 (`AsgiRunner`), RSGI 1.6 (`RsgiAdapter`), and PEP 3333 WSGI (`WsgiAdapter`) across isolated worker lanes (`fast`, `async`, `cpu`, `blocking`).
- **Free-Threaded Python Support (`3.14t` / `3.15t`)**:
  - Native PyO3 extensions (`vertebra._vertebra`, `vertebra._bundler`, `vertebra._storage`) compiled with `gil_used = false` and PEP 561 `.pyi` type stubs.
- **Frontend Bundler, QuickJS SSR & TypeScript SDK (`vertebra-bundler`, `@vertebra/client`)**:
  - Embedded Rolldown/Oxc bundler (`Bundler`) and pooled QuickJS server-side rendering engine (`SsrClient`) with Inertia v3 and HTMX integration.
  - `@vertebra/client` TypeScript SDK providing `HttpClient`, `SseClient`, `WebSocketClient`, `LiveStateClient`, `ApexRegion`, `WasmClient`, `ArrowStreamClient`, `McpClient`, `A2aClient`, and framework adapters for React, Vue, Svelte, Angular, HTMX, and Vite.
- **Native Storage, Database Pools & Telemetry (`vertebra.storage`, `vertebra.db`, `vertebra.telemetry`)**:
  - Multi-cloud and local object storage (`Store`, `StoragePipeline`) backed by Rust `object_store` (S3, GCS, Azure, R2, OCI, HTTP, Local, Memory) with presigned URLs and multipart uploads.
  - Native Rust-backed database connection pools (`PostgresPool`, `MySqlPool`, `SqlitePool`) with health checks and OpenTelemetry `sqlcommenter` trace propagation.
- **Agent Protocols (`vertebra.agent`)**:
  - Model Context Protocol (`MCPRegistry`, `MCPConfig`, `MCPSkillsConfig`, `MCPTaskConfig`, `run_stdio_http_bridge`) and Agent-to-Agent (`A2AService`, `A2AConfig`, `AgentCard`) support plus Google ADK integration.
- **CLI & Packaging (`vertebra.cli`, `vertebra.build`)**:
  - `vtb` CLI (`run`, `dev`, `init`, `build`, `js`, `mcp`, `workers`) and standalone PyApp binary builder (`PyAppBuilder`).
