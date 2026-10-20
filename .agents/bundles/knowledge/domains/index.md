---
name: domains-index
description: Index of Vertebra's nested domain vocabulary and bounded-context chapters across Python, Rust, and TypeScript.
---

# Domain Vocabulary Index

Vertebra enforces a clean-break domain naming model across Python, Rust, and TypeScript (`commit 6dc6be4`): every class, struct, and function states *what* the domain concept is without brand prefixes (`Vertebra*`), implementation-mechanism suffixes, or backward-compatibility aliases.

## Subsystems

- [Python Framework, Routing & Serialization (`framework/`)](framework/routing-and-app.md) — `App`, `Router`, `RouteDef`, `HandlerInfo`, `InertiaMiddleware`, `InertiaConfig`, `PageProps`, `JSONSerializer`, and `OptionalDependency`.
- [Python Server, Worker Loop & Protocol Adapters (`server/`)](server/worker-and-protocols.md) — `AsyncRuntime`, `BatchDispatchPlan`, `AsgiRunner`, `AsgiScope`, `AsgiReceive`, `AsgiSend`, `WsgiAdapter`, `WsgiEnviron`, and `WorkerSupervisor`.
- [Rust IPC, Shared Memory & Ring Transport (`ipc/`)](ipc/transport-and-rings.md) — `ShmRegion`, `ShmHeader`, `SpscRing`, `SlotView`, `MpmcRing`, `RingPair`, `TieredRingSet`, `BatchBridge`, `TransportNode`, `BatchSnapshot`, and `CompressionCodec`.
- [Rust HTTP Ingress, Batching & Dispatch (`http/`)](http/server-and-dispatch.md) — `Server`, `Router`, `RouteTable`, `RequestBatcher`, `BatchPolicy`, `Dispatcher`, `ShmDispatcher`, `InFlightRequests`, `BackpressureController`, and `RingFactory`.
- [Frontend Bundler, Codegen & TypeScript SDK (`frontend/`)](frontend/bundler-and-client-sdk.md) — `Bundler`, `SourceWatcher`, `SsrPool`, `BuildOrchestrator`, `DevServerProcess`, `TypeExtractor`, `HttpClient`, `ClientConfig`, `RealtimeChannel`, `SessionStore`, `HotReloadClient`, and `Console`.
