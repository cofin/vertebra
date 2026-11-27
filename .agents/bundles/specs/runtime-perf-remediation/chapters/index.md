# Chapters: Runtime Performance Remediation and Full-Stack Data Benchmarks

* [ch01 — Adaptive Rust WSGI Executor](ch01-wsgi-adaptive-pool/spec.md) — [completed] Replace serial `drive_wsgi_batch` with an adaptive inline-plus-Rust `WsgiExecutor` pool and shared `WsgiServerConfig`.
* [ch02 — Embedded Batching & Bridge Completion](ch02-embedded-bridge-and-batching/spec.md) — [completed] Right-size 1w Embedded Tokio/shards, batch-lock `BridgeCorrelation`, add `BatchResponseSink`, and remove redundant bridge copies.
* [ch03 — HTTP Ingress & PyO3 Worker Hot Paths](ch03-ingress-and-worker-hotpaths/spec.md) — [completed] Optimize `IdleTimeoutStream`, connection shard affinity, `BackpressureController`, `InFlightRequests`, `ShmDispatcher`, `Worker`, and ASGI/RSGI/native runners.
* [ch04 — Data Benchmarks & Empirical Verification](ch04-data-benchmarks-and-verification/spec.md) — [planned] Wire SQLite and PostgreSQL (native, SQLSpec, Advanced Alchemy) benchmarks and run the full Granian and database verification gate.
