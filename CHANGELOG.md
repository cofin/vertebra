# Changelog

All notable changes to Vertebra (`vertebra-core`) are documented in this file.

## 0.1.0

Initial release of Vertebra (`vertebra-core` on PyPI).

### Added

- Native Rust HTTP ingress and IPC runtime (`vertebra-core`, `vertebra-http`) supporting Hyper HTTP/1.1 and HTTP/2, ALPN TLS, optional HTTP/3 over QUIC, and gRPC wire framing.
- Zero-copy shared-memory IPC runtime supporting dual topologies (`Embedded` and `Shm`) with container-aware ring sizing and `/tmp/vtb-*` overflow fallback.
- Python application framework (`vertebra.framework`) supporting native handlers (`RequestBatch`, `Request`, `Response`, `Headers`), ASGI 3.0 (`AsgiRunner`), RSGI 1.6 (`RsgiAdapter`), and PEP 3333 WSGI (`WsgiAdapter`).
- Free-threaded Python 3.14t and 3.15t support across native PyO3 extensions (`vertebra._vertebra`, `vertebra._bundler`, `vertebra._storage`) with `gil_used = false` and PEP 810 lazy module exports.
- Native multicloud object storage (`vertebra.storage`, `_storage`) wrapping `object_store` with AWS S3, Google Cloud Storage, Azure Blob, OCI, and Cloudflare R2 presigning and transfer tracking.
- Shared-memory database and cache data plane (`vertebra.db`, `_vertebra`) across PostgreSQL, MySQL, SQLite, Oracle Thin TNS, SQL Server TDS, DuckDB, Redis, and Valkey RESP3.
- Full-stack TypeScript client SDK (`@vertebra/client`) with HTTP, WebSocket, SSE, HMR, and LiveState synchronization across React, Vue, Svelte, Angular, and HTMX adapters.
- Developer CLI toolchain (`vtb`) providing `run`, `dev`, `init`, `js`, `build`, and `mcp bridge` commands.
