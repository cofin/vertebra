# Vertebra Product Definition Index

Core product and engineering context for **Vertebra** (`vertebra` + `@vertebra/client`).

- **[Product Overview](product.md)** — Product purpose, runtime topologies (`Auto`, `Embedded`, and `Shm` across Python `3.12`–`3.15` and free-threaded `3.14t`/`3.15t`), multi-protocol execution (`native`, `ASGI`, `RSGI`, `WSGI`, `gRPC`), object storage, database drivers, telemetry, and framework extensions.
- **[Product & Engineering Guidelines](product-guidelines.md)** — Clean-break domain naming (`PY-07`), deep-module seam classification, zero-copy hot-path invariants, `msgspec.Struct(..., frozen=True, gc=False)` value objects, Python module layout, and Git/PR discipline.
- **[Technology Stack](tech-stack.md)** — Rust workspace crates (`vertebra-core`, `vertebra-http`, `vertebra-py`, `vertebra-bundler`, `vertebra-storage`, `vertebra-db`, `vertebra-bigquery`, `vertebra-spanner`), Python `vertebra-core` distribution configuration, TypeScript `@vertebra/client` subpath exports, and verification toolchains.
