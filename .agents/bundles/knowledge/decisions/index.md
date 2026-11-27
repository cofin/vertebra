# Architectural Decision Records (`decisions/`)

Durable architectural decisions governing cross-language boundaries, runtime topologies, data-plane execution, and packaging in Vertebra.

- **[ADR-001: Dual-Topology Embedded and Shared-Memory Runtime](adr-001-dual-topology-embedded-and-shm-runtime.md)** — Why Vertebra supports both in-process embedded and multi-process shared-memory topologies with automatic selection.
- **[ADR-002: Shared-Memory Database Broker and Arrow IPC Data Plane](adr-002-two-tier-shm-database-and-arrow-data-plane.md)** — Why Vertebra pairs Rust native drivers and Arrow IPC transport with a universal shared-memory broker proxy.
- **[ADR-003: PyPI `vertebra-core` Distribution and Wheel-Bundled TypeScript Client](adr-003-pypi-vertebra-core-distribution-and-wheel-bundled-client.md)** — Why the PyPI package is named `vertebra-core` while exposing `import vertebra` and bundling pre-built `@vertebra/client` assets.
- **[ADR-004: Optional Driver Wheels and Shared-Memory Ownership](adr-004-optional-driver-wheels-and-shm-ownership.md)** — Dependency isolation, canonical Python exception/cursor types, and producer-owned Arrow handoff across extension wheels.
- **[ADR-005: Persistent Object-Store TTL Envelopes](adr-005-persistent-object-store-ttl-envelopes.md)** — Why Litestar object-store entries wrap deadlines and payloads in one versioned envelope.
- **[ADR-006: Native Cursor and Connection-Scoped Leases](adr-006-native-cursor-and-connection-scoped-leases.md)** — Why each SQL extension owns a PEP 249-shaped cursor and why broker leases bind to connection checkout.
