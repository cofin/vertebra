---
title: "Architectural Decision Records Index"
description: "Index of Architectural Decision Records (ADRs) capturing durable cross-boundary choices in Vertebra."
tags:
  - decisions
  - adr
  - architecture
  - index
---

# Architectural Decision Records (`decisions/`)

Durable architectural decisions that govern cross-language boundaries, runtime topologies, data-plane execution, and packaging in Vertebra.

- **[ADR-001: Dual-Topology Embedded and Shared-Memory Runtime](adr-001-dual-topology-embedded-and-shm-runtime.md)** — Why Vertebra supports both in-process `Embedded` (`BatchBridge` + `SO_REUSEPORT`) and multi-process `Shm` (`MpmcRing` + `TieredRingSet`) topologies with automatic selection based on worker count and GIL vs. free-threaded Python.
- **[ADR-002: Shared-Memory Database Broker and Arrow IPC Data Plane](adr-002-two-tier-shm-database-and-arrow-data-plane.md)** — Why Vertebra combines pure-Rust native drivers (`vertebra-db` + `ShmLeaseTable` + `ArrowCursor`) with a universal shared-memory broker proxy (`ProxySyncConfig` / `ProxyAsyncConfig`) for SQLSpec and Advanced Alchemy.
- **[ADR-003: PyPI `vertebra-core` Distribution and Wheel-Bundled TypeScript Client](adr-003-pypi-vertebra-core-distribution-and-wheel-bundled-client.md)** — Why the PyPI package is published as `vertebra-core` while preserving the `vertebra` import namespace and embedding pre-built `@vertebra/client` bundles inside the Python wheel.
- **[ADR-004: Optional Driver Wheels and Shared-Memory Ownership](adr-004-optional-driver-wheels-and-shm-ownership.md)** — Client SDK isolation, canonical Python objects, and producer-owned zero-copy Arrow handoff across extension wheels.
