---
type: Decision
title: "ADR-004: Optional Driver Wheels and Shared-Memory Ownership"
description: "Database client dependency isolation and producer-owned Arrow mapping transfer across independently linked PyO3 extensions."
status: stable
tags:
  - decisions
  - adr
  - packaging
  - database
  - pyo3
  - shm
  - ownership
generated:
  by: flow/0.26.0
  at: "2026-11-05T00:14:04Z"
verified:
  - by: agent:flow-setup
    at: "2026-11-07T03:55:00Z"
    commit: cdb978d3
sources:
  - id: src-python-packaging
    resource: pyproject.toml
    title: "Core distribution and optional engine dependencies"
  - id: src-db-manifest
    resource: src/rs/vertebra-db/Cargo.toml
    title: "Shared Rust primitives and optional GCP and PyO3 features"
  - id: src-db-py-async
    resource: src/rs/vertebra-db/src/py_async.rs
    title: "Independent completion queues and producer-owned capsule callbacks"
  - id: src-core-shm
    resource: src/rs/vertebra-core/src/shm.rs
    title: "C ABI capsule and shared-memory mapping lifetime"
  - id: src-py-arrow-cursor
    resource: src/rs/vertebra-py/src/db.rs
    title: "Core ArrowCursor adoption of a producer-owned mapping"
---

# ADR-004: Optional Driver Wheels and Shared-Memory Ownership

<!-- truth: start -->
- BigQuery and Spanner clients ship in optional `vertebra-bigquery` and `vertebra-spanner` distributions, exposed as `vertebra._bigquery` and `vertebra._spanner` through core install extras.
- `vertebra._vertebra` owns canonical database exception classes and `ArrowCursor`; extension modules resolve those Python objects at runtime.
- Cross-extension Arrow handoff uses a named `vertebra.ShmCapsule` with an atomic ownership context and producer-owned cleanup callbacks.
- Each separately linked extension uses its own event-loop completion drainer attribute.
<!-- truth: end -->

## Context

Database SDKs bring dependency trees that HTTP-only applications do not need. Keeping every client inside `vertebra._vertebra` couples the core wheel to those SDKs. Optional extension wheels isolate them, but separately linked Rust libraries have independent static variables and PyO3 class identities. A consumer also needs to retain an Arrow mapping without reconstructing a Rust allocation owned by another extension.[^src-python-packaging][^src-db-manifest][^src-db-py-async]

## Decision

Ship native database clients in per-driver PyO3 wheels and keep reusable transport, framing, completion, and Arrow helpers in `vertebra-db`. BigQuery and Spanner use this boundary. Further native engine cutovers follow the same packaging seam.[^src-db-manifest][^src-python-packaging]

Keep database exceptions and `ArrowCursor` canonical in `vertebra._vertebra`. Optional extensions import those Python objects and use distinct event-loop drainer attributes. Transfer Arrow mappings with `construct_py_arrow_cursor`: a `vertebra.ShmCapsule` carries the pointer, length, atomic context, and cleanup callbacks. The core cursor claims the context once and invokes the producing extension's callback when its last backing owner drops.[^src-db-py-async][^src-core-shm][^src-py-arrow-cursor]

## Alternatives Rejected

| Alternative | Trade-off |
| --- | --- |
| Link every SDK into the core extension | Simplifies Python class identity and release coordination, but expands core dependencies and ties unrelated clients to the same wheel. |
| Copy Arrow IPC bytes through Python | Simplifies ownership and allocator boundaries, but duplicates result buffers and loses the in-place data transfer. |
| Reconstruct producer Rust ownership in the consumer | Avoids a callback protocol, but couples independently linked extensions to allocator and Rust type details. |

## Consequences

Core and optional engine distributions need coordinated versions, artifact builds, and installed-wheel checks. The capsule layout and callback ownership form a cross-extension compatibility seam. Changes must preserve validation, single adoption, failure cleanup, and live-view lifetime; unlinking a Unix SHM name does not unmap retained cursor backing.[^src-python-packaging][^src-core-shm][^src-py-arrow-cursor]

The [extension architecture](../architecture/worker/pyo3-ffi-and-batch-dispatch.md#database-extension-boundary), [FFI pattern](../patterns/rust/ffi-and-gil-discipline.md#cross-extension-ownership), and [runtime gotchas](../gotchas/pyo3-and-python-runtime.md#cross-extension-arrow-capsules-and-release) define the implementation rules.

[^src-python-packaging]: `pyproject.toml` — Core distribution and optional engine dependencies.
[^src-db-manifest]: `src/rs/vertebra-db/Cargo.toml` — Shared Rust primitives and optional GCP and PyO3 features.
[^src-db-py-async]: `src/rs/vertebra-db/src/py_async.rs` — Independent completion queues and producer-owned capsule callbacks.
[^src-core-shm]: `src/rs/vertebra-core/src/shm.rs` — C ABI capsule and shared-memory mapping lifetime.
[^src-py-arrow-cursor]: `src/rs/vertebra-py/src/db.rs` — Core ArrowCursor adoption of a producer-owned mapping.
