# Research: Federated Brokers with Arrow Flight

## Executive Summary

-   **Goal:** Enable multi-staged sync and fan-out across Vertebra brokers on different servers.
-   **Solution:** Introduce a **Federation Layer** using **Apache Arrow Flight** (gRPC) for high-performance data transport and a gossip-style protocol for `StateHub` synchronization.
-   **Architecture:** A "Gateway" pattern where each Broker hosts an embedded Flight Server (implemented in Rust) to handle cross-node traffic, while maintaining the existing Shared Memory architecture for local workers.
-   **Feasibility:** High. The existing `StateHub` CRDT-like structure is ready for replication. Rust's `arrow-flight` crate provides the necessary transport primitives.

## Codebase Analysis

### 1. Broker Architecture (`src/ts/src/broker/`)
-   **Event Loop:** The Broker (`server.ts`) runs a hybrid busy-spin/backoff loop optimized for local SHM polling.
-   **StateHub (`state-hub.ts`):** Manages state via `StateDelta` (opcode-based updates) with timestamps. This is effectively a CRDT (Conflict-free Replicated Data Type) foundation, making it ideal for distributed synchronization.
-   **Networking:** Currently limited to local IPC (Unix sockets/SHM). No TCP listeners exist in the data plane.

### 2. Rust Core (`src/rs/vertebra-core/`)
-   **IPC:** `ShmRing` and `ShmRegion` are the core primitives.
-   **Concurrency:** `tokio` is present in the workspace. `arrow` crate is version 53.0.
-   **Missing:** `arrow-flight` and `tonic` (gRPC) dependencies are not yet in the workspace.

## Library Documentation: Apache Arrow Flight

### **What is it?**
A framework for high-performance transport of large datasets over network interfaces, built on gRPC.

### **Key Concepts:**
-   **FlightServer:** Responds to `GetFlightInfo` (metadata) and `DoGet` (stream data).
-   **DoPut:** Client pushes data to server (useful for fan-out/ingest).
-   **DoAction:** Execute commands (useful for control plane sync).

### **Rust vs. Node/Bun Implementation:**
-   **Rust (`arrow-flight` crate):**
    -   *Pros:* Zero-copy integration with `vertebra-core` SHM regions; rigorous thread safety; high performance.
    -   *Cons:* Requires bridging to Bun Broker.
-   **Node/Bun (`apache-arrow`):**
    -   *Pros:* Easier integration with existing Broker logic.
    -   *Cons:* "Zero-copy" from JS to underlying C++ Arrow structures can be tricky; slightly higher overhead.
-   **Verdict:** **Rust Implementation.** The Broker already delegates heavy IPC work to Rust. The Flight Server should be a Rust task managed by `vertebra-js`.

## Prior Art & Patterns

### 1. Distributed Log / Event Bus
-   **Pattern:** Brokers subscribe to topics. When a message is published, it is fanned out to local workers AND forwarded to remote subscribed brokers.
-   **Vertebra Fit:** The `StateHub` is the registry. A "Federation Driver" subscribes to `StateHub` changes and broadcasts them.

### 2. Data Mesh / Object Storage Proxy
-   **Pattern:** A worker requests a dataset. If it's not local, the Broker fetches it via Flight from a remote peer.
-   **Vertebra Fit:** `ArrowClient` in Python could transparently fetch from a remote Flight endpoint if the Broker acts as a proxy.

## Risk Assessment

### Technical Risks
| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| **Thread Safety** | High | Critical | The Rust Flight Server runs in a Tokio thread pool, accessing SHM regions that are also accessed by the main thread/Python. **Must use `Arc<RwLock<...>>` or specialized lock-free structures safe for cross-process AND cross-thread access.** |
| **Serialization Overhead** | Medium | High | While Arrow is zero-copy "on wire" (mostly), moving from SHM to Network requires reading the SHM. **Ensure `write_arrow_ipc` implementation is efficient.** |
| **Discovery Complexity** | Medium | Medium | How do brokers find each other? **Start with static configuration (list of peers), evolve to dynamic discovery later.** |

### Integration Risks
-   **Python Client Compatibility:** Existing Python clients expect Unix sockets. The Broker must hide the complexity. The Python client shouldn't know if data came from localhost or a remote server.

## Recommended Approach

1.  **New Crate:** Create `vertebra-flight` in the Rust workspace.
2.  **Dependencies:** Add `arrow-flight`, `tonic`, `prost` to `Cargo.toml`.
3.  **Flight Server:** Implement a Flight Server that can:
    -   Serve Arrow streams from SHM regions (`DoGet`).
    -   Accept Arrow streams and write to SHM (`DoPut`).
    -   Expose `StateHub` sync endpoints (`DoAction`).
4.  **Broker Integration:**
    -   Add `startFlightServer(port: number)` to `vertebra-js`.
    -   Broker starts this server on startup (optional config).
    -   Broker subscribes to `StateHub`. On change, it pushes updates to configured peers via Flight Client (Rust side).

## Open Questions
-   **Auth:** Should we implementation mTLS or simple Token auth for the MVP? (Suggestion: Token/Shared Secret for MVP).
-   **Consistency:** Eventual consistency is implied. Is strict ordering required for any `StateHub` fields?
