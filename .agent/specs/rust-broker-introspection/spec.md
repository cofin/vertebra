# Rust Broker Introspection

## Overview

Enables the Vertebra CLI to query the running Rust Broker for internal state, statistics, and health status via a dedicated IPC control channel.

## Context (Migration)
Formerly `cli-broker-status-ipc`. The previous design queried the Bun Broker. The new Rust Broker requires a dedicated `Op::Status` handler on the Control Ring.

## Functional Requirements

### Must Have
- [ ] **IPC Status Opcode:** Add `Op::Status` to the shared memory control protocol.
- [ ] **Broker Handler:** `vertebra-server` handles this opcode and returns a serialized payload (Bincode/MsgPack).
- [ ] **CLI Integration:** `vertebra status` command sends this opcode and formats the response.
- [ ] **Metrics:**
    - [ ] Uptime
    - [ ] Active Connections
    - [ ] Worker Pool Health
    - [ ] Ring Buffer Saturation

## Technical Approach
- Define `StatusRequest` and `StatusResponse` structs in `vertebra-core`.
- Implement handler in `vertebra-server`'s control loop.
- Expose via `_vertebra.get_status()` in Python bindings.
