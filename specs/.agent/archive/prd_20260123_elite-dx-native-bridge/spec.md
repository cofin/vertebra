# Native Litestar Integration: Elite DX & Shared Memory Bridge

## Overview
This PRD defines the implementation of a "First-of-its-Kind" Developer Experience (DX) for Python web development. By leveraging Vertebra's polyglot architecture (Python + Rust + Bun), we will create a **Shared Memory Bridge** that connects backend state to frontend components.

This solution delivers two distinct capabilities, prioritized as follows:
1.  **Streaming RPC:** A "tRPC-style" client that supports both request/response AND streaming subscriptions via `AsyncGenerator`.
2.  **Native State:** Real-time, uni-directional state synchronization where Python objects (`NativeState`) automatically push patches to the frontend store.

## Research Reference
- **Research ID:** `research_20260123_native_bridge`
- **Key Finding:** We must extend the Type Bridge to support `AsyncGenerator` for true subscriptions.
- **Key Finding:** "Native State" should be a distinct primitive that pushes updates, rather than just a generic shared memory reader.

## Functional Requirements

### 1. Vertebra Client (RPC + Subscriptions)
**Goal:** Type-safe interaction for interactive islands.
- [ ] **Standard RPC:**
    -   Expose Python functions decorated with `@procedure` as callable JS functions.
    -   Example: `await client.todos.add("Buy milk")`.
- [ ] **Streaming Subscriptions:**
    -   Support `AsyncGenerator` return types in Python.
    -   Generate `Subscription<T>` types in TypeScript.
    -   Example: `client.chat.subscribe(msg => console.log(msg))`.

### 2. Native State (Real-time Sync)
**Goal:** Zero-latency view model synchronization.
- [ ] **NativeState Primitive:**
    -   Python class: `class Counter(NativeState): count: int = 0`.
    -   Auto-generated "Subscription" RPC endpoint for the state.
- [ ] **Frontend Store:**
    -   JS client automatically subscribes to state changes.
    -   Applies JSON Patches (or diffs) to the local store.
    -   Framework adapters (React `useVertebraState`, Svelte Stores) react to these changes.

### 3. Developer Experience (DX)
**Goal:** Seamless integration matching `litestar-vite`.
- [ ] **Asset Serving:** Bun broker serves assets from Vite manifest.
- [ ] **HMR:** Proxying of Vite dev server for hot reloading.

## Non-Functional Requirements
- **Performance:** RPC calls should incur < 5ms overhead.
- **Type Safety:** 100% type coverage for RPC inputs, outputs, AND streams.
- **Stability:** Subscription connections must auto-reconnect.

## Technical Approach
### Recommended Implementation

1.  **RPC & Subscription Layer:**
    -   **Type Bridge:** Extend `ir.py` to add `SubscriptionType` and `StreamType`.
    -   **Decorator:** Update `@procedure` to detect `AsyncGenerator` and register as a streaming endpoint.
    -   **Transport:** Implement `StreamingResponse` serializer for WebSocket/SSE framing.

2.  **State Layer:**
    -   Implement `NativeState` base class in Python using `msgspec.Struct` for schema.
    -   Implement a "Patch Emitter" that sends diffs to subscribers when state is mutated.

## Acceptance Criteria
- [ ] A JS client can subscribe to a Python `AsyncGenerator` and receive a stream of updates.
- [ ] A Python `NativeState` object update triggers a re-render in a React component without a page reload.
