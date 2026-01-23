# Research: Native State, Subscription, RPC, and Typing Bridge

**Research ID:** `research_20260123_native_bridge`
**Topic:** Native State, Subscription, RPC, and Typing Bridge implementation strategies.
**Date:** 2026-01-23

## Executive Summary

The research confirms that while Vertebra has a strong foundation for "Native" HTTP execution and a basic Type Bridge, it currently lacks the primitives for real-time state synchronization and subscriptions. The proposed "Elite DX" bridge requires extending the existing `TypeBridge` IR to support streams and implementing a new "Native State" primitive that syncs automatically with the frontend.

*   **Current State:** Optimized request/response execution (HTTP), basic RPC decorator, and static type generation.
*   **Missing:** AsyncGenerator/Stream type support, real-time subscription mechanism, client-side reactive state sync.
*   **Opportunity:** Leverage the existing shared memory architecture to deliver "zero-latency" updates to state, superior to standard WebSockets.

## 1. Codebase Analysis

### 1.1 Existing Components
*   **`vertebra.integration.litestar`**: Compiles Litestar routes into optimized runners. This is the execution engine we must hook into.
*   **`vertebra.procedures`**:
    *   `decorator.py`: Registers functions and extracts types. Needs to be updated to detect `AsyncGenerator` return types.
    *   `serialization.py`: Currently handles strictly Request/Response via `msgspec`/`pydantic`. Needs to support streaming serialization.
*   **`vertebra.types`**:
    *   `ir.py`: Defines the Type Intermediate Representation. **Gap:** No `StreamType` or `SubscriptionType` node.
    *   `bridge.py`: Serves types.
*   **`src/ts/src/client`**: Minimal skeleton. Needs a full implementation of a `VertebraClient` that manages a WebSocket (or SHM reader) connection.

### 1.2 "Inertia" & "Litestar-Vite" Context
*   The user references "Inertia" as a concept for handling page state.
*   "Litestar-Vite" provides patterns for:
    *   Asset serving (Vite integration).
    *   Type generation (which Vertebra's `TypeBridge` already mirrors/improves).
    *   **Goal:** Native State should replace the need for "Inertia" by making the backend state directly available and reactive on the frontend, effectively "syncing" the view model.

## 2. Technical Architecture & Gaps

### 2.1 Type Bridge Extensions
The `TypeBridge` must be extended to support `AsyncGenerator[Yield, Send]` or `Stream[T]`.
*   **Python:** `async def subscribe_users() -> AsyncGenerator[User, None]: ...`
*   **TypeScript:** Should generate `subscribe_users(): Subscription<User>` (observable/signal-compatible).

### 2.2 Native State Primitive
Instead of just RPC, we need a `State` primitive:
```python
# Python
class Counter(NativeState):
    count: int = 0
    def increment(self):
        self.count += 1
```
The frontend should receive patches (JSON Patch or similar) when `count` changes.

### 2.3 IPC/Transport
*   **Current:** HTTP (Native Runner) + basic RPC.
*   **Required:** A dedicated `websocket` or `shm` channel for subscriptions. The "Native" runner supports WebSockets (`is_websocket` flag in `reflect_routes`), but the RPC layer doesn't yet utilize it for subscriptions.

## 3. Risk Assessment

### 3.1 Complexity of State Sync
*   **Risk:** Bi-directional state syncing is notoriously difficult (race conditions, consistency).
*   **Mitigation:** Start with **Server-to-Client** sync (Subscription) only. Client actions are RPC calls that mutate server state, which then pushes updates. "Uni-directional Data Flow".

### 3.2 Type Generation Edge Cases
*   **Risk:** `AsyncGenerator` typing can be complex (Yield, Send, Return types).
*   **Mitigation:** Restrict initial support to `AsyncGenerator[YieldType, None]` (Uni-directional streams).

## 4. Recommended Approach

### 4.1 Phase 1: Subscription RPC
1.  Update `TypeBridge` IR to add `SubscriptionType`.
2.  Update `procedure` decorator to detect `AsyncGenerator`.
3.  Implement `StreamingResponse` serialization in `vertebra.procedures.serialization`.
4.  Update TS Client to handle streaming responses (e.g., JSON Lines or SSE-style framing over WS).

### 4.2 Phase 2: Native State (The "Inertia Killer")
1.  Implement `NativeState` base class in Python.
2.  Auto-generate "State Subscription" RPC endpoints for these classes.
3.  Frontend client automatically subscribes to these endpoints to populate a generic Store (compatible with Signals/Stores in Vue/React/Svelte).

### 4.3 Phase 3: Typing Bridge "Vite" Integration
1.  Ensure the `TypeBridge` updates trigger HMR on the frontend (already designed in `bridge.py` but needs verification).

## 5. Prior Art References
*   **tRPC (subscriptions):** Uses `observable` pattern.
*   **Inertia.js:** Uses "page props" refreshed on navigation/reload. Native State improves this by pushing updates *without* reload.
*   **LiveView / Blazor:** Server-side state rendering. We are doing "Server-Side State *Driving*" (Data over wire, not HTML).
