# Native Litestar Integration: Implementation Plan

## Methodology
This plan follows the Vertebra Workflow:
-   **Plan-Driven:** All tasks are tracked here.
-   **TDD:** Tests (Red) -> Implement (Green) -> Refactor.
-   **Phase Checkpoints:** Strict verification before moving to the next phase.

## Phase 1: Vertebra Client (RPC & Subscriptions)
**Goal:** Connect the existing `@procedure` system to the frontend with Streaming support and full type safety.

- [ ] Task: Extend Type Bridge for Streams
    -   Update `src/py/vertebra/types/ir.py` to add `SubscriptionType` and `StreamType`.
    -   Update `extractor.py` to handle `AsyncGenerator` and `Iterator` hints.
    -   Update `typescript.py` to generate `Subscription<T>` interfaces.
- [ ] Task: Implement Streaming Serializer
    -   Create `vertebra.procedures.serialization.StreamingResponse`.
    -   Implement `ndjson` (Newline Delimited JSON) or SSE-style framing for streams.
- [ ] Task: Update `@procedure` Decorator
    -   Detect `AsyncGenerator` return types.
    -   Register as `type="subscription"` in metadata.
- [ ] Task: Implement RPC Client (JS)
    -   Create `vertebra-client` package.
    -   Implement `createClient<AppRouter>()` with WebSocket transport.
    -   Implement `subscribe()` method that returns an Observable-like object.
    -   Ensure client types fully infer return values from the Type Bridge.
- [ ] Task: Checkpoint - Verify RPC & Streams
    -   Verify standard RPC calls work.
    -   Verify a JS client can receive a stream of numbers from a Python generator.
- [ ] Task: Flow - User Manual Verification 'Vertebra Client (RPC)' (Protocol in workflow.md)

## Phase 2: Native State (Real-time Sync)
**Goal:** Implement the "Native State" primitive for zero-latency sync using JSON Patch.

- [ ] Task: Implement `NativeState` Base Class
    -   Create `src/py/vertebra/state.py`.
    -   Implement `NativeState` (subclass of `msgspec.Struct`).
    -   Implement automatic `json-patch` generation on mutation (or explicit `.commit()`/`.push()`).
- [ ] Task: Auto-Generate Subscription Endpoints
    -   Create a utility to automatically register a `@procedure` subscription for a `NativeState` instance.
    -   Ensure the stream sends `{ type: 'init', state: ... }` then `{ type: 'patch', ops: ... }`.
- [ ] Task: Implement Frontend State Hooks
    -   React: `useVertebraState(key)` which consumes the patch stream and updates a local store.
    -   Vue/Svelte/Solid equivalents (using Signals/Stores).
- [ ] Task: Checkpoint - Verify State Sync
    -   Verify Python state mutations trigger frontend updates via patches.
- [ ] Task: Flow - User Manual Verification 'Vertebra Client (State Sync)' (Protocol in workflow.md)

## Phase 3: Development Experience (Litestar-Vite Patterns)
**Goal:** Apply `litestar-vite` lessons for seamless asset serving, HMR, and manifest handling.

- [ ] Task: Implement Asset Serving (Bun Broker)
    -   Update Broker to read `manifest.json` (Vite build output) to map entry points.
    -   Update Broker to serve static assets from the configured `dist/` or `public/` directory.
    -   Implement HMR proxying for the Vite dev server (matching `litestar-vite`'s dev mode).
- [ ] Task: HMR for Type Bridge
    -   Ensure `TypeBridge` updates trigger a client-side reload or HMR event.
- [ ] Task: Checkpoint - Verify DX
    -   Verify frontend assets load correctly in both Dev (HMR) and Prod (Manifest) modes.
