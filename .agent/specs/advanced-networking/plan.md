# Plan: Advanced Networking

## Phase 1: HTTP/2 & gRPC Foundation
- [ ] **Task: Enable HTTP/2 in Hyper**
    - Update `vertebra-server` to configure Hyper for HTTP/2.
    - Add `h2` crate dependency if needed for lower-level control.
- [ ] **Task: gRPC Routing**
    - Implement a detector for `application/grpc`.
    - Create a specialized IPC message type for gRPC frames (if needed) or just pass raw bytes.

## Phase 2: HTTP/3 (QUIC)
- [ ] **Task: Add `h3` / `quinn`**
    - This is complex. Start with a basic UDP listener and QUIC handshake.
    - *Decision:* Mark as experimental/optional for MVP.

## Phase 3: SSE & Real-time
- [ ] **Task: Implement SSE Handler**
    - Create a Rust handler that keeps the connection open.
    - Subscribe to a broadcast channel from the Python workers.

## Phase 4: Advanced Asset Delivery
- [ ] **Task: Manifest Parser**
    - Implement logic to read `dist/manifest.json`.
    - Map requested paths to hashed filenames.
- [ ] **Task: Early Hints**
    - When serving `index.html`, look up imports in manifest.
    - Send 103 Early Hints response before the body.
