# Design: IPC Protocol Consolidation

**Status**: Draft
**Context**: Unifies IPC patterns across Server (PRD-4), Drivers (PRD-5), Resources (PRD-6), and MCP (PRD-7).

## 1. Transport Layer

**Updated Decision (2026-01-19)**:
- **Driver/Broker data plane**: shared memory ring buffers (no sockets/TCP).
- **Control plane**: in-process or shared memory only; sockets/UDS/TCP are not used.
- **Framing**: shared memory headers + slot layout (see `specs/active/shared-memory-broker/`).

## 2. Payload Formats

We use a hybrid approach based on the specific domain, as defined in PRD-0.

| Domain | Format | Implementation |
|--------|--------|----------------|
| **Server Control** (ASGI) | MessagePack | `msgspec` (Py) / `@msgpack/msgpack` (TS) |
| **Drivers** (Data) | MessagePack + Arrow IPC | `msgspec` (Py) / `@msgpack/msgpack` (TS) |
| **Resources** (Broker + Storage) | MessagePack + Arrow IPC | `msgspec` (Py) / `@msgpack/msgpack` (TS) |
| **MCP** (AI) | Protobuf (post-MVP) | `betterproto` (Py) / `protobuf-es` (TS) |

*Note: We standardize broker + driver payloads on MessagePack for control/small results and Arrow IPC for large results to keep Python decoding fast (msgspec) and avoid bincode requirements.*

## 3. Message Schemas

### 3.1 Server (ASGI)
Defined in `src/ts/src/ipc/asgi.ts`.
- **Frame**: `{ type: string, req_id: string, ...payload }`
- **Streaming**:
    - `http.request` (body chunk)
    - `http.response.body` (body chunk)
    - `http.disconnect` (control)

### 3.2 Drivers (DB-API)
Defined in `docs/protocol.md`.
- **Request**: `{ id: string, driver: string, method: string, params: map }`
- **Response**: `{ id: string, ok: bool, result: any, error: any }`

### 3.3 Resources (Broker)
Defined in `specs/active/shared-memory-broker/`.
- **Acquire**: `{ pool_name: string, worker_id: u64, timeout: f64 }`
- **Release**: `{ connection_handle: u64 }`

### 3.4 MCP (Tool Use)
Defined in PRD-7.
- **ToolCall**: `{ tool_name: string, arguments: map<string, bytes>, request_id: string }`

## 4. Backpressure & Flow Control

**Mechanism**:
- **Shared Memory Ring (PRD-1)**: Natural backpressure when ring is full.
    - Sender blocks or yields until capacity is available.

## 5. Error Propagation

**Standard Error Object**:
```json
{
  "code": "ERROR_CODE",
  "message": "Human readable message",
  "details": { ... }
}
```
- Map Rust `anyhow` / `thiserror` to this structure.
- Map Python exceptions to this structure.
