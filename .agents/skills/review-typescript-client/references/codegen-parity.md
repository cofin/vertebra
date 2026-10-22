# TypeScript Client Codegen Parity

This reference outlines synchronization rules between Python server schemas and TypeScript client SDK definitions.

---

## Type Parity Rules

1. **Route Response Models**:
   - Updates to Python route definitions and `RouteResponse` schemas must be paired with corresponding TypeScript type updates in `@vertebra/client`.
   - Zod schemas in `zod.py` and TypeScript interface generators in `_typescript.py` must stay in lockstep.

2. **Error Definitions**:
   - Error response shapes emitted by Python handlers must match TypeScript error classes (`ClientError`, `NetworkError`, `ProtocolError`).
