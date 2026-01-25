## [2026-01-25 20:20] - Phase 7 Task 7.2: Zero-Copy Header Enforcement

- **Implemented:** Enforced `RequestHeader` usage in `AsgiWorker` (Python) and `IpcClient` (TypeScript).
- **Files changed:**
  - `src/py/vertebra/server/worker.py`: Added header parsing and correlation ID propagation.
  - `src/ts/src/ipc/client.ts`: Removed legacy "msgpack" protocol support; default is now "framed".
  - `src/py/tests/integration/server/test_asgi_worker.py`: Updated to use headers in IPC exchanges.
  - `src/py/tests/unit/server/test_ipc_headers.py`: New unit tests for worker header handling.
- **Learnings:**
  - **Gotcha:** `AsgiWorker` was silently assuming pure MsgPack payloads. Changing this required careful updates to all integration tests that manually write to rings.
  - **Pattern:** When mocking `SpscRing` in tests, always use `RequestHeader` + `payload` if the consumer expects it.
  - **Protocol:** The "msgpack" protocol (legacy) in `vertebra-js` was the default; this was unsafe as it bypassed the 16-byte header requirement for routing.
