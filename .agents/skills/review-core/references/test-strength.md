# Test Strength Criteria

This reference outlines criteria for strong versus weak tests across Rust, Python, and TypeScript test suites. Every reviewer evaluates tests against these standards.

---

## Strong Tests

1. **Fails without the change**:
   - Reverting the fix or flipping atomic orderings locally makes the test go red.
   - Red/green evidence must be documented for defect fixes.

2. **Observable contracts**:
   - Asserts on wire bytes (HTTP/2 connection preface, gRPC 5-byte framing, SSE `data:` payloads).
   - Asserts on HTTP status codes, headers, and response bodies.
   - Asserts on named struct and dictionary fields rather than opaque boolean checks.
   - Validates memory and interpreter state where relevant (`sys._is_gil_enabled()`, `sys.getrefcount` deltas, `/dev/shm` segment counts).

3. **Concurrency and stress**:
   - Uses two or more real OS threads or processes.
   - Crosses ring buffer capacity boundaries and cursor integer wrap-around.
   - Synchronizes using threading barriers, channels, or wait words, never wall-clock sleeps.
   - Sets timeouts that fail cleanly rather than hanging indefinitely.

4. **Negative and fault injection paths**:
   - Ring saturation and shed behavior (HTTP 503 with `Retry-After`).
   - Client disconnects mid-stream and peer process termination.
   - Payload limit exceedance and malformed frame handling.

5. **Deterministic time**:
   - Vitest fake timers, injected time providers, and mocked clocks.
   - Tokio time pausing in async test fixtures.

6. **Canonical layout**:
   - Tests live in the unit's canonical test module (`src/py/tests/unit/<pkg>/test_<module>.py`, `src/rs/<crate>/tests/*.rs`, `src/ts/vertebra-client/tests/unit/*.test.ts`).

---

## Weak Tests (Flag under TEST-0x)

1. **Mock-echo assertions**:
   - Asserting that a mocked method returned its configured return value.
   - Using `assert x is not None` or `hasattr(x, "attr")` as the sole assertion.

2. **False concurrency**:
   - Single-threaded tests claiming to verify lock-free or sharded structures.
   - Using `time.sleep()` or `std::thread::sleep()` to wait for asynchronous events.

3. **Incidental configuration pinning**:
   - Hardcoding ephemeral port numbers or private internal helper names without an operational contract.
   - Snapshot testing private implementation details.

4. **Blanket skips**:
   - `#[ignore]` or `pytest.mark.skip` without an issue link and rationale.
   - Swallowing errors with `let _ = result;` or `except Exception: pass`.

5. **Resource collision**:
   - Hardcoded fixed SHM segment names shared across test processes.
   - Tests depending on state left behind by previously executed tests.

6. **Non-canonical file naming**:
   - Naming files with suffixes like `test_*_fix.py`, `*remediation*`, `*audit*`, or stage numbers.
