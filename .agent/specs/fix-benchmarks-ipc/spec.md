# Spec: Fix Benchmarks & IPC

## Context
1. **BlackSheep Benchmark Broken:** The `body_1k` and `json` scenarios for BlackSheep are reporting ~21 RPS, which implies a timeout or error fallback loop (similar to what we saw with Vertebra before fixing the binary). `hello` is fine (~19k RPS).
2. **Vertebra IPC Timeout:** The Rust server can send requests to Python, but the return path (Python -> Rust) hangs or isn't notified, causing a 30s timeout in health checks.

## Goals
1. **Fix BlackSheep:** Ensure BlackSheep benchmarks run at expected speeds (~60k+ RPS on this hardware).
2. **Fix IPC Notification:** Ensure Rust `SpscReceiver` wakes up when Python sends a response.

## Technical Analysis

### BlackSheep
- Only `body_1k` and `json` are broken. `hello` works.
- `hello` returns `json`. `json` scenario returns `json`.
- `body_1k` uses `Response` directly.
- Difference: Payload size?
- 21 RPS is suspiciously close to `1 request / timeout`. 
- Likely `uvicorn` (which runs BlackSheep) is hanging on `send()` or `receive()` for larger payloads or specific types?
- Or BlackSheep app implementation has a bug.

### IPC Notification
- Rust: `worker.resp_rx.recv(&mut buffer)` blocks.
- Python: Sends response via `ring.send()`.
- Missing Link: Python must trigger the notification mechanism that Rust is waiting on.
- Rust `SpscRing` uses `Notifier` (eventfd).
- `SpscReceiver` needs to wait on that eventfd.
- Does `SpscReceiver::recv` wait on eventfd?
  - If `SpscRing` was created with a Notifier, `recv` *should* wait.
  - In `worker.rs`: `let notifier = Notifier::new()?; self.notifiers.push(notifier);`.
  - But `SpscRing::initialize`?
  - `let resp_ring = unsafe { SpscRing::initialize(..., 0, 0) }?;`
  - It does NOT take the notifier!
  - `SpscRing` has `set_notifier` method? Or `from_raw_parts`?
  - I need to check `vertebra-core` API.

## Plan
1. **Investigate BlackSheep App:** Check `blacksheep_app.py` for errors.
2. **Fix IPC:**
   - Verify `SpscRing` notification wiring in `worker.rs`.
   - Ensure Python `AsyncRing` triggers notification on send.
