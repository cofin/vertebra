# Plan: Fix Benchmarks & IPC

## Phase 1: BlackSheep Benchmark Fix
- [ ] **Task: Diagnose BlackSheep**
    - Run BlackSheep benchmark manually with verbose output.
    - Check for errors or timeouts.
- [ ] **Task: Fix BlackSheep App**
    - Correct any implementation issues in `blacksheep_app.py`.

## Phase 2: IPC Notification Fix
- [ ] **Task: Audit Rust Ring Initialization**
    - Check `vertebra-core` for how `Notifier` is attached to `SpscRing`.
    - Update `worker.rs` to attach the notifier to `resp_ring`.
- [ ] **Task: Audit Python Send**
    - Verify `vertebra-py` calls notify on send.
- [ ] **Task: Verify Fix**
    - Run `make bench-servers-quick` and check Vertebra health/performance.
