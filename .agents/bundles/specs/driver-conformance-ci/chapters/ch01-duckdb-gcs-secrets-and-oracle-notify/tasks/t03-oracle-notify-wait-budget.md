---
type: Task
id: "driver-conformance-ci:ch01:t03"
flow_id: driver-conformance-ci
chapter_id: ch01-duckdb-gcs-secrets-and-oracle-notify
title: "Enforce OracleNotifyConfig.wait_timeout_seconds idle session liveness in vertebra-oracle"
description: "Track idle dequeue elapsed time across 1-second AQ slices and run a SELECT 1 FROM DUAL liveness check when wait_timeout_seconds elapses."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on: []
files:
  - src/rs/vertebra-oracle/src/notify.rs
tests:
  - src/py/tests/unit/db/test_oracle_adapter.py
read_set:
  - src/rs/vertebra-oracle/src/notify.rs
  - src/py/vertebra/db/adapters/oracle/notify.py
  - src/py/tests/unit/db/test_oracle_adapter.py
estimated_loc: 145
tags:
  - rust
  - oracle
  - notifications
  - liveness
sources:
  - id: oracle-notify-rs
    resource: src/rs/vertebra-oracle/src/notify.rs::run_live_aq_worker
    title: "Oracle AQ listener worker loop currently discarding wait_timeout_seconds"
  - id: oracle-notify-py
    resource: src/py/vertebra/db/adapters/oracle/notify.py::OracleNotifyConfig
    title: "Python OracleNotifyConfig.wait_timeout_seconds configuration seam"
---

# Task `t03`: Enforce `OracleNotifyConfig.wait_timeout_seconds` Idle Session Liveness in `vertebra-oracle`

## Objective & Scope Boundary

Replace `let _ = wait_timeout_seconds.max(1);` in `src/rs/vertebra-oracle/src/notify.rs` with bounded `1s` dequeue slicing and an idle `SELECT 1 FROM DUAL` session liveness probe whenever idle duration reaches `wait_timeout_seconds.max(1)` per [`../spec.md` §2](../spec.md) [^oracle-notify-rs] [^oracle-notify-py]. Out of scope: changing `OracleNotifyConfig` Python field names or blocking `close()` for more than `1s`.

## Bounded Read Manifest

- `src/rs/vertebra-oracle/src/notify.rs` — AQ worker loop, shutdown signaling, and reconnect handling.
- `src/py/vertebra/db/adapters/oracle/notify.py` and `src/py/tests/unit/db/test_oracle_adapter.py` — `wait_timeout_seconds` forwarding and unit coverage.

## Contracts & Edge Cases

- Keep `DBMS_AQ.DEQUEUE` per-call wait at `1` second (`Duration::from_secs(1)`) so `stop_flag` / `close()` completes promptly even when `wait_timeout_seconds` is `30` or `60`.
- Maintain `idle_since: Instant`, resetting it whenever a message is dequeued or a liveness check succeeds; when `idle_since.elapsed() >= Duration::from_secs(wait_timeout_seconds.max(1))`, execute `SELECT 1 FROM DUAL` (abstracted behind an internal trait so Rust `#[cfg(test)]` tests verify exact ping counts and error/reconnect handling deterministically).
- If the liveness probe fails with a broken-session error, trigger the existing reconnect/error path rather than spinning silently on an orphaned TCP socket.

## Test Matrix

- Rust `notify::tests` unit cases proving: (a) `wait_timeout_seconds=2` vs `wait_timeout_seconds=5` changes exact liveness probe frequency across scripted empty dequeue slices, (b) delivered messages reset the idle timer without probing, (c) `close()` wakes within one slice, and (d) failed liveness ping surfaces reconnect/closure; plus Python `OracleNotifyConfig` validation in `test_oracle_adapter.py`.

## Verification

```bash
UV_NO_SYNC=1 PYO3_PYTHON="$PWD/.venv/bin/python" cargo test --manifest-path src/rs/Cargo.toml -p vertebra-oracle notify:: && cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-oracle --all-targets -- -D warnings && uv run --no-sync pytest src/py/tests/unit/db/test_oracle_adapter.py -q
```

## Acceptance Criteria

- [ ] `wait_timeout_seconds` actively governs idle AQ session liveness probes while keeping `close()` bounded to a `1s` slice.
- [ ] Rust `notify::` unit tests, Clippy (`-D warnings`), and `test_oracle_adapter.py` pass with zero warnings.

[^oracle-notify-rs]: `src/rs/vertebra-oracle/src/notify.rs::run_live_aq_worker` — Oracle AQ listener worker loop currently discarding `wait_timeout_seconds`.
[^oracle-notify-py]: `src/py/vertebra/db/adapters/oracle/notify.py::OracleNotifyConfig` — Python `OracleNotifyConfig.wait_timeout_seconds` configuration seam.
