---
type: Task
id: "runtime-perf-remediation:ch04:t01"
chapter_id: ch04-data-benchmarks-and-verification
title: Apply empirical 1w Tokio and WSGI pool tuning
description: Update 1w GIL Embedded Tokio default to available_cpus.clamp(1, 2) and tune WsgiExecutor stall threshold (2000 us), lazy thread count (64..128), and single now_ns() per inline item.
status: stable
state: completed
priority: P0
verification_strategy: regression_tdd
worksheet_format: compact
depends_on: []
read_set:
  - .agents/bundles/specs/runtime-perf-remediation/chapters/ch04-data-benchmarks-and-verification/spec.md
  - src/rs/vertebra-http/src/config.rs
  - src/rs/vertebra-py/src/wsgi/pool.rs
  - src/py/tests/unit/server/test_wsgi_adaptive_pool.py
files:
  - src/rs/vertebra-http/src/config.rs
  - src/rs/vertebra-py/src/wsgi/pool.rs
tests:
  - src/rs/vertebra-http/src/config.rs
  - src/rs/vertebra-py/src/wsgi/pool.rs
  - src/py/tests/unit/server/test_wsgi_adaptive_pool.py
estimated_loc: 55
plan_revision: 3
plan_commit: "9dfbd408"
commit: "5b1eb80f"
sources:
  - id: src-ch04-spec
    resource: .agents/bundles/specs/runtime-perf-remediation/chapters/ch04-data-benchmarks-and-verification/spec.md
    title: Chapter 4 empirical A/B probe results and runtime tuning contracts
  - id: src-http-config
    resource: src/rs/vertebra-http/src/config.rs::ServerConfig::apply_runtime_thread_env
    title: Embedded Tokio worker thread selection
  - id: src-wsgi-pool
    resource: src/rs/vertebra-py/src/wsgi/pool.rs::WsgiExecutor
    title: Adaptive WSGI executor configuration and inline loop
tags:
  - performance
  - embedded
  - wsgi
  - tuning
---

# Apply Empirical 1w Tokio and WSGI Pool Tuning

## Objective & Scope Boundary

Lock in the empirically verified runtime defaults identified by release A/B benchmarking on commit `9dfbd408` [^src-ch04-spec].

Update only `ServerConfig::apply_runtime_thread_env` in `src/rs/vertebra-http/src/config.rs` and `WsgiPoolConfig`/`WsgiExecutor::dispatch_batch` in `src/rs/vertebra-py/src/wsgi/pool.rs`, plus matching unit tests [^src-http-config] [^src-wsgi-pool].

## Bounded Read Manifest

- `.agents/bundles/specs/runtime-perf-remediation/chapters/ch04-data-benchmarks-and-verification/spec.md`
- `src/rs/vertebra-http/src/config.rs`
- `src/rs/vertebra-py/src/wsgi/pool.rs`
- `src/py/tests/unit/server/test_wsgi_adaptive_pool.py`

## Contracts & Edge Cases

- `src/rs/vertebra-http/src/config.rs::ServerConfig::apply_runtime_thread_env`:
  - When `VERTEBRA_TOKIO_THREADS` is unset and `self.topology == Topology::Embedded && !free_threaded`, set `self.worker_threads = available_cpus.clamp(1, 2)` (2 on `>= 2`-core hosts, 1 on 1-core hosts).
  - Preserve explicit `VERTEBRA_TOKIO_THREADS` overrides (including `EmbeddedSupervisor` multi-worker child `VERTEBRA_TOKIO_THREADS=1`), free-threaded sizing, and `RequestBatcher` `shard_count = 1` for GIL Embedded.
- `src/rs/vertebra-py/src/wsgi/pool.rs`:
  - Raise `DEFAULT_STALL_THRESHOLD_US` from `250` to `2_000` (`2 ms`) so GC or OS scheduling jitter on zero-I/O routes does not trigger the 2-second concurrent fallback window.
  - Raise `default_threads` in `WsgiPoolConfig::from_lookup` from `cpus.saturating_mul(4).clamp(32, 128)` to `cpus.saturating_mul(12).clamp(64, 128)` so a 1-worker WSGI process handles `c=128` blocking I/O without capping at 48 threads (`4,800 RPS`), while keeping thread spawning lazy.
  - Carry `item_end` forward as the next `item_start` inside `WsgiExecutor::dispatch_batch`'s `Auto` inline loop and reuse the already-computed `now` as `start` in the `total == 1` branch so `now_ns()` is called once per request instead of twice.
  - Preserve `VERTEBRA_WSGI_MODE`, `VERTEBRA_WSGI_THREADS`, and `VERTEBRA_WSGI_STALL_US` overrides and free-threaded `Concurrent` default.

## Test Matrix

| Case | Setup | Expected Behavior |
| :--- | :--- | :--- |
| 1w GIL Embedded on multi-core host | `apply_runtime_thread_env(12, false, unset)` | `worker_threads == 2`; on 1-core host `worker_threads == 1` |
| Explicit `VERTEBRA_TOKIO_THREADS=1` | `apply_runtime_thread_env(12, false, "1")` | `worker_threads == 1` (multi-worker `EmbeddedSupervisor` unaffected) |
| `WsgiPoolConfig` defaults | `from_lookup(false, 12, unset)` / `from_lookup(false, 2, unset)` | `stall_threshold_us == 2_000`; `threads == 128` on 12c, `64` on 2c |
| Inline `now_ns()` carry-forward | Multi-item fast batch followed by blocking item | Fast batch stays inline without triggering concurrent window; blocking item triggers handoff past `stall_threshold_us` |

## Verification

```bash
export PYO3_PYTHON="$(realpath .venv/bin/python)"
export LD_LIBRARY_PATH="$("$PYO3_PYTHON" -c 'import sysconfig; print(sysconfig.get_config_var("LIBDIR") or "")')"
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-http --lib config::tests
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-py wsgi::pool::tests -- --test-threads=8
UV_OFFLINE=1 make dev-ext EXT=core
uv run --no-sync pytest src/py/tests/unit/server/test_wsgi_adaptive_pool.py
```

## Acceptance Criteria

- `config::tests`, `wsgi::pool::tests`, and `test_wsgi_adaptive_pool.py` pass with zero Clippy warnings.
- Multi-worker `EmbeddedSupervisor` child processes remain pinned to `1` Tokio thread via explicit `VERTEBRA_TOKIO_THREADS=1`.
- Zero-I/O WSGI batches spawn zero OS threads and avoid false-positive stall handoffs under normal jitter.

[^src-ch04-spec]: `.agents/bundles/specs/runtime-perf-remediation/chapters/ch04-data-benchmarks-and-verification/spec.md` — Chapter 4 empirical A/B probe results and runtime tuning contracts.
[^src-http-config]: `src/rs/vertebra-http/src/config.rs::ServerConfig::apply_runtime_thread_env` — Embedded Tokio worker thread selection.
[^src-wsgi-pool]: `src/rs/vertebra-py/src/wsgi/pool.rs::WsgiExecutor` — Adaptive WSGI executor configuration and inline loop.
