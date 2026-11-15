---
type: Task
id: native-valkey-driver:ch02-rust-clients-topologies-and-streams:t04
flow_id: native-valkey-driver
chapter_id: ch02-rust-clients-topologies-and-streams
title: Expose Sync/Async Valkey Clients, Pipelines, Pub/Sub Listeners, and Streams
description: Implement PyO3 SyncValkeyClient, AsyncValkeyClient (_vertebra_valkey_completion_drainer), SyncValkeyPipeline, AsyncValkeyPipeline, SyncValkeyListener, AsyncValkeyListener, and Streams in src/rs/vertebra-valkey.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: public_seam_tdd
plan_commit: 473e1f2e
depends_on:
  - native-valkey-driver:ch02-rust-clients-topologies-and-streams:t03
files:
  - src/rs/vertebra-valkey/src/client.rs
  - src/rs/vertebra-valkey/src/pubsub.rs
  - src/rs/vertebra-valkey/src/streams.rs
  - src/rs/vertebra-valkey/src/lib.rs
  - src/py/vertebra/_valkey.pyi
  - src/rs/vertebra-valkey/tests/valkey_client.rs
  - src/py/tests/unit/test_freethreaded_gil.py
tests:
  - src/rs/vertebra-valkey/tests/valkey_client.rs
  - src/py/tests/unit/test_freethreaded_gil.py
read_set:
  - src/rs/vertebra-db/src/py_async.rs
  - src/py/vertebra/_oracle.pyi
estimated_loc: 240
tags:
  - rust
  - pyo3
  - valkey
  - pubsub
  - streams
  - async
sources:
  - id: completion-ring
    resource: src/rs/vertebra-db/src/py_async.rs::DbCompletionDrainer
    title: DbCompletionDrainer and spawn_rust_db_task_with_attr_and_cancel async bridge
  - id: oracle-pyi
    resource: src/py/vertebra/_oracle.pyi
    title: Sibling native extension .pyi stub
verification_strategy: public_seam_tdd
---

# Task `t04`: Expose Sync/Async Valkey Clients, Pipelines, Pub/Sub Listeners, and Streams

## Objective & Scope Boundary

Implement PyO3 `SyncValkeyClient` (`py.detach`), `AsyncValkeyClient` (`_vertebra_valkey_completion_drainer` `DbCompletionDrainer`), `SyncValkeyPipeline`, `AsyncValkeyPipeline`, `SyncValkeyListener`, `AsyncValkeyListener` (`subscribe`, `psubscribe`, `ssubscribe`, `unsubscribe`, `next_message`), and Streams (`xgroup_create`, `xadd`, `xreadgroup`, `xack`, `xautoclaim`) in `src/rs/vertebra-valkey/src/{client,pubsub,streams,lib}.rs` and `src/py/vertebra/_valkey.pyi` per [`../spec.md` §2](../spec.md) [^completion-ring] [^oracle-pyi].

## Bounded Read Manifest

- `src/rs/vertebra-db/src/py_async.rs` — `DbCompletionDrainer` event-loop wakeup drainer.
- `src/py/vertebra/_oracle.pyi` — Canonical PyO3 stub declarations.

## Contracts & Edge Cases

- `SyncValkeyClient` and `AsyncValkeyClient` expose KV (`get`, `set`, `delete`, `exists`, `expire`, `pexpire`, `ttl`, `pttl`, `incrby`, `decrby`, `mget`, `mset`, `scan_unlink_prefix`), Valkey 9.0 hash-field TTL (`hexpire`, `hpexpire`, `hexpireat`, `hpexpireat`, `httl`, `hpttl`, `hpersist`), Lua (`eval`, `evalsha`, `script_load`), Streams (`xgroup_create`, `xadd`, `xreadgroup`, `xack`, `xautoclaim`), Pub/Sub (`publish`, `listener`), and raw `execute_command`.
- Register `_vertebra_valkey_completion_drainer` once per Python `asyncio` event loop via `loop.add_reader(wakeup_fd, ...)`.
- **Edge Case**: Release the GIL via `py.detach(...)` around every blocking socket read/write in `SyncValkeyClient`, `SyncValkeyPipeline`, and `SyncValkeyListener`.

## Test Matrix

- `cargo test -p vertebra-valkey` and `mypy.stubtest vertebra._valkey --concise` pass cleanly.

## Verification

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-valkey && make dev && uv run --no-sync python -m mypy.stubtest vertebra._valkey --concise
```

## Acceptance Criteria

- [ ] `SyncValkeyClient`, `AsyncValkeyClient`, pipelines, listeners, Valkey 9.0 hash-field TTL, and Streams pass Rust tests and `mypy.stubtest`.
- [ ] Zero regressions in the fast verification gate.

[^completion-ring]: `src/rs/vertebra-db/src/py_async.rs::DbCompletionDrainer` — DbCompletionDrainer and spawn_rust_db_task_with_attr_and_cancel async bridge.
[^oracle-pyi]: `src/py/vertebra/_oracle.pyi` — Sibling native extension .pyi stub.
