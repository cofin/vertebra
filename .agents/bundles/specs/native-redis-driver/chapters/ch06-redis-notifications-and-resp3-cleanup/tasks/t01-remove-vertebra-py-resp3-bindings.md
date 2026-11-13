---
type: Task
id: "native-redis-driver:ch06:t01"
flow_id: native-redis-driver
chapter_id: ch06-redis-notifications-and-resp3-cleanup
title: "Remove Legacy RESP3 Helper Bindings from vertebra-py and _vertebra.pyi"
description: "Delete encode_resp_command, encode_hello3_handshake, crc16_xmodem, redis_hash_slot, and parse_cluster_redirect from _vertebra."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "native-redis-driver:ch04:t03"
files:
  - src/rs/vertebra-py/src/db.rs
  - src/py/vertebra/_vertebra.pyi
tests:
  - src/py/tests/unit/db/test_redis_valkey_resp3.py
read_set:
  - src/rs/vertebra-py/src/db.rs
  - src/py/vertebra/_vertebra.pyi
estimated_loc: 80
tags:
  - pyo3
  - resp3
  - debloat
sources:
  - id: resp3-bindings
    resource: src/rs/vertebra-py/src/db.rs::redis_hash_slot
    title: "Legacy RESP3 helper bindings on vertebra._vertebra"
  - id: vertebra-pyi
    resource: src/py/vertebra/_vertebra.pyi
    title: "Core vertebra._vertebra stub declarations"
---

# Task `t01`: Remove Legacy RESP3 Helper Bindings from vertebra-py and _vertebra.pyi

## Objective & Scope Boundary

Remove all 5 RESP3 bindings from `src/rs/vertebra-py/src/db.rs` and `src/py/vertebra/_vertebra.pyi` per [`../spec.md` §2](../spec.md) [^resp3-bindings] [^vertebra-pyi].

## Bounded Read Manifest

- `src/rs/vertebra-py/src/db.rs` — RESP3 imports, `#[pyfunction]` wrappers, and module registrations.
- `src/py/vertebra/_vertebra.pyi` — RESP3 stub declarations.

## Contracts & Edge Cases

- Keep `vertebra._redis` as the sole owner of `crc16_xmodem`, `redis_hash_slot`, and `parse_cluster_redirect`.
- **Edge Case**: `mypy.stubtest vertebra._vertebra` must pass immediately after rebuild.

## Test Matrix

- Assert `not hasattr(vertebra._vertebra, "encode_resp_command")` and run `mypy.stubtest vertebra._vertebra`.

## Verification

```bash
make dev && uv run --no-sync python -m mypy.stubtest vertebra._vertebra --concise
```

## Acceptance Criteria

- [ ] `vertebra._vertebra` exports zero RESP3 functions and passes `stubtest`.
- [ ] Zero regressions in the chapter fast verification gate.

[^resp3-bindings]: `src/rs/vertebra-py/src/db.rs::redis_hash_slot` — Legacy RESP3 helper bindings on vertebra._vertebra.
[^vertebra-pyi]: `src/py/vertebra/_vertebra.pyi` — Core vertebra._vertebra stub declarations.
