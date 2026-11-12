---
type: Task
id: "broker-pool-sharing:ch08:t04"
chapter_id: ch08-reclamation-and-acceptance
title: Add per-pool and quarantine metrics
description: Extend BrokerMetrics with quarantine and disposal totals plus per-pool PoolMetrics entries, and make BrokerPlugin.snapshot_metrics emit one DbPoolMetricSample per pool and expose quarantine counters on ZPages.
status: draft
state: pending
priority: P2
verification_strategy: behavior_tdd
depends_on:
  - "broker-pool-sharing:ch08:t02"
read_set:
  - src/py/vertebra/extensions/broker/transport.py
  - src/py/vertebra/extensions/broker/plugin.py
  - src/py/vertebra/telemetry/otel.py
  - src/py/tests/unit/extensions/test_shm_broker.py
  - .agents/bundles/specs/broker-pool-sharing/chapters/ch08-reclamation-and-acceptance/spec.md
files:
  - src/py/vertebra/extensions/broker/transport.py
  - src/py/vertebra/extensions/broker/plugin.py
  - src/py/vertebra/telemetry/otel.py
tests:
  - src/py/tests/unit/extensions/test_shm_broker.py
  - src/py/tests/unit/extensions/test_broker_transport.py
estimated_loc: 160
plan_revision: 3
plan_commit: null
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-12T01:40:00Z"
updated_at: "2026-11-12T01:40:00Z"
commit: null
sources:
  - id: metrics
    resource: src/py/vertebra/extensions/broker/transport.py::BrokerMetrics
    title: Broker metrics struct
  - id: snapshot
    resource: src/py/vertebra/extensions/broker/plugin.py::BrokerPlugin.snapshot_metrics
    title: Metric snapshot producing DbPoolMetricSample
tags:
  - metrics
  - zpages
---

# Task t04: Add Per-Pool and Quarantine Metrics

## Objective & Scope Boundary

1. `transport.py`: add `PoolMetrics` and fields `total_quarantined`, `total_disposed`, `pools: tuple[PoolMetrics, ...]` to `BrokerMetrics`; `SyncSHMBrokerTransport.metrics()` / `AsyncSHMBrokerTransport.metrics()` read per-partition counters from the lease table header (`ch02 / t02` partitions, `ch05 / t01` quarantine counters) [^metrics].
2. `plugin.py`: `snapshot_metrics() -> tuple[DbPoolMetricSample, ...]` returns one sample per pool, each carrying `quarantined` and `total_quarantined`; `_register_zpages` renders a per-pool table with quarantine and disposal columns [^snapshot]. `src/py/vertebra/telemetry/otel.py::DbPoolMetricSample` gains `quarantined: int = 0`, `total_quarantined: int = 0`, `total_disposed: int = 0`, and its recorder emits them as gauges/counters.
3. Tests in `test_shm_broker.py`.

## Bounded Read Manifest

- `transport.py` — `BrokerMetrics`, `metrics()` on SHM transports [^metrics].
- `plugin.py` — `snapshot_metrics`, `_register_zpages`, `install_state` [^snapshot].
- `test_shm_broker.py`.

## Symbol & Signature Contracts

```python
class PoolMetrics(msgspec.Struct, frozen=True, gc=False):
    pool_name: str
    active_leases: int
    quarantined: int
    total_quarantined: int
    total_disposed: int
    total_reclaimed: int

def snapshot_metrics(self) -> tuple[DbPoolMetricSample, ...]: ...
```

## Edge Cases & Error Semantics

- No pools registered → `snapshot_metrics() == ()`.
- Quarantined-but-undisposed connection → `quarantined == 1`, counted in `active_leases` capacity until disposal.
- OpenTelemetry recorder present → `record_sample` called once per pool.

## Public-Seam Test Blueprint

| Test | Input | Expected |
| --- | --- | --- |
| `test_snapshot_metrics_per_pool` | two pool keys, one lease each | two samples with distinct `pool_name`, `active_leases == 1` |
| `test_snapshot_metrics_quarantine_counters` | quarantine one lease, then dispose | before dispose `quarantined == 1`; after `quarantined == 0`, `total_quarantined == 1`, `total_disposed == 1` |
| `test_snapshot_metrics_empty` | no pools | `()` |
| `test_snapshot_metrics_records_each_pool` | fake recorder, two pools | `len(recorder.samples) == 2` |
| `test_zpages_broker_page_lists_quarantine` | render ZPages page | output contains `quarantined` column per pool |

## Verification

- `make dev && uv run --no-sync pytest src/py/tests/unit/extensions/test_shm_broker.py -q`
- `uv run --no-sync mypy src/py/vertebra/extensions/broker`

## Acceptance Criteria

- Per-pool and quarantine counters appear in `snapshot_metrics` and ZPages; matrix tests pass.

## Notes & Discoveries

- At HEAD `snapshot_metrics` returns a single `DbPoolMetricSample`; the only other caller is `src/py/tests/unit/extensions/test_broker_transport.py`, updated in this task for the tuple return.
