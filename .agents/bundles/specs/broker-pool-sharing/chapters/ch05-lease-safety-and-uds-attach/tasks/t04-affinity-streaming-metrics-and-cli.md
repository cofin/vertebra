---
type: Task
id: "broker-pool-sharing:ch05:t04"
flow_id: broker-pool-sharing
chapter_id: ch05-lease-safety-and-uds-attach
title: "Session Affinity, VTBARROW Streaming, PoolMetrics, and CLI Wiring"
description: "Pin leases across session state, stream VTBARROW/chunked results with cancel(), expose PoolMetrics, and register vtb broker serve."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "broker-pool-sharing:ch05:t03"
files:
  - src/py/vertebra/extensions/broker/transport.py
  - src/py/vertebra/extensions/broker/plugin.py
  - src/py/vertebra/cli/__init__.py
tests:
  - src/py/tests/unit/extensions/test_shm_broker.py
  - src/py/tests/unit/extensions/test_broker_transport.py
read_set:
  - src/py/vertebra/extensions/broker/transport.py
  - src/py/vertebra/extensions/broker/plugin.py
  - src/py/vertebra/extensions/broker/host.py
  - src/py/vertebra/cli/__init__.py
estimated_loc: 230
tags:
  - broker
  - streaming
  - metrics
  - cli
sources:
  - id: broker-transport
    resource: src/py/vertebra/extensions/broker/transport.py::_BaseSHMBrokerTransport
    title: "Python SHM broker transports and lease objects"
  - id: broker-plugin
    resource: src/py/vertebra/extensions/broker/plugin.py::BrokerPlugin
    title: "BrokerPlugin lifecycle and metric snapshot integration"
  - id: vtb-cli
    resource: src/py/vertebra/cli/__init__.py::cli
    title: "Root vtb Click command group"
---

# Task `t04`: Session Affinity, VTBARROW Streaming, PoolMetrics, and CLI Wiring

## Objective & Scope Boundary

Add session affinity pinning, `VTBARROW` / chunked `FetchChunk` + `Cancel` routing, per-pool `PoolMetrics` (`total_quarantined`, `total_disposed`, `total_reclaimed`), and `cli.add_command` registration for `vtb broker` per [`../spec.md` §2](../spec.md) [^broker-transport] [^broker-plugin] [^vtb-cli].

## Bounded Read Manifest

- `src/py/vertebra/extensions/broker/transport.py` — `SyncSHMBrokerTransport`, `AsyncSHMBrokerTransport`, and `BrokerMetrics`.
- `src/py/vertebra/extensions/broker/plugin.py` — `BrokerPlugin.snapshot_metrics`.
- `src/py/vertebra/extensions/broker/host.py` — `BrokerHost` and `broker` Click command group from `t03`.
- `src/py/vertebra/cli/__init__.py` — Root `cli.add_command` registrations.

## Contracts & Edge Cases

- Pin lease coordinates across `Begin`..`Commit`/`Rollback`, temp-table/session settings, and `FetchChunk` cursors; propagate `Cancel` op frames.
- Extend `BrokerMetrics` with `total_quarantined`, `total_disposed`, and `pools: tuple[PoolMetrics, ...]` surfaced by `BrokerPlugin.snapshot_metrics`.
- **Edge Case**: Cancelling an in-flight `FetchChunk` stream quarantines the lease if backend state becomes uncertain.
- **Edge Case**: `vtb broker --help` loads lazily without eager engine imports.

## Test Matrix

- Verify multi-statement transaction stays pinned to identical `(slot_index, generation)` and `VTBARROW` cursor decodes zero-copy.
- Verify two registered partitions emit distinct `PoolMetrics` entries summing to `BrokerMetrics` totals.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/extensions/test_shm_broker.py src/py/tests/unit/extensions/test_broker_transport.py -q && uv run --no-sync mypy src/py/vertebra/extensions/broker src/py/vertebra/cli
```

## Acceptance Criteria

- [ ] Session affinity, `VTBARROW` streaming, `PoolMetrics`, and `vtb broker serve` are wired and type-checked.
- [ ] Zero regressions in the chapter fast verification gate.

[^broker-transport]: `src/py/vertebra/extensions/broker/transport.py::_BaseSHMBrokerTransport` — Python SHM broker transports and lease objects.
[^broker-plugin]: `src/py/vertebra/extensions/broker/plugin.py::BrokerPlugin` — BrokerPlugin lifecycle and metric snapshot integration.
[^vtb-cli]: `src/py/vertebra/cli/__init__.py::cli` — Root vtb Click command group.
