---
type: Task
id: "broker-pool-sharing:ch06:t03"
flow_id: broker-pool-sharing
chapter_id: ch06-sqlspec-seams-and-multiprocess
title: "OTel Quarantine Gauges and Multi-Process Worker Pool-Sharing Suite"
description: "Propagate quarantine/reclaim metrics in telemetry/otel.py and verify 4-worker pool sharing, crash reclamation, and UDS auth."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "broker-pool-sharing:ch04:t03"
  - "broker-pool-sharing:ch05:t04"
  - "broker-pool-sharing:ch06:t02"
files:
  - src/py/vertebra/telemetry/otel.py
tests:
  - src/py/tests/unit/extensions/test_shm_broker.py
  - src/py/tests/integration/extensions/test_multiprocess_pool_sharing.py
read_set:
  - src/py/vertebra/telemetry/otel.py
  - src/py/vertebra/extensions/broker/host.py
  - src/py/vertebra/extensions/sqlspec/plugin.py
  - src/py/tests/integration/conftest.py
estimated_loc: 225
tags:
  - multiprocess
  - telemetry
  - postgres
  - mysql
sources:
  - id: otel-metrics
    resource: src/py/vertebra/telemetry/otel.py
    title: "OpenTelemetry and ZPages database pool metric samples"
  - id: int-conftest
    resource: src/py/tests/integration/conftest.py::vtb_postgres_config
    title: "Containerized PostgreSQL and MySQL integration fixtures"
---

# Task `t03`: OTel Quarantine Gauges and Multi-Process Worker Pool-Sharing Suite

## Objective & Scope Boundary

Expose `quarantined`, `total_quarantined`, `total_disposed`, and `total_reclaimed` in `telemetry/otel.py` and verify multi-process worker pool sharing, `SIGKILL` crash `ROLLBACK`, quarantine disposal, and UDS permission rejection per [`../spec.md` §2](../spec.md) [^otel-metrics] [^int-conftest].

## Bounded Read Manifest

- `src/py/vertebra/telemetry/otel.py` — `DbPoolMetricSample` and ZPages broker view.
- `src/py/vertebra/extensions/broker/host.py` — `BrokerHost` lifecycle and `on_worker_exit`.
- `src/py/vertebra/extensions/sqlspec/plugin.py` — `ProxySyncConfig` and `ProxyAsyncConfig`.
- `src/py/tests/integration/conftest.py` — `vtb_postgres_config` and `vtb_mysql_config` fixtures.

## Contracts & Edge Cases

- Extend `DbPoolMetricSample` in `src/py/vertebra/telemetry/otel.py` with quarantine and reclaim fields.
- Verify 4 worker processes share `<= pool_size + max_overflow` server connections on PostgreSQL (`pg_stat_activity`) and MySQL (`information_schema.PROCESSLIST`), plus offline multi-process DuckDB/UDS verification.
- **Edge Case**: `SIGKILL` of a worker holding an open `BEGIN` leaves zero `idle in transaction` sessions after `BrokerHost.on_worker_exit`.
- **Edge Case**: Forced rollback failure evicts the backend PID permanently and increments `total_quarantined`.

## Test Matrix

- Offline multi-process DuckDB + UDS attach test runs in `< 10s` without Docker; live PostgreSQL/MySQL container cases verify server-side connection counts and crash `ROLLBACK`.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/extensions/test_shm_broker.py src/py/tests/integration/extensions/test_multiprocess_pool_sharing.py -q -k 'not docker'
```

## Acceptance Criteria

- [ ] Multi-process workers share one broker-owned pool and recover cleanly from `SIGKILL` and rollback failures.
- [ ] Zero regressions in the chapter fast verification gate.

[^otel-metrics]: `src/py/vertebra/telemetry/otel.py` — OpenTelemetry and ZPages database pool metric samples.
[^int-conftest]: `src/py/tests/integration/conftest.py::vtb_postgres_config` — Containerized PostgreSQL and MySQL integration fixtures.
