---
type: Spec
flow_id: shm-statement-and-result-cache
title: "Shared-Memory Prepared Statement Cache, Python Broker Transport & ZPages/OTel Telemetry"
state: planned
plan_revision: 2
plan_commit: ""
state_revision: 0
current_task: null
last_operation: null
operation_targets: []
last_verified_checkpoint: null
created_at: 2026-10-22T04:40:00Z
updated_at: 2026-10-22T04:40:00Z
description: "Implement the lock-free seqlock ShmStatementCache in vertebra-core and vertebra-py, wire ShmBrokerTransport and ShmSessionBroker in vertebra.extensions.broker and vertebra.extensions.alchemy, and integrate live broker ZPages and OpenTelemetry pool telemetry."
tags: [feature, rust, python, ipc, database]
parent_prd: vertebra-shm-connection-broker
research: []
---

# Shared-Memory Prepared Statement Cache, Python Broker Transport & ZPages/OTel Telemetry

## Problem & Goal

With `ShmLeaseTable` and `NativeLeaseBroker` established in Chapter 1 (`shm-broker-lease-table`), three higher-level capabilities remain to complete cross-process database coordination:

1. **Cross-Process Prepared Statement & Parameter-Plan Cache (`ShmStatementCache` / `NativeStatementCache`)**:
   - Currently, every Python worker process independently normalizes SQL queries, hashes dialect-specific statement text, and constructs positional/named bind parameter plans (`ParameterizedSql` in `src/py/vertebra/extensions/sqlspec/plugin.py:130-225`).
   - Adding a 4-way set-associative seqlock statement cache (`ShmStatementCache` in `src/rs/vertebra-core/src/broker.rs`, exposed via `NativeStatementCache` in `src/rs/vertebra-py/src/broker.rs`) allows any worker to publish a compiled `(sql_hash, dialect_tag, param_count, normalized_sql, packed_param_names)` entry into shared memory and allows all peer workers to read it via optimistic seqlock validation (`(seq1 & 1) == 0 && seq1 == seq2`) with zero locks.
2. **Production `ShmBrokerTransport` & `vertebra.extensions.broker` Package**:
   - `ShmBrokerTransport` (`src/py/vertebra/extensions/alchemy/broker.py:318-360`) currently allocates an unused 64 KiB `ShmRegion`, encodes `ChannelMessage` payloads with `msgspec.msgpack.Encoder()` only to discard the bytes (`_ = self._encoder.encode(message)`), and delegates to `InMemoryBrokerTransport` (`threading.Condition` + `asyncio.sleep(0.002)`).
   - Extracting the shared broker abstractions into `src/py/vertebra/extensions/broker/` (`transport.py`, `plugin.py`, `__init__.py`) and wiring `ShmBrokerTransport` (and `ShmSessionBroker` in `src/py/vertebra/extensions/alchemy/broker.py`) to `NativeLeaseBroker`, `NativeStatementCache`, and `SyncPool.default().submit_async` (`src/py/vertebra/utils/sync_tools.py:165-180`) eliminates polling sleep loops and unifies broker lifecycle across `AlchemyPlugin` (`src/py/vertebra/extensions/alchemy/plugin.py`) and `SQLSpecPlugin` (`src/py/vertebra/extensions/sqlspec/plugin.py`).
3. **Live `/debug/zpages/broker` Inspection & OpenTelemetry Pool Metrics**:
   - `ZPagesRegistry` (`src/py/vertebra/server/platform.py:102-157`) serves `/healthz`, `/statusz`, `/varz`, and `/rpcz`, while `src/py/vertebra/telemetry/otel.py:97-168` provides `SpanExporter` and `GenAiRecorder`.
   - Adding `/debug/zpages/broker` to `ZPagesRegistry` and `DbPoolMetricSample` / `DbPoolRecorder` to `src/py/vertebra/telemetry/otel.py` exposes live cross-process active leases, overflow utilization, wait timeouts, crash-reclaimed leases, and statement cache hit ratios.

---

## Requirements Traceability

| Requirement ID | Description | Tasks |
| :--- | :--- | :--- |
| `BRK-R6` | Lock-free shared-memory prepared-statement digest and parameter-plan cache (`ShmStatementCache` / `NativeStatementCache`) using seqlock reads and clock-second-chance eviction. | `2.1`, `2.4` |
| `BRK-R7` | Production `ShmBrokerTransport` and `ShmSessionBroker` backed by `NativeLeaseBroker`, `NativeStatementCache`, and `SyncPool.default().submit_async`. | `2.2`, `2.4` |
| `BRK-R8` | Live `/debug/zpages/broker` inspection endpoint in `ZPagesRegistry` and OpenTelemetry `db.client.connections.*` / `db.client.statement_cache.*` telemetry across `BrokerPlugin`, `AlchemyPlugin`, and `SQLSpecPlugin`. | `2.3`, `2.4` |

---

## Task Breakdown

- [x] `shm-statement-and-result-cache:2.1` — Implement Seqlock `ShmStatementCache` & PyO3 `NativeStatementCache` (`verification_strategy: behavior_tdd`, `depends_on: []`)
- [x] `shm-statement-and-result-cache:2.2` — Implement `vertebra.extensions.broker` Transport & Wire `ShmBrokerTransport` to `NativeLeaseBroker` (`verification_strategy: behavior_tdd`, `depends_on: [shm-statement-and-result-cache:2.1]`)
- [x] `shm-statement-and-result-cache:2.3` — Integrate `BrokerPlugin`, `AlchemyPlugin` & `SQLSpecPlugin` with `/debug/zpages/broker` & OpenTelemetry (`verification_strategy: behavior_tdd`, `depends_on: [shm-statement-and-result-cache:2.2]`)
- [x] `shm-statement-and-result-cache:2.4` — Criterion Benchmarks & Multi-Worker `3.12`/`3.14t` Integration Verification (`verification_strategy: integration_acceptance`, `depends_on: [shm-statement-and-result-cache:2.3]`)
