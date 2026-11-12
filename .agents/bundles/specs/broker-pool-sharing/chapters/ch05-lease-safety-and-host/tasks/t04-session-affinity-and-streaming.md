---
type: Task
id: "broker-pool-sharing:ch05:t04"
chapter_id: ch05-lease-safety-and-host
title: Session affinity, VTBARROW streaming, and cancel
description: Pin SyncConnectionLease/AsyncConnectionLease across transactions, temp tables, prepared statements, and session settings, stream results as VTBARROW or row chunks over dbrpc fetch_chunk, and propagate cancel to the engine.
status: draft
state: pending
priority: P0
verification_strategy: behavior_tdd
depends_on:
  - "broker-pool-sharing:ch05:t03"
read_set:
  - src/py/vertebra/extensions/broker/transport.py
  - src/py/vertebra/extensions/sqlspec/proxy.py
  - src/py/vertebra/extensions/sqlspec/provider.py
  - src/py/vertebra/extensions/broker/dbrpc.py
  - src/py/tests/unit/extensions/test_sqlspec_shm_proxy.py
files:
  - src/py/vertebra/extensions/broker/transport.py
  - src/py/vertebra/extensions/broker/host.py
  - src/py/vertebra/extensions/sqlspec/proxy.py
tests:
  - src/py/tests/unit/extensions/test_sqlspec_shm_proxy.py
estimated_loc: 240
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
  - id: proxy
    resource: src/py/vertebra/extensions/sqlspec/proxy.py::ProxyAsyncDriver
    title: Proxy drivers
  - id: provider
    resource: src/py/vertebra/extensions/sqlspec/provider.py::_BaseSQLSpecProvider._exec_select_to_arrow_sync
    title: Arrow spill path
  - id: lease
    resource: src/py/vertebra/extensions/broker/transport.py::SyncConnectionLease
    title: Connection lease
tags:
  - broker
---

# Task t04: Session Affinity, VTBARROW Streaming, and Cancel

## Objective & Scope Boundary

1. `SyncConnectionLease` / `AsyncConnectionLease` [^lease] carry `session_pinned: bool`; the lease stays pinned from `Begin` until `Commit`/`Rollback`, and for the lease lifetime once a temp table, prepared statement, or session setting (`SET`, `SET LOCAL`, `ALTER SESSION`) is observed through the frame op flags (`DbRpcRequest.flags` bit `PIN_SESSION`, set by the proxy driver based on the upstream compiler's statement type, not SQL regexes).
2. `BrokerHost` dispatch serves `FetchChunk` with `chunk_rows` rows per response; results above `BrokerConfig.arrow_spill_rows` spill to `VTBARROW` SHM (reusing the existing spill in `_exec_select_to_arrow_sync` [^provider]) and return `arrow_shm`.
3. `ProxySyncDriver` / `ProxyAsyncDriver` [^proxy] iterate chunks lazily and send `Cancel` on cursor close/`asyncio.CancelledError`; the host calls the engine cancel and replies `DbRpcStatus.Cancelled`, after which the lease is quarantined when the engine cannot confirm the statement stopped.

## Bounded Read Manifest

- `src/py/vertebra/extensions/broker/transport.py` — leases.
- `src/py/vertebra/extensions/sqlspec/proxy.py` — `ProxySyncDriver`, `ProxyAsyncDriver`.
- `src/py/vertebra/extensions/sqlspec/provider.py` — `_exec_select_to_arrow_sync`, `_spill_arrow_object`.
- `src/py/vertebra/extensions/broker/dbrpc.py`, `src/py/tests/unit/extensions/test_sqlspec_shm_proxy.py`.

## Symbol & Signature Contracts

- `DbRpcRequest.flags: int = 0`; `PIN_SESSION: Final = 0x1`.
- `BrokerConfig.arrow_spill_rows: int = 10_000`, `BrokerConfig.chunk_rows: int = 1_000`.

## Edge Cases & Error Semantics

- Cancel after completion: no-op, `DbRpcStatus.Ok`.
- Cancel unconfirmed: lease quarantined, worker sees `LeaseQuarantinedError`.
- Worker drops a cursor mid-stream: `Close` frame frees server cursor; spilled SHM unlinked.
- Pinned lease released by worker crash: handled by `ch08 / t02`.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/unit/extensions/test_sqlspec_shm_proxy.py`. Dependency category: in-process host with SQLite-backed upstream SQLSpec config (existing fixtures in that file).

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Transaction pinning | `begin`; `INSERT`; second statement | Both on the same lease `(slot, generation)` |
| Temp table pinning | `CREATE TEMP TABLE t(x)`, commit, `SELECT * FROM t` | Succeeds on same lease |
| Chunked fetch | 2,500 rows, `chunk_rows=1000` | Three chunks of 1000/1000/500 |
| Arrow spill | 20,000 rows, `select_to_arrow` | `arrow_shm` set; SHM unlinked after table released |
| Cancel | Async iteration cancelled after first chunk | `DbRpcStatus.Cancelled` recorded; lease free or quarantined per engine confirmation |

## Verification

- `make dev && uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlspec_shm_proxy.py -q`.
- `make lint`.

## Acceptance Criteria

- [ ] Leases pin across transactions and session state; results stream with cancel propagation (R14).

## Notes & Discoveries

- [2026-11-12 01:40] `test_multi_statement_transaction_pinning_sync_and_async` and `test_large_result_sets_and_select_to_arrow_zero_copy_shm_spill_and_cleanup` already exist in the canonical file and are extended rather than duplicated.

[^proxy]: `src/py/vertebra/extensions/sqlspec/proxy.py::ProxyAsyncDriver` — Proxy drivers.
[^provider]: `src/py/vertebra/extensions/sqlspec/provider.py::_BaseSQLSpecProvider._exec_select_to_arrow_sync` — Arrow spill path.
[^lease]: `src/py/vertebra/extensions/broker/transport.py::SyncConnectionLease` — Connection lease.
