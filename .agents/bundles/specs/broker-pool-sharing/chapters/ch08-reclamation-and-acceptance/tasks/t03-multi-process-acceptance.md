---
type: Task
id: "broker-pool-sharing:ch08:t03"
chapter_id: ch08-reclamation-and-acceptance
title: Prove multi-process pool sharing for PostgreSQL and MySQL
description: Add an integration suite that starts a broker host and four worker processes against pytest-databases PostgreSQL and MySQL services, asserts one server-side pool via pg_stat_activity and information_schema.PROCESSLIST, reclaims a SIGKILLed worker mid-transaction, and proves a quarantined backend is never re-leased.
status: draft
state: pending
priority: P0
verification_strategy: integration_acceptance
depends_on:
  - "broker-pool-sharing:ch08:t02"
  - "broker-pool-sharing:ch06:t03"
  - "broker-pool-sharing:ch04:t01"
read_set:
  - src/py/tests/integration/conftest.py
  - src/py/vertebra/extensions/broker/host.py
  - src/py/vertebra/extensions/sqlspec/plugin.py
  - .agents/bundles/specs/broker-pool-sharing/chapters/ch08-reclamation-and-acceptance/spec.md
files:
  - src/py/tests/integration/extensions/__init__.py
  - src/py/tests/integration/extensions/test_broker_pool_sharing.py
tests:
  - src/py/tests/integration/extensions/test_broker_pool_sharing.py
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
  - id: pg-fixture
    resource: src/py/tests/integration/conftest.py::vtb_postgres_config
    title: PostgreSQL service fixture
  - id: mysql-fixture
    resource: src/py/tests/integration/conftest.py::vtb_mysql_config
    title: MySQL service fixture
  - id: proxy-config
    resource: src/py/vertebra/extensions/sqlspec/plugin.py::ProxyAsyncConfig
    title: Proxy async config wrapping upstream SQLSpec configs
tags:
  - acceptance
  - integration
  - postgres
  - mysql
---

# Task t03: Prove Multi-Process Pool Sharing for PostgreSQL and MySQL

## Objective & Scope Boundary

Create `src/py/tests/integration/extensions/__init__.py` and `test_broker_pool_sharing.py`. A module fixture starts `BrokerHost` in a child process (`multiprocessing.get_context("spawn")`) for the engine under test; workers are 4 spawned processes that attach through the authenticated handshake and run SQL through `ProxyAsyncConfig` (PostgreSQL via upstream `sqlspec.adapters.asyncpg.AsyncpgConfig`) or the native MySQL broker backend [^proxy-config]. Connection counts come from a separate direct admin connection: PostgreSQL `select count(*) from pg_stat_activity where application_name = %s`, MySQL `select count(*) from information_schema.PROCESSLIST where user = %s` [^pg-fixture] [^mysql-fixture]. Every test carries `@pytest.mark.driver_engine("postgres")` or `("mysql")`.

Out of scope: production code changes; failures found here route back to the owning task.

## Bounded Read Manifest

- `integration/conftest.py` — `vtb_postgres_config`, `vtb_mysql_config`, service fixtures [^pg-fixture] [^mysql-fixture].
- `extensions/broker/host.py` — `BrokerHost`, `on_worker_exit`.
- `extensions/sqlspec/plugin.py` — `ProxyAsyncConfig` [^proxy-config].
- Chapter spec acceptance scenarios.

## Symbol & Signature Contracts

- Test-local helpers: `_spawn_workers(n: int, target: Callable[..., None], *args: Any) -> list[multiprocessing.Process]`, `_server_connection_count(engine: str, admin: Any, tag: str) -> int`.
- Pool settings: `pool_size=2`, `max_overflow=0`; expected server-side connections `<= 2` while 4 workers each hold a lease in turn.

## Edge Cases & Error Semantics

- Docker unavailable → module skips with reason `"broker acceptance requires VERTEBRA_CONFORMANCE_DOCKER=1"`; under the conformance gate a skip fails the lane.
- Worker SIGKILL during `BEGIN; UPDATE ...` → after `on_worker_exit`, no row in `pg_stat_activity` with `state = 'idle in transaction'` for the tag; MySQL `information_schema.INNODB_TRX` empty for the broker user.
- Forced rollback failure (test hook terminates the backend with `pg_terminate_backend` mid-transaction before release) → backend PID recorded before termination never appears in any later lease's `pg_backend_pid()`; quarantine counter increments.
- Processes are joined with a timeout; leftovers are killed in fixture teardown.

## Public-Seam Test Blueprint

| Test | Engine | Action | Expected |
| --- | --- | --- | --- |
| `test_four_workers_share_one_pool[postgres]` | postgres | 4 workers run 20 queries each | max observed connections `<= 2`; all results correct |
| `test_four_workers_share_one_pool[mysql]` | mysql | same | max observed connections `<= 2` |
| `test_sigkill_mid_transaction_reclaimed[postgres]` | postgres | worker begins tx, SIGKILL | no idle-in-transaction backend after reclaim; next lease sees committed state only |
| `test_sigkill_mid_transaction_reclaimed[mysql]` | mysql | same | `INNODB_TRX` empty for broker user |
| `test_quarantined_backend_never_released[postgres]` | postgres | terminate backend before release | `LeaseQuarantinedError` in worker; PID absent from 10 subsequent leases; `total_quarantined == 1` |
| `test_native_mysql_registered` | mysql | broker start | `2 in BrokerEngineRegistry.registered_tags()` |

## Verification

- `make dev && VERTEBRA_CONFORMANCE_DOCKER=1 uv run --no-sync pytest src/py/tests/integration/extensions/test_broker_pool_sharing.py -q -rs`
- `VERTEBRA_CONFORMANCE_DOCKER=1 VERTEBRA_CONFORMANCE_ENGINES=mysql uv run --no-sync pytest src/py/tests/integration -q` (requires `driver-conformance-ci` gate)

## Acceptance Criteria

- All six tests pass with zero skips under Docker.

## Notes & Discoveries

- PostgreSQL zero-skip enforcement belongs to `native-postgres-driver`; the `postgres` marker here only tags the tests.
