---
type: Task
id: "broker-pool-sharing:ch05:t03"
chapter_id: ch05-lease-safety-and-host
title: Broker host lifecycle and vtb broker serve
description: Add BrokerHost running segment creation, engine discovery, attach server, frame dispatch, and quarantine disposal for both the vtb broker serve command and the supervisor-hosted broker, with bounded shutdown reporting.
status: draft
state: pending
priority: P0
verification_strategy: behavior_tdd
depends_on:
  - "broker-pool-sharing:ch05:t02"
  - "broker-pool-sharing:ch03:t02"
read_set:
  - src/py/vertebra/extensions/broker/plugin.py
  - src/py/vertebra/cli/__init__.py
  - src/py/vertebra/cli/workers.py
  - src/rs/vertebra-http/src/workers/factory.rs
  - src/py/tests/unit/extensions/test_shm_broker.py
files:
  - src/py/vertebra/extensions/broker/host.py
  - src/py/vertebra/cli/broker.py
  - src/py/vertebra/extensions/broker/plugin.py
tests:
  - src/py/tests/unit/extensions/test_shm_broker.py
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
  - id: plugin
    resource: src/py/vertebra/extensions/broker/plugin.py::BrokerPlugin
    title: Broker plugin
  - id: cli
    resource: src/py/vertebra/cli/__init__.py::cli
    title: vtb click group
  - id: factory
    resource: src/rs/vertebra-http/src/workers/factory.rs::WorkerLifecycle::respawn
    title: Supervisor worker spawn
tags:
  - broker
---

# Task t03: Broker Host Lifecycle and `vtb broker serve`

## Objective & Scope Boundary

1. Create `src/py/vertebra/extensions/broker/host.py` with `BrokerHost` (chapter §2.2): `start()` opens/creates the named segment (`LeaseBroker.open_or_create_named`), runs `BrokerEngineDiscovery.register_installed()`, registers partitions for configured pools, starts `BrokerAttachServer` when `socket_path` is set, and starts a dispatch thread that serves `dbrpc` frames from worker ring pairs and disposes quarantined slots (close the engine connection via `BrokerEngineRegistry.execute(tag, pool_id, Close frame)` or the upstream provider, then `dispose_quarantined`). `stop(timeout)` stops accepting, waits for in-flight frames, and returns descriptions of undisposed connections.
2. Create `src/py/vertebra/cli/broker.py` with a `broker` click group and `serve` command (`--app`, `--socket PATH`, `--secret-file PATH`, `--shm-name NAME`); register it in the `vtb` group following the pattern used for `workers.py` in `src/py/vertebra/cli/__init__.py` [^cli].
3. `BrokerPlugin` [^plugin] constructs a `BrokerHost` in the supervisor process (when `VERTEBRA_BROKER_SHM_FD` is absent) so supervisor and standalone hosting share one lifecycle.

## Bounded Read Manifest

- `src/py/vertebra/extensions/broker/plugin.py`, `src/py/vertebra/cli/__init__.py`, `src/py/vertebra/cli/workers.py`, `src/rs/vertebra-http/src/workers/factory.rs` (`WorkerLifecycle::respawn` env wiring) [^factory], `src/py/tests/unit/extensions/test_shm_broker.py`.

## Symbol & Signature Contracts

- `BrokerHost.stop(timeout: float = 10.0) -> list[str]`; entries formatted `"<pool_key>: slot <i> generation <g> quarantined"`.
- CLI: `vtb broker serve --socket /run/vtb/broker.sock --secret-file /run/vtb/secret --shm-name /vtb-broker-<app>`.

## Edge Cases & Error Semantics

- Socket path exists and is live: `BrokerAttachError` (never unlink a live socket); stale socket file (no listener): unlinked and rebound.
- `start()` twice: `RuntimeError("broker host already started")`.
- SIGTERM to `vtb broker serve`: `stop(10.0)`, report printed to stderr, exit `0`; non-empty report exits `1`.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/unit/extensions/test_shm_broker.py`. Dependency category: local processes and sockets.

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Standalone attach | `BrokerHost(BrokerConfig(shm_name=f"/vtb-h-{uuid8}", pool_size=2), socket_path=tmp/"s.sock", secret=b"k"*32).start()`; client attaches in a subprocess | Subprocess acquires and releases a lease; exit `0` |
| Double start | `start()` twice | `RuntimeError` |
| Live socket | Second host on same socket path | `BrokerAttachError` |
| Shutdown report | Quarantine one slot without disposal hook, `stop(0.5)` | Returns one entry; completes under 1 s |
| CLI help | `CliRunner().invoke(cli, ["broker", "serve", "--help"])` | Exit `0`; shows `--socket` |

## Verification

- `make dev && uv run --no-sync pytest src/py/tests/unit/extensions/test_shm_broker.py -q -k host`.
- `make lint`; `make docs` CLI docs check (`check CLI docs` step) passes after `ch09 / t01`.

## Acceptance Criteria

- [ ] Standalone and supervisor broker hosting share `BrokerHost` (R13).
- [ ] Shutdown is bounded and reports undisposed connections (R13, R11).

## Notes & Discoveries

- [2026-11-12 01:40] The `vtb` CLI is a click group (`cli/__init__.py::cli`); there is no `broker` command at HEAD.

[^plugin]: `src/py/vertebra/extensions/broker/plugin.py::BrokerPlugin` — Broker plugin.
[^cli]: `src/py/vertebra/cli/__init__.py::cli` — vtb click group.
[^factory]: `src/rs/vertebra-http/src/workers/factory.rs::WorkerLifecycle::respawn` — Supervisor worker spawn.
