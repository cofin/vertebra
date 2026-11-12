---
type: Task
id: "native-postgres-driver:ch36:t01"
chapter_id: ch36-replication-python-bridge
title: "Expose native replication identity probes"
description: "Call the existing dedicated PostgresReplicationConnection::probe_identity from AsyncPostgresClient"
status: draft
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on: ["native-postgres-driver:ch35:t03"]
read_set:
  - src/rs/vertebra-postgres/src/async_client.rs
  - src/rs/vertebra-postgres/src/replication/mod.rs
  - src/rs/vertebra-db/src/py_async.rs
  - src/py/vertebra/_postgres.pyi
  - src/py/tests/unit/db/test_postgres_native_notifications.py
  - src/py/tests/unit/db/test_postgres_native_replication.py
files:
  - src/rs/vertebra-postgres/src/async_client.rs
  - src/py/vertebra/_postgres.pyi
tests:
  - src/py/tests/unit/db/test_postgres_native_replication.py
estimated_loc: 120
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
commit: null
plan_revision: 8
plan_commit: 5eb75b2a
state_revision: 0
created_at: "2026-11-12T04:20:00Z"
updated_at: "2026-11-12T04:20:00Z"
generated: {by: flow-revise, at: "2026-11-12T04:20:00Z"}
verified: []
sources:
  - {id: contracts, resource: .agents/bundles/specs/native-postgres-driver/contracts.md, title: Raw replication ownership contracts}
  - {id: async, resource: "src/rs/vertebra-db/src/py_async.rs::spawn_rust_db_task_with_attr", title: Cancellation-safe async task bridge}
  - {id: identity, resource: "src/rs/vertebra-postgres/src/replication/mod.rs::verified_identity_to_python", title: Event-loop Python conversion}
tags: [postgres, replication, pyo3, cancellation]
---

# Task t01: Expose native replication identity probes

## Objective & Scope Boundary

Call the existing dedicated PostgresReplicationConnection::probe_identity from AsyncPostgresClient.replication_identity using the per-extension async drainer. Add only the identity stub and explicitly create canonical Python replication tests; raw stream handles are t03. [^contracts] [^async] [^identity]

## Bounded Read Manifest

- `src/rs/vertebra-postgres/src/async_client.rs` — Read the owning public seam and existing call-site conventions.
- `src/rs/vertebra-postgres/src/replication/mod.rs` — Read the owning public seam and existing call-site conventions.
- `src/rs/vertebra-db/src/py_async.rs` — Read the owning public seam and existing call-site conventions.
- `src/py/vertebra/_postgres.pyi` — Read the owning public seam and existing call-site conventions.
- `src/py/tests/unit/db/test_postgres_native_notifications.py` — Read the owning public seam and existing call-site conventions.
- `src/py/tests/unit/db/test_postgres_native_replication.py` — Explicit new canonical target created by t01; later tasks extend it.

Owning chapter spec and referenced local pyo3-bridge skill/FFI knowledge supply shared contracts; no undeclared source exploration is required. New methods/converter/class are declared creation targets, not assumed baseline symbols.

## Symbol & Signature Contracts

`AsyncPostgresClient.replication_identity(self,slot:str,publication:str)->Awaitable[PostgresReplicationIdentity]`; Rust uses aborting `spawn_rust_db_task_with_attr`, `PostgresReplicationConnection::probe_identity`, `verified_identity_to_python`. Clone `self.session.config()` as the existing `spawn_open_listener` call path does; no query-pool lease is borrowed. Cancellation aborts the worker future so owned transport fields drop to EOF; the non-aborting `spawn_rust_db_task_with_attr_and_cancel` is not used for this identity probe. Initialization of the completion drainer precedes worker spawn; failed setup opens zero sockets.

- All Python constructors and conversions run on the Python event loop inside `DbOutcomeProducer`; Tokio workers do not call `Python::attach`.
- The existing module drainer attribute from `async_client.rs` is reused. Stubs use `def ... -> Awaitable[T]`, matching `RustDbTaskAwaitable`.
- Dedicated sockets reject pooler modes `transaction`, `statement`, and `auto` before dialing. An `AsyncPostgresPool`-acquired client supplies these modes in tests; direct constructors default to `direct`.

## Edge Cases & Error Semantics

Queue capacity is 1..1024 inclusive; bool is TypeError and 0/1025 are InterfaceError before dialing. Invalid LSN bool/integer bounds reject before socket I/O; canonical InterfaceError covers pooler refusal, overlap, stale generations and post-terminal calls. Conversion errors leave the event lease unpublished. Socket cancellation drops owned transports; close never advances acknowledged LSN. Terminal next error preserves its class and cause, with closed snapshot true.

## Public-Seam Test Blueprint

New canonical Python module `src/py/tests/unit/db/test_postgres_native_replication.py` exercises actual installed extension against a local scripted protocol peer. Fixtures follow the existing notification module; the native client/supervisor are never mocked.

| Case | Input | Expected output/error |
| --- | --- | --- |
| direct identity | slot="vertebra_slot", publication="vertebra_pub" | identity_key=("sys-1","appdb","vertebra_slot", fingerprint); no START_REPLICATION |
| invalid pooler | transaction/statement/auto acquired clients | InterfaceError before standalone accept |
| cancelled probe | hold peer response, cancel awaitable | peer observes EOF and no later completion |
| missing slot | real peer returns absent slot metadata | canonical OperationalError; no stream session |

## Verification

Initial Red: create/extend canonical behavioral cases; run `UV_NO_SYNC=1 PYO3_PYTHON="$PWD/.venv/bin/python" make dev` then `uv run --no-sync python -m pytest src/py/tests/unit/db/test_postgres_native_replication.py -k native_replication_identity -q` and observe absent behavior. Final Green: rebuild again after Rust edits, rerun focused selector and full module, then `uv run --no-sync ruff check src/py/vertebra/_postgres.pyi src/py/tests/unit/db/test_postgres_native_replication.py`, `uv run --no-sync mypy src/py/vertebra/_postgres.pyi src/py/tests/unit/db/test_postgres_native_replication.py`, `uv run --no-sync python -m mypy.stubtest vertebra._postgres`, and PostgreSQL all-target Clippy. All commands exit0; preserve UV_NO_SYNC=1 and PYO3_PYTHON. No skip/xfail or importorskip masks the new API.

## Acceptance Criteria

- [ ] The exact public seam satisfies every literal case and refusal path above (R22.6/R23.1).
- [ ] Fresh Red and Green plus fast lint/type or native Clippy evidence is recorded with exit codes.
- [ ] Event-loop conversion, Drop ownership, and feedback fencing remain consistent with chapter section2; owned stubs/docstrings match landed methods.

## Notes & Discoveries

The canonical Python raw replication module is absent at the planning base and is explicitly created in t01; PostgresChangeSource remains a separate downstream ch24 module. No production behavior is verified by this planning artifact.

[^contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Raw replication ownership and checkpoint boundary.
[^async]: `src/rs/vertebra-db/src/py_async.rs::spawn_rust_db_task_with_attr` — Aborting worker cancellation and event-loop outcome delivery.
[^identity]: `src/rs/vertebra-postgres/src/replication/mod.rs::verified_identity_to_python` — Existing pure event-loop identity and tuple conversion seam.
