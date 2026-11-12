---
type: Task
id: "native-postgres-driver:ch36:t02"
chapter_id: ch36-replication-python-bridge
title: "Convert lossless replication events on the event loop"
description: "Extend the existing public event-loop conversion module for complete ReplicationStreamEvent values"
status: draft
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on: ["native-postgres-driver:ch36:t01"]
read_set:
  - src/rs/vertebra-postgres/src/replication/mod.rs
  - src/rs/vertebra-postgres/src/replication/pgoutput.rs
  - src/rs/vertebra-postgres/src/replication/supervisor.rs
  - src/py/vertebra/db/adapters/postgres/types.py
  - src/rs/vertebra-postgres/tests/postgres_replication_wire_tests.rs
files:
  - src/rs/vertebra-postgres/src/replication/mod.rs
tests:
  - src/rs/vertebra-postgres/tests/postgres_replication_wire_tests.rs
estimated_loc: 190
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

# Task t02: Convert lossless replication events on the event loop

## Objective & Scope Boundary

Extend the existing public event-loop conversion module for complete ReplicationStreamEvent values. Preserve actual Python types and payload schema; this conversion seam is consumed by t04 and never publishes leases. [^contracts] [^async] [^identity]

## Bounded Read Manifest

- `src/rs/vertebra-postgres/src/replication/mod.rs` — Read the owning public seam and existing call-site conventions.
- `src/rs/vertebra-postgres/src/replication/pgoutput.rs` — Read the owning public seam and existing call-site conventions.
- `src/rs/vertebra-postgres/src/replication/supervisor.rs` — Read the owning public seam and existing call-site conventions.
- `src/py/vertebra/db/adapters/postgres/types.py` — Read the owning public seam and existing call-site conventions.
- `src/rs/vertebra-postgres/tests/postgres_replication_wire_tests.rs` — Read the owning public seam and existing call-site conventions.

Owning chapter spec and referenced local pyo3-bridge skill/FFI knowledge supply shared contracts; no undeclared source exploration is required. New methods/converter/class are declared creation targets, not assumed baseline symbols.

## Symbol & Signature Contracts

`pub fn replication_event_to_python(py: Python<'_>, event: &ReplicationStreamEvent) -> PyResult<Py<PyAny>>`. Exact payload keys and per-kind mappings are the Lossless event payload contract in owning spec section2. Cache PostgresReplicationRecord via the existing PyOnceLock type resolver. Input event stays borrowed and unchanged on any PyErr. Reuse existing tuple converters, never add test-only Python exports.

- All Python constructors and conversions run on the Python event loop inside `DbOutcomeProducer`; Tokio workers do not call `Python::attach`.
- The existing module drainer attribute from `async_client.rs` is reused. Stubs use `def ... -> Awaitable[T]`, matching `RustDbTaskAwaitable`.
- Dedicated sockets reject pooler modes `transaction`, `statement`, and `auto` before dialing. An `AsyncPostgresPool`-acquired client supplies these modes in tests; direct constructors default to `direct`.

## Edge Cases & Error Semantics

Queue capacity is 1..1024 inclusive; bool is TypeError and 0/1025 are InterfaceError before dialing. Invalid LSN bool/integer bounds reject before socket I/O; canonical InterfaceError covers pooler refusal, overlap, stale generations and post-terminal calls. Conversion errors leave the event lease unpublished. Socket cancellation drops owned transports; close never advances acknowledged LSN. Terminal next error preserves its class and cause, with closed snapshot true.

## Public-Seam Test Blueprint

Existing canonical Rust wire module calls the public converter with owned real decoder/supervisor event values under Python::attach on the test caller thread, asserting actual Python record fields/types. This is not a worker-thread attachment. No converter mocking.

| Case | Input | Expected output/error |
| --- | --- | --- |
| control | identity sys-1/appdb/vertebra_slot; ack=0x100; gen=2 | kind control, payload identity object+acknowledged_lsn=256, generation2 |
| update | old key id=7; new amount Decimal("9.50"), body unchangedTOAST | old/new dict values retain Decimal and singleton PG_UNCHANGED_TOAST |
| all record variants | begin/insert/delete/truncate/message/commit literals | exact required keys, wire order, bytes content, commit-only top-level LSN |
| lossless nested values | NULL, PgInterval(1,2,3), array lowerbound0, composite, temporal infinity, JSON null, vector | identical adapter value types/content, never repr strings |
| nontransactional message | prefix="audit", content=b"abc", no xid/dedup | xid/dedup_key None, message_lsn retained |

## Verification

Initial Red: add public conversion cases, run `PYO3_PYTHON="$PWD/.venv/bin/python" PYTHONPATH="$PWD/src/py" cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_replication_wire_tests replication_bridge_conversion`, and observe absent/missing conversion behavior. Final Green: rerun that command, then full wire module and `cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings` with the same PYO3_PYTHON.

## Acceptance Criteria

- [ ] The exact public seam satisfies every literal case and refusal path above (R22.6/R23.1).
- [ ] Fresh Red and Green plus fast lint/type or native Clippy evidence is recorded with exit codes.
- [ ] Event-loop conversion, Drop ownership, and feedback fencing remain consistent with chapter section2; owned stubs/docstrings match landed methods.

## Notes & Discoveries

The canonical Python raw replication module is absent at the planning base and is explicitly created in t01; PostgresChangeSource remains a separate downstream ch24 module. No production behavior is verified by this planning artifact.

[^contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Raw replication ownership and checkpoint boundary.
[^async]: `src/rs/vertebra-db/src/py_async.rs::spawn_rust_db_task_with_attr` — Aborting worker cancellation and event-loop outcome delivery.
[^identity]: `src/rs/vertebra-postgres/src/replication/mod.rs::verified_identity_to_python` — Existing pure event-loop identity and tuple conversion seam.
