---
type: Task
id: native-postgres-driver:t23
title: Expose physical synchronous native PostgreSQL sessions
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
- native-postgres-driver:t22
files:
- src/rs/vertebra-postgres/src/lib.rs
- src/rs/vertebra-postgres/src/errors.rs
- src/rs/vertebra-postgres/src/sync_client.rs
- src/py/vertebra/_postgres.pyi
- src/py/tests/unit/db/test_postgres_driver.py
tests:
- src/py/tests/unit/db/test_postgres_driver.py::test_native_sync_lifecycle
- src/py/tests/unit/db/test_postgres_driver.py::test_native_sync_detaches
plan_revision: 2
plan_commit: "7657e8b7c2c96bdda6821aec70a2b603f438a127"
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: '2026-11-05T18:25:07Z'
updated_at: '2026-11-06T06:30:00Z'
commit: null
tags:
- postgres
- connection
- native
requirements:
- R6
---

# Expose physical synchronous native PostgreSQL sessions

## Objective

Expose physical synchronous native PostgreSQL sessions, with behavior verified through the canonical public seam listed above.

## Context

Read `../spec.md`, the named dependency worksheets, `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md`, `.agents/skills/pyo3-bridge/SKILL.md` and `.agents/bundles/knowledge/patterns/rust/ffi-and-gil-discipline.md`. This task belongs to the complete native PostgreSQL driver on one feature branch and one PR. Its completion is not permission to ship a partial driver or cut over the Python adapters. Public signatures and shared contracts in the full spec govern later task additions. No implementation starts with plan_commit null.

Ownership is exactly the frontmatter list. Shared files are changed only after predecessor tasks complete; preserve unrelated edits. One executor invocation makes one signed conventional local commit. Estimated handwritten changes including tests: 320 lines; generated lock output is reviewed separately. Keep the task near400 lines. If the specified work cannot fit or a named prerequisite is missing, stop and `/flow:revise native-postgres-driver`; keep the complete one-PR scope and split the worksheet boundary.



## Steps

1. Add RED direct-native tests in existing test_postgres_driver.py with a loopback socket server/thread fixture: constructor accepts no connection, starts closed with empty parameters, then connects/closes through real native methods. Full constructor matrix/parity coverage belongs to t25; this task focuses lifecycle and detached I/O. Cover repeated Ready no-op, close twice, retry after Closed, exact core SQLSTATE28/08 exception identity, copied parameter map and absence of backend-key attributes. Coordinate using bounded Events/barriers and reuse the fixture for t24.
2. errors.rs::postgres_error_to_py caches exact core types through PyOnceLock/import, with no stand-ins or cross-cdylib PyO3 Rust extraction. Config maps to ValueError/TypeError, Interface to InterfaceError, UnsupportedAuth/UnsupportedFeature to NotSupportedError, transport/protocol/08 to ConnectionFailure, and connection-stage other SQLSTATE including28 to DatabaseError. Attach sqlstate orNone and sanitized message. Resolve/cache the exact core GoogleCloudAuthError as well: native PostgresError::Auth credential-provider/scope/lifetime failures map to that class with no fabricated SQLSTATE; server SQLSTATE28/28P01 remains DatabaseError. UnsupportedFeature maps to core NotSupportedError. Later query tasks extend full SQLSTATE subclass behavior.
3. sync_client.rs::PySyncPostgresClient is frozen/immutable_type with project-compatible cfg freelist and SessionHandle. Its explicit keyword-only constructor and pyi exactly match every option/default in ../contracts.md, including Unix, binding, GUCs, keepalive, cloud and separate Admin/login auth maps. Copy mappings before any detach/await and validate through the single owned PostgresConfig; no ignored fields or imaginary later setters. connect reserves begin_connect, returns None for AlreadyReady, drives attempt.run using py.detach(||block_on_db(...)), then commits PendingSession. Map errors/results only attached. close first takes state, then performs bounded network cleanup detached. Getters build a new Python dict from owned snapshot. No pool/query/cursor placeholders.
4. Register class/stub and prove GIL detachment: server holds startup while an independent Python progress thread sets an Event. Commit feat(postgres): expose synchronous physical connection client.

## Verification

Write the named failing tests before behavior changes. Initial RED is the absent named module/public method or the stated incorrect behavior; record the actual failing assertion/diagnostic. After implementation, run exactly:

```bash
make dev
uv run --no-sync python -m pytest src/py/tests/unit/db/test_postgres_driver.py -k native_sync -q
uv run --no-sync python -m mypy.stubtest vertebra._postgres
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres
```

Final exit0 is required. These commands are planned, not executed evidence. Record each command/exit and public assertions during execution. Source rebuild precedes Python tests; never validate a stale extension. No external write, workflow dispatch, tag mutation or push is authorized here.

## Acceptance Criteria

- [ ] R6: the owned public behavior and negative cases satisfy the parent requirements.

- [ ] The concrete Steps contracts and negative cases pass through the named canonical tests.
- [ ] No duplicated protocol/crypto/runtime engine or fake placeholder client is introduced.
- [ ] Owned changes fit one invocation/commit; direct Markdown task state and parent checklist are synchronized after verification.

## Notes & Discoveries

None.
