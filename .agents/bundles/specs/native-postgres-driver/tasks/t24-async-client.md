---
type: Task
id: "native-postgres-driver:t24"
title: "Expose async physical sessions with cancellation-safe publication"
description: "Expose async physical sessions with cancellation-safe publication for the standalone vertebra-postgres native driver."
status: stable
state: pending
priority: P1
verification_strategy: regression_tdd
depends_on:
  - "native-postgres-driver:t23"
files:
  - src/rs/vertebra-postgres/src/lib.rs
  - src/rs/vertebra-postgres/src/async_client.rs
  - src/py/vertebra/_postgres.pyi
  - src/py/tests/unit/db/test_postgres_driver.py
tests:
  - src/py/tests/unit/db/test_postgres_driver.py
plan_revision: 2
plan_commit: 9af810f5
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-05T18:25:07Z"
updated_at: "2026-11-07T04:15:00Z"
commit: null
tags:
  - postgres
  - connection
  - native
---

# Expose async physical sessions with cancellation-safe publication

## Objective

Expose async physical sessions with cancellation-safe publication, with behavior verified through the canonical public seam listed above.

## Context

Read `../spec.md`, the named dependency worksheets, `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md`, `.agents/skills/pyo3-bridge/SKILL.md` and `.agents/bundles/knowledge/patterns/rust/ffi-and-gil-discipline.md`. This task belongs to the complete native PostgreSQL driver on one feature branch and one PR. Its completion is not permission to ship a partial driver or cut over the Python adapters. Public signatures and shared contracts in the full spec govern later task additions. No implementation starts with plan_commit null.

Ownership is exactly the frontmatter list. Shared files are changed only after predecessor tasks complete; preserve unrelated edits. One executor invocation makes one signed conventional local commit. Estimated handwritten changes including tests: 390 lines; generated lock output is reviewed separately. Keep the task near400 lines. If the specified work cannot fit or a named prerequisite is missing, stop and `/flow:revise native-postgres-driver`; keep the complete one-PR scope and split the worksheet boundary.



## Steps

1. Add async RED tests reusing t23 fixture: properties/error parity, concurrent clients, event-loop progress, overlap InterfaceError, terminal failure, repeated Ready no-op and cancellation of that no-op preserving the existing session. Setup failure uses a Future subclass whose add_done_callback waits for server-startup Event then raises; after eventual EOF/abort acknowledgment retry sees Closed, not retained Connecting.
2. Implement frozen/immutable_type AsyncPostgresClient with same SessionHandle and complete explicit constructor from ../contracts.md, identical to sync in names/defaults and mapped once into owned PostgresConfig. Full constructor matrix/parity characterization belongs to t25. connect reserves synchronously, then calls spawn_rust_db_task_with_attr using `_vertebra_postgres_completion_drainer`. Worker returns armed PendingSession; map_ok commits and returns None. Keep guard captured until mapper is invoked or discarded. Pre-spawn failure drops attempt; post-spawn setup failure requests abort through t22 and cleans up eventually. AlreadyReady no-op owns no transport/guard. close sets Closed and takes socket before scheduling bounded close; cancellation cannot restore Ready. Stub returns Awaitable[None]. No Python objects enter network future.
3. Pending cancellation holds server Ready frame, cancels task and asserts CancelledError plus server EOF/terminal state after loop drain. Queued-completion race uses controlled fallback: loop.add_reader raises NotImplementedError only for _DbCompletionDrainer registration; wrapped call_soon_threadsafe retains only its drainer callback and signals the test using original scheduling. Once completion is queued, cancel/await the Python future, invoke held drainer and assert dropped PendingSession closes without later Ready. Restore methods in finally; no timing sleeps or production test hooks.
4. Register class/stub and run both facade suites. Shared bridge repair remains t22, not duplicated here. Commit feat(postgres): expose async physical connection client.

## Verification

Write the named failing tests before behavior changes. Initial RED is the absent named module/public method or the stated incorrect behavior; record the actual failing assertion/diagnostic. After implementation, run exactly:

```bash
make dev
uv run --no-sync python -m pytest src/py/tests/unit/db/test_postgres_driver.py -k native_ -q
uv run --no-sync python -m mypy.stubtest vertebra._postgres
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-db --features pyo3 --lib py_async::tests
```

Final exit0 is required. These commands are planned, not executed evidence. Record each command/exit and public assertions during execution. Source rebuild precedes Python tests; never validate a stale extension. No external write, workflow dispatch, tag mutation or push is authorized here.

## Acceptance Criteria

- [ ] R7: the owned public behavior and negative cases satisfy the parent requirements.

- [ ] The concrete Steps contracts and negative cases pass through the named canonical tests.
- [ ] No duplicated protocol/crypto/runtime engine or fake placeholder client is introduced.
- [ ] Owned changes fit one invocation/commit; direct Markdown task state and parent checklist are synchronized after verification.

## Notes & Discoveries

None.
