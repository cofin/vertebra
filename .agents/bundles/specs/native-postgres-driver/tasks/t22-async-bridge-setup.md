---
type: Task
id: native-postgres-driver:t22
title: Abort shared database workers when Python completion setup fails
state: pending
priority: P1
verification_strategy: regression_tdd
depends_on:
- native-postgres-driver:t21
files:
- src/rs/vertebra-db/src/py_async.rs
tests:
- src/rs/vertebra-db/src/py_async.rs::tests::callback_registration_failure_aborts_worker
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
- R7
---

# Abort shared database workers when Python completion setup fails

## Objective

Abort shared database workers when Python completion setup fails, with behavior verified through the canonical public seam listed above.

## Context

Read `../spec.md`, the named dependency worksheets, `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md`, `.agents/skills/pyo3-bridge/SKILL.md` and `.agents/bundles/knowledge/patterns/rust/ffi-and-gil-discipline.md`. This task belongs to the complete native PostgreSQL driver on one feature branch and one PR. Its completion is not permission to ship a partial driver or cut over the Python adapters. Public signatures and shared contracts in the full spec govern later task additions. No implementation starts with plan_commit null.

Ownership is exactly the frontmatter list. Shared files are changed only after predecessor tasks complete; preserve unrelated edits. One executor invocation makes one signed conventional local commit. Estimated handwritten changes including tests: 180 lines; generated lock output is reviewed separately. Keep the task near400 lines. If the specified work cannot fit or a named prerequisite is missing, stop and `/flow:revise native-postgres-driver`; keep the complete one-PR scope and split the worksheet boundary.



## Steps

1. Add RED in existing canonical py_async.rs::tests: a Python asyncio.Future subclass raises from add_done_callback; the Rust future captures DropFlag before first poll and remains pending. Assert original PyErr propagates, future drop acknowledgment eventually arrives and map_ok never publishes. Wait for acknowledgment using a deterministic channel/condvar while py.detach, never sleeps or a GIL-held abort/join wait. A setup failure before future's first poll must also drop captured resources.
2. `spawn_rust_db_task_with_attr` currently calls `abort.abort()` in two separate error branches (`Py::new(DbTaskCanceller)` and `add_done_callback`) before spawning the monitor task. Replace those ad-hoc `abort.abort()` branches with one private armed abort-on-drop worker owner constructed immediately after `spawn` and before either fallible setup step. It owns `JoinHandle`; `Drop` requests abort and drops handle/output. Preserve original `PyErr` for allocation/registration failures. Both paths share the same guard rather than separate manual abort branches.
3. Only after setup succeeds move the armed guard into existing completion monitor; monitor awaits worker and disarms after join completion, so failed monitor setup/drop cannot leave a worker detached. Keep existing panic/error/cancellation mapping and per-extension drainer behavior unchanged. Cancellation/drop cleanup is eventual after runtime abort acknowledgment, not synchronous worker destruction at Python return. No new runtime/queue or public helper.
4. Run existing shared cancellation/panic/drainer regressions as well as new test. Commit fix(db): abort workers on completion setup errors.

## Verification

Write the named failing tests before behavior changes. Initial RED is the absent named module/public method or the stated incorrect behavior; record the actual failing assertion/diagnostic. After implementation, run exactly:

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-db --features pyo3 --lib callback_registration_failure_aborts_worker
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
