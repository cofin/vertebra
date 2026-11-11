---
type: Task
id: "native-postgres-driver:t04"
title: "Establish authenticated trust sessions with publication ownership"
description: "Establish authenticated trust sessions with publication ownership for the standalone vertebra-postgres native driver."
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:t03"
files:
  - src/rs/vertebra-postgres/tests/support/mod.rs
  - src/rs/vertebra-postgres/src/lib.rs
  - src/rs/vertebra-postgres/src/session.rs
  - src/rs/vertebra-postgres/tests/postgres_wire_tests.rs
tests:
  - src/rs/vertebra-postgres/tests/postgres_wire_tests.rs
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

# Establish authenticated trust sessions with publication ownership

## Objective

Establish authenticated trust sessions with publication ownership, with behavior verified through the canonical public seam listed above.

## Context

Read `../spec.md`, the named dependency worksheets, `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md`, `.agents/skills/pyo3-bridge/SKILL.md` and `.agents/bundles/knowledge/patterns/rust/ffi-and-gil-discipline.md`. This task belongs to the complete native PostgreSQL driver on one feature branch and one PR. Its completion is not permission to ship a partial driver or cut over the Python adapters. Public signatures and shared contracts in the full spec govern later task additions. No implementation starts with plan_commit null.

Ownership is exactly the frontmatter list. Shared files are changed only after predecessor tasks complete; preserve unrelated edits. One executor invocation makes one signed conventional local commit. Estimated handwritten changes including tests: 390 lines; generated lock output is reviewed separately. Keep the task near400 lines. If the specified work cannot fit or a named prerequisite is missing, stop and `/flow:revise native-postgres-driver`; keep the complete one-PR scope and split the worksheet boundary.



## Steps

1. Add canonical loopback tests startup_trust, startup_requires_auth_utf8_idle, startup_error_sqlstate, total_deadline, duplicate_auth_rejected, repeated_connect_and_close, overlapping_connect_fails, dropped_attempt_closes and dropped_pending_session_closes. Use barriers/channels and server-observed Terminate/EOF, not sleeps or assertions that merely say a task aborted. Put only reusable socket/barrier helpers in tests/support/mod.rs, imported as mod support by sibling canonical wire test modules; never import one test module into another.
2. session.rs::PostgresConnection owns live TransportStream, ordered startup parameters, Rust-only BackendKeyData and transaction status. session.rs::SessionHandle owns config and short mutex lifecycle Configured->Connecting->Ready(PostgresConnection) or terminal Closed. It is a physical connection owner, never a pool. begin_connect reserves synchronously and records absolute deadline. AlreadyReady is no-op; Connecting/Closed errors Interface. close configured/closed is idempotent, Connecting errors, Ready takes transport/sets Closed before I/O; Terminate/shutdown best effort <=100ms then drop.
3. ConnectAttempt owns reservation rollback-to-Closed and uses one timeout_at across queue/dial/startup/auth; initially accept directTCP Disable/trust and explicitly reject unsupported TLS/password challenges until subsequent tasks; t20 adds the existing Unix transport path before Python facade exposure. Startup succeeds only after one AuthenticationOk, ParameterStatus client_encoding=UTF8, optional one BackendKeyData, ReadyForQuery(I); reject order violations, Ready T/E, duplicate keys/auth or unexpected query frames. ParameterStatus last value wins and is owned UTF8. Notice frames may be consumed without logging; ErrorResponse fails via native categories.
4. Return PendingSession with live connection and armed reservation guard. Only commit atomically publishes Ready and disarms it, after rechecking deadline; dropping attempt/pending closes socket/state. No await/Python callback while holding std mutex. closed is true until Ready and after closure; copied parameters empty until successful commit and retained after close. AlreadyReady owns no guard/socket, so cancelling its no-op does not close Ready. Expose PostgresConnection as real rlib seam for query/pool tasks; shared Connection trait integration belongs with real ping/cancel methods later, never placeholders.
5. Commit feat(postgres): own startup session publication.

## Verification

Write the named failing tests before behavior changes. Initial RED is the absent named module/public method or the stated incorrect behavior; record the actual failing assertion/diagnostic. After implementation, run exactly:

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_wire_tests
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres
```

Final exit0 is required. These commands are planned, not executed evidence. Record each command/exit and public assertions during execution. Source rebuild precedes Python tests; never validate a stale extension. No external write, workflow dispatch, tag mutation or push is authorized here.

## Acceptance Criteria

- [ ] R3, R7: the owned public behavior and negative cases satisfy the parent requirements.

- [ ] The concrete Steps contracts and negative cases pass through the named canonical tests.
- [ ] No duplicated protocol/crypto/runtime engine or fake placeholder client is introduced.
- [ ] Owned changes fit one invocation/commit; direct Markdown task state and parent checklist are synchronized after verification.

## Notes & Discoveries

None.
