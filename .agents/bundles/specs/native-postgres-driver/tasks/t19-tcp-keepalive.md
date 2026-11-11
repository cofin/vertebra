---
type: Task
id: "native-postgres-driver:t19"
title: "Apply validated TCP keepalive controls to physical sessions"
description: "Apply validated TCP keepalive controls to physical sessions for the standalone vertebra-postgres native driver."
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:t18"
files:
  - .github/workflows/release.yaml
  - src/rs/Cargo.toml
  - src/rs/Cargo.lock
  - src/rs/vertebra-db/Cargo.toml
  - src/rs/vertebra-db/src/connection.rs
  - src/rs/vertebra-postgres/src/config.rs
  - src/rs/vertebra-postgres/src/session.rs
  - src/rs/vertebra-postgres/tests/postgres_wire_tests.rs
tests:
  - src/rs/vertebra-postgres/tests/postgres_wire_tests.rs
  - src/rs/vertebra-db/src/connection.rs
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

# Apply validated TCP keepalive controls to physical sessions

## Objective

Apply validated TCP keepalive controls to physical sessions, with behavior verified through the canonical public seam listed above.

## Context

Read `../spec.md`, the named dependency worksheets, `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md`, `.agents/skills/pyo3-bridge/SKILL.md` and `.agents/bundles/knowledge/patterns/rust/ffi-and-gil-discipline.md`. This task belongs to the complete native PostgreSQL driver on one feature branch and one PR. Its completion is not permission to ship a partial driver or cut over the Python adapters. Public signatures and shared contracts in the full spec govern later task additions. No implementation starts with plan_commit null.

Ownership is exactly the frontmatter list. Shared files are changed only after predecessor tasks complete; preserve unrelated edits. One executor invocation makes one signed conventional local commit. Estimated handwritten changes including tests: 300 lines; generated lock output is reviewed separately. Keep the task near400 lines. If the specified work cannot fit or a named prerequisite is missing, stop and `/flow:revise native-postgres-driver`; keep the complete one-PR scope and split the worksheet boundary.



## Steps

1. Add RED config cases keepalive enabled/disabled, positive idle/interval/retry counts, zero/out-of-range values and unsupported platform options. The public constructor exposes keepalives=True, keepalives_idle=None, keepalives_interval=None and keepalives_count=None exactly as ../contracts.md. Native physical config carries TcpKeepaliveConfig with those values, not Python socket objects. t20 validates full projection; no future setter is assumed.
2. Use socket2="=0.6.5" already resolved in workspace lock as an explicit dependency (with required platform feature for retries). Introduce shared TransportStream::set_tcp_keepalive(TcpKeepaliveConfig) selecting underlying TCP for PlainTcp or TlsTcp.get_ref().0, never Unix. Use SockRef/set_keepalive and TcpKeepalive.with_time/with_interval/with_retries under documented platform cfgs. Unsupported requested controls return typed InvalidConfig, never silent ignore; disabled clears SO_KEEPALIVE and rejects incompatible tuning values.
3. Apply once to the physical socket before startup after direct/cloud transport completes, under total deadline; no separate dial or default behavior change for existing consumers. Keep shared transport constructor literals stable by adding an explicit method rather than new mandatory ConnectOptions fields. Native tests inspect socket options/readback where supported, not brittle wall-clock packet expiry. Verify Windows target compilation cfgs in existing CI toolchain.
4. Commit feat(postgres): configure physical TCP keepalive.

## Verification

Write the named failing tests before behavior changes. Initial RED is the absent named module/public method or the stated incorrect behavior; record the actual failing assertion/diagnostic. After implementation, run exactly:

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-db --lib tcp_keepalive_
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_wire_tests tcp_keepalive_
```

Windows cfg/type checking belongs to .github/workflows/release.yaml::build_wheels on its existing Windows runner with Rust MSVC and C/C++ build tools. Add a pre-build step only for runner.os=Windows/package.name=postgres running cargo check --manifest-path src/rs/Cargo.toml -p vertebra-db -p vertebra-postgres --all-targets on that native host; do not claim a Linux cargo check --target msvc succeeds without target/toolchain preparation. Local commands above cover the current host only.

Final exit0 is required. These commands are planned, not executed evidence. Record each command/exit and public assertions during execution. Source rebuild precedes Python tests; never validate a stale extension. No external write, workflow dispatch, tag mutation or push is authorized here.

## Acceptance Criteria

- [ ] R2, R3: the owned public behavior and negative cases satisfy the parent requirements.

- [ ] The concrete Steps contracts and negative cases pass through the named canonical tests.
- [ ] No duplicated protocol/crypto/runtime engine or fake placeholder client is introduced.
- [ ] Owned changes fit one invocation/commit; direct Markdown task state and parent checklist are synchronized after verification.

## Notes & Discoveries

None.
