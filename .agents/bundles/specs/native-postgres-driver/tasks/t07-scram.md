---
type: Task
id: "native-postgres-driver:t07"
title: "Authenticate ordered SCRAM-SHA-256 exchanges"
description: "Authenticate ordered SCRAM-SHA-256 exchanges for the standalone vertebra-postgres native driver."
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:t06"
files:
  - src/rs/vertebra-postgres/Cargo.toml
  - src/rs/Cargo.lock
  - src/rs/vertebra-postgres/src/auth.rs
  - src/rs/vertebra-postgres/tests/postgres_auth_wire_tests.rs
tests:
  - src/rs/vertebra-postgres/tests/postgres_auth_wire_tests.rs
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

# Authenticate ordered SCRAM-SHA-256 exchanges

## Objective

Authenticate ordered SCRAM-SHA-256 exchanges, with behavior verified through the canonical public seam listed above.

## Context

Read `../spec.md`, the named dependency worksheets, `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md`, `.agents/skills/pyo3-bridge/SKILL.md` and `.agents/bundles/knowledge/patterns/rust/ffi-and-gil-discipline.md`. This task belongs to the complete native PostgreSQL driver on one feature branch and one PR. Its completion is not permission to ship a partial driver or cut over the Python adapters. Public signatures and shared contracts in the full spec govern later task additions. No implementation starts with plan_commit null.

Ownership is exactly the frontmatter list. Shared files are changed only after predecessor tasks complete; preserve unrelated edits. One executor invocation makes one signed conventional local commit. Estimated handwritten changes including tests: 350 lines; generated lock output is reviewed separately. Keep the task near400 lines. If the specified work cannot fit or a named prerequisite is missing, stop and `/flow:revise native-postgres-driver`; keep the complete one-PR scope and split the worksheet boundary.



## Steps

1. Add SCRAM RED cases offered plain/PLUS+plain, malformed/empty mechanism lists, zero/100001/nondecimal/overflow/duplicate iteration, server nonce mismatch/equality, wrong verifier, duplicate Continue/Final and premature AuthenticationOk. A valid scripted server test uses known secret/salt vtb-test-salt/i4096; salted-password hex2482c2af90bd00962845709770664bb70a090bcbd937e9d1fa1f855044d513bb. Test-only hmac/sha2/base64 derive ServerKey and server-proof against actual client nonce/auth-message; this is server-fixture code, never client production crypto. tokio-postgres dev oracle authenticates the same valid script.
2. Extend auth phases ScramStarted,ScramFinalSent,ScramVerifiedAwaitingOk,Complete. Use pinned ScramSha256::new(password,ChannelBinding::unsupported), message->frontend::sasl_initial_response, Continue->update then message->sasl_response, Final->finish then ScramVerifiedAwaitingOk, AuthenticationOk->Complete. Only verified-awaiting-Ok accepts final Ok; reject duplicate Final. Never call message after finish. Before update minimally validate i1..=100000 and require nonce strictly extends initial client nonce; upstream checks prefix only. Leave SASLprep/base64/crypto/complete grammar to upstream.
3. Initial selection supports plain only in this task; PLUS-only explicitly UnsupportedAuth until t09. After bounded PBKDF2 recheck original deadline before publication. No million-iteration custom cap. Commit feat(postgres): validate SCRAM startup authentication.

## Verification

Write the named failing tests before behavior changes. Initial RED is the absent named module/public method or the stated incorrect behavior; record the actual failing assertion/diagnostic. After implementation, run exactly:

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_auth_wire_tests scram_
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres
```

Final exit0 is required. These commands are planned, not executed evidence. Record each command/exit and public assertions during execution. Source rebuild precedes Python tests; never validate a stale extension. No external write, workflow dispatch, tag mutation or push is authorized here.

## Acceptance Criteria

- [ ] R5: the owned public behavior and negative cases satisfy the parent requirements.

- [ ] The concrete Steps contracts and negative cases pass through the named canonical tests.
- [ ] No duplicated protocol/crypto/runtime engine or fake placeholder client is introduced.
- [ ] Owned changes fit one invocation/commit; direct Markdown task state and parent checklist are synchronized after verification.

## Notes & Discoveries

None.
