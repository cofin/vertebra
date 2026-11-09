---
type: Task
id: "native-postgres-driver:t06"
title: "Authenticate cleartext and MD5 startup exchanges"
description: "Authenticate cleartext and MD5 startup exchanges for the standalone vertebra-postgres native driver."
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:t05"
files:
  - src/rs/vertebra-postgres/src/lib.rs
  - src/rs/vertebra-postgres/src/auth.rs
  - src/rs/vertebra-postgres/src/session.rs
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

# Authenticate cleartext and MD5 startup exchanges

## Objective

Authenticate cleartext and MD5 startup exchanges, with behavior verified through the canonical public seam listed above.

## Context

Read `../spec.md`, the named dependency worksheets, `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md`, `.agents/skills/pyo3-bridge/SKILL.md` and `.agents/bundles/knowledge/patterns/rust/ffi-and-gil-discipline.md`. This task belongs to the complete native PostgreSQL driver on one feature branch and one PR. Its completion is not permission to ship a partial driver or cut over the Python adapters. Public signatures and shared contracts in the full spec govern later task additions. No implementation starts with plan_commit null.

Ownership is exactly the frontmatter list. Shared files are changed only after predecessor tasks complete; preserve unrelated edits. One executor invocation makes one signed conventional local commit. Estimated handwritten changes including tests: 240 lines; generated lock output is reviewed separately. Keep the task near400 lines. If the specified work cannot fit or a named prerequisite is missing, stop and `/flow:revise native-postgres-driver`; keep the complete one-PR scope and split the worksheet boundary.



## Steps

1. Introduce canonical auth scripted tests for trust/cleartext/MD5, no password, wrong-password28P01, duplicate challenge/AuthenticationOk, GSS/SSPI/Kerberos unsupported methods and error redaction. Assert actual frontend password bytes and server EOF on failure. Test helpers remain in this canonical auth file; no custom production crypto.
2. auth.rs::Authentication initially has Initial,PasswordSent,Complete. Cleartext and MD5 only from Initial; password reply through upstream password_message, MD5 through authentication::md5_hash(user.as_bytes(),password.as_bytes(),[u8;4]). PasswordSent accepts only AuthenticationOk, trust accepts it from Initial. Unknown methods yield UnsupportedAuth. No same-stream retry. Plaintext is allowed only when caller explicitly selects Disable/Prefer+N; later binding Require policy rejects non-SCRAM mechanisms.
3. Session delegates auth variants to this state machine, accepting ParameterStatus/key/Ready only after Complete; notices leave phase unchanged. Keep all transport/deadline/publication ownership in the existing engine. Commit feat(postgres): authenticate password and MD5 connections.

## Verification

Write the named failing tests before behavior changes. Initial RED is the absent named module/public method or the stated incorrect behavior; record the actual failing assertion/diagnostic. After implementation, run exactly:

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_auth_wire_tests password_
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
