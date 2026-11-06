---
type: Task
id: native-postgres-driver:t05
title: Negotiate PostgreSQL SSLRequest and verify TLS identities
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
- native-postgres-driver:t04
files:
- src/rs/vertebra-postgres/Cargo.toml
- src/rs/Cargo.lock
- src/rs/vertebra-postgres/src/lib.rs
- src/rs/vertebra-postgres/src/tls.rs
- src/rs/vertebra-postgres/src/session.rs
- src/rs/vertebra-postgres/tests/postgres_tls_wire_tests.rs
tests:
- src/rs/vertebra-postgres/tests/postgres_tls_wire_tests.rs::tls_modes
- src/rs/vertebra-postgres/tests/postgres_tls_wire_tests.rs::tls_verification
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
- R4
---

# Negotiate PostgreSQL SSLRequest and verify TLS identities

## Objective

Negotiate PostgreSQL SSLRequest and verify TLS identities, with behavior verified through the canonical public seam listed above.

## Context

Read `../spec.md`, the named dependency worksheets, `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md`, `.agents/skills/pyo3-bridge/SKILL.md` and `.agents/bundles/knowledge/patterns/rust/ffi-and-gil-discipline.md`. This task belongs to the complete native PostgreSQL driver on one feature branch and one PR. Its completion is not permission to ship a partial driver or cut over the Python adapters. Public signatures and shared contracts in the full spec govern later task additions. No implementation starts with plan_commit null.

Ownership is exactly the frontmatter list. Shared files are changed only after predecessor tasks complete; preserve unrelated edits. One executor invocation makes one signed conventional local commit. Estimated handwritten changes including tests: 360 lines; generated lock output is reviewed separately. Keep the task near400 lines. If the specified work cannot fit or a named prerequisite is missing, stop and `/flow:revise native-postgres-driver`; keep the complete one-PR scope and split the worksheet boundary.



## Steps

1. Add canonical TLS mock listener tests with rcgen/tokio-rustls dev fixtures. RED table: Disable first sends startup; Prefer+N plaintext; all enabled modes+S TLS; strict+N rejection; unknown byte/EOF; TLS failure after S never reconnects plaintext. Assert SSLRequest exactly[0,0,0,8,4,210,22,47]. Verify trusted/untrusted CA, wrong hostname (VerifyCa succeeds, VerifyFull fails), server_name override, expired cert, Require with/without supplied CA and valid/missing/mismatched mTLS client identity.
2. tls.rs::connect clones original shared ConnectOptions with tls_mode Disable for initial PostgreSQL TCP dial, because shared TransportStream::connect otherwise starts TLS immediately for strict modes. Enabled modes send upstream frontend::ssl_request, flush, read exactly one byte; S upgrades PlainTcp with original options via shared upgrade_tls, N permits plaintext only Prefer. No retry or post-S downgrade. Require without CA encrypts; supplied CA verifies; VerifyCa checks chain without host and VerifyFull checks both. Prefer+S follows shared Require policy. All PEM bytes pass to shared verifier; install shared ring provider.
3. Replace t04 plaintext dial with this seam inside its existing outer deadline. Tests delay SSLRequest/TLS using controlled events/paused time and prove eventual socket closure/Closed at expiry. Keep TLS peer transport accessible only in Rust for later certificate binding and keepalive. Do not change existing shared TLS behavior or introduce native-tls stack.
4. Commit feat(postgres): negotiate protocol TLS with shared transport.

## Verification

Write the named failing tests before behavior changes. Initial RED is the absent named module/public method or the stated incorrect behavior; record the actual failing assertion/diagnostic. After implementation, run exactly:

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_tls_wire_tests
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres
```

Final exit0 is required. These commands are planned, not executed evidence. Record each command/exit and public assertions during execution. Source rebuild precedes Python tests; never validate a stale extension. No external write, workflow dispatch, tag mutation or push is authorized here.

## Acceptance Criteria

- [ ] R4: the owned public behavior and negative cases satisfy the parent requirements.

- [ ] The concrete Steps contracts and negative cases pass through the named canonical tests.
- [ ] No duplicated protocol/crypto/runtime engine or fake placeholder client is introduced.
- [ ] Owned changes fit one invocation/commit; direct Markdown task state and parent checklist are synchronized after verification.

## Notes & Discoveries

None.
