---
type: Task
id: native-postgres-driver:t09
title: Select and verify SCRAM-SHA-256-PLUS channel binding
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
- native-postgres-driver:t08
files:
- src/rs/vertebra-postgres/src/config.rs
- src/rs/vertebra-postgres/src/auth.rs
- src/rs/vertebra-postgres/src/session.rs
- src/rs/vertebra-postgres/tests/postgres_auth_wire_tests.rs
- src/rs/vertebra-postgres/tests/postgres_tls_wire_tests.rs
tests:
- src/rs/vertebra-postgres/tests/postgres_auth_wire_tests.rs::scram_plus_policy
- src/rs/vertebra-postgres/tests/postgres_tls_wire_tests.rs::scram_plus_over_tls
- src/rs/vertebra-postgres/tests/postgres_tls_wire_tests.rs::scram_plus_plain_offer_gs2_y
- src/rs/vertebra-postgres/tests/postgres_tls_wire_tests.rs::scram_plus_downgrade_rejected
plan_revision: 1
plan_commit: null
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
updated_at: '2026-11-05T20:03:00Z'
commit: null
tags:
- postgres
- connection
- native
requirements:
- R5
---

# Select and verify SCRAM-SHA-256-PLUS channel binding

## Objective

Select and verify SCRAM-SHA-256-PLUS channel binding, with behavior verified through the canonical public seam listed above.

## Context

Read `../spec.md`, the named dependency worksheets, `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md`, `.agents/skills/pyo3-bridge/SKILL.md` and `.agents/bundles/knowledge/patterns/rust/ffi-and-gil-discipline.md`. This task belongs to the complete native PostgreSQL driver on one feature branch and one PR. Its completion is not permission to ship a partial driver or cut over the Python adapters. Public signatures and shared contracts in the full spec govern later task additions. No implementation starts with plan_commit null.

Ownership is exactly the frontmatter list. Shared files are changed only after predecessor tasks complete; preserve unrelated edits. One executor invocation makes one signed conventional local commit. Estimated handwritten changes including tests: 350 lines; generated lock output is reviewed separately. Keep the task near400 lines. If the specified work cannot fit or a named prerequisite is missing, stop and `/flow:revise native-postgres-driver`; keep the complete one-PR scope and split the worksheet boundary.



## Steps

1. Add RED mechanism-policy table cases for TLS with endpoint binding versus plaintext/unsupported-binding, policies Disable/Prefer/Require, PLUS-only/plain-only/both, and trust/clear/MD5 under Require. Full native config owns ChannelBindingPolicy default Prefer and public constructors expose channel_binding="prefer" exactly as ../contracts.md. Validate disable/prefer/require once in native configuration; the adapter and pool constructors forward this same field.
2. Disable chooses only plain SCRAM with ChannelBinding::unsupported(), emitting GS2 n,,. Prefer with usable TLS endpoint binding chooses PLUS when advertised; if only plain SCRAM is advertised, construct the plain exchange with ChannelBinding::unrequested(), emitting GS2 y,, to declare supported-but-unrequested binding per RFC5802 section6. Prefer on plaintext or genuinely unavailable binding uses ChannelBinding::unsupported() and n,,; it fails if the only offered mechanism is unusable PLUS. Require demands usable TLS binding plus advertised PLUS and rejects trust/password/MD5/plain before credentials. Never retry a failed PLUS or y,, exchange with n,,, another mechanism or a new plaintext connection.
3. PLUS constructs upstream ScramSha256 with ChannelBinding::tls_server_end_point(actual_leaf_digest). Keep the existing ordered auth state machine and verification gates. TLS scripted server verifies c= contains GS2 p=tls-server-end-point header plus expected leaf digest; leaf mismatch/changed cert or invalid server proof closes. Add scram_plus_plain_offer_gs2_y: trusted TLS supplies usable binding but advertises only SCRAM-SHA-256; assert actual client-first begins y,, and client-final c= decodes to y,,. A valid plain-only server proof may complete this exchange.
4. Add scram_plus_downgrade_rejected: the scripted server represents a PLUS-capable server whose advertisement is stripped to plain, receives y,,, performs the ordinary Continue/client-final exchange, then sends SASLFinal e=server-does-support-channel-binding. The upstream finish() error must close without Ready, credential retry, second startup or n,, replay. This tests downgrade signaling on the wire, rather than only constructor selection. Continue using pinned public ChannelBinding APIs and upstream SCRAM crypto; no independent implementation. Commit feat(postgres): authenticate certificate-bound SCRAM sessions.

## Verification

Write the named failing tests before behavior changes. Initial RED is the absent named module/public method or the stated incorrect behavior; record the actual failing assertion/diagnostic. After implementation, run exactly:

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_auth_wire_tests scram_plus_
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_tls_wire_tests scram_plus_
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres
```

Final exit0 is required. These commands are planned, not executed evidence. Record each command/exit and public assertions during execution. Source rebuild precedes Python tests; never validate a stale extension. No external write, workflow dispatch, tag mutation or push is authorized here.

## Acceptance Criteria

- [ ] R5: usable TLS plus plain-only advertisement emits y,, through ChannelBinding::unrequested(); Disable/unavailable binding emits n,, and downgrade rejection cannot cause replay.

- [ ] The concrete Steps contracts and negative cases pass through the named canonical tests.
- [ ] No duplicated protocol/crypto/runtime engine or fake placeholder client is introduced.
- [ ] Owned changes fit one invocation/commit; direct Markdown task state and parent checklist are synchronized after verification.

## Notes & Discoveries

None.
