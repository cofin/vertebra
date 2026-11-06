---
type: Task
id: native-postgres-driver:t18
title: Authenticate automatic AlloyDB metadata and IAM sessions
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
- native-postgres-driver:t17
files:
- src/rs/vertebra-db/src/connection.rs
- src/rs/vertebra-postgres/src/cloud/mod.rs
- src/rs/vertebra-postgres/src/session.rs
- src/rs/vertebra-postgres/tests/postgres_cloud_wire_tests.rs
tests:
- src/rs/vertebra-db/src/connection.rs::tests::alloydb_split_transport_stages
- src/rs/vertebra-postgres/tests/postgres_cloud_wire_tests.rs::alloydb_metadata_iam
- src/rs/vertebra-postgres/tests/postgres_cloud_wire_tests.rs::alloydb_token_expires_during_tls
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
- R9
---

# Authenticate automatic AlloyDB metadata and IAM sessions

## Objective

Authenticate automatic AlloyDB metadata and IAM sessions, with behavior verified through the canonical public seam listed above.

## Context

Read `../spec.md`, the named dependency worksheets, `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md`, `.agents/skills/pyo3-bridge/SKILL.md` and `.agents/bundles/knowledge/patterns/rust/ffi-and-gil-discipline.md`. This task belongs to the complete native PostgreSQL driver on one feature branch and one PR. Its completion is not permission to ship a partial driver or cut over the Python adapters. Public signatures and shared contracts in the full spec govern later task additions. No implementation starts with plan_commit null.

Ownership is exactly the frontmatter list. Shared files are changed only after predecessor tasks complete; preserve unrelated edits. One executor invocation makes one signed conventional local commit. Estimated handwritten changes including tests: approximately430 lines, covering one cohesive shared TLS/metadata extraction, PG guard placement and real held-TLS regression; generated lock output is reviewed separately. The approximately430-line estimate uses the near400 allowance for this cohesive ownership change; do not split merely to create a passthrough seam. If actual implementation becomes genuinely oversized or a prerequisite is missing, revise ownership before adding scope and preserve the complete one-PR requirement.



## Steps

1. Add RED canonical shared transport tests in connection.rs::tests::alloydb_split_transport_stages. A mock verified AlloyDB TLS listener records bytes received after the completed handshake: the TLS-only stage must return without MetadataExchangeRequest. The legacy TransportStream::connect path still sends metadata exactly once and preserves its existing response/error behavior. Keep existing cloudsql_mtls_verifies_instance_cn_and_alloydb_runs_metadata_exchange as a compatibility regression.
2. Extract public TransportStream::connect_alloydb_tls(options:&ConnectOptions)->Result<TransportStream,TransportError> in shared connection.rs. It accepts only EndpointTarget::AlloyDb, performing existing TCP/TLS1.3/peer-identity verification and returning the verified TLS stream before metadata. Reuse existing cloud TLS/identity builders from t14; do not introduce a second TLS verifier/parser, credential registry or metadata encoder. Callers drive this stage inside their original absolute deadline; it must not start a fresh per-stage timeout budget.
3. Expose module-level public finish_alloydb_metadata(stream:&mut TransportStream,options:&ConnectOptions,absolute_deadline:tokio::time::Instant)->Result<(),TransportError>. Require the AlloyDB endpoint and TLS transport, then drive the existing exchange_alloydb_metadata_over_stream helper under timeout_at(absolute_deadline,...). An elapsed deadline fails before writing; no duration reset, second handshake, duplicate codec or token-lifetime policy belongs in shared transport. Refactor existing TransportStream::connect/connect_inner to compose connect_alloydb_tls and finish_alloydb_metadata with the one deadline computed at connect entry, preserving non-PostgreSQL legacy behavior and raw-token API compatibility.
4. cloud/mod.rs::connect_alloydb obtains prepared material from t13/t16, including actual ScopedLoginSnapshot. Under the original ConnectAttempt deadline, call TransportStream::connect_alloydb_tls, then immediately call login_snapshot.ensure_usable(clock.now_ms()) after TLS returns and before invoking finish_alloydb_metadata. A failed guard maps native PostgresError::Auth to existing core GoogleCloudAuthError, drops the stream and emits zero token-bearing metadata bytes. On success invoke finish_alloydb_metadata with that same deadline, then enter the existing PostgreSQL startup/auth engine. Built-in and IAM modes both use alloydb.login metadata; IAM also checks the same snapshot before its password/SASL response. Default5433 and existing AuthType2 IAM/1 built-in remain unchanged.
5. Add actual alloydb_token_expires_during_tls integration using the existing auth clock fixture plus server barriers: issue a genuinely supplied finite login token valid before dial; hold the real verified TLS handshake at a server-side gate; move the injected clock exactly to expiry-minus-effective-skew (and separately beyond expiry) while held, release TLS, then observe client completion. The TLS stage must complete before the guard rejects, the server must observe EOF with zero bytes of the length-prefixed MetadataExchangeRequest, and no PostgreSQL startup or retry occurs. A sibling still-valid boundary case proves exactly one metadata exchange; a held metadata response proves original-deadline exhaustion without budget reset. Purposeful clock advance/barrier release tests this boundary; an unrelated password/SASL test or pre-TLS rejection is insufficient. No fake expiry, production sleep or new policy registry is introduced.
6. Retain existing nonzero/malformed/truncated metadata response, wrong-SAN/valid-CA, TLS1.2 rejection, refresh failure and normal auth tests. Do not send PostgreSQL SSLRequest inside cloud TLS, expose tokens, perform a second metadata handshake or fall back to direct TCP. Construct caches inside the PG optional crate from distinct cloud_auth_options/iam_auth_options. Commit feat(postgres): guard AlloyDB credentials after shared TLS setup.

## Verification

Write the named failing tests before behavior changes. Initial RED is the absent named module/public method or the stated incorrect behavior; record the actual failing assertion/diagnostic. After implementation, run exactly:

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-db --lib alloydb_split_transport_stages
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-db --lib cloudsql_mtls_verifies_instance_cn_and_alloydb_runs_metadata_exchange
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_cloud_wire_tests alloydb_token_expires_during_tls
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_cloud_wire_tests alloydb_
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres
```

Final exit0 is required. These commands are planned, not executed evidence. Record each command/exit and public assertions during execution. Source rebuild precedes Python tests; never validate a stale extension. No external write, workflow dispatch, tag mutation or push is authorized here.

## Acceptance Criteria

- [ ] R5/R9: PG checks the actual scoped token immediately after verified AlloyDB TLS and before zero-or-one metadata exchange; expired/inside-skew fails with canonical provider auth identity and zero metadata bytes.
- [ ] Legacy TransportStream::connect composes the same shared stages without changing non-PG behavior; every stage consumes the original absolute deadline.

- [ ] The concrete Steps contracts and negative cases pass through the named canonical tests.
- [ ] No duplicated protocol/crypto/runtime engine or fake placeholder client is introduced.
- [ ] Owned changes fit one invocation/commit; direct Markdown task state and parent checklist are synchronized after verification.

## Notes & Discoveries

None.
