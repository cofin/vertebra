---
type: Task
id: "native-postgres-driver:t02"
title: "Validate native connection options and error categories"
description: "Validate native connection options and error categories for the standalone vertebra-postgres native driver."
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:t01"
files:
  - src/rs/vertebra-postgres/src/lib.rs
  - src/rs/vertebra-postgres/src/config.rs
  - src/rs/vertebra-postgres/src/errors.rs
tests:
  - src/rs/vertebra-postgres/src/config.rs
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

# Validate native connection options and error categories

## Objective

Validate native connection options and error categories, with behavior verified through the canonical public seam listed above.

## Context

Read `../spec.md`, the named dependency worksheets, `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md`, `.agents/skills/pyo3-bridge/SKILL.md` and `.agents/bundles/knowledge/patterns/rust/ffi-and-gil-discipline.md`. This task belongs to the complete native PostgreSQL driver on one feature branch and one PR. Its completion is not permission to ship a partial driver or cut over the Python adapters. Public signatures and shared contracts in the full spec govern later task additions. No implementation starts with plan_commit null.

Ownership is exactly the frontmatter list. Shared files are changed only after predecessor tasks complete; preserve unrelated edits. One executor invocation makes one signed conventional local commit. Estimated handwritten changes including tests: 350 lines; generated lock output is reviewed separately. Keep the task near400 lines. If the specified work cannot fit or a named prerequisite is missing, stop and `/flow:revise native-postgres-driver`; keep the complete one-PR scope and split the worksheet boundary.



## Steps

1. Read ../contracts.md::Connection constructors as the exact public option inventory/defaults for both client and pool constructors. Add direct configuration RED cases for missing/empty/NUL user/database/host/server_name, NUL password/application_name, invalid signed port, nonfinite/nonpositive timeout, mismatched PEM pair and invalid five-spelling sslmode. Formatting errors never prints password/private key. No DNS/network/credential refresh occurs in validation.
2. Declare the full owned PostgresConfig option record matching every field/default in contracts.md, including unix_socket, channel_binding, session_gucs, keepalive controls, mutually exclusive cloud resources/network types, enable_iam_auth, cloud_auth_options and iam_auth_options. Copy maps into owned String maps; owned field declarations do not introduce extra public setters. This task implements base scalar/TLS validation; t10, t11, t12, t13, t14, t15, t16 provide typed cloud internals and t20 completes advanced option normalization plus Unix/startup projection before any Python facade task. t23/t24 never expose a partial constructor.
3. Signed port converts to u16 only after1..65535 validation; finite positive timeout converts to Duration. Map exact disable/prefer/require/verify-ca/verify-full spellings to shared TlsMode. TCP host is IPv4/IPv6/DNS, never a URI or Unix directory; the separate unix_socket is an exact socket file path. Existing adapter owns the sole URL/libpq parser. Keep secrets out of Debug and never reparse DSNs here.
4. Add native errors.rs::PostgresError {Config,Interface,UnsupportedAuth,UnsupportedFeature,Transport,Protocol,Server{sqlstate,message}}. Retain five-byte ASCII SQLSTATE and sanitized server M field, dropping detail/hint/context and secret literals. Connection-stage Python conversion later imports exact ConnectionFailure for08/transport/protocol, InterfaceError/NotSupportedError for state/mechanism/platform errors and DatabaseError for28/other server errors; input errors become ValueError/TypeError. Commit feat(postgres): validate owned connection options.

## Verification

Write the named failing tests before behavior changes. Initial RED is the absent named module/public method or the stated incorrect behavior; record the actual failing assertion/diagnostic. After implementation, run exactly:

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --lib config::tests
cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings
```

Final exit0 is required. These commands are planned, not executed evidence. Record each command/exit and public assertions during execution. Source rebuild precedes Python tests; never validate a stale extension. No external write, workflow dispatch, tag mutation or push is authorized here.

## Acceptance Criteria

- [ ] R2, R3: the owned public behavior and negative cases satisfy the parent requirements.

- [ ] The concrete Steps contracts and negative cases pass through the named canonical tests.
- [ ] No duplicated protocol/crypto/runtime engine or fake placeholder client is introduced.
- [ ] Owned changes fit one invocation/commit; direct Markdown task state and parent checklist are synchronized after verification.

## Notes & Discoveries

None.
