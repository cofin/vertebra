---
type: Task
id: native-postgres-driver:t14
title: Enforce cloud TLS1.3 and service-specific peer identities
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
- native-postgres-driver:t13
files:
- src/rs/vertebra-db/src/connection.rs
- src/rs/vertebra-postgres/src/cloud/material.rs
- src/rs/vertebra-postgres/tests/postgres_cloud_wire_tests.rs
tests:
- src/rs/vertebra-db/src/connection.rs::tests::cloud_peer_identities
- src/rs/vertebra-postgres/tests/postgres_cloud_wire_tests.rs::cloud_identity_policy
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
- R9
---

# Enforce cloud TLS1.3 and service-specific peer identities

## Objective

Enforce cloud TLS1.3 and service-specific peer identities, with behavior verified through the canonical public seam listed above.

## Context

Read `../spec.md`, the named dependency worksheets, `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md`, `.agents/skills/pyo3-bridge/SKILL.md` and `.agents/bundles/knowledge/patterns/rust/ffi-and-gil-discipline.md`. This task belongs to the complete native PostgreSQL driver on one feature branch and one PR. Its completion is not permission to ship a partial driver or cut over the Python adapters. Public signatures and shared contracts in the full spec govern later task additions. No implementation starts with plan_commit null.

Ownership is exactly the frontmatter list. Shared files are changed only after predecessor tasks complete; preserve unrelated edits. One executor invocation makes one signed conventional local commit. Estimated handwritten changes including tests: 390 lines; generated lock output is reviewed separately. Keep the task near400 lines. If the specified work cannot fit or a named prerequisite is missing, stop and `/flow:revise native-postgres-driver`; keep the complete one-PR scope and split the worksheet boundary.



## Steps

1. Add RED tests for AlloyDB wrong SAN with valid CA, selected IP/PSC DNS SAN success, TLS1.2 rejection, Cloud SQL DNS-SAN-only success, legacy exact CN success, wrong SAN/CN rejection and CA failure. The current AlloyDB cluster:_/expected_cnNone path is CA-only and must be repaired.
2. Add explicit CloudPeerIdentity to shared cloud EndpointTarget variants; update existing construction tests in this owned file. AlloyDb identity is selected IP or DNS SAN; after CA/signature verification require SAN match, with no CN/CA-only fallback. CloudSql identity carries metadata/configured DNS plus expected nonempty project:instance CN: verify CA first, try DNS SAN with normalized trailing-dot retry, then exact CN fallback; reject if both fail. Follow maintained official Cloud SQL connector tls_verify.go semantics.
3. Shared cloud TLS config permitsTLS1.3 only, preserving direct-TCP sslmode behavior. Feed verified material from t12/t13 into this explicit policy, never default silently to old policies. Retain actual leaf for later SCRAM binding. Commit fix(db): verify cloud database peer identities.

## Verification

Write the named failing tests before behavior changes. Initial RED is the absent named module/public method or the stated incorrect behavior; record the actual failing assertion/diagnostic. After implementation, run exactly:

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-db --lib cloud_peer_identities
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_cloud_wire_tests cloud_identity_
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-db --lib cloudsql_mtls_
```

Final exit0 is required. These commands are planned, not executed evidence. Record each command/exit and public assertions during execution. Source rebuild precedes Python tests; never validate a stale extension. No external write, workflow dispatch, tag mutation or push is authorized here.

## Acceptance Criteria

- [ ] R4, R9: the owned public behavior and negative cases satisfy the parent requirements.

- [ ] The concrete Steps contracts and negative cases pass through the named canonical tests.
- [ ] No duplicated protocol/crypto/runtime engine or fake placeholder client is introduced.
- [ ] Owned changes fit one invocation/commit; direct Markdown task state and parent checklist are synchronized after verification.

## Notes & Discoveries

None.
