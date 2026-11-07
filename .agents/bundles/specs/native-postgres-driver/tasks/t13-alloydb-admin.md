---
type: Task
id: native-postgres-driver:t13
title: Discover AlloyDB network targets and issue metadata-enabled certificates
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
- native-postgres-driver:t12
files:
- src/rs/vertebra-postgres/src/cloud/mod.rs
- src/rs/vertebra-postgres/src/cloud/alloydb.rs
- src/rs/vertebra-postgres/tests/postgres_cloud_wire_tests.rs
tests:
- src/rs/vertebra-postgres/tests/postgres_cloud_wire_tests.rs::alloydb_admin_contract
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
- R9
---

# Discover AlloyDB network targets and issue metadata-enabled certificates

## Objective

Discover AlloyDB network targets and issue metadata-enabled certificates, with behavior verified through the canonical public seam listed above.

## Context

Read `../spec.md`, the named dependency worksheets, `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md`, `.agents/skills/pyo3-bridge/SKILL.md` and `.agents/bundles/knowledge/patterns/rust/ffi-and-gil-discipline.md`. This task belongs to the complete native PostgreSQL driver on one feature branch and one PR. Its completion is not permission to ship a partial driver or cut over the Python adapters. Public signatures and shared contracts in the full spec govern later task additions. No implementation starts with plan_commit null.

Ownership is exactly the frontmatter list. Shared files are changed only after predecessor tasks complete; preserve unrelated edits. One executor invocation makes one signed conventional local commit. Estimated handwritten changes including tests: 370 lines; generated lock output is reviewed separately. Keep the task near400 lines. If the specified work cannot fit or a named prerequisite is missing, stop and `/flow:revise native-postgres-driver`; keep the complete one-PR scope and split the worksheet boundary.



## Steps

1. Add mock Admin RED assertions for versioned instance GET/cluster POST, exact JSON field names, Admin/login token separation, selected public/private/PSC address, missing fields, issued key mismatch and expired chain.
2. Use coherent v1alpha APIs matching maintained official Go connector and PSC fields: GET https://alloydb.googleapis.com/v1alpha/{instance_path}/connectionInfo reads ipAddress(private), publicIpAddress, pscDnsName/pscAutoDnsName. POST https://alloydb.googleapis.com/v1alpha/{cluster_path}:generateClientCertificate with {publicKey:real_RSA_public_PEM,certDuration:"3600s",useMetadataExchange:true}. Omit optional requestId; never invent pem_csr/access_token/clientCert body fields. Admin bearer is cloud-platform.
3. Decode pemCertificateChain:[PEMs] and caCert:PEM; join leaf-first chain and validate generated key/signed NotAfter through t11. Build default5433 target with selected IP/PSC DNS SAN identity, mandatoryTLS1.3 and metadata auth_type2 IAM/1 built-in. Both modes obtain alloydb.login token for metadata; IAM also uses it as PostgreSQL password. Duration is only a hint; use effective certificate/token expiry.
4. The existing shared transport performs metadata once after verified immediate TLS, never PostgreSQL SSLRequest. Commit feat(postgres): issue AlloyDB connection credentials.

## Verification

Write the named failing tests before behavior changes. Initial RED is the absent named module/public method or the stated incorrect behavior; record the actual failing assertion/diagnostic. After implementation, run exactly:

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_cloud_wire_tests alloydb_admin_
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres
```

Final exit0 is required. These commands are planned, not executed evidence. Record each command/exit and public assertions during execution. Source rebuild precedes Python tests; never validate a stale extension. No external write, workflow dispatch, tag mutation or push is authorized here.

## Acceptance Criteria

- [ ] R9: the owned public behavior and negative cases satisfy the parent requirements.

- [ ] The concrete Steps contracts and negative cases pass through the named canonical tests.
- [ ] No duplicated protocol/crypto/runtime engine or fake placeholder client is introduced.
- [ ] Owned changes fit one invocation/commit; direct Markdown task state and parent checklist are synchronized after verification.

## Notes & Discoveries

None.
