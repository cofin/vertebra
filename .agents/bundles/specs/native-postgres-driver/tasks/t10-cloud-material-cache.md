---
type: Task
id: "native-postgres-driver:t10"
title: "Cache cloud connection material under one total deadline"
description: "Cache cloud connection material under one total deadline for the standalone vertebra-postgres native driver."
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:t09"
files:
  - src/rs/vertebra-postgres/Cargo.toml
  - src/rs/Cargo.lock
  - src/rs/vertebra-postgres/src/lib.rs
  - src/rs/vertebra-postgres/src/config.rs
  - src/rs/vertebra-postgres/src/cloud/mod.rs
  - src/rs/vertebra-postgres/src/cloud/material.rs
  - src/rs/vertebra-postgres/tests/postgres_cloud_wire_tests.rs
tests:
  - src/rs/vertebra-postgres/tests/postgres_cloud_wire_tests.rs
plan_revision: 2
plan_commit: cdb978d3
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

# Cache cloud connection material under one total deadline

## Objective

Cache cloud connection material under one total deadline, with behavior verified through the canonical public seam listed above.

## Context

Read `../spec.md`, the named dependency worksheets, `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md`, `.agents/skills/pyo3-bridge/SKILL.md` and `.agents/bundles/knowledge/patterns/rust/ffi-and-gil-discipline.md`. This task belongs to the complete native PostgreSQL driver on one feature branch and one PR. Its completion is not permission to ship a partial driver or cut over the Python adapters. Public signatures and shared contracts in the full spec govern later task additions. No implementation starts with plan_commit null.

Ownership is exactly the frontmatter list. Shared files are changed only after predecessor tasks complete; preserve unrelated edits. One executor invocation makes one signed conventional local commit. Estimated handwritten changes including tests: 330 lines; generated lock output is reviewed separately. Keep the task near400 lines. If the specified work cannot fit or a named prerequisite is missing, stop and `/flow:revise native-postgres-driver`; keep the complete one-PR scope and split the worksheet boundary.



## Steps

1. Add material-cache RED tests with injected clock and mock HTTP: concurrent calls share one refresh, valid cache is reused, refresh lead triggers renewal, failed refresh never returns expired material, cancellation releases the refresh lock, and partial responses never publish. Existing sockets survive material expiry; a reconnect always requests valid material.
2. Native CloudConnectorConfig owns parsed CloudSqlInstanceUri/AlloyDbClusterUri, IpType, auth mode, copied cloud_auth_options and optional designated iam_auth_options, official Admin endpoint and refresh lead240s. CloudConnector::prepare(config, absolute_deadline) returns PreparedCloudMaterial with EndpointTarget, peer identity policy, signed cert expiry, effective expiry and optional login snapshot/generation. Cache keys include credential identity, resource URI, requested network type, API endpoint and auth mode; scope-distinct token caches remain separate. CloudMaterialCache uses one async singleflight lock and validates now+240s<effective_expiry before reuse. Effective expiry is min(signed NotAfter, certificate-bound token expiry), never a duration hint alone.
3. Add direct crate dependencies on vertebra-core, workspace reqwest/serde/serde_json/url; shared cloud types remain in vertebra-db. One internal reqwest client performs authenticated GET/POST under the original absolute deadline, with bounded1MiB response bodies and typed DTOs. Validate paths/IPs/required fields, percent-encode URI segments and redact bearer/private-key data. Official endpoints use HTTPS; an explicit loopback HTTP override serves only mock fixtures. Avoid internal retries that escape the deadline. Credential caches are constructed inside this optional crate from the separate public Admin/login option maps, never another cdylib's PyO3/Rust layout.
4. Construct scope-keyed native caches through the already existing GcpTokenCache::from_options API so this task compiles independently; tests here use explicit local/static credentials and do not claim source grant correctness. t16 repairs scope propagation for all requested automatic sources before t17/t18 enable real cloud connections. Keep transport/session ownership in existing engine. Commit feat(postgres): cache expiring cloud connection material.

## Verification

Write the named failing tests before behavior changes. Initial RED is the absent named module/public method or the stated incorrect behavior; record the actual failing assertion/diagnostic. After implementation, run exactly:

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_cloud_wire_tests material_
cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings
```

Final exit0 is required. These commands are planned, not executed evidence. Record each command/exit and public assertions during execution. Source rebuild precedes Python tests; never validate a stale extension. No external write, workflow dispatch, tag mutation or push is authorized here.

## Acceptance Criteria

- [ ] R5, R9: the owned public behavior and negative cases satisfy the parent requirements.

- [ ] The concrete Steps contracts and negative cases pass through the named canonical tests.
- [ ] No duplicated protocol/crypto/runtime engine or fake placeholder client is introduced.
- [ ] Owned changes fit one invocation/commit; direct Markdown task state and parent checklist are synchronized after verification.

## Notes & Discoveries

None.
