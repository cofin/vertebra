---
type: Task
id: native-postgres-driver:t20
title: Project complete connection options and establish Unix startup
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
- native-postgres-driver:t19
files:
- src/rs/vertebra-postgres/src/config.rs
- src/rs/vertebra-postgres/src/protocol.rs
- src/rs/vertebra-postgres/src/tls.rs
- src/rs/vertebra-postgres/src/session.rs
- src/rs/vertebra-postgres/tests/postgres_wire_tests.rs
tests:
- src/rs/vertebra-postgres/src/config.rs::tests::complete_option_validation
- src/rs/vertebra-postgres/tests/postgres_wire_tests.rs::unix_startup
- src/rs/vertebra-postgres/tests/postgres_wire_tests.rs::startup_session_gucs
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
- R2
- R3
---

# Project complete connection options and establish Unix startup

## Objective

Project complete connection options and establish Unix startup, making the full public constructor reachable before facade acceptance.

## Context

Read ../spec.md, ../contracts.md::Connection constructors, direct dependencies and .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md. All work stays in the one complete-driver feature branch/PR. No implementation starts with plan_commit null. Ownership is exactly the listed files, sequenced after predecessors; preserve other edits. This is one invocation and one signed conventional local commit, estimated380 handwritten lines including tests. If near400 cannot hold, revise worksheets without shrinking the required one-PR scope.

## Steps

1. Add RED complete-option tests matching contracts.md: Unix/cloud resource exclusions; Cloud SQL/AlloyDB mutual exclusion; PUBLIC/PRIVATE/PSC; disable/prefer/require binding; positive bounded keepalive tuning; copied str:str maps; separate Admin/login options; opaque Admin token without designated login map; and non-UTF8 requested client_encoding. Constructor/config validation is pure, with no DNS/file/network/credential refresh. Invalid mapping key/value types fail TypeError at FFI; native semantic conflicts fail ValueError.
2. Normalize the full t02 record once into EndpointTarget plus credential/GUC/keepalive policies used by direct clients and pool configuration comparison. unix_socket is an exact nonempty NUL-free socket file path, never a directory derived here. It excludes either cloud URI and requires sslmode disable, no TLS PEM/server_name options, no explicit keepalive tuning and no channel_binding=require. Default keepalives=True carries no socket action on Unix. For cloud, mandatory TLS1.3 identity is independent of direct sslmode; Admin comes from cloud_auth_options and login from iam_auth_options or same-source scoped derivation. A static Admin token cannot be inferred to carry login scopes. Direct-host enable_iam_auth is valid and projects to t21: obtain a fresh login token before ordinary TLS/startup, with default sqlservice.login or authoritative explicit iam_auth_options scopes. IAM supersedes supplied password and never invokes Admin certificate APIs or weakens direct TLS.
3. Unix connect uses existing ConnectOptions::unix/TransportStream::connect inside the same ConnectAttempt deadline, then the same startup/auth decoder without SSLRequest. A Unix listener test verifies no network/TLS prefix, UTF8 startup, Ready gate, close/EOF and timeout/cancellation ownership. Unsupported platforms return native UnsupportedFeature mapped to exact core NotSupportedError; use platform-gated native Unix tests and actual Windows build verification rather than claiming Unix support there.
4. Project session_gucs directly into upstream startup parameters, retaining exact string values. Validate nonempty NUL-free names/values; reject reserved protocol keys user/database/options/replication. client_encoding may only be UTF8 and is deduplicated. Build application_name default/config field first then overlay session_gucs, preserving existing build_session_gucs map precedence; test explicit application_name override and custom dotted GUC/value with spaces. No SQL SET interpolation or -c token parser is introduced. Public application_name/session maps remain authoritative.
5. Commit feat(postgres): project full options into Unix and startup sessions.

## Verification

Write named RED tests first, then require exit0:

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --lib complete_option_validation
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_wire_tests unix_
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_wire_tests startup_session_gucs
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres
```

Record actual command exits/assertions during execution; these are planned checks, not runtime evidence. No external repository write or workflow dispatch is authorized.

## Acceptance Criteria

- [ ] R2, R3: the owned public behavior and negative cases satisfy the parent requirements.

- [ ] Every named public option is reachable through owned validation/projection, with no ignored fields or duplicate DSN parser.
- [ ] The listed canonical positive/negative tests pass and all commands exit0.
- [ ] Task and parent checklist state agree after one verified local commit.

## Notes & Discoveries

None.
