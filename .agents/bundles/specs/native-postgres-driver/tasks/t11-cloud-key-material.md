---
type: Task
id: native-postgres-driver:t11
title: Generate real cloud public keys and validate issued certificate material
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
- native-postgres-driver:t10
files:
- src/rs/vertebra-postgres/Cargo.toml
- src/rs/Cargo.lock
- src/rs/vertebra-postgres/src/cloud/mod.rs
- src/rs/vertebra-postgres/src/cloud/keys.rs
- src/rs/vertebra-postgres/tests/postgres_cloud_wire_tests.rs
tests:
- src/rs/vertebra-postgres/tests/postgres_cloud_wire_tests.rs::issued_key_and_certificate
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
- R4
- R9
---

# Generate real cloud public keys and validate issued certificate material

## Objective

Generate real cloud public keys and validate issued certificate material, with behavior verified through the canonical public seam listed above.

## Context

Read `../spec.md`, the named dependency worksheets, `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md`, `.agents/skills/pyo3-bridge/SKILL.md` and `.agents/bundles/knowledge/patterns/rust/ffi-and-gil-discipline.md`. This task belongs to the complete native PostgreSQL driver on one feature branch and one PR. Its completion is not permission to ship a partial driver or cut over the Python adapters. Public signatures and shared contracts in the full spec govern later task additions. No implementation starts with plan_commit null.

Ownership is exactly the frontmatter list. Shared files are changed only after predecessor tasks complete; preserve unrelated edits. One executor invocation makes one signed conventional local commit. Estimated handwritten changes including tests: 260 lines; generated lock output is reviewed separately. Keep the task near400 lines. If the specified work cannot fit or a named prerequisite is missing, stop and `/flow:revise native-postgres-driver`; keep the complete one-PR scope and split the worksheet boundary.



## Steps

1. Add RED tests showing generated public PEM parses and matches the private key, issued leaf matches that key, joined chain preserves leaf-first order, malformed/mismatched/expired certificates fail, and signed NotAfter dominates duration hints. Existing generate_ephemeral_keypair_pem returns a self-signed certificate plus private key; it does not produce the public PEM required by Admin APIs.
2. Add rsa="0.9" and use rsa::rand_core::OsRng, RsaPrivateKey::new(&mut rng,2048), and pkcs1 EncodeRsaPublicKey/EncodeRsaPrivateKey with LF PEM. Generate RSA2048 outside Python attachment; bounded blocking key generation may outlive cancellation but no result/material is published after deadline. This produces PKCS1 RSA PUBLIC KEY and RSA PRIVATE KEY PEM matching official Go connector formatting. Do not send a self-signed certificate as public_key. Decode cert.public_key().raw SPKI via rsa::pkcs8::DecodePublicKey and compare public modulus/exponent to the generated key. Parse signed cert.validity().not_after.timestamp() using vetted x509-parser with no trailing DER and require expiry>now.
3. Keep the issued PEM chain and private key in owned Rust material, redacted from Debug/errors. The Cloud SQL/AlloyDB API modules call this validation once before cache publication. Certificate mismatch or unparsable expiry is terminal setup failure. Commit feat(postgres): validate issued cloud client identities.

## Verification

Write the named failing tests before behavior changes. Initial RED is the absent named module/public method or the stated incorrect behavior; record the actual failing assertion/diagnostic. After implementation, run exactly:

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_cloud_wire_tests issued_key_
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_tls_wire_tests certificate_binding_
```

Final exit0 is required. These commands are planned, not executed evidence. Record each command/exit and public assertions during execution. Source rebuild precedes Python tests; never validate a stale extension. No external write, workflow dispatch, tag mutation or push is authorized here.

## Acceptance Criteria

- [ ] R4, R9: the owned public behavior and negative cases satisfy the parent requirements.

- [ ] The concrete Steps contracts and negative cases pass through the named canonical tests.
- [ ] No duplicated protocol/crypto/runtime engine or fake placeholder client is introduced.
- [ ] Owned changes fit one invocation/commit; direct Markdown task state and parent checklist are synchronized after verification.

## Notes & Discoveries

None.
