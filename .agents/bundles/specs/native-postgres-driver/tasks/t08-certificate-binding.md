---
type: Task
id: native-postgres-driver:t08
title: Derive tls-server-end-point binding from the verified peer certificate
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
- native-postgres-driver:t07
files:
- src/rs/vertebra-postgres/Cargo.toml
- src/rs/Cargo.lock
- src/rs/vertebra-postgres/src/lib.rs
- src/rs/vertebra-postgres/src/binding.rs
- src/rs/vertebra-postgres/src/tls.rs
- src/rs/vertebra-postgres/tests/postgres_tls_wire_tests.rs
tests:
- src/rs/vertebra-postgres/tests/postgres_tls_wire_tests.rs::certificate_binding_vectors
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
- R5
---

# Derive tls-server-end-point binding from the verified peer certificate

## Objective

Derive tls-server-end-point binding from the verified peer certificate, with behavior verified through the canonical public seam listed above.

## Context

Read `../spec.md`, the named dependency worksheets, `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md`, `.agents/skills/pyo3-bridge/SKILL.md` and `.agents/bundles/knowledge/patterns/rust/ffi-and-gil-discipline.md`. This task belongs to the complete native PostgreSQL driver on one feature branch and one PR. Its completion is not permission to ship a partial driver or cut over the Python adapters. Public signatures and shared contracts in the full spec govern later task additions. No implementation starts with plan_commit null.

Ownership is exactly the frontmatter list. Shared files are changed only after predecessor tasks complete; preserve unrelated edits. One executor invocation makes one signed conventional local commit. Estimated handwritten changes including tests: 300 lines; generated lock output is reviewed separately. Keep the task near400 lines. If the specified work cannot fit or a named prerequisite is missing, stop and `/flow:revise native-postgres-driver`; keep the complete one-PR scope and split the worksheet boundary.

Primary sources: x509-parser0.18.0 src/signature_algorithm.rs (public PSS hash API), RFC5929 section4.1, RFC4055 and RFC5758. RFC9266 does not define an EdDSA SHA256 substitution for PostgreSQL tls-server-end-point.

## Steps

1. Add certificate-binding RED vectors for RSA PKCS1, ECDSA and DSA signature hashes, RSA-PSS with explicit SHA224/256/384/512 or omitted SHA1 parameters, SHA1/MD5 substitution, trailing/malformed DER, unknown signature/hash and Ed25519/Ed448 no-hash cases. Hash the entire leaf DER, never SPKI, TLS exporter or chain concatenation.
2. Pin `x509-parser = { version = "=0.18.0", default-features = false }` and use existing RustCrypto sha2::Sha224/Sha256/Sha384/Sha512. `let (remaining, cert) = x509_parser::parse_x509_certificate(der)?` must leave no trailing data. Inspect cert.signature_algorithm.algorithm; use public SignatureAlgorithm::try_from(&cert.signature_algorithm) for RSASSA_PSS(params), then params.hash_algorithm_oid(), whose omitted hash defaults to SHA1. Never inspect MGF hash as the signature digest.
3. Implement binding.rs::tls_server_end_point(&[u8])->Result<Vec<u8>,PostgresError>. Map RSA PKCS1 and ECDSA SHA224/256/384/512 signature OIDs to the corresponding digest; DSA SHA224/256 similarly. RSA/DSA/ECDSA SHA1 and RSA MD5 use SHA256 per RFC5929. PSS SHA1 default also uses SHA256; PSS SHA224/256/384/512 use that hash. Unknown, malformed, ambiguous or no-hash EdDSA signatures make binding unavailable. Prefer may select advertised plain SCRAM; Require fails. Certificate trust/weak-signature acceptance remains rustls's responsibility and is never bypassed.
4. Capture leaf from TransportStream::TlsTcp rustls peer_certificates after a successful handshake and carry optional binding bytes with the transport into auth selection; plaintext has none. Validate digest vectors independently using approved parser/reference certificate fixtures. Commit feat(postgres): derive certificate endpoint channel binding.

## Verification

Write the named failing tests before behavior changes. Initial RED is the absent named module/public method or the stated incorrect behavior; record the actual failing assertion/diagnostic. After implementation, run exactly:

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_tls_wire_tests certificate_binding_
cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings
```

Final exit0 is required. These commands are planned, not executed evidence. Record each command/exit and public assertions during execution. Source rebuild precedes Python tests; never validate a stale extension. No external write, workflow dispatch, tag mutation or push is authorized here.

## Acceptance Criteria

- [ ] R4, R5: the owned public behavior and negative cases satisfy the parent requirements.

- [ ] The concrete Steps contracts and negative cases pass through the named canonical tests.
- [ ] No duplicated protocol/crypto/runtime engine or fake placeholder client is introduced.
- [ ] Owned changes fit one invocation/commit; direct Markdown task state and parent checklist are synchronized after verification.

## Notes & Discoveries

None.
