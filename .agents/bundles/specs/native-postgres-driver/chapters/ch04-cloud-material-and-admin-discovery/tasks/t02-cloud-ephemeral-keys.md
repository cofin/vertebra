---
type: Task
id: "native-postgres-driver:ch04:t02"
chapter_id: ch04-cloud-material-and-admin-discovery
title: Generate PKCS#1 RSA-2048 ephemeral keys and validate issued X.509 chains
description: Implement cloud::keys generating off-GIL PKCS#1 RSA-2048 PEM keypairs via rsa 0.9 and validating Admin-issued X.509 leaf certificates for exact SPKI key match, leaf-first chain order, and signed NotAfter expiry.
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch04:t01"
read_set:
  - src/rs/vertebra-db/src/dialer.rs
  - src/rs/vertebra-postgres/src/binding.rs
  - src/rs/vertebra-postgres/src/cloud/mod.rs
  - src/rs/vertebra-postgres/src/cloud/material.rs
  - .agents/bundles/specs/native-postgres-driver/research/cloud-material_20261105.md
files:
  - src/rs/vertebra-postgres/src/cloud/mod.rs
  - src/rs/vertebra-postgres/src/cloud/keys.rs
tests:
  - src/rs/vertebra-postgres/tests/postgres_cloud_wire_tests.rs
estimated_loc: 215
plan_revision: 3
plan_commit: dd432f18
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
updated_at: "2026-11-07T16:40:00Z"
commit: null
sources:
  - id: cloud-research
    resource: .agents/bundles/specs/native-postgres-driver/research/cloud-material_20261105.md
    title: Official Go Cloud SQL and AlloyDB PKCS#1 RSA PUBLIC KEY PEM and signed NotAfter validation rules
  - id: db-dialer
    resource: src/rs/vertebra-db/src/dialer.rs::CloudSqlInstanceUri
    title: Existing dialer module whose self-signed certificate helper must NOT be sent as Admin public_key
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Client private key redaction and connection deadline invariants
tags:
  - postgres
  - rsa
  - x509
  - mtls
---

# Task t02: Generate PKCS#1 RSA-2048 Ephemeral Keys and Validate Issued X.509 Chains

## Objective & Scope Boundary

Implement `src/rs/vertebra-postgres/src/cloud/keys.rs` using `rsa = { version = "0.9", features = ["pem", "sha2"] }`, `rustls-pemfile`, and `x509-parser = "=0.18.1"` to generate off-GIL 2048-bit RSA keypairs formatted as PKCS#1 LF PEM (`RSA PUBLIC KEY` and `RSA PRIVATE KEY`) and validate Admin-issued X.509 certificate chains (exact RSA SPKI modulus/exponent match, leaf-first chain ordering, zero trailing DER, and signed `NotAfter > now`) before cache publication [^cloud-research] [^db-dialer] [^pg-contracts]. Cloud SQL (`ch04:t03`) and AlloyDB (`ch04:t04`) Admin clients invoke this validator ([../spec.md](../spec.md)).

## Bounded Read Manifest

- `src/rs/vertebra-db/src/dialer.rs` — Existing `generate_ephemeral_keypair_pem` (which emits a self-signed `BEGIN CERTIFICATE` rather than a public key PEM and MUST NOT be reused for Admin `public_key` / `publicKey` requests) [^db-dialer].
- `src/rs/vertebra-postgres/src/binding.rs` — Zero-trailing-DER `x509_parser::parse_x509_certificate` usage from `ch03:t02`.
- `src/rs/vertebra-postgres/src/cloud/mod.rs` — Cloud module exports from `ch04:t01`.
- `src/rs/vertebra-postgres/src/cloud/material.rs` — `PreparedCloudMaterial` fields and redaction conventions from `ch04:t01`.
- `.agents/bundles/specs/native-postgres-driver/research/cloud-material_20261105.md` — Official Go connector PKCS#1 RSA-2048 PEM formatting and issued leaf verification rules [^cloud-research].

## Symbol & Signature Contracts

- `src/rs/vertebra-postgres/src/cloud/mod.rs`:
  - Export `pub mod keys;`.
- `src/rs/vertebra-postgres/src/cloud/keys.rs`:
  - `pub struct EphemeralRsaKeyPair`:
    - Owns `public_key_pem: String` (`-----BEGIN RSA PUBLIC KEY-----\n...\n-----END RSA PUBLIC KEY-----\n`, LF line endings via `rsa::pkcs1::EncodeRsaPublicKey::to_pkcs1_pem(LineEnding::LF)`), `private_key_pem: Vec<u8>` (`-----BEGIN RSA PRIVATE KEY-----\n...`, LF line endings via `rsa::pkcs1::EncodeRsaPrivateKey::to_pkcs1_pem(LineEnding::LF)`), and `RsaPublicKey` components (`n`, `e`).
    - Custom `fmt::Debug` redacting `private_key_pem` (`"<redacted>"`).
  - `pub async fn generate_ephemeral_rsa_keypair(deadline: tokio::time::Instant) -> Result<EphemeralRsaKeyPair, PostgresError>`:
    - Offloads `RsaPrivateKey::new(&mut rsa::rand_core::OsRng, 2048)` onto `tokio::task::spawn_blocking` outside any Python attachment and awaits completion under `tokio::time::timeout_at(deadline, ...)` (if cancelled or past `deadline`, bounded blocking keygen may finish in the background thread, but `Err(PostgresError::Transport(_))` is returned immediately and no key material is published).
  - `#[derive(Clone)] pub struct ValidatedClientIdentity`:
    - `pub cert_chain_pem: Vec<u8>` (joined leaf-first PEM blocks normalized with LF trailing newlines)
    - `pub private_key_pem: Vec<u8>` (redacted in `Debug`)
    - `pub signed_not_after_ms: u64`
  - `pub fn validate_issued_certificate_chain(chain_pems: &[String], expected_key: &EphemeralRsaKeyPair, now_unix_ms: u64) -> Result<ValidatedClientIdentity, PostgresError>`:
    1. Reject empty `chain_pems` or empty/non-CERTIFICATE PEM blocks (`rustls_pemfile` or PEM decode), ensuring every entry in `chain_pems` parses cleanly as X.509 DER with zero trailing DER bytes (`x509_parser::parse_x509_certificate(der)` -> `remaining.is_empty()`).
    2. Inspect `chain_pems[0]` (the leaf certificate): decode `leaf.public_key().raw` (SubjectPublicKeyInfo DER) via `rsa::pkcs8::DecodePublicKey::from_public_key_der` and verify `cert_pub.n() == expected_key.public_key().n() && cert_pub.e() == expected_key.public_key().e()`. Reject mismatched RSA keys or non-RSA SPKIs with `PostgresError::Protocol` / `Transport`.
    3. Read `leaf.validity().not_after.timestamp()`: reject negative/unparsable timestamps or `signed_not_after_ms <= now_unix_ms` (signed X.509 `NotAfter` is authoritative over any control-plane duration hint).
    4. Join `chain_pems` in exact leaf-first order (`[leaf, intermediate, ...]`) into `cert_chain_pem` so `rustls` client authentication presents the leaf first.

## Edge Cases & Error Semantics

- Never pass `-----BEGIN CERTIFICATE-----` as `public_key` / `publicKey`; `public_key_pem()` begins strictly with `"-----BEGIN RSA PUBLIC KEY-----\n"` and uses `\n` (LF), never `\r\n`.
- Swapping leaf and CA order (`[ca_pem, leaf_pem]`) where `chain_pems[0]` has CA's public key fails the SPKI `(n, e)` equality check against `expected_key` immediately.
- A certificate whose `NotAfter` equals `now_unix_ms / 1000` (already expired at current second) or precedes `now_unix_ms` fails terminally even if an API response claimed a `3600s` duration hint.
- Private key PEM bytes never appear in `Debug` output or `PostgresError` messages.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/rs/vertebra-postgres/tests/postgres_cloud_wire_tests.rs` (test names prefixed `issued_key_`).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| `issued_key_pkcs1_pem_roundtrip_and_redaction` | `generate_ephemeral_rsa_keypair(Instant::now() + Duration::from_secs(5))` | `public_key_pem` starts with `"-----BEGIN RSA PUBLIC KEY-----\n"`, contains zero `'\r'` or `"BEGIN CERTIFICATE"`, decodes via `DecodeRsaPublicKey` as 2048-bit RSA matching `DecodeRsaPrivateKey`, and `format!("{key:?}")` excludes `"BEGIN RSA PRIVATE KEY"`. |
| `issued_key_valid_leaf_first_chain` | `[leaf_pem_signing_generated_spki, intermediate_ca_pem]` with `NotAfter = 2_000_000_000` at `now_ms = 1_900_000_000_000` | Returns `Ok(identity)` with `signed_not_after_ms == 2_000_000_000_000` and `cert_chain_pem` starting with exact `leaf_pem` followed by `intermediate_ca_pem`. |
| `issued_key_mismatched_spki_or_reversed_chain` | (a) Leaf signed for a second freshly generated RSA-2048 keypair; (b) reversed `[intermediate_ca_pem, valid_leaf_pem]`; (c) ECDSA P-256 leaf | Each returns `Err` (`Protocol` / `Transport`) and refuses to construct `ValidatedClientIdentity`. |
| `issued_key_expired_and_not_after_dominates_hint` | Leaf `NotAfter = 1_700_000_120` validated at `now_ms = 1_700_000_120_000` (expired) vs `now_ms = 1_700_000_000_000` (120s left despite 3600s request hint) | At `1_700_000_120_000` fails terminally; at `1_700_000_000_000` reports exact `signed_not_after_ms == 1_700_000_120_000` (not `+3600s`). |
| `issued_key_malformed_pem_and_deadline_abort` | Empty `&[]`, corrupt base64 PEM, trailing DER inside PEM block, or already-expired `deadline` passed to `generate_ephemeral_rsa_keypair` | Malformed chains fail synchronously; expired `deadline` returns `Err(PostgresError::Transport(_))` without publishing key material. |

## Verification

Write the `issued_key_*` tests in `src/rs/vertebra-postgres/tests/postgres_cloud_wire_tests.rs` first (confirming Red), implement `cloud/keys.rs`, and verify exit `0` alongside existing X.509 binding tests:

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_cloud_wire_tests issued_key_
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_tls_wire_tests certificate_binding_
```

Commit as `feat(postgres): validate issued cloud client identities`.

## Acceptance Criteria

- [ ] `generate_ephemeral_rsa_keypair` runs RSA-2048 generation off-GIL in `spawn_blocking` under `deadline` and emits LF-normalized PKCS#1 `RSA PUBLIC KEY` and `RSA PRIVATE KEY` PEMs (R4, R9).
- [ ] `validate_issued_certificate_chain` enforces leaf-first ordering, zero trailing DER, exact RSA `(n, e)` SPKI equality against the generated keypair, and `signed_not_after_ms > now_unix_ms` (R4, R9).
- [ ] Private keys are redacted across all `Debug` and error formatting paths (R5, R9).

## Notes & Discoveries

- [2026-11-07 05:00] `vertebra_db::dialer::generate_ephemeral_keypair_pem` remains untouched for existing unit fixtures in `vertebra-db`, while `vertebra-postgres` uses `cloud::keys` so Cloud SQL and AlloyDB Admin endpoints receive real PKCS#1 RSA public keys.

[^cloud-research]: `.agents/bundles/specs/native-postgres-driver/research/cloud-material_20261105.md` — PKCS#1 RSA-2048 PEM format and leaf SPKI/expiry validation rules.
[^db-dialer]: `src/rs/vertebra-db/src/dialer.rs::CloudSqlInstanceUri` — Shared dialer module context.
[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Private key redaction and connection timeout invariants.
