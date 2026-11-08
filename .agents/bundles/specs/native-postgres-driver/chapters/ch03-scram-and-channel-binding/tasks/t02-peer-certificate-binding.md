---
type: Task
id: "native-postgres-driver:ch03:t02"
chapter_id: ch03-scram-and-channel-binding
title: Derive RFC 5929 tls-server-end-point digests from negotiated peer certificates
description: Implement binding::tls_server_end_point using x509-parser 0.18.1 and sha2 across RSA, ECDSA, DSA, RSA-PSS, SHA-1/MD5 substitution, and EdDSA unavailable-binding rules, and capture peer leaf digests in tls::connect.
status: stable
state: in_progress
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch03:t01"
read_set:
  - src/rs/vertebra-db/src/connection.rs
  - src/rs/vertebra-postgres/src/lib.rs
  - src/rs/vertebra-postgres/src/tls.rs
  - src/rs/vertebra-postgres/tests/postgres_tls_wire_tests.rs
  - .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
files:
  - src/rs/vertebra-postgres/src/lib.rs
  - src/rs/vertebra-postgres/src/binding.rs
  - src/rs/vertebra-postgres/src/tls.rs
tests:
  - src/rs/vertebra-postgres/tests/postgres_tls_wire_tests.rs
estimated_loc: 225
plan_revision: 7
plan_commit: 1c75af65
state_revision: 0
claimed_by: agent:ch03-t02-binding
claimed_at: "2026-11-08T00:23:12Z"
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-05T18:25:07Z"
updated_at: "2026-11-08T00:23:12Z"
commit: null
sources:
  - id: proto-research
    resource: .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
    title: x509-parser 0.18.1 SignatureAlgorithm::RSASSA_PSS and RFC 5929 / RFC 9266 digest rules
  - id: rs-transport
    resource: src/rs/vertebra-db/src/connection.rs::TransportStream
    title: TransportStream::TlsTcp rustls ServerConnection/ClientConnection peer_certificates access
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Peer certificate channel-binding availability and TLS trust boundaries
tags:
  - postgres
  - x509
  - channel-binding
  - tls
---

# Task t02: Derive RFC 5929 `tls-server-end-point` Digests from Negotiated Peer Certificates

## Objective & Scope Boundary

Implement `src/rs/vertebra-postgres/src/binding.rs` (`tls_server_end_point`) using pinned `x509-parser = "=0.18.1"` (`default-features = false`) and `sha2::{Sha224, Sha256, Sha384, Sha512}` per RFC 5929 Section 4.1, RFC 4055, RFC 5758, and RFC 9266, and capture `tls_server_end_point: Option<Vec<u8>>` from `TransportStream::TlsTcp` inside `src/rs/vertebra-postgres/src/tls.rs` [^proto-research] [^rs-transport] [^pg-contracts]. Policy-driven `SCRAM-SHA-256-PLUS` negotiation and `GS2` `y,,` downgrade signaling land in `ch03:t03` ([../spec.md](../spec.md)).

## Bounded Read Manifest

- `src/rs/vertebra-db/src/connection.rs::TransportStream` — `TlsTcp(Box<tokio_rustls::client::TlsStream<TcpStream>>)` `get_ref().1.peer_certificates()` access after TLS handshake completion [^rs-transport].
- `src/rs/vertebra-postgres/src/lib.rs` — Crate module exports on the `rlib` seam.
- `src/rs/vertebra-postgres/src/tls.rs` — `DialedTransport` (`tls_server_end_point: Option<Vec<u8>>`) and `tls::connect` from `ch02:t03`.
- `src/rs/vertebra-postgres/tests/postgres_tls_wire_tests.rs` — `rcgen` and `tokio-rustls` certificate fixtures from `ch02:t03`.
- `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` — `x509-parser` `0.18.1` `src/signature_algorithm.rs` (`SignatureAlgorithm::RSASSA_PSS`) and EdDSA non-substitution rules [^proto-research].

## Symbol & Signature Contracts

- `src/rs/vertebra-postgres/src/lib.rs`:
  - Export `pub mod binding;` on the Rust `rlib` seam (zero Python `#[pymodule]` registrations).
- `src/rs/vertebra-postgres/src/binding.rs`:
  - `pub fn tls_server_end_point(der: &[u8]) -> Result<Vec<u8>, PostgresError>`:
    1. Parse `let (remaining, cert) = x509_parser::parse_x509_certificate(der)` and reject non-empty `remaining` (trailing DER bytes) or parse errors with `PostgresError::Protocol`.
    2. Dispatch on `cert.signature_algorithm.algorithm` using the explicit OID mappings below before invoking `SignatureAlgorithm::try_from` only for RSA-PSS. The pinned classifier omits the supported DSA-SHA224/SHA256 OIDs, so a blanket fallible classifier call must not reject them:
       - RSA PKCS#1 v1.5 and ECDSA with SHA-224 (`1.2.840.113549.1.1.14`, `1.2.840.10045.4.3.1`), SHA-256 (`...11`, `...3.2`), SHA-384 (`...12`, `...3.3`), SHA-512 (`...13`, `...3.4`) -> `Sha224`, `Sha256`, `Sha384`, `Sha512`.
       - DSA with SHA-224 (`2.16.840.1.101.3.4.3.1`) -> `Sha224`; DSA with SHA-256 (`2.16.840.1.101.3.4.3.2`) -> `Sha256`.
       - RSA/DSA/ECDSA with SHA-1 (`1.2.840.113549.1.1.5`, `1.2.840.10040.4.3`, `1.2.840.10045.4.1`) and RSA with MD5 (`1.2.840.113549.1.1.4`) -> substitute `Sha256` per RFC 5929 Section 4.1.
       - RSA-PSS (`1.2.840.113549.1.1.10`) -> require a present parameters SEQUENCE and validate complete DER consumption of every parameter and its nested values using public ASN.1/DER parsers re-exported by `x509-parser`; reject trailing or malformed parameter contents with `PostgresError::UnsupportedAuth`. The pinned `RsaSsaPssParams` helper discards an inner remainder and cannot alone establish complete consumption. Then obtain `SignatureAlgorithm::RSASSA_PSS(params)` and inspect `params.hash_algorithm_oid()` ONLY (never use the MGF1 hash OID as the signature digest): an omitted `hashAlgorithm` field within the present SEQUENCE, including an empty SEQUENCE (`30 00`), defaults to `SHA-1` (`1.3.14.3.2.26`) -> substitute `Sha256`; explicit SHA-224 (`2.16.840.1.101.3.4.2.4`), SHA-256 (`...1`), SHA-384 (`...2`), SHA-512 (`...3`) -> use that exact digest; unknown PSS hash OID or malformed PSS params -> `Err(PostgresError::UnsupportedAuth(_))`.
       - RSA-MD2 (`1.2.840.113549.1.1.2`), Ed25519 (`1.3.101.112`), Ed448 (`1.3.101.113`), or any unknown/ambiguous signature OID -> return `Err(PostgresError::UnsupportedAuth(_))` (RFC 5929 substitutes SHA-256 only for MD5/SHA-1; MD2 is unsupported, and no-hash EdDSA has no defined endpoint digest. RFC 9266 defines `tls-exporter`; never invent an EdDSA SHA-256 substitution).
    3. Compute the selected digest over the **entire `der` slice** (`der`, never `cert.tbs_certificate.subject_pki.raw`, TLS exporter output, or concatenated intermediate CA DER).
- `src/rs/vertebra-postgres/src/tls.rs`:
  - After `TransportStream::upgrade_tls` succeeds in `tls::connect`, inspect `TransportStream::TlsTcp(tls)` -> `tls.get_ref().1.peer_certificates().and_then(|certs| certs.first())`:
    - When `tls_server_end_point(leaf.as_ref())` returns `Ok(bytes)`, store `Some(bytes)` on `DialedTransport.tls_server_end_point`.
    - When `tls_server_end_point` returns `Err(PostgresError::UnsupportedAuth(_))` (such as a negotiated Ed25519 peer leaf), record binding as unavailable (`None`) while preserving the selected TLS mode's trust policy; malformed DER returns `Err(PostgresError::Protocol(_))`. Plaintext transports always carry no endpoint binding.

## Edge Cases & Error Semantics

- RSA-PSS certificates where `hashAlgorithm` is SHA-384 while `maskGenAlgorithm` (`mgf1`) uses SHA-256 MUST hash with SHA-384 (`params.hash_algorithm_oid()`), proving MGF1 is never mistaken for the signature hash.
- Multi-certificate server chains (`[leaf_der, intermediate_ca_der]`) hash strictly `leaf_der` (`certs[0]`), never `leaf_der || intermediate_ca_der` or the issuer CA's signature algorithm.
- Appending even a single trailing byte (`0x00`) after valid X.509 DER or truncating DER fails `tls_server_end_point` immediately.
- The selected TLS mode retains its existing verification policy: `Require`/`Prefer` without `ca_pem` provide encryption-only TLS; supplying a CA verifies chain/validity, and `VerifyFull` also checks hostname. Binding hashes the negotiated peer leaf and does not add or bypass certificate verification. Weak-signature rejection in verifying modes remains the transport verifier's responsibility.
- A missing whole RSA-PSS parameters field, trailing bytes inside its SEQUENCE, malformed nested algorithm identifiers, or unknown signature-hash OIDs produce unavailable binding; a present empty SEQUENCE supplies default SHA-1. MGF1 never selects the endpoint digest.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/rs/vertebra-postgres/tests/postgres_tls_wire_tests.rs` (test names prefixed `certificate_binding_`). Keep actual signed DER constants inline in this owned file for algorithms unavailable through pinned `rcgen`/`ring`; do not add a fixture file or dependency. Existing live TLS success assertions change from `tls_server_end_point() == None` to the actual negotiated leaf digest; plaintext remains `None`.
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| `certificate_binding_rsa_ecdsa_dsa_hashes` | X.509 DER fixtures signed with RSA PKCS#1 (SHA-224/256/384/512), ECDSA (SHA-224/256/384/512), and DSA (SHA-224/256) | `tls_server_end_point(der)` equals exact `Sha224` (`28B`), `Sha256` (`32B`), `Sha384` (`48B`), or `Sha512` (`64B`) over full `der` (and differs from SPKI digest). |
| `certificate_binding_sha1_md5_substitution` | X.509 DER fixtures carrying RSA-MD5, RSA-SHA1, DSA-SHA1, and ECDSA-SHA1 signature algorithm OIDs | Each returns `Ok(Sha256::digest(der).to_vec())` (`32B`) per RFC 5929 Section 4.1. |
| `certificate_binding_rsa_pss_explicit_and_default` | RSA-PSS DER fixtures with (a) present empty parameters SEQUENCE with omitted `hashAlgorithm` (default SHA-1), (b) explicit SHA-224/256/384/512, and (c) `hashAlgorithm = SHA-384` + `mgf1 = SHA-256` | (a) returns `Sha256(der)`; (b) returns matching 28/32/48/64-byte digest; (c) returns 48-byte `Sha384(der)` (not 32-byte `Sha256`). |
| `certificate_binding_eddsa_and_unknown_unavailable` | RSA-MD2 (`1.2.840.113549.1.1.2`), Ed25519 (`1.3.101.112`), Ed448 (`1.3.101.113`), and synthetic unknown OID (`1.2.3.4.5`) X.509 DER certificates; RSA-PSS with absent whole parameters, trailing SEQUENCE contents, or malformed nested parameters | Each returns `Err(PostgresError::UnsupportedAuth(_))` rather than substituting SHA-256 or accepting incomplete parameter parsing. |
| `certificate_binding_malformed_and_trailing_der` | Empty slice `&[]`, truncated DER, and valid DER with `[0x00]` appended | Each returns `Err(PostgresError::Protocol(_))`. |
| `certificate_binding_live_tls_leaf_vs_chain` | Live `tokio-rustls` server presenting `[leaf_ecdsa_p384_sha384, intermediate_rsa_sha256]` vs Ed25519 leaf vs plaintext `Prefer` + `N` | Multi-cert chain captures `Sha384(leaf_der)` (`48B`); Ed25519 TLS session completes trust startup with binding marked unavailable; plaintext carries no binding. |

## Verification

Write the `certificate_binding_*` unit-vector and live-TLS tests in `src/rs/vertebra-postgres/tests/postgres_tls_wire_tests.rs` first (confirming Red), implement `binding.rs` and `tls.rs`, and verify exit `0`:

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_tls_wire_tests certificate_binding_
cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings
```

Commit as `feat(postgres): derive certificate endpoint channel binding`.

## Acceptance Criteria

- [ ] `binding::tls_server_end_point` hashes full leaf X.509 DER via `x509-parser = "=0.18.1"` across RSA PKCS#1, ECDSA, DSA, RSA-PSS (explicit and default SHA-1), and SHA-1/MD5 -> SHA-256 substitution without inspecting MGF1 as the signature digest (R4, R5).
- [ ] Unsupported MD2, Ed25519, Ed448, unknown signature OIDs, and absent/malformed/incompletely consumed PSS parameters return unavailable-binding errors; trailing/malformed certificate DER fails with `PostgresError::Protocol`. Only MD5/SHA-1 receive SHA-256 substitution (R4, R5).
- [ ] `tls::connect` extracts only the leaf (`certs[0]`) from `TransportStream::TlsTcp` after successful `rustls` handshakes under the selected TLS verification policy while leaving plaintext sessions unbound (R4).

## Notes & Discoveries

- Exposing `pub mod binding` on the Rust `rlib` seam allows `postgres_tls_wire_tests` to test DSA, MD5/SHA-1, RSA-PSS MGF1-divergent, and Ed448 X.509 DER vectors directly alongside live `tokio-rustls` TLS 1.3 leaf extraction tests without relaxing `rustls` signature verification.

[^proto-research]: `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` — `x509-parser` `0.18.1` PSS parameter and RFC 5929 / RFC 9266 analysis.
[^rs-transport]: `src/rs/vertebra-db/src/connection.rs::TransportStream` — Shared `TlsTcp` peer certificate chain access.
[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Channel-binding policy and TLS verification boundaries.
