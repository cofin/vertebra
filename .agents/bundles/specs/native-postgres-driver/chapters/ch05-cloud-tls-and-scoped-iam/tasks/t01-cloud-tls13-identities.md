---
type: Task
id: "native-postgres-driver:ch05:t01"
chapter_id: ch05-cloud-tls-and-scoped-iam
title: Enforce cloud TLS 1.3 and service-specific peer identities
description: Enforce TLS 1.3 exclusively on cloud endpoints, verify AlloyDB IP/DNS SANs without CN/CA-only fallback, verify Cloud SQL DNS SAN before exact project:instance CN fallback, and retain verified leaf DER for channel binding.
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch04:t04"
read_set:
  - src/rs/vertebra-db/src/connection.rs
  - src/rs/vertebra-postgres/src/cloud/material.rs
  - .agents/bundles/specs/native-postgres-driver/research/cloud-material_20261105.md
  - .agents/bundles/specs/native-postgres-driver/contracts.md
  - .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
files:
  - src/rs/vertebra-db/src/connection.rs
  - src/rs/vertebra-postgres/src/cloud/material.rs
tests:
  - src/rs/vertebra-db/src/connection.rs
  - src/rs/vertebra-postgres/tests/postgres_cloud_wire_tests.rs
estimated_loc: 235
plan_revision: 8
plan_commit: 8c540809
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
updated_at: "2026-11-07T05:00:00Z"
commit: null
sources:
  - id: db-conn
    resource: src/rs/vertebra-db/src/connection.rs::TransportStream
    title: Shared cloud mTLS ClientConfig builder and CaAndOptionalCnVerifier
  - id: cloud-research
    resource: .agents/bundles/specs/native-postgres-driver/research/cloud-material_20261105.md
    title: Official Cloud SQL tls_verify.go and AlloyDB TLS 1.3 SAN verification rules
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Mandatory cloud TLS 1.3 and peer identity contract
tags:
  - postgres
  - tls
  - cloudsql
  - alloydb
---

# Task t01: Enforce Cloud TLS 1.3 and Service-Specific Peer Identities

## Objective & Scope Boundary

Repair shared cloud mTLS verification in `src/rs/vertebra-db/src/connection.rs` and wire prepared cloud material in `src/rs/vertebra-postgres/src/cloud/material.rs` so both Cloud SQL and AlloyDB enforce TLS 1.3 exclusively, verify service-specific peer identities (replacing AlloyDB's current CA-only `expected_cn: None` path), and retain the verified server leaf certificate DER for later `SCRAM-SHA-256-PLUS` `tls-server-end-point` channel binding [^db-conn] [^cloud-research] [^pg-contracts]. Direct TCP `sslmode` negotiation (`SSLRequest`) remains unchanged. Split AlloyDB TLS/metadata stages belong to `native-postgres-driver:ch06:t01`.

## Bounded Read Manifest

- `src/rs/vertebra-db/src/connection.rs` — Existing `EndpointTarget::{CloudSql, AlloyDb}`, `build_cloud_mtls_config`, `CaAndOptionalCnVerifier`, and `cloudsql_mtls_*` unit tests (`socket2 = { version = "=0.6.5", features = ["all"] }` is already added to `src/rs/vertebra-db/Cargo.toml` and locked in `src/rs/Cargo.lock` by `ch01:t02`, so zero Cargo manifest or lockfile edits occur here) [^db-conn].
- `src/rs/vertebra-postgres/src/cloud/material.rs` — Sole constructor of `EndpointTarget::{CloudSql, AlloyDb}` and `PreparedCloudMaterial`, consuming `CloudSqlDiscovery` (`ch04:t03`) and `AlloyDbDiscovery` (`ch04:t04`).
- `.agents/bundles/specs/native-postgres-driver/research/cloud-material_20261105.md` — Official Cloud SQL Go connector `tls_verify.go` DNS-SAN-then-CN order and AlloyDB SAN requirements [^cloud-research].
- `.agents/bundles/specs/native-postgres-driver/contracts.md` — Chapter [spec.md](../spec.md) and root [contracts.md](../../../contracts.md) cloud TLS 1.3 invariants [^pg-contracts].
- `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` — Shared `vertebra-db` vs optional engine crate boundaries.

## Symbol & Signature Contracts

- `src/rs/vertebra-db/src/connection.rs`:
  - Add `#[derive(Clone, Debug, PartialEq, Eq)] pub enum CloudPeerIdentity`:
    - `CloudSql { server_name: Option<String>, expected_cn: String }` — `expected_cn` must be nonempty `"{project}:{instance}"`.
    - `AlloyDb { expected_san: String }` — `expected_san` is the selected target IP literal (`PUBLIC`/`PRIVATE`) or PSC DNS name (`PSC`), never empty.
  - Extend `EndpointTarget::CloudSql` and `EndpointTarget::AlloyDb` with `peer_identity: CloudPeerIdentity` (updating existing `EndpointTarget` construction sites and unit tests in `connection.rs`).
  - Update `build_cloud_mtls_config`:
    - Construct `ClientConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])` using the `ring` provider so TLS 1.2 and older protocol versions are rejected at handshake time, independent of direct `ConnectOptions.tls_mode`.
    - Delegate peer certificate verification to `CloudPeerVerifier`:
      1. Always verify chain trust and certificate validity time against `server_ca_pem` via `WebPkiServerVerifier` first.
      2. For `CloudPeerIdentity::AlloyDb { expected_san }`: parse `ServerName::try_from(expected_san)` (covering both `IpAddress` and `DnsName`) and require `rustls::client::verify_server_name` SAN match. Never fall back to Subject CN or CA-only acceptance.
      3. For `CloudPeerIdentity::CloudSql { server_name, expected_cn }`: follow official `tls_verify.go` semantics — when `server_name` is `Some(name)` (nonempty), try `verify_server_name` against `name` and, if `name` ends with `.`, retry with trailing `.` stripped (`trim_end_matches('.')`); if SAN verification succeeds, accept immediately; if `server_name` is `None` or SAN verification fails, fall back to exact Subject `CommonName == expected_cn`; if CN also fails (or `expected_cn` is empty), reject with `CertificateError::NotValidForName`.
    - Preserve TLS 1.3 signature verification via `verify_tls13_signature` on the `ring` crypto provider.
  - Expose `pub fn peer_leaf_der(&self) -> Option<&[u8]>` on `TransportStream`, returning the verified leaf `CertificateDer` slice from `TlsTcp` (`conn.peer_certificates().and_then(|certs| certs.first()).map(|c| c.as_ref())`) so `vertebra-postgres` SCRAM channel binding can hash the exact peer leaf certificate.
- `src/rs/vertebra-postgres/src/cloud/material.rs`:
  - As the sole constructor of `EndpointTarget::{CloudSql, AlloyDb}` and `PreparedCloudMaterial` from `CloudSqlDiscovery` (`ch04:t03`) and `AlloyDbDiscovery` (`ch04:t04`), populate `CloudPeerIdentity::CloudSql { server_name, expected_cn: format!("{}:{}", uri.project, uri.instance) }` from Cloud SQL `connectSettings` DNS metadata and URI, and `CloudPeerIdentity::AlloyDb { expected_san }` from the selected AlloyDB IP or PSC DNS target, never defaulting silently to legacy CN-only or CA-only policies.

## Edge Cases & Error Semantics

- Valid AlloyDB CA signature with matching Subject CN (`cluster-a`) but missing/mismatched IP or DNS SAN must fail with `TransportError::Tls` (`sqlstate() == Some("08001")`).
- Cloud SQL certificate with valid CA, matching DNS SAN (`"db.psc.goog."` normalized to `"db.psc.goog"`), and non-matching Subject CN (`"other:inst"`) succeeds via the primary SAN path.
- Cloud SQL certificate with no SAN (or mismatched SAN when `server_name` is set) but exact Subject CN `"my-proj:my-inst"` succeeds via the legacy CN fallback.
- Cloud SQL certificate where both SAN and `expected_cn` fail (or `expected_cn` is empty) fails with `TransportError::Tls`.
- Cloud SQL or AlloyDB mock server configured with `rustls::version::TLS12` only fails handshake immediately with `TransportError::Tls`.
- Wrong/untrusted CA fails before SAN/CN acceptance for both Cloud SQL and AlloyDB.
- Direct TCP `TransportStream` `TlsMode` (`Disable`, `Prefer`, `Require`, `VerifyCa`, `VerifyFull`) continues to allow TLS 1.2 and TLS 1.3 per standard PostgreSQL `SSLRequest` behavior.

## Public-Seam Test Blueprint

- **Canonical test files**: `src/rs/vertebra-db/src/connection.rs` (`cloud_peer_identities` and `cloudsql_mtls_*` unit tests) and `src/rs/vertebra-postgres/tests/postgres_cloud_wire_tests.rs` (`cloud_identity_*` integration tests).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| AlloyDB wrong SAN with valid CA | `CloudPeerIdentity::AlloyDb { expected_san: "10.1.2.3" }`, server leaf signed by trusted CA with CN `"10.1.2.3"` and SAN `"10.9.9.9"` | Handshake fails with `Err(TransportError::Tls(_))` (repairs old CA-only `expected_cn: None` bug). |
| AlloyDB IP SAN & PSC DNS SAN success | `expected_san: "10.1.2.3"` with IP SAN `10.1.2.3`, and `expected_san: "abc.alloydb-psc.goog"` with DNS SAN `"abc.alloydb-psc.goog"` over TLS 1.3 | Both handshakes succeed; `stream.peer_leaf_der()` equals exact server leaf DER bytes. |
| Cloud TLS 1.2 server rejection | `EndpointTarget::CloudSql` and `EndpointTarget::AlloyDb` dialing a `rustls` server restricted to `&rustls::version::TLS12` | Both handshakes fail with `Err(TransportError::Tls(_))`. |
| Cloud SQL DNS-SAN-only with trailing dot | `CloudPeerIdentity::CloudSql { server_name: Some("inst.psc.sql.goog."), expected_cn: "proj:inst" }`, leaf SAN `"inst.psc.sql.goog"`, CN `"different:cn"` | Handshake succeeds via normalized DNS SAN match and retains leaf DER. |
| Cloud SQL legacy exact CN fallback | `CloudPeerIdentity::CloudSql { server_name: Some("inst.psc.sql.goog"), expected_cn: "proj:inst" }`, leaf has no SAN and CN `"proj:inst"` | Handshake succeeds via exact `project:instance` CN fallback. |
| Cloud SQL wrong SAN and wrong CN | `server_name: Some("inst.psc.sql.goog")`, `expected_cn: "proj:inst"`, leaf SAN `"other.sql.goog"` and CN `"proj:other"` | Handshake fails with `Err(TransportError::Tls(_))`. |
| Cloud untrusted CA rejection | Leaf has matching SAN `"10.1.2.3"` and CN `"proj:inst"`, signed by an untrusted CA | Handshake fails with `Err(TransportError::Tls(_))`. |

## Verification

- Initial Red baseline:
  - `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-db --lib cloud_peer_identities`
  - `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_cloud_wire_tests cloud_identity_`
- Final Green + Clippy gate:
  - `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-db --lib cloud_peer_identities`
  - `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_cloud_wire_tests cloud_identity_`
  - `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-db --lib cloudsql_mtls_`
  - `cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-db -p vertebra-postgres --all-targets -- -D warnings`

## Acceptance Criteria

- [ ] `EndpointTarget::{CloudSql, AlloyDb}` carry explicit `CloudPeerIdentity`, enforcing TLS 1.3 only without altering direct TCP `sslmode` behavior (R4, R9).
- [ ] AlloyDB requires verified CA plus exact IP or DNS SAN match with zero CN/CA-only fallback; Cloud SQL verifies CA first, tries normalized DNS SAN, falls back to exact `project:instance` CN, and rejects if both fail (R4, R9).
- [ ] `TransportStream::peer_leaf_der()` exposes the verified peer leaf DER bytes from `PreparedCloudMaterial` connections for downstream `SCRAM-SHA-256-PLUS` binding, and commit `fix(db): verify cloud database peer identities` passes all canonical tests (R4, R9).

## Notes & Discoveries

- None.

[^db-conn]: `src/rs/vertebra-db/src/connection.rs::TransportStream` — Shared cloud mTLS configuration and certificate verifier.
[^cloud-research]: `.agents/bundles/specs/native-postgres-driver/research/cloud-material_20261105.md` — Official Cloud SQL and AlloyDB peer verification rules.
[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Native PostgreSQL cloud TLS 1.3 and peer identity contract.
