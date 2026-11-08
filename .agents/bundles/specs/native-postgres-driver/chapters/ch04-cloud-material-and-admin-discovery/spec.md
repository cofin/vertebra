---
type: Spec
parent_flow: native-postgres-driver
chapter_id: ch04-cloud-material-and-admin-discovery
title: "Chapter 04: Cloud Material Cache, Ephemeral Keys, and Admin API Discovery"
description: Single-flight deadline-bounded cloud material cache, off-GIL PKCS#1 RSA-2048 ephemeral key generation with X.509 leaf validation, and Cloud SQL v1beta4 plus AlloyDB v1alpha Admin discovery clients.
status: stable
state: planned
planning_status: complete
depends_on_chapters:
  - ch03-scram-and-channel-binding
replan_trigger: null
current_task: "native-postgres-driver:ch04:t01"
plan_revision: 8
plan_commit: 8c540809
state_revision: 0
last_verified_checkpoint: null
last_reviewed_commit: null
created_at: "2026-11-05T00:00:00Z"
updated_at: "2026-11-07T16:40:00Z"
generated:
  by: flow-revise
  at: "2026-11-07T16:40:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-07T16:40:00Z"
    commit: dd432f18
sources:
  - id: cloud-research
    resource: .agents/bundles/specs/native-postgres-driver/research/cloud-material_20261105.md
    title: Cloud SQL v1beta4 and AlloyDB v1alpha Admin API schemas, PKCS#1 RSA keys, and expiry semantics
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Cloud constructor options, credential map separation, and secret redaction invariants
  - id: db-dialer
    resource: src/rs/vertebra-db/src/dialer.rs::CloudSqlInstanceUri
    title: Shared CloudSqlInstanceUri, AlloyDbClusterUri, IpType, and AuthType primitives
  - id: core-gcp-auth
    resource: src/rs/vertebra-core/src/gcp_auth.rs::GcpTokenCache
    title: Native Rust GcpTokenCache construction from owned primitive option maps
tags:
  - postgres
  - cloud-sql
  - alloydb
  - mtls
  - gcp
---

# Chapter 04: Cloud Material Cache, Ephemeral Keys, and Admin API Discovery

<!-- truth: start -->
**Bounded-Truth Summary**

- **Scope:** Implement the zero-sidecar Google Cloud control-plane engine under `src/rs/vertebra-postgres/src/cloud/`: single-flight `CloudMaterialCache` and `CloudConnector` (`ch04:t01`), PKCS#1 RSA-2048 ephemeral key generation and signed X.509 leaf validation (`ch04:t02`), Cloud SQL Admin `v1beta4` raw discovery (`ch04:t03`), and AlloyDB Admin `v1alpha` raw discovery (`ch04:t04`).
- **Key Invariants:**
  - `CloudMaterialCache` keys entries by `(credential_identity, resource_uri, ip_type, admin_endpoint, auth_mode)`, coalesces concurrent refreshes per key under one async singleflight lock, and reuses cached material strictly while `now_ms + 240_000 < effective_expiry_ms` where `effective_expiry_ms = min(signed_cert_not_after_ms, cert_bound_login_token_expiry_ms)`.
  - Ephemeral client keys use `rsa = { version = "0.9", features = ["pem", "sha2"] }` (`RsaPrivateKey::new(&mut OsRng, 2048)`) outside Python GIL attachment to emit PKCS#1 LF PEM (`RSA PUBLIC KEY` / `RSA PRIVATE KEY`) — never `vertebra_db`'s self-signed X.509 certificate helper — and verify leaf SPKI modulus/exponent equality plus signed `NotAfter > now` via `x509-parser = "=0.18.1"` and `rustls-pemfile`.
  - `cloudsql.rs` (`fetch_cloudsql_discovery` -> `CloudSqlDiscovery`) and `alloydb.rs` (`fetch_alloydb_discovery` -> `AlloyDbDiscovery`) return raw validated Admin discovery records to `cloud/material.rs`, making `cloud/material.rs` the **sole** constructor of `EndpointTarget::{CloudSql, AlloyDb}` and `PreparedCloudMaterial` across `ch04` and `ch05`.
  - Admin HTTP calls run through one internal `reqwest::Client` under `tokio::time::timeout_at(absolute_deadline, ...)` with a `1 MiB` response body cap, percent-encoded URI segments, HTTPS enforcement (permitting explicit loopback `http://` overrides solely for mock tests), and separate Admin (`cloud-platform`) vs login (`sqlservice.login` / `alloydb.login`) token caches constructed inside `vertebra-postgres`.
- **Verification Gate:** `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --locked && cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets --locked -- -D warnings`.
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

Chapter 04 builds the control-plane discovery, ephemeral RSA keypair, X.509 leaf validation, and single-flight material caching layer (`R4`, `R5`, `R9`) for Cloud SQL (`:3307`) and AlloyDB (`:5433`) under the parent specification ([../../spec.md](../../spec.md)) and frozen API contracts ([../../contracts.md](../../contracts.md)) [^cloud-research] [^pg-contracts].

### 1.2 Non-Goals

- Mandatory TLS 1.3 `CloudPeerIdentity` verifier enforcement on `EndpointTarget::{CloudSql, AlloyDb}` (`ch05:t01`), full automatic ADC source OAuth scope propagation (`ch05:t03`), and live Cloud SQL / AlloyDB mTLS PostgreSQL data-plane sessions (`ch05:t04`, `ch06:t01`) are out of scope for Chapter 04.
- `cloudsql.rs` and `alloydb.rs` do **not** construct `EndpointTarget` or `PreparedCloudMaterial` directly or own `GcpTokenCache` instances; `cloud/material.rs` centralizes both seams so Chapter 05 never edits the Admin REST clients.

### 1.3 Upstream Baseline & Code Analysis

Shared URI/network primitives (`CloudSqlInstanceUri`, `AlloyDbClusterUri`, `IpType`, `AuthType`) live in `vertebra-db`, and native credential caches come from `vertebra-core::gcp_auth::GcpTokenCache` using owned primitive maps without crossing `cdylib` PyO3 layouts [^db-dialer] [^core-gcp-auth].

Applicable repository guidance: [database-drivers-and-dialers.md](../../../../knowledge/domains/data/database-drivers-and-dialers.md), [ffi-and-gil-discipline.md](../../../../knowledge/patterns/rust/ffi-and-gil-discipline.md), and [pyo3-bridge SKILL.md](../../../../../skills/pyo3-bridge/SKILL.md).

## 2. Requirements, Concrete Contracts & Architecture

### Concrete Module & Signature Contracts

```rust
// src/rs/vertebra-postgres/src/cloud/material.rs
pub const MATERIAL_REFRESH_LEAD_MS: u64 = 240_000;
pub const MAX_ADMIN_RESPONSE_BYTES: usize = 1_048_576;

pub enum CloudResourceTarget {
    CloudSql(vertebra_db::dialer::CloudSqlInstanceUri),
    AlloyDb(vertebra_db::dialer::AlloyDbClusterUri),
}

pub struct CloudConnectorConfig { /* target, ip_type, auth_type, cloud_auth_options, iam_auth_options, admin_endpoint, refresh_lead_ms */ }
pub struct PreparedCloudMaterial { /* endpoint_target, peer_dns_name, expected_cn, server_ca_pem, client_cert_chain_pem, client_key_pem, signed_cert_not_after_ms, effective_expiry_ms, login_token */ }
pub struct CloudMaterialCache { /* singleflight per CacheKey + injected Clock seam */ }
pub struct CloudConnector { /* reqwest::Client + CloudMaterialCache + scope-distinct token caches */ }
impl CloudConnector {
    pub async fn prepare(&self, config: &CloudConnectorConfig, absolute_deadline: tokio::time::Instant) -> Result<PreparedCloudMaterial, PostgresError>;
}

// src/rs/vertebra-postgres/src/cloud/keys.rs
pub struct EphemeralRsaKeyPair { /* PKCS#1 public_key_pem, redacted private_key_pem, modulus/exponent */ }
pub struct ValidatedClientIdentity { pub cert_chain_pem: Vec<u8>, pub private_key_pem: Vec<u8>, pub signed_not_after_ms: u64 }
pub async fn generate_ephemeral_rsa_keypair(deadline: tokio::time::Instant) -> Result<EphemeralRsaKeyPair, PostgresError>;
pub fn validate_issued_certificate_chain(chain_pems: &[String], expected_key: &EphemeralRsaKeyPair, now_unix_ms: u64) -> Result<ValidatedClientIdentity, PostgresError>;

// src/rs/vertebra-postgres/src/cloud/{cloudsql,alloydb}.rs (raw discovery returned to material.rs)
pub(crate) struct CloudSqlDiscovery { pub host: String, pub port: u16, pub peer_dns_name: Option<String>, pub expected_cn: String, pub server_ca_pem: Vec<u8>, pub client_identity: ValidatedClientIdentity }
pub(crate) struct AlloyDbDiscovery { pub host: String, pub port: u16, pub peer_dns_name: Option<String>, pub server_ca_pem: Vec<u8>, pub client_identity: ValidatedClientIdentity }
```

### Caller -> Seam -> Collaborator Map

- `CloudConnector::prepare` (`cloud/material.rs`) -> `CloudMaterialCache` singleflight slot + scope-distinct token resolution (`cloud-platform` Admin token + `sqlservice.login` / `alloydb.login` login snapshot) -> `cloudsql::fetch_cloudsql_discovery` (`cloud/cloudsql.rs`) or `alloydb::fetch_alloydb_discovery` (`cloud/alloydb.rs`).
- `cloudsql.rs` / `alloydb.rs` -> `keys::generate_ephemeral_rsa_keypair` (`tokio::task::spawn_blocking` off-GIL RSA-2048 PKCS#1 PEM) + bounded `1 MiB` `reqwest::Client` `GET`/`POST` under `tokio::time::timeout_at(absolute_deadline, ...)` + `keys::validate_issued_certificate_chain` (`x509-parser = "=0.18.1"` + `rsa::pkcs8::DecodePublicKey`) -> raw `CloudSqlDiscovery` / `AlloyDbDiscovery` -> `cloud/material.rs` (sole constructor of `EndpointTarget::{CloudSql, AlloyDb}` and `PreparedCloudMaterial`) -> `CloudMaterialCache`.

### Edge-Case & Invariant Matrix

| Scenario | Input / Control-Plane Condition | Required Behavior |
| --- | --- | --- |
| Concurrent cold/expiring opens | `N` tasks call `prepare` when cache is empty or `now_ms + 240_000 >= effective_expiry_ms` | Single Admin discovery + cert issuance runs; all `N` callers share result; failed refresh never returns stale/expired material; cancelled caller releases lock cleanly. |
| API duration hint vs signed X.509 `NotAfter` | AlloyDB `certDuration: "3600s"` or Cloud SQL response returns leaf signed for only `300s` (or already expired) | `effective_expiry_ms = min(signed_not_after_ms, cert_bound_token_expiry_ms)`; expired leaf fails terminally before cache publication. |
| Issued leaf key mismatch | Admin mock returns valid X.509 cert whose SPKI RSA `(n, e)` differs from generated `EphemeralRsaKeyPair` | `validate_issued_certificate_chain` rejects with `PostgresError::Transport` / `Protocol` and never caches. |
| Cloud SQL engine / region / IP mismatch | `connectSettings` returns `MYSQL_8_0`, mismatched `region`, or omits requested `PUBLIC`/`PRIVATE`/`PSC` address | Fail explicitly without falling back to `localhost` or another network type. |
| AlloyDB API version & body contract | `AlloyDbClusterUri` target (`PUBLIC` / `PRIVATE` / `PSC`, `Password` / `Iam`) | Call coherent `v1alpha` `connectionInfo` (instance path) and `:generateClientCertificate` (cluster path) with `{"publicKey", "certDuration": "3600s", "useMetadataExchange": true}`, fetching `alloydb.login` token in both auth modes. |

### Alternatives Comparison

| Option | Decision & Rationale |
| --- | --- |
| Fresh `rsa` PKCS#1 public key PEM vs `vertebra_db::dialer::generate_ephemeral_keypair_pem` | **Chosen:** Implement `cloud::keys::EphemeralRsaKeyPair` emitting `-----BEGIN RSA PUBLIC KEY-----` (`pkcs1`). `dialer::generate_ephemeral_keypair_pem` emits a self-signed X.509 `CERTIFICATE`, which Google Cloud SQL and AlloyDB Admin APIs reject for `public_key` / `publicKey`. |
| Coherent AlloyDB `v1alpha` REST endpoints vs `dialer.rs`'s `v1beta` helper | **Chosen:** Use `v1alpha` for both `connectionInfo` and `:generateClientCertificate` in `cloud/alloydb.rs`, matching Google's maintained Go AlloyDB connector including `publicIpAddress` and `pscDnsName` discovery fields. |

## 3. Documentation & OKF Knowledge Plan

Progress: `[0/0 files documented in ch04]`

- Knowledge updates are deferred to Chapter 30 (`ch30:t02`), which documents the Cloud SQL `v1beta4` / AlloyDB `v1alpha` zero-sidecar material lifecycle and PKCS#1 validation invariants in `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md`.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task(s) | Public Seam / Test Verification |
| --- | --- | --- |
| R4 (Validated ephemeral RSA client keys and signed X.509 identity material) | `ch04:t02`, `ch04:t03`, `ch04:t04` | `src/rs/vertebra-postgres/tests/postgres_cloud_wire_tests.rs` |
| R5 (Separate Admin vs login credential caches and IAM token lifetime bounds) | `ch04:t01`, `ch04:t03`, `ch04:t04` | `src/rs/vertebra-postgres/tests/postgres_cloud_wire_tests.rs` |
| R9 (Zero-sidecar Cloud SQL and AlloyDB discovery, single-flight renewal, and total deadline) | `ch04:t01`, `ch04:t02`, `ch04:t03`, `ch04:t04` | `src/rs/vertebra-postgres/tests/postgres_cloud_wire_tests.rs` |

## 5. Implementation Plan

- [ ] native-postgres-driver:ch04:t01 — Cache expiring cloud connection material under one total deadline (tasks/t01-cloud-material-cache.md)
- [ ] native-postgres-driver:ch04:t02 — Generate PKCS#1 RSA-2048 ephemeral keys and validate issued X.509 chains (tasks/t02-cloud-ephemeral-keys.md)
- [ ] native-postgres-driver:ch04:t03 — Discover Cloud SQL v1beta4 addresses and issue scoped ephemeral certificates (tasks/t03-cloudsql-admin-client.md)
- [ ] native-postgres-driver:ch04:t04 — Discover AlloyDB v1alpha targets and issue metadata-enabled client certificates (tasks/t04-alloydb-admin-client.md)

**Chapter Recovery Checkpoint:** `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --locked && cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets --locked -- -D warnings` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Verified Baseline**: `HEAD @ dd432f18`; chapter `state: planned`, `planning_status: complete`, depends on `ch03-scram-and-channel-binding`.
- **Remaining Tasks in Chapter**: `t01`, `t02`, `t03`, `t04` (`4/4` pending), awaiting `native-postgres-driver:ch03:t03`.
- **Documentation & OKF Status**: `[0/0 files documented in ch04]`; Cloud SQL `v1beta4` and AlloyDB `v1alpha` OKF curation scheduled in `ch30:t02`.
- **Downstream Dependents**: Unblocks `ch05-cloud-tls-and-scoped-iam` (`ch05:t01`–`ch05:t04`).

[^cloud-research]: `.agents/bundles/specs/native-postgres-driver/research/cloud-material_20261105.md` — Cloud SQL `v1beta4` and AlloyDB `v1alpha` Admin REST schemas and key material rules.
[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Cloud constructor option maps and credential redaction invariants.
[^db-dialer]: `src/rs/vertebra-db/src/dialer.rs::CloudSqlInstanceUri` — Shared Cloud SQL and AlloyDB URI, IP-type, and auth-mode types.
[^core-gcp-auth]: `src/rs/vertebra-core/src/gcp_auth.rs::GcpTokenCache` — Native Rust Google Cloud token cache primitive.
