---
type: Task
id: "native-postgres-driver:ch04:t04"
chapter_id: ch04-cloud-material-and-admin-discovery
title: Discover AlloyDB v1alpha targets and issue metadata-enabled client certificates
description: Implement cloud::alloydb calling coherent AlloyDB v1alpha connectionInfo and generateClientCertificate endpoints with PKCS#1 RSA keys, useMetadataExchange=true, alloydb.login token resolution across both auth modes, and port 5433 SAN targets.
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch04:t03"
read_set:
  - src/rs/vertebra-db/src/dialer.rs
  - src/rs/vertebra-postgres/src/cloud/mod.rs
  - src/rs/vertebra-postgres/src/cloud/material.rs
  - src/rs/vertebra-postgres/src/cloud/keys.rs
  - src/rs/vertebra-postgres/tests/postgres_cloud_wire_tests.rs
  - .agents/bundles/specs/native-postgres-driver/research/cloud-material_20261105.md
files:
  - src/rs/vertebra-postgres/src/cloud/mod.rs
  - src/rs/vertebra-postgres/src/cloud/material.rs
  - src/rs/vertebra-postgres/src/cloud/alloydb.rs
tests:
  - src/rs/vertebra-postgres/tests/postgres_cloud_wire_tests.rs
estimated_loc: 245
plan_revision: 9
plan_commit: b0de4dcb
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
    title: Coherent AlloyDB v1alpha connectionInfo and generateClientCertificate REST contracts
  - id: db-dialer
    resource: src/rs/vertebra-db/src/dialer.rs::AlloyDbClusterUri
    title: AlloyDbClusterUri fields, port 5433, and AuthType 1/2 MetadataExchangeRequest codes
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: AlloyDB IP-type selection, IAM/built-in login token rules, and redaction invariants
tags:
  - postgres
  - alloydb
  - mtls
  - metadata-exchange
---

# Task t04: Discover AlloyDB `v1alpha` Targets and Issue Metadata-Enabled Client Certificates

## Objective & Scope Boundary

Implement `src/rs/vertebra-postgres/src/cloud/alloydb.rs` and wire `CloudResourceTarget::AlloyDb` into `CloudConnector::prepare` (`cloud/material.rs`) using coherent AlloyDB `v1alpha` REST endpoints (`GET .../connectionInfo` on the instance path and `POST ...:generateClientCertificate` on the cluster path with `useMetadataExchange: true`), obtaining `alloydb.login` tokens in both `AuthType::Password` (`1`) and `AuthType::Iam` (`2`) modes and returning validated port `5433` `PreparedCloudMaterial` [^cloud-research] [^db-dialer] [^pg-contracts]. Mandatory TLS 1.3 IP/DNS-SAN peer verification (`ch05:t01`) and split post-TLS metadata token guards (`ch06:t01`) follow in Chapters 05–06 ([../spec.md](../spec.md)).

## Bounded Read Manifest

- `src/rs/vertebra-db/src/dialer.rs::{AlloyDbClusterUri, IpType, AuthType}` — Parsed `project`, `location`, `cluster`, `instance`, default port `5433`, and `AuthType` codes (`1` built-in, `2` IAM; note: do NOT call `AlloyDbClusterUri::admin_api_path()`, which hardcodes `/v1beta/` without percent-encoding) [^db-dialer].
- `src/rs/vertebra-postgres/src/cloud/mod.rs` — Cloud module exports.
- `src/rs/vertebra-postgres/src/cloud/material.rs` — `CloudConnector`, `CloudConnectorConfig`, `PreparedCloudMaterial`, and bounded `1 MiB` HTTP helpers.
- `src/rs/vertebra-postgres/src/cloud/keys.rs` — `generate_ephemeral_rsa_keypair` and `validate_issued_certificate_chain` from `ch04:t02`.
- `src/rs/vertebra-postgres/tests/postgres_cloud_wire_tests.rs` — Mock Admin server and clock fixtures from `ch04:t01`–`t03`.
- `.agents/bundles/specs/native-postgres-driver/research/cloud-material_20261105.md` — Official Go AlloyDB connector `v1alpha` request/response fields and PSC discovery rules [^cloud-research].

## Symbol & Signature Contracts

- `src/rs/vertebra-postgres/src/cloud/mod.rs`:
  - Export `pub mod alloydb;`.
- `src/rs/vertebra-postgres/src/cloud/alloydb.rs`:
  - Typed `serde` DTOs matching AlloyDB `v1alpha` (never inventing `pem_csr`, `access_token`, or `clientCert` fields, and omitting optional `requestId`):
    - `ConnectionInfoResponse`: `ip_address: Option<String>` (`#[serde(default, rename = "ipAddress")]`), `public_ip_address: Option<String>` (`#[serde(default, rename = "publicIpAddress")]`), `psc_dns_name: Option<String>` (`#[serde(default, rename = "pscDnsName")]`), `psc_auto_dns_name: Option<String>` (`#[serde(default, rename = "pscAutoDnsName")]`), `instance_uid: Option<String>` (`#[serde(default, rename = "instanceUid")]`).
    - `GenerateClientCertificateRequest<'a>`: `#[serde(rename = "publicKey")] public_key: &'a str`, `#[serde(rename = "certDuration")] cert_duration: &'a str` (`"3600s"`), `#[serde(rename = "useMetadataExchange")] use_metadata_exchange: bool` (`true`).
    - `GenerateClientCertificateResponse`: `#[serde(default, rename = "pemCertificateChain")] pem_certificate_chain: Vec<String>`, `#[serde(default, rename = "caCert")] ca_cert: String`.
  - Raw discovery struct returned to `cloud/material.rs` (`alloydb.rs` never constructs `EndpointTarget::AlloyDb`, `PreparedCloudMaterial`, or `GcpTokenCache`, keeping `cloud/material.rs` the sole owner of those seams for `ch05:t01` and `ch05:t03`):
    - `pub(crate) struct AlloyDbDiscovery { pub host: String, pub port: u16, pub peer_dns_name: Option<String>, pub server_ca_pem: Vec<u8>, pub client_identity: ValidatedClientIdentity }`
  - `pub(crate) async fn fetch_alloydb_discovery(connector: &CloudConnector, uri: &AlloyDbClusterUri, ip_type: IpType, admin_endpoint: &str, admin: &GcpTokenSnapshot, absolute_deadline: tokio::time::Instant) -> Result<AlloyDbDiscovery, PostgresError>`:
    1. Use `connector.get_json::<ConnectionInfoResponse>(url, admin, deadline)` and `post_json::<GenerateClientCertificateResponse,_>(url, &body, admin, deadline)` with the borrowed Admin snapshot for quota/bearer headers. Percent-encode `project`, `location`, `cluster`, and `instance` segments to form:
       - `cluster_path = /v1alpha/projects/{p}/locations/{l}/clusters/{c}`
       - `instance_path = {cluster_path}/instances/{i}`
    2. `GET {admin_endpoint}{instance_path}/connectionInfo` with `Authorization: Bearer <admin.access_token>`:
       - Select network host strictly matching `ip_type` on port `5433`: `IpType::Private` -> non-empty `ipAddress`; `IpType::Public` -> non-empty `publicIpAddress`; `IpType::Psc` -> non-empty `pscDnsName` (or `pscAutoDnsName`, stripping any trailing `.` for SAN identity and setting `peer_dns_name = Some(host.clone())`). Fail explicitly if the requested network field is missing or empty without switching to another `IpType` or `localhost`.
    3. Generate PKCS#1 RSA-2048 keypair (`keys::generate_ephemeral_rsa_keypair(absolute_deadline)`) and `POST {admin_endpoint}{cluster_path}:generateClientCertificate` with `Authorization: Bearer <admin.access_token>` and exact JSON body `{"publicKey": keypair.public_key_pem(), "certDuration": "3600s", "useMetadataExchange": true}`.
    4. Require non-empty `caCert` and non-empty `pemCertificateChain`, validate `pemCertificateChain` (preserving leaf-first chain order `[leaf, intermediate, ...]`) against `keypair` and `connector.now_unix_ms()` via `keys::validate_issued_certificate_chain`, and return `AlloyDbDiscovery { host, port: 5433, peer_dns_name, server_ca_pem, client_identity }`.
- `src/rs/vertebra-postgres/src/cloud/material.rs`:
  - In `CloudConnector::prepare` before `AdminDiscoveryFetcher::fetch` dispatch for `CloudResourceTarget::AlloyDb(uri)`: resolve the Admin bearer token (`https://www.googleapis.com/auth/cloud-platform`) from the Admin credential cache AND resolve an `https://www.googleapis.com/auth/alloydb.login` token snapshot from the login credential cache (actual `expiry_ms: u64`, never sentinel lifetime) in **both** `AuthType::Password` (`1` built-in) and `AuthType::Iam` (`2` IAM) modes (AlloyDB's post-TLS `MetadataExchangeRequest` requires `oauth2_token` in both modes; IAM mode additionally reuses that login snapshot as the PostgreSQL startup password in `ch06:t01`). Call `alloydb::fetch_alloydb_discovery(...)`, compute `effective_expiry_ms = min(discovery.client_identity.signed_not_after_ms, login_token_expiry_ms)` (`"3600s"` is only a request hint), and construct `EndpointTarget::AlloyDb` (port `5433`, `auth_type` `1` or `2`, `oauth2_token`) + `PreparedCloudMaterial` solely inside `cloud/material.rs` before caching.

- `AdminDiscoveryFetcher::fetch` implements the existing `CloudMaterialFetcher` lifetime/`Send` contract by calling this raw discovery function, then mapping its validated owned fields to `CloudDiscovery` inside `material.rs`. It receives the resolved `CloudTokenBundle` and forwards the Admin snapshot (preserving quota headers); it never resolves credentials in the REST module. Keep `CloudConnector::prepare`, injected `Clock::now_unix_ms`, per-key flight/result ownership, and existing helper signatures unchanged. Check `Instant::now() < absolute_deadline` and actual login usability before any token-bearing operation and publication. The Admin wire fixture implements the existing production `resolve_address` method with deterministic PSC/hostname-to-loopback resolution and delegates `fetch` to real `AdminDiscoveryFetcher`; no external DNS is used. Canonical cloud tests use their in-file fixtures; no hidden `tests/support/mod.rs` read or import is introduced.

## Edge Cases & Error Semantics

- `POST :generateClientCertificate` hits the **cluster** resource path (`/v1alpha/projects/{p}/locations/{l}/clusters/{c}:generateClientCertificate`), never the `/instances/{i}` path, and never includes `access_token`, `pem_csr`, `clientCert`, or `requestId` in the JSON body.
- Multi-certificate `pemCertificateChain` (`[leaf_pem, intermediate_pem]`) preserves exact leaf-first ordering in `client_cert_chain_pem` while validating `leaf_pem` against the generated RSA-2048 public key.
- Missing `caCert`, empty `pemCertificateChain`, key mismatch, expired leaf `NotAfter`, or missing requested `ipAddress` / `publicIpAddress` / `pscDnsName` fails terminally before `CloudMaterialCache` publication.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/rs/vertebra-postgres/tests/postgres_cloud_wire_tests.rs` (test names prefixed `alloydb_admin_`).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| `alloydb_admin_v1alpha_paths_and_json_contract` | `projects/p1/locations/us-central1/clusters/c1/instances/i1`, `IpType::Private`, `AuthType::Iam`, Admin token `"vtb-admin-tok"`, login token `"vtb-alloy-login"` (`expiry = +1200s`), 2-cert `[leaf, intermediate]` (`NotAfter = +3600s`) | `GET /v1alpha/projects/p1/locations/us-central1/clusters/c1/instances/i1/connectionInfo` + `POST /v1alpha/projects/p1/locations/us-central1/clusters/c1:generateClientCertificate` with exact JSON keys `["certDuration", "publicKey", "useMetadataExchange"]` (`"3600s"`, PKCS#1 RSA PEM, `true`); joins `[leaf, intermediate]` leaf-first; port `5433`, `auth_type == AuthType::Iam` (`2`), `effective_expiry_ms == now_ms + 1_200_000`. |
| `alloydb_admin_password_mode_still_fetches_login_token` | Same URI with `AuthType::Password` (`1`) and `IpType::Public` (`publicIpAddress = "34.120.1.9"`) | Fetches `alloydb.login` token for metadata exchange (`auth_type == AuthType::Password` / `1`), selects `"34.120.1.9:5433"`, and bounds `effective_expiry_ms` by `min(signed_cert_not_after_ms, login_token_expiry_ms)`. |
| `alloydb_admin_psc_dns_and_missing_network_target` | (a) `IpType::Psc` with `pscDnsName = "inst.psc.alloydb.goog."`; (b) `IpType::Public` when `publicIpAddress` is absent (`ipAddress` present) | (a) selects `"inst.psc.alloydb.goog:5433"` with DNS SAN metadata; (b) fails explicitly without falling back to `ipAddress`. |
| `alloydb_admin_missing_fields_key_mismatch_and_expired_chain` | Empty `pemCertificateChain`, empty `caCert`, leaf signed for wrong RSA key, or leaf `NotAfter <= now` despite `"3600s"` request | Each fails terminally and leaves `CloudMaterialCache` unpopulated. |

## Verification

Write the `alloydb_admin_*` mock Admin API tests in `src/rs/vertebra-postgres/tests/postgres_cloud_wire_tests.rs` first (confirming Red), implement `cloud/alloydb.rs` and `material.rs` dispatch, and verify exit `0`:

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_cloud_wire_tests alloydb_admin_
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres
```

Commit as `feat(postgres): issue AlloyDB connection credentials`.

## Acceptance Criteria

- [ ] `cloud::alloydb` calls coherent `v1alpha` `connectionInfo` (instance path) and `:generateClientCertificate` (cluster path) with exact `{"publicKey", "certDuration": "3600s", "useMetadataExchange": true}` JSON and percent-encoded path segments, returning raw `AlloyDbDiscovery` to `cloud/material.rs` (R9).
- [ ] Both `AuthType::Iam` (`2`) and `AuthType::Password` (`1`) resolve `alloydb.login` tokens separately from `cloud-platform` Admin bearer tokens inside `cloud/material.rs`, and `effective_expiry_ms` honors signed leaf `NotAfter` and login token expiry over `"3600s"` (R5, R9).
- [ ] Leaf-first multi-certificate chains, RSA key matching, and requested `PUBLIC`/`PRIVATE`/`PSC` port `5433` SAN targets are validated before cache publication (R4, R9).

## Notes & Discoveries

- [2026-11-07 05:00] Using `v1alpha` in `cloud/alloydb.rs` rather than `AlloyDbClusterUri::admin_api_path()` (`/v1beta/`) aligns `connectionInfo` (`publicIpAddress`, `pscDnsName`) and `:generateClientCertificate` (`useMetadataExchange: true`) with Google's maintained Go AlloyDB connector.

[^cloud-research]: `.agents/bundles/specs/native-postgres-driver/research/cloud-material_20261105.md` — AlloyDB `v1alpha` REST schemas and `MetadataExchangeRequest` token requirements.
[^db-dialer]: `src/rs/vertebra-db/src/dialer.rs::AlloyDbClusterUri` — Shared AlloyDB resource URI and `:5433` `AuthType` codes.
[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — AlloyDB network selection, token isolation, and redaction rules.
