---
type: Task
id: "native-postgres-driver:ch04:t03"
chapter_id: ch04-cloud-material-and-admin-discovery
title: Discover Cloud SQL v1beta4 addresses and issue scoped ephemeral certificates
description: Implement cloud::cloudsql calling Cloud SQL Admin v1beta4 connectSettings and generateEphemeralCert with separate Admin vs sqlservice.login tokens, strict engine/region/IP validation, and validated port 3307 mTLS material.
status: stable
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch04:t02"
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
  - src/rs/vertebra-postgres/src/cloud/cloudsql.rs
tests:
  - src/rs/vertebra-postgres/tests/postgres_cloud_wire_tests.rs
estimated_loc: 245
plan_revision: 5
plan_commit: 0147d80c
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
updated_at: "2026-11-08T02:05:22Z"
commit: aa1cfbb1
sources:
  - id: cloud-research
    resource: .agents/bundles/specs/native-postgres-driver/research/cloud-material_20261105.md
    title: Cloud SQL Admin v1beta4 connectSettings and generateEphemeralCert wire schemas
  - id: db-dialer
    resource: src/rs/vertebra-db/src/dialer.rs::CloudSqlInstanceUri
    title: CloudSqlInstanceUri parsing, expected project:instance CN, and default port 3307
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Cloud SQL IP-type selection and IAM vs password credential separation
tags:
  - postgres
  - cloud-sql
  - mtls
  - iam
---

# Task t03: Discover Cloud SQL `v1beta4` Addresses and Issue Scoped Ephemeral Certificates

## Objective & Scope Boundary

Implement `src/rs/vertebra-postgres/src/cloud/cloudsql.rs` and wire `CloudResourceTarget::CloudSql` into `CloudConnector::prepare` (`cloud/material.rs`) to discover instance metadata via `GET .../sql/v1beta4/projects/{project}/instances/{instance}/connectSettings` and issue ephemeral client certificates via `POST ...:generateEphemeralCert` using separate Admin (`cloud-platform`) and IAM login (`sqlservice.login`) tokens, returning validated port `3307` `PreparedCloudMaterial` [^cloud-research] [^db-dialer] [^pg-contracts]. Mandatory TLS 1.3 SAN-with-CN-fallback transport verification lands in `ch05:t01` and live Cloud SQL session startup lands in `ch05:t04` ([../spec.md](../spec.md)).

## Bounded Read Manifest

- `src/rs/vertebra-db/src/dialer.rs::{CloudSqlInstanceUri, IpType, AuthType}` — Instance URI fields (`project`, `region`, `instance`), `expected_server_cn()` (`project:instance`), and default port `3307` (note: percent-encode `project` and `instance` path segments rather than calling unencoded `admin_api_path()`) [^db-dialer].
- `src/rs/vertebra-postgres/src/cloud/mod.rs` — Cloud module exports.
- `src/rs/vertebra-postgres/src/cloud/material.rs` — `CloudConnector`, `CloudConnectorConfig`, `PreparedCloudMaterial`, and bounded `1 MiB` JSON HTTP helpers from `ch04:t01`.
- `src/rs/vertebra-postgres/src/cloud/keys.rs` — `generate_ephemeral_rsa_keypair` and `validate_issued_certificate_chain` from `ch04:t02`.
- `src/rs/vertebra-postgres/tests/postgres_cloud_wire_tests.rs` — Mock HTTP and injected clock test harness from `ch04:t01`–`t02`.
- `.agents/bundles/specs/native-postgres-driver/research/cloud-material_20261105.md` — Cloud SQL `v1beta4` `connectSettings` and `generateEphemeralCert` JSON field contracts [^cloud-research].

## Symbol & Signature Contracts

- `src/rs/vertebra-postgres/src/cloud/mod.rs`:
  - Export `pub mod cloudsql;`.
- `src/rs/vertebra-postgres/src/cloud/cloudsql.rs`:
  - Typed `serde` DTOs (never untyped `serde_json::Value` path guessing or invented `clientCert` / private-key response fields):
    - `ConnectSettingsResponse`: `server_ca_cert: Option<SslCert>` (`#[serde(rename = "serverCaCert")]`), `ip_addresses: Vec<IpMapping>` (`#[serde(default, rename = "ipAddresses")]`, `r#type: String`, `ip_address: String`), `region: String`, `database_version: String` (`#[serde(rename = "databaseVersion")]`), `psc_enabled: bool` (`#[serde(default, rename = "pscEnabled")]`), `dns_name: Option<String>` (`#[serde(default, rename = "dnsName")]`), `dns_names: Vec<DnsNameEntry>` (`#[serde(default, rename = "dnsNames")]`).
    - `GenerateEphemeralCertRequest<'a>`: `public_key: &'a str`, `#[serde(skip_serializing_if = "Option::is_none")] access_token: Option<&'a str>`.
    - `GenerateEphemeralCertResponse`: `ephemeral_cert: Option<SslCert>` (`#[serde(rename = "ephemeralCert")]` containing `cert: String`).
  - Raw discovery struct returned to `cloud/material.rs` (`cloudsql.rs` never constructs `EndpointTarget::CloudSql`, `PreparedCloudMaterial`, or `GcpTokenCache`, keeping `cloud/material.rs` the sole owner of those seams for `ch05:t01` and `ch05:t03`):
    - `pub(crate) struct CloudSqlDiscovery { pub host: String, pub port: u16, pub peer_dns_name: Option<String>, pub expected_cn: String, pub server_ca_pem: Vec<u8>, pub client_identity: ValidatedClientIdentity }`
  - `pub(crate) async fn fetch_cloudsql_discovery(connector: &CloudConnector, uri: &CloudSqlInstanceUri, ip_type: IpType, admin_endpoint: &str, admin_token: &str, iam_access_token: Option<&str>, absolute_deadline: tokio::time::Instant) -> Result<CloudSqlDiscovery, PostgresError>`:
    1. Build percent-encoded instance base `{admin_endpoint}/sql/v1beta4/projects/{enc(project)}/instances/{enc(instance)}` (encoding `:` in domain-scoped projects like `google.com:proj`).
    2. `GET {instance_base}/connectSettings` with `Authorization: Bearer <admin_token>` and generate an ephemeral PKCS#1 RSA-2048 keypair (`keys::generate_ephemeral_rsa_keypair(absolute_deadline)`):
       - Require `settings.region == uri.region` and `settings.database_version.starts_with("POSTGRES")` (reject `MYSQL_*`, `SQLSERVER_*`, or region mismatch with `PostgresError::Config` / `Transport`).
       - Require non-empty `serverCaCert.cert`.
       - Select network host strictly for `ip_type` on port `3307`: `IpType::Public` -> `"PRIMARY"` IP; `IpType::Private` -> `"PRIVATE"` IP; `IpType::Psc` -> advertised PSC DNS (`dns_names` PSC entry or `psc_enabled && dns_name`), failing explicitly if absent without ever falling back to `localhost` or another `IpType`.
    3. `POST {instance_base}:generateEphemeralCert` with `Authorization: Bearer <admin_token>` and JSON body `{"public_key": keypair.public_key_pem(), "access_token": iam_tok}` when `iam_access_token == Some(iam_tok)`, or `{"public_key": keypair.public_key_pem()}` (omitting `access_token` completely) when `iam_access_token.is_none()`.
    4. Validate `[ephemeral_cert.cert]` against `keypair` and `now_ms` via `keys::validate_issued_certificate_chain` and return `CloudSqlDiscovery { host, port: 3307, peer_dns_name, expected_cn: uri.expected_server_cn(), server_ca_pem, client_identity }`.
- `src/rs/vertebra-postgres/src/cloud/material.rs`:
  - In `CloudConnector::prepare` for `CloudResourceTarget::CloudSql(uri)`: resolve the Admin bearer token (`https://www.googleapis.com/auth/cloud-platform` / `sqlservice.admin`) from the Admin credential cache and, in `AuthType::Iam`, separately resolve the login token snapshot (`https://www.googleapis.com/auth/sqlservice.login`) from the login credential cache (never sending the login token in the HTTP `Authorization` header or sending the Admin token as `access_token`!). Call `cloudsql::fetch_cloudsql_discovery(...)`, compute `effective_expiry_ms = min(discovery.client_identity.signed_not_after_ms, login_token_expiry_ms)` in IAM mode (or `discovery.client_identity.signed_not_after_ms` in password mode), and construct `EndpointTarget::CloudSql` + `PreparedCloudMaterial` solely inside `cloud/material.rs` before caching.

## Edge Cases & Error Semantics

- Domain-scoped project IDs (`google.com:my-proj:us-central1:db1`) percent-encode `google.com%3Amy-proj` in the HTTP URL path while retaining exact `"google.com:my-proj:db1"` in `expected_cn`.
- Requesting `IpType::Private` when `ipAddresses` contains only `PRIMARY` (or `IpType::Psc` when `pscEnabled` is `false` / no PSC DNS name exists) fails terminally with `PostgresError::Transport` / `Config` and never dials the `PRIMARY` IP.
- Non-PostgreSQL `databaseVersion` (e.g. `"MYSQL_8_0"`), `region` mismatch (`"europe-west1"` vs URI `"us-central1"`), missing `serverCaCert.cert`, or missing `ephemeralCert.cert` fails before cache publication.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/rs/vertebra-postgres/tests/postgres_cloud_wire_tests.rs` (test names prefixed `cloudsql_admin_`).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| `cloudsql_admin_iam_discovery_and_cert` | `google.com:proj:us-central1:pg1`, `IpType::Private`, `AuthType::Iam`, Admin token `"vtb-admin-tok"`, login token `"vtb-login-tok"` (`expiry = +900s`), leaf `NotAfter = +3600s` | `GET /sql/v1beta4/projects/google.com%3Aproj/instances/pg1/connectSettings` and `POST ...:generateEphemeralCert` carry `Authorization: Bearer vtb-admin-tok`; POST JSON has `"public_key"` (`BEGIN RSA PUBLIC KEY`) + `"access_token": "vtb-login-tok"`; selects `PRIVATE` IP `:3307`, `expected_cn = Some("google.com:proj:pg1")`, and `effective_expiry_ms == now_ms + 900_000`. |
| `cloudsql_admin_password_omits_access_token` | `my-proj:us-east1:pg2`, `IpType::Public`, `AuthType::Password` | POST `:generateEphemeralCert` JSON object has exact single key `{"public_key": ...}` with zero `"access_token"` key; selects `PRIMARY` IP `:3307`; `effective_expiry_ms == signed_cert_not_after_ms`. |
| `cloudsql_admin_psc_dns_selection_and_missing_ip` | (a) `IpType::Psc` with `pscEnabled = true`, `dnsName = "abc.psc.sql.goog"`; (b) `IpType::Private` when only `PRIMARY` exists; (c) `IpType::Psc` with `pscEnabled = false` | (a) selects `"abc.psc.sql.goog:3307"`; (b) and (c) fail explicitly without switching network types. |
| `cloudsql_admin_region_and_engine_guards` | `connectSettings` returns `region = "us-west1"` for `"proj:us-central1:pg1"` or `databaseVersion = "MYSQL_8_0"` | Fails before `:generateEphemeralCert` (or before caching) and leaves `CloudMaterialCache` empty. |
| `cloudsql_admin_missing_ca_or_key_mismatch` | Missing `serverCaCert.cert`, missing `ephemeralCert.cert`, or `ephemeralCert.cert` signed for wrong RSA public key | Fails terminally via `validate_issued_certificate_chain` and never caches partial material. |

## Verification

Write the `cloudsql_admin_*` mock Admin API tests in `src/rs/vertebra-postgres/tests/postgres_cloud_wire_tests.rs` first (confirming Red), implement `cloud/cloudsql.rs` and `material.rs` dispatch, and verify exit `0`:

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_cloud_wire_tests cloudsql_admin_
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres
```

Commit as `feat(postgres): issue Cloud SQL connection credentials`.

## Acceptance Criteria

- [x] `cloud::cloudsql` calls exact `v1beta4` `connectSettings` and `:generateEphemeralCert` paths with percent-encoded segments, returning raw `CloudSqlDiscovery` to `cloud/material.rs` while `cloud/material.rs` enforces strict Admin bearer vs `sqlservice.login` body token separation and constructs `PreparedCloudMaterial` (R5, R9).
- [x] `region`, `POSTGRES*` `databaseVersion`, requested `PUBLIC`/`PRIVATE`/`PSC` endpoint availability, non-empty server CA, and issued RSA leaf identity/expiry are all verified before caching (R4, R9).
- [x] Returned `PreparedCloudMaterial` targets port `3307` with metadata DNS + exact `project:instance` CN identity metadata for `ch05:t01` (R4, R9).

## Notes & Discoveries

- [2026-11-07 05:00] Percent-encoding `CloudSqlInstanceUri.project` and `instance` in `cloudsql.rs` fixes colon-bearing domain-scoped project IDs without altering `CloudSqlInstanceUri::expected_server_cn()` (`project:instance`).

[^cloud-research]: `.agents/bundles/specs/native-postgres-driver/research/cloud-material_20261105.md` — Cloud SQL `v1beta4` REST schema and IAM token binding rules.
[^db-dialer]: `src/rs/vertebra-db/src/dialer.rs::CloudSqlInstanceUri` — Shared Cloud SQL URI parser and `:3307` CN conventions.
[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Cloud SQL network selection and credential isolation contracts.
