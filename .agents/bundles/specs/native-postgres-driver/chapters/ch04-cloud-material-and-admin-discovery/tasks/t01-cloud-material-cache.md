---
type: Task
id: "native-postgres-driver:ch04:t01"
chapter_id: ch04-cloud-material-and-admin-discovery
title: Cache expiring cloud connection material under one total deadline
description: Implement CloudConnectorConfig, PreparedCloudMaterial, singleflight CloudMaterialCache (240s refresh lead), and bounded 1 MiB reqwest HTTP helpers under a single absolute deadline.
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch03:t03"
read_set:
  - src/rs/vertebra-core/src/gcp_auth.rs
  - src/rs/vertebra-db/src/connection.rs
  - src/rs/vertebra-db/src/dialer.rs
  - src/rs/vertebra-postgres/src/config.rs
  - .agents/bundles/specs/native-postgres-driver/contracts.md
  - .agents/bundles/specs/native-postgres-driver/research/cloud-material_20261105.md
files:
  - src/rs/vertebra-postgres/src/lib.rs
  - src/rs/vertebra-postgres/src/cloud/mod.rs
  - src/rs/vertebra-postgres/src/cloud/material.rs
tests:
  - src/rs/vertebra-postgres/tests/postgres_cloud_wire_tests.rs
estimated_loc: 240
plan_revision: 4
plan_commit: 45ff9563
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
    title: Single-flight 240s refresh lead, effective expiry, and bounded 1 MiB Admin HTTP client rules
  - id: core-gcp-auth
    resource: src/rs/vertebra-core/src/gcp_auth.rs::GcpTokenCache
    title: Native GcpTokenCache::from_options construction inside vertebra-postgres
  - id: db-dialer
    resource: src/rs/vertebra-db/src/dialer.rs::CloudSqlInstanceUri
    title: Shared CloudSqlInstanceUri, AlloyDbClusterUri, IpType, and AuthType definitions
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Separate cloud_auth_options vs iam_auth_options and redaction guarantees
tags:
  - postgres
  - cloud
  - cache
  - singleflight
---

# Task t01: Cache Expiring Cloud Connection Material Under One Total Deadline

## Objective & Scope Boundary

Create `src/rs/vertebra-postgres/src/cloud/mod.rs` and `src/rs/vertebra-postgres/src/cloud/material.rs` implementing `CloudConnectorConfig`, `PreparedCloudMaterial`, single-flight `CloudMaterialCache` (`240s` refresh lead), bounded `1 MiB` `reqwest` JSON transport helpers, and crate-local `GcpTokenCache` wiring under the caller's single `absolute_deadline` [^cloud-research] [^core-gcp-auth] [^db-dialer] [^pg-contracts]. PKCS#1 RSA key generation (`ch04:t02`), Cloud SQL `v1beta4` (`ch04:t03`), AlloyDB `v1alpha` (`ch04:t04`), and full ADC scope propagation (`ch05:t03`) build on this seam ([../spec.md](../spec.md)).

## Bounded Read Manifest

- `src/rs/vertebra-core/src/gcp_auth.rs::GcpTokenCache` — Existing `from_options` constructor and `Clock` trait seam [^core-gcp-auth].
- `src/rs/vertebra-db/src/connection.rs::EndpointTarget` — Shared `CloudSql` and `AlloyDb` transport targets.
- `src/rs/vertebra-db/src/dialer.rs::{CloudSqlInstanceUri, AlloyDbClusterUri, IpType, AuthType}` — Shared cloud target types [^db-dialer].
- `src/rs/vertebra-postgres/src/config.rs` — `PostgresConfig` cloud fields (`cloud_sql_instance`, `cloud_sql_ip_type`, `alloydb_instance_uri`, `alloydb_ip_type`, `enable_iam_auth`, `cloud_auth_options`, `iam_auth_options`).
- `.agents/bundles/specs/native-postgres-driver/contracts.md` — Separate Admin vs IAM credential option maps and redaction rules ([../../../contracts.md](../../../contracts.md)).
- `.agents/bundles/specs/native-postgres-driver/research/cloud-material_20261105.md` — Refresh lead (`240s`), effective expiry, and loopback override rules [^cloud-research].

## Symbol & Signature Contracts

- `src/rs/vertebra-postgres/src/lib.rs`:
  - Export `pub mod cloud;` on the Rust `rlib` seam (zero Python `#[pymodule]` registrations).
- `src/rs/vertebra-postgres/src/cloud/mod.rs` and `src/rs/vertebra-postgres/src/cloud/material.rs`:
  - `pub const MATERIAL_REFRESH_LEAD_MS: u64 = 240_000;` (`240s` before `effective_expiry_ms`).
  - `pub const MAX_ADMIN_RESPONSE_BYTES: usize = 1_048_576;` (`1 MiB` hard cap on streamed Admin HTTP bodies).
  - `#[derive(Clone, Debug, PartialEq, Eq, Hash)] pub enum CloudResourceTarget { CloudSql(CloudSqlInstanceUri), AlloyDb(AlloyDbClusterUri) }`
  - `#[derive(Clone)] pub struct CloudConnectorConfig`:
    - Owns `target: CloudResourceTarget`, `ip_type: IpType`, `auth_type: AuthType`, copied `cloud_auth_options: HashMap<String, String>`, optional designated `iam_auth_options: Option<HashMap<String, String>>`, validated `admin_endpoint: String` (default `https://sqladmin.googleapis.com` for Cloud SQL and `https://alloydb.googleapis.com` for AlloyDB; HTTPS mandatory except explicit loopback `http://127.0.0.1`, `http://localhost`, or `http://[::1]` overrides for local mock fixtures), and `refresh_lead_ms: u64` (`240_000`).
    - Redacted `Debug` implementation concealing option map values.
  - `#[derive(Clone)] pub struct PreparedCloudMaterial`:
    - Owns `endpoint_target: EndpointTarget`, `peer_dns_name: Option<String>`, `expected_cn: Option<String>`, `server_ca_pem: Vec<u8>`, `client_cert_chain_pem: Vec<u8>`, `client_key_pem: Vec<u8>` (redacted in `Debug`), `signed_cert_not_after_ms: u64`, `effective_expiry_ms: u64` (`min(signed_cert_not_after_ms, cert_bound_token_expiry_ms)` — never a duration hint alone), and optional `login_token: Option<LoginTokenSnapshot>` (`token` redacted in `Debug`, `expiry_ms: Option<u64>`, `generation: u64`).
  - `pub struct CloudMaterialCache`:
    - Keyed by `(credential_identity_fingerprint, target_uri, ip_type, admin_endpoint, auth_type)` so distinct Admin/IAM credential maps or scopes never collide.
    - Uses one async singleflight `tokio::sync::Mutex` per cache key, injected `Arc<dyn Clock>` (`SystemClock` in production), and reuses an entry strictly when `now_ms.saturating_add(config.refresh_lead_ms) < entry.effective_expiry_ms`.
  - `pub struct CloudConnector`:
    - Owns one internal `reqwest::Client` (no internal retry middleware that escapes `absolute_deadline`), `CloudMaterialCache`, and scope-keyed native `GcpTokenCache` builders constructed via `GcpTokenCache::from_options` inside `vertebra-postgres` (never extracting PyO3 `Arc`/`PyRef` across `cdylib`s; tests in `ch04` use explicit local/static tokens while `ch05:t03` upgrades automatic ADC source scope propagation before live sessions in `ch05:t04` / `ch06:t01`).
    - Exposes bounded `get_json` / `post_json` helpers reading chunked bodies up to `MAX_ADMIN_RESPONSE_BYTES` under `tokio::time::timeout_at(absolute_deadline, ...)`, percent-encoding path segments (`encode_uri_segment`), and redacting bearer tokens and private keys from all errors.
    - Acts as the **sole** module (`cloud/material.rs`) that resolves Admin/login token snapshots and constructs `EndpointTarget::{CloudSql, AlloyDb}` and `PreparedCloudMaterial` (`effective_expiry_ms: u64`), consuming raw discovery records (`CloudSqlDiscovery` / `AlloyDbDiscovery`) from `cloudsql.rs` (`ch04:t03`) and `alloydb.rs` (`ch04:t04`) so `ch05:t01` (`CloudPeerIdentity` on `EndpointTarget`) and `ch05:t03` (`ScopedTokenProvider`) never need to edit `cloudsql.rs` or `alloydb.rs`.
    - `pub async fn prepare(&self, config: &CloudConnectorConfig, absolute_deadline: tokio::time::Instant) -> Result<PreparedCloudMaterial, PostgresError>` (with a trait/closure fetcher hook exercised by mock HTTP servers in `ch04:t01` and wired to `cloudsql::fetch_cloudsql_discovery` / `alloydb::fetch_alloydb_discovery` in `ch04:t03`–`t04`).

## Edge Cases & Error Semantics

- Concurrent callers hitting an empty or inside-lead (`now_ms + 240_000 >= effective_expiry_ms`) cache entry execute exactly `1` refresh request; if that refresh fails or times out, all waiters receive the error and the cache never falls back to returning expired or inside-lead material.
- Cancelling or timing out the leading caller mid-refresh releases the per-key async lock so an immediate subsequent `prepare` call acquires the lock and succeeds cleanly without deadlocking.
- Truncated HTTP bodies, HTTP `4xx`/`5xx` responses, or responses exceeding `1_048_576` bytes fail before deserialization and never publish partial state into `CloudMaterialCache`.
- Reject non-HTTPS `admin_endpoint` URLs for non-loopback hosts (e.g. `http://sqladmin.googleapis.com` or `http://10.0.0.5:8080`) with `PostgresError::Config`.
- Existing open sockets created from an earlier `PreparedCloudMaterial` snapshot remain unaffected when the cached entry later crosses `effective_expiry_ms`, whereas a new `prepare` call fetches fresh material.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/rs/vertebra-postgres/tests/postgres_cloud_wire_tests.rs` (test names prefixed `material_`) using an injected manual clock and loopback mock HTTP server.
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| `material_singleflight_and_reuse` | `8` concurrent `prepare` calls at `t = 1_000s` returning cert `signed_not_after = 4_600s`; follow-up call at `t = 4_359s` (`now + 240s == 4_599s < 4_600s`) | Mock HTTP server observes exactly `1` fetch across all `9` calls; all callers receive identical `PreparedCloudMaterial`. |
| `material_refresh_lead_and_effective_expiry` | Cached cert `signed_cert_not_after_ms = 4_600_000` bound to login token `expiry_ms = 2_000_000`: call at `t = 1_759s` vs `t = 1_760s` (`1_760 + 240 == 2_000`) | `effective_expiry_ms` is `2_000_000` (not `4_600_000` or a duration hint); `t = 1_759s` hits cache, `t = 1_760s` triggers single renewal (`2` total fetches). |
| `material_failed_refresh_never_serves_stale` | Clock advances past `effective_expiry_ms - 240_000`; renewal mock returns HTTP `503` or malformed JSON | `prepare` returns `Err(PostgresError::Transport(_))` and never returns the previously cached material; existing already-cloned snapshot remains usable by an active mock socket while reconnect requests fresh material. |
| `material_cancellation_releases_lock` | Task A starts `prepare` against stalled mock HTTP server and is aborted / hits `50ms` `absolute_deadline`; Task B calls `prepare` after mock unblocks | Task A fails at `absolute_deadline` (`<= 100ms`); Task B immediately acquires the per-key lock and succeeds without hanging. |
| `material_oversized_and_partial_response_rejected` | Mock HTTP server streams `1_048_577` bytes (`1 MiB + 1B`) or drops socket mid-JSON body | Fails with `PostgresError::Transport` / `Protocol` without buffering unbounded memory or populating cache. |
| `material_endpoint_https_and_redaction` | `admin_endpoint = "http://example.com"` vs `"http://127.0.0.1:<port>"`; error triggered while configured with bearer `"vtb-secret-admin-token"` | Non-loopback `http://` fails `PostgresError::Config`; loopback `http://` succeeds; `{:?}` and error strings omit `"vtb-secret-admin-token"`. |

## Verification

Write the `material_*` mock HTTP and clock tests in `src/rs/vertebra-postgres/tests/postgres_cloud_wire_tests.rs` first (confirming Red), implement `cloud/mod.rs` and `cloud/material.rs`, and verify exit `0`:

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_cloud_wire_tests material_
cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings
```

Commit as `feat(postgres): cache expiring cloud connection material`.

## Acceptance Criteria

- [ ] `CloudMaterialCache` enforces singleflight refresh per `(credential, resource, ip_type, endpoint, auth_mode)` key with `now + 240s < min(signed_not_after, cert_bound_token_expiry)` reuse and clean lock release on cancellation (R5, R9).
- [ ] Bounded `reqwest` helpers enforce `absolute_deadline`, `1 MiB` response caps, URI segment percent-encoding, non-loopback HTTPS validation, and secret redaction (R5, R9).
- [ ] Credential caches are constructed inside `vertebra-postgres` from owned primitive option maps without cross-`cdylib` PyO3 extraction (R1, R5, R9).

## Notes & Discoveries

- [2026-11-07 05:00] `ch04:t01` wires `GcpTokenCache::from_options` for deterministic local/static token tests; `ch05:t03` introduces `ScopedTokenProvider` and repairs automatic ADC source scope propagation before `ch05:t04` and `ch06:t01` open live cloud database sessions.

[^cloud-research]: `.agents/bundles/specs/native-postgres-driver/research/cloud-material_20261105.md` — Cloud material singleflight, `240s` refresh lead, and bounded HTTP invariants.
[^core-gcp-auth]: `src/rs/vertebra-core/src/gcp_auth.rs::GcpTokenCache` — Native Rust credential cache constructor and `Clock` trait.
[^db-dialer]: `src/rs/vertebra-db/src/dialer.rs::CloudSqlInstanceUri` — Shared Cloud SQL and AlloyDB URI/network types.
[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Cloud credential map separation and redaction rules.
