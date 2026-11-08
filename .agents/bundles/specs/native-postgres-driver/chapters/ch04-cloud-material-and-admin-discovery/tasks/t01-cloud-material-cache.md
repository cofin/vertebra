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
  - src/rs/vertebra-postgres/src/cloud/material.rs
estimated_loc: 250
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
updated_at: "2026-11-08T01:06:11Z"
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

Create `cloud/mod.rs` and `cloud/material.rs` with clock injection, typed discovery/token inputs, per-key result-sharing refresh, credential identity isolation, and bounded authenticated JSON HTTP under one caller deadline. Export `pub mod cloud;` from `lib.rs`; add no Python registrations or constructor keywords. Real RSA issuance and concrete Admin discovery dispatch land in tasks 02–04. Automatic ADC scope propagation lands in `ch05:t03`, before cloud sessions [^cloud-research] [^core-gcp-auth] [^db-dialer] [^pg-contracts].

## Bounded Read Manifest

Read exactly the six frontmatter `read_set` files. Owned production/test targets are read while editing; this creates the canonical `postgres_cloud_wire_tests.rs` target. Keep its mock HTTP server, manual clock, barriers, and response fixtures in that file; do not import/read `tests/support/mod.rs` or another undeclared fixture. Pinned API evidence and signatures below are self-contained in [cloud research](../../../research/cloud-material_20261105.md).

- `vertebra-core/src/gcp_auth.rs::{GcpAuthSource,GcpTokenCache,current_unix_ms}` supplies native token acquisition, `Option<Arc<GcpTokenCache>>` construction, and wall time; it contains no `Clock`/`SystemClock` trait.
- `vertebra-db/src/connection.rs::EndpointTarget` supplies transport targets.
- `vertebra-db/src/dialer.rs::{CloudSqlInstanceUri,AlloyDbClusterUri,IpType,AuthType}` supplies equality and public components, without `Hash` implementations.
- `vertebra-postgres/src/config.rs` supplies owned cloud option maps and resource/network/auth selection.
- `contracts.md` freezes actual static login expiry, credential separation, and secret handling.
- `cloud-material_20261105.md` supplies control-plane and pinned transport/concurrency contracts.

## Symbol & Signature Contracts

All following types live in owned `cloud/material.rs` and are re-exported by `cloud/mod.rs` for the native Rust collaborator seam. Secret-bearing types implement redacted `Debug` manually (prepared Debug omits the nested shared `EndpointTarget`, whose derived Debug includes keys/tokens); no Python API exposes collaborator injection.

```rust
pub trait Clock: Send + Sync { fn now_unix_ms(&self) -> u64; }
pub struct SystemClock;
impl Clock for SystemClock { /* delegate to vertebra_core::gcp_auth::current_unix_ms */ }
pub const MATERIAL_REFRESH_LEAD_MS: u64 = 240_000;
pub const MAX_ADMIN_RESPONSE_BYTES: usize = 1_048_576;
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CloudResourceTarget { CloudSql(CloudSqlInstanceUri), AlloyDb(AlloyDbClusterUri) }
pub struct LoginTokenSnapshot {
    pub token: String, pub expiry_ms: u64, pub generation: u64,
    pub quota_project_id: Option<String>, pub refresh_skew_ms: u64,
}
pub struct CloudTokenBundle {
    pub admin: Arc<GcpTokenSnapshot>, pub login: Option<LoginTokenSnapshot>,
}
pub struct CloudDiscovery {
    pub host: String, pub port: u16, pub peer_dns_name: Option<String>,
    pub expected_cn: Option<String>, pub server_ca_pem: Vec<u8>,
    pub client_cert_chain_pem: Vec<u8>, pub client_key_pem: Vec<u8>,
    pub signed_cert_not_after_ms: u64,
}
pub type DiscoveryFuture<'a> = Pin<Box<dyn Future<Output = Result<CloudDiscovery, PostgresError>> + Send + 'a>>;
pub type AddressFuture<'a> = Pin<Box<dyn Future<Output = Result<std::net::SocketAddr, PostgresError>> + Send + 'a>>;
pub trait CloudMaterialFetcher: Send + Sync {
    fn fetch<'a>(&'a self, connector: &'a CloudConnector,
        config: &'a CloudConnectorConfig, tokens: &'a CloudTokenBundle,
        absolute_deadline: tokio::time::Instant) -> DiscoveryFuture<'a>;
    fn resolve_address<'a>(&'a self, host: &'a str, port: u16,
        deadline: tokio::time::Instant) -> AddressFuture<'a>;
}
pub struct AdminDiscoveryFetcher;
impl CloudConnector {
    pub fn new(clock: Arc<dyn Clock>, fetcher: Arc<dyn CloudMaterialFetcher>) -> Result<Self, PostgresError>;
    pub fn now_unix_ms(&self) -> u64;
    pub async fn prepare(&self, config: &CloudConnectorConfig,
        absolute_deadline: tokio::time::Instant) -> Result<PreparedCloudMaterial, PostgresError>;
    pub async fn get_json<T: serde::de::DeserializeOwned>(&self, url: &str,
        admin: &GcpTokenSnapshot, deadline: tokio::time::Instant) -> Result<T, PostgresError>;
    pub async fn post_json<T: serde::de::DeserializeOwned, B: serde::Serialize + ?Sized>(
        &self, url: &str, body: &B, admin: &GcpTokenSnapshot,
        deadline: tokio::time::Instant) -> Result<T, PostgresError>;
}
pub fn encode_uri_segment(component: &str) -> String;
```

- `CloudConnectorConfig` derives `Clone`, owns `target`, `ip_type: IpType`, `auth_type: AuthType`, `cloud_auth_options: HashMap<String,String>`, `iam_auth_options: Option<HashMap<String,String>>`, `admin_endpoint: String`, and `refresh_lead_ms: u64` (`240_000`). Validate endpoints before credentials/network: absolute HTTPS URL, no userinfo/query/fragment; explicit HTTP overrides allow only `localhost`, `127.0.0.1`, or `[::1]` loopback hosts. Normalize endpoint trailing slashes once. Defaults are `https://sqladmin.googleapis.com` and `https://alloydb.googleapis.com`.
- `PreparedCloudMaterial` derives `Clone`, owns `endpoint_target: EndpointTarget`, `peer_dns_name`, `expected_cn`, all three PEM byte vectors, `signed_cert_not_after_ms: u64`, `effective_expiry_ms: u64`, and `login_token: Option<LoginTokenSnapshot>`; `LoginTokenSnapshot` also derives `Clone`. Its expiry is always an actual known lifetime, never optional/sentinel. `CloudDiscovery` is the production fetcher result; `material.rs` invokes `fetcher.resolve_address` under deadline and alone constructs `EndpointTarget::{CloudSql,AlloyDb}`, and constructs prepared material. Concrete Admin fetchers validate issued material in tasks 02–04; the injected Rust collaborator supports cache/transport testing without a test-only prepared-material setter.
- Implement `Hash for CloudResourceTarget` manually: hash `0u8, project, region, instance` for Cloud SQL and `1u8, project, location, cluster, instance` for AlloyDB, each component with `Hash::hash`, in that order. Derived `Eq` compares precisely those variant/components. Never hash `Debug` strings or edit `dialer.rs`. Private `CacheKey` derives `Eq,Hash` using target plus `ip_tag: u8` (`Public=0,Private=1,Psc=2`), `auth_tag: u8` (`Password=1,Iam=2`), normalized endpoint, and credential fingerprint. The fingerprint is SHA-256 over length-prefixed sorted map key/value pairs, a distinct `None`/`Some` IAM-map tag, and exact Admin/login scope lists. Neither plaintext keys nor fingerprints appear in diagnostics. Both credential maps, including actual expiry and quota/proxy options, participate; different credentials/scopes/resources never share flights or entries.
- `CloudConnector::new` is pure apart from building its production `reqwest::Client`: redirects disabled with `reqwest::redirect::Policy::none()`, no retry middleware, environment proxy behavior retained, an explicit loopback Admin endpoint selects a separate lazily built `.no_proxy()` client with the same redirect policy, avoiding environment proxies deterministically. Store the injected clock/fetcher, `CloudMaterialCache::new(clock.clone())` (`pub fn new(clock: Arc<dyn Clock>) -> Self`), and scope/identity-keyed token cache slots. Production uses `Arc::new(SystemClock)` plus `Arc::new(AdminDiscoveryFetcher)`; injected clocks apply to material and login guards.
- `AdminDiscoveryFetcher::resolve_address` uses deadline-guarded `tokio::net::lookup_host((host,port))`, rejects an empty answer with static `Transport`, and returns the first resolved address while preserving the original host/peer DNS separately. Its constructor adds no resolver map/override. Cache/HTTP test fetchers resolve explicit numeric addresses locally; Admin wire test wrappers forward `fetch` to `AdminDiscoveryFetcher` and override the same production resolver method with a deterministic in-file hostname-to-loopback fixture (including PSC), avoiding external DNS. `AdminDiscoveryFetcher::fetch` delegates through `CloudConnector` helpers, returning `CloudDiscovery` (never `EndpointTarget`/prepared material). Task 01 returns explicit `UnsupportedFeature("cloud discovery unavailable")` for both unfinished dispatch branches; task 03 implements Cloud SQL and task 04 AlloyDB. These staged branches provide no session fallback and disappear before chapter completion. Test fetchers use the same production trait, HTTP helpers and token bundle; they cannot override token resolution/expiry.

**Native token resolution and real lifetime:** `prepare` resolves tokens inside the refresh flight, before invoking the fetcher. Admin uses `cloud-platform`; login is required for Cloud SQL IAM (`sqlservice.login`) and AlloyDB in both modes (`alloydb.login`). Builders execute in `spawn_blocking` under the same deadline because `from_options` can discover ADC/read files; late results are discarded. Handle its actual `Option<Arc<GcpTokenCache>>` return (`None` -> sanitized `Config`), not `Result`. Poll native `get_or_refresh().await` directly under deadline; its boxed source future is `Send` and does not require an offload/runtime nesting wrapper.

For designated static login `iam_auth_options` containing `access_token`/`bearer_token`, the production resolver requires a nonempty token (equal aliases when both supplied), canonical unsigned ASCII decimal `expires_at_unix_ms` (`parsed.to_string() == raw`), explicit comma/ASCII-whitespace `scopes` containing every required scope, and `now_unix_ms().saturating_add(refresh_skew_ms) < supplied_expiry` (`refresh_skew_ms` default `300_000`, canonical decimal if supplied). Construct `Arc::new(GcpTokenCache::new(GcpAuthSource::Static { access_token, expires_at_unix_ms: Some(supplied_expiry), quota_project_id }, refresh_skew_ms))` directly; retain exactly that native snapshot expiry in `LoginTokenSnapshot`. Static login never calls `from_options`, which discards expiry and produces `u64::MAX / 2`. Reject missing/unknown expiry/scope with sanitized `Config`; chapter 05 centralizes this same validation in `StaticLoginCredential` and maps it to canonical `Auth` errors. No expiry side table or public constructor option is added. Admin-only opaque static tokens are allowed for Admin requests; their sentinel expiry never bounds login or cert material. If login is needed and only an opaque Admin token exists, reject rather than promote it to a login grant. Nonstatic login may derive from Admin source options with the requested login scope; automatic source-wide scope correctness is completed by `ch05:t03` and no cloud session opens beforehand. Check actual login lifetime again before token-bearing HTTP and before publication. Private `resolve_tokens(&self, config: &CloudConnectorConfig, deadline: Instant) -> impl Future<Output=Result<CloudTokenBundle,PostgresError>> + Send` owns this logic; `LoginTokenSnapshot::ensure_usable(&self, now_ms: u64) -> Result<(),PostgresError>` implements the exact strict inequality. Store `refresh_skew_ms` on the typed snapshot while `LoginTokenSnapshot.expiry_ms` remains exact. The production fetcher checks the same guard before Cloud SQL token-bearing issuance; the cache publication guard applies to both services.

**Target construction:** Cloud SQL uses `EndpointTarget::CloudSql { instance: uri.clone(), addr, server_ca_pem, client_cert_pem: discovery.client_cert_chain_pem, client_key_pem }`; AlloyDB uses `EndpointTarget::AlloyDb { cluster: uri.clone(), addr, server_ca_pem, client_cert_pem, client_key_pem, user_agent: "vertebra-postgres".into(), oauth2_token: login.token.clone(), auth_type }`. Clone PEM bytes only for the separate prepared fields. Preserve original `peer_dns_name` and `expected_cn` for chapter 05 verification policy. Neither path emits PostgreSQL startup or opens the data-plane socket in chapter 04.

**Per-key flight algorithm (Tokio 1.54.1):** `CloudMaterialCache` owns short table/slot bookkeeping and `Arc<tokio::sync::Mutex<MaterialSlot>>` per key. A slot contains cached `Option<Arc<PreparedCloudMaterial>>` and optional flight sender `Option<watch::Sender<FlightResult>>`, where `type FlightResult = Option<Result<Arc<PreparedCloudMaterial>,PostgresError>>;`. No map/slot/token-table mutex guard crosses network, token, DNS, keygen, watch wait, or body awaits.

1. Reject `Instant::now() >= deadline` before acquiring any lock, even on cache hit. Deadline-wrap async slot-lock acquisition. Return a cached clone only when clock `now.saturating_add(refresh_lead_ms) < effective_expiry_ms` and any login snapshot remains outside its own refresh skew; otherwise resolve a fresh flight (static unusability then fails closed). Recheck monotonic deadline immediately before return.
2. Under the slot lock, subscribe to an existing unfinished flight with `Sender::subscribe()`. Otherwise install `watch::channel(None)` and create a caller-owned `RefreshOwner` guard holding a cloned sender. A completed failed/cancelled flight is eligible for replacement by this later caller; followers retain their original receivers. Drop the slot lock before refreshing/waiting.
3. Followers clone the result from `borrow_and_update()` in a scope that drops the watch `Ref` before awaiting `changed()`. Loop until `Some(result)`; deadline-wrap the wait and recheck deadline before returning. Every caller subscribed before publication receives the same cloned success/error, including failed refresh, without starting its own retry. A follower timing out/cancelling only drops its receiver.
4. The owner runs native token resolution -> fetcher -> DNS/target construction -> expiry validation under its own original absolute deadline. A later follower cannot extend that budget. On successful completion, reacquire the slot under the same deadline, recheck monotonic time and `signed_not_after > wall_now`, login usability and effective expiry. Cache only complete usable success; send `Some(result.clone())` with `send_replace` and disarm owner. For any error, including failed completion-lock acquisition, call private `RefreshOwner::fail(error: PostgresError) -> PostgresError`: synchronously `send_replace(Some(Err(error.clone())))`, disarm the guard and return that same error, without acquiring a slot lock. Never serve old inside-lead material.
5. `RefreshOwner::drop` synchronously `send_replace(Some(Err(Transport("cloud material refresh cancelled"))))` when armed, requiring no async lock. Owner abort or timeout drops unfinished HTTP/body work, wakes every joined follower, and leaves a completed failed flight that the next caller replaces. An owner that times out during completion lock acquisition publishes the exact deadline failure via `fail` before returning; there is no detached refresh worker or forever-pending flight. Prior established socket snapshots remain owned and unchanged.

**HTTP contract:** `get_json`/`post_json` serialize requests with Admin bearer and optional `x-goog-user-project`, then use a single absolute deadline across send, headers, and every `Response::chunk().await`. Reject advertised `Content-Length > 1_048_576`; still count every actual chunk using checked length before extending, including missing/false lengths. Stop at `1_048_577` bytes without appending the excess, reject truncation and non-success status, and deserialize only a complete bounded body. Reject already-expired deadlines at helper entry and immediately before/after each await and before returning cached/decoded success: pinned Tokio `timeout_at` polls an immediately-ready future before checking its timer. Percent-encode every UTF8 path component with uppercase `%HH` for bytes outside RFC3986 unreserved `A-Z a-z 0-9 - . _ ~`; no whole-path/form encoding. Use static errors or sanitized status/category, never response bodies, raw URLs with secrets, reqwest source chains, bearer tokens, private keys or option values.

## Edge Cases & Error Semantics

- One failed refresh produces one error result for all joined callers, with a later independent request allowed to retry. No stale material is returned after a renewal failure.
- Owner abort fails joined followers; follower abort leaves owner and other followers running. Different keys progress concurrently while one HTTP request stalls.
- `effective_expiry_ms = min(signed_cert_not_after_ms, login.expiry_ms)` when login exists; password Cloud SQL uses signed certificate expiry alone. Reject expired certificates/tokens and saturating-clock boundaries before publication. API duration hints and static-cache sentinels establish no lifetime.
- Cap/truncation/HTTP failure/invalid JSON, credential builder failure, DNS failure, deadline expiry, or empty/unknown login lifetime publishes no cache entry. Login token is never the HTTP bearer and Admin token is never a login grant.

## Public-Seam Test Blueprint

Create `src/rs/vertebra-postgres/tests/postgres_cloud_wire_tests.rs`; `material_*` tests exercise public native `CloudConnector::new/prepare` plus production token resolution and HTTP helpers. Define `ManualClock(AtomicU64): Clock`, in-file loopback `TcpListener` HTTP framing with exact `Content-Length`/chunking, and semaphore/barrier-controlled fetchers returning typed `CloudDiscovery`. No `mod support`, hidden read expansion, sleeps, real credentials, environment ADC discovery, external network, or test-only cache setters. Set `allow_ambient_adc=false`, `proxy_policy=direct` in option maps. Use Tokio paused time and explicit entered/release barriers; a bounded independent real-time watchdog prevents fixture hangs.

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| `material_singleflight_and_reuse` | Eight joined `prepare` futures at `now=1_000_000`, cert expiry `4_600_000`; release HTTP only after all eight futures are locally polled pending; follow-up at `4_359_000` | One HTTP fetch for nine calls, identical generation/material; future bound `Send` checked by spawning the owning wrapper. |
| `material_refresh_lead_and_effective_expiry` | Designated production `iam_auth_options={access_token:"mock-login",expires_at_unix_ms:"2000000",scopes:"https://www.googleapis.com/auth/sqlservice.login",refresh_skew_ms:"0",allow_ambient_adc:"false",proxy_policy:"direct"}` and signed cert expiry `4_600_000`; calls at `1_759_000` and `1_760_000` | Native static snapshot and prepared expiry both exactly `2_000_000`; first reuses, second renews at strict 240s equality (two total fetches). Fixture chooses zero login skew explicitly so login remains usable at material lead; a separate default-skew case rejects expiry `now+300_000`. No sentinel is asserted as actual expiry. |
| `material_unknown_static_lifetime_rejected` | Missing expiry, `"02000000"`, overflow, missing/incorrect scope, unequal token aliases, or only opaque Admin token when IAM login required | Sanitized `Config`, zero fetcher/HTTP login uses, no cache entry; supplied secrets absent from errors/Debug. |
| `material_failed_refresh_fans_out` | Seed generation, advance into lead; poll eight futures pending behind one held renewal; respond once with 503, then repeat with malformed JSON | All eight receive the same error, HTTP count increments once per batch, no stale return. A later independent prepare succeeds with exactly one additional request. |
| `material_owner_and_follower_cancellation` | Hold owner HTTP; join two followers; abort one follower, then abort owner | Remaining follower receives cancellation error; peer sees dropped response connection, no detached refresh survives; next call succeeds. Aborting only follower leaves owner/remaining follower successful with one fetch. |
| `material_absolute_deadline_all_stages` | At paused time, hold token response, HTTP headers, body chunk, DNS resolver and follower result separately under `deadline=now+50ms`; advance to 49ms then 51ms; also already-expired deadline with ready cache/ready HTTP | Pending at 49ms; `Transport` by 51ms, no late success or cache publication; already-expired call refuses even ready success. Last valid snapshot is unaffected. |
| `material_cap_and_partial_body` | Exact `1_048_576` valid JSON body vs `1_048_577`, misleading/missing Content-Length, chunked overflow, truncated JSON/socket EOF | Exact cap accepted, excess/truncation rejected without cache publication or unbounded aggregate buffering. |
| `material_identity_isolation` | Vary one of URI components/variant, IP type, auth mode, Admin endpoint, Admin/IAM token/options/scopes; reorder map insertion | Distinct semantic keys have independent requests/flights; equivalent map order shares; equal parsed URI values hash equally; stalled key never blocks another key's fetch. |
| `material_endpoint_https_and_redaction` | Reject `http://example.com`, `http://10.0.0.5`, userinfo/query/fragment; allow explicit loopback; server echoes secret in 403 body | Config rejection before network, loopback works, returned error/Debug excludes bearer/private PEM/echoed body; redirects cannot forward credentials. |
| `material_uri_encoding` | `"google.com:proj"`, slash, space, percent and UTF8 component bytes | `%3A`, `%2F`, `%20`, `%25`, UTF8 `%HH` encoding; unreserved bytes unchanged. |

The only private-state regression is a colocated `cloud/material.rs::tests::material_slot_deadline` unit test: acquire its private slot mutex, poll `prepare` pending at virtual +49ms, advance to +51ms, assert `Transport` while the original guard is still held, then release it. This avoids exposing a public test-only lock hook; it is included in the frontmatter `tests` and focused verification.

## Verification

Confirm meaningful Red for the public cache/HTTP scenarios before implementation. Compile the `CloudMaterialFetcher` boxed future and owning `prepare` future as `Send`; source `TokenFetchFuture<'a>` already explicitly includes `Send`. Verify Green with:

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_cloud_wire_tests material_ --locked
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --lib material_slot_deadline --locked
cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets --locked -- -D warnings
```

Commit as `feat(postgres): cache expiring cloud connection material`.

## Acceptance Criteria

- [ ] Local `Clock`, component-consistent hashing, isolated cache identity and concrete `Send` fetcher lifetimes compile within the three owned production files (R5, R9).
- [ ] Joined callers share success/failure/cancellation, later calls retry completed failures, lock wait is deadline bounded, and no guard crosses network awaits (R5, R9).
- [ ] Real designated static login expiry survives native acquisition and bounds material; unknown lifetime/provenance fails before transmission, with no sentinel lifetime or Python constructor growth (R5, R9).
- [ ] HTTP helpers enforce the total deadline including ready-success checks and streamed 1 MiB cap, strict endpoint/encoding, proxy/quota policy and secret redaction (R5, R9).

## Notes & Discoveries

- The shared token source future is explicitly `Send`; static `from_options` returns `Option<Arc<_>>` with unknown-expiry sentinel, so designated login uses `GcpAuthSource::Static { expires_at_unix_ms: Some(actual), .. }` and independent injected wall-time guards. Chapter 05 centralizes strict credentials without changing this lifetime contract.

[^cloud-research]: `.agents/bundles/specs/native-postgres-driver/research/cloud-material_20261105.md` — Cloud lifecycle and pinned Tokio/reqwest API evidence.
[^core-gcp-auth]: `src/rs/vertebra-core/src/gcp_auth.rs::{GcpTokenCache,GcpAuthSource,TokenFetchFuture,current_unix_ms}` — Native token constructor, real static expiry and `Send` source future.
[^db-dialer]: `src/rs/vertebra-db/src/dialer.rs::{CloudSqlInstanceUri,AlloyDbClusterUri,IpType,AuthType}` — Shared equality and URI components.
[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Frozen constructor and actual login lifetime/provenance requirements.
