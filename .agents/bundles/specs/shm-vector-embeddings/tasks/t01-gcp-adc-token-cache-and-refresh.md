---
type: Task
task_id: t01-gcp-adc-token-cache-and-refresh
flow_id: shm-vector-embeddings
title: "Active GCP ADC Credential Caching, Proactive Refresh & Per-Session Generation Rotation"
description: "Implement RefreshingGcpCredentialProvider and GcpTokenCache in vertebra-storage and vertebra.storage with 300s proactive refresh skew, single-flight lock deduplication, and per-session generation tracking across ObjectStoreEngine and DuckDBDriver."
scope: task
status: draft
state: completed
strategy: tdd
depends_on: []
traces_to:
  - REQ-01
  - NFR-02
tags:
  - storage
  - gcp
  - adc
  - duckdb
  - credentials
generated:
  by: flow/0.26.0
  at: 2026-11-01T18:32:00Z
verified:
  - by: agent:plan-generator
    at: 2026-11-01T18:32:00Z
    commit: d3d5f8b
sources:
  - id: storage-engine
    resource: src/rs/vertebra-storage/src/store.rs::ObjectStoreEngine
    title: Multi-cloud Rust ObjectStoreEngine with native GCS, S3, Azure, OCI, R2, and HTTP backends
  - id: gcs-config
    resource: src/py/vertebra/storage/_store.py::GCSConfig
    title: Zero-config Google Cloud Storage ADC and metadata-server credential resolution
  - id: duckdb-engine
    resource: src/rs/vertebra-db/src/duckdb_engine.rs::DuckdbConfig
    title: DuckDB native engine configuration and Arrow SHM execution in vertebra-db
  - id: py-duckdb
    resource: src/py/vertebra/db/duckdb.py::SyncDuckDBDriver
    title: Python SyncDuckDBDriver, AsyncDuckDBDriver, DuckDBConfig, and DuckDBSecretConfig
---

# Task `t01`: Active GCP ADC Credential Caching, Proactive Refresh & Per-Session Generation Rotation

<!-- truth: start -->
- **Proactive Refresh (`300s` Skew) & Single-Flight Lock**: `GcpTokenCache` caches `(access_token, expires_at_unix_ms, generation: u64, quota_project_id)` and refreshes tokens when `now_ms + 300_000 >= expires_at_unix_ms` (or on forced invalidation after HTTP `401`) under a single-flight lock so concurrent workers and `3.14t`/`3.15t` threads never stampede token endpoints.
- **Full GCP ADC Flow Coverage**: `RefreshingGcpCredentialProvider` in `vertebra-storage`[^storage-engine] and `GCSConfig` in `vertebra.storage`[^gcs-config] support GCE/Cloud Run metadata server, local `authorized_user` (`refresh_token` + `x-goog-user-project`), `service_account` JSON keys, and keyless `target_service_account` IAM `:generateAccessToken` impersonation without falling back to `StaticCredentialProvider` for expiring credentials.
- **Monotonic `generation: u64` Session Sync**: `DuckdbConfig`[^duckdb-engine] and `SyncDuckDBDriver` / `AsyncDuckDBDriver`[^py-duckdb] track `applied_token_generation: u64` per connection against `GcpTokenCache.generation()`, executing `CREATE OR REPLACE SECRET ... (TYPE gcs, BEARER_TOKEN ?)` with `SET httpfs_connection_caching = true; SET enable_http_metadata_cache = true;` whenever the generation advances.
<!-- truth: end -->

---

## 1. Objective & Scope

### 1.1 Goal

Replace one-shot static OAuth2 access token injection in [`src/rs/vertebra-storage/src/store.rs`](../../../../../src/rs/vertebra-storage/src/store.rs)[^storage-engine] and [`src/py/vertebra/storage/_store.py`](../../../../../src/py/vertebra/storage/_store.py)[^gcs-config] with an active, TTL-aware `GcpTokenCache` and `RefreshingGcpCredentialProvider` (`object_store::CredentialProvider<Credential = GcpCredential>`), and wire per-session `applied_token_generation: u64` tracking into [`src/py/vertebra/db/duckdb.py`](../../../../../src/py/vertebra/db/duckdb.py)[^py-duckdb] (`DuckDBConfig`, `DuckDBSecretConfig`, `SyncDuckDBDriver`, `AsyncDuckDBDriver`) so `gs://` bucket operations and Vertex AI / AlloyDB connections never fail with `401 Unauthorized` after 60 minutes.

### 1.2 In-Scope Files

- [`src/rs/vertebra-storage/src/store.rs`](../../../../../src/rs/vertebra-storage/src/store.rs) (and new module `src/rs/vertebra-storage/src/gcp_auth.rs` registered in [`src/rs/vertebra-storage/src/lib.rs`](../../../../../src/rs/vertebra-storage/src/lib.rs))
- [`src/py/vertebra/_storage.pyi`](../../../../../src/py/vertebra/_storage.pyi)
- [`src/py/vertebra/storage/_store.py`](../../../../../src/py/vertebra/storage/_store.py)
- [`src/py/vertebra/storage/__init__.py`](../../../../../src/py/vertebra/storage/__init__.py)
- [`src/py/vertebra/db/duckdb.py`](../../../../../src/py/vertebra/db/duckdb.py)
- `src/py/tests/unit/storage/test_gcs_token_refresh.py`

---

## 2. Public Seam & Implementation Specification

### 2.1 Rust `GcpTokenCache` & `RefreshingGcpCredentialProvider` (`src/rs/vertebra-storage/src/gcp_auth.rs`)

1. **`GcpTokenSnapshot` & `GcpTokenCache`**:
   - Define `GcpTokenSnapshot { pub access_token: String, pub expires_at_unix_ms: u64, pub generation: u64, pub quota_project_id: Option<String> }`.
   - Define `GcpAuthSource`:
     - `Static { access_token: String, expires_at_unix_ms: Option<u64>, quota_project_id: Option<String> }`
     - `AuthorizedUser { client_id: String, client_secret: String, refresh_token: String, quota_project_id: Option<String>, token_uri: String }`
     - `MetadataServer { service_account: String, quota_project_id: Option<String>, endpoint: String }`
     - `ImpersonatedServiceAccount { base: Box<GcpAuthSource>, target_service_account: String, delegates: Vec<String>, scopes: Vec<String>, lifetime_secs: u32 }`
   - Implement `GcpTokenCache`:
     - Holds `RwLock<Option<Arc<GcpTokenSnapshot>>>`, `tokio::sync::Mutex<()>` single-flight refresh lock, `AtomicU64` monotonic `generation`, and configurable `refresh_skew_ms: u64` (default `300_000` ms = `300s`).
     - `pub fn needs_refresh(&self, now_unix_ms: u64) -> bool`: returns `true` when no snapshot exists or `now_unix_ms.saturating_add(self.refresh_skew_ms) >= snapshot.expires_at_unix_ms`.
     - `pub async fn get_or_refresh(&self) -> Result<Arc<GcpTokenSnapshot>, String>`: fast-path read lock when `!needs_refresh(now)`; slow-path acquires the single-flight `tokio::sync::Mutex`, re-checks `needs_refresh(now)` (so waiting tasks reuse the freshly fetched token without issuing duplicate HTTP calls), fetches the new token + `expires_in` / RFC 3339 `expireTime`, increments `generation` via `fetch_add(1, Ordering::AcqRel) + 1`, stores the new `Arc<GcpTokenSnapshot>`, and returns it.
     - `pub fn invalidate(&self, failed_generation: u64)`: clears or marks expired only if the current snapshot's `generation == failed_generation` (preventing a stampede when multiple callers report `401` for the same expired generation).
2. **`RefreshingGcpCredentialProvider`**:
   - Implements `object_store::CredentialProvider` (`type Credential = object_store::gcp::GcpCredential`).
   - In `get_credential(&self) -> object_store::Result<Arc<GcpCredential>>`, awaits `self.cache.get_or_refresh()` and returns `Arc::new(GcpCredential { bearer: snapshot.access_token.clone() })`.
   - Wire `ObjectStoreEngine::build_gcs` in [`src/rs/vertebra-storage/src/store.rs`](../../../../../src/rs/vertebra-storage/src/store.rs)[^storage-engine] so that when `refresh_token` + `client_id` + `client_secret`, `target_service_account`, `use_metadata_server`, or a refreshable `GcpTokenCache` is configured, `builder.with_credentials(Arc::new(RefreshingGcpCredentialProvider::new(cache)))` is installed instead of `StaticCredentialProvider`.

### 2.2 Python `GcpTokenCache` & `GCSConfig` (`src/py/vertebra/storage/_store.py`)

```python
class GcpTokenSnapshot(msgspec.Struct, frozen=True, gc=False):
    """Immutable GCP OAuth2 access token snapshot with monotonic generation."""

    access_token: str
    expires_at_unix_ms: int
    generation: int
    quota_project_id: str | None = None
```

- Expose `GcpTokenCache` in `vertebra.storage` (backed by the Rust `GcpTokenCache` or synchronized with it) with:
  - `get_token(*, force_refresh: bool = False) -> GcpTokenSnapshot`
  - `get_token_async(*, force_refresh: bool = False) -> Awaitable[GcpTokenSnapshot]`
  - `generation: int` property
  - `needs_refresh(now_unix_ms: int | None = None) -> bool`
- Update `GCSConfig` in [`src/py/vertebra/storage/_store.py`](../../../../../src/py/vertebra/storage/_store.py)[^gcs-config]:
  - Add `refresh_skew_seconds: int = 300` field.
  - Ensure `GCSConfig.resolve_adc_token()` returns or populates `GcpTokenSnapshot` (parsing both integer `expires_in` from OAuth2/Metadata responses and ISO-8601 `expireTime` from IAM `:generateAccessToken` responses).
  - Ensure `GCSConfig.to_url_and_options()` passes structured refresh configuration (`refresh_token`, `client_id`, `client_secret`, `quota_project_id`, `target_service_account`, `refresh_skew_ms`) to `ObjectStoreEngine` so long-lived `ObjectStore("gs://...")` instances refresh automatically in Rust without Python intervention.

### 2.3 Per-Session `applied_token_generation` in `SyncDuckDBDriver` & `AsyncDuckDBDriver` (`src/py/vertebra/db/duckdb.py`)

- Add `gcs_token_cache: GcpTokenCache | None = None` support to `DuckDBConfig`, `DuckDBSecretConfig`, `SyncDuckDBDriver`, and `AsyncDuckDBDriver`:
  - Each DuckDB connection wrapper tracks `_applied_token_generation: int = 0`.
  - Before executing a query when `gcs_token_cache` is configured, compare `snapshot = gcs_token_cache.get_token()` against `self._applied_token_generation`:
    - On first attachment (`_applied_token_generation == 0`), execute `SET httpfs_connection_caching = true; SET enable_http_metadata_cache = true;`.
    - Whenever `snapshot.generation != self._applied_token_generation`, execute `CREATE OR REPLACE SECRET vtb_gcs_adc (TYPE gcs, BEARER_TOKEN ?)` with `snapshot.access_token` and update `self._applied_token_generation = snapshot.generation`.

---

## 3. TDD Verification Plan

### 3.1 Red-Green-Refactor Sequence

1. **Red**:
   - Write Rust unit tests in `src/rs/vertebra-storage/src/gcp_auth.rs` using a local mock HTTP token server (`tokio::net::TcpListener`) verifying:
     - Token caching returns identical `generation == 1` when `now + 300s < expires_at`.
     - Token within the `300s` skew window (`expires_in = 240`) triggers proactive refresh and increments `generation` to `2`.
     - `16` concurrent `get_or_refresh()` tasks during expiry issue **exactly `1`** upstream HTTP refresh request (single-flight deduplication).
     - `ImpersonatedServiceAccount` exchanges base credentials via `:generateAccessToken`, parses RFC 3339 `expireTime`, and refreshes on skew.
     - `invalidate(1)` forces refresh once and ignores duplicate `invalidate(1)` calls after generation advances to `2`.
   - Write Python unit tests in `src/py/tests/unit/storage/test_gcs_token_refresh.py` verifying:
     - `GCSConfig` + `GcpTokenCache` refreshes expired/skewed tokens across `authorized_user`, metadata server, and `target_service_account`.
     - Multi-session DuckDB `applied_token_generation` test: two pooled sessions (`session_a`, `session_b`) both rotate their `vtb_gcs_adc` secret when `GcpTokenCache` advances from generation `1` to `2` (proving `session_b` is never skipped after `session_a` refreshes the shared cache).
2. **Green**:
   - Implement `gcp_auth.rs`, wire `RefreshingGcpCredentialProvider` into `ObjectStoreEngine::build_gcs`, update `_storage.pyi` and `src/py/vertebra/storage/_store.py`, and add generation-tracked secret rotation in `src/py/vertebra/db/duckdb.py`.
3. **Refactor & Verify**:
   - Run `cargo test -p vertebra-storage`, `make dev`, `mypy.stubtest vertebra._storage`, and `pytest src/py/tests/unit/storage/test_gcs_token_refresh.py`.

### 3.2 Verification Commands

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-storage
make dev
.venv/bin/python -m mypy.stubtest vertebra._storage --allowlist tools/stubtest_allowlist.txt
.venv/bin/pytest src/py/tests/unit/storage/test_gcs_token_refresh.py -v
```

---

## 4. Task Notes

- Implemented `GcpTokenSnapshot`, `GcpAuthSource`, `GcpTokenCache`, and `RefreshingGcpCredentialProvider` in `src/rs/vertebra-storage/src/gcp_auth.rs` with 300s proactive refresh skew (`DEFAULT_REFRESH_SKEW_MS = 300_000`), `tokio::sync::Mutex` single-flight refresh deduplication, generation-checked invalidation, and support for `AuthorizedUser`, `MetadataServer`, `ImpersonatedServiceAccount`, and `Static` sources.
- Hardened `vertebra-storage` for non-blocking GIL execution and high-concurrency async Python workloads:
  - Refactored `spawn_rust_task` (`src/rs/vertebra-storage/src/lib.rs`) to pre-allocate `Py<TaskCompletionCallback>` on the caller thread and defer all Python result conversion (`PyBytes`, `StorageByteView`, dict/list materialization) into `TaskCompletionCallback.__call__` on the Python event loop thread, so Tokio worker threads never construct Python objects or copy buffers under `Python::attach`.
  - Scaled `STORAGE_RUNTIME` worker threads from `2` to `available_parallelism().clamp(4, 16)` (overridable via `VERTEBRA_STORAGE_IO_THREADS`) and wrapped `ObjectStore::new` in `py.detach(|| ...)`.
  - Exposed native PyO3 `GcpTokenCache` and `GcpTokenSnapshot` (`get_cached`, `py.detach` sync `get_token`, and `RustTaskAwaitable` `get_token_async`) in `vertebra._storage`, eliminating `asyncio.to_thread` pool contention for async token refresh.
  - Removed event-loop SHA-256 hashing and synchronous `Path.exists()` stat calls from `ObjectStore.put_async`/`put_file_async`, and added `presign_get_async`/`presign_put_async` backed by `py_sign_gcs_v4_iam_url_async`.
- Wired `GcpTokenCache::from_gcs_options(&opts)` and `RefreshingGcpCredentialProvider` into `ObjectStoreEngine` (`src/rs/vertebra-storage/src/store.rs`) so `ObjectStore("gs://...")` dynamically refreshes expired/skewed tokens.
- Added `GcpTokenSnapshot`, `GcpTokenCache`, and `GCSConfig.refresh_skew_seconds = 300` to `src/py/vertebra/storage/_store.py` and exported them in `src/py/vertebra/storage/__init__.py`.
- Added per-session `applied_token_generation: int` and `last_gcs_secret_sql: str` secret rotation in `SyncDuckDBDriver` and `AsyncDuckDBDriver` (`src/py/vertebra/db/duckdb.py`), with async pre-refresh on the Rust Tokio runtime before `asyncio.to_thread` query dispatch.
- Verified with `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-storage`, `pytest src/py/tests/unit/storage/test_gcs_token_refresh.py` (including 200-task high-concurrency async storage + token refresh test), `ruff`, `mypy`, and `stubtest`.

---

## Provenance Footnotes

[^storage-engine]: `src/rs/vertebra-storage/src/store.rs::ObjectStoreEngine` — Multi-cloud Rust `ObjectStoreEngine` with native GCS, S3, Azure, OCI, R2, and HTTP backends.
[^gcs-config]: `src/py/vertebra/storage/_store.py::GCSConfig` — Zero-config Google Cloud Storage ADC and metadata-server credential resolution.
[^duckdb-engine]: `src/rs/vertebra-db/src/duckdb_engine.rs::DuckdbConfig` — DuckDB native engine configuration and Arrow SHM execution in `vertebra-db`.
[^py-duckdb]: `src/py/vertebra/db/duckdb.py::SyncDuckDBDriver` — Python `SyncDuckDBDriver`, `AsyncDuckDBDriver`, `DuckDBConfig`, and `DuckDBSecretConfig`.
