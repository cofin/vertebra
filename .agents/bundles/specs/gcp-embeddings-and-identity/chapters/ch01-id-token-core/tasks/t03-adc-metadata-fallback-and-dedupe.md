---
type: Task
id: "gcp-embeddings-and-identity:ch01:t03"
chapter_id: ch01-id-token-core
title: End ambient ADC discovery with the metadata server and delete duplicated heuristics
description: Make GcpTokenCache::from_options resolve the metadata server after ambient ADC files and token variables, stop explicit credential options from falling through to ambient sources, and remove the use_metadata_server heuristics in GCSConfig and GCS signing.
status: draft
state: pending
priority: P1
verification_strategy: regression_tdd
depends_on:
  - "gcp-embeddings-and-identity:ch01:t02"
read_set:
  - src/rs/vertebra-core/src/gcp_auth.rs
  - src/rs/vertebra-core/tests/it/gcp_auth.rs
  - src/rs/vertebra-storage/src/signing.rs
  - src/py/vertebra/storage/_store.py
  - src/py/tests/unit/storage/test_gcs_token_refresh.py
  - .agents/bundles/specs/gcp-embeddings-and-identity/chapters/ch01-id-token-core/spec.md
files:
  - src/rs/vertebra-core/src/gcp_auth.rs
  - src/rs/vertebra-storage/src/signing.rs
  - src/py/vertebra/storage/_store.py
tests:
  - src/rs/vertebra-core/tests/it/gcp_auth.rs
  - src/py/tests/unit/storage/test_gcs_token_refresh.py
estimated_loc: 160
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
created_at: "2026-11-12T00:35:00Z"
updated_at: "2026-11-12T00:35:00Z"
commit: null
sources:
  - id: core-from-options
    resource: src/rs/vertebra-core/src/gcp_auth.rs::GcpTokenCache::from_options
    title: Option precedence, ambient ADC discovery, and impersonation base selection
  - id: storage-signing
    resource: src/rs/vertebra-storage/src/signing.rs
    title: Signing token-cache resolution with .or_else metadata retries
  - id: py-gcs-config
    resource: src/py/vertebra/storage/_store.py::GCSConfig.to_token_cache_options
    title: Python heuristic that injects use_metadata_server when no ADC file exists
tags:
  - gcp
  - adc
  - fail-loud
---

# Task t03: End Ambient ADC Discovery with the Metadata Server and Delete Duplicated Heuristics

## Objective & Scope Boundary

Change `src/rs/vertebra-core/src/gcp_auth.rs::GcpTokenCache::from_options` so that [^core-from-options]:

1. When any explicit credential key is present (`service_account_key`, `service_account_path`, `service_account`, `application_credentials_path`, `application_credentials`, `client_id`, `client_secret`, `refresh_token`, `access_token`, `bearer_token`, after `google_`/`gcs_` prefix normalization) and that credential does not produce a base source, `from_options` returns `None`. Ambient ADC files, ambient token variables, and the metadata server are not consulted. `target_service_account` without explicit credentials keeps today's behavior (ambient base, then metadata base).
2. When no explicit credential key, `use_metadata_server`, or `use_gcloud_cli` is set, ambient discovery is allowed, and neither the ambient ADC file nor `GOOGLE_OAUTH_ACCESS_TOKEN` / `CLOUDSDK_AUTH_ACCESS_TOKEN` resolves, the base source is `GcpAuthSource::MetadataServer { service_account: metadata_service_account or "default", endpoint: metadata_host or GCE_METADATA_HOST or "metadata.google.internal" (with http:// prefix), scopes: custom scopes or empty, quota_project_id }`.

Delete the now-redundant heuristics: the `.or_else(|| ... "use_metadata_server" ...)` retries in `src/rs/vertebra-storage/src/signing.rs` [^storage-signing] and the `use_metadata_server` injection block at the end of `src/py/vertebra/storage/_store.py::GCSConfig.to_token_cache_options` [^py-gcs-config]. Extract the duplicated metadata-endpoint construction in `from_options` into one private `GcpTokenCache::metadata_source(opts, quota_project_id, scopes)` associated function.

Out of scope: changing `from_options`'s `Option` return type, `from_scoped_options` messages, the embedder and Oracle call sites (`ch03`).

## Bounded Read Manifest

- `src/rs/vertebra-core/src/gcp_auth.rs::GcpTokenCache::from_options`, `::normalize_gcp_options`, `::discover_adc_file_path`, `::GcpTokenCache::from_scoped_options`.
- `src/rs/vertebra-core/tests/it/gcp_auth.rs` (loopback helpers, `metadata_server_and_impersonation_custom_scopes`).
- `src/rs/vertebra-storage/src/signing.rs` (token-cache resolution function containing both `use_metadata_server` fallbacks).
- `src/py/vertebra/storage/_store.py::GCSConfig.to_token_cache_options`, `::GCSConfig._resolve_adc_file` (still used by `GCSConfig` ADC path resolution; keep it).
- `src/py/tests/unit/storage/test_gcs_token_refresh.py::test_gcs_config_ambient_adc_precedence_and_isolation`.
- Chapter spec.

## Symbol & Signature Contracts

```rust
impl GcpTokenCache {
    pub fn from_options(raw_opts: &HashMap<String, String>, default_scopes: &[&str]) -> Option<Arc<Self>>; // unchanged signature
    fn metadata_source(
        opts: &HashMap<String, String>,
        quota_project_id: Option<String>,
        scopes: Vec<String>,
    ) -> GcpAuthSource;
}
const EXPLICIT_CREDENTIAL_KEYS: &[&str] = &[
    "service_account_key", "service_account_path", "service_account",
    "application_credentials_path", "application_credentials",
    "client_id", "client_secret", "refresh_token", "access_token", "bearer_token",
];
```

Resulting precedence inside `from_options`: inline/path service account -> explicit authorized-user triple -> explicit ADC path -> explicit access token -> `use_metadata_server` -> `use_gcloud_cli` -> (no explicit keys only) ambient ADC file -> ambient token variables -> metadata server (ambient allowed) -> impersonation wrapping when `target_service_account` is set.

## Edge Cases & Error Semantics

- `allow_ambient_adc = "false"` with no options still returns `None`; `vertebra._vertebra.SyncGcpTokenCache({}, allow_ambient_adc=False)` still raises `ValueError("unable to configure GcpTokenCache from options")`.
- A partial authorized-user triple (`client_id` only) counts as explicit and returns `None`.
- `metadata_host` and `metadata_service_account` options apply to the ambient metadata step exactly as they do to `use_metadata_server`.
- The ambient metadata step makes no network call at construction; the first `get_or_refresh` reports `metadata server request failed: ...` off Google Cloud.
- `GCSConfig.to_token_cache_options` output for a config without explicit credentials no longer contains `use_metadata_server`; resolution reaches the metadata server through core order with identical runtime behavior.

## Public-Seam Test Blueprint

- **Canonical test files**: `src/rs/vertebra-core/tests/it/gcp_auth.rs` (tests prefixed `adc_`; environment-mutating tests use a process-wide `static ENV_LOCK: std::sync::Mutex<()>` and restore `HOME`, `CLOUDSDK_CONFIG`, `GOOGLE_APPLICATION_CREDENTIALS`, `GCE_METADATA_HOST`, and token variables) and `src/py/tests/unit/storage/test_gcs_token_refresh.py` (existing `_MockTokenServer`, `monkeypatch`).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| `adc_ambient_ends_with_metadata_server` | Empty options; `HOME` and `CLOUDSDK_CONFIG` pointed at an empty temp dir; `GOOGLE_APPLICATION_CREDENTIALS`, token variables unset; `GCE_METADATA_HOST=127.0.0.1:<p>` serving `{"access_token":"meta-ambient","expires_in":3600}` | `from_options` returns `Some`; `auth_source()` is `MetadataServer`; `get_or_refresh` yields `meta-ambient` |
| `adc_explicit_missing_key_never_falls_through` | `{"service_account_path": "/nonexistent/key.json"}` with a valid ambient `authorized_user` ADC file in `CLOUDSDK_CONFIG` | `from_options` returns `None` |
| `adc_partial_authorized_user_is_explicit` | `{"client_id": "cid"}` with ambient ADC present | `None` |
| `adc_ambient_disabled_returns_none` | `{"allow_ambient_adc": "false"}` with `GCE_METADATA_HOST` set | `None` |
| `test_gcs_config_ambient_adc_precedence_and_isolation` (updated) | Existing ADC-file scenario plus a second config with no ADC file and `GCE_METADATA_HOST` at the mock server | ADC-file path unchanged (`oauth_calls == 1`, `metadata_calls == 0`); no-ADC path yields `mock-oauth-token-meta-1`; `"use_metadata_server" not in cfg.to_token_cache_options()` |

## Verification

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-core --test it adc_
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-core --test it gcp_auth::
cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-core -p vertebra-storage --all-targets -- -D warnings
make dev
uv run pytest src/py/tests/unit/storage/test_gcs_token_refresh.py src/py/tests/unit/storage/test_signing.py -q
uv run ruff check src/py/vertebra/storage/_store.py src/py/tests/unit/storage/test_gcs_token_refresh.py
uv run mypy src/py/vertebra/storage/_store.py
```

Write `adc_*` tests first (Red: ambient case returns `None`, explicit-missing case returns the ambient source), implement, re-run all commands. Commit as `fix(core): end ambient ADC discovery at the metadata server`.

## Acceptance Criteria

- [ ] Ambient discovery ends with the metadata server when allowed (R4).
- [ ] Explicit credential keys that fail to load never fall through to ambient sources (R4).
- [ ] `signing.rs` and `GCSConfig.to_token_cache_options` no longer inject `use_metadata_server` (R4).
- [ ] Existing `gcp_auth` and storage token tests pass unchanged except the updated precedence test.

## Notes & Discoveries

- [2026-11-12 00:35] Before this task, `from_options` falls through from an unreadable explicit `service_account_path` to the ambient ADC file, which silently changes identity; the explicit-key rule closes that path.

[^core-from-options]: `src/rs/vertebra-core/src/gcp_auth.rs::GcpTokenCache::from_options` — Precedence being changed.
[^storage-signing]: `src/rs/vertebra-storage/src/signing.rs` — Metadata retries deleted here.
[^py-gcs-config]: `src/py/vertebra/storage/_store.py::GCSConfig.to_token_cache_options` — Heuristic deleted here.
