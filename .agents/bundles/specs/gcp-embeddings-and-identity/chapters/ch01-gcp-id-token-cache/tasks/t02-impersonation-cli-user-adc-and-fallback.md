---
type: Task
id: "gcp-embeddings-and-identity:ch01:t02"
flow_id: gcp-embeddings-and-identity
chapter_id: ch01-gcp-id-token-cache
title: "Add Impersonation, CLI, and User ADC ID Tokens and Finalize ADC Fallback"
description: "Support IAM generateIdToken impersonation, gcloud print-identity-token, and authorized_user refresh-token ID minting, reject static tokens, and end ambient ADC discovery with MetadataServer."
status: stable
state: completed
commit: a601dee4
last_verified_commit: a601dee4
last_verified_at: "2026-11-13T04:49:30Z"
verification_evidence: "cargo test gcp_auth:: (13 passed) + clippy + stubtest + pytest (19 passed, 2 skipped)"
claimed_by: null
claimed_at: null
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "gcp-embeddings-and-identity:ch01:t01"
files:
  - src/rs/vertebra-core/src/gcp_identity.rs
  - src/rs/vertebra-core/src/gcp_auth.rs
  - src/rs/vertebra-storage/src/signing.rs
tests:
  - src/rs/vertebra-core/tests/it/gcp_auth.rs
read_set:
  - src/rs/vertebra-core/src/gcp_identity.rs
  - src/rs/vertebra-core/src/gcp_auth.rs
  - src/rs/vertebra-storage/src/signing.rs
  - src/rs/vertebra-core/tests/it/gcp_auth.rs
estimated_loc: 245
tags:
  - gcp
  - identity
  - oidc
  - adc
sources:
  - id: core-auth-source
    resource: src/rs/vertebra-core/src/gcp_auth.rs::GcpAuthSource
    title: "ImpersonatedServiceAccount, GcloudCli, AuthorizedUser, Static, and from_options precedence"
  - id: storage-signing
    resource: src/rs/vertebra-storage/src/signing.rs
    title: "Duplicate metadata-server token-cache fallback in GCS signing"
---

# Task `t02`: Add Impersonation, CLI, and User ADC ID Tokens and Finalize ADC Fallback

## Objective & Scope Boundary

Complete all remaining `GcpAuthSource` arms in `gcp_identity.rs` — including `AuthorizedUser` ADC refresh-token ID minting without ever rejecting user ADC at construction — and finalize canonical ambient ADC discovery in `gcp_auth.rs` + `signing.rs` per [`../spec.md` §2](../spec.md) [^core-auth-source] [^storage-signing]. Out of scope: PyO3 bindings (`t03`–`t04`).

## Bounded Read Manifest

- `src/rs/vertebra-core/src/gcp_identity.rs` — `GcpIdTokenCache` source dispatch from `t01`.
- `src/rs/vertebra-core/src/gcp_auth.rs` — `GcpAuthSource` variants and `GcpTokenCache::from_options` [^core-auth-source].
- `src/rs/vertebra-storage/src/signing.rs` — GCS signing `GcpTokenCache` construction [^storage-signing].
- `src/rs/vertebra-core/tests/it/gcp_auth.rs` — loopback IAM/OAuth servers and CLI stub helpers.

## Contracts & Edge Cases

- `ImpersonatedServiceAccount`: acquire base access token via `fetch_source_access_token(base)` (including `AuthorizedUser` base) and `POST {iam_endpoint}/v1/projects/-/serviceAccounts/{sa}:generateIdToken` with `{"audience": audience, "includeEmail": options.include_email}`, reading response `"token"` for any audience.
- `GcloudCli`: execute `gcloud auth print-identity-token --audiences=<audience> --quiet` with timeout enforcement.
- `AuthorizedUser`: accept at construction; when `audience == client_id`, post `grant_type=refresh_token` (`client_id`, `client_secret`, `refresh_token`, omitting `scope`) to `token_uri` and extract `id_token` (returning `Err` containing `contained no id_token` and `gcloud auth application-default login` if missing, with zero CLI fallback). When `audience != client_id`, fail before network I/O with exact guidance naming `<client_id> (the ADC client_id)`, `target_service_account`, and `IAP programmaticClients`.
- `Static`: fail at `GcpIdTokenCache::new` / `from_options` with `static access tokens cannot mint ID tokens`.
- `GcpTokenCache::from_options`: end ambient discovery (`allow_ambient_adc != false` and zero explicit credential options) with `GcpAuthSource::MetadataServer`; explicit invalid options (`service_account_path` missing, malformed JSON) return `None` without ambient fallthrough. Remove duplicate `use_metadata_server` retry from `vertebra-storage/src/signing.rs`.

## Test Matrix

- Extend `src/rs/vertebra-core/tests/it/gcp_auth.rs`: impersonated `generateIdToken` across multiple audiences; `authorized_user` ADC succeeding on `audience == client_id`, failing pre-network on Cloud Run URL with `target_service_account` + `programmaticClients` remediation message, failing loud when refresh response omits `id_token`, and succeeding for any audience when `target_service_account` is set; `gcloud` CLI success + timeout; `Static` rejection at construction; explicit bad `service_account_path` returning `None` despite ambient ADC; ambient fallback ending at `MetadataServer` unless `allow_ambient_adc=false`.

## Verification

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-core --test it gcp_auth:: && cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-core -p vertebra-storage --features vertebra-core/gcp-auth --all-targets -- -D warnings
```

## Acceptance Criteria

- [x] Impersonation, `gcloud`, and `authorized_user` ADC (`client_id` audience) mint valid cached ID tokens, static sources fail at construction, and ambient ADC ends cleanly at `MetadataServer` (`R1`, `R4`).
- [x] Zero regressions in `vertebra-core` and `vertebra-storage` Clippy/tests.

[^core-auth-source]: `src/rs/vertebra-core/src/gcp_auth.rs::GcpAuthSource` — `ImpersonatedServiceAccount`, `GcloudCli`, `AuthorizedUser`, `Static`, and `from_options` precedence.
[^storage-signing]: `src/rs/vertebra-storage/src/signing.rs` — Duplicate metadata-server token-cache fallback in GCS signing.
