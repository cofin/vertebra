---
type: Task
id: "gcp-embeddings-and-identity:ch06:t01"
chapter_id: ch06-docs-okf-and-limitations
title: Document Google embedding transports, ADC order, and ID token caches
description: Update the embeddings usage guide, the storage authentication guide, and the vertebra.storage reference for the Vertex REST transport, Gemini multimodal parts, auto_truncate, RemoteEmbedder ID tokens, explicit Oracle rotation, ADC order, ID token caches, per-credential audience rules, IAP programmaticClients, and Cloud Run usage.
status: draft
state: pending
priority: P2
verification_strategy: documentation_validation
depends_on:
  - "gcp-embeddings-and-identity:ch05:t03"
read_set:
  - docs/usage/embeddings-and-vectors.md
  - docs/usage/storage-auth-and-signing.md
  - docs/reference/storage.md
  - src/py/vertebra/embeddings/_google.py
  - src/py/vertebra/_vertebra.pyi
  - .agents/bundles/specs/gcp-embeddings-and-identity/spec.md
files:
  - docs/usage/embeddings-and-vectors.md
  - docs/usage/storage-auth-and-signing.md
  - docs/reference/storage.md
tests: []
estimated_loc: 230
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
  - id: docs-embeddings
    resource: docs/usage/embeddings-and-vectors.md
    title: Embedder Selection Matrix and Google Cloud sections
  - id: docs-storage-auth
    resource: docs/usage/storage-auth-and-signing.md
    title: Google Cloud credentials and token caches section
  - id: docs-storage-ref
    resource: docs/reference/storage.md
    title: GCS token caches reference section
tags:
  - docs
---

# Task t01: Document Google Embedding Transports, ADC Order, and ID Token Caches

## Objective & Scope Boundary

Edit three existing pages in place [^docs-embeddings] [^docs-storage-auth] [^docs-storage-ref]:

1. `docs/usage/embeddings-and-vectors.md`
   - `Embedder Selection Matrix`: `VertexEmbedder` -> "Vertex AI REST `:predict` / `:embedContent` with ADC token caches"; `GeminiEmbedder` -> "Gemini API gRPC `BatchEmbedContents` (`x-goog-api-key` or ADC)"; `RemoteEmbedder` -> "Rust HTTP/2 gRPC `BatchEmbed` with optional ID tokens".
   - `Google Cloud & In-Database SQL Pushdown`: replace the packed-`f32` sentence with the REST routing rule (per-model method and instance limits), the project requirement, `max_concurrent_requests`, and `auto_truncate=False`; add a short `embed_multimodal` example for `GeminiEmbedder` (inline image + caption) and `VertexEmbedder("gemini-embedding-2")` (`gs://` video); add a `RemoteEmbedder(id_token_cache=AsyncGcpIdTokenCache())` example for Cloud Run; state that `EmbeddingError` covers HTTP, gRPC, row-count, and dimension failures; replace the Oracle rotation sentence with the explicit-cache rule and a one-line `token_cache=AsyncGcpTokenCache({}, ["https://www.googleapis.com/auth/cloud-platform"])` example.
2. `docs/usage/storage-auth-and-signing.md`, section `Google Cloud credentials and token caches`
   - A numbered ADC resolution list: explicit options, ambient ADC file, ambient access-token variables, metadata server; explicit credentials that fail to load raise instead of falling through; `allow_ambient_adc=False` disables ambient steps.
   - A new `### ID tokens for Cloud Run, Cloud Functions, and IAP` subsection with sync and async `get_token(audience)` examples and per-audience caching behavior (tokens refresh before the JWT `exp` minus `refresh_skew_ms`; `max_id_token_audiences`, `include_email`, `metadata_id_token_format`).
   - A credential table inside that subsection:

     | Credential | How the token is minted | Allowed audience |
     | --- | --- | --- |
     | Service-account key | Signed `target_audience` assertion at the key `token_uri` | Any |
     | Metadata server (GCE, GKE, Cloud Run, Cloud Functions) | `/computeMetadata/v1/instance/service-accounts/default/identity` | Any |
     | `target_service_account` impersonation | IAM Credentials `generateIdToken` with `includeEmail: true` | Any |
     | gcloud CLI (`use_gcloud_cli`) | `gcloud auth print-identity-token --audiences` | As gcloud permits |
     | `authorized_user` ADC (`gcloud auth application-default login`) | OAuth refresh-token grant at `https://oauth2.googleapis.com/token`, response `id_token` | Only the ADC `client_id` |

   - The audience-mismatch error text for `authorized_user` ADC and its two fixes: set `target_service_account` (the user needs `roles/iam.serviceAccountTokenCreator` on that account), or add the ADC `client_id` to the IAP `programmaticClients` list and pass the `client_id` as the audience.
   - A `#### IAP programmatic clients` paragraph: IAP accepts ID tokens whose `aud` is the IAP OAuth client ID or any client ID listed in the IAP settings `accessSettings.oauthSettings.programmaticClients`; adding the ADC `client_id` there lets developer `authorized_user` credentials call the IAP resource directly with `get_token(client_id)`.
   - A `#### Cloud Run` paragraph: the audience is the service URL (`https://<service>-<hash>-<region>.a.run.app`) or a configured custom audience; on Cloud Run, GCE, and GKE the metadata server mints it with no options; from a developer machine, `authorized_user` ADC needs `target_service_account` impersonation of an account holding `roles/run.invoker`, because its tokens carry the `client_id` audience. Example: `SyncGcpIdTokenCache({"target_service_account": "invoker@my-project.iam.gserviceaccount.com"}).get_token("https://svc-abc-uc.a.run.app")`.
3. `docs/reference/storage.md`, section `GCS token caches` renamed to `GCP token caches`: add `SyncGcpIdTokenCache`, `AsyncGcpIdTokenCache`, and `GcpIdTokenSnapshot` with one sentence each and a link to the new guide subsection.

Out of scope: OKF (`t02`, `t03`), changelog.

## Bounded Read Manifest

- `docs/usage/embeddings-and-vectors.md` (sections above).
- `docs/usage/storage-auth-and-signing.md` (`Google Cloud credentials and token caches`).
- `docs/reference/storage.md` (`GCS token caches`).
- `src/py/vertebra/embeddings/_google.py::VertexEmbedder.__init__`, `::GeminiEmbedder.embed_multimodal` (signatures as shipped).
- `src/py/vertebra/_vertebra.pyi::SyncGcpIdTokenCache`, `::AsyncGcpIdTokenCache`.
- Root spec (`2.2` contracts).

## Symbol & Signature Contracts

Examples use only shipped signatures:

```python
from vertebra.embeddings import GeminiEmbedder, MultimodalPart, RemoteEmbedder, VertexEmbedder
from vertebra.storage import AsyncGcpIdTokenCache, SyncGcpIdTokenCache

vertex = VertexEmbedder("gemini-embedding-001", project="my-project", dimensions=768)
gemini = GeminiEmbedder("gemini-embedding-2", api_key="...")
rows = await gemini.embed_multimodal([MultimodalPart(mime_type="image/png", data=png_bytes, text="diagram")])
remote = RemoteEmbedder("https://embedder-abc.a.run.app", id_token_cache=AsyncGcpIdTokenCache())
snapshot = SyncGcpIdTokenCache({"target_service_account": "invoker@my-project.iam.gserviceaccount.com"}).get_token("https://svc-abc-uc.a.run.app")
iap_snapshot = SyncGcpIdTokenCache().get_token("123-abc.apps.googleusercontent.com")
```

## Edge Cases & Error Semantics

- No page mentions `RefreshingGcpCredentialProvider` in the embedder table, `use_metadata_server` as a required option, or base64 text descriptors for media.
- Code blocks avoid single-use intermediate variables and internal `msgspec` options.
- No page states that `authorized_user` credentials cannot mint ID tokens; the guide shows the `client_id` audience rule instead.
- The documented mismatch message matches `ch01:t04` verbatim: `authorized_user credentials mint ID tokens only for audience '<client_id>' (the ADC client_id), not '<audience>'; set target_service_account to impersonate a service account for any audience, or add '<client_id>' to the IAP programmaticClients allowlist and request audience '<client_id>'`.

## Public-Seam Test Blueprint

- **Gate**: `documentation_validation`.
- **Worked Check Matrix**:

| Case | Input | Expected |
| --- | --- | --- |
| Docs build | `make docs` | Exit `0` (docstring scan, CLI docs, Zensical build, link checks) |
| Stale claims removed | `git grep -n "RefreshingGcpCredentialProvider\|packed-\`f32\` fields directly" docs/usage/embeddings-and-vectors.md` | No output |
| ID token section present | `git grep -n "ID tokens for Cloud Run" docs/usage/storage-auth-and-signing.md` | One match |
| IAP guidance present | `git grep -n "programmaticClients" docs/usage/storage-auth-and-signing.md` | At least two matches (fix text and IAP paragraph) |
| Cloud Run guidance present | `git grep -n "#### Cloud Run" docs/usage/storage-auth-and-signing.md` | One match |
| No stale rejection claim | `git grep -n "cannot mint audience-bound" docs` | No output |
| Reference names | `git grep -n "SyncGcpIdTokenCache" docs/reference/storage.md` | At least one match |

## Verification

```bash
make docs
git grep -n "RefreshingGcpCredentialProvider" docs/usage/embeddings-and-vectors.md
git grep -n "ID tokens for Cloud Run" docs/usage/storage-auth-and-signing.md
git grep -n "programmaticClients" docs/usage/storage-auth-and-signing.md
git grep -n "cannot mint audience-bound" docs
```

Commit as `docs: document Vertex REST embeddings, ADC order, and GCP ID tokens`.

## Acceptance Criteria

- [ ] Embeddings guide reflects the shipped Vertex, Gemini, and Remote transports and the Oracle rule (R12).
- [ ] Credentials guide documents ADC order and ID token caches with examples (R12).
- [ ] Credentials guide documents the per-credential audience table, the `authorized_user` mismatch fixes, IAP `programmaticClients`, and Cloud Run usage (R12).
- [ ] Storage reference lists the ID token classes (R12).
- [ ] `make docs` exits `0`.

## Notes & Discoveries

- None yet.

[^docs-embeddings]: `docs/usage/embeddings-and-vectors.md` — Embeddings guide.
[^docs-storage-auth]: `docs/usage/storage-auth-and-signing.md` — Credentials guide.
[^docs-storage-ref]: `docs/reference/storage.md` — Storage reference.
