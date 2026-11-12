---
type: Spec
flow_id: gcp-embeddings-and-identity
title: Google Cloud Embedding Transports and Audience-Bound Identity Tokens
description: Corrects the Vertex AI and Gemini embedding wire contracts, sends real multimodal media, hardens embedder authentication and micro-batching, standardizes Application Default Credentials resolution, and adds cached audience-bound OIDC ID token minting for Rust and Python callers.
status: draft
state: planned
has_chapters: true
chapters:
  - ch01-id-token-core
  - ch02-id-token-python-surface
  - ch03-google-embedder-runtime
  - ch04-gemini-wire-and-multimodal
  - ch05-vertex-rest-and-remote-identity
  - ch06-docs-okf-and-limitations
current_chapter: ch01-id-token-core
current_task: null
parent_prd: integration-remediation
depends_on_flows: []
plan_revision: 1
plan_commit: null
state_revision: 0
last_verified_checkpoint: null
last_reviewed_chapter: null
last_reviewed_commit: null
created_at: "2026-11-12T00:35:00Z"
updated_at: "2026-11-12T00:35:00Z"
generated:
  by: agent:plan-generator
  at: "2026-11-12T00:35:00Z"
verified: []
sources:
  - id: core-gcp-auth
    resource: src/rs/vertebra-core/src/gcp_auth.rs::GcpTokenCache
    title: Native GCP access-token cache, credential source resolution, and HTTP clients
  - id: core-auth-source
    resource: src/rs/vertebra-core/src/gcp_auth.rs::GcpAuthSource
    title: Credential source variants (static, authorized user, service account, metadata server, impersonation, gcloud CLI)
  - id: py-gcp-auth
    resource: src/rs/vertebra-py/src/gcp_auth.rs::PySyncGcpTokenCache
    title: PyO3 SyncGcpTokenCache, AsyncGcpTokenCache, and GcpTokenSnapshot bindings
  - id: core-vector-codec
    resource: src/rs/vertebra-core/src/vector.rs::encode_batch_embed_grpc_request
    title: Hand-written BatchEmbedContents protobuf request encoder and packed-float response decoder
  - id: py-google-embedders
    resource: src/py/vertebra/embeddings/_google.py::VertexEmbedder
    title: VertexEmbedder and GeminiEmbedder transports, token handling, and micro-batcher
  - id: py-embedder-base
    resource: src/py/vertebra/embeddings/_base.py::Embedder
    title: Embedder facade, MultimodalPart, EmbeddingError, and sync portal
  - id: py-remote-embedder
    resource: src/py/vertebra/embeddings/_remote.py::RemoteEmbedder
    title: Cluster gRPC embedder client with static bearer authentication
  - id: py-oracle-pushdown
    resource: src/py/vertebra/db/adapters/oracle/embeddings.py::OracleEmbeddingPushdown
    title: Oracle DBMS_VECTOR pushdown with Vertex credential rotation
  - id: py-gcs-config
    resource: src/py/vertebra/storage/_store.py::GCSConfig
    title: GCS configuration that builds token-cache option maps and re-exports token caches
  - id: storage-signing
    resource: src/rs/vertebra-storage/src/signing.rs
    title: GCS signing token-cache resolution with metadata-server retries
  - id: mcp-bridge
    resource: src/py/vertebra/agent/mcp/bridge.py::run_stdio_http_bridge
    title: MCP stdio bridge token_provider seam that consumes ID tokens in a later flow
  - id: okf-embeddings
    resource: .agents/bundles/knowledge/domains/agent/embeddings-and-vectors.md
    title: Embeddings OKF chapter that currently describes transports that do not exist at HEAD
  - id: rust-idtoken-reference
    resource: https://docs.rs/google-cloud-auth/1.18.0/google_cloud_auth/credentials/idtoken/index.html
    title: Official google-cloud-auth ID token flows (metadata identity endpoint, service-account target_audience, IAM generateIdToken)
  - id: google-oauth-refresh
    resource: https://developers.google.com/identity/protocols/oauth2/native-app#offline
    title: Google OAuth 2.0 refresh-token grant whose response carries an id_token for authorized_user ADC
  - id: iap-programmatic-clients
    resource: https://cloud.google.com/iap/docs/sharing-oauth-clients#programmatic_access
    title: IAP programmaticClients allowlist for OAuth client IDs used as ID token audiences
  - id: vertex-embed-reference
    resource: https://cloud.google.com/vertex-ai/generative-ai/docs/reference/rest/v1/projects.locations.publishers.models/embedContent
    title: Vertex AI v1 predict and embedContent embedding REST contracts
  - id: gemini-content-proto
    resource: https://github.com/googleapis/googleapis/blob/master/google/ai/generativelanguage/v1beta/content.proto
    title: Gemini Developer API Content and Part protobuf field numbers
tags:
  - gcp
  - embeddings
  - identity
  - oidc
  - vertex-ai
  - gemini
  - rust
  - pyo3
---

# Specification: Google Cloud Embedding Transports and Audience-Bound Identity Tokens

<!-- truth: start -->
- **Identity Core**: `src/rs/vertebra-core/src/gcp_identity.rs::GcpIdTokenCache` mints audience-bound OIDC ID tokens from the credential source resolved by `GcpTokenCache::from_options` [^core-gcp-auth] [^core-auth-source]: service-account keys exchange a `target_audience` JWT at the key's `token_uri`, the metadata server serves `/computeMetadata/v1/instance/service-accounts/{account}/identity`, impersonation calls IAM Credentials `https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/{sa}:generateIdToken` with `includeEmail`, the gcloud CLI runs `auth print-identity-token --audiences` [^rust-idtoken-reference], and `authorized_user` ADC posts the OAuth `refresh_token` grant to `https://oauth2.googleapis.com/token` and takes the response `id_token` [^google-oauth-refresh].
- **Credential Audiences**: service-account keys, the metadata server, and explicit `target_service_account` impersonation mint any audience. `authorized_user` ADC (`gcloud auth application-default login`) mints only the credential's `client_id` audience; any other audience fails before network I/O with an error naming two fixes: set `target_service_account`, or add the `client_id` to IAP `programmaticClients` and request the `client_id` as the audience [^iap-programmatic-clients]. Static access-token sources are rejected at construction.
- **Per-Audience Cache**: entries are keyed by audience, refreshed single-flight per audience, expire from the JWT `exp` claim minus `refresh_skew_ms` (default `300_000`), reject tokens whose `aud` claim differs from the request, invalidate by generation, and hold at most `max_audiences` (default `64`) entries.
- **Python Surface**: `vertebra._vertebra` exposes `GcpIdTokenSnapshot`, `SyncGcpIdTokenCache`, and `AsyncGcpIdTokenCache` with detached-GIL refreshes; `vertebra.storage` re-exports them next to the access-token caches [^py-gcp-auth] [^py-gcs-config]. `mcp-protocol-completeness` consumes `AsyncGcpIdTokenCache.get_token(audience)` through the MCP `token_provider` seam ([#113](https://github.com/cofin/vertebra/issues/113)) [^mcp-bridge].
- **ADC Order**: `GcpTokenCache::from_options` resolves explicit options, then the ambient ADC file, then ambient access-token variables, then the metadata server when ambient discovery is allowed. Explicit credential options that fail to load return no cache instead of falling through. Duplicated metadata-server heuristics in `_google.py`, the Oracle pushdown, `GCSConfig`, and GCS signing are deleted [^storage-signing] [^py-oracle-pushdown].
- **Gemini Wire**: `encode_batch_embed_grpc_request` writes `Part.text` on field `2` (it writes field `1` at HEAD), and `encode_batch_embed_contents_grpc_request` adds `Part.inline_data` (field `3`, `Blob{mime_type=1,data=2}`) and `Part.file_data` (field `6`, `FileData{mime_type=1,file_uri=2}`) [^core-vector-codec] [^gemini-content-proto].
- **Gemini Multimodal**: `GeminiEmbedder.embed_multimodal` sends one `Content` per item with real media parts and returns one row per item; it never base64-encodes media into text [^py-google-embedders].
- **Vertex REST**: `VertexEmbedder` posts JSON to `https://{location}-aiplatform.googleapis.com/v1/projects/{project}/locations/{location}/publishers/google/models/{model}:predict` (or `:embedContent` for `gemini-embedding-2`), honors per-model instance limits from `ModelSpec`, fans out with bounded concurrency, and supports Vertex multimodal `:embedContent` with `gs://` URIs [^vertex-embed-reference].
- **Embedder Runtime**: `_GoogleEmbedder` normalizes token caches to async, retries one HTTP `401` per request after generation invalidation, raises `EmbeddingError` for HTTP, gRPC, row-count, and dimension failures, and keeps micro-batch queues per running event loop [^py-embedder-base].
- **Remote Identity**: `RemoteEmbedder` accepts an ID token cache for Cloud Run-hosted embedding services, with the endpoint origin as the default audience [^py-remote-embedder].
- **Roadmap**: `6` chapters and `17` tasks; `[0/9 files documented]`; plan anchored at `c8722c76` pending `plan_commit`.
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

The Google embedding and identity stack has five verified defects at `c8722c76`:

1. `VertexEmbedder._embed_batch_impl` posts a Gemini Developer API `google.ai.generativelanguage.v1beta.GenerativeService/BatchEmbedContents` gRPC frame to `https://{location}-aiplatform.googleapis.com`, a host that does not serve that service, so every default `Embedder()` call against real Vertex AI fails [^py-google-embedders].
2. `encode_batch_embed_grpc_request` writes the text of each `Part` on protobuf field `1`, but `google.ai.generativelanguage.v1beta.Part.text` is field `2`; the Gemini API therefore receives empty content [^core-vector-codec] [^gemini-content-proto].
3. `GeminiEmbedder.embed_multimodal` base64-encodes media bytes into a `"[mime:base64]text"` string and embeds that string as text, while the embeddings OKF chapter claims inline media support [^okf-embeddings].
4. The embedder runtime builds token caches with a forced `use_metadata_server` option, silently falls back to `vertebra.storage._store` on `TypeError`, awaits synchronous caches on the event loop, raises bare `RuntimeError`, shares one micro-batch queue across event loops, and never checks that the response row count matches the request. The Oracle pushdown repeats the token-cache fallback and silently skips credential rotation on `ValueError` [^py-oracle-pushdown].
5. No component mints OIDC ID tokens. Service-to-service calls to Cloud Run, Cloud Functions, and IAP-protected services need an audience-bound ID token, and `mcp-protocol-completeness` needs one for authenticated MCP transports ([#113](https://github.com/cofin/vertebra/issues/113)) [^mcp-bridge].

The flow fixes each defect at its owning seam. Identity minting lives in `vertebra-core` next to `GcpTokenCache` so every wheel reuses one credential resolver. Embedding transports follow the documented Google contracts: Vertex AI REST JSON (`:predict`, `:embedContent`) and the Gemini Developer API gRPC `BatchEmbedContents` service with correct field numbers.

### 1.2 Non-Goals

- Wiring ID tokens into MCP clients, the MCP stdio bridge, or A2A transports; `mcp-protocol-completeness` owns that integration and consumes the `AsyncGcpIdTokenCache` contract defined here.
- Verifying ID token signatures (Google-signed JWKS validation). This flow mints and caches tokens for outbound calls; inbound verification belongs to server security guards.
- Workload Identity Federation (`external_account`) credentials and AWS/Azure identity; `GcpAuthSource` has no `external_account` variant and adding one is a separate flow.
- Live Vertex AI or Gemini API calls in CI; tests use loopback HTTP servers and injected transports. Live behavior is recorded as a limitation.
- Changing local embedders (`FastEmbedder`, `GemmaEmbedder`, `StaticEmbedder`), `EmbeddingBroker`, or SQL pushdown adapters other than the Oracle credential rotation path.
- Switching `RemoteEmbedder`'s default transport or wire format.

### 1.3 Upstream Execution Baseline

- Branch `feat/cleanup-2` at `c8722c76` (equal to `origin/main`). No upstream flow dependency.
- Downstream consumer: `mcp-protocol-completeness` imports `AsyncGcpIdTokenCache` and `GcpIdTokenSnapshot` from `vertebra.storage` (or `vertebra._vertebra`) and calls `get_token(audience)` per request.
- Parent PRD: [integration-remediation](../integration-remediation/spec.md).

### 1.4 Code Analysis & Seam Summary

| Area | Files Examined (`path/to/file::Symbol`) | Public Seam, Dependency Category & Integration |
| --- | --- | --- |
| Credential resolution | `src/rs/vertebra-core/src/gcp_auth.rs::GcpTokenCache::from_options`, `::GcpAuthSource` [^core-gcp-auth] [^core-auth-source] | In-process option parsing plus remote-owned OAuth, metadata, and IAM endpoints (loopback test servers). ID token minting reuses the resolved source through crate-visible accessors. |
| Python auth bindings | `src/rs/vertebra-py/src/gcp_auth.rs::PySyncGcpTokenCache`, `::PyAsyncGcpTokenCache` [^py-gcp-auth] | PyO3 frozen classes over `Arc<GcpTokenCache>`; `spawn_rust_db_task` for async, `block_on_db` under `py.detach` for sync. The ID token classes copy this pattern in a sibling module. |
| Gemini wire codec | `src/rs/vertebra-core/src/vector.rs::encode_batch_embed_grpc_request`, `::decode_batch_embed_grpc_response_into_slab`; `src/rs/vertebra-py/src/vector.rs::encode_gemini_embed_grpc_frame` [^core-vector-codec] | In-process pure encoding; tested with literal byte frames. |
| Google embedders | `src/py/vertebra/embeddings/_google.py::VertexEmbedder`, `::GeminiEmbedder` [^py-google-embedders] | External boundary through the injectable `GrpcTransportCallable` `(url, headers, body) -> (status, headers, body)`; default transport is `vertebra._vertebra.gcp_http2_post_async`. |
| Embedder facade | `src/py/vertebra/embeddings/_base.py::Embedder`, `::MultimodalPart`, `::EmbeddingError` [^py-embedder-base] | Public facade; unchanged signatures. |
| Remote embedder | `src/py/vertebra/embeddings/_remote.py::RemoteEmbedder` [^py-remote-embedder] | Second concrete ID token consumer alongside the MCP flow. |
| Oracle pushdown | `src/py/vertebra/db/adapters/oracle/embeddings.py::OracleEmbeddingPushdown._ensure_oracle_credential_fresh` [^py-oracle-pushdown] | Engine-owned capability; duck-typed token caches in tests. |
| Storage auth | `src/py/vertebra/storage/_store.py::GCSConfig.to_token_cache_options`; `src/rs/vertebra-storage/src/signing.rs` [^py-gcs-config] [^storage-signing] | Duplicated metadata-server heuristics that become dead once core ADC order includes the metadata server. |

### 1.5 Relevant OKF Knowledge Chapters & Skills

- [embeddings-and-vectors.md](../../knowledge/domains/agent/embeddings-and-vectors.md), [object-storage.md](../../knowledge/domains/storage/object-storage.md), [gcp-bigquery-and-spanner-drivers.md](../../knowledge/domains/data/gcp-bigquery-and-spanner-drivers.md), [pyo3-ffi-and-batch-dispatch.md](../../knowledge/architecture/worker/pyo3-ffi-and-batch-dispatch.md), [workflow.md](../../knowledge/workflow.md).
- Skills: [pyo3-bridge](../../../skills/pyo3-bridge/SKILL.md), [python-framework](../../../skills/python-framework/SKILL.md), [runtime-ipc](../../../skills/runtime-ipc/SKILL.md), [review-tests](../../../skills/review-tests/SKILL.md).

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Functional Requirements

- **R1.** `GcpIdTokenCache` mints audience-bound ID tokens from service-account keys, the metadata server, `target_service_account` impersonation (IAM `generateIdToken`, any audience), the gcloud CLI, and `authorized_user` ADC (OAuth refresh-token grant, `client_id` audience only, with an audience-mismatch error naming impersonation and IAP `programmaticClients`), and rejects static sources at construction ([#113](https://github.com/cofin/vertebra/issues/113)).
- **R2.** ID tokens are cached per audience with single-flight refresh, JWT `exp`-based expiry and refresh before `exp` minus `refresh_skew_ms`, `aud` claim validation, generation-fenced invalidation, and a bounded audience map.
- **R3.** Python exposes `GcpIdTokenSnapshot`, `SyncGcpIdTokenCache`, and `AsyncGcpIdTokenCache` with stubs, detached-GIL refreshes, `GoogleCloudAuthError` fetch failures, and `vertebra.storage` re-exports.
- **R4.** `GcpTokenCache::from_options` ends ambient discovery with the metadata server, explicit credential options never fall through to ambient sources, and the duplicated metadata heuristics in Python and GCS signing are deleted.
- **R5.** The Gemini gRPC encoder writes `Part.text` on field `2` and encodes inline-data and file-data parts for multimodal contents.
- **R6.** `GeminiEmbedder.embed_multimodal` sends real media parts, validates `MultimodalPart` combinations, rejects `gs://` URIs, and returns exactly one row per item.
- **R7.** `VertexEmbedder` uses Vertex AI REST `:predict` or `:embedContent` per model, requires a project at request time, respects per-model instance limits, fans out with bounded concurrency, validates titles and dimensions, and defaults `auto_truncate` to `False`.
- **R8.** `VertexEmbedder.embed_multimodal` sends Vertex `:embedContent` requests with `inlineData` and `fileData` parts for `gemini-embedding-2`.
- **R9.** The shared Google embedder runtime normalizes token caches to async, resolves default caches through core ADC order without storage fallbacks, retries one `401`, raises `EmbeddingError` for transport failures, validates row counts, and keeps micro-batch queues per event loop.
- **R10.** `OracleEmbeddingPushdown` rotates credentials only from an explicitly supplied token cache and propagates token errors.
- **R11.** `RemoteEmbedder` authenticates with an ID token cache (endpoint-origin default audience, `401` invalidation retry) as an alternative to a static bearer token.
- **R12.** User docs, OKF chapters, and a limitations record describe the shipped behavior.

### 2.2 Full Public Type, Struct, Schema & Signature Contracts

```rust
// src/rs/vertebra-core/src/gcp_identity.rs (feature "gcp-auth")
pub const DEFAULT_ID_TOKEN_REFRESH_SKEW_MS: u64 = 300_000;
pub const DEFAULT_MAX_ID_TOKEN_AUDIENCES: usize = 64;
pub const MAX_ID_TOKEN_AUDIENCE_BYTES: usize = 2_048;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum MetadataIdTokenFormat { Standard, #[default] Full }

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GcpIdTokenOptions {
    pub refresh_skew_ms: u64,
    pub include_email: bool,
    pub metadata_format: MetadataIdTokenFormat,
    pub max_audiences: usize,
}
impl Default for GcpIdTokenOptions { /* 300_000, true, Full, 64 */ }

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GcpIdTokenSnapshot {
    pub id_token: String,
    pub audience: String,
    pub expires_at_unix_ms: u64,
    pub generation: u64,
}

pub struct GcpIdTokenCache { /* Arc<GcpTokenCache>, GcpIdTokenOptions, per-audience entries + flights, AtomicU64 generation */ }
impl GcpIdTokenCache {
    pub fn new(credentials: Arc<GcpTokenCache>, options: GcpIdTokenOptions) -> Result<Arc<Self>, String>;
    pub fn from_options(raw_opts: &HashMap<String, String>) -> Result<Arc<Self>, String>;
    pub fn generation(&self) -> u64;
    pub fn refresh_skew_ms(&self) -> u64;
    pub fn get_cached(&self, audience: &str, now_unix_ms: u64) -> Option<Arc<GcpIdTokenSnapshot>>;
    pub fn needs_refresh(&self, audience: &str, now_unix_ms: u64) -> bool;
    pub fn invalidate(&self, audience: &str, failed_generation: u64);
    pub async fn get_or_refresh(&self, audience: &str, force_refresh: bool) -> Result<Arc<GcpIdTokenSnapshot>, String>;
}
pub fn validate_id_token_audience(audience: &str) -> Result<&str, String>;

// src/rs/vertebra-core/src/gcp_auth.rs (added to impl GcpTokenCache)
pub fn auth_source(&self) -> &GcpAuthSource;
pub(crate) fn http_client(&self) -> &reqwest::Client;
pub(crate) fn metadata_http_client(&self) -> &reqwest::Client;
pub(crate) async fn fetch_source_access_token(&self, source: &GcpAuthSource) -> Result<String, String>;

// src/rs/vertebra-core/src/vector.rs
pub enum GeminiContentPart<'a> {
    Text(&'a str),
    InlineData { mime_type: &'a str, data: &'a [u8] },
    FileData { mime_type: &'a str, file_uri: &'a str },
}
pub fn encode_batch_embed_contents_grpc_request(
    model: &str,
    contents: &[Vec<GeminiContentPart<'_>>],
    task_type: Option<u32>,
    titles: Option<&[Option<String>]>,
    output_dimensionality: Option<u32>,
) -> Result<Vec<u8>, String>;
```

```python
# vertebra._vertebra (src/py/vertebra/_vertebra.pyi), re-exported by vertebra.storage
@final
class GcpIdTokenSnapshot:
    def __new__(cls, *, id_token: str, audience: str, expires_at_unix_ms: int, generation: int) -> GcpIdTokenSnapshot: ...
    id_token: str  # read-only properties: id_token, audience, expires_at_unix_ms, generation
@final
class SyncGcpIdTokenCache:
    def __new__(cls, options: dict[str, str] | None = None, *, allow_ambient_adc: bool = True) -> SyncGcpIdTokenCache: ...
    def to_async(self) -> AsyncGcpIdTokenCache: ...
    generation: int; refresh_skew_ms: int  # read-only properties
    def needs_refresh(self, audience: str, *, now_unix_ms: int | None = None) -> bool: ...
    def invalidate(self, audience: str, failed_generation: int) -> None: ...
    def get_token(self, audience: str, *, force_refresh: bool = False) -> GcpIdTokenSnapshot: ...
@final
class AsyncGcpIdTokenCache:  # same surface; to_sync(); get_token returns RustDbTaskAwaitable[GcpIdTokenSnapshot]
def encode_gemini_embed_contents_grpc_frame(
    model: str,
    contents: Sequence[Sequence[tuple[Literal["text", "inline_data", "file_data"], str, str | bytes]]],
    task: str | None = None,
    titles: Sequence[str | None] | None = None,
    output_dimensionality: int | None = None,
) -> bytes: ...

# src/py/vertebra/embeddings/_google.py
class _GoogleEmbedder(Embedder):
    DEFAULT_SCOPES: ClassVar[tuple[str, ...]] = ("https://www.googleapis.com/auth/cloud-platform",)
    async def embed_multimodal(self, items: Sequence[MultimodalPart | str], *, task: str = "document") -> VectorBatchView: ...
class VertexEmbedder(_GoogleEmbedder):
    def __init__(self, model: str = "gemini-embedding-001", *, dimensions: int = 768, project: str | None = None,
                 location: str = "us-central1", endpoint: str | None = None,
                 token_cache: SyncGcpTokenCache | AsyncGcpTokenCache | None = None,
                 target_service_account: str | None = None, quota_project_id: str | None = None,
                 cache: VectorCache | None = None, max_batch_size: int = 64, max_batch_tokens: int = 16_384,
                 coalesce_window_ms: float = 1.0, max_concurrent_requests: int = 8, auto_truncate: bool = False,
                 transport: GrpcTransportCallable | None = None) -> None: ...
class GeminiEmbedder(_GoogleEmbedder):  # constructor signature unchanged from HEAD
    ...

# src/py/vertebra/embeddings/_models.py
class ModelSpec(msgspec.Struct, frozen=True, gc=False):
    ...  # existing fields
    vertex_method: Literal["predict", "embed_content"] = "predict"
    max_instances_per_request: int = 1

# src/py/vertebra/embeddings/_remote.py
class RemoteEmbedder(Embedder):
    def __init__(self, endpoint: str = "http://127.0.0.1:8000", model: str = "gemini-embedding-001", *,
                 dimensions: int = 768, bearer_token: str | None = None,
                 id_token_cache: SyncGcpIdTokenCache | AsyncGcpIdTokenCache | None = None,
                 audience: str | None = None, cache: VectorCache | None = None,
                 transport: GrpcTransportCallable | None = None) -> None: ...
```

`GcpIdTokenCache::from_options` accepts every `GcpTokenCache::from_options` key plus `id_token_refresh_skew_ms`, `include_email` (`true`/`false`), `metadata_id_token_format` (`full`/`standard`), and `max_id_token_audiences`. Unparseable values for these keys return `Err` naming the key.

### 2.3 Caller -> Seam -> Collaborator Integration Map

- MCP transport (later flow) or `RemoteEmbedder._embed_batch_impl` -> `AsyncGcpIdTokenCache.get_token(audience)` -> `GcpIdTokenCache::get_or_refresh` -> source dispatch: `ServiceAccount` -> `POST {token_uri}` (`grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer`, assertion claims `iss`, `sub`, `aud={token_uri}`, `target_audience`, `iat`, `exp`) -> `{"id_token": ...}`; `MetadataServer` -> `GET {endpoint}/computeMetadata/v1/instance/service-accounts/{account}/identity?audience=...&format=full` with `Metadata-Flavor: Google`; `ImpersonatedServiceAccount` -> `GcpTokenCache::fetch_source_access_token(base)` -> `POST {iam_endpoint}/v1/projects/-/serviceAccounts/{account}:generateIdToken` `{"audience", "includeEmail"}` -> `{"token": ...}`; `GcloudCli` -> `gcloud auth print-identity-token --audiences=<audience> --quiet`; `AuthorizedUser` -> audience must equal `client_id` -> `POST {token_uri}` (`grant_type=refresh_token`, `client_id`, `client_secret`, `refresh_token`, no `scope`) -> `{"id_token": ...}`.
- `Embedder()` / user code -> `VertexEmbedder._embed_batch_impl` -> `_GoogleEmbedder._post_authorized` -> `AsyncGcpTokenCache.get_token` + transport `POST .../{model}:predict|:embedContent` -> `decode_json` into response structs -> `VectorBatchView.from_f32_rows(rows, normalize=True)`.
- `GeminiEmbedder._embed_batch_impl` / `embed_multimodal` -> `encode_gemini_embed_contents_grpc_frame` (`vertebra-py`) -> `encode_batch_embed_contents_grpc_request` (`vertebra-core`) -> `_GoogleEmbedder._post_authorized` (API key or ADC) -> `decode_gemini_embed_grpc_frame_to_batch`.
- `GcpTokenCache::from_options` callers (`GCSConfig`, GCS signing, database IAM configs, BigQuery/Spanner credentials, embedders) -> ADC order ending in `GcpAuthSource::MetadataServer`.

### 2.4 Non-Functional Constraints & Edge-Case / Failure-Mode Matrix

| Scenario / Boundary | Trigger / Input | Expected Behavior / Error Contract | Invariant Preserved |
| --- | --- | --- | --- |
| User ADC, client audience | ADC file of type `authorized_user`, no `target_service_account`, audience equal to `client_id` | Refresh-token grant at `token_uri`; snapshot from the response `id_token`, cached until `exp` minus skew | User credentials work without impersonation |
| User ADC, other audience | Same ADC file, audience `https://svc-abc-uc.a.run.app` | `Err` / `GoogleCloudAuthError` with message `authorized_user credentials mint ID tokens only for audience '<client_id>' (the ADC client_id), not '<audience>'; set target_service_account to impersonate a service account for any audience, or add '<client_id>' to the IAP programmaticClients allowlist and request audience '<client_id>'`; no request sent | No token with an unrequested audience is ever returned |
| User ADC, impersonated | Same ADC file plus `target_service_account` | Refresh grant for the base access token, then `generateIdToken` for any audience | Impersonation lifts the audience restriction |
| User ADC, no `id_token` | Refresh response lacks `id_token` | `Err` containing `contained no id_token` and `gcloud auth application-default login` | Fail loud; no gcloud CLI fallback |
| Static source | `access_token` option only | `GcpIdTokenCache::from_options` returns `Err` containing `static access tokens cannot mint ID tokens` | No unbound token is ever returned |
| Audience mismatch | Token server returns JWT with `aud` `https://other` for request `https://svc` | `Err("ID token audience mismatch ...")`; nothing cached | Cached entries match their key |
| Expired on arrival | JWT `exp` at or before now | `Err("ID token already expired ...")` | Snapshots are always usable at return time |
| Concurrent refresh | `8` concurrent `get_or_refresh("https://svc", false)` | One upstream fetch; all callers share one snapshot | Single flight per audience |
| Audience pressure | `65` distinct audiences with `max_audiences = 64` | Expired entries evicted first, then the earliest-expiring entry | Memory bounded |
| Invalid audience | `""`, `"   "`, string with control character, or more than `2048` bytes | `Err` from `validate_id_token_audience`; Python `ValueError` before any network call | No malformed query strings |
| Explicit bad credential | `service_account_path` points to a missing file | `GcpTokenCache::from_options` returns `None` even when ambient ADC exists | Explicit configuration never silently swaps identity |
| No ambient credentials | No options, no ADC file, no token variables, ambient allowed | Metadata-server source; first fetch fails with `metadata server request failed` off Google Cloud | Standard ADC order |
| Ambient disabled | `allow_ambient_adc=False`, no options | `None` / `ValueError("unable to configure GcpTokenCache from options")` | Existing opt-out preserved |
| Gemini text part | Any text embedding request | `Part.text` encoded with tag byte `0x12` | Matches `content.proto` |
| Gemini `gs://` media | `MultimodalPart(uri="gs://b/o.png")` on `GeminiEmbedder` | `ValueError` naming `VertexEmbedder` | Developer API never receives unsupported URIs |
| Conflicting part | `MultimodalPart(data=b"x", uri="https://...")` or no data, uri, or text | `ValueError` | One media source per part |
| Row count drift | Response returns `1` row for `2` inputs | `EmbeddingError("... returned 1 embeddings for 2 inputs")` | Rows align with inputs |
| Vertex project missing | No `project`, `GOOGLE_CLOUD_PROJECT`, or `GCP_PROJECT` | `ImproperConfigurationError` on first request; construction succeeds | `Embedder()` stays constructible |
| Vertex title misuse | Non-`None` title with `task="query"` | `ValueError`; no request sent | Vertex rejects titles outside `RETRIEVAL_DOCUMENT` |
| Dimension drift | Vertex returns `3072` values for `dimensions=768` | `EmbeddingError` | Output shape matches configuration |
| Token expiry mid-call | First POST returns `401` | Invalidate snapshot generation, refresh, retry once; second `401` raises `EmbeddingError` | At most two attempts |
| Cross-loop use | `embed_query_sync` (portal loop) concurrent with `embed_query` (main loop) | Separate queues and drain tasks per loop | Futures resolve on their own loop |
| Oracle without cache | `OracleEmbeddingPushdown(..., token_cache=None)` | No credential rotation SQL is issued | DBA-managed credentials untouched |
| Oracle token failure | Supplied cache raises `GoogleCloudAuthError` | Error propagates from `embed_query` | No silent skip |

Every native module stays `gil_used = false`; sync refreshes run under `py.detach`, async refreshes run on the shared database runtime without attaching Python. Tokens never appear in `Debug` or `__repr__` output.

### 2.5 Acceptance Scenarios (Given / When / Then)

- Given a service-account key whose loopback `token_uri` returns `{"id_token": <JWT aud=https://svc exp=now+3600>}`, when `SyncGcpIdTokenCache({"service_account_key": key_json}).get_token("https://svc")` runs twice, then both snapshots carry the same `id_token`, `audience == "https://svc"`, and the token server records one request.
- Given a loopback metadata server, when `AsyncGcpIdTokenCache({"use_metadata_server": "true", "metadata_host": base}).get_token("https://svc")` is awaited, then the server receives `GET /computeMetadata/v1/instance/service-accounts/default/identity?audience=https%3A%2F%2Fsvc&format=full` with `Metadata-Flavor: Google`.
- Given ADC `authorized_user` credentials with `client_id = "123-abc.apps.googleusercontent.com"` and no impersonation, when `SyncGcpIdTokenCache().get_token("123-abc.apps.googleusercontent.com")` runs, then the token endpoint receives one `grant_type=refresh_token` form and the snapshot carries the response `id_token`.
- Given the same credentials, when `get_token("https://svc-abc-uc.a.run.app")` runs, then `GoogleCloudAuthError` names `'123-abc.apps.googleusercontent.com' (the ADC client_id)`, `target_service_account`, and `IAP programmaticClients`, and no request is sent.
- Given the same credentials plus `target_service_account`, when `get_token("https://svc-abc-uc.a.run.app")` runs, then IAM `generateIdToken` receives `{"audience": "https://svc-abc-uc.a.run.app", "includeEmail": true}`.
- Given `GeminiEmbedder("gemini-embedding-2", api_key="k")` and `[MultimodalPart(mime_type="image/png", data=b"\x89P"), "caption"]`, when `embed_multimodal` runs, then the gRPC body contains a `Blob` with `image/png` and raw bytes `\x89P`, a text part `caption` on field `2`, and the result has `2` rows.
- Given `VertexEmbedder("gemini-embedding-001", project="p", dimensions=4)` and `3` texts, when `embed_many` runs, then `3` `:predict` requests (one instance each) reach `/v1/projects/p/locations/us-central1/publishers/google/models/gemini-embedding-001:predict` and rows return in input order.

### 2.6 Alternatives Considered

| Option | Depth / Locality / Seam | Decision |
| --- | --- | --- |
| Separate `GcpIdTokenCache` in `vertebra-core` reusing `GcpTokenCache` sources (chosen) | Deep module with one entry point (`get_or_refresh(audience)`); credential parsing stays in one place | Chosen: one resolver for both token kinds and no duplicated option parsing |
| Add `get_id_token(audience)` to `GcpTokenCache` | Mixes single-snapshot access-token state with a multi-key map; widens the C-ABI capsule contract | Rejected: complicates the capsule ABI and generation semantics |
| Adopt `google-cloud-auth` `idtoken` builders | Official flows, but a second credential parser, separate token cache, and retry policies that diverge from `GcpTokenCache` | Rejected: two ADC resolvers with different precedence |
| Vertex over gRPC `PredictionService/EmbedContent` with hand-written protobuf | Fast packed floats, but `EmbedContentRequest` mixes deprecated and nested config fields whose numbers are unverified in-repo | Rejected: REST JSON contracts are documented and testable with literals; network latency dominates float parsing |
| Keep Gemini on REST JSON instead of gRPC | Simpler, but drops the existing packed-float decoder that is correct for `BatchEmbedContentsResponse` | Rejected: only the request encoder is wrong; the gRPC path stays |
| Silent `autoTruncate=true` default | Matches the API default but loses input text silently | Rejected: `auto_truncate=False` default surfaces over-length input as `EmbeddingError` |

## 3. Documentation & OKF Knowledge Plan

- **Progress**: `[0/9 files documented]`
- `[ ]` `docs/usage/embeddings-and-vectors.md` — Vertex REST transport, Gemini gRPC multimodal example, `auto_truncate`, `RemoteEmbedder` ID token authentication (`ch06:t01`).
- `[ ]` `docs/usage/storage-auth-and-signing.md` — ADC resolution order, an ID token section with sync and async examples, per-credential audience rules, IAP `programmaticClients`, and Cloud Run usage (`ch06:t01`).
- `[ ]` `docs/reference/storage.md` — Re-exported `GcpIdTokenSnapshot`, `SyncGcpIdTokenCache`, `AsyncGcpIdTokenCache` (`ch06:t01`).
- `[ ]` `.agents/bundles/knowledge/domains/agent/embeddings-and-vectors.md` — Corrected transports, multimodal parts, runtime invariants (`ch06:t02`).
- `[ ]` `.agents/bundles/knowledge/architecture/worker/pyo3-ffi-and-batch-dispatch.md` — `gcp_identity.rs` bindings and the multimodal encoder binding (`ch06:t02`).
- `[ ]` `.agents/bundles/knowledge/domains/storage/object-storage.md` — ID token re-exports and ADC order (`ch06:t02`).
- `[ ]` `.agents/bundles/knowledge/domains/agent/embeddings-and-identity-limitations.md` — New limitations record (`ch06:t03`).
- `[ ]` `.agents/bundles/knowledge/domains/index.md` — Link to the limitations record (`ch06:t03`).
- `[ ]` `.agents/bundles/knowledge/domains/data/gcp-bigquery-and-spanner-drivers.md` — ADC order statement for scoped credentials (`ch06:t03`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement / Doc Target | Owning Chapter / Task | Verification Strategy | Canonical Test / Doc Gate |
| --- | --- | --- | --- |
| R1 (service account, metadata) | `ch01 / t01` | `behavior_tdd` | `src/rs/vertebra-core/tests/it/gcp_auth.rs::id_token_service_account_*`, `::id_token_metadata_*` |
| R1 (impersonation, CLI, static rejection) | `ch01 / t02` | `behavior_tdd` | `src/rs/vertebra-core/tests/it/gcp_auth.rs::id_token_impersonated_*`, `::id_token_gcloud_*`, `::id_token_rejects_static` |
| R1 (authorized-user ADC, audience mismatch), R2 (`exp` refresh) | `ch01 / t04` | `behavior_tdd` | `src/rs/vertebra-core/tests/it/gcp_auth.rs::id_token_authorized_user_*`, `test_gcs_token_refresh.py::test_id_token_authorized_user_*` |
| R2 | `ch01 / t01` | `behavior_tdd` | `src/rs/vertebra-core/tests/it/gcp_auth.rs::id_token_cache_*` |
| R3 (bindings) | `ch02 / t01` | `behavior_tdd` | `src/py/tests/unit/storage/test_gcs_token_refresh.py::test_id_token_*` |
| R3 (stubs, re-exports) | `ch02 / t02` | `static_validation` | `mypy.stubtest vertebra._vertebra`, `test_gcs_token_refresh.py::test_id_token_caches_reexported_from_storage` |
| R4 | `ch01 / t03` | `regression_tdd` | `tests/it/gcp_auth.rs::adc_*`, `test_gcs_token_refresh.py::test_gcs_config_ambient_adc_precedence_and_isolation` |
| R5 | `ch04 / t01` | `regression_tdd` | `src/py/tests/unit/db/test_vector_wire_codecs.py::test_gemini_*`, `vector.rs` unit tests |
| R6 | `ch04 / t02` | `regression_tdd` | `src/py/tests/unit/embeddings/test_google_embedders.py::test_gemini_multimodal_*` |
| R7 | `ch05 / t01` | `regression_tdd` | `test_google_embedders.py::test_vertex_*` |
| R8 | `ch05 / t02` | `behavior_tdd` | `test_google_embedders.py::test_vertex_multimodal_*` |
| R9 (auth base) | `ch03 / t01` | `regression_tdd` | `test_google_embedders.py::test_google_embedder_*` |
| R9 (per-loop batching) | `ch03 / t02` | `regression_tdd` | `test_google_embedders.py::test_micro_batch_*` |
| R10 | `ch03 / t03` | `regression_tdd` | `test_google_embedders.py::test_oracle_*` |
| R11 | `ch05 / t03` | `behavior_tdd` | `test_google_embedders.py::test_remote_embedder_id_token_*` |
| R12 (user docs) | `ch06 / t01` | `documentation_validation` | `make docs` |
| R12 (OKF) | `ch06 / t02`, `ch06 / t03` | `documentation_validation` | `make review-check`, link and frontmatter checks |

## 5. Implementation Plan

- [ ] ch01-id-token-core — Rust ID token cache, source dispatch (service account, metadata, impersonation, gcloud CLI, authorized-user ADC), and ADC metadata fallback (chapters/ch01-id-token-core/spec.md)
- [ ] ch02-id-token-python-surface — PyO3 ID token caches, stubs, and `vertebra.storage` re-exports (chapters/ch02-id-token-python-surface/spec.md)
- [ ] ch03-google-embedder-runtime — Shared Google embedder auth, errors, per-loop batching, and Oracle rotation (chapters/ch03-google-embedder-runtime/spec.md)
- [ ] ch04-gemini-wire-and-multimodal — Gemini `Part` field fix, multimodal encoder, and real media embedding (chapters/ch04-gemini-wire-and-multimodal/spec.md)
- [ ] ch05-vertex-rest-and-remote-identity — Vertex REST transport, Vertex multimodal, and `RemoteEmbedder` ID tokens (chapters/ch05-vertex-rest-and-remote-identity/spec.md)
- [ ] ch06-docs-okf-and-limitations — User docs, OKF chapters, and limitations record (chapters/ch06-docs-okf-and-limitations/spec.md)
- **Chapter / Phase Recovery Checkpoint**: `make lint-all && make rs-test && make py-test`

## 6. Current State & Remaining Plan Snapshot

- **Verified Baseline (`HEAD`)**: `c8722c76` — plan authored; `0/6` chapters completed.
- **Active Chapter**: `ch01-id-token-core` (`0/4` tasks completed).
- **Remaining Plan**: `ch01`–`ch06` (`17` tasks).
- **Documentation & OKF Sync**: `[0/9 files documented]`.
- **Active Invariants & Triggers**: tasks touching `_google.py` (`ch03:t01`, `ch03:t02`, `ch04:t02`, `ch05:t01`, `ch05:t02`) and `_vertebra.pyi` (`ch02:t02`, `ch04:t01`) run strictly in roadmap order; any Rust change in `vertebra-core` or `vertebra-py` requires `make dev` before Python tests.

[^core-gcp-auth]: `src/rs/vertebra-core/src/gcp_auth.rs::GcpTokenCache` — Access-token cache, `from_options` resolution, and HTTP clients.
[^core-auth-source]: `src/rs/vertebra-core/src/gcp_auth.rs::GcpAuthSource` — Credential source variants.
[^py-gcp-auth]: `src/rs/vertebra-py/src/gcp_auth.rs::PySyncGcpTokenCache` — PyO3 access-token cache bindings.
[^core-vector-codec]: `src/rs/vertebra-core/src/vector.rs::encode_batch_embed_grpc_request` — Gemini request encoder and response decoder.
[^py-google-embedders]: `src/py/vertebra/embeddings/_google.py::VertexEmbedder` — Google embedders at HEAD.
[^py-embedder-base]: `src/py/vertebra/embeddings/_base.py::Embedder` — Embedder facade, `MultimodalPart`, `EmbeddingError`.
[^py-remote-embedder]: `src/py/vertebra/embeddings/_remote.py::RemoteEmbedder` — Cluster gRPC embedder client.
[^py-oracle-pushdown]: `src/py/vertebra/db/adapters/oracle/embeddings.py::OracleEmbeddingPushdown` — Oracle credential rotation.
[^py-gcs-config]: `src/py/vertebra/storage/_store.py::GCSConfig` — Token-cache option maps and re-exports.
[^storage-signing]: `src/rs/vertebra-storage/src/signing.rs` — Signing token-cache resolution with metadata retries.
[^mcp-bridge]: `src/py/vertebra/agent/mcp/bridge.py::run_stdio_http_bridge` — MCP `token_provider` seam.
[^okf-embeddings]: `.agents/bundles/knowledge/domains/agent/embeddings-and-vectors.md` — Embeddings OKF chapter.
[^rust-idtoken-reference]: `https://docs.rs/google-cloud-auth/1.18.0/google_cloud_auth/credentials/idtoken/index.html` — Official ID token flows (verified against the vendored `google-cloud-auth 1.18.0` sources `credentials/idtoken/{mds,service_account,impersonated}.rs`).
[^google-oauth-refresh]: `https://developers.google.com/identity/protocols/oauth2/native-app#offline` — Refresh-token grant used for `authorized_user` ID tokens.
[^iap-programmatic-clients]: `https://cloud.google.com/iap/docs/sharing-oauth-clients#programmatic_access` — IAP `programmaticClients` allowlist.
[^vertex-embed-reference]: `https://cloud.google.com/vertex-ai/generative-ai/docs/reference/rest/v1/projects.locations.publishers.models/embedContent` — Vertex AI embedding REST contracts.
[^gemini-content-proto]: `https://github.com/googleapis/googleapis/blob/master/google/ai/generativelanguage/v1beta/content.proto` — `Part.text = 2`, `Part.inline_data = 3`, `Part.file_data = 6`.
