---
type: Spec
flow_id: gcp-embeddings-and-identity
title: "Google Cloud Embedding Transports, Identity Tokens, and Local Broker Parity"
description: "Implements audience-bound OIDC ID token caches across all ADC sources, repairs Vertex AI REST and Gemini multimodal embedding wire contracts, aligns SQL embedding pushdown adapters, and hardens local EmbeddingBroker multi-loop state and GemmaEmbedder precision forwarding."
status: stable
state: completed
planning_quality: standard
planning_degraded_reason: null
has_chapters: true
chapters:
  - ch01-gcp-id-token-cache
  - ch02-vertex-and-gemini-embedder-wire-parity
  - ch03-pushdown-and-local-embedding-broker
  - ch04-docs-and-okf-sync
current_chapter: null
current_task: null
parent_prd: integration-remediation
plan_revision: 2
plan_commit: 18baadcd
last_verified_checkpoint: 1cd60f53
last_reviewed_chapter: ch04-docs-and-okf-sync
last_reviewed_commit: 1cd60f53
created_at: "2026-11-12T00:35:00Z"
updated_at: "2026-11-13T15:20:00Z"
generated:
  by: flow-plan
  at: "2026-11-12T23:45:00Z"
verified:
  - by: agent:code-reviewer
    at: "2026-11-13T15:20:00Z"
    commit: 1cd60f53
tags:
  - gcp
  - embeddings
  - identity
  - oidc
  - vertex-ai
  - gemini
sources:
  - id: core-gcp-auth
    resource: src/rs/vertebra-core/src/gcp_auth.rs::GcpTokenCache
    title: "Native GCP access-token cache, credential source resolution, and HTTP clients"
  - id: py-gcp-auth
    resource: src/rs/vertebra-py/src/gcp_auth.rs::PySyncGcpTokenCache
    title: "PyO3 SyncGcpTokenCache, AsyncGcpTokenCache, and GcpTokenSnapshot bindings"
  - id: core-vector-codec
    resource: src/rs/vertebra-core/src/vector.rs::encode_batch_embed_grpc_request
    title: "Gemini BatchEmbedContents protobuf request encoder and response decoder"
  - id: py-google-embedders
    resource: src/py/vertebra/embeddings/_google.py::VertexEmbedder
    title: "VertexEmbedder and GeminiEmbedder transports, token handling, and micro-batcher"
  - id: py-remote-embedder
    resource: src/py/vertebra/embeddings/_remote.py::RemoteEmbedder
    title: "Cluster gRPC embedder client"
  - id: py-broker
    resource: src/py/vertebra/embeddings/_broker.py::EmbeddingBroker
    title: "Local EmbeddingBroker batching queue and sidecar SHM transport"
  - id: py-local
    resource: src/py/vertebra/embeddings/_local.py::GemmaEmbedder
    title: "Local GemmaEmbedder and FastEmbedder BackendFactory wiring"
  - id: py-oracle-pushdown
    resource: src/py/vertebra/db/adapters/oracle/embeddings.py::OracleEmbeddingPushdown
    title: "Oracle 26ai DBMS_VECTOR pushdown and Vertex credential rotation"
  - id: py-spanner-pushdown
    resource: src/py/vertebra/db/adapters/spanner/embeddings.py::SpannerEmbeddingPushdown
    title: "Spanner SQL embedding generation and vector search pushdown"
  - id: py-gcs-config
    resource: src/py/vertebra/storage/_store.py::GCSConfig
    title: "GCS configuration and public GCP token-cache re-exports"
  - id: storage-signing
    resource: src/rs/vertebra-storage/src/signing.rs
    title: "GCS V4 signing token-cache resolution"
  - id: okf-embeddings
    resource: .agents/bundles/knowledge/domains/agent/embeddings-and-vectors.md
    title: "Embeddings and vector slabs OKF chapter"
  - id: okf-limitations
    resource: .agents/bundles/knowledge/domains/agent/embeddings-limitations.md
    title: "Existing embeddings limitations record"
---

# Specification: Google Cloud Embedding Transports, Identity Tokens, and Local Broker Parity

<!-- truth: start -->
- **Baseline (`18baadcd`)**: `vertebra-core` mints only OAuth2 access tokens [^core-gcp-auth]; `encode_batch_embed_grpc_request` encodes `Part.text` on protobuf field `1` instead of field `2` [^core-vector-codec]; `VertexEmbedder` posts Gemini `GenerativeService/BatchEmbedContents` gRPC frames to `*-aiplatform.googleapis.com` and `GeminiEmbedder.embed_multimodal` embeds base64 text descriptors [^py-google-embedders]; `EmbeddingBroker` shares loop-bound primitives across event loops [^py-broker]; and `GemmaEmbedder` drops its validated `precision` option [^py-local].
- **Target Architecture**: `GcpIdTokenCache` (`SyncGcpIdTokenCache` / `AsyncGcpIdTokenCache` in `vertebra._vertebra` and `vertebra.storage`, [#113](https://github.com/cofin/vertebra/issues/113)) mints per-audience single-flight OIDC ID tokens from GCE metadata, service-account JWTs, IAM `generateIdToken` impersonation, `gcloud`, and `authorized_user` ADC refresh tokens (`client_id` audience, never rejecting user ADC) [^py-gcp-auth] [^py-gcs-config]. `VertexEmbedder` calls Vertex AI v1 REST `:predict` / `:embedContent`, `GeminiEmbedder` encodes protobuf field `2` text plus field `3` `inline_data` and field `6` `file_data` parts, SQL pushdown adapters enforce uniform batch/dimension/credential contracts [^py-oracle-pushdown] [^py-spanner-pushdown], and `EmbeddingBroker` isolates state per running event loop.
- **Scope & Boundaries**: Covers Rust/PyO3 ID token caches, ADC metadata fallback, cloud/remote/SQL/local embedder correctness, user docs, and OKF sync (`4` chapters, `15` compact tasks). Inbound JWKS verification, `external_account` WIF, and MCP bridge CLI wiring (owned by `mcp-protocol-completeness`) are out of scope.
<!-- truth: end -->

## 1. Specification, Context & Provenance

At baseline `18baadcd`, outbound calls to Cloud Run, Cloud Functions, and IAP-protected endpoints have no audience-bound OIDC ID token cache ([#113](https://github.com/cofin/vertebra/issues/113)) [^core-gcp-auth], and `GcpTokenCache::from_options` omits the GCE metadata server at the end of ambient ADC discovery, forcing duplicate `use_metadata_server` heuristics in Python and `vertebra-storage` [^py-gcs-config] [^storage-signing]. On the embedding plane, `VertexEmbedder` posts `GenerativeService/BatchEmbedContents` gRPC payloads to Vertex AI hosts that do not serve that service, `encode_batch_embed_grpc_request` writes `Part.text` on protobuf field `1` rather than field `2`, and `GeminiEmbedder.embed_multimodal` converts media into text strings [^core-vector-codec] [^py-google-embedders]. In addition, `OracleEmbeddingPushdown` silently swallows credential errors [^py-oracle-pushdown], `SpannerEmbeddingPushdown` and sibling SQL pushdown classes diverge on `embed_batch`, task binding, and strict vector element validation [^py-spanner-pushdown], `EmbeddingBroker` shares one `_pending` queue, `_flush_task`, and `asyncio.Lock` across event loops [^py-broker], and `GemmaEmbedder` ignores `self._precision` when constructing `BackendFactory` [^py-local]. Parent PRD: [integration-remediation](../integration-remediation/spec.md). Relevant OKF: [embeddings-and-vectors.md](../../knowledge/domains/agent/embeddings-and-vectors.md) and [embeddings-limitations.md](../../knowledge/domains/agent/embeddings-limitations.md).

## 2. Requirements, Concrete Contracts & Architecture

- **R1 (ID Token Sources & User ADC Support, [#113](https://github.com/cofin/vertebra/issues/113))**: `GcpIdTokenCache` (`src/rs/vertebra-core/src/gcp_identity.rs`) mints audience-bound OIDC ID tokens from `ServiceAccount` (`target_audience` JWT assertion at `token_uri`), `MetadataServer` (`/computeMetadata/v1/instance/service-accounts/{account}/identity?audience=...&format=full`), `ImpersonatedServiceAccount` (IAM `generateIdToken` with `includeEmail`, any audience), `GcloudCli` (`auth print-identity-token --audiences`), and `AuthorizedUser` ADC (`grant_type=refresh_token` without `scope` -> response `id_token` for the credential's `client_id` audience; non-`client_id` audiences fail before network I/O naming `target_service_account` impersonation and IAP `programmaticClients`). Static access-token sources fail at construction.
- **R2 (Per-Audience Cache Invariants)**: Entries are keyed by validated audience (`validate_id_token_audience`, `<= 2048` bytes, non-empty, no control chars), refreshed single-flight per audience, expire at JWT `exp` minus `refresh_skew_ms` (`300_000` default), verify JWT `aud` matches the requested audience, support generation-fenced `invalidate(audience, failed_generation)`, and bound capacity to `max_audiences` (`64` default).
- **R3 (Python ID Token Surface)**: `vertebra._vertebra` exports frozen `gil_used = false` classes `GcpIdTokenSnapshot`, `SyncGcpIdTokenCache`, and `AsyncGcpIdTokenCache` (`get_token(audience, *, force_refresh=False)`, `needs_refresh`, `invalidate`, `to_async`/`to_sync`), typed in `_vertebra.pyi` and re-exported in `vertebra.storage` (`__all__`) [^py-gcp-auth] [^py-gcs-config].
- **R4 (Canonical ADC Order)**: `GcpTokenCache::from_options` resolves explicit options -> ambient ADC file -> ambient token env vars -> `GcpAuthSource::MetadataServer` (when `allow_ambient_adc` is true). Explicit bad credential options return `None` without ambient fallthrough; duplicated metadata heuristics in `signing.rs`, `GCSConfig`, `_google.py`, and `oracle/embeddings.py` are removed [^storage-signing].
- **R5 (Gemini Protobuf Part Codec)**: `encode_batch_embed_grpc_request` writes `Part.text` on field `2` (`0x12`), and `encode_batch_embed_contents_grpc_request` / PyO3 `encode_gemini_embed_contents_grpc_frame` encode `GeminiContentPart::{Text, InlineData, FileData}` (`Part.inline_data = 3` `Blob{mime_type=1,data=2}`, `Part.file_data = 6` `FileData{mime_type=1,file_uri=2}`) [^core-vector-codec].
- **R6 (Gemini Multimodal)**: `GeminiEmbedder.embed_multimodal` sends real `inline_data` and `file_data` parts per item, rejects conflicting/empty `MultimodalPart` inputs and `gs://` URIs (`ValueError` naming `VertexEmbedder`), and returns one row per item.
- **R7 & R8 (Vertex AI REST & Multimodal)**: `ModelSpec` adds `vertex_method: Literal["predict", "embed_content"] = "predict"` and `max_instances_per_request: int = 1`. `VertexEmbedder` posts JSON to `{endpoint}/v1/projects/{project}/locations/{location}/publishers/google/models/{model}:{predict|embedContent}` (`https://aiplatform.googleapis.com` when `location == "global"`), requires `project` at request time (`ImproperConfigurationError`), defaults `auto_truncate: bool = False`, chunks by `max_instances_per_request` and `20,000` tokens with bounded `asyncio.Semaphore(max_concurrent_requests)`, checks exact output `dimensions`, and supports `embed_multimodal` (`inlineData` base64 `{mimeType, data}` and `fileData` `{mimeType, fileUri}` including `gs://` URIs) on `gemini-embedding-2` [^py-google-embedders].
- **R9 & R11 (Google Embedder Base & Remote Identity)**: `_GoogleEmbedder` normalizes `SyncGcpTokenCache` via `.to_async()`, retries one HTTP `401` after generation invalidation, maps HTTP/gRPC/row-count failures to `EmbeddingError`, and isolates micro-batch queues per running event loop. `RemoteEmbedder` accepts `id_token_cache` and `audience` (defaulting to endpoint origin, mutually exclusive with `bearer_token`) with one `401` invalidation retry [^py-remote-embedder].
- **R10 & R12 (SQL Pushdown Parity & Local Broker / Gemma Hardening)**: `OracleEmbeddingPushdown` rotates credentials strictly from an explicit `token_cache` (normalizing sync caches via `.to_async()`) and propagates errors [^py-oracle-pushdown]. `BigQueryEmbeddingPushdown`, `SpannerEmbeddingPushdown`, and `PostgresEmbeddingPushdown` expose uniform `embed_batch`, `top_k > 0`, boolean-element rejection, and exact `dimensions` validation, with `SpannerEmbeddingPushdown` binding `@task_type` and `@output_dimensionality` on `AI.GENERATE_EMBEDDING` [^py-spanner-pushdown]. `EmbeddingBroker` isolates `_pending`, `_flush_task`, and `_subprocess_lock` per running event loop and guarantees `/vtb-qvec-*` SHM cleanup on sidecar success and failure [^py-broker]; `GemmaEmbedder` validates `precision` in `{"int8", "q4", "bf16", "fp32"}` and forwards it through `BackendFactory.options` into `FastEmbedder.build_backend` [^py-local].
- **R13 (Docs & OKF Sync)**: User guides and OKF chapters match shipped behavior, trimming `embeddings-and-vectors.md` to `<= 200` lines [^okf-embeddings] and updating `embeddings-limitations.md` in place without duplicate limitation files [^okf-limitations].

## 3. Documentation & OKF Knowledge Plan

- `docs/usage/embeddings-and-vectors.md`, `docs/usage/storage-auth-and-signing.md`, `docs/reference/storage.md` (`ch04:t01`).
- `.agents/bundles/knowledge/domains/agent/embeddings-and-vectors.md` (`211L -> <= 200L`), `.agents/bundles/knowledge/domains/storage/object-storage.md`, `.agents/bundles/knowledge/architecture/worker/pyo3-ffi-and-batch-dispatch.md` (`ch04:t02`).
- `.agents/bundles/knowledge/domains/agent/embeddings-limitations.md` (updated in place) and `.agents/bundles/knowledge/domains/data/gcp-bigquery-and-spanner-drivers.md` (`<= 200L`, `ch04:t03`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Chapter / Task | Primary Files | Verification |
| --- | --- | --- | --- |
| `R1`–`R4` (ID token cache, user ADC, ADC order, PyO3/storage exports) | `ch01` (`t01`–`t04`) | `src/rs/vertebra-core/src/{gcp_identity,gcp_auth}.rs`, `src/rs/vertebra-py/src/gcp_identity.rs`, `src/py/vertebra/{_vertebra.pyi,storage/_store.py}` | `cargo test -p vertebra-core --test it gcp_auth::` & `pytest test_gcs_token_refresh.py` |
| `R5`–`R9`, `R11` (Gemini field-2/multimodal, Vertex REST, `_GoogleEmbedder`, `RemoteEmbedder`) | `ch02` (`t01`–`t04`) | `src/rs/vertebra-{core,py}/src/vector.rs`, `src/py/vertebra/embeddings/{_google,_models,_remote}.py` | `cargo test -p vertebra-core --lib vector::` & `pytest test_vector_wire_codecs.py test_google_embedders.py` |
| `R10`, `R12` (SQL pushdown parity, `EmbeddingBroker` per-loop state, `GemmaEmbedder` precision) | `ch03` (`t01`–`t04`) | `src/py/vertebra/db/adapters/{oracle,bigquery,spanner,postgres}/embeddings.py`, `src/py/vertebra/embeddings/{_broker,_local}.py` | `pytest test_embedding_broker.py test_google_embedders.py test_bigquery.py test_spanner.py` |
| `R13` (User docs, `embeddings-and-vectors.md <= 200L`, `embeddings-limitations.md` in-place sync) | `ch04` (`t01`–`t03`) | `docs/usage/*.md`, `docs/reference/storage.md`, `.agents/bundles/knowledge/**` | `make docs && make review-check` |

## 5. Implementation Plan

- [x] [b3253fee] [ch01-gcp-id-token-cache](chapters/ch01-gcp-id-token-cache/spec.md) — Audience-bound Rust & PyO3 `SyncGcpIdTokenCache` / `AsyncGcpIdTokenCache` (supporting GCE metadata, SA JWTs, IAM impersonation, `gcloud`, and `authorized_user` ADC), core ADC fallback, stubs, and `vertebra.storage` exports.
- [x] [d031307b] [ch02-vertex-and-gemini-embedder-wire-parity](chapters/ch02-vertex-and-gemini-embedder-wire-parity/spec.md) — Gemini protobuf field-2 + inline/file media wire codec, `_GoogleEmbedder` per-loop auth base, Vertex AI REST `:predict` / `:embedContent` + multimodal, and `RemoteEmbedder` ID-token auth.
- [x] [ef00252d] [ch03-pushdown-and-local-embedding-broker](chapters/ch03-pushdown-and-local-embedding-broker/spec.md) — Explicit Oracle credential rotation, BigQuery/Spanner/Postgres pushdown parity, per-event-loop `EmbeddingBroker` state + SHM cleanup, and `GemmaEmbedder` precision forwarding.
- [x] [01a74272] [ch04-docs-and-okf-sync](chapters/ch04-docs-and-okf-sync/spec.md) — Embeddings and GCP identity user docs, `embeddings-and-vectors.md` (`<= 200L`) + FFI/storage OKF sync, and in-place `embeddings-limitations.md` update.

## 6. Current State & Remaining Plan Snapshot

- **Active Frontier**: All `4` chapters (`15/15` tasks) completed through `ch04-docs-and-okf-sync` (`01a74272`); ready for `/flow:finish` contraction and archive.
- **Cross-Flow Unblock**: `ch01-gcp-id-token-cache` is complete on `feat/gcp-embeddings-and-identity`, ready to unblock `mcp-protocol-completeness` `ch06` bridge OIDC token authentication ([#113](https://github.com/cofin/vertebra/issues/113)).
- **Release Gate (`/flow:finish`)**: `make lint-all && make rs-test && make py-test && make docs && make review-check`.

[^core-gcp-auth]: `src/rs/vertebra-core/src/gcp_auth.rs::GcpTokenCache` — Native GCP access-token cache, credential source resolution, and HTTP clients.
[^py-gcp-auth]: `src/rs/vertebra-py/src/gcp_auth.rs::PySyncGcpTokenCache` — PyO3 `SyncGcpTokenCache`, `AsyncGcpTokenCache`, and `GcpTokenSnapshot` bindings.
[^core-vector-codec]: `src/rs/vertebra-core/src/vector.rs::encode_batch_embed_grpc_request` — Gemini `BatchEmbedContents` protobuf request encoder and response decoder.
[^py-google-embedders]: `src/py/vertebra/embeddings/_google.py::VertexEmbedder` — `VertexEmbedder` and `GeminiEmbedder` transports, token handling, and micro-batcher.
[^py-remote-embedder]: `src/py/vertebra/embeddings/_remote.py::RemoteEmbedder` — Cluster gRPC embedder client.
[^py-broker]: `src/py/vertebra/embeddings/_broker.py::EmbeddingBroker` — Local `EmbeddingBroker` batching queue and sidecar SHM transport.
[^py-local]: `src/py/vertebra/embeddings/_local.py::GemmaEmbedder` — Local `GemmaEmbedder` and `FastEmbedder` `BackendFactory` wiring.
[^py-oracle-pushdown]: `src/py/vertebra/db/adapters/oracle/embeddings.py::OracleEmbeddingPushdown` — Oracle 26ai `DBMS_VECTOR` pushdown and Vertex credential rotation.
[^py-spanner-pushdown]: `src/py/vertebra/db/adapters/spanner/embeddings.py::SpannerEmbeddingPushdown` — Spanner SQL embedding generation and vector search pushdown.
[^py-gcs-config]: `src/py/vertebra/storage/_store.py::GCSConfig` — GCS configuration and public GCP token-cache re-exports.
[^storage-signing]: `src/rs/vertebra-storage/src/signing.rs` — GCS V4 signing token-cache resolution.
[^okf-embeddings]: `.agents/bundles/knowledge/domains/agent/embeddings-and-vectors.md` — Embeddings and vector slabs OKF chapter.
[^okf-limitations]: `.agents/bundles/knowledge/domains/agent/embeddings-limitations.md` — Existing embeddings limitations record.
