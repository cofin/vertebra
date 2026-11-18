---
type: Gotcha
title: Embeddings Limitations
description: Verified runtime boundaries and non-goals for Google Cloud OIDC identity caches, remote gRPC embeddings, and Oracle SQL pushdown.
status: stable
tags:
- agent
- embeddings
- gcp-identity
- vertex-ai
- gemini
- limitations
generated:
  by: flow/0.28.0
  at: "2026-11-12T01:15:00Z"
verified:
- by: agent:flow-executor
  at: "2026-11-17T05:50:00Z"
  commit: 5a833832
- by: agent:flow-executor
  at: "2026-11-13T05:25:00Z"
  commit: 93d7fb1f
- by: agent:flow-sync
  at: "2026-11-12T01:15:00Z"
  commit: c8722c76
sources:
- id: core-gcp-identity
  resource: src/rs/vertebra-core/src/gcp_identity.rs
  title: Audience-scoped GCP OIDC ID token cache and credential paths
- id: google-embedder
  resource: src/py/vertebra/embeddings/_google.py
  title: VertexEmbedder REST and GeminiEmbedder gRPC embedders
- id: remote-embedder
  resource: src/py/vertebra/embeddings/_remote.py
  title: RemoteEmbedder optional id_token_cache bearer attachment
- id: jwks-verifier
  resource: src/py/vertebra/framework/security.py::JWKSVerifier
  title: Inbound HTTP and gRPC JWKS JWT verification with key rotation
- id: oracle-embeddings
  resource: src/py/vertebra/db/adapters/oracle/embeddings.py
  title: OracleEmbeddingPushdown optional token_cache and DBA credential boundary
---

# Embeddings Limitations

<!-- truth: start -->
- **`authorized_user` ADC OIDC audience boundary**: Google's OAuth token endpoint rejects custom `audience` targets on `authorized_user` refresh-token exchanges and returns an `id_token` whose `aud` claim equals the credential's own `client_id`. Callers targeting Cloud Run service URLs pass `target_service_account` for IAM Credentials `generateIdToken` impersonation, or add the `authorized_user` `client_id` to an IAP backend's `programmaticClients` allowlist [^core-gcp-identity].
- **Workload Identity Federation (`external_account`)**: `external_account` credential files are outside the supported credential sources for `SyncGcpTokenCache` and `SyncGcpIdTokenCache` and raise `GoogleCloudAuthError` (`GCP_AUTH_CONFIG_INVALID`) [^core-gcp-identity].
- **Inbound JWKS verification placement**: `SyncGcpIdTokenCache`, `AsyncGcpIdTokenCache`, and `RemoteEmbedder(id_token_cache=...)` mint and attach outbound `Authorization: Bearer` ID tokens, while inbound HTTP and gRPC Bearer JWT signature, `iss`, `aud`, and `exp` verification lives in Python via [`JWKSVerifier`](../../../../../src/py/vertebra/framework/security.py)[^jwks-verifier] (`GrpcServiceRegistry(jwks_verifier=...)` and framework guards) rather than inside Rust `vertebra-http` [^remote-embedder] [^jwks-verifier].
- **Oracle pushdown without explicit `token_cache`**: `OracleEmbeddingPushdown(..., token_cache=None)` omits OAuth token injection and requires DBA-configured database credentials such as `DBMS_VECTOR_CHAIN` credential objects [^oracle-embeddings].
- **Loopback CI verification**: Unit suites validate Vertex REST, Gemini multimodal gRPC protobuf frames, and OIDC token exchanges against in-process loopback HTTP/2 mock servers rather than live Google Cloud endpoints [^google-embedder].
<!-- truth: end -->

Current embedder architecture is documented in [Shared-Memory Vector Slabs, Zero-Weight Cloud & SQL Embeddings, and Local Broker](embeddings-and-vectors.md).

[^core-gcp-identity]: `src/rs/vertebra-core/src/gcp_identity.rs` — Audience-scoped GCP OIDC ID token cache and credential paths.
[^google-embedder]: `src/py/vertebra/embeddings/_google.py` — `VertexEmbedder` REST and `GeminiEmbedder` gRPC embedders.
[^remote-embedder]: `src/py/vertebra/embeddings/_remote.py` — `RemoteEmbedder` optional `id_token_cache` bearer attachment.
[^jwks-verifier]: `src/py/vertebra/framework/security.py::JWKSVerifier` — Inbound HTTP and gRPC JWKS JWT verification with key rotation.
[^oracle-embeddings]: `src/py/vertebra/db/adapters/oracle/embeddings.py` — `OracleEmbeddingPushdown` optional `token_cache` and DBA credential boundary.
