---
type: Task
id: "gcp-embeddings-and-identity:ch06:t03"
chapter_id: ch06-docs-okf-and-limitations
title: Record embedding and identity limitations and update the domain index and ADC statement
description: Create the embeddings and identity limitations record, link it from the domains index, and update the scoped ADC statement in the BigQuery and Spanner drivers chapter.
status: draft
state: pending
priority: P2
verification_strategy: documentation_validation
depends_on:
  - "gcp-embeddings-and-identity:ch06:t02"
read_set:
  - .agents/bundles/knowledge/domains/storage/object-storage-limitations.md
  - .agents/bundles/knowledge/domains/index.md
  - .agents/bundles/knowledge/domains/data/gcp-bigquery-and-spanner-drivers.md
  - src/rs/vertebra-core/src/gcp_identity.rs
  - src/py/vertebra/embeddings/_google.py
  - .agents/bundles/specs/gcp-embeddings-and-identity/spec.md
files:
  - .agents/bundles/knowledge/domains/agent/embeddings-and-identity-limitations.md
  - .agents/bundles/knowledge/domains/index.md
  - .agents/bundles/knowledge/domains/data/gcp-bigquery-and-spanner-drivers.md
tests: []
estimated_loc: 110
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
  - id: okf-limitations-precedent
    resource: .agents/bundles/knowledge/domains/storage/object-storage-limitations.md
    title: Limitations record format precedent
  - id: okf-domains-index
    resource: .agents/bundles/knowledge/domains/index.md
    title: Domains index with the agent section
  - id: okf-gcp-drivers
    resource: .agents/bundles/knowledge/domains/data/gcp-bigquery-and-spanner-drivers.md
    title: Scoped ADC and transport classification bullet
tags:
  - okf
  - limitations
---

# Task t03: Record Embedding and Identity Limitations and Update the Domain Index and ADC Statement

## Objective & Scope Boundary

1. Create `.agents/bundles/knowledge/domains/agent/embeddings-and-identity-limitations.md` with `type: Gotcha`, `status: stable`, `tags`, `generated`, `verified`, `sources` + footnotes, and a truth block following the precedent [^okf-limitations-precedent]. Required entries (each a single verified statement with a source):
   - Live Vertex AI, Gemini API, metadata-server, IAM Credentials, and gcloud paths are verified only against loopback servers and stub binaries; no CI job calls Google endpoints.
   - Request size limits for inline media are enforced by the APIs, not client-side; oversized requests surface as `EmbeddingError` with the response snippet.
   - Gemini Developer API multimodal accepts only `https://` file URIs (Files API); `gs://` requires `VertexEmbedder`.
   - Vertex multimodal requires a model routed to `:embedContent` (`gemini-embedding-2`); unknown models default to `:predict` with one instance per request.
   - `authorized_user` ADC mints ID tokens only for its OAuth `client_id` audience through the refresh-token grant; other audiences (including Cloud Run service URLs) require `target_service_account` impersonation or an IAP resource that lists the `client_id` in `programmaticClients`. Refresh tokens issued without the `openid` scope return no `id_token` and fail loud.
   - ID tokens are not signature-verified on receipt; only `aud` and `exp` claims are checked to guard the cache.
   - `GcpIdTokenCache` has no C-ABI capsule; other wheels cannot share it.
   - `external_account` (Workload Identity Federation) credentials are unsupported for both token kinds.
   - `OracleEmbeddingPushdown` performs no credential rotation without an explicit token cache.
   - The ambient metadata-server step makes no construction-time probe, so missing credentials off Google Cloud fail on first fetch.
2. Add the record to `.agents/bundles/knowledge/domains/index.md` under `## Agent Protocols & Harness (agent/)` with a one-line description [^okf-domains-index].
3. Update the `Scoped ADC & Transport Classification` bullet in `.agents/bundles/knowledge/domains/data/gcp-bigquery-and-spanner-drivers.md` to state the ADC order (explicit options, ambient ADC file, ambient token variables, metadata server) and that explicit credentials never fall through [^okf-gcp-drivers].

Out of scope: user docs (`t01`), other OKF chapters (`t02`).

## Bounded Read Manifest

- `.agents/bundles/knowledge/domains/storage/object-storage-limitations.md` (format).
- `.agents/bundles/knowledge/domains/index.md` (agent section).
- `.agents/bundles/knowledge/domains/data/gcp-bigquery-and-spanner-drivers.md` (`Scoped ADC & Transport Classification` bullet).
- `src/rs/vertebra-core/src/gcp_identity.rs`, `src/py/vertebra/embeddings/_google.py` (cited symbols).
- Root spec.

## Symbol & Signature Contracts

Sources in the new record cite at least `src/rs/vertebra-core/src/gcp_identity.rs::GcpIdTokenCache`, `src/rs/vertebra-core/src/gcp_auth.rs::GcpTokenCache`, `src/py/vertebra/embeddings/_google.py::VertexEmbedder`, `src/py/vertebra/embeddings/_google.py::GeminiEmbedder`, and `src/py/vertebra/db/adapters/oracle/embeddings.py::OracleEmbeddingPushdown`.

## Edge Cases & Error Semantics

- Entries describe verified current behavior only; no planned work or history.
- No numbered labels or finding identifiers.

## Public-Seam Test Blueprint

- **Gate**: `documentation_validation`.
- **Worked Check Matrix**:

| Case | Input | Expected |
| --- | --- | --- |
| Review manifest gate | `make review-check` | Exit `0` |
| Index link | `git grep -n "embeddings-and-identity-limitations.md" .agents/bundles/knowledge/domains/index.md` | One match |
| ADC order stated | `git grep -n "metadata server" .agents/bundles/knowledge/domains/data/gcp-bigquery-and-spanner-drivers.md` | At least one match |
| Symbol resolution | `git grep -n "GcpIdTokenCache" src/rs/vertebra-core/src/gcp_identity.rs` | Match |

## Verification

```bash
make review-check
git grep -n "embeddings-and-identity-limitations.md" .agents/bundles/knowledge/domains/index.md
git grep -n "metadata server" .agents/bundles/knowledge/domains/data/gcp-bigquery-and-spanner-drivers.md
```

Commit as `docs(okf): record GCP embedding and identity limitations`.

## Acceptance Criteria

- [ ] Limitations record lists every caveat above with sources (R12).
- [ ] Domains index links the record (R12).
- [ ] BigQuery/Spanner chapter states the ADC order (R12).

## Notes & Discoveries

- None yet.

[^okf-limitations-precedent]: `.agents/bundles/knowledge/domains/storage/object-storage-limitations.md` — Format precedent.
[^okf-domains-index]: `.agents/bundles/knowledge/domains/index.md` — Index updated here.
[^okf-gcp-drivers]: `.agents/bundles/knowledge/domains/data/gcp-bigquery-and-spanner-drivers.md` — ADC statement updated here.
