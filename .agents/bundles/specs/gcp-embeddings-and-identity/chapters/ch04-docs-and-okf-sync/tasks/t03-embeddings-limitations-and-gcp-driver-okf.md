---
type: Task
id: "gcp-embeddings-and-identity:ch04:t03"
flow_id: gcp-embeddings-and-identity
chapter_id: ch04-docs-and-okf-sync
title: "Update Existing embeddings-limitations.md In Place and Align GCP Driver ADC OKF"
description: "Rewrite .agents/bundles/knowledge/domains/agent/embeddings-limitations.md in place without creating duplicate limitation files, and update canonical ADC order in gcp-bigquery-and-spanner-drivers.md."
status: stable
state: completed
commit: 01a74272
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: documentation_validation
plan_commit: 18baadcd
depends_on:
  - "gcp-embeddings-and-identity:ch04:t02"
files:
  - .agents/bundles/knowledge/domains/agent/embeddings-limitations.md
  - .agents/bundles/knowledge/domains/data/gcp-bigquery-and-spanner-drivers.md
tests: []
read_set:
  - .agents/bundles/knowledge/domains/agent/embeddings-limitations.md
  - .agents/bundles/knowledge/domains/index.md
  - .agents/bundles/knowledge/domains/data/gcp-bigquery-and-spanner-drivers.md
  - src/rs/vertebra-core/src/gcp_identity.rs
  - src/rs/vertebra-core/src/gcp_auth.rs
estimated_loc: 95
tags:
  - okf
  - limitations
  - embeddings
  - gcp
sources:
  - id: okf-limitations
    resource: .agents/bundles/knowledge/domains/agent/embeddings-limitations.md
    title: "Existing embeddings limitations file indexed in domains/index.md and knowledge/index.md"
  - id: okf-gcp-drivers
    resource: .agents/bundles/knowledge/domains/data/gcp-bigquery-and-spanner-drivers.md
    title: "BigQuery and Spanner driver ADC precedence documentation"
---

# Task `t03`: Update Existing `embeddings-limitations.md` In Place and Align GCP Driver ADC OKF

## Objective & Scope Boundary

Update `.agents/bundles/knowledge/domains/agent/embeddings-limitations.md` in place (never creating a duplicate `embeddings-and-identity-limitations.md` file) and synchronize canonical ADC discovery order in `gcp-bigquery-and-spanner-drivers.md` (`<= 200L`) per [`../spec.md` §2](../spec.md) [^okf-limitations] [^okf-gcp-drivers].

## Bounded Read Manifest

- `.agents/bundles/knowledge/domains/agent/embeddings-limitations.md` — existing limitations record [^okf-limitations].
- `.agents/bundles/knowledge/domains/index.md` — existing `embeddings-limitations.md` index link.
- `.agents/bundles/knowledge/domains/data/gcp-bigquery-and-spanner-drivers.md` — scoped ADC description [^okf-gcp-drivers].
- `src/rs/vertebra-core/src/gcp_identity.rs` and `src/rs/vertebra-core/src/gcp_auth.rs` — verified identity and ADC boundaries.

## Contracts & Edge Cases

- Rewrite `embeddings-limitations.md` in present tense (`<= 200` lines, `<= 40`-line truth block, expanded YAML mappings, `verified` commit updated to an ancestor of `HEAD`): remove resolved Vertex gRPC / Gemini text-descriptor defects and record current verified boundaries (`authorized_user` ADC mints only its `client_id` audience without `target_service_account` impersonation or IAP `programmaticClients`; `external_account` Workload Identity Federation is unimplemented; inbound JWKS signature verification is out of scope of outbound `GcpIdTokenCache`; CI tests run against loopback HTTP/gRPC harnesses rather than live Google APIs; `OracleEmbeddingPushdown` leaves DBA-managed credentials untouched when `token_cache=None`).
- Update `gcp-bigquery-and-spanner-drivers.md` (`<= 200` lines) so its ADC precedence description reflects `GcpTokenCache::from_options` ending ambient discovery at `MetadataServer` with explicit-option isolation.

## Test Matrix

- Verify `.agents/bundles/knowledge/domains/agent/embeddings-and-identity-limitations.md` does not exist; verify both touched OKF files are `<= 200` lines; run `make docs && make review-check`.

## Verification

```bash
test ! -e .agents/bundles/knowledge/domains/agent/embeddings-and-identity-limitations.md && make docs && make review-check
```

## Acceptance Criteria

- [ ] `embeddings-limitations.md` is updated in place (`<= 200L`) with zero duplicate limitation files, and `gcp-bigquery-and-spanner-drivers.md` (`<= 200L`) matches core ADC order (`R13`).
- [ ] Zero regressions in `make docs && make review-check`.

[^okf-limitations]: `.agents/bundles/knowledge/domains/agent/embeddings-limitations.md` — Existing embeddings limitations file indexed in `domains/index.md` and `knowledge/index.md`.
[^okf-gcp-drivers]: `.agents/bundles/knowledge/domains/data/gcp-bigquery-and-spanner-drivers.md` — BigQuery and Spanner driver ADC precedence documentation.
