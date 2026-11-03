---
type: Task
id: gcp-wire-protocol-correctness:t06
title: Document official GCP clients, configuration, and TLS requirements
state: pending
priority: P2
verification_strategy: documentation_validation
depends_on: [gcp-wire-protocol-correctness:t05]
files:
  - .agents/bundles/product/tech-stack.md
  - .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
  - docs/usage/databases.md
  - docs/usage/packaging-and-deployment.md
  - docs/changelog/index.md
tests: []
plan_revision: 2
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
created_at: 2026-11-03T16:10:00Z
updated_at: 2026-11-03T16:10:00Z
commit: null
tags: [docs, gcp, spanner, bigquery]
---

# t06 — Document official GCP clients, configuration, and TLS requirements

## Objective

Contributor and user documentation describe the official Google Cloud Rust clients, the new Spanner configuration (`admin_endpoint`, `ca_pem` policy), `partitioned_query` / `fetch_arrow`, the BigQuery `ca_pem` scope, TLS trust requirements, and the 0.3.0 changelog entries.

## Context

- Spec R13 (`../spec.md`); research doc for facts (crate versions, `rust-version = "1.91.0"`, TLS roots, REST DDL).
- `documentation_validation`: `make docs` (docstring scan, CLI docs check, Zensical build, artifact verification) is the gate.
- Rules: user docs stay conceptual (no `make` commands); Mermaid diagrams in `docs/` keep 4–6 nodes; install commands use `pymdownx.tabbed` blocks; knowledge chapters keep their `<!-- truth: start -->` / `<!-- truth: end -->` blocks accurate; the changelog omits version-bump entries and uses exact class/function names. The changelog entries for unreleased work carry no PR links yet.

## Steps

1. **Baseline.** `make docs` → exit 0 before edits.
2. **`.agents/bundles/product/tech-stack.md`.**
   - Truth block, Rust bullet: state that the GCP clients require Rust ≥ 1.91 (`rust-version` of the `google-cloud-*` crates) and that the workspace still pins no toolchain.
   - Truth block, `vertebra-db` entry: add `google-cloud-spanner =0.34.8-preview`, `google-cloud-bigquery =0.19.0`, `google-cloud-auth 1.18`, `google-cloud-gax 1.15`, `google-cloud-longrunning 1.14` (all `default-features = false`), `tonic 0.14` (`tls-ring`, `tls-webpki-roots`), `futures 0.3`; note that `opentelemetry` crates come in transitively and no metrics exporter is enabled.
   - Add `vertebra_db::gcp` (credential bridge and ring provider install) to the crate description.
3. **`.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md`.** Replace statements about the hand-written Spanner/BigQuery gRPC wire code and `grpc.rs` with: Spanner and BigQuery Storage run on the official crates behind `SpannerClient` / `BigQueryClient`; tests use `Spanner::from_stub`, `DatabaseAdmin::from_stub`, `Read::from_stub`, and pure `ArrowAppendPlan` tests; DDL uses REST with the `admin_endpoint` rule; Spanner `ca_pem` → Omni TLS for non-googleapis endpoints, rejected for googleapis; BigQuery `ca_pem` REST-only; one record batch per `AppendRowsRequest`; dictionary batches rejected. Update the chapter's `sources:` list (drop `src/rs/vertebra-db/src/grpc.rs`, add `src/rs/vertebra-db/src/gcp.rs`).
4. **`docs/usage/databases.md`.** Under the native drivers section add a "Google Cloud Spanner and BigQuery" subsection: `fetch_arrow` runs a Data Boost partitioned query and falls back to a single-stream query when partitioning is not possible; `SpannerConfig.admin_endpoint` (default derivation for Google endpoints and the emulator's 9010 → 9020 REST port, required otherwise); `SpannerConfig.ca_pem` (Spanner Omni or private servers only); `BigQueryConfig.ca_pem` (REST API only; Storage Read/Write use the system trust store). Include one short Python example constructing `SpannerConfig(..., endpoint="http://localhost:9010")` for the emulator.
5. **`docs/usage/packaging-and-deployment.md`.** Add a "TLS trust store for Google Cloud clients" note: gRPC clients trust the OS store plus bundled Mozilla roots; Spanner DDL over REST uses the OS trust store, so container images need a CA bundle (for example `ca-certificates`); proxy variables `HTTPS_PROXY` / `NO_PROXY` apply to REST calls.
6. **`docs/changelog/index.md` 0.3.0.**
   - `### Changed`: "`vertebra-db` Spanner and BigQuery Storage clients run on the official `google-cloud-spanner` and `google-cloud-bigquery` crates; the hand-written gRPC transport is removed." and "`SpannerClient.execute_partitioned_arrow` and `execute_partitioned_arrow_async` are replaced by `partitioned_query` and `partitioned_query_async`, which return rows; `SyncSpannerDriver.fetch_arrow` / `AsyncSpannerDriver.fetch_arrow` build the `ArrowCursor`." and "Spanner query results no longer include `transaction_id` or `read_timestamp_micros`."
   - `### Added`: "`SpannerConfig.admin_endpoint` and `SpannerClient(admin_endpoint=...)` select the REST endpoint for DDL."
   - `### Fixed`: commits without a commit timestamp, DDL operations that are still running, and BigQuery appends with missing or mismatched responses now raise errors instead of reporting success; malformed Arrow input to `BigQueryClient.append_arrow` raises an error instead of panicking; Spanner and BigQuery Storage requests use the correct protobuf field numbers.
7. **Verify.**

## Verification

- Initial: `make docs` → exit 0 (baseline).
- Final (each exits 0): `make docs`; `make lint-all`; `! grep -rnE "grpc\.rs|execute_partitioned_arrow" docs .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md .agents/bundles/product --exclude-dir=changelog`. The gate excludes `docs/changelog/` because this task's changelog entry names the removed module, and it does not scan `.agents/bundles/knowledge/architecture/http/` or `.agents/bundles/knowledge/domains/{extensions,agent}/`, whose `grpc.rs` references point at the unrelated `src/rs/vertebra-http/src/grpc.rs`.
- Commit: `git commit -S -m "docs: describe official Google Cloud clients, Spanner admin endpoint, and TLS trust requirements"`.

## Acceptance Criteria

- [ ] R13: tech stack, knowledge chapter, usage docs, deployment docs, and changelog describe the change; `make docs` passes.

## Notes & Discoveries
