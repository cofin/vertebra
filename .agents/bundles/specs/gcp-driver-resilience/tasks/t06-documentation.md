---
type: Task
id: gcp-driver-resilience:t06
title: Document typed errors, completion safety, and auth behavior
state: pending
priority: P2
verification_strategy: documentation_validation
depends_on: [gcp-driver-resilience:t04]
files:
  - docs/usage/databases.md
  - docs/usage/storage-auth-and-signing.md
  - docs/changelog/index.md
  - .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
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
tags: [docs, exceptions, auth]
---

# t06 — Document typed errors, completion safety, and auth behavior

## Objective

User and contributor docs describe the Google Cloud exception hierarchy, `TaskPanicError` and cancellation semantics, GCP auth proxy/scope/gcloud behavior, REST warning handling, the GoogleSQL placeholder rules, and the 0.3.0 changelog entries.

## Context

- Spec R11 (`../spec.md`).
- `documentation_validation`: `make docs` is the gate.
- Rules: user docs stay conceptual (no `make` commands); changelog entries use exact names and carry no PR links for unreleased work; knowledge truth blocks stay accurate.

## Steps

1. **Baseline.** `make docs` → exit 0.
2. **`docs/usage/databases.md`.** Add an "Error handling for Google Cloud drivers" subsection: `GoogleCloudError` (subclass of `VertebraError`) with `GoogleCloudAuthError`, `SpannerError` / `SpannerAbortedError`, `BigQueryError` / `BigQueryRowError`; attributes `code`, `code_name`, `message`, `http_status`, `retry_delay_ms`, `reason`, `row_errors`; a short example catching `SpannerAbortedError`; note that sync and async methods raise the same classes. Add a sentence that cancelling an awaited driver call stops the Rust task at its next suspension point, that RPCs already sent may still complete, and that a Rust panic in debug builds surfaces as `TaskPanicError`. Add the GoogleSQL rule: `$1..$N` placeholders are rewritten to `@p1..@pN` outside string literals, backtick identifiers, and comments.
3. **`docs/usage/storage-auth-and-signing.md`.** Document: token and IAM requests honor `HTTPS_PROXY` / `NO_PROXY`, metadata-server requests never use a proxy; `GCLOUD_BIN` selects the gcloud binary and gcloud calls time out after 30 s; each client requests its own OAuth scopes (Spanner, BigQuery, GCS, or cloud-platform for `GcpTokenCache`), overridable with the `scopes` option.
4. **`.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md`.** Add the exception mapping (Rust error variant → Python class and code), `ProxyPolicy`, `from_options(opts, default_scopes)`, and the completion-bridge panic/cancel contract with the `panic = "abort"` release caveat.
5. **`docs/changelog/index.md` 0.3.0.**
   - `### Added`: "`GoogleCloudError`, `GoogleCloudAuthError`, `SpannerError`, `SpannerAbortedError`, `BigQueryError`, and `BigQueryRowError` in `vertebra._vertebra`, carrying the canonical gRPC status." and "`TaskPanicError` for panics in background Rust tasks."
   - `### Changed`: "`GcpTokenCache::from_options` takes the default OAuth scopes from the caller; `from_gcs_options` is removed." and "Google Cloud token requests honor proxy environment variables."
   - `### Fixed`: cancelled async driver calls stop their Rust task; panics no longer leave futures pending; `gcloud` token fetches no longer block the async runtime; BigQuery queries that report only warnings succeed; BigQuery result URLs percent-encode job id, location, and page token; positional placeholder rewriting preserves non-ASCII text and GoogleSQL literals; Spanner and BigQuery fallbacks use status codes instead of message text.
6. **Verify.**

## Verification

- Initial: `make docs` → exit 0.
- Final (each exits 0): `make docs`; `make lint-all`; `! grep -rn "from_gcs_options" docs .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md --exclude-dir=changelog`. The gate excludes `docs/changelog/` because this task's changelog entry names the removed constructor.
- Commit: `git commit -S -m "docs: describe Google Cloud exception hierarchy, task cancellation, and auth behavior"`.

## Acceptance Criteria

- [ ] R11: usage docs, knowledge chapter, and changelog describe the changes; `make docs` passes.

## Notes & Discoveries
