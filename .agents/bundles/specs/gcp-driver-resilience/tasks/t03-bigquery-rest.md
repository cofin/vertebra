---
type: Task
id: gcp-driver-resilience:t03
title: BigQuery REST warnings and URL encoding
state: in_progress
priority: P2
verification_strategy: regression_tdd
depends_on: [gcp-driver-resilience:t02]
files:
  - src/rs/vertebra-db/src/bigquery.rs
tests:
  - src/rs/vertebra-db/tests/bigquery_wire_tests.rs
plan_revision: 2
plan_commit: 24e2931
state_revision: 1
claimed_by: executor
claimed_at: 2026-11-03T20:29:36Z
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: 2026-11-03T15:30:00Z
updated_at: 2026-11-03T16:10:00Z
commit: null
tags: [rust, gcp, bigquery, rest, bugfix]
---

# t03 — BigQuery REST warnings and URL encoding

## Objective

`check_rest_error` stops failing queries that only report `errors[]` warnings, REST API errors carry the canonical status name and first reason, and `build_get_query_results_url` percent-encodes its components with `url::Url`.

## Context

- Spec R6, R7 (`../spec.md`).
- `regression_tdd`: the existing REST mock pattern in `src/rs/vertebra-db/tests/bigquery_wire_tests.rs::test_bigquery_rest_fastpath_parameters_and_nested_row_decoding` (local HTTP server; `cfg.rest_endpoint = format!("http://{addr}")`) drives the public `BigQueryClient::query_with_options`.
- BigQuery REST semantics: `QueryResponse.errors` holds "errors or warnings"; failure is a non-2xx response with an `error` object (`code`, `message`, `status` canonical name, `errors[].reason`) or a `Job.status.errorResult` (`reason`, `message`).
- `BigQueryError::Api { status: u16, message: String }` is matched with `..` at `execute_jobs_insert_query` (`status: 404`).
- `url = { workspace = true }` is already a `vertebra-db` dependency.

## Steps

1. **Write failing tests (RED)** in `bigquery_wire_tests.rs` (one local HTTP mock helper `async fn serve_json_routes(routes: Vec<(&'static str, &'static str, u16, Value)>) -> (SocketAddr, Arc<Mutex<Vec<String>>>)` that matches `METHOD path-prefix`, records the full request target (path + query), and replies with the status and JSON):
   - `query_ignores_errors_warnings_without_error_result`: `POST /bigquery/v2/projects/p1/queries` → `200 {"jobComplete": true, "jobReference": {"jobId": "j1", "location": "US"}, "schema": {"fields": [{"name": "n", "type": "INT64", "mode": "NULLABLE"}]}, "rows": [{"f": [{"v": "1"}]}], "totalRows": "1", "errors": [{"reason": "warning", "message": "slot contention"}]}` → `query_with_options("SELECT 1", &[], &BigQueryQueryOptions::default())` returns one row `[BigQueryCellValue::Int64(1)]`.
   - `query_fails_on_status_error_result`: `destination_table = Some("d1.t2")` (forces `jobs.insert`); `POST /bigquery/v2/projects/p1/jobs` → `200 {"status": {"state": "DONE", "errorResult": {"reason": "invalidQuery", "message": "Syntax error at [1:1]"}}}` → `Err(BigQueryError::Api { status: 400, message, reason: Some(r), canonical_status: None })` with `message == "Syntax error at [1:1]"` and `r == "invalidQuery"`.
   - `query_maps_http_error_status_and_reason`: `POST .../queries` → `403 {"error": {"code": 403, "message": "Access Denied", "status": "PERMISSION_DENIED", "errors": [{"reason": "accessDenied", "message": "Access Denied"}]}}` → `Api { status: 403, canonical_status: Some("PERMISSION_DENIED"), reason: Some("accessDenied"), .. }`.
   - `get_query_results_url_encodes_job_location_and_page_token`: `POST .../queries` → `{"jobComplete": false, "jobReference": {"jobId": "job/1", "location": "asia northeast1"}}`; `GET .../queries/` → first call `{"jobComplete": true, "jobReference": {...}, "schema": ..., "rows": [...], "pageToken": "a+b/c=="}`, second call the last page without `pageToken`. Assert the recorded targets include `/bigquery/v2/projects/p1/queries/job%2F1?formatOptions.useInt64Timestamp=true&location=asia+northeast1` and a later target containing `pageToken=a%2Bb%2Fc%3D%3D`. (`url::form_urlencoded` encodes a space as `+`.)
   - RED: `cd src/rs && cargo test -p vertebra-db --test bigquery_wire_tests query_` → compile errors (`reason`, `canonical_status`) and the warning test failing.

2. **Implement (GREEN) in `bigquery.rs`.**
   - `BigQueryError::Api { status: u16, message: String, reason: Option<String>, canonical_status: Option<String> }` with `#[error("BigQuery REST API error (HTTP {status}): {message}")]`.
   - Private `fn rest_error_details(error: &Value) -> (Option<String>, Option<String>)` returning (`error.status` as canonical name, `error.errors[0].reason`).
   - `send_rest_json`: on non-2xx parse the body; build `Api { status, message, reason, canonical_status }` from the `error` object (message falls back to the raw text).
   - `check_rest_error`: `error` object → `Api { status: error.code or 500, message, reason, canonical_status }`; `status.errorResult` → `Api { status: 400, message, reason: errorResult.reason, canonical_status: None }`; delete the `errors[]` branch.
   - `build_get_query_results_url(&self, job_id, location, page_token) -> Result<String, BigQueryError>`: parse `self.config.effective_rest_endpoint()` with `url::Url::parse` (error → `InvalidArgument`); `path_segments_mut()` (error → `InvalidArgument("REST endpoint cannot be a base URL")`) extended with `["bigquery", "v2", "projects", project_id, "queries", job_id]`; `query_pairs_mut()` appends `formatOptions.useInt64Timestamp=true`, then `location` and `pageToken` when non-empty; return `url.to_string()`. Update all three callers to propagate the error with `?`: `execute_jobs_insert_query` (results URL after `jobs.insert`), and in `collect_query_response_pages` both the `jobComplete == false` poll URL and the `pageToken` page URL.

3. **Refactor & verify.**

## Verification

- RED: `cd src/rs && cargo test -p vertebra-db --test bigquery_wire_tests query_` → compile errors / failure; `cd src/rs && cargo test -p vertebra-db --test bigquery_wire_tests get_query_results_url` → assertion failure on the encoded target.
- GREEN (each exits 0):
  - `cd src/rs && cargo test -p vertebra-db --test bigquery_wire_tests`
  - `cd src/rs && cargo clippy -p vertebra-db -p vertebra-py --all-targets -- -D warnings && cargo fmt --check`
  - `make dev && uv run pytest src/py/tests/unit/db/test_bigquery.py -q`
  - `! grep -n 'format!("{}/bigquery/v2/projects/{}/queries/{}' src/rs/vertebra-db/src/bigquery.rs`
- Commit: `git commit -S -m "fix(rs-db): ignore BigQuery REST warnings and percent-encode query result URLs"`.

## Acceptance Criteria

- [ ] R6: warnings no longer fail queries; `error` and `status.errorResult` fail with reason and canonical status.
- [ ] R7: job id, location, and page token are percent-encoded.

## Notes & Discoveries
