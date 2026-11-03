---
type: Spec
flow_id: gcp-driver-resilience
title: GCP Driver Resilience and Typed Errors
description: Make the PyO3 completion bridge panic-safe and cancellable, raise one typed Google Cloud exception hierarchy (rooted at VertebraError) from sync and async bindings, fix GCP auth (async gcloud, proxy policy, explicit scopes), fix BigQuery REST error and URL handling, lex GoogleSQL correctly in the placeholder rewriter, and document the result.
state: active
parent_prd: integration-remediation
research:
  - ../integration-remediation/research/google-cloud-rust_20261103.md
  - ../integration-remediation/research/audit-findings_20261103.md
plan_revision: 2
plan_commit: 24e2931
state_revision: 5
current_task: gcp-driver-resilience:t04
last_verified_checkpoint: 0ec5f3b
created_at: 2026-11-03T15:30:00Z
updated_at: 2026-11-03T20:41:00Z
tags: [rust, gcp, pyo3, python, exceptions, auth, bugfix]
---

# Flow: GCP Driver Resilience and Typed Errors

## 1. Specification & Context

### 1.1 Problem Statement & Objectives

After `gcp-wire-protocol-correctness` moves Spanner and BigQuery Storage onto the official crates, these defects remain in code Vertebra owns (audit sections 4.2, 4.3, 4.6):

- `vertebra-py/src/completion.rs::spawn_rust_db_task` does not catch panics, so the Python future never resolves; it has no cancellation, so a cancelled awaitable still runs its Rust task. `_vertebra.pyi` declares `RustDbTaskAwaitable.__new__` without a Rust `#[new]`.
- Sync bindings raise `RuntimeError(str(e))`, async bindings may raise `ValueError`, no gRPC code reaches Python, and the Python drivers detect errors by substring (`SpannerCore.is_partitioned_arrow_fallback_error`, `"aborted" in str(exc)` in `db/spanner/driver.py`, `BigQueryCore.is_storage_api_fallback_error`).
- `vertebra-core/src/gcp_auth.rs::fetch_token_from_gcloud_cli` runs a blocking `std::process::Command::output()` inside async code; `GcpTokenCache::new` disables proxies for every request; `from_options` falls back to a GCS scope for impersonation; `from_gcs_options` is an alias.
- `bigquery.rs::check_rest_error` treats `errors[]` warnings as fatal; `build_get_query_results_url` does not encode its components.
- `spanner.rs::rewrite_spangres_placeholders` pushes bytes as `char` inside literals and comments and ignores GoogleSQL escapes, backticks, triple-quoted and raw strings, and `#` comments, although it only runs on GoogleSQL text.

Non-goals: module splits and shared retry/status helpers (`runtime-structure-and-throughput`); unifying the two Python `GcpTokenCache` classes; id generation fixes; Python `SpannerTransaction.execute` semantics (PRD backlog); emulator suites.

### 1.2 Upstream Execution Baseline

- Branch: `fix/gcp-driver-resilience` created from `main` after the `gcp-wire-protocol-correctness` PR merges.
- Merge order: `critical-correctness-fixes`, then `gcp-wire-protocol-correctness`, then this flow; rebase on `main` before opening the PR.
- Parent PRD: [integration-remediation](../integration-remediation/spec.md), Chapter 9.
- Prerequisite: `gcp-wire-protocol-correctness` completed (final `SpannerError` / `BigQueryError` variants, `vertebra_db::gcp`).

### 1.3 Code Analysis & Seam Summary

| Area | Files Examined (`path/to/file::Symbol`) | Public Seam, Dependency Category & Integration |
| --- | --- | --- |
| Completion bridge | `src/rs/vertebra-py/src/completion.rs::{spawn_rust_db_task, DbCompletionDrainer, RustDbTaskAwaitable, DbOutcomeProducer}` | In-process; inline `#[cfg(test)]` tests drive a real asyncio loop through `pyo3` `auto-initialize`. |
| Exception mapping | `src/rs/vertebra-py/src/{spanner.rs::spanner_err_to_py, bigquery.rs::bq_err_to_py, gcp_auth.rs, lib.rs}`, `src/rs/vertebra-db/src/{spanner.rs::SpannerError, bigquery.rs::BigQueryError}`, `src/py/vertebra/_typing.py::VertebraError` | New `vertebra-py/src/errors.rs` creates the hierarchy with `PyErr::new_type` on top of `VertebraError` and registers it in `vertebra._vertebra`. |
| Python drivers | `src/py/vertebra/db/spanner/core.py::SpannerCore.is_partitioned_arrow_fallback_error`, `src/py/vertebra/db/spanner/driver.py::{SyncSpannerDriver.run_in_transaction, AsyncSpannerDriver.run_in_transaction, *.fetch_arrow}`, `src/py/vertebra/db/bigquery/core.py::BigQueryCore.is_storage_api_fallback_error` | Code checks on `GoogleCloudError.code`. |
| Auth | `src/rs/vertebra-core/src/gcp_auth.rs::{fetch_token_from_gcloud_cli, GcpAuthSource::GcloudCli, GcpTokenCache::{new, from_options, from_gcs_options}}`, callers in `vertebra-db`, `vertebra-storage/src/{store.rs,lib.rs,signing.rs,gcp_auth.rs}`, `vertebra-py/src/gcp_auth.rs` | Remote-owned token endpoints; tests use local TCP mocks and explicit constructor parameters (gcloud binary, timeout, proxy policy); no environment mutation. |
| BigQuery REST | `src/rs/vertebra-db/src/bigquery.rs::{check_rest_error, build_get_query_results_url, send_rest_json, collect_query_response_pages, execute_jobs_insert_query}` | External boundary; local HTTP mocks. |
| Placeholder rewriter | `src/rs/vertebra-db/src/spanner.rs::rewrite_spangres_placeholders` | In-process pure function. |
| Docs | `docs/usage/{databases.md,storage-auth-and-signing.md}`, `docs/changelog/index.md`, `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` | Documentation updated in t06. |

### 1.4 Relevant Patterns, Domain Terms & Knowledge Chapters

- [Product guidelines](../../product/product-guidelines.md) (`VertebraError` is the package exception root), [PyO3 FFI & GIL discipline](../../knowledge/patterns/rust/ffi-and-gil-discipline.md), [PyO3 gotchas](../../knowledge/gotchas/pyo3-and-python-runtime.md), [polyglot test matrix](../../knowledge/testing/polyglot-test-matrix.md), [workflow](../../knowledge/workflow.md).
- Skills: `.agents/skills/pyo3-bridge/SKILL.md`, `.agents/skills/review-pyo3-bridge/SKILL.md`, `.agents/skills/python-framework/SKILL.md`.
- Terms: **canonical code** = the `google.rpc.Code` number (0–16); **completion ring** = `CompletionQueue` drained on the event loop by `_DbCompletionDrainer`; **proxy policy** = how a `GcpTokenCache` routes token/IAM requests (`Environment`, `Direct`, `Explicit`).

## 2. Requirements & Architecture

### 2.1 Functional Requirements

- **R1**: A panic in a task spawned by `spawn_rust_db_task`, or in its `map_ok` closure, resolves the Python future with `vertebra._vertebra.TaskPanicError` carrying the panic message.
- **R2**: Cancelling the awaiting coroutine (or the future) aborts the Tokio task at its next `.await`; RPCs already on the wire may still complete server-side. `_vertebra.pyi::RustDbTaskAwaitable` declares no `__new__`.
- **R3**: `gcloud` token fetches use `tokio::process::Command` with `kill_on_drop(true)` and a timeout; `GcpAuthSource::GcloudCli` carries `binary: PathBuf` and `timeout: Duration` (defaults: `GCLOUD_BIN` or `gcloud`, 30 s).
- **R4**: `GcpTokenCache` takes a `ProxyPolicy` (`Environment` default, `Direct`, `Explicit(reqwest::Proxy)`) for token and IAM requests; metadata-server requests never use a proxy.
- **R5**: `GcpTokenCache::from_options(opts, default_scopes)` takes default scopes from the caller with no GCS default; `from_gcs_options` is removed; callers pass `SPANNER_OAUTH_SCOPES`, `BIGQUERY_OAUTH_SCOPES`, `GCS_OAUTH_SCOPES`, or `DEFAULT_GCP_SCOPES`.
- **R6**: `check_rest_error` fails only on a top-level `error` object or `status.errorResult`; REST API errors carry the canonical status name and first `errors[].reason`.
- **R7**: `build_get_query_results_url` builds the URL with `url::Url` and percent-encodes path segments and query values at all three call sites.
- **R8**: Sync and async Spanner, BigQuery, and `GcpTokenCache` bindings raise `GoogleCloudError` (subclass of `vertebra.typing.VertebraError`) → {`GoogleCloudAuthError`, `SpannerError` → `SpannerAbortedError`, `BigQueryError` → `BigQueryRowError`} with attributes `code`, `code_name`, `message`, `http_status`, `retry_delay_ms`, `reason` (`row_errors` on `BigQueryRowError`).
- **R9**: The Python drivers use code checks: `SpannerCore.is_partitioned_query_fallback_error` (codes 3, 7, 9, 12, 14), `BigQueryCore.is_storage_api_fallback_error` (codes 7, 9, 12, 14), and `run_in_transaction` retries only `SpannerAbortedError`.
- **R10**: `rewrite_spangres_placeholders` copies text by `char`, leaves `$N` untouched inside GoogleSQL literals (single, double, triple-quoted, raw, bytes, with backslash escapes), backtick identifiers, and `--`, `#`, `/* */` comments, and rewrites `$N` elsewhere to `@pN`.
- **R11**: Documentation covers the exception hierarchy, `TaskPanicError`, proxy and scope behavior, REST warnings, and the 0.3.0 changelog.

### 2.2 Public Interfaces, Deep Modules & Seam Contracts

`_vertebra.pyi` additions:

```python
from vertebra.typing import VertebraError

class TaskPanicError(RuntimeError): ...
class GoogleCloudError(VertebraError):
    code: int
    code_name: str
    message: str
    http_status: int | None
    retry_delay_ms: int | None
    reason: str | None
class GoogleCloudAuthError(GoogleCloudError): ...
class SpannerError(GoogleCloudError): ...
class SpannerAbortedError(SpannerError): ...
class BigQueryError(GoogleCloudError): ...
class BigQueryRowError(BigQueryError):
    row_errors: list[tuple[int, int, str]]
```

Rust: `vertebra_py::errors::{exception_types, spanner_error_to_py, bigquery_error_to_py, auth_error_to_py}` (mappers are `fn(Python<'_>, E) -> PyErr`); each public mapper resolves the cached `exception_types(py)` and delegates to a private `*_with(py, &GcpExceptionTypes, E)` variant, and the uncached `exception_types_with_base(py, base)` builds the hierarchy on any base class so Rust tests use a stand-in base instead of importing `vertebra`; `spawn_rust_db_task`'s `map_err` becomes `FnOnce(Python<'_>, E) -> PyErr`. `vertebra_core::gcp_auth::{ProxyPolicy, GcpTokenCache::{new, with_proxy_policy, from_options(raw_opts, default_scopes)}, GcpAuthSource::GcloudCli { binary, timeout, quota_project_id, impersonate_service_account }, GCLOUD_CLI_TIMEOUT, default_gcloud_binary}`.

### 2.3 Non-Functional & Safety Constraints

- No `Python::attach` on Tokio worker threads; exception instances are created on the event-loop thread (drainer) or inside the sync call.
- Release builds use `panic = "abort"`; R1 applies to unwinding builds (`make dev`, tests). Documented in `completion.rs` and the docs.
- Tests never mutate process environment variables; behavior that depends on `GCLOUD_BIN`, proxies, or timeouts is injected through constructors.
- `vertebra._vertebra` imports `vertebra._typing` (stdlib + `msgspec` only) at module initialization to obtain `VertebraError`.
- Python: no inline comments, Google docstrings, top-level imports, `pytest.mark.parametrize`.

### 2.4 Acceptance Scenarios (Given / When / Then)

- Given a spawned future that panics with `"boom"`, when Python awaits it, then `TaskPanicError("Rust task panicked: boom")` is raised.
- Given a spawned future that sleeps 5 s, when the awaiting task is cancelled after 50 ms, then the future is dropped within 1 s and never completes.
- Given `SpannerClient.query("SELECT 1", "not json")` and `await SpannerClient.query_async("SELECT 1", "not json")`, then both raise `SpannerError` with `code == 3`, `code_name == "INVALID_ARGUMENT"`, and `isinstance(exc, VertebraError)`.
- Given a gax error with status `ABORTED` and `RetryInfo` 250 ms, then the Python exception is `SpannerAbortedError` with `code == 10` and `retry_delay_ms == 250`.
- Given a query response with `errors: [{"reason": "warning"}]` and no `status.errorResult`, then rows are returned.
- Given job id `job/1`, location `asia northeast1`, and page token `a+b/c==`, then the results URL encodes all three.
- Given `SELECT 'é$1', ..., WHERE x = $7` with every GoogleSQL literal and comment form, then only `$7` becomes `@p7` and non-ASCII text is preserved.
- Given impersonation options without `scopes` and `default_scopes = &["…/spanner.data"]`, then the IAM request lists only that scope.
- Given `ProxyPolicy::Explicit(proxy)`, then token requests go through the proxy while metadata requests go direct.
- Given `GcloudCli { binary: <script sleeping 60 s>, timeout: 200 ms }`, then the fetch returns a timeout error and the runtime keeps scheduling other tasks.

### 2.5 Alternatives Considered

- **`create_exception!` types rooted at `PyException`.** Simple, but violates the product rule that `VertebraError` is the package's exception root (`except VertebraError` would miss GCP errors). Rejected.
- **Python-defined exceptions raised via `import_exception!` from `vertebra.db`.** Gives Python `__init__` signatures, but every error path would import `vertebra.db`, which eagerly imports all drivers and `_vertebra`. Rejected.
- **Chosen: `PyErr::new_type` at module init** with base `vertebra._typing.VertebraError` (light import), stored in a `PyOnceLock`, registered on `vertebra._vertebra`; attributes are set on each instance and declared in the stub (stubtest does not require instance attributes on the class).
- **`#[pyclass(extends = PyException)]` with getters.** Cannot extend a Python-defined base and needs initializer plumbing per raise. Rejected.
- **`catch_unwind` around the spawned future.** No abort handle; the worker + completion task pair gives both panic detection and `AbortHandle`.
- **Environment variables to steer gcloud/proxy in tests.** Racy across parallel tests and needs `unsafe` `set_var`. Rejected for constructor injection.
- **Treating REST `errors[]` as fatal.** Fails successful queries with warnings. Rejected.

## 3. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task(s) | Verification Strategy | Confirmed Public Seam & Canonical Test Surface |
| --- | --- | --- | --- |
| R1, R2 | `t01` | `regression_tdd` | `src/rs/vertebra-py/src/completion.rs` `tests::{panicking_task_raises_task_panic_error, panicking_map_ok_raises_task_panic_error, cancelled_await_aborts_rust_task}`; stubtest |
| R3, R4, R5 | `t02` | `regression_tdd` | `src/rs/vertebra-core/tests/it/gcp_auth.rs::{gcloud_cli_token_fetch_is_async_and_times_out, explicit_proxy_routes_token_requests_and_metadata_bypasses_it, impersonation_defaults_to_caller_scopes, metadata_server_and_impersonation_custom_scopes}` |
| R6, R7 | `t03` | `regression_tdd` | `src/rs/vertebra-db/tests/bigquery_wire_tests.rs::{query_ignores_errors_warnings_without_error_result, query_fails_on_status_error_result, query_maps_http_error_status_and_reason, get_query_results_url_encodes_job_location_and_page_token}` |
| R8, R9 | `t04` | `behavior_tdd` | `src/rs/vertebra-py/src/errors.rs::tests::{hierarchy_derives_from_stand_in_base, aborted_status_maps_to_spanner_aborted_error, rest_api_error_prefers_canonical_status, rest_api_error_maps_http_status, row_errors_map_to_bigquery_row_error, ddl_timeout_maps_to_deadline_exceeded}`; `src/py/tests/unit/db/test_spanner.py::{test_google_cloud_error_hierarchy_derives_from_vertebra_error, test_spanner_client_invalid_params_raise_typed_error, test_spanner_client_invalid_params_raise_typed_error_async, test_partitioned_query_fallback_uses_status_codes, test_run_in_transaction_retries_only_aborted}`; `src/py/tests/unit/db/test_bigquery.py::{test_bigquery_client_invalid_params_raise_typed_error, test_storage_api_fallback_uses_status_codes}` |
| R10 | `t05` | `regression_tdd` | `src/rs/vertebra-db/tests/spanner_wire_tests.rs::spangres_placeholder_rewriter_lexes_googlesql_literals_and_comments` |
| R11 | `t06` | `documentation_validation` | `make docs` |

## 4. Implementation Plan

### Phase 1: Runtime safety and auth

- [x] t01 — Panic-safe, cancellable completion bridge (tasks/t01-completion-bridge.md)
- [x] t02 — Async gcloud fetch, proxy policy, explicit OAuth scopes (tasks/t02-gcp-auth.md)
- **Phase 1 Recovery Checkpoint**: `make lint && cd src/rs && cargo test -p vertebra-py && cargo test -p vertebra-core --features gcp-auth --test it && cargo test -p vertebra-storage && cd ../.. && make dev && uv run python -m mypy.stubtest vertebra._vertebra`

### Phase 2: Errors and SQL

- [x] t03 — BigQuery REST warnings and URL encoding (tasks/t03-bigquery-rest.md)
- [x] t05 — Char-aware GoogleSQL placeholder lexer (tasks/t05-googlesql-placeholders.md)
- [~] t04 — Typed Google Cloud exception hierarchy across bindings and drivers (tasks/t04-typed-exceptions.md)
- **Phase 2 Recovery Checkpoint**: `make lint-all && make rs-test && make py-test`

### Phase 3: Documentation

- [ ] t06 — Document typed errors, completion safety, and auth behavior (tasks/t06-documentation.md)
- **Phase 3 Recovery Checkpoint**: `make docs && make lint-all`

## 5. Continuity Snapshot

- **Flow ID**: `gcp-driver-resilience` (`state: planned`, `plan_revision: 2`, `plan_commit: 24e2931`)
- **Current Task**: `null`
- **Last Verified Checkpoint**: `null`
- **Key Decisions**:
  - Exceptions are created with `PyErr::new_type` on top of `VertebraError` and registered on `vertebra._vertebra`.
  - Completion bridge uses a worker task, a completion task, an `AbortHandle`, and a future done-callback.
  - Auth behavior that tests vary (gcloud binary, timeout, proxy) is injected through constructors.
  - REST failure signal is `error` or `status.errorResult` only.
  - Task order: t01, t02, t03, t05, t04 (t03 and t05 follow t02 because all three edit `bigquery.rs` / `spanner.rs`).
- **Recent Discoveries**:
  - Release profile aborts on panic.
  - `vertebra._typing` imports only stdlib and `msgspec`, so `_vertebra` can import it during initialization.
