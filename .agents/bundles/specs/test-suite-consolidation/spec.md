---
type: Spec
flow_id: test-suite-consolidation
title: Test Suite Consolidation and Coverage Gate
state: closed
plan_revision: 2
plan_commit: ""
state_revision: 1
current_task: null
last_operation: finish
operation_targets: []
last_verified_checkpoint: 2026-10-22T17:45:00Z
created_at: 2026-10-22T03:30:00Z
updated_at: 2026-10-22T17:45:00Z
description: Consolidate Python, Rust, and TypeScript test suites into canonical module-mirrored layouts, replace weak or source-inspection assertions with behavioral checks, and enforce a branch-coverage floor in CI.
tags: [test, python, rust, typescript]
parent_prd: vertebra-design-quality
research: [../vertebra-design-quality/findings.md]
---

# Flow: Test Suite Consolidation and Coverage Gate

**Flow ID:** `test-suite-consolidation`

## Specification

### Parent Context

Parent PRD: [`vertebra-design-quality`](../vertebra-design-quality/spec.md) (Chapter 9 of 10).
- **Depends on:** [`hot-path-performance`](../hot-path-performance/spec.md) (Chapter 8) and all preceding structural/domain chapters (Chapters 2–7) so production module layouts across Rust (`vertebra-core`, `vertebra-http`, `vertebra-py`), Python (`src/py/vertebra/`), and TypeScript (`src/ts/vertebra-client/src/`) are in their final locations before test files are mirrored and coverage is locked.
- **Unblocks:** [`knowledge-resync`](../knowledge-resync/spec.md) (Chapter 10).
- **Roadmap Requirement Owned:** **R11** — Test suite consolidation: reorganize Python, Rust, and TypeScript tests to mirror the production module layout (one canonical test file per module); split mega-tests into single-behavior test functions; replace weak assertions (`is not None`, `callable(...)`, `hasattr(...)`, `inspect.getsource(...)`, constant-echo checks) with behavioral assertions; consolidate Rust `vertebra-http` and `vertebra-core` integration tests into single `tests/it/main.rs` binaries; enforce a coverage floor in `pyproject.toml` and `make check`.
- **Cross-Chapter Finding Note:** Findings `TST-10` (`ViteDevProxy` / `JavaScriptConfig` tests) and `TST-11` (`BuildOrchestrator` tests) are owned by Chapter 7 (`frontend-and-tooling-consolidation` tasks `7.2` and `7.4`) where those modules are refactored; Task `9.2` moves their resulting test files into the canonical `src/py/tests/unit/framework/test_vite.py` and `src/py/tests/unit/build/test_orchestrator.py` paths.

### Code Analysis Summary

1. **Weak and Source-Inspection Assertions (`TST-01`, `TST-02`):**
   - `src/py/tests/unit/test_stage1_hotpath_and_314t.py:68-94` (`test_subdomain_consolidation_and_shims`) and `src/py/tests/unit/test_drop_in_names_and_generics.py:73-95` (`test_canonical_extension_exports`) only import symbols and assert `is not None` or `callable(...)`.
   - `src/py/tests/unit/test_stage1_hotpath_and_314t.py:97-109` (`test_sql_gateway_uses_async_pool_without_to_thread`) inspects source strings via `inspect.getsource(SqlGatewayService.handle)` and asserts `"to_thread" not in src` instead of spying on `asyncio.to_thread` during an actual `await gw.handle(...)` invocation.
   - `src/py/tests/unit/server/test_runner.py:59-66,582-593,603-611` uses `inspect.signature` and `inspect.getsource(run_worker)` instead of exercising `run_worker` and `_call_handler` behaviorally.
   - `src/py/tests/unit/server/test_asgi.py:151-168` (`test_build_scope_native_vertebra_request`) asserts `hasattr(Request, "asgi_scope")` and then calls `pytest.skip(...)` when `try_get_batch()` is empty instead of constructing a `Request.from_parts(...)` instance and asserting `req.asgi_scope()` output.
   - `src/py/tests/unit/server/test_routing.py:59-65`, `src/py/tests/unit/framework/test_inertia_middleware.py:526`, `src/py/tests/unit/extensions/test_litestar_plugin.py:217-220,244-246`, and `src/py/tests/unit/extensions/test_data_storage_integrations.py:100,119` contain weak `is not None` / `callable(...)` checks where exact structural or value assertions apply.
2. **PR-Wave, Multi-Domain, and Misplaced Python Test Files (`TST-03`, `TST-05`, `FW-47`, `PYB-61`):**
   - Ten PR-wave or multi-domain test files sit under `src/py/tests/unit/` (`test_stage1_hotpath_and_314t.py`, `test_drop_in_names_and_generics.py`, `test_telemetry_and_platform.py`, `test_packaging_and_pgo.py`, `test_codegen_suite.py`, `test_framework_subdomains.py`, `test_pg_dataplane_and_datafusion.py`, `test_oracle_apex_and_gateway.py`, `test_sqlalchemy_and_sqlspec_vtb.py`, and `unit/server/test_zero_copy_buffer.py`).
   - `src/py/tests/unit/framework/` holds `test_orchestrator.py` (tests `vertebra.build`), and `src/py/tests/unit/server/` holds `test_routing.py` (tests `vertebra.framework._router`) and `test_litestar_{direct_fastpath,routing_acceleration,rust_offload}.py` (test `vertebra.extensions.litestar`).
   - `src/py/tests/unit/codegen/` sits at top-level instead of `src/py/tests/unit/build/codegen/`, and `src/py/tests/unit/test_utils_{init,json,misc}.py` sit at top-level instead of `src/py/tests/unit/utils/`.
3. **Mega-Tests, Tooling Tests in Library Suite, Fixtures, Integration Tests, and Nested Imports (`TST-04`, `TST-06`, `TST-07`, `TST-08`, `TST-09`):**
   - Multi-concern test functions such as `test_sqlspec_storage_adapter_crud_jsonl_arrow_parquet_and_streaming` (`test_data_storage_integrations.py:23-74`), `test_litestar_plugin_on_app_init_and_server_lifespan` (`test_litestar_plugin.py:199-251`), `test_channel_bus_fanout_and_sse_formatter` (`test_framework_subdomains.py:55-98`), and `test_response_cache_store_ttl_etag_and_304` (`test_framework_subdomains.py:101-147`) bundle 4–7 independent behaviors into single functions.
   - `src/py/tests/unit/test_bench_infrastructure.py:1-214` mutates `sys.path.insert(0, ...)` to test `tools/benchmarks/` inside `src/py/tests/`, and `test_packaging_and_pgo.py:142-172` tests `tools/pgo/profile_build.py` inside the library suite.
   - `src/py/tests/conftest.py:29-82` defines five unused fixtures (`codegen_dir`, `shm_name`, `mock_asgi_app`, `asgi_send`, `asgi_receive`), uses function-scoped imports (`lines 19, 31, 40`), and wipes `/dev/shm/test-*` and `/dev/shm/vtb-*` globally after every test (`lines 18-26`), colliding with parallel pytest workers.
   - `src/py/tests/integration/storage/conftest.py:1-58` provides a Floci S3 fixture with zero tests under `src/py/tests/integration/storage/`.
   - 131 function-scoped `import` statements (`PLC0415`) exist across `src/py/tests/` (`test_runner.py`, `test_asgi.py`, `test_wsgi.py`, `test_rsgi.py`, `test_cli.py`, `test_server_init.py`, `test_lifecycle.py`).
4. **Rust Integration Test Sprawl and Duplication (`TST-12`, `CORE-39`, `HTTP-16`):**
   - `src/rs/vertebra-http/tests/` compiles 17+ separate Cargo integration test binaries (`apex_plsql_gateway_tests.rs`, `body_limits.rs`, `compression.rs`, `cors.rs`, `apex_session_routing_tests.rs`, `file_server_tests.rs`, `health_endpoint_tests.rs`, `http2_prior_knowledge.rs`, `ipc_roundtrip.rs`, `lifecycle_tests.rs`, `openapi_docs_tests.rs`, `proxy_headers_tests.rs`, `rate_limit.rs`, `security_headers_tests.rs`, `server_integration.rs`, `static_files.rs`, `tls_integration.rs`, `ws_pubsub_hub_tests.rs`), and `src/rs/vertebra-core/tests/` compiles 4 separate binaries (`shm_streaming_tests.rs`, `test_mpmc.rs`, `test_ring.rs`, `test_shm.rs`).
   - Duplicate unit vs. integration ring tests (`spsc_recv_hybrid_timeout`, `spsc_full_returns_error`, `method_name` roundtrips) and `vertebra-http` batcher/collector tests (`test_collector.rs` vs `src/batcher.rs`) remain across inline `#[cfg(test)]` and `tests/`.
5. **TypeScript Test Layout and Coverage Gaps (`TST-13`):**
   - `src/ts/vertebra-client/tests/` mixes `tests/unit/{client,store}.test.ts` and `tests/{apex,arrow,live,realtime,telemetry,wasm}.test.ts`.
   - `tests/unit/client.test.ts:121-171` bundles `computeBackoffDelay`, `QueryCache`, and `initHtmx`, while `tests/realtime.test.ts:224-248` bundles `HotReloadClient`.
   - `OfflineDeltaQueue` (`src/live/index.ts`) and the framework adapters (`src/react/`, `src/vue/`, `src/svelte/`, `src/angular/`) lack dedicated unit test suites.
6. **Coverage Gate Configuration (R11):**
   - `pyproject.toml` has no `[tool.coverage.run]` or `[tool.coverage.report]` section and no `fail_under` floor.

### Relevant Patterns

- **Mirror Production Layout:** Every test file path under `src/py/tests/unit/<subpackage>/test_<module>.py` and `src/ts/vertebra-client/tests/<module>.test.ts` maps 1:1 to a production module, eliminating PR-wave grab-bag files.
- **Single-Binary Rust Integration Suites:** Cargo compiles each `tests/*.rs` file as a separate crate binary. Grouping integration modules under `tests/it/main.rs` links `vertebra-http` and `vertebra-core` once per crate while keeping `tests/loom_spsc.rs` isolated behind `#![cfg(loom)]`.
- **Behavioral Assertions Only:** Tests verify observable outputs, state transitions, wire bytes, and call arguments rather than `is not None`, `callable(...)`, `hasattr(...)`, or `inspect.getsource(...)`.
- **Top-Level Imports in Tests:** Enforce Ruff `PLC0415` across `src/py/tests/` so imports live at module top-level except where `monkeypatch.setitem(sys.modules, ...)` specifically tests missing optional dependencies.

### Requirements

- **C9-R1 (`TST-01`, `TST-02`):** Record the baseline `make coverage` line and branch percentages before modifying tests; delete import-only tautologies and replace weak `is not None`, `callable`, `hasattr`, `inspect.signature`, and `inspect.getsource` assertions with behavioral checks.
- **C9-R2 (`TST-03`, `TST-05`, `FW-47`, `PYB-61`):** Reorganize `src/py/tests/unit/` so every test file mirrors `src/py/vertebra/<subpackage>/<module>.py` (plus `src/py/tests/unit/native/` for `vertebra._vertebra` PyO3 tests) with zero PR-wave or multi-domain filenames remaining.
- **C9-R3 (`TST-04`, `TST-06`, `TST-07`, `TST-08`, `TST-09`):** Split multi-concern mega-tests into single-behavior functions, relocate `tools/benchmarks` and `tools/pgo` tests under `tools/{benchmarks,pgo}/tests/`, prune unused fixtures and scope `/dev/shm` cleanup in `src/py/tests/conftest.py`, add `src/py/tests/integration/storage/test_floci_roundtrip.py` with the `integration` marker, and hoist all function-scoped imports (`PLC0415`) in `src/py/tests/`.
- **C9-R4 (`TST-12`, `CORE-39`, `HTTP-16`):** Consolidate `src/rs/vertebra-http/tests/*.rs` into `src/rs/vertebra-http/tests/it/main.rs` and `src/rs/vertebra-core/tests/*.rs` into `src/rs/vertebra-core/tests/it/main.rs` (keeping `loom_spsc.rs` separate), and remove duplicate inline vs. integration ring and batcher tests.
- **C9-R5 (`TST-13`):** Flatten `src/ts/vertebra-client/tests/` into 1:1 module test files (`client.test.ts`, `reconnect.test.ts`, `query.test.ts`, `htmx.test.ts`, `hmr.test.ts`, `store.test.ts`, `realtime.test.ts`, `live.test.ts`, `arrow.test.ts`, `apex.test.ts`, `telemetry.test.ts`, `wasm.test.ts`, `react.test.tsx`, `vue.test.ts`, `svelte.test.ts`, `angular.test.ts`) with `OfflineDeltaQueue` and framework adapter behavioral tests.
- **C9-R6 (`R11`):** Configure `[tool.coverage.run]` (`source = ["vertebra"]`, `branch = true`) and `[tool.coverage.report]` (`fail_under = <measured floor>`) in `pyproject.toml` and wire `--cov-fail-under` into `make coverage` and `make check`.

### Target Layout

```text
src/py/tests/
  conftest.py
  unit/
    build/
      test_orchestrator.py
      test_bundler.py
      test_packaging.py
      codegen/
        test_extractor.py
        test_ir.py
        test_pipeline.py
        test_typescript.py
    cli/
      test_cli.py
    data/
      test_pg_gateway.py
      test_datafusion.py
      test_apex_gateway.py
      test_gateway.py
    extensions/
      test_litestar.py
      test_sqlalchemy.py
      test_sqlspec.py
      test_alchemy_storage.py
    framework/
      test_app.py
      test_router.py
      test_types.py
      test_middleware.py
      test_channels.py
      test_cache.py
      test_tasks.py
      test_dependencies.py
      test_inertia.py
      test_vite.py
      test_hmr.py
      test_import_map.py
      test_js_config.py
      test_openapi.py
    native/
      test_request_response.py
      test_transport.py
      test_freethreaded.py
    server/
      test_init.py
      test_worker.py
      test_dispatch.py
      test_loops.py
      test_lifecycle.py
      test_platform.py
      test_asgi.py
      test_wsgi.py
      test_rsgi.py
    storage/
      test_store.py
      test_signing.py
    telemetry/
      test_telemetry.py
    utils/
      test_init.py
      test_serializers.py
      test_misc.py
  integration/
    storage/
      conftest.py
      test_floci_roundtrip.py
tools/
  benchmarks/tests/test_benchmarks.py
  pgo/tests/test_pgo.py
src/rs/
  vertebra-core/tests/
    loom_spsc.rs
    it/
      main.rs
      mpmc.rs
      ring.rs
      shm.rs
      shm_streaming.rs
  vertebra-http/tests/
    it/
      main.rs
      apex_plsql_gateway.rs
      body_limits.rs
      compression.rs
      cors.rs
      apex_routing.rs
      file_server.rs
      health_endpoint.rs
      http2_prior_knowledge.rs
      ipc_roundtrip.rs
      lifecycle.rs
      openapi_docs.rs
      proxy_headers.rs
      rate_limit.rs
      security_headers.rs
      server_integration.rs
      static_files.rs
      tls_integration.rs
      ws_pubsub_hub.rs
src/ts/vertebra-client/tests/
  apex.test.ts
  arrow.test.ts
  client.test.ts
  hmr.test.ts
  htmx.test.ts
  live.test.ts
  query.test.ts
  react.test.tsx
  realtime.test.ts
  reconnect.test.ts
  store.test.ts
  svelte.test.ts
  telemetry.test.ts
  vue.test.ts
  angular.test.ts
  wasm.test.ts
```

### Risks

- **Accidental Coverage Loss During Test Redistribution:** Moving and splitting tests across 10 PR-wave files can drop test cases if not tracked systematically. Mitigation: Task `9.1` records the exact baseline test count and line/branch coverage before any edits, and Tasks `9.2` and `9.3` verify that coverage does not drop and every function in the old files is accounted for in the target mapping table.
- **Shared-Memory Collision in Rust Single-Binary Tests:** Combining `vertebra-core` and `vertebra-http` integration tests into single `it` binaries runs submodules concurrently in one process by default. Mitigation: Ensure every test in `tests/it/` generates a unique SHM segment name (using atomic counter + PID) rather than static `/vtb-test-*` names.
- **Floci Container Availability in Local Runs:** Developers running `make py-test` without Docker or Floci should not fail unit tests. Mitigation: Mark `test_floci_roundtrip.py` with `@pytest.mark.integration` and skip cleanly when `FLOCI_ENDPOINT` is unset and container startup is unavailable, while running all unit tests unconditionally.

## Implementation Plan

### Phase 1: Python Coverage Baseline, Weak-Test Cleanup, and Canonical Layout

- [x] `9.1` Record coverage baseline and replace weak or import-only Python test assertions (`characterization`)
- [x] `9.2` Reorganize Python unit tests to mirror `src/py/vertebra/` 1:1 and replace `inspect.getsource` checks (`characterization`, depends on `9.1`)
- [x] `9.3` Split mega-tests, relocate `tools/` tests, clean `conftest.py`, add Floci integration test, and enforce top-level test imports (`characterization`, depends on `9.2`)

### Phase 2: Rust and TypeScript Test Consolidation

- [x] `9.4` Consolidate `vertebra-core` and `vertebra-http` integration tests into `tests/it/main.rs` and deduplicate ring/batcher tests (`characterization`)
- [x] `9.5` Flatten TypeScript tests to mirror `src/ts/vertebra-client/src/` and add `OfflineDeltaQueue` and adapter tests (`characterization`)

### Phase 3: Coverage Floor Gate

- [x] `9.6` Enforce branch-coverage floor in `pyproject.toml` and `Makefile` (`static_validation`, depends on `9.3`, `9.4`, `9.5`)
- [x] Chapter gate: `make check` and `uv run pytest tools/benchmarks/tests tools/pgo/tests` exit 0, coverage meets or exceeds `fail_under`, Rust builds exactly one `it` binary per crate (plus `loom_spsc` under `cfg(loom)`), and `tests-reviewer` reports 0 open findings.

## Requirement-to-task trace

| Requirement | Tasks |
| :--- | :--- |
| `C9-R1` (`R11` baseline & weak assertions) | `9.1` |
| `C9-R2` (`R11` canonical Python layout & behavioral spy) | `9.2` |
| `C9-R3` (`R11` mega-tests, `tools/` tests, `conftest.py`, Floci integration, `PLC0415`) | `9.3` |
| `C9-R4` (`R11` Rust `tests/it/main.rs` & test deduplication) | `9.4` |
| `C9-R5` (`R11` TypeScript mirrored tests & adapter coverage) | `9.5` |
| `C9-R6` (`R11` `pyproject.toml` & `make check` coverage floor) | `9.6` |

## Finding-to-task trace

| Finding ID | Task | Resolution Summary |
| :--- | :--- | :--- |
| `TST-01` | `9.1` | Delete import-only tautologies and replace weak `is not None` / `callable` / `hasattr` / `inspect.signature` assertions with behavioral assertions after recording `make coverage` baseline. |
| `TST-02` | `9.2` | Replace `inspect.getsource(SqlGatewayService.handle)` check with a `monkeypatch.setattr(asyncio, "to_thread", ...)` spy while executing `await gw.handle(...)`. |
| `TST-03` | `9.2` | Split and redistribute all 10 PR-wave and multi-domain test files into canonical per-module test files mirroring `src/py/vertebra/`. |
| `TST-05` | `9.2` | Move misplaced test files (`test_orchestrator.py`, `test_routing.py`, `test_litestar_*.py`, `unit/codegen/*`, `test_utils_*.py`) into their matching domain subdirectories. |
| `FW-47` | `9.2` | Split `src/py/tests/unit/test_framework_subdomains.py` into `unit/framework/test_{channels,cache,tasks,dependencies,openapi}.py`. |
| `PYB-61` | `9.2` | Split PyO3 bridge tests out of `test_stage1_hotpath_and_314t.py`, `test_drop_in_names_and_generics.py`, and `unit/server/test_zero_copy_buffer.py` into `unit/native/test_{request_response,transport,freethreaded}.py`. |
| `TST-04` | `9.3` | Split multi-concern mega-tests into focused single-behavior test functions. |
| `TST-06` | `9.3` | Move `test_bench_infrastructure.py` and PGO tests to `tools/benchmarks/tests/test_benchmarks.py` and `tools/pgo/tests/test_pgo.py`, removing `sys.path.insert` and constant-echo checks. |
| `TST-07` | `9.3` | Prune 5 unused fixtures in `src/py/tests/conftest.py`, hoist imports to top-level, and scope `/dev/shm` cleanup to session-tracked segment names. |
| `TST-08` | `9.3` | Add `src/py/tests/integration/storage/test_floci_roundtrip.py` exercising `floci_s3_Endpoint` and register the `integration` marker in `pyproject.toml`. |
| `TST-09` | `9.3` | Hoist all 131 function-scoped imports in `src/py/tests/` to module top-level and enable `PLC0415` on `src/py/tests`. |
| `TST-12` | `9.4` | Consolidate `src/rs/vertebra-http/tests/*.rs` and `src/rs/vertebra-core/tests/*.rs` into single `tests/it/main.rs` binaries. |
| `CORE-39` | `9.4` | Remove duplicate unit vs. integration ring tests (`spsc_recv_hybrid_timeout`, `spsc_full_returns_error`, `method_name` roundtrips) across `vertebra-core`. |
| `HTTP-16` | `9.4` | Consolidate duplicate `BatchCollector` unit tests in `vertebra-http/src/batcher.rs` and `tests/it/`. |
| `TST-13` | `9.5` | Flatten `src/ts/vertebra-client/tests/unit/` into `tests/<module>.test.ts`, split `QueryCache`/`initHtmx`/`HotReloadClient`/`ReconnectScheduler`, and add `OfflineDeltaQueue` and framework adapter tests. |

## Continuity Snapshot

- **State:** `planned` — 6 tasks across 3 phases ready for sequential execution (`9.1` -> `9.2` -> `9.3` in parallel with `9.4` and `9.5`, converging at `9.6`).
- **Next Action:** Execute `test-suite-consolidation:9.1` after Chapter 8 (`hot-path-performance`) completes.
