---
type: Spec
flow_id: runtime-structure-and-throughput
title: Fastpath, Rust Module, and Helper Cleanup
description: Unify per-framework fastpaths behind a shared core and provider registry, make loop backend selection table-driven, split large Rust GCP crates into focused submodules with shared protobuf wire helpers, remove hot-path copies, give VertexEmbedder a Rust HTTP/2 transport, and remove Python fallbacks behind mandatory Rust utils helpers.
state: in_progress
parent_prd: integration-remediation
research:
- ../integration-remediation/research/audit-findings_20261103.md
plan_revision: 1
plan_commit: "af319da17dec0148a18532952cb7ea2a91d1c9d7"
state_revision: 2
current_task: runtime-structure-and-throughput:t02
last_verified_checkpoint: null
created_at: '2026-11-03T03:30:00Z'
updated_at: '2026-11-06T15:22:00Z'
tags:
- fastpath
- rust
- performance
- utils
- embeddings
---

# Fastpath, Rust Module, and Helper Cleanup

## 1. Specification & Context

### 1.1 Upstream Execution Baseline

This flow executes on `feat/runtime-structure-and-throughput`, stacked on `fix/post-merge-review-and-plan-sync` (`7657e8b7c2c96bdda6821aec70a2b603f438a127`) above `main @ 80ff8a65e43a2bcef9e48a7763e89a7352118232`. Prerequisite database layout ([PR #75](https://github.com/cofin/vertebra/pull/75), `f7417015`), database foundation ([PR #76](https://github.com/cofin/vertebra/pull/76), `21c99f95`), and engine primitives ([PR #77](https://github.com/cofin/vertebra/pull/77), `80ff8a65`) are squash-merged on `main`, and post-merge Rust SHM/async-bridge hardening (`src/rs/vertebra-db/src/arrow_shm.rs` `// SAFETY:` blocks and `src/rs/vertebra-db/src/py_async.rs` setup-abort cleanup) is already committed in `e2b282b7`.

### 1.2 Code Analysis Summary

| Existing symbol | Verified consequence |
| --- | --- |
| `src/py/vertebra/server/_fastpath.py::{attach_framework_fastpath,wrap_asgi_fastpath,extract_scope_body_fast,serialize_fastpath_special_result}` | Inspects framework classes by string name, invokes `body_bytes_fn()` twice (`has_prebuffered_body` and `extract_scope_body_fast`), buffers `receive()` chunks without a payload size cap, sets both `_vertebra_fastpath` and `__vertebra_fastpath__`, and copies `ArrowCursor` bytes with `bytes(buffer_view_fn())`. |
| `src/py/vertebra/extensions/{litestar,starlette,fastapi,flask,django}/fastpath.py` | Duplicate `CompiledRoute`, `DirectPlan`, scalar query converters, `step_coro_sync`, and type-guard helpers across five framework fastpath modules. |
| `src/py/vertebra/server/_loops.py::{resolve_loop_backend,AsyncLoopShardedResource,SyncLoopShardedResource}` | `if not quarantined_prefixes and import_optional("rloop") is not None` blocks `rloop` auto-selection whenever any unrelated C extension is quarantined on 3.14t/3.15t; loop-bucket `call_soon_threadsafe` dispatch blocks are duplicated. |
| `src/py/vertebra/framework/{_app.py,_welcome.py}` | `_welcome.py` embeds multi-hundred-line HTML/CSS templates inline in Python source; `App.add_route` welcome eviction must trigger strictly on `GET /`. |
| `src/rs/vertebra-py/src/completion.rs` | Already delegates async completion draining to `vertebra_db::py_async::spawn_rust_db_task_with_attr`; needs renaming to `src/rs/vertebra-py/src/async_bridge.rs` while shared varint helpers move to `src/rs/vertebra-core/src/proto.rs`. |
| `src/rs/vertebra-{spanner,bigquery}/src/client.rs` & `src/py/vertebra/db/adapters/{bigquery,spanner}/driver.py` | Monolithic `client.rs` files combine client transport, value codecs, and Arrow IPC; `append_arrow_stream` copies IPC payloads instead of slicing `PyBackedBytes`/`Bytes`; `SpannerConfig.max_commit_delay_ms` must be preserved and wire-verified across the submodule split; BigQuery and Spanner Python drivers duplicate `ObjectStore` staging resolution. |
| `src/py/vertebra/embeddings/{_base.py,_google.py,_broker.py}` & `src/py/vertebra/db/adapters/oracle/embeddings.py` | `VertexEmbedder._default_http_transport` uses stdlib `urllib.request` in threads instead of Rust HTTP/2; `Embedder._run_sync` spins ephemeral threads per call; `EmbeddingBroker` lacks bounded local thread offload when sidecar quarantine is disabled; `GeminiEmbedder.embed_multimodal` drops inline media bytes; `OracleEmbeddingPushdown` embeds batch items row-by-row instead of `DBMS_VECTOR.UTL_TO_EMBEDDINGS`. |
| `src/py/vertebra/utils/{text.py,humanize.py,uuids.py,module_loader.py}` & `src/py/vertebra/extensions/sqlspec/{bigquery.py,spanner.py}` | `text.py` and `humanize.py` carry hundreds of lines of `_py_*` fallbacks and `_LATIN_FOLD_MAP` behind mandatory `vertebra._vertebra` functions; `uuids.py` retains `_load_uuid_utils` test hooks and multi-tier optional shims; SQLSpec adapters retain redundant internal aliases. |

### 1.3 Relevant Patterns and Glossary

Read [hot-path and async dispatch](../../knowledge/patterns/python/hot-path-and-async-dispatch.md), [serialization and utils](../../knowledge/patterns/python/serialization-and-utils.md), [module and class layout](../../knowledge/patterns/python/module-and-class-layout.md), [FFI and GIL discipline](../../knowledge/patterns/rust/ffi-and-gil-discipline.md), [embeddings and vectors](../../knowledge/domains/agent/embeddings-and-vectors.md), [database drivers and dialers](../../knowledge/domains/data/database-drivers-and-dialers.md), and the `python-framework`, `pyo3-bridge`, `runtime-ipc`, and `review-performance` skills.

`vertebra._vertebra` is mandatory in every Vertebra installation, so Python modules under `vertebra.utils` bind directly to `vertebra._vertebra` without pure-Python fallback duplicates while keeping all public `vertebra.utils.*` helper names and signatures unchanged (`vertebra.utils.__init__` remains thin).

## 2. Requirements

### 2.1 Functional Requirements and Architecture

- **R1.** Unify framework fastpath compilation and direct dispatch primitives in `src/py/vertebra/server/_fastpath_core.py`, register per-framework `FastPathProvider` implementations (`detect(app)`, `build(app, prefix)`), collapse fastpath attributes onto `__vertebra_fastpath__`, bind `extensions/litestar/fastpath.py::FastPath.from_app`, and consolidate shared type guards in `src/py/vertebra/utils/type_guards.py`.
- **R2.** Enforce a configurable payload size limit (`413 Payload Too Large`) in `extract_scope_body_fast`, read prebuffered request bodies once in `wrap_asgi_fastpath`, dispatch `serialize_fastpath_special_result` by exact type with zero-copy `ArrowCursor` buffer views, and preserve raw `bytes` header tuples in `AsgiSend.send_raw_response`.
- **R3.** Replace imperative loop-backend branching in `src/py/vertebra/server/_loops.py` with a table-driven backend registry whose free-threaded quarantine check applies strictly to candidate loop modules, deduplicate loop-sharded `call_soon_threadsafe` dispatch in `AsyncLoopShardedResource` / `SyncLoopShardedResource`, evict the welcome route in `App.add_route` strictly when `GET /` is registered, and load welcome HTML/CSS from package data assets.
- **R4.** Extract shared protobuf wire/varint codecs into `src/rs/vertebra-core/src/proto.rs` (deduplicating `vertebra-db/src/dialer.rs`, `vertebra-core/src/vector.rs`, and `vertebra-py/src/vector.rs`) and rename `src/rs/vertebra-py/src/completion.rs` to `src/rs/vertebra-py/src/async_bridge.rs`.
- **R5.** Split `src/rs/vertebra-spanner/src/` and `src/rs/vertebra-bigquery/src/` into `client.rs`, `codec.rs`, and `arrow_ipc.rs`, deduplicate `json_cell_to_protobuf_value`, preserve and wire-verify `SpannerConfig.max_commit_delay_ms` across Rust `SyncSpannerClient` / `AsyncSpannerClient` commit options, and share one GCP `ObjectStore` staging resolver across BigQuery and Spanner Python drivers.
- **R6.** Refactor `vertebra-bigquery` `append_arrow_stream` to slice `PyBackedBytes` / `Bytes::slice` zero-copy with in-place frame headers and eliminate avoidable per-RPC metadata and parameter clones in `vertebra-spanner` and `vertebra-bigquery`.
- **R7.** Provide a Rust HTTP/2 gRPC-framed transport for `VertexEmbedder` (`SyncGcpTokenCache` / `AsyncGcpTokenCache`), reuse a persistent background portal in `Embedder._run_sync` without double-initializing `Embedder.__new__`, add bounded `asyncio.to_thread` local inference offload in `EmbeddingBroker`, forward inline media bytes in `GeminiEmbedder.embed_multimodal`, and batch `OracleEmbeddingPushdown` calls via `DBMS_VECTOR.UTL_TO_EMBEDDINGS`.
- **R8.** Remove `_py_*` fallbacks and `_LATIN_FOLD_MAP` from `src/py/vertebra/utils/{text,humanize}.py`, eliminate cross-module private imports and `_load_uuid_utils` test hooks in `src/py/vertebra/utils/uuids.py`, ensure `import_optional` fast-paths `sys.modules` hits, remove redundant internal aliases in `src/py/vertebra/extensions/sqlspec/{bigquery,spanner}.py`, and sync docs and OKF chapters.

### 2.2 Non-Functional and Safety Constraints

- Zero inline `#` comments in Python files; use Google-style docstrings and PEP 585/604/695 annotations exclusively.
- Keep `src/py/vertebra/utils/__init__.py` thin (`test_utils_init.py` enforces a 1-line module docstring).
- Every Rust `unsafe` block carries an explicit `// SAFETY:` justification, and sync blocking operations detach the Python GIL via `py.detach(...)`.
- Preserve all public `vertebra.utils.*` function names and signatures, `Sync*`/`Async*` class names, and upstream `sqlspec` `<Engine>SyncConfig` / `<Engine>AsyncConfig` infix naming.

### 2.3 Acceptance Scenarios

| Given | When | Then |
| --- | --- | --- |
| Litestar, Starlette, FastAPI, Flask, Django, or native `App` | `attach_framework_fastpath` compiles routes | Matched `FastPathProvider` attaches `__vertebra_fastpath__` without class-name string sniffing; direct route execution matches full ASGI/WSGI semantics. |
| Streaming ASGI request exceeding `max_body_size` or handler returning `ArrowCursor` | Dispatched through `_fastpath.py` | Oversized chunked body aborts immediately with HTTP `413`; `ArrowCursor` exposes its zero-copy buffer view without `bytes(...)` copying. |
| Free-threaded 3.14t/3.15t interpreter with an unrelated C extension quarantined | `resolve_loop_backend("auto")` runs | `rloop` remains selectable unless `"rloop"` itself is quarantined; `GET /health` registration preserves the debug welcome page on `GET /` until `GET /` is registered. |
| BigQuery `append_arrow_stream` and Spanner commit with `max_commit_delay_ms=25` | Executed via sync and async clients | `append_arrow_stream` slices `Bytes` without whole-payload copies, and Spanner `CommitRequest.max_commit_delay` encodes `25ms`. |
| `VertexEmbedder`, `GeminiEmbedder.embed_multimodal`, `EmbeddingBroker`, and `OracleEmbeddingPushdown` | Executing single, multimodal, and batch embeddings | `VertexEmbedder` dispatches over Rust HTTP/2 with `AsyncGcpTokenCache`, inline media bytes are encoded, local broker inference runs off the event loop with bounded concurrency, and Oracle batches via `DBMS_VECTOR.UTL_TO_EMBEDDINGS`. |
| `vertebra.utils.{text,humanize,uuids,module_loader}` | Imported and called across unit suites | All public helpers delegate cleanly to `vertebra._vertebra` / stdlib with zero `_py_*` duplicate tables or test monkeypatch hooks. |

### 2.4 Alternatives Considered

| Decision | Chosen seam and alternative |
| --- | --- |
| Fastpath discovery | Explicit `FastPathProvider` registry over lazy framework fastpath modules avoids importing uninstalled web frameworks while eliminating brittle `type(app).__name__` checks. |
| Protobuf wire helpers | Minimal zero-copy varint/tag helpers in `vertebra-core::proto` share wire primitives across `vertebra-core`, `vertebra-py`, and `vertebra-db` without pulling `prost` into core rings. |
| `vertebra.utils` fallbacks | Direct binding to mandatory `vertebra._vertebra` removes hundreds of lines of dead pure-Python fallback tables while keeping every public `vertebra.utils.*` symbol intact. |

## 3. Requirement-to-Task Traceability Matrix

| Requirement | Owning tasks | Verification strategy | Canonical public seam / gate |
| --- | --- | --- | --- |
| R1 | [t01](tasks/t01-fastpath-core-and-providers.md) | behavior_tdd | `src/py/tests/unit/server/test_litestar_direct_fastpath.py`, `src/py/tests/unit/extensions/test_starlette_plugin.py`, `src/py/tests/unit/extensions/test_fastapi_plugin.py`, `src/py/tests/unit/extensions/test_flask_plugin.py`, `src/py/tests/unit/extensions/test_django_plugin.py`, `src/py/tests/unit/utils/test_dispatch_schema_converters.py` |
| R2 | [t02](tasks/t02-hotpath-body-cap-and-zero-copy.md) | behavior_tdd | `src/py/tests/unit/server/test_stage1_hotpath_and_314t.py`, `src/py/tests/unit/server/test_zero_copy_buffer.py`, `src/py/tests/unit/server/test_asgi.py` |
| R3 | [t03](tasks/t03-loop-registry-and-welcome-assets.md) | behavior_tdd | `src/py/tests/unit/server/test_runtime_tuning.py`, `src/py/tests/unit/framework/test_app.py` |
| R4 | [t04](tasks/t04-rust-proto-and-async-bridge.md) | behavior_tdd | `src/rs/vertebra-core/src/proto.rs`, `src/rs/vertebra-py/src/async_bridge.rs`, `src/py/tests/unit/db/test_vector_wire_codecs.py`, `src/py/tests/unit/ffi/test_ffi.py` |
| R5 | [t05](tasks/t05-gcp-submodules-and-commit-delay.md) | behavior_tdd | `src/rs/vertebra-spanner/tests/spanner_wire_tests.rs`, `src/rs/vertebra-bigquery/tests/bigquery_wire_tests.rs`, `src/py/tests/unit/db/test_spanner.py`, `src/py/tests/unit/db/test_bigquery.py` |
| R6 | [t06](tasks/t06-gcp-zero-copy-arrow-and-allocations.md) | behavior_tdd | `src/rs/vertebra-bigquery/tests/bigquery_wire_tests.rs`, `src/rs/vertebra-spanner/tests/spanner_wire_tests.rs`, `src/py/tests/unit/db/test_bigquery.py` |
| R7 | [t07](tasks/t07-embeddings-http2-and-broker-offload.md) | behavior_tdd | `src/py/tests/unit/embeddings/test_google_embedders.py`, `src/py/tests/unit/embeddings/test_embedding_broker.py`, `src/py/tests/unit/db/test_enterprise_olap_drivers.py` |
| R8 | [t08](tasks/t08-utils-cleanup-and-docs.md) | behavior_tdd | `src/py/tests/unit/utils/test_text_and_humanize.py`, `src/py/tests/unit/utils/test_utils_core.py`, `src/py/tests/unit/test_utils_init.py`, `src/py/tests/unit/extensions/test_sqlspec_bigquery.py`, `src/py/tests/unit/extensions/test_sqlspec_spanner.py` |

## 4. Implementation Plan

Every worksheet executes as one atomic, GPG-signed conventional commit with disjoint file ownership across parallelizable tasks.

### Phase 1: Python Server Fastpath, Loops, and Rust Core Wire Helpers

- [x] t01 — Extract shared fastpath core and FastPathProvider registry (tasks/t01-fastpath-core-and-providers.md)
- [ ] t02 — Enforce hot-path body size cap, zero-copy Arrow responses, and raw byte headers (tasks/t02-hotpath-body-cap-and-zero-copy.md)
- [ ] t03 — Make loop backend selection table-driven and externalize welcome page assets (tasks/t03-loop-registry-and-welcome-assets.md)
- [ ] t04 — Add vertebra-core protobuf wire helpers and rename vertebra-py async_bridge (tasks/t04-rust-proto-and-async-bridge.md)

**Phase 1 Recovery Checkpoint:** `make rs-test`, `make dev`, and `uv run --no-sync pytest src/py/tests/unit/server src/py/tests/unit/framework src/py/tests/unit/extensions src/py/tests/unit/ffi -q` exit 0.

### Phase 2: Rust GCP Crate Modularization, Zero-Copy Throughput, Embeddings, and Utils Cleanup

- [ ] t05 — Split Spanner and BigQuery Rust submodules, verify max_commit_delay_ms, and unify staging stores (tasks/t05-gcp-submodules-and-commit-delay.md)
- [ ] t06 — Slice BigQuery append_arrow_stream zero-copy and trim GCP RPC allocations (tasks/t06-gcp-zero-copy-arrow-and-allocations.md)
- [ ] t07 — Add Rust HTTP/2 VertexEmbedder transport, bounded EmbeddingBroker offload, and batch Oracle pushdown (tasks/t07-embeddings-http2-and-broker-offload.md)
- [ ] t08 — Remove pure-Python utils fallbacks, clean SQLSpec aliases, and sync docs/OKF (tasks/t08-utils-cleanup-and-docs.md)

**Phase 2 Recovery Checkpoint:** `make lint-all`, `make rs-test`, `make py-test`, `make ts-test`, and `make docs` exit 0, followed by independent `@code-reviewer` and `@quality-reviewer` passes before archiving the flow on `feat/runtime-structure-and-throughput`.

## 5. Continuity Snapshot

- State is `planned`; all 8 worksheets (`t01`–`t08`) are `pending` at `plan_revision: 1`, anchored at `plan_commit: "7657e8b7c2c96bdda6821aec70a2b603f438a127"`.
- Prerequisite `arrow_shm.rs` `// SAFETY:` comments and `py_async.rs` setup-abort cleanup were completed during post-merge remediation (`e2b282b7`) and are excluded from `t04` to prevent duplicate churn.
- Next action on `feat/runtime-structure-and-throughput`: `/flow:implement runtime-structure-and-throughput` starting at `t01`.
