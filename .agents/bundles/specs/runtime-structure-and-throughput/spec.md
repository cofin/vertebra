---
type: Spec
flow_id: runtime-structure-and-throughput
title: Fastpath, Rust Module, and Helper Cleanup
description: Unify per-framework fastpaths behind a shared core and provider registry, make loop backend selection table-driven, split large Rust GCP crates into focused submodules with shared protobuf wire helpers, remove hot-path copies, give VertexEmbedder a Rust HTTP/2 transport, and remove Python fallbacks behind mandatory Rust utils helpers.
state: draft
parent_prd: integration-remediation
research:
  - ../integration-remediation/research/audit-findings_20261103.md
plan_revision: 0
plan_commit: null
state_revision: 0
current_task: null
last_verified_checkpoint: null
created_at: 2026-11-03T03:30:00Z
updated_at: 2026-11-04T01:17:00Z
tags: [fastpath, rust, performance, utils, embeddings]
---

# Flow Charter: Fastpath, Rust Module, and Helper Cleanup

## Charter

This flow removes structural duplication and hot-path allocation waste across the Python server fastpaths, Rust crates, embeddings transport, and `vertebra.utils` helpers without changing public behavior. `vertebra.utils.*` helpers remain public user API: their names and semantics stay intact while internal `_py_*` fallbacks shadowing mandatory Rust bindings are removed. This charter may split into two flows (Python runtime fastpath/utils and Rust/embeddings throughput) at `/flow:plan` time if task count exceeds 8.

## Scope

1. **Fastpath core.**
   - Create `src/py/vertebra/server/_fastpath_core.py` holding shared `CompiledRoute`, `DirectPlan`, parameter coercion, scalar query converters, `step_coro_sync`, and an exception-serializer hook.
   - Define `FastPathProvider` (`detect(app)`, `build(app, prefix)`) registered by each `extensions/<fw>/fastpath.py`, replacing class-name sniffing in `attach_framework_fastpath`.
   - Collapse fastpath attribute aliases to one canonical attribute, bind `extensions/litestar/fastpath.py::FastPath` explicitly via `from_app`, and consolidate duplicated type guards in `vertebra.utils.type_guards`.
2. **Hot path.**
   - Enforce a payload size cap in `extract_scope_body_fast`.
   - Call the body accessor once in `wrap_asgi_fastpath`.
   - Dispatch `serialize_fastpath_special_result` by type and pass Arrow buffers zero-copy.
   - Preserve `bytes` headers in `_AsgiSend.send_raw_response`.
3. **Loops and welcome route.**
   - Make `server/_loops.py::resolve_loop_backend` table-driven and scope its free-threaded quarantine check strictly to loop modules (`AsyncLoopShardedResource` / `SyncLoopShardedResource`).
   - Consolidate repeated loop bucket + `call_soon_threadsafe` dispatch blocks into one helper.
   - Evict the welcome route in `framework/_app.py::add_route` only when `GET /` is registered, and move `_welcome.py` HTML/CSS to package data.
4. **Rust module structure and GCP polish.**
   - Add `vertebra-core/src/proto.rs` (minimal protobuf wire reader/writer and varint helpers) deduplicating varint codecs in `vertebra-db/src/dialer.rs`, `vertebra-core/src/vector.rs`, and `vertebra-py/src/vector.rs`.
   - Split `src/rs/vertebra-spanner/src/` and `src/rs/vertebra-bigquery/src/` into focused submodules (`client.rs`, `codec.rs`, `arrow_ipc.rs`), rename `src/rs/vertebra-py/src/completion.rs` to `src/rs/vertebra-py/src/async_bridge.rs` (delegating to `vertebra_db::py_async` while `SyncCompletionQueue`, `AsyncCompletionQueue`, and `Notifier` remain in `src/rs/vertebra-core/src/notify.rs`), add `// SAFETY:` comments on `src/rs/vertebra-db/src/arrow_shm.rs` `unsafe` blocks, deduplicate `json_cell_to_protobuf_value`, adopt `getrandom` where non-GCP hand-rolled PRNG helpers remain, and wire Python `SpannerConfig.max_commit_delay_ms` through to Rust `SyncSpannerClient` / `AsyncSpannerClient` commit options.
5. **Rust throughput.**
   - Refactor `vertebra-bigquery` `append_arrow_stream` to slice `PyBackedBytes` + `Bytes::slice` zero-copy with in-place frame headers instead of copying entire IPC payloads.
   - Eliminate avoidable per-RPC parameter and metadata clones in `src/rs/vertebra-spanner` and `src/rs/vertebra-bigquery`.
6. **Embeddings transport and broker.**
   - Wire `embeddings/_google.py::VertexEmbedder` (`SyncGcpTokenCache` / `AsyncGcpTokenCache`) onto a Rust HTTP/2 client with gRPC trailers and Vertex `Predict` endpoint framing, keeping its micro-batcher loop-local.
   - Reuse one background portal in `Embedder._run_sync`, offload `EmbeddingBroker` model inference via `asyncio.to_thread` with a bounded queue structure, send media bytes in `GeminiEmbedder.embed_multimodal`, avoid double initialization in `Embedder.__new__`, and upgrade `vertebra/db/adapters/oracle/embeddings.py::OracleEmbeddingPushdown` batch embedding to `DBMS_VECTOR.UTL_TO_EMBEDDINGS`.
7. **Utils (public helpers preserved).**
   - Remove `_py_*` fallbacks and `_LATIN_FOLD_MAP` from `utils/text.py` and `utils/humanize.py`, binding public helpers directly to `vertebra._vertebra`.
   - Remove private cross-imports from `humanize.py` into `text.py` privates.
   - Remove the test hook and redundant fallback tiers in `utils/uuids.py`, keeping whichever of Rust or stdlib benchmarks faster.
   - Verify `utils/module_loader.py::import_optional` fast-paths `sys.modules` hits alongside cached negative lookups, and ensure every public `vertebra.utils.*` helper has documentation and public-seam unit tests.

## Out of Scope

- Spanner/BigQuery official crate migration and `grpc.rs` removal (completed in `gcp-wire-protocol-correctness`), GCP task panic/cancel and typed error hierarchy (completed in `gcp-driver-resilience`), `Sync*`/`Async*` class splits (completed on `refactor/sync-async-names`), `vertebra-bigquery`/`vertebra-spanner` wheel extraction (`db-layout-and-contract`), `embeddings/__init__.py` lazy exports and pushdown relocation (`db-engine-primitives`), `allocate_shm_region` explicit errors (`storage-completeness`), and MCP core (`mcp-webmcp-core`).

## Public Seams

`FastPathProvider`, unchanged `vertebra.utils.*` public functions, and `vertebra._vertebra` (`async_bridge` symbols).

## Dependencies

`gcp-wire-protocol-correctness` (completed), `gcp-driver-resilience` (completed), `refactor/sync-async-names` (`08ac03a`), `native-data-platform/db-layout-and-contract`, and `db-engine-primitives`.

## Acceptance Boundary

- `make bench-check` shows no regression and `make rs-bench` improves on Arrow append/read paths.
- Per-framework fastpath conformance tests pass across Litestar, Starlette, FastAPI, Flask, Django, and `App`.
- `make lint-all`, `make rs-test`, `make py-test`, `make ts-test`, and `make docs` pass.

## Draft Task Outline

- t01 Fastpath core + `FastPathProvider` registry.
- t02 Hot-path body cap, zero-copy Arrow response, and raw header bytes.
- t03 Table-driven loop backend + welcome route package data.
- t04 `vertebra-core::proto` + `vertebra-py/src/async_bridge.rs` rename + `vertebra-db/src/arrow_shm.rs` SAFETY comments.
- t05 `vertebra-spanner` / `vertebra-bigquery` submodule split + `max_commit_delay_ms`.
- t06 Zero-copy `append_arrow_stream` + Spanner/BigQuery allocation trimming.
- t07 `VertexEmbedder` HTTP/2 transport, `EmbeddingBroker` offload, and `OracleEmbeddingPushdown` batch `UTL_TO_EMBEDDINGS`.
- t08 `vertebra.utils` fallback removal, docs, and public-seam tests.
