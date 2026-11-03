---
type: Spec
flow_id: runtime-structure-and-throughput
title: Fastpath, Rust Module, and Helper Cleanup
description: Unify per-framework fastpaths behind a shared core and provider registry, make loop backend selection table-driven, split the Rust GCP modules with shared proto/status/retry/auth helpers, remove hot-path copies, give VertexEmbedder a real HTTP/2 transport, and remove Python fallbacks behind the public utils helpers.
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
updated_at: 2026-11-03T03:30:00Z
tags: [fastpath, rust, performance, utils, embeddings]
---

# Flow Charter: Fastpath, Rust Module, and Helper Cleanup

## Charter

This flow removes structural duplication and hot-path waste left by the merged range, without changing public behavior. The `vertebra.utils.*` helpers are public user API: their names and behavior are preserved, and only the internal Python fallbacks behind mandatory Rust are removed. This charter may split into two flows (Python runtime and Rust GCP structure) at `/flow:plan` time if it exceeds 8 tasks.

## Scope

1. **Fastpath core.**
   - New `server/_fastpath_core.py` holds a shared `CompiledRoute`, `DirectPlan`, parameter coercion, scalar query converters, `step_coro_sync`, and an exception-serializer hook.
   - A `FastPathProvider` protocol (`detect(app)`, `build(app, prefix)`) is registered by each `extensions/<fw>/fastpath.py`. This replaces class-name sniffing in `attach_framework_fastpath`.
   - The fastpath attribute aliases collapse to one.
   - `litestar/fastpath.py::FastPath` binds explicitly through `from_app`.
   - The duplicated type guards move to `utils.type_guards`.
2. **Hot path.**
   - `extract_scope_body_fast` enforces a body-size cap.
   - `wrap_asgi_fastpath` calls the body accessor once.
   - `serialize_fastpath_special_result` dispatches on type and passes Arrow buffers zero-copy.
   - `storage/_streaming.py::to_fastpath_tuple` streams without a full copy and returns 416 for invalid ranges.
   - `_AsgiSend.send_raw_response` keeps bytes headers.
3. **Loops.**
   - `server/_loops.py::resolve_loop_backend` becomes table-driven, and its quarantine check considers only loop modules.
   - The repeated "bucket and call_soon_threadsafe" blocks collapse into one helper.
   - `framework/_app.py::add_route` evicts the welcome route only for `GET /`.
   - `_welcome.py` HTML/CSS moves to package data.
4. **Rust GCP structure.**
   - `vertebra-core/src/proto.rs`: wire reader/writer, varint, and well-known types. It replaces the varint copies in `dialer.rs` and `vector.rs`.
   - `vertebra-db/src/grpc/{status,framing,channel,retry,auth}.rs`: one `google.rpc.Status` decoder, one LPM framer shared with `vertebra-http`/`vertebra-py`, one retry policy with backoff that honors RetryInfo, and shared Google metadata.
   - `vertebra-db/src/spanner/` and `bigquery/` submodule splits, plus an `arrow_ipc.rs` for the IPC helpers.
   - `completion.rs` is renamed to `async_bridge`.
   - The Spanner request id moves to `extra_headers`.
   - `GrpcPoolConfig` window settings are made effective.
   - Glob re-exports and the empty `duckdb` feature are removed.
   - `json_cell_to_protobuf_value` is deduplicated.
   - `percent-encoding`, `url`, and the `getrandom` crate (with a fork guard) are adopted.
   - One `GcpTokenCache` is shared by `_vertebra` and `_storage`.
   - Dead code is removed (`SpannerTypeCode::Unspecified` no-op, `generate_request_uuid_v4`, the public test-only `decode_append_rows_request`).
   - SAFETY comments are added on the `arrow_shm.rs` unsafe blocks, and `allocate_shm_region` errors instead of falling back to anonymous memory.
   - Python honors `max_commit_delay_ms`.
5. **Rust throughput.**
   - `append_arrow_stream` uses `PyBackedBytes` + `Bytes::slice` with in-place frame headers and streaming encode.
   - `read_table_to_shm_arrow_with_compression` writes in stream order.
   - `GrpcFrameStream` takes a zero-copy path for complete frames.
   - `GrpcChannelPool::acquire_sender` uses `ArcSwap` and does not lock while dialling.
   - Spanner per-RPC allocations are removed (cached metadata/URI/bearer, no param clones, a `VecDeque` in `PartialResultSetAssembler`).
6. **Embeddings transport.**
   - `embeddings/_google.py::VertexEmbedder` uses the Rust HTTP/2 client with trailers and the correct Vertex service. The micro-batcher is loop-local.
   - `Embedder._run_sync` reuses one portal.
   - `EmbeddingBroker` offloads inference with `to_thread` and fixes the queue data structure.
   - `GeminiEmbedder.embed_multimodal` sends the media.
   - `OracleEmbedder` batches through `UTL_TO_EMBEDDINGS`.
   - `Embedder.__new__` no longer double-initializes.
   - `embeddings/__init__.py` stops eagerly importing the engine embedders.
7. **Utils (public helpers kept).**
   - Remove the `_py_*` fallbacks and `_LATIN_FOLD_MAP` from `utils/text.py` and `utils/humanize.py`, binding the public functions directly to Rust.
   - Stop `humanize.py` importing text privates.
   - Remove the test hook and redundant tiers from `utils/uuids.py`, keeping whichever of Rust and stdlib benchmarks faster.
   - `module_loader.py::import_optional` uses `sys.modules` and caches misses.
   - Every public helper gets a docs entry and a public-seam test.

## Out of Scope

- Wire correctness (Chapter 2) and MCP (Chapter 7).

## Public Seams

`FastPathProvider`, the unchanged `vertebra.utils.*` public names, and `_vertebra.pyi` (renamed bridge types).

## Dependencies

Chapters 2 and 3.

## Acceptance Boundary

- `make bench-check` shows no regression.
- `make rs-bench` improves on the append/read paths.
- The fastpath per-framework conformance tests pass.
- `make lint-all`, `make rs-test`, `make py-test`, and `make ts-test` pass.

## Draft Task Outline

- t01 Fastpath core + provider registry.
- t02 Hot-path fixes.
- t03 Loops/welcome cleanup.
- t04 Rust proto/grpc shared modules.
- t05 Rust Spanner/BigQuery module splits + dead code.
- t06 Rust throughput.
- t07 Embeddings transport/broker.
- t08 Utils fallback removal + docs.
