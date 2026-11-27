# Planner Notes: ch02-embedded-bridge-and-batching

## Files Written

- `chapters/ch02-embedded-bridge-and-batching/spec.md`
- `chapters/ch02-embedded-bridge-and-batching/tasks/t01-embedded-runtime-and-shard-sizing.md`
- `chapters/ch02-embedded-bridge-and-batching/tasks/t02-zero-copy-inprocess-and-batch-sink.md`
- `chapters/ch02-embedded-bridge-and-batching/tasks/t03-batch-locked-bridge-correlation.md`
- `chapters/ch02-embedded-bridge-and-batching/tasks/t04-zero-copy-transport-batch-submit.md`
- `chapters/ch02-embedded-bridge-and-batching/tasks/t05-wire-asgi-and-rsgi-batch-flush.md`
- `chapters/ch02-embedded-bridge-and-batching/tasks/t06-wire-native-and-wsgi-batch-flush.md`
- `chapters/ch02-embedded-bridge-and-batching/planner-notes.md`

## Task Summaries

- `runtime-perf-remediation:ch02:t01`: Default non-free-threaded `Topology::Embedded` `worker_threads` to `1` when `VERTEBRA_TOKIO_THREADS` is unset, size `RequestBatcher` shards to active Python consumers, and switch `EmbeddedSupervisor::spawn_one` and `spawn_python_workers` to cgroup-aware `config::num_cpus()`.
- `runtime-perf-remediation:ch02:t02`: Store `http::uri::PathAndQuery` zero-copy in `InProcessRequest` via `from_path_and_query` while keeping `InProcessRequest::new` and `BatchSnapshot` compatible, and add `BatchResponseSink` (`OnceLock` fast path + `RwLock` override) to `BatchBridge`.
- `runtime-perf-remediation:ch02:t03`: Acquire one shard `Mutex` and one `active_requests.fetch_add` per batch in `dispatch_bridge_vec`, move `PathAndQuery` zero-copy, register `BatchResponseSink` in `ensure_sink_registered`, and add a zero-bucket single-shard fast path in `BridgeCorrelation::complete_many` / `complete_bridge_batch`.
- `runtime-perf-remediation:ch02:t04`: Add `ResponseSubmitter::submit_batch_flushed` in `vertebra-py/src/transport.rs` converting owned `headers_raw` and `PreparedRawBody::Owned` via `Bytes::from` without a second copy on Embedded mode, and remove redundant copies in `submit_results` and `submit_response`.
- `runtime-perf-remediation:ch02:t05`: Wire `flush_and_gather_inline` (driving `drive_asgi_batch` and `drive_native_batch` in `asgi.rs`) and `drive_rsgi_batch` (`rsgi/http.rs`) batch flush sites to `ResponseSubmitter::submit_batch_flushed` without touching `ch03` scope/protocol structs.
- `runtime-perf-remediation:ch02:t06`: Wire the inline batch flush site of `drive_wsgi_batch` (`wsgi/pool.rs` / `wsgi/runner.rs`) to `ResponseSubmitter::submit_batch_flushed` without touching `ch01` `WsgiExecutor` internals.

## Root Spec Issues

None. All contracts (`InProcessRequest` with `node`, private `path_query`, and accessors; `vertebra-core/Cargo.toml` `http` dependency in `t02`; and `drive_native_batch` in `asgi.rs::flush_and_gather_inline` assigned to `t05`) are reconciled with root `spec.md`.

## Open Questions

- None (the 1 vs 2 Tokio thread A/B check for 1w GIL Embedded is already deferred to `ch04-data-benchmarks-and-verification`).
