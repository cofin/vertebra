# Planner Notes — `ch03-ingress-and-worker-hotpaths`

## Files Written

- `chapters/ch03-ingress-and-worker-hotpaths/spec.md`
- `chapters/ch03-ingress-and-worker-hotpaths/tasks/t01-http1-idle-timeout-and-ingress-fastpath.md`
- `chapters/ch03-ingress-and-worker-hotpaths/tasks/t02-backpressure-zero-arc-and-coarse-clock.md`
- `chapters/ch03-ingress-and-worker-hotpaths/tasks/t03-correlation-insert-with-now.md`
- `chapters/ch03-ingress-and-worker-hotpaths/tasks/t04-batcher-shm-scratch-and-complete-many.md`
- `chapters/ch03-ingress-and-worker-hotpaths/tasks/t05-pyo3-request-body-and-asgi-scope.md`
- `chapters/ch03-ingress-and-worker-hotpaths/tasks/t06-pyo3-rsgi-lazy-stream-and-shared-meta.md`
- `chapters/ch03-ingress-and-worker-hotpaths/tasks/t07-worker-eventfd-drain-mixed-native-and-extractors.md`
- `chapters/ch03-ingress-and-worker-hotpaths/planner-notes.md`

## Task Summaries

- `t01`: Lazy `IdleTimeoutStream` activity flag, per-connection collector shard affinity (`handle_with_peer_shard`), empty-body `CONTENT_LENGTH`/`CONTENT_ENCODING` header gate, and single-pass `encode_header_map_compact`.
- `t02`: Borrowed `AdmissionPermit<'a>` (zero per-request `Arc::clone`), `fetch_sub(1, SeqCst)` + `queue_depth.load(SeqCst)` Dekker drop without standalone `mfence`, and 64-call strided `coarse_elapsed_ms()` for closed-circuit `record_success()`.
- `t03`: `InFlightRequests::insert_with_now` accepting a batch-level `Instant` and call-site adoption in `BridgeCorrelation::dispatch_bridge_vec`.
- `t04`: `RequestBatcher`-owned `ShmDispatchScratch` (`staged`, `nodes`, `components`, `rejected_replies`) forwarded via `Dispatcher::dispatch_requests_from_shard_with_scratch`, batch-level `insert_with_now` in `ShmDispatcher`, and single-shard fast path in `ShmDispatcher::complete_many`.
- `t05`: Direct `PyBytes::new(py, self.batch.body(self.index))` in `Request::body`, batch-shared `Arc<str>` `root_path` in `AsgiScope`, and `populated_mask: u32`-tracked `[MaybeUninit<Py<PyAny>>; 24]` drop via `trailing_zeros()`.
- `t06`: Inline unary `HttpProtocolState` with lazy `Arc<RsgiStreamState>` / `Option<Arc<Mutex<Vec<u8>>>>` in `RsgiHttpProtocol` and batch-shared `Arc<RsgiServerMeta>` in `RsgiScope`.
- `t07`: Split `_on_notify_readable` / `_drain_batches` with 1-turn grace yield in `Worker._run_inline_event_loop`, single-pass `native_only` mixed sync/async loop in `Worker._process_batch`, and specialized 1-arg simple extractors in `compile_handler_spec`.

## Root Spec Issues

- Cross-chapter touchpoint (non-breaking): `t03` updates `src/rs/vertebra-http/src/dispatcher/bridge.rs::BridgeCorrelation::dispatch_bridge_vec` (which `ch02` batch-locks) to pass a batch-level `Instant` to `InFlightRequests::insert_with_now`; this is already ordered by `depends_on_chapters: [ch02-embedded-bridge-and-batching]`.
- Thin forwarding helper: `t04` adds `Dispatcher::dispatch_requests_from_shard_with_scratch` in `src/rs/vertebra-http/src/dispatcher/mod.rs` so `RequestBatcher` passes its owned `ShmDispatchScratch` cleanly without matching `DispatchBackend` variants directly.
- Note for `t02`: `AdmissionPermit<'a>` borrows `&'a BackpressureController`; all in-repo request handling (`response.rs::dispatch_request_inner`) holds the permit within the `&Arc<ServerState>` borrow scope, and `cargo test -p vertebra-http` will confirm no integration test requires a `'static` owned permit.

## Open Questions

None.
