---
type: Research
title: Vertebra design-quality audit findings
description: Evidence ledger from read-only audits of every Vertebra module at commit 2f5e19a (source-identical to edceea6), cross-referenced to the roadmap task that resolves each finding.
status: stable
tags: [research, audit, refactor, performance]
---

# Vertebra design-quality audit findings

Read-only audits covered the Rust crates (`vertebra-core`, `vertebra-storage`, `vertebra-db`, `vertebra-http`, `vertebra-py`, `vertebra-bundler`), the Python package (`server`, `supervisor`, `telemetry`, `framework`, `extensions`, `storage`, `db`, `wasm`, `utils`, `build`, `cli`), the TypeScript SDK, the test suites, benchmarks, `Makefile`, `pyproject.toml`, and CI. Line numbers are against `2f5e19a`; `git diff --stat 2f5e19a edceea6` touches only `.agents/bundles/log.md`, so they hold for `feat/stack-followups`.

Prefixes: `CORE` (vertebra-core, vertebra-storage, vertebra-db), `HTTP` (vertebra-http), `PYB` (vertebra-py and the Python server, supervisor, telemetry), `FW` (Python framework, extensions, storage, db, wasm, utils), `FE` (bundler, TypeScript SDK, build, CLI), `TST` (tests), `TOOL` (Makefile, pyproject, CI), `MCP` (MCP/A2A/agent alignment with litestar-mcp), `OKF` (code defects found during the knowledge-bundle audit).

Total findings: 279.

## Finding-to-task cross-reference

| Finding | Summary | Owning task |
|---|---|---|
| CORE-01 | MPMC `try_recv` permanently wedges the ring on `BufferTooSmall` | `core-ipc-consolidation:2.1` |
| CORE-02 | MPMC `try_send_with` trusts the closure's returned length; a panic wedges the slot | `core-ipc-consolidation:2.1` |
| CORE-03 | SHM `submit_raw_response` frames (ResponseSlot) are never decoded by the HTTP response reader | `core-ipc-consolidation:2.5` |
| CORE-04 | `write_response_slot` silently truncates `headers_len` to u16 | `core-ipc-consolidation:2.1` |
| CORE-05 | `BufferPool` (slot pool) creates aliasing `&mut Vec<u8>` across live buffers | `core-ipc-consolidation:2.2` |
| CORE-06 | The futex fallback turns blocking `recv`/`send` into hard errors | `core-ipc-consolidation:2.2` |
| CORE-07 | The futex park/re-check loop is copy-pasted 10 times in `SpscRing` | `core-ipc-consolidation:2.4` |
| CORE-08 | Dead SPSC/MPMC API variants (ext=0) | `core-ipc-consolidation:2.4` |
| CORE-09 | `SlotView`'s borrowed flag is only half-enforced | `core-ipc-consolidation:2.4` |
| CORE-10 | MPMC has no wake path; idle workers spin and yield inside `poll_batch` | `hot-path-performance:8.2` |
| CORE-11 | `SharedMutex`, `SharedRwLock`, `SharedCondvar`, `WaitWord` and the flock fallback are unused | `core-ipc-consolidation:2.2` |
| CORE-12 | `BufferChannel` / `BufferSender` / `BufferReceiver` / `BufferMessage` and SPSC `split()` handles are unused | `core-ipc-consolidation:2.2` |
| CORE-13 | `logger.rs`, `telemetry.rs`, `guards.rs`, `protocol/state.rs`, `router::Router` and `protocol::{RequestHeader, ResponseHeader, ControlMessageKind, response_flags}` have no production consumer | `core-ipc-consolidation:2.2`, `bridge-and-server-consolidation:4.10` |
| CORE-14 | The `cleanup.rs` registry is never populated, so `cleanup_all()` is a no-op | `core-ipc-consolidation:2.2` |
| CORE-15 | The whole `vertebra-db` crate is unused and duplicates Python | `core-ipc-consolidation:2.3` |
| CORE-16 | `Opcode` lists "Bun Native" operations and silently maps unknown values to `AsgiHttp` | `core-ipc-consolidation:2.2` |
| CORE-17 | Two independent flag bit spaces describe the same frame states | `core-ipc-consolidation:2.5` |
| CORE-18 | `ResponseSlot::decode_from` allocates two `String`s per header plus the body `Vec` on the HTTP hot path | `core-ipc-consolidation:2.5` |
| CORE-19 | `ResponseData::deserialize` copies headers and body out of the ring | `core-ipc-consolidation:2.5` |
| CORE-20 | `BatchSnapshot::deserialize_from` copies twice, allocates three times, and never validates node offsets | `core-ipc-consolidation:2.1`, `hot-path-performance:8.4` |
| CORE-21 | The batch wire header is serialized twice and the size constant is redefined | `core-ipc-consolidation:2.6` |
| CORE-22 | `BatchSnapshot` has four constructors where one would do | `core-ipc-consolidation:2.6` |
| CORE-23 | The `RingPairError`→`TransportError` mapping is written out three times | `core-ipc-consolidation:2.6` |
| CORE-24 | Two different types are both named `BufferPool` | `core-ipc-consolidation:2.6` |
| CORE-25 | Four hard-coded response tiers are spread over three structs and env vars | `core-ipc-consolidation:2.6` |
| CORE-26 | `try_recv_response` always scans small→xlarge, so large tiers starve under load | `core-ipc-consolidation:2.6` |
| CORE-27 | `BatchBridge` holds a std `Mutex` across a blocking crossbeam `send`, called from the async dispatcher | `core-ipc-consolidation:2.7` |
| CORE-28 | Redundant `BatchBridge` API wrappers | `core-ipc-consolidation:2.7` |
| CORE-29 | Per-header `Vec` allocations in `BridgeResponse`; a Tokio `Mutex` around a single-consumer receiver | `core-ipc-consolidation:2.7` |
| CORE-30 | `ShmRegion` has three copies of the mmap/error-cleanup block plus unused constructors | `core-ipc-consolidation:2.8` |
| CORE-31 | `Notifier` carries bench-only spin helpers, duplicate cfg imports, and redundant `unsafe impl`s | `core-ipc-consolidation:2.2` |
| CORE-32 | The default macOS build parks with a 1 ms spin-sleep; ulock is opt-in | `core-ipc-consolidation:2.8` |
| CORE-33 | Root re-exports and `VERSION` are unused, and the transport module doc is broken | `core-ipc-consolidation:2.2` |
| CORE-34 | `telemetry::sanitize_sql_statement` corrupts UTF-8, rewrites `JOIN (` subqueries, and documents `VALUES` handling it does not have | `bridge-and-server-consolidation:4.10` |
| CORE-35 | `vertebra-core` dependencies that exist only for dead code | `core-ipc-consolidation:2.2` |
| CORE-36 | `vertebra-storage` duplicates crypto helpers and runs its own Tokio runtime | `core-ipc-consolidation:2.9` |
| CORE-37 | Storage signing pyfunctions default `timestamp=1700000000` (a fixed date in Nov 2023) | `core-ipc-consolidation:2.9` |
| CORE-38 | `NativeObjectStore` hand-writes every method as a sync + `_async` pair | `core-ipc-consolidation:2.9` |
| CORE-39 | Duplicate and dead-code tests in core | `core-ipc-consolidation:2.2`, `test-suite-consolidation:9.4` |
| CORE-40 | Benches measure dead code or duplicate each other, and there is no Makefile target for Rust benches | `review-infrastructure:1.6`, `core-ipc-consolidation:2.2`, `hot-path-performance:8.1` |
| CORE-41 | Unconditional SeqCst RMW on every send/recv (benchmark-gated candidate) | `hot-path-performance:8.3` |
| CORE-42 | `encode_headers_compact` truncates silently; `decode_headers_raw` is test-only | `core-ipc-consolidation:2.5` |
| CORE-43 | `BufferPool::release_multi` is a no-op wrapper and `BufferHandle` is an alias shim | `core-ipc-consolidation:2.2` |
| CORE-44 | The workspace splits deps between `[workspace.dependencies]` and per-crate pins | `core-ipc-consolidation:2.3` |
| FE-01 | stub/placeholder: `sourcemap`, `target`, `external`, `define`, `format`, `tree_shaking`, `splitting` are accepted and ex | `frontend-and-tooling-consolidation:7.3` |
| FE-02 | dead code: `hmr_rx: Option<broadcast::Receiver<HmrEvent>>` is written in `start_dev`/`stop_dev` and never read, so watch | `frontend-and-tooling-consolidation:7.1` |
| FE-03 | stub/placeholder + dead code: `IsolatedDeclarationsGenerator::emit_dts` calls `Parser::parse` only to check diagnostics, | `frontend-and-tooling-consolidation:7.3` |
| FE-04 | hot-path performance / memory: `cache: DashMap<u64, CachedRender>` gets one insert per unique `(component, page_json)` w | `frontend-and-tooling-consolidation:7.4` |
| FE-05 | lock/GIL contention: `self.available.pop().ok_or(BundlerError::PoolExhausted)?` fails immediately when every slot is bus | `frontend-and-tooling-consolidation:7.4` |
| FE-06 | allocation/copy: Every render looks up `globals.JSON.parse` and `__ssr_render` by name. Every rebuild clones the whole b | `frontend-and-tooling-consolidation:7.4` |
| FE-07 | allocation/copy + API surface bloat: `Bundler.ssr_render` turns `SsrResult` into `HashMap<String, Py<PyAny>>`. Python th | `frontend-and-tooling-consolidation:7.4` |
| FE-08 | duplication: About 15 hand-written `.map_err(\ | `frontend-and-tooling-consolidation:7.1` |
| FE-09 | over-abstraction / deps: `PySourceWatcher` puts `Mutex` around both the watcher and the receiver. The only channel use i | `frontend-and-tooling-consolidation:7.1` |
| FE-10 | build cost / unsafe: `rquickjs` enables `bindgen`, which needs libclang and regenerates bindings at build time, plus `cl | `frontend-and-tooling-consolidation:7.4` |
| FE-11 | lock/GIL contention: `check_file` parses while holding the GIL; `transform_file` releases it with `py.detach`. | `frontend-and-tooling-consolidation:7.1` |
| FE-12 | standards violation (shim): A re-export shim exists because maturin installs the extension as the top-level module `_bun | `bridge-and-server-consolidation:4.1` |
| FE-13 | stub/placeholder (functional bug): `_run_native(dev=True)` creates a local `bundler`, calls `start_dev`, and returns. Th | `frontend-and-tooling-consolidation:7.2` |
| FE-14 | functional bug (dead path): `_native_watch_loop` calls `watcher.poll_events(timeout_ms)`, but the Rust signature is `pol | `frontend-and-tooling-consolidation:7.2` |
| FE-15 | duplication: `_collect_named_types` is copied byte-for-byte into 6 classes. `_render_interface`/`_type_ref` appear twice | `frontend-and-tooling-consolidation:7.5` |
| FE-16 | layering/organization: One module holds parameter markers + `Parameter()`/`Body()` factories (63-176), JSON-schema→IR co | `frontend-and-tooling-consolidation:7.5` |
| FE-17 | API surface bloat: `generate(routes: Iterable[RouteMetadata] \ | `frontend-and-tooling-consolidation:7.5` |
| FE-18 | standards violation: Five call sites do `msgspec.json.format(encode_json_bytes(x), indent=2).decode()` while `vertebra.u | `frontend-and-tooling-consolidation:7.5` |
| FE-19 | stub/placeholder: Codegen writes `routes.ts` with the constant `export const routes = {};`. | `frontend-and-tooling-consolidation:7.5` |
| FE-20 | duplication + standards: Vite binary resolution is written twice (`_resolve_command` and `run_vite_build`). The dev serv | `frontend-and-tooling-consolidation:7.6` |
| FE-21 | API surface bloat: `vertebra.build` eagerly imports and re-exports 38 names, including constants (`PBS_BASE_URL`, `ONEFI | `frontend-and-tooling-consolidation:7.6` |
| FE-22 | standards violation: `class TypeNode(msgspec.Struct, frozen=True, tag_field="_type", tag=True)` and its subclasses omit  | `frontend-and-tooling-consolidation:7.5` |
| FE-23 | duplication: The shell launcher embeds a second Python implementation of trailer parsing (`"<8sQ32s"`, `VTB1FILE`, ZSTD/ | `frontend-and-tooling-consolidation:7.6` |
| FE-24 | stub/placeholder: `--pgo` / `pgo_enabled` only writes `"pgo_enabled": true` into `manifest.json`. No PGO happens, even t | `frontend-and-tooling-consolidation:7.6` |
| FE-25 | stub/placeholder: `vtb init NAME --template` prints "Initializing project…" and creates nothing. Tests only check that i | `frontend-and-tooling-consolidation:7.6` |
| FE-26 | dead code: `_ = profile`, `_ = verbose`, `_ = log_level`, `_ = file`: the options are accepted and thrown away. | `frontend-and-tooling-consolidation:7.6` |
| FE-27 | duplication + drift: Both build the server env dict by hand. `dev` sets `VERTEBRA_PYTHON_APP` but not `LITESTAR_APP`/`PY | `frontend-and-tooling-consolidation:7.6` |
| FE-28 | build/exports bug: The tsup entry list has `src/react/index.ts`, but the file is `src/react/index.tsx`. The existing `di | `frontend-and-tooling-consolidation:7.7` |
| FE-29 | duplication: `RealtimeChannel` (lazy connect, subscribe map, backoff reconnect) is a subset of `WebSocketClient` (the sa | `frontend-and-tooling-consolidation:7.8` |
| FE-30 | duplication: Four classes each carry their own `reconnectAttempts`/`reconnectTimer`/`scheduleReconnect`/clear-on-close s | `frontend-and-tooling-consolidation:7.8` |
| FE-31 | duplication: Four framework bindings each rebuild the same `{data, loading, error, refresh}` query and `{mutate, loading | `frontend-and-tooling-consolidation:7.8` |
| FE-32 | stub/placeholder: `ArrowStreamClient` stores `endpoint` + `headers` and checks magic bytes, but has no fetch or stream m | `frontend-and-tooling-consolidation:7.8` |
| FE-33 | dead export / docs drift: `types.ts` re-exports `computeBackoffDelay` and nobody imports it from there. The package desc | `frontend-and-tooling-consolidation:7.7` |
| FE-34 | hot-path allocation: `fnv1aStateId`/`fnv1aFieldId` allocate a `new TextEncoder()` per call, and the 64-bit variant uses  | `frontend-and-tooling-consolidation:7.8` |
| FW-01 | stub/placeholder: `SyncPostgresDriver.execute_tuples` returns `list(self._in_memory_rows)`, which is always empty. `Sync | `extensions-and-data-consolidation:6.4` |
| FW-02 | stub/placeholder: `DBAPIConnection.execute_raw_tuples` returns `[]` for any non-SQLite URL. `commit/rollback` are no-ops | `extensions-and-data-consolidation:6.5` |
| FW-03 | stub/placeholder: `Vector`/`BinaryJson` are plain classes with `process_bind_param(value, dialect_name)` signatures and  | `extensions-and-data-consolidation:6.5` |
| FW-04 | stub/placeholder: `export_arrow_ipc` writes `b"ARROW1\x00\x00" + msgpack`, which is not Arrow IPC. `export_parquet` writ | `extensions-and-data-consolidation:6.5` |
| FW-05 | bug: The bind dict contains `rrf_k` and `candidate_limit`, but the SQL inlines both with `int()` and has no `:rrf_k`/`:c | `extensions-and-data-consolidation:6.5` |
| FW-06 | stub/placeholder: The `WasmPluginHost` docstring claims a "WASI 0.2 sandbox with fuel and memory limits". `register_modu | `extensions-and-data-consolidation:6.6` |
| FW-07 | stub/placeholder, duplication: `QueueService` is a `list` with `pop(0)` (O(n)). `QueueConfig.backend_url` ("memory, Valk | `framework-consolidation:5.7` |
| FW-08 | stub/placeholder: `EmailConfig` advertises smtp/resend/sendgrid/mailgun/ses plus host, port and api_key. `EmailService.s | `framework-consolidation:5.7` |
| FW-09 | stub/placeholder, hot-path: The `ShmBrokerTransport` docstring says it is "coordinating across worker processes", but it | `extensions-and-data-consolidation:6.6` |
| FW-10 | stub/placeholder: `ApexLangRunner.compile_spec` ignores `spec_source` and emits two `SET_*` calls plus a comment. ORDS→M | `extensions-and-data-consolidation:6.6` |
| FW-11 | stub/placeholder: `DataFusionEngine.explain_plan` is a substring match of table names in the SQL. No DataFusion is invol | `extensions-and-data-consolidation:6.4` |
| FW-12 | standards violation (SQL construction): `plsql = f"BEGIN {package_procedure}({k} => :{k} …); END;"` uses an agent-suppli | `extensions-and-data-consolidation:6.4` |
| FW-13 | layering/organization: The framework layer re-exports `ChannelsBackend`/`MemoryStore` from `vertebra.extensions.litestar | `framework-consolidation:5.1` |
| FW-14 | hot-path performance (startup): `-X importtime`: `vertebra.framework` takes 117.9 ms cumulative. `framework.channels` ta | `framework-consolidation:5.1` |
| FW-15 | duplication: `SsrResult` and `SsrClient` are defined twice with different Protocol shapes (`_inertia` lacks `invalidate/ | `framework-consolidation:5.5` |
| FW-16 | layering/organization: Private-module plus public-re-export pairs. `_ssr/__init__.py` holds only a docstring containing  | `framework-consolidation:5.5` |
| FW-17 | stub/placeholder: The module docstring says "These are placeholder structs … live in later chapters". `Headers.get` lowe | `framework-consolidation:5.5` |
| FW-18 | duplication, hot-path: FNV-1a is implemented twice in Python and again in Rust. Measured cost: `_make_handler_id` 4.0 µs | `framework-consolidation:5.4` |
| FW-19 | API surface bloat: The root package exports private `_make_handler_id` alongside its public wrapper `handler_id`. `_app. | `framework-consolidation:5.4` |
| FW-20 | API surface bloat, layering: `offload.__all__` exports 8 underscore names. `fastpath` imports 5 of them. `router.__all__ | `extensions-and-data-consolidation:6.1` |
| FW-21 | dead code (redundant wrappers): `LitestarRouteCompiler` and `MiddlewareOffloadCompiler` are one-line delegating classes. | `framework-consolidation:5.2` |
| FW-22 | duplication: Three copies of: install state into dict-or-`update()` state, look up `litestar.di.Provide` with `importlib | `extensions-and-data-consolidation:6.2` |
| FW-23 | duplication, naming: `SQLAlchemyAsyncConfig`/`SQLAlchemySyncConfig`, `SyncDatabaseConfig`/`AsyncDatabaseConfig` and `Pro | `extensions-and-data-consolidation:6.2` |
| FW-24 | API surface bloat (re-export shims): `alchemy` re-exports 11 `extensions.sqlalchemy` symbols. `sqlspec.plugin` re-export | `extensions-and-data-consolidation:6.2` |
| FW-25 | dead code: msgspec is a hard dependency (`dependencies = ["msgspec>=0.18", …]`), yet `StructStub`, `MSGSPEC_INSTALLED` a | `framework-consolidation:5.2` |
| FW-26 | dead code: 0 references outside `utils/`: all of `loop_policy` (no tests either); `ensure_sqlspec/sqlalchemy/advanced_al | `framework-consolidation:5.2` |
| FW-27 | hot-path performance, duplication: On any msgspec `ValueError`/`TypeError` the encoder silently re-encodes with stdlib ` | `framework-consolidation:5.3` |
| FW-28 | standards violation: Five modules import from the private `vertebra.utils.serializers._json` instead of the public packa | `framework-consolidation:5.2` |
| FW-29 | stub/placeholder: Both builders are dict accumulators with no consumer in `src/`. Only the tests reference them. OpenAPI | `framework-consolidation:5.6` |
| FW-30 | hot-path performance: `Provide.parameter_names` calls `inspect.signature` on every access. `Container.resolve` reads `se | `framework-consolidation:5.6` |
| FW-31 | stub/placeholder, duplication: `CsrfConfig`, `RateLimitConfig` and `SecurityConfig` (JWT issuer/audience, IAP) are not e | `framework-consolidation:5.6` |
| FW-32 | duplication, organization: Long `if method == …` chains, with the `{"jsonrpc": "2.0", "id": …}` envelope hand-built 8 ti | `framework-consolidation:5.8` |
| FW-33 | duplication: `query`/`mutation`/`subscription` are three identical decorators that differ only in `kind`. The MCP decora | `framework-consolidation:5.6` |
| FW-34 | hot-path performance (minor): `from_headers` builds a lowercased copy of the whole header dict on every request. The `pu | `framework-consolidation:5.6` |
| FW-35 | hot-path performance, allocation: `__setattr__` recomputes the FNV field id (0.93 µs of the 2.5 µs write) and calls the  | `framework-consolidation:5.6` |
| FW-36 | standards violation, organization: Module-global `_ws_clients` set. Monkey-patches `type_bridge.invalidate` (44-45). Thr | `framework-consolidation:5.5` |
| FW-37 | standards violation, hot-path: The sidecar is spawned with `shell=True` from `f"{runtime} {self._entry}"` (S602) plus `p | `framework-consolidation:5.5` |
| FW-38 | duplication: The `if is_partial and include_fields and key not in include_fields: continue` check is repeated 6 times. C | `framework-consolidation:5.5` |
| FW-39 | naming, stub/placeholder, duplication: `MemoryStore` shadows Litestar's own `MemoryStore`, and its docstring claims "RES | `extensions-and-data-consolidation:6.4` |
| FW-40 | dead code, standards violation: `_FallbackScopeState`, `_FallbackASGIResponse` and the `fallback_get` `SimpleNamespace`  | `extensions-and-data-consolidation:6.1` |
| FW-41 | hot-path performance, duplication: `LitestarRouterTable.match` scans every route for the method and runs a regex on each | `framework-consolidation:5.4` |
| FW-42 | layering/organization, hot-path: 1,365 lines mixing provider configs, five signing schemes and store I/O. The native→`Ob | `extensions-and-data-consolidation:6.3` |
| FW-43 | bug, organization: `SigningConfig.sign_oci_headers(..., timestamp: int = 1_700_000_000)` defaults to a fixed 2023 epoch, | `extensions-and-data-consolidation:6.3` |
| FW-44 | duplication, hot-path: Three prefix-qualifying adapters (`_qualify`/`_unqualify`). `delete_object` does `exists()` then  | `extensions-and-data-consolidation:6.3` |
| FW-45 | hot-path performance, duplication: `UploadTracker` loops use `list.pop(0)` (O(n²)) four times. There are four sync/async | `extensions-and-data-consolidation:6.3` |
| FW-46 | hot-path performance, duplication: Every frame calls `msgspec.msgpack.decode(..., type=spec.input_type)`/`encode`, build | `extensions-and-data-consolidation:6.7` |
| FW-47 | test weakness/duplication: Tautologies: `assert isinstance(ChannelsBackend(history=4), ChannelsBackend)`, `assert Litest | `test-suite-consolidation:9.1`, `test-suite-consolidation:9.2` |
| FW-48 | standards violation: 749 hits. Actionable subset: BLE001 ×16 (e.g. `storage/_store.py:403,1225`, `extensions/litestar/of | `framework-consolidation:5.2` |
| FW-49 | standards violation (lazy exports): The root `__getattr__` caches correctly but defines no `__dir__`, so `dir(vertebra)` | `framework-consolidation:5.1` |
| FW-50 | layering/organization, stub/placeholder: `LitestarEcosystemBridge` bundles unrelated services: MCP, A2A, the stub `Queue | `framework-consolidation:5.8` |
| FW-51 | duplication, naming: `ConfirmationGate` never consumes approved tokens, so one approval authorizes unlimited replays. `A | `extensions-and-data-consolidation:6.6` |
| FW-52 | dead code: Vulture flags 45 methods in `vertebra.db` with no `src/` caller, among them `DatabaseRegistry.get_driver`, `O | `extensions-and-data-consolidation:6.4` |
| FW-53 | correctness: `load_ords_openapi` sets `parameters=dict(op_data.get("requestBody", {}))` and uses it as the MCP `input_sc | `framework-consolidation:5.8` |
| HTTP-01 | `mcp_bridge.rs` is a stub that implies a working feature | `http-ingress-consolidation:3.10` |
| HTTP-02 | 13 ingress modules are unreachable from the server | `http-ingress-consolidation:3.6` |
| HTTP-03 | Python evaluates CORS for a Rust offload spec that Rust never consumes | `http-ingress-consolidation:3.6` |
| HTTP-04 | `compression.rs` has two parallel APIs and a third negotiator elsewhere | `http-ingress-consolidation:3.6` |
| HTTP-05 | A new `HealthEndpoint` is built on every request, so lame-duck never works | `http-ingress-consolidation:3.6` |
| HTTP-06 | The SHM streaming response path is dead in production | `http-ingress-consolidation:3.4` |
| HTTP-07 | `FLAG_*` constants are redefined twice | `http-ingress-consolidation:3.1` |
| HTTP-08 | Five copies of the same mpsc-receiver body wrapper | `http-ingress-consolidation:3.1` |
| HTTP-09 | Request header encoder copies the core codec and can corrupt framing | `core-ipc-consolidation:2.5` |
| HTTP-10 | Response headers are parsed three times with several allocations each | `http-ingress-consolidation:3.4` |
| HTTP-11 | Global `Mutex` and unbounded `VecDeque` on every request completion | `http-ingress-consolidation:3.5` |
| HTTP-12 | Duplicated circuit transition and magic state numbers | `http-ingress-consolidation:3.5` |
| HTTP-13 | `RejectionReason::status` is unused while `response.rs` re-maps statuses | `http-ingress-consolidation:3.6` |
| HTTP-14 | The stream watermark API is never used in production | `http-ingress-consolidation:3.5` |
| HTTP-15 | The adaptive batch policy is a placeholder; the batcher never lingers | `http-ingress-consolidation:3.2` |
| HTTP-16 | Weak tests | `http-ingress-consolidation:3.2`, `test-suite-consolidation:9.4` |
| HTTP-17 | `direct_worker.rs` is production-dead and `DirectRequest` copies `PendingRequest` | `http-ingress-consolidation:3.2` |
| HTTP-18 | Dispatcher is an enum flag plus Option fields | `http-ingress-consolidation:3.3` |
| HTTP-19 | Correlation-ID packing is repeated six times and weakens generation protection | `http-ingress-consolidation:3.1` |
| HTTP-20 | Shard mutex is locked once per item on both dispatch and completion | `http-ingress-consolidation:3.3` |
| HTTP-21 | `dispatch_shm` allocates a `HashMap` per batch | `http-ingress-consolidation:3.3` |
| HTTP-22 | Background tasks leak and shutdown can hang | `http-ingress-consolidation:3.3` |
| HTTP-23 | Worker crash recovery is written but not wired | `http-ingress-consolidation:3.3` |
| HTTP-24 | Most `ServerConfig` fields are never read | `http-ingress-consolidation:3.10` |
| HTTP-25 | A per-connection `peek` result is discarded and the h2c upgrade helpers are dead | `http-ingress-consolidation:3.8` |
| HTTP-26 | Plain and TLS serve paths are duplicated and inconsistent | `http-ingress-consolidation:3.8` |
| HTTP-27 | Docs disagree with the code | `http-ingress-consolidation:3.10`, `knowledge-resync:10.1` |
| HTTP-28 | Alt-Svc advertises an HTTP/3 listener that is never started | `http-ingress-consolidation:3.8` |
| HTTP-29 | Router hashes the method string on every request; core has a parallel `Router` | `http-ingress-consolidation:3.2` |
| HTTP-30 | Per-request path and query `String` copies | `http-ingress-consolidation:3.2` |
| HTTP-31 | Round-robin shard counter is a global atomic hit per request | `http-ingress-consolidation:3.6` |
| HTTP-32 | Hand-rolled SHA-256 and a byte-scanning DER "parser" | `http-ingress-consolidation:3.8` |
| HTTP-33 | The profiling endpoint returns placeholder data | `http-ingress-consolidation:3.7` |
| HTTP-34 | Access logger buffers without bound and emits invalid JSON | `http-ingress-consolidation:3.7` |
| HTTP-35 | Pub/sub and SSE fan out the same event three ways | `http-ingress-consolidation:3.9` |
| HTTP-36 | gRPC spawns a task to emit frames it already has | `http-ingress-consolidation:3.9` |
| HTTP-37 | `main.rs` duplicates app import logic and ships a demo route table | `http-ingress-consolidation:3.10` |
| HTTP-38 | Unused and oversized Cargo dependencies and features | `http-ingress-consolidation:3.10` |
| HTTP-39 | Module names hide what modules contain; dead response collector | `http-ingress-consolidation:3.2` |
| HTTP-40 | `response.rs` mixes four concerns | `http-ingress-consolidation:3.6` |
| HTTP-41 | Benchmark gaps and duplicate bench bodies | `review-infrastructure:1.6`, `http-ingress-consolidation:3.10`, `hot-path-performance:8.1` |
| HTTP-42 | Discovery cache uses a `DashMap` for three fixed paths | `http-ingress-consolidation:3.7` |
| HTTP-43 | Static files are read fully into memory and stat'ed per request | `http-ingress-consolidation:3.7` |
| HTTP-44 | SSE wrapper functions and per-event allocations | `http-ingress-consolidation:3.9` |
| HTTP-45 | HMR re-implements the WebSocket handshake | `http-ingress-consolidation:3.9` |
| MCP-01 | Native `McpServer` is a partial, unmounted MCP implementation | `framework-consolidation:5.8` |
| MCP-02 | MCP protocol version drift across layers | `http-ingress-consolidation:3.10`, `framework-consolidation:5.8` |
| MCP-03 | Duplicate stdio bridge, Python and Rust, both weaker than litestar-mcp's | `http-ingress-consolidation:3.10`, `framework-consolidation:5.8` |
| MCP-04 | Native A2A server and card are pre-1.0 and stateful in process | `framework-consolidation:5.8` |
| MCP-05 | `LitestarEcosystemBridge` is a placeholder, and its attach helper silently no-ops on real Litestar apps | `framework-consolidation:5.8` |
| MCP-06 | Decorator names collide with litestar-mcp but behave differently | `framework-consolidation:5.8` |
| MCP-07 | Rust discovery cache and SPA exclusions are unwired and hardcoded | `http-ingress-consolidation:3.7` |
| MCP-08 | `vertebra.framework` eagerly imports protocol stubs | `framework-consolidation:5.1` |
| MCP-09 | Apex extension produces non-executable MCP tools from stub types | `framework-consolidation:5.8` |
| MCP-10 | Tests validate dead surface and fake state instead of wire behavior | `framework-consolidation:5.8` |
| OKF-01 | bug: `(internal_id << 8) \ | `http-ingress-consolidation:3.1` |
| OKF-02 | bug: Zero-copy reads trust `SlotHeader::len` written by the worker process; a corrupted or hostile worker causes out-of- | `core-ipc-consolidation:2.1` |
| OKF-03 | bug: `select_ring` picks the smallest tier with `total_size <= slot_size`, but `try_send_with` rejects `len > slot_size  | `core-ipc-consolidation:2.1` |
| OKF-04 | bug: `headers_raw.len() as u16` truncates header blocks over 64 KiB (same as CORE-04). | `core-ipc-consolidation:2.1` |
| OKF-05 | bug: `./wasm` and `./arrow` exports have no tsup entries (same as FE-28). | `frontend-and-tooling-consolidation:7.7` |
| OKF-06 | tooling gap: Tests are gated on the `http3` feature, which no Makefile target or CI job enables. | `frontend-and-tooling-consolidation:7.9` |
| OKF-07 | stub/placeholder: Supervision and crash-recovery code has no non-test caller. | `http-ingress-consolidation:3.3` |
| OKF-08 | tooling bug: `vtb-ft-native-4w` silently becomes an `echo` placeholder when `src/rs/target-ft/release/vertebra-server` i | `review-infrastructure:1.6` |
| PYB-01 | stub/placeholder: Four producers emit `x-vertebra-sendfile` / `-range` headers; `rg -i "x-vertebra\ | `bridge-and-server-consolidation:4.3` |
| PYB-02 | hot-path performance: `VERTEBRA_WORKERS` is the process count in Rust, but Python passes it as `worker_count` → `pool_si | `bridge-and-server-consolidation:4.6` |
| PYB-03 | lock/GIL contention: `submit_raw_response` calls `send_shm_response_detached` (which can call `send_response_blocking`)  | `bridge-and-server-consolidation:4.2` |
| PYB-04 | lock/GIL contention: `Response.body` getter runs `std::fs::File::open/seek/read_exact` and `std::fs::read` with the GIL/ | `bridge-and-server-consolidation:4.3` |
| PYB-05 | duplication: `submit_results` and `submit_response` duplicate SHM prep, the `(bool, TransportError)` error mapping (148- | `bridge-and-server-consolidation:4.2` |
| PYB-06 | allocation/copy: Every response body is copied into `Bytes` (`Bytes::copy_from_slice`) and then copied again into the SH | `hot-path-performance:8.5` |
| PYB-07 | hot-path performance: Async mode submits one response per FFI call; each `submit_response` flushes the response notifier | `bridge-and-server-consolidation:4.7`, `hot-path-performance:8.6` |
| PYB-08 | duplication: `AsyncRuntime` reimplements `spawn`, `spawn_batch`, `schedule`, `schedule_batch`, `stop` of `AsgiEventLoopP | `bridge-and-server-consolidation:4.6` |
| PYB-09 | naming: `AsgiEventLoopPool.loop` property advances the round-robin counter on every read (`return self.next_loop()`); `_ | `bridge-and-server-consolidation:4.6` |
| PYB-10 | allocation/copy: `schedule`/`schedule_batch` wrap each coroutine in an extra `_wrap()` coroutine plus a Task plus a clos | `bridge-and-server-consolidation:4.6` |
| PYB-11 | over-abstraction: ~90 lines of hand-rolled coroutine stepping (`_drive_coro`, `_step_coro`, `_handle_yielded`, `_resume_ | `bridge-and-server-consolidation:4.8` |
| PYB-12 | allocation/copy: A new closure (sync) or coroutine function (async/RSGI/WSGI) is defined per request inside the dispatch | `bridge-and-server-consolidation:4.7` |
| PYB-13 | hot-path performance: `_handler_dispatch_spec` (an `lru_cache` keyed on the handler) is called 2x per request (partition | `bridge-and-server-consolidation:4.7` |
| PYB-14 | duplication: 14 inline constructions of 500/404 `Response(status=..., body=b"internal server error", ...)` with inconsis | `bridge-and-server-consolidation:4.2`, `bridge-and-server-consolidation:4.7` |
| PYB-15 | dead code: The blocking `async_mode=False` branch of `_dispatch_batch` is used only by tests (`test_rsgi_protocol.py:258 | `bridge-and-server-consolidation:4.7` |
| PYB-16 | dead code: `_handler_params` has 0 references; `_handler_meta`, `_resolve_handler_args`, `_call_handler` are referenced  | `bridge-and-server-consolidation:4.7` |
| PYB-17 | stub/placeholder: `run_worker(error_handler=...)` is accepted and discarded (`_ = error_handler`); `_dispatch_batch(hand | `bridge-and-server-consolidation:4.7` |
| PYB-18 | dead code: Python branches on "all defaults → `init_transport()` else pass same values"; Rust discards `ring_name` (`let | `bridge-and-server-consolidation:4.1` |
| PYB-19 | layering/organization: SHM loop decides whether mounts are already instantiated via `type(adapter).__name__ in {"AsgiRun | `bridge-and-server-consolidation:4.7` |
| PYB-20 | hot-path performance: Each wake does `asyncio.wait_for(ready_event.wait(), timeout)` (timeout handle + Event wait + clea | `bridge-and-server-consolidation:4.7`, `hot-path-performance:8.7` |
| PYB-21 | duplication: Two protocol detectors with different rules: `_detect_protocol` uses `>= 3` params and defaults to `"wsgi"` | `bridge-and-server-consolidation:4.7` |
| PYB-22 | standards violation: Re-export shims: `_native.__all__` re-exports `AsgiEventLoopPool`, `GilAuditReport`, `is_free_threa | `bridge-and-server-consolidation:4.7` |
| PYB-23 | layering/organization: Server core imports `vertebra.extensions.litestar` at module top (pulls Litestar integration into | `bridge-and-server-consolidation:4.8` |
| PYB-24 | stub/placeholder: `FastApiExtension.try_handle` always returns `None`; the extension registry (`register_extension`/`get | `bridge-and-server-consolidation:4.8` |
| PYB-25 | stub/placeholder: `scope["extensions"]` advertises `zerocopysend`, `trailers`, `early_hint`, `debug`, `tls`, `pathsend`; | `bridge-and-server-consolidation:4.8` |
| PYB-26 | dead code: Python dict `AsgiScope` + `build_scope` fallback; native `AsgiScope` is imported unconditionally at `_adapter | `bridge-and-server-consolidation:4.8` |
| PYB-27 | duplication: `run_single` and `handle_path` duplicate scope/sender setup; `run_stream_session` reaches into `self._adapt | `bridge-and-server-consolidation:4.8` |
| PYB-28 | allocation/copy: `AsgiReceive` reads `request.body` (fresh `PyBytes` copy of the slab per call); `Request.body_view` (ze | `bridge-and-server-consolidation:4.4` |
| PYB-29 | hot-path performance: `ensure_headers` builds `HashMap<String,String>` (2 allocs/header) and drops duplicate headers (la | `bridge-and-server-consolidation:4.5` |
| PYB-30 | allocation/copy: `__len__` and `__iter__` call `collect_keys`, which `format!`s every `HTTP_*` key and clones every stat | `bridge-and-server-consolidation:4.5` |
| PYB-31 | duplication: Three WSGI environs/inputs (Rust `WsgiEnviron`/`WsgiInput`, Python `WsgiEnviron`/`BodyStream`) and three ru | `bridge-and-server-consolidation:4.5` |
| PYB-32 | lock/GIL contention: File wrappers are stat'ed up to 3x (`Path::is_file` under GIL in `WsgiFileWrapper::py_new`, `Path(p | `bridge-and-server-consolidation:4.3` |
| PYB-33 | stub/placeholder: `LargeBodyBypass` parses `CONTENT_LENGTH` and only logs; it bypasses nothing. | `bridge-and-server-consolidation:4.5` |
| PYB-34 | duplication: `RsgiHeaders` (owned `Vec<(String,String)>`, 2 allocs/header at scope build) duplicates `Headers` (zero-cop | `bridge-and-server-consolidation:4.4` |
| PYB-35 | allocation/copy: `RsgiScope::from_batch` eagerly copies 8 `String`s + headers per request even if the app reads only `pa | `bridge-and-server-consolidation:4.4` |
| PYB-36 | duplication: `run_single` copies the body twice (`request.body` → `PyBytes`, then `RsgiHttpProtocol(body=...)` → `Bytes: | `bridge-and-server-consolidation:4.9` |
| PYB-37 | stub/placeholder: `client_disconnect()` resolves immediately (apps awaiting it think the client left at once); `response | `bridge-and-server-consolidation:4.9` |
| PYB-38 | stub/placeholder: `RsgiWebsocketProtocol` is an in-memory fake (preloaded `incoming` list, `sent_messages()` accessor);  | `bridge-and-server-consolidation:4.9` |
| PYB-39 | allocation/copy: Every `send_bytes`/`send_str`/`client_disconnect` allocates a new `_RsgiReadyAwaitable` pyclass. | `bridge-and-server-consolidation:4.9` |
| PYB-40 | dead code: `#[allow(dead_code)] fn _assert_py_string`. | `bridge-and-server-consolidation:4.1` |
| PYB-41 | dead code: `pub async fn read_file_range_chunks` has 0 callers; it is the only user of `tokio`, `tokio-util`, `futures-u | `bridge-and-server-consolidation:4.1` |
| PYB-42 | allocation/copy: `stream_file_chunks` reads the whole range into `Vec<Vec<u8>>` (one `to_vec` per chunk) then copies eac | `bridge-and-server-consolidation:4.3` |
| PYB-43 | duplication: Three `from_parts` builders duplicate the method-tag table (with different unknown fallbacks: `0`, `255`, ` | `bridge-and-server-consolidation:4.4` |
| PYB-44 | duplication: `HeadersIterator::__next__` reimplements the compact-header parser instead of using `iter_raw_headers`; all | `bridge-and-server-consolidation:4.4` |
| PYB-45 | allocation/copy: `Request.headers` copies the header bytes out of the slab (`Bytes::copy_from_slice`) and clones a `Head | `bridge-and-server-consolidation:4.4`, `hot-path-performance:8.5` |
| PYB-46 | dead code: `RequestBatch.submit_buffers` has 0 references; `Response.from_buffer` duplicates `Response.__new__` exactly; | `bridge-and-server-consolidation:4.1` |
| PYB-47 | docs drift: Stub declares `AsgiScope.pop`, `WsgiEnviron.pop`, `RequestBatchIterator.submit_buffers` (none exist in Rust) | `bridge-and-server-consolidation:4.1` |
| PYB-48 | dead code: `_coerce_response` catches `AttributeError` on `raw.request_id = request_id`, but `Response` defines a `reque | `bridge-and-server-consolidation:4.7` |
| PYB-49 | lock/GIL contention: `WsgiStartResponse.__call__` holds a `std::sync::Mutex` across Python API calls (`exc.get_item(1)`, | `bridge-and-server-consolidation:4.5`, `bridge-and-server-consolidation:4.9` |
| PYB-50 | stub/placeholder: `wsgi.errors` is `sys.stderr` captured once per process (`PyOnceLock`); later reassignment (logging re | `bridge-and-server-consolidation:4.5` |
| PYB-51 | stub/placeholder: `quarantine_unsafe_modules` logs "falling back to standard asyncio loop" but changes nothing; `audit(b | `bridge-and-server-consolidation:4.6` |
| PYB-52 | duplication: `is_free_threaded` and `_is_gil_currently_enabled` both wrap `sys._is_gil_enabled`. | `bridge-and-server-consolidation:4.6` |
| PYB-53 | stub/placeholder: `ShmAllocationPolicy` (fields `shm_dir`, `min_free_bytes` never read), `CgroupQuota`, `LameDuckControl | `bridge-and-server-consolidation:4.10` |
| PYB-54 | standards violation: Uptime is computed from `time.time()` (wall clock jumps under NTP). | `bridge-and-server-consolidation:4.10` |
| PYB-55 | duplication: Docstring claims a "Non-blocking GIL-releasing" logger, but `LogSink` is a pure-Python `deque` with no writ | `bridge-and-server-consolidation:4.10` |
| PYB-56 | duplication: Python `TraceContext.parse/format` and `SqlTracePropagator.sanitize_sql` duplicate Rust `W3cTraceContext` a | `bridge-and-server-consolidation:4.10` |
| PYB-57 | hot-path performance: `GenAiRecorder._samples` grows unbounded (no drain, no cap); `samples` copies to a tuple per read. | `bridge-and-server-consolidation:4.10` |
| PYB-58 | standards violation: The bundler extension is installed as top-level `_bundler` and re-exported through a shim; no stub  | `bridge-and-server-consolidation:4.1` |
| PYB-59 | stub/placeholder: `_ipc` docstring: "exists as a namespace for future Python-side IPC helpers"; `_js` is an empty packag | `bridge-and-server-consolidation:4.10` |
| PYB-60 | test weakness/duplication: Module docs list 4 of 25 exports; `free_threaded_const_exists` asserts nothing (`let _ = FREE | `bridge-and-server-consolidation:4.1` |
| PYB-61 | test weakness/duplication: Tests assert signatures (`"loop" in sig.parameters`), `callable(...)`, `hasattr(Request, "asg | `test-suite-consolidation:9.1`, `test-suite-consolidation:9.2` |
| PYB-62 | standards violation: 235 hits; relevant: `PLC0415` import-outside-top-level ×16 (e.g. `_native.py:197-198,327,361,567,67 | `bridge-and-server-consolidation:4.7` |
| TOOL-01 | broken CI step: The "Commit History" step runs `git add tools/benchmarks/results/*.duckdb`, but `.gitignore` ignores `*. | `frontend-and-tooling-consolidation:7.9` |
| TOOL-02 | duplicated builds: The benchmark job runs `make install` (destroy venv, `clean` deletes `src/rs/target`, then `dev` debu | `frontend-and-tooling-consolidation:7.9` |
| TOOL-03 | duplicated builds: `dev` runs `cargo build --workspace --exclude vertebra-py --exclude vertebra-bundler --exclude verteb | `frontend-and-tooling-consolidation:7.9` |
| TOOL-04 | missing benchmark tooling: Criterion benches exist (`vertebra-core/benches/{spsc,mpmc,spin_wait,router,guards,ring_throu | `review-infrastructure:1.6` |
| TOOL-05 | duplication + bogus dep: `[project.optional-dependencies].dev` (pytest, `pytest-anyio>=0.0.0`, ruff, mypy) duplicates `[ | `frontend-and-tooling-consolidation:7.9` |
| TOOL-06 | weak gates: Ruff selects only `E,F,I,UP,B,SIM`, so standards the repo enforces by review (no inline comments, docstrings | `frontend-and-tooling-consolidation:7.9` |
| TOOL-07 | duplication: `test-rust` runs `make rs-test` (`cargo test --workspace --features tls,compression`) on ubuntu, and the `t | `frontend-and-tooling-consolidation:7.9` |
| TOOL-08 | duplication: Internal-mirror registry detection and overrides are written in both the Makefile preamble and `setup-env.sh`. | `frontend-and-tooling-consolidation:7.9` |
| TOOL-09 | ergonomics: `make lint` applies Biome and Ruff `--unsafe-fixes` automatically. `py-test -v` prints 571 lines per run. | `frontend-and-tooling-consolidation:7.9` |
| TOOL-10 | missing gate: CI never runs `bun run build` for TS, so FE-28 (missing react/wasm/arrow outputs) cannot fail CI. There is | `frontend-and-tooling-consolidation:7.7` |
| TST-01 | test weakness: Import-only tests: `assert BuildOrchestrator is not None` ×12, `assert callable(...)`, `assert isinstance | `test-suite-consolidation:9.1` |
| TST-02 | test weakness (brittle): `assert "to_thread" not in inspect.getsource(store_mod)` checks source text, not behavior. | `test-suite-consolidation:9.2` |
| TST-03 | ad-hoc test files: These files are named after PR waves ("stage1", "drop_in_names", "*_and_*") rather than modules, and  | `test-suite-consolidation:9.2` |
| TST-04 | test weakness (mega-tests): Single tests named `test_queues_channels_stores_email_and_procedures`, `test_mcp_a2a_wasm_an | `test-suite-consolidation:9.3` |
| TST-05 | misplaced tests: `vertebra.build` tests are spread over `unit/framework/`, `unit/codegen/`, `unit/extensions/`, and `uni | `test-suite-consolidation:9.3` |
| TST-06 | test weakness + standards: `sys.path.insert(0, repo_root)` hack, 17 function-scoped imports, and tautologies against con | `test-suite-consolidation:9.3` |
| TST-07 | dead code + standards + perf: 5 of 6 fixtures are unused (`codegen_dir`, `shm_name`, `mock_asgi_app`, `asgi_send`, `asgi | `test-suite-consolidation:9.3` |
| TST-08 | stub/placeholder: The integration tree has 8 Floci emulator fixtures and zero tests. `pytest` has no `integration` marke | `test-suite-consolidation:9.3` |
| TST-09 | standards violation: 131 `PLC0415` function-scoped imports in tests (`ruff --select PLC0415`). | `test-suite-consolidation:9.3` |
| TST-10 | test weakness (mocks hide bugs): Every orchestrator test patches `_run_bundler` or the bundler module, and no test cover | `frontend-and-tooling-consolidation:7.2` |
| TST-11 | test weakness: The defaults tests re-assert literal defaults. `test_hmr_watcher_creation` asserts only `result.is_ok()`, | `frontend-and-tooling-consolidation:7.4` |
| TST-12 | test organization / build cost: 10 `*_tests.rs` + 8 `test_*.rs` files. Each one compiles and links as its own binary aga | `test-suite-consolidation:9.4` |
| TST-13 | test organization: Two layouts. `client.test.ts` also tests `QueryCache` (react) and `initHtmx` (htmx). `arrow.test.ts`/ | `test-suite-consolidation:9.5` |


## Rust core, storage, and db (prefix CORE)

#### Correctness and lock-free smells (top priority)

**CORE-01 — MPMC `try_recv` permanently wedges the ring on `BufferTooSmall`.** `src/rs/vertebra-core/src/channel.rs:1563`, `:1596` · category: hot-path performance / correctness
- Evidence: after a successful `head` CAS, `let len = unsafe { self.read_slot(index, out)? };`. The `?` returns before `self.sequence(index).store(head + self.mask + 1, Release)`. The claimed slot's sequence is never released. Once the ring wraps, producers see `diff < 0` and report `Full` forever. `try_recv_with_flags` has the same bug.
- Proposed change: check `len <= out.len()` against the `SlotHeader` before the CAS (peek), or always publish the sequence and then return the error. Fold both functions into one `MpmcRing::claim_read(|slot, flags| ...)` primitive (see CORE-07).
- Risk: low. This fixes a deadlock; the success path is unchanged.
- Verify: add `mpmc_buffer_too_small_does_not_wedge` to `src/rs/vertebra-core/tests/test_mpmc.rs`; `cargo test -p vertebra-core mpmc_`.

**CORE-02 — MPMC `try_send_with` trusts the closure's returned length; a panic wedges the slot.** `src/rs/vertebra-core/src/channel.rs:1506-1520` · correctness
- Evidence: `let written = f(payload_slice); (*header).len = written as u32;`. Nothing checks `written <= payload_capacity()`, and readers call `from_raw_parts(payload_ptr, len)` (`:1672`). A bad return value causes an out-of-bounds read across slots. If `f` panics after the CAS, the sequence is never stored and every consumer stalls at that index. The production caller is `ShmBatchWriter::serialize_into` in `vertebra-http/src/dispatcher/shm.rs:358`.
- Proposed change: `assert!(written <= cap)` (or clamp and return `PayloadTooLarge`). Add a drop guard that publishes the sequence with `len = 0` and a poison flag if `f` unwinds. Mirror the guard in `SpscRing::try_send_with` for symmetry.
- Risk: low.
- Verify: `cargo test -p vertebra-core mpmc_try_send_with`, with new tests for an oversized return and for a panic under `catch_unwind`.

**CORE-03 — SHM `submit_raw_response` frames (ResponseSlot) are never decoded by the HTTP response reader.** `src/rs/vertebra-core/src/transport/ring_pair.rs:242-266`, `src/rs/vertebra-core/src/ipc.rs:24-170` · correctness / duplication
- Evidence: two response wire formats share the same tiered rings:
  - VRES `ResponseData` (`ring_pair.rs:31-77`), written by `Transport::send_response` (`vertebra-py/src/api.rs:443`).
  - `ResponseSlot` (`ipc.rs`), written through `send_response_bytes` (`vertebra-py/src/api.rs:416`).
  
  `vertebra-http/src/response_reader.rs:271` only calls `try_recv_response()`, which rejects anything without the VRES magic (`"bad magic"`), logs a warning, and breaks the drain loop. `Dispatcher::handle_raw_response_slot` (`vertebra-http/src/dispatcher/mod.rs:229`) has no caller in any crate.
- Proposed change: standardize on one frame, `ResponseSlot` (it already carries streaming/trailer flags). Delete `ResponseData`, `RESPONSE_MAGIC`, `write_response_slot` and `send_response`/`send_response_blocking`. `TieredRingSet::try_recv_with(|frame| ...)` should hand raw slices to `handle_raw_response_slot`.
- Risk: high. This is a cross-crate wire change that touches http, py and core.
- Verify: new SHM end-to-end test in `vertebra-http/tests/streaming_body_tests.rs` driving `submit_raw_response` through `RingPair`; `make rs-test && make py-test`.

**CORE-04 — `write_response_slot` silently truncates `headers_len` to u16.** `src/rs/vertebra-core/src/transport/ring_pair.rs:191`
- Evidence: `let headers_len = headers_raw.len() as u16;` while the copy writes the full `headers_raw.len()`. Headers larger than 64 KiB produce a frame where the decoder reads the body from the wrong offset. `total_size` (`:210`) uses the untruncated length.
- Proposed change: return `RingPairError::InvalidResponse("headers > 65535")`. This goes away with CORE-03.
- Risk: low.
- Verify: `cargo test -p vertebra-core tiered_ring_set`.

**CORE-05 — `BufferPool` (slot pool) creates aliasing `&mut Vec<u8>` across live buffers.** `src/rs/vertebra-core/src/buffer.rs:93`, `:208`, `:216` · correctness
- Evidence: `storage: UnsafeCell<Vec<u8>>`, and `as_mut_slice` does `let storage: &mut Vec<u8> = unsafe { &mut *self.pool.storage.get() };`. Two `Buffer`s on two threads (the `buffer_pool_concurrent_acquire_release` test does exactly this) each create a `&mut` to the whole Vec. That is UB under Stacked Borrows. The free list is also a `Mutex<Vec<usize>>`, and `release` does an O(n) `contains` in debug builds (`:172`).
- Proposed change: the only non-test user is the dead `BufferChannel` (CORE-12). Delete `BufferPool`/`Buffer`/`BufferHandle`/`acquire_multi`/`release_multi` with it. If a slot pool is kept, store `Box<[UnsafeCell<u8>]>` and derive slices from a raw pointer.
- Risk: low (the type is dead in production).
- Verify: `cargo +nightly miri test -p vertebra-core buffer_pool` before the change shows UB; after deletion, `cargo test -p vertebra-core`.

**CORE-06 — The futex fallback turns blocking `recv`/`send` into hard errors.** `src/rs/vertebra-core/src/sync.rs:172-187`, `src/rs/vertebra-core/src/platform/mod.rs:17-46`
- Evidence: `wait_on_address` returns `Err(SyncError::Unsupported)` once the fallback is enabled. `SpscRing::send`/`recv`/`send_with` propagate it with `result?` (`channel.rs:365`, `:507`). Only the timeout variant has a sleep fallback (`fallback_wait_timeout`, `sync.rs:221`). On unsupported platforms, blocking calls fail instead of degrading.
- Proposed change: make `wait_on_address` call `fallback_wait_timeout(word, expected, Duration::MAX-ish loop)` too. Delete the flock `FileLock` path (`sync.rs:30-97`), whose only user is the dead `SharedMutex` (CORE-11).
- Risk: low.
- Verify: `cargo test -p vertebra-core sync::`.

#### Duplication inside rings and transport

**CORE-07 — The futex park/re-check loop is copy-pasted 10 times in `SpscRing`.** `src/rs/vertebra-core/src/channel.rs:346-370, 372-402, 404-444, 481-512, 514-552, 615-646, 649-675, 717-743, 746-783, 785-823` · duplication
- Evidence: each copy has the same sequence: `expected = wait_word.load(SeqCst)` → `waiters.fetch_add` → re-check heads → `wait_on_address[_timeout]` → `fetch_sub`. Seven copies are receive-side and three are send-side, and the try-recv publish block (`store head; fetch_add wait word; if waiters>0 wake`) appears 5 more times. This is the code the memory notes as subtle (the SeqCst handshake), so duplicating it multiplies the risk.
- Proposed change: add private `SpscRing::park_until(&self, side: Side, deadline: Option<Instant>) -> Result<bool, ChannelError>`, `publish_write(next)` and `publish_read(next, count)`. All public `recv*`/`send*` become 3-line wrappers over `try_*`. Expected size drops by about 450 lines.
- Risk: medium. The lock-free ordering must be preserved exactly.
- Verify: `cargo test -p vertebra-core spsc_` plus `cargo bench -p vertebra-core --bench spsc` (no regression on `spsc_ping_pong` or `spsc_recv_hybrid_pure_wakeup`).

**CORE-08 — Dead SPSC/MPMC API variants (ext=0).** `src/rs/vertebra-core/src/channel.rs:404` `send_timeout`, `:464` `try_recv_with_flags`, `:649` `recv_with_flags`, `:717` `recv_view`, `:746` `recv_view_timeout`, `:785` `recv_timeout_with_flags`, `:843` / `:1783` `try_recv_batch` (+ `read_slot_to_vec` `:887`), `:978` / `:1750` `read_slot_with_flags`, `:1581` MPMC `try_recv_with_flags`, `:214` / `:1365` `from_region` as `pub`, `:143` `RingStats` (used only by `stats()` in one http log line) · dead code / API surface bloat
- Evidence: `rg` counts outside `channel.rs`: `send_timeout` 0, `try_recv_with_flags` 0, `recv_with_flags` 0, `recv_view` 0, `recv_view_timeout` 0, `recv_timeout_with_flags` 0, `try_recv_batch` 0 (7 inline tests only), `from_region` 0. `recv_hybrid` is used only by benches and tests.
- Proposed change: delete them. Collapse the receive surface to `try_recv(out) -> (len, flags)`, `try_recv_view`, `try_recv_with(reader)` and `recv_timeout_with(deadline, reader)`. Make `from_region` private.
- Risk: low.
- Verify: `cargo build --workspace --all-targets && make rs-test`.

**CORE-09 — `SlotView`'s borrowed flag is only half-enforced.** `src/rs/vertebra-core/src/channel.rs:94-110`, `:446-462`, `:685-714`
- Evidence: `try_recv_view` CASes `slot_borrowed`, but `try_recv`/`recv*` never check it. Copy-receiving while a view is alive reads the same slot and advances `read_head` twice; `SlotView::release` then stores `slot_index+1`, which rewinds the head. The SPSC contract makes this caller misuse, but the guard suggests a safety it does not provide.
- Proposed change: after CORE-08, the only view consumer is `TieredRingSet::try_recv_response`. Either check `slot_borrowed` in every receive path, or drop the flag and document single-consumer use. Release could use `read_head.fetch_add`-style advancement from the current head.
- Risk: low.
- Verify: `cargo test -p vertebra-core slot_view`.

**CORE-10 — MPMC has no wake path; idle workers spin and yield inside `poll_batch`.** `src/rs/vertebra-core/src/channel.rs:1535-1546`, `:1614-1644`, `:1693-1715`; `src/rs/vertebra-core/src/transport/mod.rs:79-91` · hot-path performance
- Evidence: `MpmcRing::recv`/`recv_timeout`/`recv_timeout_with` loop on `crossbeam_utils::Backoff::snooze()`, which degrades to `thread::yield_now()`. `Transport::poll_batch` (SHM backend) calls `recv_timeout_with(timeout, ...)`, and Python calls it inside `py.detach` with `timeout_ms` (`vertebra-py/src/api.rs:91`). Each idle worker burns a core for the whole timeout. The request side already has `request_notifier` (eventfd) and a futex word in the header (`wait_write`), and MPMC uses neither.
- Proposed change: give `MpmcRing` the same `recv_waiters` / `wait_write` handshake as SPSC (shared `park_until`, CORE-07), or park on `request_notifier.wait()` after a bounded spin.
- Risk: medium (wakeup protocol).
- Verify: new `mpmc_idle_poll_cpu` criterion or a `getrusage` test; `cargo bench -p vertebra-core --bench shm_transport` (`transport_parity/shm_ring_roundtrip`).

#### Dead modules and dead crates

**CORE-11 — `SharedMutex`, `SharedRwLock`, `SharedCondvar`, `WaitWord` and the flock fallback are unused.** `src/rs/vertebra-core/src/sync.rs:30-97`, `:133-170`, `:236-610` · dead code
- Evidence: `rg SharedMutex|SharedRwLock|SharedCondvar|WaitWord` matches only inside `sync.rs`. `rustix` is used only by `FileLock` (`rg rustix::` → `sync.rs` only).
- Proposed change: shrink `sync.rs` to `SyncError` + `wait_on_address[_timeout]` + `wake_one`/`wake_all` (about 120 lines). Merge it into `platform/mod.rs` as `platform::futex`. Drop the `rustix` dependency (`vertebra-core/Cargo.toml:35`).
- Risk: low.
- Verify: `cargo build -p vertebra-core && make rs-test`.

**CORE-12 — `BufferChannel` / `BufferSender` / `BufferReceiver` / `BufferMessage` and SPSC `split()` handles are unused.** `src/rs/vertebra-core/src/channel.rs:1017-1271`, `:267-278` · dead code
- Evidence: ext=0 for `BufferChannel`, `BufferSender`, `BufferReceiver`, `BufferMessage`, `SpscSender` and `SpscReceiver`. Each also carries `#[allow(dead_code)]` fields (`:1141`).
- Proposed change: delete them (about 260 lines), and `buffer::BufferPool` with them (CORE-05).
- Risk: low.
- Verify: `cargo test -p vertebra-core`.

**CORE-13 — `logger.rs`, `telemetry.rs`, `guards.rs`, `protocol/state.rs`, `router::Router` and `protocol::{RequestHeader, ResponseHeader, ControlMessageKind, response_flags}` have no production consumer.** `src/rs/vertebra-core/src/logger.rs:1-246`, `telemetry.rs:1-156`, `guards.rs:1-169`, `protocol/state.rs:1-530`, `router.rs:41-76`, `protocol/mod.rs:24-150`, `:186-229` · stub/placeholder
- Evidence:
  - `rg AsyncLogCore|W3cTraceContext|sanitize_sql_statement|SpanRecord` matches only the defining files. None are exposed by `vertebra-py`.
  - `protocol::state` symbols are referenced only inside `state.rs`. The Python `StateDeltaFrame` (`src/py/vertebra/framework/live/__init__.py:90`) has its own encoder.
  - `guards` is used only by `benches/guards.rs`.
  - `router::Router` is used only by `benches/router.rs`; `vertebra-http/src/router.rs` builds its own `matchit::Router` per method.
  - `RequestHeader`/`ResponseHeader`/`ControlMessageKind` are referenced only in `protocol/mod.rs`.
  
  These modules were added by feature waves `2f5e19a` (telemetry/logger) and `68ee07b` (live state). They duplicate Python implementations and are never called.
- Proposed change: delete all of them. Move `normalize_typed_path` (`router.rs:78-190`) into `vertebra-http/src/router.rs`, its only caller. If Rust trace parsing is wanted later, implement it where ingress reads headers (`vertebra-http`), not in core. This keeps every feature because the Python paths remain.
- Risk: low.
- Verify: `cargo build --workspace --all-targets && make rs-test && make py-test`.

**CORE-14 — The `cleanup.rs` registry is never populated, so `cleanup_all()` is a no-op.** `src/rs/vertebra-core/src/cleanup.rs:1-6`, `:31-35`; caller `vertebra-http/src/ring_factory.rs:318` · stub/placeholder / docs drift
- Evidence: the module doc says entries are "added by `ShmRegion::create_named` via `register_name`". `create_named` (`shm.rs:142-186`) never calls it, and `rg register_name` matches only `cleanup.rs`. Production regions are anonymous memfd, or shm_open unlinked immediately (`shm.rs:370`), so nothing needs cleanup.
- Proposed change: delete `cleanup.rs`, the http call, and `ShmRegion::{create_named, open_named}` + `shm_unlink_named` (used only by tests and the dead `sync.rs`), along with `tests/test_shm.rs::named_*`. If named regions are needed later, register them in `create_named`.
- Risk: low.
- Verify: `make rs-test`.

**CORE-15 — The whole `vertebra-db` crate is unused and duplicates Python.** `src/rs/vertebra-db/src/lib.rs:1-523`, `src/rs/Cargo.toml:9` · dead code / duplication
- Evidence: `crate-type = ["rlib"]`, with no dependent in any `Cargo.toml` and no PyO3 module. The same logic lives in `src/py/vertebra/db/dialer/{cloudsql,alloydb}.py`, `db/redis.py`, `db/valkey.py` and `db/oracle/wallet.py` (for example `AlloyDbDialer.encode_metadata_exchange`, `"vertebra-db/0.1"` user agent). Five of its eight dependencies (`base64`, `serde_json`, `sha2`, `tokio`, `url`) have zero `rg` hits inside the crate.
- Proposed change: remove the workspace member and the directory. If native acceleration is wanted, wire the functions into `vertebra-py` behind the Python API, with a benchmark that justifies it.
- Risk: low (no consumers). Builds get faster.
- Verify: `cd src/rs && cargo metadata --format-version 1 >/dev/null && make rs-test && uv run pytest src/py/tests/unit/db`.

**CORE-16 — `Opcode` lists "Bun Native" operations and silently maps unknown values to `AsgiHttp`.** `src/rs/vertebra-core/src/ipc.rs:178-222` · dead code / naming
- Evidence: the variants `HashPassword`, `VerifyPassword`, `GenerateRandom` and `CsrfSign` ("Bun Native") are leftovers from an earlier design. `rg Opcode::` matches only `ipc.rs` and the dead `protocol/state.rs`. The fallback `_ => Opcode::AsgiHttp` masks corruption.
- Proposed change: delete `Opcode` along with `protocol/state.rs`. This also drops the `serde` derive need from `ipc.rs`.
- Risk: low.
- Verify: `cargo build --workspace`.

#### Wire format and flags

**CORE-17 — Two independent flag bit spaces describe the same frame states.** `src/rs/vertebra-core/src/ipc.rs:3-8` (u16: STREAMING=1<<0 … TRAILERS=1<<5), `src/rs/vertebra-core/src/protocol/mod.rs:153-184` (u32: STREAMING=1<<6 … TRAILERS=1<<9, plus `response_flags`) · duplication / naming
- Evidence: `protocol::flags::{STREAMING, FINAL, HEADERS_SENT, TRAILERS}` have 0 uses; the `ipc::FLAG_*` set is the live one (http, py). `flags::{BATCH, OK, ERROR}` are used only in codec tests. `ipc::FLAG_COMPRESSED` has 0 uses.
- Proposed change: create one module, `protocol::flags`, with two typed sets. `RequestFlags(u32)` holds `PRIORITY`, `COMPRESSED` + codec bits and `FIRE_AND_FORGET`. `FrameFlags(u16)` holds `STREAMING`, `FINAL`, `ERROR`, `HEADERS_SENT` and `TRAILERS`. Delete `response_flags` and the unused constants.
- Risk: medium (cross-crate rename, no wire change if the `FrameFlags` bit values match today's ipc ones).
- Verify: `make rs-test && make py-test`.

**CORE-18 — `ResponseSlot::decode_from` allocates two `String`s per header plus the body `Vec` on the HTTP hot path.** `src/rs/vertebra-core/src/ipc.rs:126-170`; caller `vertebra-http/src/dispatcher/shm.rs:262`, `:303` · allocation/copy
- Evidence: `String::from_utf8_lossy(...).into_owned()` for key and value, then `src[offset..].to_vec()` for the body. The encoder takes `&[(String, String)]` (`ipc.rs:35`, `:54`), which forces Python→Rust header conversion into owned Strings.
- Proposed change: add a borrowed `ResponseSlotView<'a> { request_id, status, flags, headers: RawHeaders<'a>, body: &'a [u8] }`, with headers in the compact `iter_raw_headers` format already used for requests (`transport/mod.rs:896`). The http side converts straight into `HeaderMap`. Encoding takes `&[u8]` precompacted headers.
- Risk: medium.
- Verify: new criterion `response_slot_decode` in `benches/shm_transport.rs`; `cargo bench -p vertebra-core --bench shm_transport`.

**CORE-19 — `ResponseData::deserialize` copies headers and body out of the ring.** `src/rs/vertebra-core/src/transport/ring_pair.rs:64-69`, `:255-266` · allocation/copy
- Evidence: two `Bytes::copy_from_slice` calls per response, while the doc says "zero-copy slot views" (`:254`).
- Proposed change: superseded by CORE-03/CORE-18. Hand `SlotView` slices to the decoder and copy only the body once into the `Bytes` that hyper needs (or use `Bytes::from_owner` over a pooled buffer).
- Risk: medium.
- Verify: `cargo bench -p vertebra-core --bench shm_transport -- response_ring`.

**CORE-20 — `BatchSnapshot::deserialize_from` copies twice, allocates three times, and never validates node offsets.** `src/rs/vertebra-core/src/transport/mod.rs:786-819`, accessors `:676-724` · allocation/copy / correctness
- Evidence:
  - `vec![TransportNode::default(); node_count]` + memcpy, `data[..].to_vec()` for the slab, then `Arc::new`. The doc admits "future optimization".
  - `node_count * node_size` is unchecked.
  - Node `*_offset + *_len` are never checked against `slab_len`, so a malformed frame panics in `path_bytes`/`body` inside the Python worker.
  - `slab_bytes()` `expect`s decompression success (`:663`).
- Proposed change: validate all node ranges once in `deserialize_from` and return `DeserializeFailed`. Reuse the transport `BufferPool` pairs for SHM deserialization (`new_pooled`). Medium term, decode as `Arc<SlotLease>` borrowing the MPMC slot until the batch is dropped.
- Risk: low for validation; medium for pooling.
- Verify: `cargo test -p vertebra-core batch_snapshot_` + new `batch_snapshot_rejects_out_of_range_offsets`; `cargo bench -p vertebra-core --bench shm_transport -- batch_snapshot_serde`.

**CORE-21 — The batch wire header is serialized twice and the size constant is redefined.** `src/rs/vertebra-core/src/transport/mod.rs:741-779` (`BatchSnapshot::serialize_into`), `:841-890` (`ShmBatchWriter`), `:843` (`const BATCH_HEADER_SIZE: usize = 16;` shadows `:823`) · duplication
- Proposed change: add a private `BatchHeader { flags, node_count, slab_len }` with `write_to`/`read_from`, used by both serializers and the deserializer. Remove the shadowed const.
- Risk: low.
- Verify: `cargo test -p vertebra-core batch_snapshot_serialize`.

**CORE-22 — `BatchSnapshot` has four constructors where one would do.** `src/rs/vertebra-core/src/transport/mod.rs:577-631` · API surface bloat
- Evidence: `new`, `new_with_flags`, `new_pooled` and `new_pooled_with_flags` differ only in two fields.
- Proposed change: `BatchSnapshot::new(slab, nodes, flags, pool: Option<Arc<BatchBufferPool>>)` (crate-private) plus `deserialize_from`.
- Risk: low.
- Verify: `cargo build --workspace`.

**CORE-23 — The `RingPairError`→`TransportError` mapping is written out three times.** `src/rs/vertebra-core/src/transport/mod.rs:123-127`, `:145-149`, `:160-164` · duplication
- Proposed change: `impl From<RingPairError> for TransportError`, then `?` everywhere.
- Risk: low.
- Verify: `cargo test -p vertebra-core transport::`.

#### Layering and naming

**CORE-24 — Two different types are both named `BufferPool`.** `src/rs/vertebra-core/src/buffer.rs:90` (slot index pool) and `src/rs/vertebra-core/src/transport/mod.rs:466` (slab/node `Vec` pair recycler) · naming
- Evidence: `bridge.rs:16` imports the transport one, `channel.rs:11` imports the buffer one.
- Proposed change: rename the transport type to `BatchBufferPool` (it recycles `BatchSnapshot` buffers). Delete the buffer one (CORE-05/12).
- Risk: low.
- Verify: `cargo build --workspace`.

**CORE-25 — Four hard-coded response tiers are spread over three structs and env vars.** `src/rs/vertebra-core/src/tiered_ring.rs:14-91`, `src/rs/vertebra-core/src/transport/ring_pair.rs:103-293`, `:432-477`, `:534-637` · duplication / layering
- Evidence: `small`/`medium`/`large`/`xlarge` are written out field by field in `TieredRingConfig`, `TieredRingSet`, `select_ring`/`select_tier` (duplicate ladders `tiered_ring.rs:78-90` vs `ring_pair.rs:269-281`), `handles`, `reclaim`, `fd_set` (recomputes `RingLayout` 5 times), `RingPairFds` (8 fields) and `to_env_vars` (12 literal pairs). `TieredRingConfig::select_tier` is used only by a bench and tests. `RingPairFds::*_size` is emitted but `from_env` discards it (`_req_ring_size`, `:491`).
- Proposed change: `pub const RESPONSE_TIERS: usize = 4; struct TieredRingSet { rings: [Arc<SpscRing>; 4], tiers: [RingTier; 4] }`, with `select` doing a single `position`. `RingPairFds { request: RingFd, responses: [RingFd; 4], notify: NotifyFds }`, with env names generated from a `TIER_NAMES` table. Fold `tiered_ring.rs` into `ring_pair.rs`. Delete `shm_handle_from_ring` (`:297`), `request_ring_handle` (`:427`, ext=0) and the duplicate `RingPair::send_response_bytes` wrapper (`:527`).
- Risk: medium (the env protocol must stay identical; covered by `env_vars_protocol_complete`).
- Verify: `cargo test -p vertebra-core ring_pair` + `cargo test -p vertebra-http ring_factory`.

**CORE-26 — `try_recv_response` always scans small→xlarge, so large tiers starve under load.** `src/rs/vertebra-core/src/transport/ring_pair.rs:255-266` · hot-path performance
- Evidence: it returns the first hit. `drain_worker` (`vertebra-http/src/response_reader.rs:270`) loops until `None`, so a steady stream of small responses delays medium/large completions without bound. Each empty poll also costs 4 `slot_borrowed` CAS operations.
- Proposed change: round-robin the start index (`Cell<u8>` in the reader), or drain each tier up to N frames per pass.
- Risk: low.
- Verify: new `tiered_ring_set_fair_drain` test; `cargo bench -p vertebra-core --bench shm_transport -- response_ring`.

**CORE-27 — `BatchBridge` holds a std `Mutex` across a blocking crossbeam `send`, called from the async dispatcher.** `src/rs/vertebra-core/src/bridge.rs:143-149`, `:191-227`; caller `vertebra-http/src/dispatcher/bridge.rs:21-55` (from `dispatcher/mod.rs:157`) · lock/GIL contention
- Evidence: `batch_writer()` takes `write_lock`, and `seal()` → `self.batch_tx.send(snapshot)` blocks when the bounded channel (`channel_depth`, 4 in `main.rs:181`) is full. This runs on a Tokio worker thread with the lock held, stalling the runtime and every other dispatcher. `DataSlab::write` and `TransportArena::push` also use `fetch_add(AcqRel)` atomics even though `write_lock` already serializes writers (`transport/mod.rs:318-334`, `:401-413`).
- Proposed change: use `try_send` and surface `TransportError::RingFull` (back-pressure through the existing `BackpressureController`), or move sealing to a dedicated thread. Make `DataSlab`/`TransportArena` plain `&mut self` types owned by `BatchWriter` (the lock guard), so the atomics and `unsafe impl Sync` go away.
- Risk: medium.
- Verify: `cargo test -p vertebra-core bridge::` + `cargo bench -p vertebra-http --bench server_throughput -- bridge_dispatch`.

**CORE-28 — Redundant `BatchBridge` API wrappers.** `src/rs/vertebra-core/src/bridge.rs:94-107` (`init` vs `init_with_codec`), `:151-158` and `:188-193` (`push_request`/`seal_batch`, each taking the lock per call; production uses `batch_writer()`), `:245` (`let _ = slab_data;` cfg hack), `:267-273` (`submit_responses_async`, ext=0), `:283-289` (`try_recv_responses`, ext=0) · API surface bloat
- Proposed change: keep `init(BridgeConfig)`, `batch_writer()`, `poll_batch`, `submit_responses` and `recv_responses`. Gate `try_compress_slab` with a `#[cfg]` function pair instead of the `let _` hack. Tests move to `batch_writer()`.
- Risk: low.
- Verify: `cargo test -p vertebra-core bridge::`.

**CORE-29 — Per-header `Vec` allocations in `BridgeResponse`; a Tokio `Mutex` around a single-consumer receiver.** `src/rs/vertebra-core/src/bridge.rs:21-26`, `:76`, `:278-281` · allocation/copy / lock contention
- Evidence: `headers: Vec<(Vec<u8>, Vec<u8>)>` costs 2 allocations per header per response. `resp_rx: tokio::sync::Mutex<Receiver<..>>` takes a lock on every `recv_responses().await`, and there is exactly one reader (`vertebra-http/src/main.rs:87`).
- Proposed change: `headers_raw: Bytes` in the compact header format (as the SHM path already does). Hand the `Receiver` out once with `take_response_receiver()`, so no lock is needed.
- Risk: medium (touches `vertebra-py/src/api.rs` builders).
- Verify: `cargo bench -p vertebra-http --bench server_throughput -- bridge_dispatch`.

**CORE-30 — `ShmRegion` has three copies of the mmap/error-cleanup block plus unused constructors.** `src/rs/vertebra-core/src/shm.rs:278-324`, `:327-390`, `:393-418` (duplicate mmap paths); `:109-120` `from_fd` (ext=0); `:204` `size()` (duplicates `len()`, ext=0); `:147-158` (empty `if` with "Log but don't fail" comment); `:212` `from_raw` is `pub` for tests · duplication / dead code / stub
- Proposed change: `create_memfd_region` and `create_shm_open_region` produce an fd and then call `map_fd_region(fd, size)` (taking an optional unlink name). Delete `from_fd` and `size`. Delete the named API (CORE-14). Make `from_raw` `#[cfg(test)]` or `pub(crate)`.
- Risk: low.
- Verify: `cargo test -p vertebra-core shm`.

**CORE-31 — `Notifier` carries bench-only spin helpers, duplicate cfg imports, and redundant `unsafe impl`s.** `src/rs/vertebra-core/src/notify.rs:14-18` (identical `use` under both cfgs), `:274-290` `has_pending`, `:317-415` `spin_wait_or_park[_with]` (bench-only), `:423-426` `unsafe impl Send/Sync` (OwnedFd is already Send+Sync) · dead code / standards
- Proposed change: delete the spin helpers (benchmark against the ring-native `recv_hybrid` instead) and the `unsafe impl`s; use a single `use`. Move `has_pending` into `#[cfg(test)]`.
- Risk: low.
- Verify: `cargo test -p vertebra-core notify`.

**CORE-32 — The default macOS build parks with a 1 ms spin-sleep; ulock is opt-in.** `src/rs/vertebra-core/src/platform/macos.rs:13-16`, `:72-123`; `vertebra-core/Cargo.toml:31` · hot-path performance
- Evidence: without `macos-ulock`, `wait_on_address` spins 1024 iterations and then `nanosleep(1ms)`, so wake latency is at least 1 ms. CI already enables `macos-ulock` (`.github/workflows/ci.yml:113`), which means the shipped default is the path nobody tests.
- Proposed change: make ulock the macOS implementation. Keep spin-sleep only as the `ENOSYS` fallback via `sync::enable_fallback`. Remove the feature.
- Risk: medium (uses a private Apple API, which the code already depends on when the feature is enabled).
- Verify: `cargo test -p vertebra-core` on macOS CI; `cargo bench -p vertebra-core --bench spsc -- spsc_recv_hybrid_pure_wakeup`.

**CORE-33 — Root re-exports and `VERSION` are unused, and the transport module doc is broken.** `src/rs/vertebra-core/src/lib.rs:7`, `:27-30`; `src/rs/vertebra-core/src/transport/mod.rs:1` (`//! (arena+slab reset after snapshot)`); `transport/ring_pair.rs:1-8` (says "SPSC ring pair", but the request ring is MPMC) · dead code / docs drift
- Evidence: `rg "vertebra_core::(BatchBridge|RingPair|TieredRingSet|BatchSnapshot|TransportError|TransportNode|VERSION)"` returns 0 hits; every caller uses module paths.
- Proposed change: drop the re-exports and `VERSION`. Write a real module doc for `transport`. Fix the ring_pair doc.
- Risk: low.
- Verify: `cargo doc -p vertebra-core --no-deps`.

**CORE-34 — `telemetry::sanitize_sql_statement` corrupts UTF-8, rewrites `JOIN (` subqueries, and documents `VALUES` handling it does not have.** `src/rs/vertebra-core/src/telemetry.rs:100-133` · correctness / docs drift
- Evidence:
  - `out.push(bytes[i] as char)` maps each UTF-8 byte to a Latin-1 char, which mangles non-ASCII text.
  - `bytes[i..i + 3].eq_ignore_ascii_case(b"IN ")` also matches the tail of `JOIN (SELECT …)`, which becomes `JOIN (?)`.
  - The doc promises `VALUES (...)` sanitizing; there is no code for it.
  - The function has no caller (CORE-13).
- Proposed change: delete it with CORE-13. If it is resurrected, require a word boundary before `IN`, copy unmatched spans as `&str` slices, and add `VALUES`.
- Risk: low.
- Verify: n/a after deletion.

#### Dependencies and features

**CORE-35 — `vertebra-core` dependencies that exist only for dead code.** `src/rs/vertebra-core/Cargo.toml:15-36` · unused deps
- Evidence:
  - `jsonwebtoken` (guards only), `rmp-serde` (state.rs only), `rustix` (sync FileLock only).
  - `serde_json` (guards + state only).
  - `serde` (Serialize derives on dead `RouteId`, `Opcode` and state types).
  - `matchit` (only the dead `Router`; `normalize_typed_path` does not need it).
  - `tokio` with workspace `features = ["full"]`, while core uses only `tokio::sync::{mpsc, Mutex}` (`bridge.rs`).
  - Dev-dependency `tokio-test` has 0 uses (`rg tokio_test` → none).
- Proposed change: after CORE-11/13/16, remove `jsonwebtoken`, `rmp-serde`, `rustix`, `serde_json`, `serde` and `matchit`. Use `tokio = { workspace = true, default-features = false, features = ["sync"] }` (and `rt`/`macros` as dev-deps for `#[tokio::test]`). Remove `tokio-test`. Drop `jsonwebtoken`/`rmp-serde`/`rustix` from `[workspace.dependencies]` (`src/rs/Cargo.toml:30-39`), since no other crate uses them.
- Risk: low.
- Verify: `cd src/rs && cargo tree -p vertebra-core -e normal | wc -l` before and after; `make rs-test`.

**CORE-36 — `vertebra-storage` duplicates crypto helpers and runs its own Tokio runtime.** `src/rs/vertebra-storage/src/signing.rs:15-27`, `:416-429`; `src/rs/vertebra-storage/src/store.rs:21-40`; duplicates in `vertebra-http/src/profile.rs:16` (`constant_time_eq`) and `vertebra-http/src/tls.rs:242` (`sha256_hex`) · duplication
- Evidence: `constant_time_eq` and `sha256_hex` are each implemented twice across crates. `STORAGE_RUNTIME` is a second multi-thread runtime inside the same Python process as `vertebra-py`'s runtime (its own worker pool).
- Proposed change: use `subtle::ConstantTimeEq` (already transitive through rustls) or a tiny shared `vertebra-core::util::crypto` module. The runtime overlap is out of scope for core, but note it for the py audit: expose one shared runtime handle through `vertebra-py`, or size `STORAGE_RUNTIME` to 1–2 workers.
- Risk: low.
- Verify: `cargo test -p vertebra-storage`.

**CORE-37 — Storage signing pyfunctions default `timestamp=1700000000` (a fixed date in Nov 2023).** `src/rs/vertebra-storage/src/lib.rs:447`, `:479`, `:534`, `:570`, `:610` · correctness / stub
- Evidence: `#[pyo3(signature = (..., timestamp=1700000000))]` on HMAC, S3 V4, GCS V4, OCI Cavage and Azure SAS signing. A caller that omits `timestamp` gets URLs and headers that expired in 2023, and OCI/Azure will reject the `Date` header.
- Proposed change: `timestamp: Option<i64> = None` → `Utc::now().timestamp()`. Tests pass an explicit timestamp.
- Risk: low.
- Verify: `uv run pytest src/py/tests -k "storage and sign"`; `cargo test -p vertebra-storage`.

**CORE-38 — `NativeObjectStore` hand-writes every method as a sync + `_async` pair.** `src/rs/vertebra-storage/src/lib.rs:240-444` (about 14 pairs: `put`/`put_async`, `get`/`get_async`, `get_buffer`/`get_buffer_async`, `get_range`/`_async`, `head`, `exists`, `delete`, `copy`, `list`, `put_file`, …) · duplication
- Evidence: each pair repeats the argument conversion and `to_py_err` mapping; only `block_on_storage` vs `spawn_rust_task` differs.
- Proposed change: a `storage_method!` macro, or private `fn op_<name>(&self, ...) -> impl Future`, plus two one-line pymethods each. `extract_buffer_bytes` should take `Py<PyBytes>` zero-copy via `Bytes::from_owner` instead of `copy_from_slice` (`:43-45`).
- Risk: low.
- Verify: `uv run pytest src/py/tests -k storage`.

#### Tests and benchmarks

**CORE-39 — Duplicate and dead-code tests in core.** `src/rs/vertebra-core/src/channel.rs:1852`, `:1879`, `:1902`, `:2007` vs `tests/test_ring.rs:41`, `:72`, `:127` (`spsc_recv_hybrid_timeout` exists in both); `channel.rs:2205-2374` (7 `try_recv_batch` tests of a dead API); `channel.rs:1933` (`BufferChannel`); `sync.rs:612-738` (5 tests of dead locks); `logger.rs:214-245`, `guards.rs:87-169`, `protocol/state.rs:466-530`, `protocol/mod.rs:231-285` (tests of dead modules); `transport/mod.rs:1112-1145` (`method_name` string-table tests) · test weakness/duplication
- Evidence: `rg "fn spsc_recv_hybrid_timeout"` finds 2 definitions. `test_ring.rs::spsc_full_returns_error` repeats `channel.rs::spsc_full_empty_edges`.
- Proposed change: keep one canonical test home per type. Integration behavior goes in `tests/test_ring.rs` / `tests/test_mpmc.rs`; layout invariants stay inline. Delete tests with their dead code. Add the missing tests: CORE-01 wedge, CORE-02 panic/oversize, CORE-20 malformed offsets, an MPMC multi-producer stress test with sequence verification, and a CORE-26 fairness test.
- Risk: low.
- Verify: `cargo test -p vertebra-core -- --list | wc -l` before and after; `make rs-test`.

**CORE-40 — Benches measure dead code or duplicate each other, and there is no Makefile target for Rust benches.** `src/rs/vertebra-core/benches/router.rs`, `benches/guards.rs` (dead modules); `benches/mpmc.rs:4-15` vs `benches/ring_throughput.rs:78-95` (both single-thread MPMC); `benches/spin_wait.rs:17-66` (reimplements `recv_hybrid` over `Notifier`) vs `benches/spsc.rs:61-205`; `Makefile:233-272` (`bench*` targets run only `python -m tools.benchmarks`) · test weakness/duplication
- Evidence: `rg "cargo bench" Makefile .github` returns 0 hits. No bench covers MPMC contention, `BatchBridge` seal/poll (core), `ResponseSlot` encode/decode, or `TransportArena`/`DataSlab` push.
- Proposed change:
  - Delete `router.rs`, `guards.rs` and `mpmc.rs`. Merge `spin_wait.rs` into `spsc.rs`.
  - Add `benches/mpmc_contention.rs` (N producers × 1 consumer, batch-sized slots), `benches/bridge.rs` (push×64 + seal + poll), and a `response_slot` group in `shm_transport.rs`.
  - Add `make rs-bench` (`cd src/rs && cargo bench -p vertebra-core --features compression`).
- Risk: low.
- Verify: `make rs-bench`.

**CORE-41 — Unconditional SeqCst RMW on every send/recv (benchmark-gated candidate).** `src/rs/vertebra-core/src/channel.rs:297-303`, `:333-337`, `:454-460`, `:102-106` · hot-path performance
- Evidence: every successful SPSC operation does `wait_*_ptr().fetch_add(1, SeqCst)` (a locked RMW on a shared cache line) even when no waiter exists, followed by a SeqCst load of the waiter count. With a SeqCst head store plus a SeqCst waiter load, the futex word only needs bumping when `waiters > 0`: a waiter that is not observed is ordered after the head store and its re-check sees the new head. A past fix made the bump unconditional, so this has to be proven before it changes.
- Proposed change: after CORE-07 centralizes `publish_*`, try `head.store(SeqCst); if waiters.load(SeqCst) > 0 { word.fetch_add; wake }`. Accept the change only if a loom model and the stress tests pass and `spsc_ping_pong` improves.
- Risk: high (this is the documented lost-wakeup area). Keep it behind the benchmark.
- Verify: `cargo bench -p vertebra-core --bench spsc` + new `loom` test `spsc_no_lost_wakeup` (`RUSTFLAGS="--cfg loom" cargo test -p vertebra-core --test loom_spsc`).

**CORE-42 — `encode_headers_compact` truncates silently; `decode_headers_raw` is test-only.** `src/rs/vertebra-core/src/transport/mod.rs:931-955` · correctness / dead code
- Evidence: header names longer than 255 bytes and values longer than 65535 bytes are cut with `.min(...)` and no error. `decode_headers_raw` is used only by `bridge.rs:561` (a test).
- Proposed change: move the header codec to `transport/headers.rs` as `RawHeaders` (encode → `Result`, iterate → borrowed). Make `decode_headers_raw` `#[cfg(test)]`. Share it with `ResponseSlot` (CORE-18).
- Risk: low.
- Verify: `cargo test -p vertebra-core decode_`.

**CORE-43 — `BufferPool::release_multi` is a no-op wrapper and `BufferHandle` is an alias shim.** `src/rs/vertebra-core/src/buffer.rs:153-156`, `:201`; also `SLOT_SIZES` `:22` and `RegionState::{Uninitialized, Stale}` `:31-33` (ext=0) · dead code / standards
- Evidence: `release_multi(handles) { drop(handles) }`. `pub type BufferHandle = Buffer;` has 0 users and is an alias of the kind the repository rules forbid.
- Proposed change: delete them (with CORE-05).
- Risk: low.
- Verify: `cargo build --workspace`.

**CORE-44 — The workspace splits deps between `[workspace.dependencies]` and per-crate pins.** `src/rs/vertebra-storage/Cargo.toml:16-34`, `src/rs/vertebra-db/Cargo.toml:13-21` · standards
- Evidence: `base64`, `sha2` and `url` are pinned separately in storage and db, and `async-trait`, `futures`, `hex`, `hmac`, `http`, `reqwest`, `chrono` are only crate-local. `vertebra-storage` omits `repository.workspace = true`, which every other crate sets.
- Proposed change: after deleting `vertebra-db`, move the storage crypto/http deps into `[workspace.dependencies]` where `vertebra-http` already uses the same crates (`sha2`, `base64`, `reqwest`). Add `repository.workspace`.
- Risk: low.
- Verify: `cd src/rs && cargo metadata --format-version 1 > /dev/null && cargo tree -d | head`.

## HTTP ingress (prefix HTTP)

#### HTTP-01: `mcp_bridge.rs` is a stub that implies a working feature
- **Where:** `src/rs/vertebra-http/src/mcp_bridge.rs:36-78`. Category: stub/placeholder, duplication.
- **Evidence:** `McpStdioBridge::run_with_responder` is an `async fn` that only does blocking `std::io` (`BufReader::lines`, `writeln!`). The HTTP call is delegated to a caller-supplied closure, and `McpBridgeConfig.endpoint_url` (`:32`) is never read. The only caller is `tests/mcp_discovery_bridge_tests.rs:72`. The real bridge lives in Python: `src/py/vertebra/cli/mcp.py:47` `run_mcp_stdio_bridge`, and `:24` re-implements `extract_mcp_observability_headers`.
- **Change:** Delete `mcp_bridge.rs`, its `pub mod`, and its two tests. The `vtb mcp bridge` feature stays in Python. If native header extraction is wanted later, expose one function through `vertebra-py`.
- **Risk:** Low. There are no non-test callers.
- **Verify:** `make rs-test && uv run pytest src/py/tests/unit/cli/test_mcp_cli.py`

#### HTTP-02: 13 ingress modules are unreachable from the server
- **Where:** `src/rs/vertebra-http/src/response.rs:75-206` (`handle`). Category: layering/organization, stub/placeholder.
- **Evidence:** An `rg` for `crate::<mod>` shows no references from `server.rs`, `response.rs`, or `main.rs` to any of these: `access_log`, `assets`, `compression`, `cors`, `discovery`, `grpc`, `multipart`, `payload`, `profile`, `pubsub`, `sse`, `direct_worker`, `mcp_bridge`. `ws_mount` is only used for `is_websocket_upgrade` from `hmr.rs`. `vertebra-py` does not depend on `vertebra-http`, so none of this is reachable from Python either.
- **Change:** Add an `ingress::Pipeline` deep module, built once from `ServerConfig`. It owns ordered request interceptors (health, discovery, profile, static assets, CORS preflight, gRPC detection) and response filters (CORS headers, compression, Alt-Svc, access log). `handle` becomes `pipeline.intercept(&parts)` → route → dispatch → `pipeline.finish(&mut resp)`. Each config section gets env/JSON wiring in `config.rs`.
- **Risk:** Medium. This changes the hot path, so benchmark it.
- **Verify:** `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-http && cargo bench --manifest-path src/rs/Cargo.toml -p vertebra-http --bench server_throughput`

#### HTTP-03: Python evaluates CORS for a Rust offload spec that Rust never consumes
- **Where:** `src/rs/vertebra-http/src/cors.rs:5-108`. Category: duplication.
- **Evidence:** `src/py/vertebra/extensions/litestar/offload.py:49-58` builds `CorsOffloadSpec` (including `allow_origin_regex`) "for Rust vertebra-http". It then evaluates CORS in Python itself (`evaluate_cors_preflight` at `:133`). Rust's `CorsOffloadConfig` has no `allow_origin_regex` and no deserializer, and `rg offload src/rs/vertebra-http/src` finds only doc comments. `resolve_allow_origin` allocates a `String` per call (`:25,27`), and `join(", ")` runs on every preflight (`:63,71`).
- **Change:** Rename to `CorsPolicy`. Precompute the allow-methods/allow-headers/expose `HeaderValue`s at build time, add the origin regex, deserialize from the spec JSON passed via env, and apply it in `ingress::Pipeline`. The Python evaluator then stays only as a fallback when not running under `vertebra-server` (coordinate with the Python auditor).
- **Risk:** Medium (behavior parity with Litestar).
- **Verify:** `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-http cors && uv run pytest src/py/tests -k offload`

#### HTTP-04: `compression.rs` has two parallel APIs and a third negotiator elsewhere
- **Where:** `src/rs/vertebra-http/src/compression.rs:8-137` vs `:139-346`. Category: duplication.
- **Evidence:** API one is `ResponseCodec`, `CompressionConfig`, and `compress_response_body`, with proper q-value negotiation. API two is `CompressionBackend`, `CompressionOffloadConfig`, and `compress_if_eligible`. It repeats the encoder code, uses naive substring checks (`accept.contains("br")` at `:331`, which also matches `"br;q=0"`), and has different defaults (512 vs 500, `gzip_level` vs `gzip_compress_level`). `assets.rs:284` `accepts_encoding` is a third Accept-Encoding parser.
- **Change:** Use one `ResponseCompressor { codecs: SmallVec<ResponseCodec>, levels, min_size, excluded: PathMatcher }` with `negotiate(&HeaderMap) -> Option<ResponseCodec>` and `encode(codec, &[u8]) -> Bytes` (reusing output `Vec` capacity). `assets` calls `negotiate`.
- **Risk:** Low.
- **Verify:** `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-http compress`

#### HTTP-05: A new `HealthEndpoint` is built on every request, so lame-duck never works
- **Where:** `src/rs/vertebra-http/src/response.rs:92-97`. Category: hot-path performance, stub/placeholder.
- **Evidence:** `crate::health::HealthEndpoint::default().try_intercept(path)` runs on every request. `Default` calls `new("/healthz", "/readyz")`, which allocates two `String`s (`health.rs:41,50-51`), so every routed request pays two allocations. Because the instance is temporary, `set_state(Draining|ShuttingDown)` (`health.rs:63`) can never be observed. `/__vertebra/health` is also handled twice: `response.rs:30,87` (only when backpressure is set) and `health.rs:70`.
- **Change:** Store an `Arc<HealthEndpoint>` (paths as `&'static str` / `Box<str>`) in `ServerState`. Use one health responder that adds the backpressure snapshot when present. Flip it to `Draining` on SIGTERM before the accept loop stops.
- **Risk:** Low.
- **Verify:** `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-http health && cargo bench --manifest-path src/rs/Cargo.toml -p vertebra-http --bench server_throughput -- direct_dispatch`

#### HTTP-06: The SHM streaming response path is dead in production
- **Where:** `src/rs/vertebra-http/src/dispatcher/shm.rs:248-343`, `src/rs/vertebra-http/src/response_reader.rs:35-112`. Category: dead code, stub/placeholder.
- **Evidence:** `Dispatcher::handle_raw_response_slot` (`dispatcher/mod.rs:229`) has zero callers. `drain_worker` (`response_reader.rs:267-295`) only calls `response_rings.try_recv_response()`, which deserializes unary `ResponseData` (`vertebra-core/src/transport/ring_pair.rs:255-266`). `PendingRequests`/`PendingResponder` (including the never-used `register_unary`) are only used by `tests/streaming_body_tests.rs:111`. That means SHM workers cannot stream, despite PR #10's "multi-frame SHM streaming".
- **Change:** Make the reader peek frame flags and route streaming frames through a single correlation table. Extend `InFlightRequests` entries to `enum Slot { Unary(oneshot), Stream(mpsc::Sender<Frame>) }`, and delete the `PendingRequests` DashMap. Add a SHM streaming integration test.
- **Risk:** High (wire protocol between worker and server).
- **Verify:** `make rs-test && make py-test`

#### HTTP-07: `FLAG_*` constants are redefined twice
- **Where:** `src/rs/vertebra-http/src/response_reader.rs:29-33`, `src/rs/vertebra-http/src/grpc.rs:22`. Category: duplication.
- **Evidence:** The canonical definitions are in `vertebra-core/src/ipc.rs:3-8`. `shm.rs:252` already imports from `vertebra_core::ipc`, so one file uses core and another uses local copies.
- **Change:** Delete the local copies and `use vertebra_core::ipc::FLAG_*`. Point the tests at core.
- **Risk:** Low.
- **Verify:** `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-http`

#### HTTP-08: Five copies of the same mpsc-receiver body wrapper
- **Where:** `response.rs:33` `ReceiverFrameStream`, `collector.rs:20` `PendingResponseBody`, `sse.rs:28` `SseFrameReceiverStream`, `http3.rs:69` `H3ChannelBody`, `grpc.rs:319` `GrpcStreamBody` (all under `src/rs/vertebra-http/src/`). Category: duplication, API surface bloat.
- **Evidence:** Each is a struct around `mpsc::Receiver<Result<Frame<Bytes>, Infallible>>` that forwards `poll_recv`. Two of them are further wrapped in `StreamBody`.
- **Change:** Add one `body::FrameReceiver` that implements `http_body::Body` directly. Set `ResponseBody = Either<Full<Bytes>, FrameReceiver>`. Delete the other four plus `PendingResponseBody::into_inner`.
- **Risk:** Low.
- **Verify:** `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-http streaming`

#### HTTP-09: Request header encoder copies the core codec and can corrupt framing
- **Where:** `src/rs/vertebra-http/src/response.rs:255-271`, `:315-317`. Category: duplication, correctness.
- **Evidence:** `encode_headers_mp` duplicates `vertebra-core/src/transport/mod.rs:931` `encode_headers_compact` but drops the clamping. `n.len() as u8` and `v.len() as u16` silently wrap for names over 255 bytes or values over 64 KiB, which desyncs `iter_raw_headers` on the worker. `encode_response_headers` is a one-line pass-through wrapper.
- **Change:** Add `vertebra_core::transport::encode_headers_compact_into<'a, I: IntoIterator<Item = (&'a [u8], &'a [u8])>>(iter, &mut Vec<u8>)`. Use it for both `HeaderMap` and slot headers, and delete both http copies.
- **Risk:** Low.
- **Verify:** `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-core transport && cargo test --manifest-path src/rs/Cargo.toml -p vertebra-http`

#### HTTP-10: Response headers are parsed three times with several allocations each
- **Where:** `src/rs/vertebra-http/src/dispatcher/shm.rs:256-298`, `src/rs/vertebra-http/src/main.rs:93`, `src/rs/vertebra-http/src/response.rs:289-292`. Category: allocation/copy.
- **Evidence:** The slot is parsed three times (`peek_request_id`, `peek_flags`, `decode_from`). Headers go from `Vec<(String, String)>` to `Vec<(Vec<u8>, Vec<u8>)>` (`:270-274`) to compact `Bytes`, and then `build_response` re-parses them through `Response::builder().header()` one header at a time. `raw_slot.to_vec()` copies whole frames (`:291,342`), and the header-conversion block appears twice (`:270-275`, `:293-298`).
- **Change:** Add a `ResponseSlotView<'a>` that borrows header and body bytes. Build the `HeaderMap` once with `HeaderMap::with_capacity` and `HeaderName::from_bytes` into `PendingResponse { headers: HeaderMap }`, then `build_response` does `*resp.headers_mut() = headers`.
- **Risk:** Medium.
- **Verify:** `make rs-test` plus a new bench `response_decode` (see Hot paths)

#### HTTP-11: Global `Mutex` and unbounded `VecDeque` on every request completion
- **Where:** `src/rs/vertebra-http/src/backpressure.rs:397-439`. Category: lock/GIL contention.
- **Evidence:** `record_success` and `record_failure` take `self.circuit.lock()` and `samples.push_back((now, bool))` on every response (called from `response.rs:197-203`). With the default 10 s window (`:70`), 100k rps holds about 1M samples, and `prune` walks the deque under the lock. Every tokio worker serializes on this lock.
- **Change:** Use a lock-free bucketed window: `[AtomicU64; 2 * BUCKETS]` per-second success/failure plus an epoch, with phase changes via CAS on the `AtomicU8` state. Take the mutex only on transitions.
- **Risk:** Medium (circuit semantics).
- **Verify:** `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-http backpressure` plus a new bench `backpressure/record_success_contended`

#### HTTP-12: Duplicated circuit transition and magic state numbers
- **Where:** `src/rs/vertebra-http/src/backpressure.rs:289-305` vs `:307-321`. Category: duplication.
- **Evidence:** The Open→HalfOpen block is copied verbatim. The `AtomicU8` uses literal 0/1/2 (`:222,290,302,308,409,425,436`) that shadow the `CircuitState` enum. `CircuitPhase` is `pub` but internal.
- **Change:** Use `#[repr(u8)] CircuitState`, a single `fn advance_phase(&self, inner: &mut CircuitBreakerInner)`, and make `CircuitPhase` private.
- **Risk:** Low.
- **Verify:** `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-http backpressure`

#### HTTP-13: `RejectionReason::status` is unused while `response.rs` re-maps statuses
- **Where:** `src/rs/vertebra-http/src/backpressure.rs:89-97`, `src/rs/vertebra-http/src/response.rs:208-223`. Category: duplication, dead code.
- **Evidence:** `rejection_response` hardcodes the same status mapping in a second `match`.
- **Change:** Add `RejectionReason::{status(), reason_phrase()}` as `const fn` returning `StatusCode`/`&'static str`, and use them in `rejection_response`.
- **Risk:** Low.
- **Verify:** `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-http test_server`

#### HTTP-14: The stream watermark API is never used in production
- **Where:** `src/rs/vertebra-http/src/backpressure.rs:181-276`. Category: dead code, stub/placeholder.
- **Evidence:** `acquire_stream`, `on_stream_chunk_enqueued/dequeued`, `wait_until_resumed`, and `StreamGuard` are only called from `tests/backpressure_streams_tests.rs:30-31`. No SSE, WS, or streaming body uses them.
- **Change:** Wire them into `FrameReceiver`-producing paths (SHM stream task, `SseHub::subscribe_response`, WS mount) via a `StreamLease` held by the body, or drop the API.
- **Risk:** Medium.
- **Verify:** `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-http backpressure_streams`

#### HTTP-15: The adaptive batch policy is a placeholder; the batcher never lingers
- **Where:** `src/rs/vertebra-http/src/policy.rs:16-105`, `src/rs/vertebra-http/src/collector.rs:255-277`. Category: stub/placeholder, docs drift.
- **Evidence:** `ema_arrival_rate` is written (`:98`) and never read. `timeout()`, `matches_preferred()`, and `Fixed.timeout` have no callers in `src`. `record_batch` is a pass-through to `adapt`. `RequestBatcher::run` drains right after `try_recv` exhausts, yet the module docs (`collector.rs:1-5`, `policy.rs:1-6`) promise timeout and preferred-size drains. `main.rs:304` picks `default_adaptive()`, which behaves exactly like Fixed.
- **Change:** Collapse to `BatchLimit { max: usize }` with honest docs ("opportunistic drain"). If lingering is wanted, implement it explicitly with `tokio::time::sleep_until` bounded by `max_queue_delay`.
- **Risk:** Low.
- **Verify:** `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-http collector && cargo bench --manifest-path src/rs/Cargo.toml -p vertebra-http --bench server_throughput`

#### HTTP-16: Weak tests
- **Where:** `src/rs/vertebra-http/tests/test_collector.rs:99-125`, `src/rs/vertebra-http/src/route_table.rs:70-85`, `src/rs/vertebra-http/src/policy.rs:118-173`. Category: test weakness/duplication.
- **Evidence:** `test_drain_by_timeout` passes because drain is immediate; it never exercises a timeout. `handler_id_reference_values` is labeled "cross-language verification" but only `eprintln!`s values and asserts `!= 0`. The policy tests check fields nothing reads.
- **Change:** Assert exact FNV-1a values shared with Python `_make_handler_id` (`src/py/vertebra/framework/_app.py:80`) in both test suites. Delete or rename the timeout test to `drains_without_linger`. Drop the EMA tests along with HTTP-15.
- **Risk:** Low.
- **Verify:** `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-http handler_id && uv run pytest src/py/tests -k handler_id`

#### HTTP-17: `direct_worker.rs` is production-dead and `DirectRequest` copies `PendingRequest`
- **Where:** `src/rs/vertebra-http/src/direct_worker.rs:1-215`, `src/rs/vertebra-http/src/dispatcher/types.rs:13-31`, `src/rs/vertebra-http/src/dispatcher/direct.rs:401-435`. Category: dead code, duplication, allocation/copy.
- **Evidence:** `DirectWorkerPool`, `run_adaptive_worker*`, and `AdaptiveWorkerConfig` are only referenced by the module's own tests. `Dispatcher::new_direct` is only used by the bench and `dispatcher/tests.rs`, and no `Topology` selects direct mode. `DirectRequest` repeats every `PendingRequest` field, and `dispatch_direct` rebuilds every request into a new `Vec` per batch.
- **Change:** Make the direct channel carry `Batch` unchanged; consumers check `flags & FIRE_AND_FORGET`. Delete `DirectRequest`/`DirectBatch`. Move `direct_worker.rs` into `benches/support/` (or delete it).
- **Risk:** Low.
- **Verify:** `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-http dispatcher && cargo bench --manifest-path src/rs/Cargo.toml -p vertebra-http --bench server_throughput -- direct_dispatch`

#### HTTP-18: Dispatcher is an enum flag plus Option fields
- **Where:** `src/rs/vertebra-http/src/dispatcher/mod.rs:47-129`, `:170-226`. Category: layering/organization.
- **Evidence:** `Inner { pending: DashMap, next_id, direct_tx: Option, shm_dispatcher: Option, mode }`. The DashMap is allocated even in SHM and direct modes. There is a double `Arc` (`Arc<Dispatcher { inner: Arc<Inner> }>`, with `main.rs:255`). `complete` and `complete_many` repeat the shard math and the `error!("... not configured")` arms.
- **Change:** Use `enum DispatchBackend { Direct(mpsc::Sender<Batch>), Bridge(BridgeCorrelation), Shm(Arc<ShmDispatcher>) }` with `Dispatcher { backend, backpressure }`. Implement `dispatch` and `complete_many` on each backend.
- **Risk:** Low.
- **Verify:** `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-http dispatcher`

#### HTTP-19: Correlation-ID packing is repeated six times and weakens generation protection
- **Where:** `src/rs/vertebra-http/src/dispatcher/shm.rs:156-176,276-277,333-334,386-387`, `src/rs/vertebra-http/src/dispatcher/mod.rs:174-175,203-204`. Category: duplication, correctness.
- **Evidence:** The slab returns `(gen << 32) | idx` (`correlation.rs:64`), and the dispatcher packs it as `(internal_id << 8) | shard`. That shifts out the top 8 bits of the generation, so stale-ID detection wraps after 2^24 slot reuses instead of 2^32.
- **Change:** Add a `CorrelationId(u64)` newtype in `correlation.rs` with explicit `{shard: u8, slot: u24/u32, gen: u24}` packing and `encode`/`decode`. Use it everywhere.
- **Risk:** Medium (wire value, opaque to Python).
- **Verify:** `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-http correlation`

#### HTTP-20: Shard mutex is locked once per item on both dispatch and completion
- **Where:** `src/rs/vertebra-http/src/dispatcher/shm.rs:157`, `src/rs/vertebra-http/src/dispatcher/mod.rs:205`, `src/rs/vertebra-http/src/dispatcher/shm.rs:388`. Category: lock/GIL contention.
- **Evidence:** `dispatch_shm` locks a `std::sync::Mutex<InFlightRequests>` per request. `complete_many` locks per response, so reader tasks and batchers contend on 8 mutexes.
- **Change:** Group by shard and lock once per shard per drain/batch. Alternatively, give each worker its own slab owned by its reader, with ID allocation done in the batcher under one lock per batch.
- **Risk:** Medium.
- **Verify:** new bench `shm_dispatch/roundtrip_1000` (see Hot paths); `make rs-test`

#### HTTP-21: `dispatch_shm` allocates a `HashMap` per batch
- **Where:** `src/rs/vertebra-http/src/dispatcher/shm.rs:86,179-181,350`. Category: allocation/copy.
- **Evidence:** It allocates `HashMap<(usize, u32), ShmDispatchBatch>` per batch, fresh `Vec`s per worker batch, and `components.iter().map(...).collect()` per flush.
- **Change:** Add a reusable `ShmBatchScratch` owned by each `RequestBatcher` shard: a `Vec<ShmDispatchBatch>` indexed by worker with a linear flags scan, cleared rather than reallocated.
- **Risk:** Low.
- **Verify:** `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-http test_spsc_violation`

#### HTTP-22: Background tasks leak and shutdown can hang
- **Where:** `src/rs/vertebra-http/src/dispatcher/shm.rs:58-68`, `src/rs/vertebra-http/src/main.rs:261`, `src/rs/vertebra-http/src/response_reader.rs:204-230`. Category: layering/organization (lifecycle bug).
- **Evidence:**
  - `ShmDispatcher::new` spawns an infinite sweeper that holds an `Arc<Self>`, so the dispatcher is never dropped and needs a runtime just to construct.
  - `sweep_expired` returns a `Vec` that nobody reads (`correlation.rs:95-125`).
  - The `CancellationToken` in `main.rs` is never cancelled.
  - The AsyncFd loop checks `is_cancelled()` only before `readable().await`, with no `select!`, so an idle reader never exits.
- **Change:** Add `ShmDispatcher::spawn_sweeper(self: &Arc<Self>, shutdown: CancellationToken)` that holds a `Weak` reference. `select!` on `shutdown.cancelled()` in both reader loops. Cancel the token after `run_with_collector` returns, and return a count instead of a `Vec` from the sweep.
- **Risk:** Medium.
- **Verify:** `make rs-test`; manual: `VERTEBRA_WORKERS=2 vertebra-server` + Ctrl-C exits within 5 s

#### HTTP-23: Worker crash recovery is written but not wired
- **Where:** `src/rs/vertebra-http/src/correlation.rs:131-157`, `src/rs/vertebra-http/src/ring_factory.rs:180-330`. Category: dead code.
- **Evidence:** `sweep_for_worker`, `WorkerLifecycle` (`check_alive`/`record_crash`/`respawn_delay`/`circuit_breaker_tripped`), `cleanup_stale_shm`, `install_panic_cleanup_hook`, and `RingFactoryError` have no callers outside tests. A crashed worker's requests wait 30 s for a 504. Naming also collides: `ring_factory::WorkerState` vs `dispatcher::types::WorkerState`.
- **Change:** Add a `workers::Supervisor` task in `main.rs` that polls `WorkerLifecycle`, calls `sweep_for_worker` (502) on death, and respawns with backoff. Rename one `WorkerState` (e.g. `WorkerHealth`).
- **Risk:** Medium.
- **Verify:** `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-http ring_factory`

#### HTTP-24: Most `ServerConfig` fields are never read
- **Where:** `src/rs/vertebra-http/src/config.rs:15-53`, `:167`. Category: dead code, API surface bloat.
- **Evidence:** `worker_threads`, `batch_size`, `batch_timeout`, `keepalive_timeout`, `max_connections`, `thread_count`, `h2_keep_alive_interval_ms`, `enable_h2c_upgrade`, `trusted_proxies`, and `workers.app_path` have zero reads outside `config.rs`. mTLS is always `MtlsConfig::default()` from env. `main.rs:148,196` re-read `VERTEBRA_WORKERS`/`VERTEBRA_APP` instead of using `config`.
- **Change:** Delete the unread fields, or wire them (keepalive → `http1().keep_alive`/`header_read_timeout`; `max_connections` → accept semaphore; h2 keepalive → `http2().keep_alive_interval`; mTLS env vars). `main.rs` reads only `ServerConfig`.
- **Risk:** Low.
- **Verify:** `make lint && cargo test --manifest-path src/rs/Cargo.toml -p vertebra-http test_server`

#### HTTP-25: A per-connection `peek` result is discarded and the h2c upgrade helpers are dead
- **Where:** `src/rs/vertebra-http/src/server.rs:148`, `src/rs/vertebra-http/src/h2c.rs:64-223`. Category: hot-path performance, dead code.
- **Evidence:** `let _ = detect_cleartext_protocol(&stream).await?` costs one `peek` syscall per connection and waits for the first bytes. hyper-util `AutoBuilder` already detects the h2 preface. `parse_h2c_upgrade_headers`, `build_h2c_switching_protocols_response`, `decode_base64url` (hand-rolled), and `PrependStream` are tests-only.
- **Change:** Drop the peek. Either implement `Upgrade: h2c` (hyper upgrade + `PrependStream`), gated by `enable_h2c_upgrade`, or delete the helpers.
- **Risk:** Low.
- **Verify:** `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-http h2c`

#### HTTP-26: Plain and TLS serve paths are duplicated and inconsistent
- **Where:** `src/rs/vertebra-http/src/server.rs:141-197`. Category: duplication.
- **Evidence:** `AutoBuilder::new` runs per connection (`:151,184`). The TLS path skips all h2 window/stream settings that the plain path applies (`:153-158`). The `service_fn` closures are duplicated. `let _ = &tls_acceptor` is a lint workaround (`:100`).
- **Change:** Build one `AutoBuilder` (it is `Clone`) in `ServerState`, with one generic `serve_connection<I: Read + Write>(io, state)`. Enable TLS with `cfg` on an `Option<TlsAcceptor>` type alias.
- **Risk:** Low.
- **Verify:** `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-http --features tls test_tls`

#### HTTP-27: Docs disagree with the code
- **Where:** `src/rs/vertebra-http/src/server.rs:4-5`; `.agents/bundles/knowledge/architecture/http/batching-dispatch-and-backpressure.md:4,35,41`; `.agents/bundles/knowledge/architecture/http/ingress-and-routing.md:37`; `.agents/bundles/knowledge/domains/http/server-and-dispatch.md:45`. Category: docs drift.
- **Evidence:**
  - The server doc says "without TLS, only HTTP/1.1", but AutoBuilder serves h2c prior knowledge.
  - The knowledge base claims "256-shard PendingRequests (65,536 slots) in `correlation.rs`". Reality is 8 shards (`shm.rs:42`) of `InFlightRequests`, and `PendingRequests` is the dead DashMap in `response_reader.rs`.
  - "Graceful Drain ... `drain_timeout` 30s / `ShutdownHandle`" does not exist (`rg ShutdownHandle` returns 0 hits).
- **Change:** Rewrite these chapters after the refactor, in present tense.
- **Risk:** Low.
- **Verify:** `rg -n "ShutdownHandle|256-shard" .agents/bundles` returns 0 hits

#### HTTP-28: Alt-Svc advertises an HTTP/3 listener that is never started
- **Where:** `src/rs/vertebra-http/src/response.rs:187-193`, `src/rs/vertebra-http/src/http3.rs:54-66,125`, `:249`. Category: stub/placeholder, allocation/copy.
- **Evidence:** `serve_http3_endpoint` is never called from `server.rs` or `main.rs`, yet `handle` injects Alt-Svc whenever `config.http3.enabled`. Each response formats a `String` and re-validates a `HeaderValue`. The h3 request body is copied twice (`copy_to_bytes` then `extend_from_slice`).
- **Change:** Precompute `alt_svc: Option<HeaderValue>` in `ServerState`. Start the QUIC endpoint from `run_with_listener` when enabled, reusing `TlsAcceptor::rustls_config`. Use `body_buf.put(chunk)`.
- **Risk:** Medium.
- **Verify:** `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-http --features http3 http3`

#### HTTP-29: Router hashes the method string on every request; core has a parallel `Router`
- **Where:** `src/rs/vertebra-http/src/router.rs:35-37,72-106`; `src/rs/vertebra-core/src/router.rs:41-70`. Category: hot-path performance, duplication.
- **Evidence:** `HashMap<String, matchit::Router>` with SipHash plus a `Cow` uppercase scan runs per request. The 405 path allocates `Vec<String>`. `vertebra-core::router::Router` (`RouteId`) is a second router used only by `vertebra-core/benches/router.rs`.
- **Change:** Index `[Option<matchit::Router<HandlerId>>; 9]` by the existing `collector::method_tag`, with a small `Vec` for extension methods. Make it the single `vertebra_core::router::Router` and delete the other.
- **Risk:** Low.
- **Verify:** `cargo bench --manifest-path src/rs/Cargo.toml -p vertebra-http --bench server_throughput -- router && cargo test --manifest-path src/rs/Cargo.toml -p vertebra-http test_router`

#### HTTP-30: Per-request path and query `String` copies
- **Where:** `src/rs/vertebra-http/src/response.rs:163-164`, `src/rs/vertebra-http/src/collector.rs:142-144`. Category: allocation/copy.
- **Evidence:** `path.to_owned()` and `query.map(str::to_owned)` run on every request, even though `Uri` is already `Bytes`-backed. SHM then converts them to `Bytes::from(String)` (`shm.rs:199-201`).
- **Change:** Set `PendingRequest.path_and_query: http::uri::PathAndQuery` (cheap clone, no copy) and expose `path()`/`query()` slices.
- **Risk:** Low.
- **Verify:** `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-http`

#### HTTP-31: Round-robin shard counter is a global atomic hit per request
- **Where:** `src/rs/vertebra-http/src/server.rs:41-47`. Category: lock/GIL contention.
- **Evidence:** `next_shard.fetch_add(1)` on one cache line from every tokio worker for every request.
- **Change:** Assign the collector shard once per connection at accept time and capture the `Sender` clone in the `service_fn` closure.
- **Risk:** Low.
- **Verify:** `cargo bench --manifest-path src/rs/Cargo.toml -p vertebra-http --bench server_throughput`

#### HTTP-32: Hand-rolled SHA-256 and a byte-scanning DER "parser"
- **Where:** `src/rs/vertebra-http/src/tls.rs:191-312`. Category: duplication, correctness.
- **Evidence:** A 70-line SHA-256 that copies the whole certificate (`data.to_vec()`). CN/SAN extraction searches for raw OID bytes and supports only short-form lengths under 128, so it misses long SANs and can match OIDs inside other fields. `ring` is already in the tree via `rustls` `features=["ring"]`. `peer_identity` (`:398`) and `normalize_trusted_proxy_headers` (`:125`) are never called from `serve_tls`.
- **Change:** Move identity code to `tls/identity.rs` behind the `tls` feature. Use `ring::digest::digest(&SHA256, der)` (direct optional dep) and a proper TLV walker (or `x509-parser`). Wire the identity into a request extension in `serve_tls`.
- **Risk:** Medium.
- **Verify:** `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-http --features tls mtls`

#### HTTP-33: The profiling endpoint returns placeholder data
- **Where:** `src/rs/vertebra-http/src/profile.rs:76-99`, `:16-25`, `:51-53`. Category: stub/placeholder, duplication.
- **Evidence:** `/debug/pprof/profile` returns `{"profile":"cpu","pid":..,"threads":..}` with no profile. `ProfileEndpoint::is_authorized` is a pass-through wrapper. `constant_time_eq` is duplicated in `src/rs/vertebra-storage/src/signing.rs:420`.
- **Change:** Implement it with `pprof` behind a `profiling` feature (CPU) and mimalloc stats (heap). Move `constant_time_eq` to `vertebra-core::guards`.
- **Risk:** Medium.
- **Verify:** `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-http profile`

#### HTTP-34: Access logger buffers without bound and emits invalid JSON
- **Where:** `src/rs/vertebra-http/src/access_log.rs:47-161`. Category: allocation/copy, lock/GIL contention, correctness.
- **Evidence:** It holds `Mutex<Vec<String>>` with no consumer. It calls `to_string()` per numeric token (`:135-139`). The JSON format uses Rust `{:?}` (`:115`), which escapes control and non-ASCII characters as `\u{..}`, and that is not valid JSON. `format_utc_clf_timestamp` (`:166`) is not used by any format.
- **Change:** Format into a reusable buffer using `itoa`-style `write!` and hand off to the PR #33 `AsyncLogCore` sink in `vertebra-core`. Use a proper JSON string escaper; add a `%t` token using the timestamp formatter.
- **Risk:** Medium.
- **Verify:** `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-http access_log`

#### HTTP-35: Pub/sub and SSE fan out the same event three ways
- **Where:** `src/rs/vertebra-http/src/pubsub.rs:100-168`, `:347-409`; `src/rs/vertebra-http/src/sse.rs:259-325`. Category: duplication, allocation/copy, naming.
- **Evidence:** Each publish:
  - copies `topic.to_owned()` and the idempotency key;
  - clones the envelope twice (history plus broadcast);
  - sends on three channels (raw, envelope, pattern scan);
  - converts with `from_utf8_lossy` into a `String` for an `SseEvent`;
  - re-encodes into a second replay ring in `SseHub`.

  `ShmBroadcastRing` is a heap `Vec<Mutex<Vec<u8>>>`, not shared memory, and `read_since` clones every frame.
- **Change:** One `realtime::TopicHub` (topic → `{seq, replay: VecDeque<(u64, Bytes)>, tx: broadcast<Bytes>}`) used by both SSE and pub/sub, with pre-encoded SSE frames stored once. Rename to `BroadcastRing` or back it with `vertebra-core` SHM rings.
- **Risk:** Medium.
- **Verify:** `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-http pubsub sse`

#### HTTP-36: gRPC spawns a task to emit frames it already has
- **Where:** `src/rs/vertebra-http/src/grpc.rs:337-371`. Category: allocation/copy.
- **Evidence:** It spawns a task and an mpsc channel to stream a fully materialized `Vec<GrpcFrame>`.
- **Change:** Build the body from an iterator (`StreamBody::new(stream::iter(frames.map(..).chain(trailers)))`) or encode into one `Bytes` plus trailers. Wire `is_grpc_request` into `ingress::Pipeline`.
- **Risk:** Low.
- **Verify:** `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-http grpc`

#### HTTP-37: `main.rs` duplicates app import logic and ships a demo route table
- **Where:** `src/rs/vertebra-http/src/main.rs:43-51` vs `:122-130`, `:118-120`, `:198-205`, `:211-217`, `:74`, `:181`. Category: duplication.
- **Evidence:**
  - The raw-callable wrapping is copied twice.
  - `splitn(2, ':')` parsing is copied twice; the worker copy indexes `parts[1]` without validation.
  - The no-Python build hardcodes `/hello` and `/echo`.
  - Mount prefixes skip HEAD and OPTIONS.
  - `BatchBridge::init` runs even in SHM mode.
- **Change:** Add an `AppSpec { module, attr }` parsed once in `ServerConfig`, plus `fn load_app(py, &AppSpec) -> Bound<PyAny>` used by both route building and worker start. Init the bridge only for the embedded topology, and register all methods for mounts.
- **Risk:** Low.
- **Verify:** `make py-test` (CLI run tests) + `cargo build --manifest-path src/rs/Cargo.toml -p vertebra-http --no-default-features`

#### HTTP-38: Unused and oversized Cargo dependencies and features
- **Where:** `src/rs/vertebra-http/Cargo.toml`. Category: dead code.
- **Evidence:**
  - `tokio-test` dev-dep has 0 uses.
  - `serde_json` and `rustls-pemfile` are listed again in dev-deps although they are normal or optional deps.
  - `hyper`/`hyper-util` use `features=["full"]`, which pulls in client and other unused features.
  - `brotli`/`flate2`/`zstd` are unconditional, while the `compression` feature only forwards to `vertebra-core`.
  - `tracing-subscriber` is a lib dependency used only by `main.rs`.
- **Change:** Drop `tokio-test`; pick explicit hyper features (`server`, `http1`, `http2`); gate codecs behind `compression`; move `tracing-subscriber` under the `python`/`server` feature.
- **Risk:** Low.
- **Verify:** `make lint-all && cargo build --manifest-path src/rs/Cargo.toml -p vertebra-http --no-default-features`

#### HTTP-39: Module names hide what modules contain; dead response collector
- **Where:** `src/rs/vertebra-http/src/collector.rs:19-186`, `src/rs/vertebra-http/src/response_reader.rs`. Category: naming, dead code.
- **Evidence:** `collector` holds the request batcher plus response-side types. `ResponseCollector` (`:45-128`) is only used by `tests/streaming_body_tests.rs:36`. `response_reader` is SHM-specific but sits at the crate root.
- **Change:** Rename to `batcher.rs` (`RequestBatcher`, `PendingRequest`, `Batch`). Move `PendingResponse` to `body.rs`, delete `ResponseCollector`, and move the reader to `dispatcher/shm_reader.rs`.
- **Risk:** Low.
- **Verify:** `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-http`

#### HTTP-40: `response.rs` mixes four concerns
- **Where:** `src/rs/vertebra-http/src/response.rs:1-317`. Category: layering/organization.
- **Evidence:** It contains the ingress `handle`, body type aliases and constructors, the header codec (`:255-317`), hand-written health JSON (`:225-253`), and a version tag (`:274`) that lives apart from `method_tag` (`collector.rs:194`).
- **Change:** Split into `ingress/mod.rs` (`handle`), `body.rs` (`ResponseBody`, `FrameReceiver`, `build_response`), and `health.rs` (health JSON). Put `method_tag`/`version_tag` on a `RequestTags` impl next to `PendingRequest`.
- **Risk:** Low.
- **Verify:** `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-http`

#### HTTP-41: Benchmark gaps and duplicate bench bodies
- **Where:** `src/rs/vertebra-http/benches/server_throughput.rs:166-455`, `Makefile:234-264`. Category: test weakness/duplication.
- **Evidence:** `bench_bridge_dispatch_adaptive` repeats `bench_bridge_dispatch` except for the policy, and it depends on the first bench's responder thread already running (`:341-343`). `make bench` runs the Python `tools.benchmarks`, and no make target runs these Criterion benches. There are no benches for SHM dispatch, the header codec, backpressure, compression, or SSE fan-out.
- **Change:** Add a `fn bridge_server(policy) -> SocketAddr` helper, a `make rs-bench` target, and the benches listed in Hot paths.
- **Risk:** Low.
- **Verify:** `cargo bench --manifest-path src/rs/Cargo.toml -p vertebra-http --bench server_throughput`

#### HTTP-42: Discovery cache uses a `DashMap` for three fixed paths
- **Where:** `src/rs/vertebra-http/src/discovery.rs:28-124`. Category: over-abstraction, allocation/copy.
- **Evidence:** `DashMap<String, CachedDiscoveryDoc>` holds only three fixed documents. Each `serve` rebuilds a `HeaderMap` with four inserts. `etag: String` duplicates `etag_header`. The MCP protocol version `"2026-07-28"` is hardcoded (`:84`).
- **Change:** Use `struct WellKnownDocs { agent_card, mcp, oauth: ArcSwapOption<CachedDoc> }` (or `RwLock`, written once) with prebuilt headers. Pass the protocol version from config.
- **Risk:** Low.
- **Verify:** `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-http well_known`

#### HTTP-43: Static files are read fully into memory and stat'ed per request
- **Where:** `src/rs/vertebra-http/src/assets.rs:244`, `:312-331`, `:153`. Category: hot-path performance, allocation/copy.
- **Evidence:** `tokio::fs::read` loads the whole file for every non-range response. Up to three `is_file()` blocking stats run per request for precompressed sidecars, plus a metadata call. There is no in-memory cache for fingerprinted assets.
- **Change:** Build a startup index from the manifest/static dir of `(path → {len, mtime, sidecars})`. Stream large files with `tokio_util::io::ReaderStream` into `FrameReceiver`, and cache small fingerprinted files as `Bytes`.
- **Risk:** Medium.
- **Verify:** `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-http assets`

#### HTTP-44: SSE wrapper functions and per-event allocations
- **Where:** `src/rs/vertebra-http/src/sse.rs:146-148`, `:223-242`, `:162-193`. Category: API surface bloat, allocation/copy.
- **Evidence:** `encode_sse_event` is a pass-through to `SseEvent::encode`. `push` and `push_encoded` repeat the eviction logic. `split_whatwg_lines` returns a `Vec<&str>` per encode, and `sanitize_header_field` allocates even on the clean path.
- **Change:** Remove the wrapper, keep one `push_encoded`, use an iterator for line splitting, and use `Cow<str>` for sanitize.
- **Risk:** Low.
- **Verify:** `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-http sse`

#### HTTP-45: HMR re-implements the WebSocket handshake
- **Where:** `src/rs/vertebra-http/src/hmr.rs:153-184`; `src/rs/vertebra-http/src/ws_mount.rs:225-290`. Category: duplication.
- **Evidence:** HMR builds its own 101 response with `derive_accept_key`, while `ws_mount::build_handshake_response` already exists. `WsConnectionRegistry` is not wired.
- **Change:** HMR calls `ws_mount::build_handshake_response(key, false, Accept { .. })` and drives the socket with `run_ws_connection`, registering in `WsConnectionRegistry` so shutdown `close_all` covers it.
- **Risk:** Low.
- **Verify:** `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-http hmr ws_mount`


## PyO3 bridge, server, supervisor, telemetry (prefix PYB)

| ID | file:line | Category | Evidence | Proposed change | Risk | Verify |
|---|---|---|---|---|---|---|
| PYB-01 | `src/rs/vertebra-py/src/rsgi_protocol.rs:156-221`, `src/rs/vertebra-py/src/wsgi_worker.rs:117-120`, `src/py/vertebra/server/_asgi/_queues.py:169-180`, `src/py/vertebra/server/_wsgi/_adapter.py:103-104` | stub/placeholder | Four producers emit `x-vertebra-sendfile` / `-range` headers; `rg -i "x-vertebra\|sendfile" src/rs/vertebra-http src/rs/vertebra-core` returns 0 hits. Only consumer is the Python `Response.body` getter (`response.rs:142-168`); the transport sends the Rust `body` field (empty for RSGI `response_file`). Result: RSGI file responses go out with an empty body, and the internal absolute path leaks to clients as a response header on all four paths. | Model file bodies explicitly: `Response { body: ResponseBody::{Bytes, File{path, range}} }`, never as a header. Transport/ingress streams `File` (coordinate with HTTP auditor). Until then, read the file (inside `py.detach`) at `take_response`/submit time and never emit the header. | high (wire behavior + header leak) | `uv run pytest src/py/tests/unit/server/test_rsgi_protocol.py src/py/tests/unit/server/test_wsgi_file_wrapper.py`; add e2e asserting body bytes and no `x-vertebra-*` header in `src/py/tests/unit/server/test_e2e.py` |
| PYB-02 | `src/py/vertebra/supervisor/_worker.py:50`, `src/py/vertebra/server/_native.py:229-242`, `src/rs/vertebra-http/src/main.rs:147-152`, `src/rs/vertebra-http/src/config.rs:193-196` | hot-path performance | `VERTEBRA_WORKERS` is the process count in Rust, but Python passes it as `worker_count` → `pool_size` → `AsgiEventLoopPool(size=pool_size)` when `> 1`. With `vtb-shm-asgi-4w` each of 4 processes spawns 4 loop threads on a GIL build (16 loops contending one GIL per host). `VERTEBRA_THREADS` is ignored whenever `worker_count` is passed. | Rename the Python knob to `loop_shards`, sourced from a dedicated `VERTEBRA_LOOP_SHARDS` (default 1 on GIL builds, `CgroupQuota.effective_workers` on 3.14t). Stop forwarding `VERTEBRA_WORKERS` into `run_worker`. | med (perf defaults change) | `make bench TARGETS=vtb-shm-asgi-4w`; `make bench-ft TARGETS=vtb-ft-native-4w` |
| PYB-03 | `src/rs/vertebra-py/src/api.rs:346-363,414-419` | lock/GIL contention | `submit_raw_response` calls `send_shm_response_detached` (which can call `send_response_blocking`) and `send_response_bytes` while attached to the interpreter, and swallows every error (`let _ =`). It also encodes the frame twice (once into `frame`, again in `send_response`) and returns the frames anyway. | Run all transport I/O under `py.detach`, propagate `TransportError` as `RuntimeError`, and return `None` in SHM mode (return frames only when no transport, for tests). | med | `cargo test -p vertebra-py`; `uv run pytest src/py/tests/unit/server/test_zero_copy_buffer.py` |
| PYB-04 | `src/rs/vertebra-py/src/response.rs:142-168` | lock/GIL contention | `Response.body` getter runs `std::fs::File::open/seek/read_exact` and `std::fs::read` with the GIL/thread attached, silently falling back to empty on errors. | Delete the file branch (after PYB-01, the body is explicit). Getter returns stored bytes only. | low | `uv run pytest src/py/tests/unit/server -k "file or sendfile"` |
| PYB-05 | `src/rs/vertebra-py/src/api.rs:110-185,190-252` | duplication | `submit_results` and `submit_response` duplicate SHM prep, the `(bool, TransportError)` error mapping (148-156 vs 218-226), and the bridge fallback; stale message `call init_batch_channel() first` (164, 234) names a function that does not exist. | One private `ResponseSubmitter::submit(py, iter)` used by both; single error type; message names `init_transport()`. | low | `cargo test -p vertebra-py`; `uv run pytest src/py/tests/unit/ffi/test_ffi.py` |
| PYB-06 | `src/rs/vertebra-py/src/response.rs:35-55,87`; `api.rs:118-129` | allocation/copy | Every response body is copied into `Bytes` (`Bytes::copy_from_slice`) and then copied again into the SHM slot; each header becomes two `Vec<u8>` allocations and is re-encoded with `encode_headers_compact` at submit. | Store body as `Bytes::from_owner(PyBackedBytes)` for `bytes` inputs (zero-copy until slot write) and store headers pre-encoded (`Bytes` compact form) at construction; decode lazily in the `headers` getter. | med (deferred decref without GIL) | `make bench SCENARIOS=body_10k,json`; `cargo test -p vertebra-py` |
| PYB-07 | `src/py/vertebra/server/_native.py:565-663`; `api.rs:144,214` | hot-path performance | Async mode submits one response per FFI call; each `submit_response` flushes the response notifier (one eventfd/pipe write syscall per response). | Coalesce per loop tick: `ResponseSink` that buffers and flushes via `submit_results` from a `loop.call_soon` callback, or a Rust-side `submit_response(..., flush=False)` + `flush_responses()`. | med | `make bench TARGETS=vtb-shm-native-4w SCENARIOS=hello,io_10ms` |
| PYB-08 | `src/py/vertebra/server/_native.py:57-167` vs `src/py/vertebra/server/_runtime_tuning.py:47-246` | duplication | `AsyncRuntime` reimplements `spawn`, `spawn_batch`, `schedule`, `schedule_batch`, `stop` of `AsgiEventLoopPool` line-for-line (single loop). `_asgi/_runner.py:19` imports it from `_native`, forcing function-scoped imports in `_native` to dodge the cycle (`_native.py:327,361`). | Delete `AsyncRuntime`; use `EventLoopPool(size=1)` everywhere. Move the pool to `server/_loops.py` so `_asgi` never imports `_native`. | low | `uv run pytest src/py/tests/unit/server/test_runtime_tuning.py src/py/tests/unit/ffi/test_ffi.py` |
| PYB-09 | `src/py/vertebra/server/_runtime_tuning.py:90-93` | naming | `AsgiEventLoopPool.loop` property advances the round-robin counter on every read (`return self.next_loop()`); `_dispatch_batch` (`_native.py:483`) and `_instantiate_mount_runners` (`363-367`) read it as if it were stable. | Remove the property; callers use `primary_loop` (shard 0) or `next_loop()` explicitly. Rename class to `EventLoopPool` (it is not ASGI-specific). | low | `uv run pytest src/py/tests/unit/server/test_runtime_tuning.py` |
| PYB-10 | `src/py/vertebra/server/_runtime_tuning.py:148-209`; `_native.py:125-167` | allocation/copy | `schedule`/`schedule_batch` wrap each coroutine in an extra `_wrap()` coroutine plus a Task plus a closure per item. | Use `asyncio.run_coroutine_threadsafe` (single call) or create the task and chain with `asyncio.futures._chain_future`-equivalent `task.add_done_callback`; one closure per shard batch. | low | `uv run pytest src/py/tests/unit/server/test_runtime_tuning.py -k schedule` |
| PYB-11 | `src/py/vertebra/server/_asgi/_runner.py:30-33,180-270` | over-abstraction | ~90 lines of hand-rolled coroutine stepping (`_drive_coro`, `_step_coro`, `_handle_yielded`, `_resume_coro_in_task`, `_BatchProcessor`, `_DIRECT_SEND` env toggle) reimplement eager task start; `_drive_coro` is a pure alias of `_step_coro`; `_current_loop` uses `get_event_loop_policy()` (deprecated in 3.14). | Install `asyncio.eager_task_factory` on owned loops (3.12+) and use `loop.create_task` in one `call_soon_threadsafe` per batch; delete the driver and the env toggle. | med (anyio/contextvar semantics; eager factory is the stdlib-correct version) | `uv run pytest src/py/tests/unit/server/test_asgi.py`; `make bench TARGETS=vtb-shm-asgi-4w` |
| PYB-12 | `src/py/vertebra/server/_native.py:574-582,589-603,624-638,652-658` | allocation/copy | A new closure (sync) or coroutine function (async/RSGI/WSGI) is defined per request inside the dispatch loop, with default-arg capture. | Module-level `async def _run_async(invoker, request, sink)` / `def _run_sync(...)`; dispatch passes args (`executor.submit(_run_sync, inv, req, sink)`). | low | `uv run pytest src/py/tests/unit/server/test_runner.py` |
| PYB-13 | `src/py/vertebra/server/_native.py:403,501,572,587,819` | hot-path performance | `_handler_dispatch_spec` (an `lru_cache` keyed on the handler) is called 2x per request (partition + dispatch). | Precompile `dict[int, HandlerSpec]` once in `run_worker` (it already walks handlers at `226-227`); partition stores the spec in the plan. | low | `uv run pytest src/py/tests/unit/server/test_runner.py -k partition` |
| PYB-14 | `src/py/vertebra/server/_native.py:453,464,513-517,525-529,561,580,595-601,630-636,656,661`; `_asgi/_adapter.py:53-66`; `_rsgi/_adapter.py:117-122`; `_wsgi/_runner.py:107-112` | duplication | 14 inline constructions of 500/404 `Response(status=..., body=b"internal server error", ...)` with inconsistent bodies/headers (`"Internal Server Error (FFI Error)"` at `_asgi/_runner.py:170`). WSGI async-path error at `_native.py:655` is not logged. | `Response.error(status, request_id)` staticmethod in Rust (interned bodies), used everywhere; one logging helper. | low | `uv run pytest src/py/tests/unit/server` |
| PYB-15 | `src/py/vertebra/server/_native.py:485-563` | dead code | The blocking `async_mode=False` branch of `_dispatch_batch` is used only by tests (`test_rsgi_protocol.py:258`, `test_ffi.py:20`); `run_worker` always passes `async_mode=True`. RSGI mounts in that branch run serially with `.result()` per request (548-556). | Delete the blocking branch and `_call_handler`; tests drive the async path through a test `ResponseSink`. | low | `uv run pytest src/py/tests/unit/server/test_rsgi_protocol.py src/py/tests/unit/ffi/test_ffi.py` |
| PYB-16 | `src/py/vertebra/server/_native.py:798-826` | dead code | `_handler_params` has 0 references; `_handler_meta`, `_resolve_handler_args`, `_call_handler` are referenced only by tests (`test_runner.py:59-66,161-199,560-575`). | Delete all four; tests target `HandlerSpec`. | low | `uv run pytest src/py/tests/unit/server/test_runner.py` |
| PYB-17 | `src/py/vertebra/server/_native.py:184,196,482` | stub/placeholder | `run_worker(error_handler=...)` is accepted and discarded (`_ = error_handler`); `_dispatch_batch(handlers=...)` likewise (`_ = handlers`). | Wire `error_handler` into the single error path (PYB-14); drop the unused `handlers` param. | low | `uv run pytest src/py/tests/unit/server/test_runner.py -k error` |
| PYB-18 | `src/py/vertebra/server/_native.py:211-224`; `src/rs/vertebra-py/src/api.rs:34-41,71` | dead code | Python branches on "all defaults → `init_transport()` else pass same values"; Rust discards `ring_name` (`let _ = ring_name`). | Single `init_transport(mode=..., slot_size=..., slots=..., batch_size=...)` call; remove `ring_name` from Rust, stub, and env plumbing. | low | `uv run pytest src/py/tests/unit/ffi/test_ffi.py` |
| PYB-19 | `src/py/vertebra/server/_native.py:688-696` | layering/organization | SHM loop decides whether mounts are already instantiated via `type(adapter).__name__ in {"AsgiRunner", ...}` string compare. | Instantiate mounts once in `run_worker` for both modes; pass instantiated mounts. | low | `uv run pytest src/py/tests/unit/server -k shm` |
| PYB-20 | `src/py/vertebra/server/_native.py:699-729` | hot-path performance | Each wake does `asyncio.wait_for(ready_event.wait(), timeout)` (timeout handle + Event wait + clear) before draining; `asyncio._get_running_loop()` private API at `607,642`. | Drain directly in the `add_reader` callback (`clear_request_notification` → `try_get_batch` loop); stop via signal handler calling `loop.stop()`; use `asyncio.get_running_loop()` in a try. | med | `make bench TARGETS=vtb-shm-native-4w` |
| PYB-21 | `src/py/vertebra/server/_native.py:290-306` vs `src/py/vertebra/server/_rsgi/_adapter.py:40-56` | duplication | Two protocol detectors with different rules: `_detect_protocol` uses `>= 3` params and defaults to `"wsgi"`; `detect_interface` uses `== 3`/`== 2` and defaults to `"asgi"`. | Keep one `detect_interface` in `server/_protocol.py`; delete `_detect_protocol`. | low | `uv run pytest src/py/tests/unit/server/test_rsgi_protocol.py -k detect` |
| PYB-22 | `src/py/vertebra/server/_native.py:36-43`; `_runtime_tuning.py:18-23`; `supervisor/_worker.py:28`; `_asgi/_extensions.py:24-45` | standards violation | Re-export shims: `_native.__all__` re-exports `AsgiEventLoopPool`, `GilAuditReport`, `is_free_threaded`; `_runtime_tuning.__all__` re-exports `GilAuditReport`; `_worker.__all__` re-exports `import_app`; `_asgi/_extensions` re-exports 7 Litestar symbols. | Remove all re-exports; import from defining modules. | low | `make lint` |
| PYB-23 | `src/py/vertebra/server/_asgi/_extensions.py:12-20` | layering/organization | Server core imports `vertebra.extensions.litestar` at module top (pulls Litestar integration into every ASGI import). | Move framework integrations (`LitestarExtension`, re-exports) into `vertebra.extensions.litestar`; `_asgi` exposes only wire helpers. | med (import graph) | `uv run python -X importtime -c "import vertebra.server._asgi"`; `make py-test` |
| PYB-24 | `src/py/vertebra/server/_asgi/_extensions.py:250-371` | stub/placeholder | `FastApiExtension.try_handle` always returns `None`; the extension registry (`register_extension`/`get_extensions`, auto-registered at import) is never consulted by `AsgiAdapter`/`AsgiRunner`; `LitestarExtension._resolve_handler` rebuilds the full route table per call. | Either wire the registry into `AsgiAdapter.run_single` with a precompiled route table (Litestar) or delete the registry + `FastApiExtension`. | med | `uv run pytest src/py/tests/unit/server/test_asgi_scope_and_extensions.py` |
| PYB-25 | `src/rs/vertebra-py/src/asgi_scope.rs:99-119`; `src/py/vertebra/server/_asgi/_queues.py:151-167,182-202`; `src/py/vertebra/server/_asgi/_runner.py:155-160` | stub/placeholder | `scope["extensions"]` advertises `zerocopysend`, `trailers`, `early_hint`, `debug`, `tls`, `pathsend`; `AsgiSend` copies the "zero-copy" view (`bytes(view)`), and `trailers`/`early_hints`/`debug_info` collected in `AsgiResponse` are dropped when `_make_dispatch_coro` builds the native `Response` (status/body/headers only). `tls` is advertised on plaintext connections. Each access builds 9 dicts. | Advertise only what the transport carries; add `trailers`/`early_hints` fields to native `Response` or stop advertising; pass the memoryview to `Response.from_buffer` for real zero-copy; build `tls` only when TLS; share one prebuilt extensions dict copied per scope. | med (spec conformance) | `uv run pytest src/py/tests/unit/server/test_asgi_scope_and_extensions.py` |
| PYB-26 | `src/py/vertebra/server/_asgi/_scope.py:31-83`; `_asgi/_adapter.py:9` | dead code | Python dict `AsgiScope` + `build_scope` fallback; native `AsgiScope` is imported unconditionally at `_adapter.py:9`, so the fallback only runs for duck-typed test doubles. `hasattr(request, "asgi_scope")` runs per request. Name collides with native `AsgiScope`. | Delete `_scope.py`; `run_single` calls `request.asgi_scope(self._mount_prefix)`. | low | `uv run pytest src/py/tests/unit/server/test_asgi.py` |
| PYB-27 | `src/py/vertebra/server/_asgi/_adapter.py:36-95`; `_asgi/_runner.py:124-144` | duplication | `run_single` and `handle_path` duplicate scope/sender setup; `run_stream_session` reaches into `self._adapter._app`; `create_stream_queues` is a pass-through constructor. | Merge `AsgiAdapter` into `AsgiRunner` (one class owning `app`); drop the pass-through. | low | `uv run pytest src/py/tests/unit/server/test_asgi.py src/py/tests/unit/server/test_realtime_backpressure.py` |
| PYB-28 | `src/py/vertebra/server/_asgi/_queues.py:58-68`; `src/rs/vertebra-py/src/request.rs:139-147` | allocation/copy | `AsgiReceive` reads `request.body` (fresh `PyBytes` copy of the slab per call); `Request.body_view` (zero-copy memoryview) exists but is unused in production. | Deliver `bytes(request.body_view)` only when the app needs bytes (ASGI requires bytes, so one copy) and drop the `isinstance`/`bytes()` re-wrap; cache the body on `Request` via `OnceLock<Py<PyBytes>>` so ASGI/WSGI/RSGI share one copy. | low | `make bench SCENARIOS=body_1k,body_10k,echo` |
| PYB-29 | `src/rs/vertebra-py/src/wsgi_environ.rs:286-298,338-385` | hot-path performance | `ensure_headers` builds `HashMap<String,String>` (2 allocs/header) and drops duplicate headers (last wins), while the uncached path returns the first match: `HTTP_X` differs before vs after `keys()`/`len()` is called. PEP 3333 requires combining duplicates with `", "`. `__contains__` allocates `to_ascii_lowercase().replace(...)` per call. | Replace cache with `Vec<(CgiKey, Range)>` over the slab built once; combine duplicates; lookups compare bytes without allocation (reuse `eq_http_header_name`). | med (behavior fix) | `uv run pytest src/py/tests/unit/server/test_wsgi.py` |
| PYB-30 | `src/rs/vertebra-py/src/wsgi_environ.rs:391-430,578-580` | allocation/copy | `__len__` and `__iter__` call `collect_keys`, which `format!`s every `HTTP_*` key and clones every static key into `String`. | Precompute CGI keys once in the header cache (PYB-29); iterator yields interned/borrowed keys. | low | `uv run pytest src/py/tests/unit/server/test_wsgi.py` |
| PYB-31 | `src/py/vertebra/server/_wsgi/_environ.py:34-180`; `src/py/vertebra/server/_wsgi/_runner.py:34-112`; `src/rs/vertebra-py/src/wsgi_worker.rs:57-128` | duplication | Three WSGI environs/inputs (Rust `WsgiEnviron`/`WsgiInput`, Python `WsgiEnviron`/`BodyStream`) and three runners (Rust `run_wsgi_callable` used only by tests, Python `WsgiAdapter.run_environ` used in prod, `WsgiRunner` loop with 0 references outside `__init__`). Python fallback quotes `PATH_INFO` (`_environ.py:106`) while Rust does not (behavior drift). | Keep native `WsgiEnviron`/`WsgiInput` + native `run_wsgi_callable` as the single runner (`WsgiAdapter.__call__` → `run_wsgi_callable(app, request.wsgi_environ(...), request_id)`); delete Python environ, `BodyStream`, `WsgiRunner`. | med | `uv run pytest src/py/tests/unit/server/test_wsgi.py src/py/tests/unit/server/test_wsgi_file_wrapper.py`; `make bench TARGETS=vtb-shm-wsgi-4w` |
| PYB-32 | `src/py/vertebra/server/_wsgi/_adapter.py:80-82`; `_wsgi/_extensions.py:31-42`; `src/rs/vertebra-py/src/wsgi_environ.rs:172-183`; `wsgi_worker.rs:79-83` | lock/GIL contention | File wrappers are stat'ed up to 3x (`Path::is_file` under GIL in `WsgiFileWrapper::py_new`, `Path(path).is_file()` in `resolve_wsgi_file_wrapper`, `metadata` in `run_wsgi_callable`), then the whole file is read into memory and the sendfile header is added too. | Stat once (inside `py.detach`) when the response is built; carry `ResponseBody::File` (PYB-01). | low | `uv run pytest src/py/tests/unit/server/test_wsgi_file_wrapper.py` |
| PYB-33 | `src/py/vertebra/server/_wsgi/_extensions.py:45-67` | stub/placeholder | `LargeBodyBypass` parses `CONTENT_LENGTH` and only logs; it bypasses nothing. | Implement the bypass (stream `wsgi.input` from the slab without materializing) or delete it. | low | `uv run pytest src/py/tests/unit/server/test_wsgi.py -k large` |
| PYB-34 | `src/rs/vertebra-py/src/rsgi_scope.rs:11-123` vs `src/rs/vertebra-py/src/headers.rs:63-176` | duplication | `RsgiHeaders` (owned `Vec<(String,String)>`, 2 allocs/header at scope build) duplicates `Headers` (zero-copy over compact bytes) minus `get_all`. | Delete `RsgiHeaders`; add `get_all` to `Headers`; `RsgiScope.headers` returns `Headers` over the slab. | low | `uv run pytest src/py/tests/unit/server/test_rsgi_protocol.py` |
| PYB-35 | `src/rs/vertebra-py/src/rsgi_scope.rs:142-171` | allocation/copy | `RsgiScope::from_batch` eagerly copies 8 `String`s + headers per request even if the app reads only `path`. | Hold `Arc<BatchSnapshot>` + index (like `Request`) and materialize getters lazily. | low | `uv run pytest src/py/tests/unit/server/test_rsgi_protocol.py` |
| PYB-36 | `src/py/vertebra/server/_rsgi/_adapter.py:89-126,128-158`; `src/rs/vertebra-py/src/request.rs:246-251` | duplication | `run_single` copies the body twice (`request.body` → `PyBytes`, then `RsgiHttpProtocol(body=...)` → `Bytes::copy_from_slice`) although `Request.rsgi_protocol()` builds it from the slab in one copy (used only by tests); `handle_request` is an alias of `run_single`; `handle_http` duplicates `run_single` setup. | `run_single` uses `request.rsgi_scope(...)`/`request.rsgi_protocol()`; delete `handle_request` alias; `handle_http` builds a synthetic `Request` and calls `run_single`. | low | `uv run pytest src/py/tests/unit/server/test_rsgi_protocol.py` |
| PYB-37 | `src/rs/vertebra-py/src/rsgi_protocol.rs:124-126,35-62,244-258` | stub/placeholder | `client_disconnect()` resolves immediately (apps awaiting it think the client left at once); `response_stream` buffers the whole stream in `Arc<Mutex<Vec<u8>>>` and `take_response` copies it again (`Bytes::copy_from_slice`). | Back `client_disconnect` with a real future set by the transport (or a never-resolving future until shutdown); stream chunks as `submit_raw_response` `FLAG_STREAMING` frames; at minimum `mem::take` the Vec instead of copying. | med | `uv run pytest src/py/tests/unit/server/test_rsgi_protocol.py -k stream` |
| PYB-38 | `src/rs/vertebra-py/src/rsgi_protocol.rs:271-353` | stub/placeholder | `RsgiWebsocketProtocol` is an in-memory fake (preloaded `incoming` list, `sent_messages()` accessor); referenced only by `src/py/tests`. | Move to a test fixture (Python) or wire to `WebSocketMountRegistry`; drop from the public module until wired. | low | `uv run pytest src/py/tests/unit/server/test_rsgi_protocol.py -k websocket` |
| PYB-39 | `src/rs/vertebra-py/src/rsgi_protocol.rs:13,52-62,318-353` | allocation/copy | Every `send_bytes`/`send_str`/`client_disconnect` allocates a new `_RsgiReadyAwaitable` pyclass. | One frozen stateless `RsgiDone` singleton (`PyOnceLock`) for `None` results; keep the valued awaitable only for `__call__`/`accept`. | low | `cargo test -p vertebra-py` |
| PYB-40 | `src/rs/vertebra-py/src/rsgi_protocol.rs:378-381` | dead code | `#[allow(dead_code)] fn _assert_py_string`. | Delete. | low | `cargo clippy -p vertebra-py -- -D warnings` |
| PYB-41 | `src/rs/vertebra-py/src/asgi_bridge.rs:12-41`; `src/rs/vertebra-py/Cargo.toml:13-16` | dead code | `pub async fn read_file_range_chunks` has 0 callers; it is the only user of `tokio`, `tokio-util`, `futures-util` in `vertebra-py`. | Delete the fn and the three deps; rename module to `file_io.rs`. | low | `cargo build -p vertebra-py`; `cargo tree -p vertebra-py -e normal --depth 1` |
| PYB-42 | `src/rs/vertebra-py/src/asgi_bridge.rs:44-95` | allocation/copy | `stream_file_chunks` reads the whole range into `Vec<Vec<u8>>` (one `to_vec` per chunk) then copies each into `PyBytes`; callers `b"".join` it (`_queues.py:176-177`): 3 copies of the file. | After PYB-01, pathsend no longer reads in Python. If a chunk API remains, allocate `PyBytes::new_with` and read into it inside detach (1 copy). | low | `uv run pytest src/py/tests/unit/server -k pathsend` |
| PYB-43 | `src/rs/vertebra-py/src/asgi_scope.rs:251-299`; `request.rs:62-108`; `wsgi_environ.rs:438-501` | duplication | Three `from_parts` builders duplicate the method-tag table (with different unknown fallbacks: `0`, `255`, `0`) and the slab/`TransportNode` layout; `AsgiScope::py_new` (219-238) builds a fourth. | One `SyntheticRequest::build(method, path, query, headers, body) -> Arc<BatchSnapshot>` helper (method tag via `vertebra_core` parser); all `from_parts` call it. | low | `cargo test -p vertebra-py`; `uv run pytest src/py/tests/unit/server -k from_parts` |
| PYB-44 | `src/rs/vertebra-py/src/headers.rs:178-210` | duplication | `HeadersIterator::__next__` reimplements the compact-header parser instead of using `iter_raw_headers`; allocates an owned `String` per key. | Store an offset and reuse `iter_raw_headers`; yield `PyString` (interned for known names). | low | `cargo test -p vertebra-py` |
| PYB-45 | `src/rs/vertebra-py/src/request.rs:47-54` | allocation/copy | `Request.headers` copies the header bytes out of the slab (`Bytes::copy_from_slice`) and clones a `Headers` per access. | Make the snapshot slab `Bytes` (core change) and slice it; cache `Py<Headers>` in `OnceLock`. | med (core type) | `make bench SCENARIOS=hello,json` |
| PYB-46 | `src/rs/vertebra-py/src/batch.rs:44-61`; `src/rs/vertebra-py/src/response.rs:117-135,206-213` | dead code | `RequestBatch.submit_buffers` has 0 references; `Response.from_buffer` duplicates `Response.__new__` exactly; `get_headers`/`get_body` duplicate pub fields. | Delete `submit_buffers`, `from_buffer` (or make `__new__` the only constructor), and the getters (or make fields private and keep getters). | low | `cargo test -p vertebra-py`; `make py-test` |
| PYB-47 | `src/py/vertebra/_vertebra.pyi:27,54,84,114,115,273,274,280-284`; `src/rs/vertebra-py/src/lib.rs:44` | docs drift | Stub declares `AsgiScope.pop`, `WsgiEnviron.pop`, `RequestBatchIterator.submit_buffers` (none exist in Rust); `update(other=None, **kwargs)` but Rust takes one positional; `raw_query -> bytes \| None` (always bytes); `wsgi_environ(server_port: int \| str)` (Rust `u16`); `Response.status/body/headers` as assignable attrs (getter-only); `__version__` exported but missing. | Fix the stub, and either implement `pop` on both mappings (frameworks call `scope.pop`) or drop it; add a stubtest gate. | low | `uv run python -m mypy.stubtest vertebra._vertebra --allowlist /dev/null` |
| PYB-48 | `src/py/vertebra/server/_native.py:832-841` | dead code | `_coerce_response` catches `AttributeError` on `raw.request_id = request_id`, but `Response` defines a `request_id` setter (`response.rs:175-178`): the rebuild branch never runs. | Delete the try/except branch. | low | `uv run pytest src/py/tests/unit/server/test_runner.py -k coerce` |
| PYB-49 | `src/rs/vertebra-py/src/wsgi_worker.rs:35-42`; `rsgi_protocol.rs:130-134,232-240` | lock/GIL contention | `WsgiStartResponse.__call__` holds a `std::sync::Mutex` across Python API calls (`exc.get_item(1)`, `PyErr::from_value`); RSGI uses `if let Ok(mut st) = self.state.lock()` and silently ignores poisoning. | Use `pyo3::sync::MutexExt::lock_py_attached` (or extract Python values before locking); treat poisoning as an error. | low | `cargo test -p vertebra-py`; `make bench-ft` |
| PYB-50 | `src/rs/vertebra-py/src/wsgi_environ.rs:16,329-334`; `src/py/vertebra/server/_wsgi/_environ.py:31` | stub/placeholder | `wsgi.errors` is `sys.stderr` captured once per process (`PyOnceLock`); later reassignment (logging redirection, pytest capture) is ignored. | Look up `sys.stderr` per environ access (cheap attr read) or accept an `errors` stream at worker init. | low | `uv run pytest src/py/tests/unit/server/test_wsgi.py -k errors` |
| PYB-51 | `src/py/vertebra/server/_gil_audit.py:55-76,79-95` | stub/placeholder | `quarantine_unsafe_modules` logs "falling back to standard asyncio loop" but changes nothing; `audit(before_modules=..., before_extensions=...)` are two names for one value; `run_worker` calls `audit()` with no baseline (`_native.py:236`), so every loaded extension becomes a "suspect". | Make quarantine set the loop policy/factory it claims; keep one `before_extensions` param; snapshot extensions in `supervisor/_worker.py` before `import_app` and pass it. | low | `uv run pytest src/py/tests/unit/server/test_runtime_tuning.py -k gil` |
| PYB-52 | `src/py/vertebra/server/_runtime_tuning.py:26-31` vs `_gil_audit.py:17-22` | duplication | `is_free_threaded` and `_is_gil_currently_enabled` both wrap `sys._is_gil_enabled`. | Single `GilAuditReport.gil_enabled()` staticmethod; `is_free_threaded()` = `not gil_enabled()`. | low | `make lint` |
| PYB-53 | `src/py/vertebra/server/_shm_hardening.py:13-67`; `src/py/vertebra/server/platform.py:22-156` | stub/placeholder | `ShmAllocationPolicy` (fields `shm_dir`, `min_free_bytes` never read), `CgroupQuota`, `LameDuckController`, `ZPagesRegistry` are imported only by tests; nothing in worker/supervisor uses them. | Wire: `CgroupQuota.detect()` sizes loop shards (PYB-02); `LameDuckController.enter_lame_duck()` on SIGTERM in `_install_signal_handlers`; z-pages via `vertebra-http` health route; SHM policy into the Rust ring factory (or move it to `vertebra-core`). | med | `uv run pytest src/py/tests/unit/telemetry/test_telemetry_and_platform.py` |
| PYB-54 | `src/py/vertebra/server/platform.py:111,148` | standards violation | Uptime is computed from `time.time()` (wall clock jumps under NTP). | Use `time.monotonic()` for uptime, keep wall time only for display. | low | `uv run pytest src/py/tests/unit/telemetry -k statusz` |
| PYB-55 | `src/py/vertebra/telemetry/logger.py:1,116-128,171-207`; `src/rs/vertebra-core/src/logger.rs:131-199` | duplication | Docstring claims a "Non-blocking GIL-releasing" logger, but `LogSink` is a pure-Python `deque` with no writer (only `drain()`), `overflow_policy="block"` is accepted and treated as drop-oldest, and the len-check/popleft/append + counters race on 3.14t. Rust `AsyncLogCore`/`BoundedLogQueue` implement exactly this and are not bound. | Expose `AsyncLogCore` through `_vertebra` (`LogSink` = thin pyclass, `write_bytes` detaches); delete the Python queue. | med | `uv run pytest src/py/tests/unit/telemetry`; `cargo test -p vertebra-core logger` |
| PYB-56 | `src/py/vertebra/telemetry/otel.py:20-88`; `telemetry/db_propagation.py:73-77`; `src/rs/vertebra-core/src/telemetry.rs:5-62,101` | duplication | Python `TraceContext.parse/format` and `SqlTracePropagator.sanitize_sql` duplicate Rust `W3cTraceContext` and `sanitize_sql_statement` (both unbound to Python). Trace-id extraction is duplicated inside `logger.py:141-154` and `184-207`. | Bind the Rust functions in `vertebra-py` (`telemetry.rs` module) and make the Python types thin wrappers; one `_trace_ids(getter)` helper. | low | `uv run pytest src/py/tests/unit/telemetry` |
| PYB-57 | `src/py/vertebra/telemetry/otel.py:139-168` | hot-path performance | `GenAiRecorder._samples` grows unbounded (no drain, no cap); `samples` copies to a tuple per read. | Bounded `deque(maxlen=...)` + `drain()` like `SpanExporter`. | low | `uv run pytest src/py/tests/unit/telemetry -k genai` |
| PYB-58 | `src/py/vertebra/_bundler.py:1-15`; `pyproject.toml:111` | standards violation | The bundler extension is installed as top-level `_bundler` and re-exported through a shim; no stub exists (`_bundler` is in mypy `ignore_missing_imports`); `GIL_USED` uses `getattr` fallback. | Build with maturin `module-name = "vertebra._bundler"` (like `_vertebra`), delete the shim, add `_bundler.pyi`. | low | `make dev`; `uv run python -c "import vertebra._bundler"`; `make lint` |
| PYB-59 | `src/py/vertebra/server/_ipc/__init__.py:1-5`; `src/py/vertebra/server/_js/__init__.py` | stub/placeholder | `_ipc` docstring: "exists as a namespace for future Python-side IPC helpers"; `_js` is an empty package. No importers. | Delete both packages. | low | `make py-test` |
| PYB-60 | `src/rs/vertebra-py/src/lib.rs:1-8,85-93`; `api.rs:466-486` | test weakness/duplication | Module docs list 4 of 25 exports; `free_threaded_const_exists` asserts nothing (`let _ = FREE_THREADED`); `send_shm_response_with_backpressure` is a `#[cfg(test)]` wrapper around one `py.detach` call. | Update docs; delete the tautology test; inline the wrapper into the test. | low | `cargo test -p vertebra-py` |
| PYB-61 | `src/py/tests/unit/server/test_runner.py:59-66,589,610`; `test_stage1_hotpath_and_314t.py:73-76`; `test_asgi.py:156`; `test_routing.py:64`; `src/py/tests/unit/test_bench_infrastructure.py:13-40` | test weakness/duplication | Tests assert signatures (`"loop" in sig.parameters`), `callable(...)`, `hasattr(Request, "asgi_scope")`, and benchmark config constants (`target.port == 8012`). Ad-hoc files named by stage/PR (`test_stage1_hotpath_and_314t.py`, `test_zero_copy_buffer.py`) and a combined `test_telemetry_and_platform.py`. | Replace with behavior assertions; fold into canonical files (`test_runner.py`, `test_asgi.py`, `test_rsgi_protocol.py`, `telemetry/test_logger.py`, `telemetry/test_otel.py`, `server/test_platform.py`); delete config-constant tests. | low | `make py-test` |
| PYB-62 | ruff `--select ALL` over `server/`, `supervisor/`, `telemetry/`, `_bundler.py` | standards violation | 235 hits; relevant: `PLC0415` import-outside-top-level ×16 (e.g. `_native.py:197-198,327,361,567,673-680`, `_wsgi/_runner.py:63-64`), `PLW0603` global ×5 (`_native.py:195,283,428`), `SLF001` ×5, `C901`/`PLR0912`/`PLR0915` ×12 (`_dispatch_batch`), private-module imports `vertebra.utils.serializers._json` (`_native.py:31`, `_asgi/_extensions.py:22`). | Fix via the module splits (cycles disappear with PYB-08); import serializers from the public package; replace `_RUNNING`/`_wsgi_executor` globals with a `Worker` object. | low | `make lint` |

## Python framework, extensions, storage, db, utils (prefix FW)

Tooling summary:

- `ruff check` with project config: clean.
- `ruff check --select ALL --statistics`: **749** hits. Mostly noise: ANN401 250, CPY001 99, D107 74, COM812 55, TC003 26, PLR0913 24. The ones worth fixing: BLE001 16, PLR2004 14, TC001 15, PLC0415 10, PLW0603 9, SLF001 9, S608 6, PYI036 6, S110 4, RUF022 2, S324 1, S602 1, PLW1509 1. See FW-48.
- `vulture --min-confidence 60`: **378** hits (186 unused function/method, 145 unused variable (mostly protocol params), 3 unused imports at 90%: `utils/module_loader.py:20 ModuleType`, `utils/sync_tools.py:21 TracebackType`, `utils/type_guards.py:21 TypeGuard`; 100%: `extensions/apex/session.py:29`, `extensions/sqlspec/plugin.py:41,50` unused `statement` protocol params). Files with the most hits: `extensions/sqlalchemy/dialects.py` 25, `framework/security` 20, `extensions/sqlspec/storage.py` 19, `extensions/grpc/_decorator.py` 18, `framework/email` 14, `framework/queues` 13, `storage/_store.py` 12, `db/oracle/containers.py` 12. Almost every `vertebra.db.*` method is unreferenced from `src/` (e.g. `db/cdc.py:55-116`, `db/datafusion.py:61-76`, `db/postgres.py:48-171`, `db/rls.py:30-36`, `db/pipeline.py:57-101`).
- No inline `#` comments and no `@dataclass` value objects found in scope. `dataclasses` is only used by `utils/type_guards.py` to *serialize user* dataclasses (legitimate).
- Startup cost, measured: warm `import vertebra.framework` = **118 ms**, of which **81 ms** is `framework.channels → extensions.litestar` (FW-14).
- Micro timings (repo venv, CPython 3.12): `LiveState.__setattr__` 2.5 µs/write, with 0.93 µs of that spent recomputing FNV-1a. `_make_handler_id` 4.0 µs in pure Python.

| ID | file:line | Category | Evidence | Proposed change | Risk | Verify |
|---|---|---|---|---|---|---|
| FW-01 | `src/py/vertebra/db/postgres.py:135-142`, `db/mysql.py:58-61`, `db/redis.py:87-104`, `db/valkey.py:61-106` | stub/placeholder | `SyncPostgresDriver.execute_tuples` returns `list(self._in_memory_rows)`, which is always empty. `SyncMysqlDriver.execute_tuples` returns `[]`. Redis and Valkey drivers are `dict`s, and `publish()` returns `1` without sending anything. The `Async*Driver` classes call the sync fakes. `ChannelsBackend.from_valkey` and `MemoryStore.from_valkey` (extensions/litestar) are built on these fakes. | Either bind to a real native client (Rust `vertebra-db` via `_vertebra`) behind the same `SyncPostgresDriver`/`AsyncValkeyDriver` names, or rename them to `InMemoryKeyValue`/`NullSqlDriver` test doubles under `tests/`, and remove the `from_valkey/from_redis` constructors until a real client exists. Keep the config structs (`PostgresConfig`, `ValkeyConfig`) and the RESP encoders, which are real. | high: it touches the public "native driver" surface | `uv run pytest src/py/tests/unit/db -q` |
| FW-02 | `extensions/sqlalchemy/dialects.py:128-142,152-170,191-263` | stub/placeholder | `DBAPIConnection.execute_raw_tuples` returns `[]` for any non-SQLite URL. `commit/rollback` are no-ops. Every DBAPI exception is aliased to `Exception`. `register_all()` calls `registry.register("postgresql.vtb", module, "PostgresDialect")`, but `PostgresDialect` is a `DialectDescriptor` **instance**, not a `sqlalchemy.engine.Dialect` subclass, so `create_engine("postgresql+vtb://")` cannot work. `SyncEngineAdapter` (266-291) is a thin wrapper over `SyncPool`. | Implement `VertebraSqliteDialect(SQLiteDialect)` etc. as real SQLAlchemy dialect subclasses with `import_dbapi()`, or delete the `+vtb` surface and keep only the URL parsing. Fold `SyncEngineAdapter` into `SyncPool`. | high | `uv run pytest src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py -q` plus a new `create_engine("sqlite+vtb://")` round-trip test |
| FW-03 | `extensions/sqlalchemy/types.py:89-209`, `optimizations.py:18-179`, `alembic.py:20-54` | stub/placeholder | `Vector`/`BinaryJson` are plain classes with `process_bind_param(value, dialect_name)` signatures and no `TypeDecorator` base, so SQLAlchemy never calls them. `SelectInLoadSpec`/`InsertSentinelSpec` only store numbers and never produce `selectinload()`/`orm_insert_sentinel()`. Both `create()` methods take duplicate alias args (`batch_size`/`chunksize`, `sentinel_column`/`column_name`). `AlembicImpl` is a `Struct` with a `__dialect__` field. Bug: `Vector.process_result_value` always decodes SQLite blobs as float32 (`types.py:148-151`) even when `storage_format="float64"`. | Make `Vector(TypeDecorator)` and `BinaryJson(TypeDecorator)` real types. Have the specs return real loader options (`SelectInLoadSpec.to_option()`). Drop the alias params. Make `AlembicImpl` a `DefaultImpl` subclass. | med | `uv run pytest src/py/tests/unit/extensions -k "vector or alembic or optimizations"` |
| FW-04 | `extensions/sqlspec/storage.py:148-187` | stub/placeholder | `export_arrow_ipc` writes `b"ARROW1\x00\x00" + msgpack`, which is not Arrow IPC. `export_parquet` writes `PAR1 + msgpack + PAR1`, which is not Parquet. `export_arrow_capsule` calls `__arrow_c_stream__()`, discards the result, and writes an empty payload (164-165). | Use `pyarrow.ipc.new_stream` and `pyarrow.parquet.write_table` behind `import_optional("pyarrow")`, or rename to `export_msgpack_rows` and drop the format claims. | med | `uv run pytest src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py -k arrow` |
| FW-05 | `extensions/sqlspec/oracle.py:123-156` | bug | The bind dict contains `rrf_k` and `candidate_limit`, but the SQL inlines both with `int()` and has no `:rrf_k`/`:candidate_limit` placeholders. python-oracledb rejects unused named binds (`DPY-4008`). `table`, `id_column`, `select_columns` and `vector_metric` are interpolated without validation. | Remove the two extra binds. Validate identifiers through one shared `SqlIdentifier.validate()` helper (see FW-12). Make `OracleHybridSearch` a module of functions or keep the class but add the validator. | low | `uv run pytest src/py/tests/unit/extensions -k rrf` plus a new assertion that `set(binds) == placeholders(sql)` |
| FW-06 | `wasm/__init__.py:43-91` | stub/placeholder | The `WasmPluginHost` docstring claims a "WASI 0.2 sandbox with fuel and memory limits". `register_module` only checks the 8-byte magic header. `call_export` runs a **Python** callable registered in `_host_exports`, and "fuel" is a caller-supplied `fuel_cost` compared against `max_fuel`. No Wasm executes. The class also lacks `__slots__`. | Bind to a real runtime (wasmtime via a Rust crate) or mark the feature unimplemented: keep `WasmSandboxPolicy` and delete the host until it exists. Tests that exercise it (`test_framework_subdomains.py:172`) test a stub. | med | `uv run pytest src/py/tests/unit/framework/test_framework_subdomains.py -k wasm` |
| FW-07 | `framework/queues/__init__.py:65-124`; `extensions/worker/runner.py:28-80` | stub/placeholder, duplication | `QueueService` is a `list` with `pop(0)` (O(n)). `QueueConfig.backend_url` ("memory, Valkey, Redis, Postgres, Oracle") and `max_concurrency`, `TaskRetryPolicy` and `CronSchedule` are never read. `extensions.worker.WorkerRunner` separately implements lanes with semaphores, and the two "lane" concepts (`TaskLane` cpu/io/agent vs `LaneConfig.queue_name`) never meet. | Merge both into one `framework/queues` module: `QueueService` dispatches through `WorkerRunner` lanes and uses a `collections.deque`; retry/cron are either applied or removed. Delete `extensions/worker` (move `run_oneshot`/`parse_lanes` into `QueueService`). | med | `uv run pytest src/py/tests/unit/extensions/test_worker_lanes.py src/py/tests/unit/framework -k queue` |
| FW-08 | `framework/email/__init__.py:43-79` | stub/placeholder | `EmailConfig` advertises smtp/resend/sendgrid/mailgun/ses plus host, port and api_key. `EmailService.send` always appends to `_outbox` and returns `f"msg-{n}"` whatever the backend. | Implement `smtp` via `smtplib` (stdlib) and make the other backends explicit `NotImplementedError` until added, or narrow `EmailBackendType` to `"memory"`. | low | `uv run pytest src/py/tests/unit/framework -k email` |
| FW-09 | `extensions/alchemy/broker.py:318-360, 195-230` | stub/placeholder, hot-path | The `ShmBrokerTransport` docstring says it is "coordinating across worker processes", but it creates a `ShmRegion` that is never read or written. `publish()` does `_ = self._encoder.encode(message)` and discards the result. Leasing is purely in-process. `acquire` polls with `condition.wait(min(remaining, 0.01))` and `acquire_async` busy-sleeps 2 ms. `metrics()` copies fields by hand (270-278). | Implement the SHM slot table (or delete `ShmBrokerTransport` and `use_shm`). Replace polling with `Condition.wait()` plus `notify()` and an `asyncio.Condition`/`Event` for async. Use `msgspec.structs.replace` for the snapshot. | med | `uv run pytest src/py/tests/unit/extensions -k broker` |
| FW-10 | `extensions/apex/apexlang.py:31-53`; `apex/openapi.py:101-125,151-152`; `apex/gateway.py:248-255` | stub/placeholder | `ApexLangRunner.compile_spec` ignores `spec_source` and emits two `SET_*` calls plus a comment. ORDS→MCP tools use `_noop_tool_handler`, which echoes kwargs. `execute_ords_rest` without a transport returns `{"status": "dispatched"}` and sends nothing. | Make `ords_transport` required. Generate MCP tool handlers that issue ORDS HTTP calls through an injected client. Implement ApexLang compilation or remove the runner. | med | `uv run pytest src/py/tests/unit/extensions/test_oracle_apex_and_gateway.py` |
| FW-11 | `db/datafusion.py:46-84`; `db/cdc.py:39-118` | stub/placeholder | `DataFusionEngine.explain_plan` is a substring match of table names in the SQL. No DataFusion is involved. `ChangeFeed` "spills" oversized payloads into an in-process `dict` (`_spill_store`, line 73) instead of `spill_table`, so other workers cannot hydrate them. `_history` grows without bound (99). The two `encode_json` envelopes duplicate each other (74-91). | Back `ChangeFeed` with the spill table through an injected driver, and replace `_history` with a bounded `deque(maxlen=…)`. Bind DataFusion through `datafusion` (optional dependency) or delete the engine and keep `DataFusionConfig`. | med | `uv run pytest src/py/tests/unit/db -k "cdc or datafusion"` |
| FW-12 | `extensions/apex/gateway.py:199-200`; `sqlalchemy/optimizations.py:108-114`; `db/pipeline.py:29-35,122`; `sqlspec/oracle.py:123-149` | standards violation (SQL construction) | `plsql = f"BEGIN {package_procedure}({k} => :{k} …); END;"` uses an agent-supplied procedure name and bind keys. Ruff S608 flags 6 sites. The allow-list in `ApexPolicyEvaluator` only applies when `allowed_packages` is non-empty (65). | Add one `vertebra.db.SqlIdentifier` (validate `^[A-Za-z_][A-Za-z0-9_$#.]*$`, quote per dialect) and route every identifier interpolation through it. Make `allowed_packages` deny-by-default. | med | `uv run pytest src/py/tests/unit -k "apex or pipeline or delete_using"` plus `ruff check --select S608` |
| FW-13 | `framework/channels/__init__.py:5-13`; `framework/stores/__init__.py:7-10` | layering/organization | The framework layer re-exports `ChannelsBackend`/`MemoryStore` from `vertebra.extensions.litestar`, so the core depends on an optional extension. | Move `ChannelsBackend` (rename `ChannelBus`) and `MemoryStore` (rename `ResponseCacheStore`) into `framework/channels.py` and `framework/stores.py`. `extensions/litestar` imports them and keeps only the `swap_litestar_*` adapters. | low | `make py-test` |
| FW-14 | `framework/__init__.py:5-22`; `extensions/litestar/__init__.py:3-46` | hot-path performance (startup) | `-X importtime`: `vertebra.framework` takes 117.9 ms cumulative. `framework.channels` takes 80.7 ms because `extensions.litestar.__init__` eagerly imports `ecosystem → framework.ssr → _ssr._backends → urllib.request` (13.9 ms), `db → db.oracle.wallet` (18.8 ms) and asyncio. The root `vertebra` uses PEP 562 lazy exports; `framework` does not. | Lazy `__getattr__` + `__dir__` in `framework/__init__.py` and `extensions/litestar/__init__.py`. Combined with FW-13, this removes the extension import from the framework path. Target: `import vertebra.framework` under 15 ms. | low | `python -X importtime -c "import vertebra.framework"` plus `uv run pytest src/py/tests/unit/test_utils_init.py` |
| FW-15 | `framework/_inertia.py:199-209` vs `framework/_ssr/_backends.py:28-40`; `framework/ssr/__init__.py:10-48`; `_inertia.py:44-58`; `_ssr/_backends.py:192-213`; `_inertia.py:446-461` vs `_ssr/_prerender.py:56-72` | duplication | `SsrResult` and `SsrClient` are defined twice with different Protocol shapes (`_inertia` lacks `invalidate/stop`), and `ssr/__init__` re-exports the `_inertia` copies. `SsrConfig` (modes `quickjs/unix_socket/http_sidecar`) duplicates `InertiaConfig.ssr_mode/ssr_url/ssr_timeout/ssr_pool_size/sidecar_port`, and neither is consumed by `create_ssr_client(mode="native"/…)`. `SsrFrame` (unix-socket framing) has no client using it. The HTML page shell is duplicated in two places. | One `framework/ssr/` package holding `SsrConfig` (single mode enum `native|sidecar`), `SsrResult`, `SsrClient`, `SsrClient.from_config()`, and `render_page_shell()`. `InertiaConfig` takes `ssr: SsrConfig \| None`. Delete `SsrFrame` until a unix-socket client exists. | low | `uv run pytest src/py/tests/unit/framework/test_ssr.py src/py/tests/unit/framework/test_prerender.py src/py/tests/unit/framework/test_inertia_middleware.py` |
| FW-16 | `framework/_inertia.py` + `framework/inertia/__init__.py:5-45`; `framework/_ssr/` + `framework/ssr/`; `framework/_ssr/__init__.py:1-4` | layering/organization | Private-module plus public-re-export pairs. `_ssr/__init__.py` holds only a docstring containing a stray import line. | Collapse into `framework/inertia/` and `framework/ssr/` packages with no re-export shims. | low | `make py-test` |
| FW-17 | `framework/_types.py:1-49`; used at `_inertia.py:17,373-417` | stub/placeholder | The module docstring says "These are placeholder structs … live in later chapters". `Headers.get` lowercases every pair on every lookup. `InertiaMiddleware.process` returns these stubs. | Delete `_types.py`. `InertiaMiddleware.process` returns `(status, headers: list[tuple[bytes, bytes]], body)` (the shape the litestar fastpath already uses) or the native `Response`. | low | `uv run pytest src/py/tests/unit/framework/test_inertia_middleware.py` |
| FW-18 | `framework/_router.py:25-27,102-118`; `framework/live/__init__.py:22-28,191-207`; Rust `vertebra-http/src/route_table.rs:17`, `vertebra-core/src/protocol/state.rs:237,249` | duplication, hot-path | FNV-1a is implemented twice in Python and again in Rust. Measured cost: `_make_handler_id` 4.0 µs, `compute_field_id` 0.93 µs. `_vertebra.pyi` exposes no free `handler_id/state_id/field_id`. | Expose `handler_id`, `state_id`, `field_id` from `vertebra-py` and use them in `_router` and `live`, keeping a single `vertebra.utils.hashing.fnv1a64/32` fallback only if the native module is optional. | low | `uv run pytest src/py/tests/unit/server/test_routing.py src/py/tests/unit/framework/test_live_state.py src/py/tests/unit/ffi -q` |
| FW-19 | `__init__.py:18,37,49`; `framework/_app.py:13-23`; `extensions/litestar/router.py:12`; `framework/_router.py:67-99` vs `_app.py:106-124` | API surface bloat | The root package exports private `_make_handler_id` alongside its public wrapper `handler_id`. `_app.__all__` re-exports `LifecycleHook` and `_make_handler_id`. `_app` imports four private `_router` helpers. Route decorators are written twice: module `get/post/put/delete/patch` and `App.get/…`. | Public `handler_id(method, path)` only. `App._route` reuses `_router._route`. Remove re-exports from `_app.__all__`. Make `Router.normalize_path/compile_template` staticmethods on `RouteDef`. | low | `uv run pytest src/py/tests/unit/framework/test_app.py src/py/tests/unit/server/test_routing.py` |
| FW-20 | `extensions/litestar/offload.py:14-33,210-305`; `fastpath.py:10-16`; `router.py:18,201-202` | API surface bloat, layering | `offload.__all__` exports 8 underscore names. `fastpath` imports 5 of them. `router.__all__` exports `_unpack_route_handler_map_entry`, which `offload` imports. | Add a `LitestarHandlerIntrospector` class (in `extensions/litestar/introspection.py`) owning `guards/middleware/opt/exception_handlers/request_class/after_response/unpack_entry/is_default_local_fs/rebuild_trie`. Drop the underscore names from every `__all__`. | low | `uv run pytest src/py/tests/unit/extensions/test_litestar_plugin.py` |
| FW-21 | `extensions/litestar/router.py:82-93,160-162`; `offload.py:89-96`; `storage/_store.py:1260-1365`; `storage/registry.py:59-61`; `framework/_inertia.py:131-171`; `sqlspec/plugin.py:237-240,382-385` | dead code (redundant wrappers) | `LitestarRouteCompiler` and `MiddlewareOffloadCompiler` are one-line delegating classes. `extract_litestar_route_entries` is `list(…values())`. `SyncStore`/`AsyncStore` (≈105 lines) only forward to `ObjectStore`, which already has sync and `_async` methods. `register_store` is the same as `register_alias`. The six Inertia prop factories (`defer/merge/…`) only call constructors. `SQLSpecConfig.wrap` and `SQLSpecPlugin.wrap_config` are the same function. | Delete all of them and keep the underlying function or class. Inertia props become `msgspec.Struct(frozen=True, gc=False)` with a `kind` tag. | low | `make py-test` |
| FW-22 | `extensions/alchemy/plugin.py:267-342`; `extensions/sqlspec/plugin.py:447-525`; `extensions/alchemy/broker.py:396-459` | duplication | Three copies of: install state into dict-or-`update()` state, look up `litestar.di.Provide` with `importlib`, wrap providers with `SkipValidation`, register DI keys. Also duplicated: `_is_in_memory_uri` vs `_is_in_memory_dsn`, `_inject_engine_pool_defaults` vs `_inject_sqlspec_pool_defaults` vs `AlchemyPlugin.wrap_config`. | Extract `extensions/_plugin.py::StatePluginBase` (`install_into(app_config)`, `register_provider(key, fn)`) and a `PoolDefaults.apply(config)` helper. All three plugins subclass it. | low | `uv run pytest src/py/tests/unit/extensions/test_drop_in_names_and_generics.py` |
| FW-23 | `extensions/alchemy/plugin.py:23-122`; `extensions/sqlspec/plugin.py:79-196,243-318,517-525` | duplication, naming | `SQLAlchemyAsyncConfig`/`SQLAlchemySyncConfig`, `SyncDatabaseConfig`/`AsyncDatabaseConfig` and `ProxySyncConfig`/`ProxyAsyncConfig` are byte-identical except for `is_async` and the default DSN. The sqlspec base configs re-implement upstream sqlspec config classes as holders that return injected instances. `_inject_sqlspec_pool_defaults` injects SQLAlchemy-only engine kwargs (`pool_pre_ping`, `pool_recycle`) into sqlspec `pool_config`. | One generic `SQLAlchemyConfig[EngineT, SessionT, MakerT](is_async: bool)`. Use upstream sqlspec config types directly (Protocol only) and delete the clones. Stop injecting SQLAlchemy kwargs into sqlspec pools. | med (public names change) | `uv run pytest src/py/tests/unit/extensions -k "sqlspec or alchemy"` |
| FW-24 | `extensions/alchemy/__init__.py:78-118`; `extensions/sqlspec/plugin.py:11,21-22`; `typing.py:20-49` vs `_typing.py:327-378` | API surface bloat (re-export shims) | `alchemy` re-exports 11 `extensions.sqlalchemy` symbols. `sqlspec.plugin` re-exports `BrokerPlugin` and `ChannelMessage`. `vertebra.typing.__getattr__("Struct")` raises `MissingDependencyError`, while `_typing.__getattr__("Struct")` silently returns `StructStub`: the same name has two behaviors. `typing.__getattr__` does not cache into `globals()`. | Delete the cross-package re-exports. Keep one typing facade: `typing.py` re-exports aliases, and optional types resolve once through `_typing` and are cached. | low | `uv run pytest src/py/tests/unit/framework/test_typing.py` |
| FW-25 | `_typing.py:73-107,194-264,294-323`; `pyproject.toml:14` | dead code | msgspec is a hard dependency (`dependencies = ["msgspec>=0.18", …]`), yet `StructStub`, `MSGSPEC_INSTALLED` and the `Struct` lazy shim remain. `AttrsInstanceStub`, `NumpyArrayStub`, `UnsetType` and `ORJSON_INSTALLED` have 0 references. `BaseModelStub.model_dump` takes 10 ignored args. | Delete the unused stubs and flags. Keep `Empty`, `UNSET`, the `*_INSTALLED` flags that are used, and `MissingDependencyError`. Sort `__all__` (RUF022 at `_typing.py:23`). | low | `uv run pytest src/py/tests/unit/test_utils_misc.py src/py/tests/unit/test_utils_json.py` |
| FW-26 | `utils/loop_policy.py:1-156`; `utils/module_loader.py:114-143,60-72`; `utils/portal.py:141-220`; `utils/sync_tools.py:38-43`; `utils/config_tools.py:119-160`; `utils/uuids.py:60-105`; `utils/serializers/_schema.py` public API; `utils/serializers/_numpy.py` dec/predicate; `utils/singleton.py` | dead code | 0 references outside `utils/`: all of `loop_policy` (no tests either); `ensure_sqlspec/sqlalchemy/advanced_alchemy/uvloop/pyarrow/msgspec`; `module_to_os_path`; `PortalManager`/`get_global_portal`; `NoValue`; `discover_config_from_pyproject/read_pyproject_section/normalize_connection_config`; `uuid6/uuid7/nanoid`; `SchemaSerializer/schema_dump/serialize_collection/get_collection_serializer/get_serializer_metrics`; `numpy_array_dec_hook/predicate`. `SingletonMeta` and `get_env` are referenced only by tests. Three unused imports (vulture 90%). | Delete these, then trim `utils/serializers/__init__.__all__` to what is used. | low | `make lint && make py-test` |
| FW-27 | `utils/serializers/_json.py:44-85,100-106,147-162` | hot-path performance, duplication | On any msgspec `ValueError`/`TypeError` the encoder silently re-encodes with stdlib `json.dumps`, and the decoder re-parses with `json.loads` after msgspec rejects the input, which fails again for invalid JSON (two parses on the error path, and output can diverge). `JSONSerializer.encode` duplicates `encode_json`. Mutable globals: `_default_serializer`, `_uuid_utils_type` (PLW0603 ×3). There are no msgpack helpers, so 11 direct `msgspec.msgpack` calls exist: `live:126,131`, `sqlspec/storage.py:152,177,186`, `grpc/_decorator.py:267-313`, `alchemy/broker.py:327`. | Remove the stdlib fallbacks. Keep only the `int > 2**64` case through `enc_hook` if needed. Add a `MsgpackCodec` class with cached `Encoder`/`Decoder[T]` per type plus `encode_msgpack/decode_msgpack` to `vertebra.utils.serializers`. Make `JSONSerializer` the module singleton (class attribute), not a `global`. | low | `uv run pytest src/py/tests/unit/test_utils_json.py src/py/tests/unit/utils -q` |
| FW-28 | `framework/_dev_middleware.py:11`; `framework/_inertia.py:18`; `framework/_ssr/_prerender.py:10`; `framework/_ssr/_backends.py:21`; `extensions/litestar/offload.py:12` | standards violation | Five modules import from the private `vertebra.utils.serializers._json` instead of the public package. | Import from `vertebra.utils.serializers`. | low | `make lint` |
| FW-29 | `framework/openapi/__init__.py:35-91`; `framework/asyncapi/__init__.py:34-69` | stub/placeholder | Both builders are dict accumulators with no consumer in `src/`. Only the tests reference them. OpenAPI always emits a single `200` response, ignores path params and request bodies, and calls `msgspec.json.schema_components` directly (71). `ScalarDocsConfig` and `schema_path` are never served. | Add `OpenAPISpecBuilder.from_app(app: App)` that walks `App.routes`/`ParamSpec`, plus an `App.enable_docs()` route serving `schema_path`/Scalar. Otherwise delete both modules. | med | `uv run pytest src/py/tests/unit/framework -k openapi` |
| FW-30 | `framework/di/__init__.py:31-39,55-58,114-142` | hot-path performance | `Provide.parameter_names` calls `inspect.signature` on every access. `Container.resolve` reads `self._graph.providers`, a property that returns `dict(self._providers)` (a full copy), three times per resolved key and again for every dependency. The compiled `topological_order` is never used by `resolve`. | Precompute `parameter_names` once in `DependencyGraph.register` (store as `tuple[str, ...]` on `Provide`). Make `providers` a `MappingProxyType`, or have `resolve` read `_providers` directly. Iterate `topological_order` to build a per-key resolution plan. | low | `uv run pytest src/py/tests/unit/framework/test_framework_subdomains.py -k di` plus a new micro-benchmark (see Hot paths) |
| FW-31 | `framework/security/__init__.py:81-106,109-160` | stub/placeholder, duplication | `CsrfConfig`, `RateLimitConfig` and `SecurityConfig` (JWT issuer/audience, IAP) are not enforced by any middleware. Only `LitestarEcosystemBridge` stores them. The four `requires_*` factories repeat the same closure pattern. | Implement `CsrfMiddleware`/`RateLimiter` against the native App, or delete the configs. Collapse the guards into `Guard.requires(roles=…, scopes=…, capabilities=…, tenants=…)`. | low | `uv run pytest src/py/tests/unit/framework -k security` |
| FW-32 | `framework/mcp/__init__.py:104-173`; `framework/a2a/__init__.py:105-167` | duplication, organization | Long `if method == …` chains, with the `{"jsonrpc": "2.0", "id": …}` envelope hand-built 8 times. MCP registers prompts but has no `prompts/list`/`prompts/get`/`resources/read`. `tools/call` returns `str(out)`. `A2AServer._tasks` grows without bound. (Protocol parity is covered by the separate researcher.) | Add a shared `framework/_jsonrpc.py::JsonRpcDispatcher` with a method table and msgspec `JsonRpcRequest/Response` Structs. MCP and A2A register handlers on it. Bound or TTL the task map. | low | `uv run pytest src/py/tests/unit/framework -k "mcp or a2a"` |
| FW-33 | `framework/procedures/__init__.py:71-128`; `framework/mcp/__init__.py:176-232` | duplication | `query`/`mutation`/`subscription` are three identical decorators that differ only in `kind`. The MCP decorators follow the same pattern. | `ProcedureDef.decorator(kind)` classmethod (or `ProcedureRouter.query/mutation/subscription` methods that also register). | low | `uv run pytest src/py/tests/unit/framework -k procedure` |
| FW-34 | `framework/htmx/__init__.py:45-57,77-106` | hot-path performance (minor) | `from_headers` builds a lowercased copy of the whole header dict on every request. The `push_url/replace_url` tri-state branch is written twice. | Accept raw ASGI headers (`list[tuple[bytes, bytes]]`) and scan once. Add a private `_tristate()` helper. | low | `uv run pytest src/py/tests/unit/framework -k htmx` |
| FW-35 | `framework/live/__init__.py:100-176,237-259,277-285,351-355` | hot-path performance, allocation | `__setattr__` recomputes the FNV field id (0.93 µs of the 2.5 µs write) and calls the `state_id` property on every delta. `_pending_deltas` grows without bound (20,000 frames after 20k writes with no drain). `StateDeltaFrame` is `array_like` but `encode/decode` build lists by hand per opcode. `__vertebra_action_id__` is written but never read. It uses `msgspec.msgpack` directly. | Cache `name→field_id` in `__init_subclass__` (it already builds `field_map`; add the reverse map). Store `state_id` as a class attribute. Model frames as a tagged union of `array_like` Structs (one per opcode) so `msgspec` does the encoding. Bound the pending queue or coalesce per field (LWW makes older deltas for the same field redundant). Drop `__vertebra_action_id__`. | low | `uv run pytest src/py/tests/unit/framework/test_live_state.py` plus a cross-language frame test against `vertebra-core/src/protocol/state.rs` |
| FW-36 | `framework/_dev_middleware.py:17,26-31,41-45,97-115,151-183` | standards violation, organization | Module-global `_ws_clients` set. Monkey-patches `type_bridge.invalidate` (44-45). Three silent `except Exception: pass` blocks (104, 161, 182). The content-type `if/elif` ladder belongs in a lookup table. | `DevMiddleware` owns `_clients`. `TypeBridge` accepts an `on_invalidate` callback. Use a `_ASSET_ROUTES: dict[str, tuple[str, str]]` table. Log instead of pass. Merge `_type_bridge.py` and `_dev_middleware.py` into `framework/dev.py`. | low | `uv run pytest src/py/tests/unit/framework/test_dev_middleware.py src/py/tests/unit/framework/test_hmr_routing.py` |
| FW-37 | `framework/_ssr/_backends.py:88-106,146-170,43-51` | standards violation, hot-path | The sidecar is spawned with `shell=True` from `f"{runtime} {self._entry}"` (S602) plus `preexec_fn` (PLW1509). `render()` makes a blocking `urllib.request.urlopen(…, timeout=5)` call on the request path. The native client is imported with `importlib` instead of a module-level optional import. | `subprocess.Popen([runtime, entry], start_new_session=True)`. Add an async `render_async` using the existing `HttpClient`, or run on `SyncPool`. Use a module-level `import_optional("vertebra._bundler")`. | low | `uv run pytest src/py/tests/unit/framework/test_ssr.py` |
| FW-38 | `framework/_inertia.py:241-336,419-433,44-58` | duplication | The `if is_partial and include_fields and key not in include_fields: continue` check is repeated 6 times. Comma-split header parsing is written 4 times (251-253, 431-432). `md5` is used for the manifest version (S324). `InertiaConfig.sidecar_port` duplicates the port in `ssr_url`. | Add a private `_csv_header(request, name) -> frozenset[str]`. Compute `wanted(key)` once per prop. Use a prop-kind dispatch table. Switch to `hashlib.blake2b(digest_size=4)`. Remove `sidecar_port`. | low | `uv run pytest src/py/tests/unit/framework/test_inertia_middleware.py` |
| FW-39 | `extensions/litestar/store.py:19-121`; `extensions/litestar/channels.py:22-132` | naming, stub/placeholder, duplication | `MemoryStore` shadows Litestar's own `MemoryStore`, and its docstring claims "RESP3 invalidation", which is not implemented. With a driver, the local dict caches forever, a miss goes to the (fake, FW-01) driver, and `delete_all()` leaves driver keys behind. `ChannelsBackend` publishes to the driver but never subscribes, so cross-worker fan-out does not happen. `from_valkey/from_redis` is duplicated in both classes. | Rename to `ResponseCacheStore` and `ChannelBus` (see FW-13). Extract `KeyValueBackend.from_url()` and use it in both. Implement driver subscribe in `stream_events`, or remove the driver parameter. | med | `uv run pytest src/py/tests/unit/extensions/test_litestar_plugin.py` |
| FW-40 | `extensions/litestar/fastpath.py:142-190,108-132`; `plugin.py:122-152`; `channels.py:142-149,160-173`; `store.py:138-140,150-164` | dead code, standards violation | `_FallbackScopeState`, `_FallbackASGIResponse` and the `fallback_get` `SimpleNamespace` exist for "when litestar is not installed", inside the Litestar extension. Type checks fall back to `type(x).__name__ == "ChannelsPlugin"` string comparison. Each module-level `_load_*` helper repeats the `importlib` plus `ImportError` pattern. | The extension requires litestar: import it at the top of the module (guarded once in `extensions/litestar/__init__.py` through `import_optional`). Delete the fallbacks and the name-string checks. | low | `uv run pytest src/py/tests/unit/extensions/test_litestar_plugin.py` |
| FW-41 | `extensions/litestar/router.py:56-71,186-198`; `framework/_router.py:247-269` | hot-path performance, duplication | `LitestarRouterTable.match` scans every route for the method and runs a regex on each (O(routes) per request), even though the Rust matchit table exists. The param converters duplicate `framework._router._coerce_param` with a different type vocabulary. | Have `match()` delegate to the native matchit table (`handler_id`) and keep the regex only as the test oracle. Put shared converters in `framework/_router.py::PARAM_CONVERTERS`, used by both. | low | `uv run pytest src/py/tests/unit/extensions/test_litestar_plugin.py src/py/tests/unit/server/test_routing.py` |
| FW-42 | `storage/_store.py:42-356,617-958,1230-1258` | layering/organization, hot-path | 1,365 lines mixing provider configs, five signing schemes and store I/O. The native→`ObjectMeta`/`PutResult` conversion is repeated 8 times (626-631, 656-661, 674-679, 729-736, 804-814, 836-842, 898-905, 947-957). `_resolve_local(key)` runs twice per local call (620-621 then 635). `hasattr(self._native, "x_async")` runs on every async call. Async methods fall back to **sync** I/O on the loop (843, 863, 872, 890, 906, 915, 928, 940, 958). `put` hashes the whole payload with sha256 when the native etag is missing (625, 836). `list()` re-filters the prefix after the native call already filtered it (813, 956). The native init swallows every exception (403). | Split into `storage/config.py` (provider configs), `storage/signing.py` (`PresignedUrlSigner` with HMAC/S3/GCS/OCI/Azure methods) and `storage/store.py`. Add `ObjectMeta.from_native()`/`PutResult.from_native()`. Resolve the async capabilities once in `__init__`. Run the non-native async fallback on `SyncPool`. Drop the sha256 fallback (use a size/mtime etag). Log native init failures. | med | `uv run pytest src/py/tests/unit/storage -q` |
| FW-43 | `storage/_store.py:281-355` | bug, organization | `SigningConfig.sign_oci_headers(..., timestamp: int = 1_700_000_000)` defaults to a fixed 2023 epoch, so callers that omit it sign stale `date` headers, which OCI rejects for clock skew. Both `sign_oci_headers` and `sign_azure_sas` are `@staticmethod`s that ignore every `SigningConfig` field. | Default to `None` and use `int(time.time())`. Move both into `PresignedUrlSigner` (FW-42), reading credentials from config. | low | `uv run pytest src/py/tests/unit/storage -k "oci or azure"` |
| FW-44 | `extensions/alchemy/storage.py:118-132,167-200`; `extensions/sqlspec/storage.py:273-284`; `framework/stores/__init__.py:29-53` | duplication, hot-path | Three prefix-qualifying adapters (`_qualify`/`_unqualify`). `delete_object` does `exists()` then `delete()`, two round-trips per object, although `ObjectStore.delete` already ignores missing keys. `_resolve_sync_data` and `_resolve_async_data` duplicate each other. | Add `vertebra.storage.PrefixedObjectStore(store, prefix)` and build `ObjectStoreBackend`/`SQLSpecStore` on it. Call `delete` directly. Add a shared `BufferSource.read()` helper. | low | `uv run pytest src/py/tests/unit/extensions/test_data_storage_integrations.py` |
| FW-45 | `storage/pipeline.py:76-134`; `extensions/sqlspec/storage.py:39-44` | hot-path performance, duplication | `UploadTracker` loops use `list.pop(0)` (O(n²)) four times. There are four sync/async twin methods. `StreamingStorageResponse.aiter_chunks` downloads the whole object before "streaming". | Iterate and then `clear()` (or use a `deque`). Share the body through a private generator over the work list. Have `aiter_chunks` use `get_range_async` per chunk. | low | `uv run pytest src/py/tests/unit/storage -k tracker` |
| FW-46 | `extensions/grpc/_decorator.py:220-330` | hot-path performance, duplication | Every frame calls `msgspec.msgpack.decode(..., type=spec.input_type)`/`encode`, building a new decoder each time (267, 276, 313). The whole request body is buffered before dispatch (244, 315-330), so client-streaming and bidi calls are not streamed. The four kind branches duplicate the encode-and-send code. `except Exception` puts `str(exc)` into `grpc-message` (252-253). | Cache `Decoder(input_type)`/`Encoder()` on `GrpcMethodSpec` (or through `MsgpackCodec`, FW-27). Decode LPM frames incrementally from `receive()` with an async generator. Add a single `_send_message(item, more)` helper. Send a generic INTERNAL message and log the details. | low | `uv run pytest src/py/tests/unit/extensions/test_grpc_service.py` |
| FW-47 | `src/py/tests/unit/extensions/test_drop_in_names_and_generics.py:88-94`; `src/py/tests/unit/framework/test_framework_subdomains.py:43-235`; `src/py/tests/unit/server/test_stage1_hotpath_and_314t.py`; `src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py` | test weakness/duplication | Tautologies: `assert isinstance(ChannelsBackend(history=4), ChannelsBackend)`, `assert LitestarRouteCompiler is not None` and similar (7 asserts). 16 subdomains are crammed into 5 multi-domain tests in a feature-wave file, with stage- and PR-named files instead of canonical module test files. Several tests assert stub behavior (Wasm host fuel, email outbox, fake Valkey). | Delete the tautologies. Split into `tests/unit/framework/test_{di,security,queues,mcp,a2a,…}.py` and `tests/unit/extensions/{litestar,sqlalchemy,sqlspec,…}/test_*.py`. Rewrite stub tests once FW-01 through FW-11 land. | low | `make py-test` |
| FW-48 | scope-wide (ruff `--select ALL`) | standards violation | 749 hits. Actionable subset: BLE001 ×16 (e.g. `storage/_store.py:403,1225`, `extensions/litestar/offload.py:473`, `utils/module_loader.py:56,102,109`), S110 ×4 (`_dev_middleware.py:104,161,182`, `_ssr/_backends.py:121`), PLW0603 ×9 (`serializers/_json.py:102,154`, `utils/uuids.py:110,125`), SLF001 ×9, PYI036 ×6 (`utils/sync_tools.py:220-222,410-412`), RUF022 ×2, S324, S602, PLC0415 at `utils/loop_policy.py:75`. The 9 PLC0415 hits in `__init__.py`/`_typing.py` are PEP 562 lazy loaders and acceptable. | Fix in place in the touched slices. Consider promoting `BLE001, S110, PLW0603, PYI036, RUF022` into the project ruff selection. | low | `make lint` |
| FW-49 | `__init__.py:65-80`; `typing.py:20-49`; `framework/__init__.py:5-22` | standards violation (lazy exports) | The root `__getattr__` caches correctly but defines no `__dir__`, so `dir(vertebra)` omits lazy names. `typing.__getattr__` re-runs `import_optional` on every access (no `globals()` cache). `framework/__init__` is eager (FW-14). | Add `__dir__` returning `__all__` in every lazy module. Cache resolved names in `typing.py`. Use one shared pattern: `utils/module_loader.lazy_exports(mapping)` returns `(__getattr__, __dir__)`. | low | `uv run pytest src/py/tests/unit/framework/test_typing.py src/py/tests/unit/test_utils_init.py` |
| FW-50 | `extensions/litestar/ecosystem.py:23-93` | layering/organization, stub/placeholder | `LitestarEcosystemBridge` bundles unrelated services: MCP, A2A, the stub `QueueService`/`EmailService` created by default, `AutowireLoader`, and two configs it only stores (`security_config`, `ssr_config`). It is the reason `extensions.litestar` imports half of `framework` (FW-14). | Delete the bridge. `LitestarPlugin` takes optional `mcp=`, `a2a=`, `queues=` and registers them into state/DI through `StatePluginBase` (FW-22). Autowire goes through its own plugin hook. | med | `uv run pytest src/py/tests/unit/extensions/test_litestar_plugin.py src/py/tests/unit/framework/test_framework_subdomains.py` |
| FW-51 | `extensions/apex/gateway.py:87-103,187-198`; `extensions/apex/session.py:59-104` | duplication, naming | `ConfirmationGate` never consumes approved tokens, so one approval authorizes unlimited replays. `ApexSessionContext` is copied field by field (187-198). `OracleSessionManager` is a namespace class containing only static and class methods. | `ConfirmationGate.consume(token) -> bool` (pop). Use `msgspec.structs.replace(context, app_id=…)`. Move the `OracleSessionManager` functions onto `ApexSessionContext` (`.attach_plsql()`, `.detach_plsql()`, `.scoped(pool)`). | low | `uv run pytest src/py/tests/unit/extensions/test_oracle_apex_and_gateway.py` |
| FW-52 | `db/_registry.py:103`; `db/oracle/driver.py:99-183`; `db/dialer/cloudsql.py:79-112`; `db/dialer/alloydb.py:53-89` | dead code | Vulture flags 45 methods in `vertebra.db` with no `src/` caller, among them `DatabaseRegistry.get_driver`, `OraclePoolRegistry.get_pool`, `prepare_bind_params`, `CloudSqlDialer.build_tls_context` (which raises `NotImplementedError` when client PEMs are provided, cloudsql.py:112) and `AlloyDbDialer.build_handshake_frame`. | Wire these into real drivers (FW-01) or delete them. Remove the `NotImplementedError` branch by loading certificates through a memfd temp file, as its own message suggests. | low | `uv run pytest src/py/tests/unit/db -q` |
| FW-53 | `extensions/apex/openapi.py:67-99` | correctness | `load_ords_openapi` sets `parameters=dict(op_data.get("requestBody", {}))` and uses it as the MCP `input_schema`. An OpenAPI `requestBody` object is not a JSON Schema, and path/query parameters are dropped. | Build `input_schema` from `parameters[]` plus `requestBody.content["application/json"].schema`. | low | `uv run pytest src/py/tests/unit/extensions/test_oracle_apex_and_gateway.py -k ords` |

## Bundler, TypeScript SDK, build, CLI, tests, tooling (prefixes FE, TST, TOOL)

#### Rust bundler

| ID | Location | Category | Evidence | Proposed change | Risk | Verify |
|---|---|---|---|---|---|---|
| FE-01 | `src/rs/vertebra-bundler/src/config.rs:24-50`, `:56` | stub/placeholder | `sourcemap`, `target`, `external`, `define`, `format`, `tree_shaking`, `splitting` are accepted and exposed with `#[pyo3(get,set)]`, but `rg` shows they are read only in the defaults test. `Bundler::build_internal` (`bundler.rs:211-247`) transforms just the entry file. `_orchestrator.py:166` passes `sourcemap=True`, and the Rust side silently ignores it. | Either wire them into OXC (`TransformOptions` target, `CodegenOptions` source map) or delete them. For a no-features-removed debloat: keep `minify` + `target` (map `target` into `TransformOptions::from_target`) + `sourcemap` (write a `.map` via `CodegenOptions { source_map_path }`), and delete `external/define/format/tree_shaking/splitting` until a real bundling graph exists. Rename `Bundler.build` → `Bundler.transform_entry` so the name matches what it does. | med: Python callers pass `sourcemap` | `cargo test -p vertebra-bundler`; `uv run pytest src/py/tests/unit/framework/test_orchestrator.py` |
| FE-02 | `src/rs/vertebra-bundler/src/bundler.rs:25,68,93` | dead code | `hmr_rx: Option<broadcast::Receiver<HmrEvent>>` is written in `start_dev`/`stop_dev` and never read, so watcher events pile up in a channel nobody drains (capacity 100, then `Lagged`). | Delete `hmr_rx`. Expose `poll_events()` on `Bundler`, or have `start_dev` return the `PySourceWatcher` it owns. | low | `cargo test -p vertebra-bundler` |
| FE-03 | `src/rs/vertebra-bundler/src/dts.rs:1-126`, `lib.rs:20,34-36` | stub/placeholder + dead code | `IsolatedDeclarationsGenerator::emit_dts` calls `Parser::parse` only to check diagnostics, then scans `source_text.lines()` for `export interface`/`export function` prefixes, which breaks on multi-line signatures. The module doc still says "OXC AST-backed". `minify_css` and `encode_ssr_worker_frame` have zero Python callers (`rg emit_dts\|minify_css\|encode_ssr src/py` → none). `encode_ssr_worker_frame` builds JSON with `{component:?}` (Rust `Debug` escaping, e.g. `\u{1f600}`), which is not valid JSON for non-ASCII input. | Replace `emit_dts` with the `oxc_isolated_declarations` crate (same 0.155 line) in `src/rs/vertebra-bundler/src/declarations.rs`. Delete `encode_ssr_worker_frame`: the sidecar SSR framing lives in Python. Move `minify_css` into the transform module, or switch to `lightningcss` if CSS output matters. | low (no callers) | `cargo test -p vertebra-bundler dts` |
| FE-04 | `src/rs/vertebra-bundler/src/ssr.rs:63,152-157,246-253` | hot-path performance / memory | `cache: DashMap<u64, CachedRender>` gets one insert per unique `(component, page_json)` with no eviction. The TTL is checked only on read, and the map is cleared only by `invalidate()`. Inertia `page_json` includes per-request props/URL, so in production the cache grows without bound and hits are rare. | Bound it: replace with a fixed-capacity LRU (`quick_cache` or `moka::sync::Cache` with `max_capacity` + `time_to_live`), or make caching opt-in per component. Remove the `created_at` scan logic. | med | `cargo test -p vertebra-bundler ssr`; add `benches/ssr_render.rs` |
| FE-05 | `src/rs/vertebra-bundler/src/ssr.rs:159` | lock/GIL contention | `self.available.pop().ok_or(BundlerError::PoolExhausted)?` fails immediately when every slot is busy. The GIL is released (`py.detach`) and 3.14t is a target, so concurrent requests above `pool_size` fail. `NativeSsrClient.render` (`framework/_ssr/_backends.py:53-59`) does not retry. | Block on a slot: pair `ArrayQueue` with a `parking_lot::Condvar`/`Semaphore`, using a bounded wait (e.g. 50 ms) before `PoolExhausted`. | med | new `cargo test -p vertebra-bundler ssr_concurrent_render_waits_for_slot` |
| FE-06 | `src/rs/vertebra-bundler/src/ssr.rs:195-208`, `:171-175` | allocation/copy | Every render looks up `globals.JSON.parse` and `__ssr_render` by name. Every rebuild clones the whole bundle `String` out of the `RwLock`. | Cache `Persistent<Function>` for `__ssr_render` and `JSON.parse` in `SlotEngine`. Store the bundle as `Arc<str>` and swap it atomically (`arc_swap::ArcSwap<str>`). | low | `cargo test -p vertebra-bundler ssr` + bench |
| FE-07 | `src/rs/vertebra-bundler/src/bundler.rs:163-186`; `framework/_ssr/_backends.py:55-59` | allocation/copy + API surface bloat | `Bundler.ssr_render` turns `SsrResult` into `HashMap<String, Py<PyAny>>`. Python then rebuilds a msgspec `SsrResult` from it, so the body is copied 3 times. Separately, `SsrPool` and `SsrResult` are registered as their own pyclasses (`lib.rs:32-33`, `ssr.rs:294-318`) and Python never uses them. | Expose one SSR surface: register `SsrPool` as `vertebra._bundler.SsrPool` and drop `Bundler.init_ssr/ssr_render/ssr_invalidate/ssr_shutdown`. Have `NativeSsrClient` own an `SsrPool` and read `result.head`/`result.body` from the pyclass directly. Remove the unused `_paths` argument (`bundler.rs:192`). | low | `uv run pytest src/py/tests/unit/framework/test_ssr.py`; `cargo test -p vertebra-bundler` |
| FE-08 | `src/rs/vertebra-bundler/src/bundler.rs:47-50,65,75,89,103-104,154,175`; `hmr.rs:157,168-181,189,203,212`; `ssr.rs:299,305` | duplication | About 15 hand-written `.map_err(\|e\| PyRuntimeError::new_err(e.to_string()))` calls, while `errors.rs:28` already implements `From<BundlerError> for PyErr`. `Bundler::new` returns `PyResult` for an infallible constructor, and `Default` then `expect`s it (`:35,250-254`). | Return `crate::errors::Result<T>` from internals and use `?`. Make `Bundler::new` infallible and `#[derive(Default)]`. Add `BundlerError::Poisoned` and use it in place of the string-built poison errors. | low | `cargo clippy -p vertebra-bundler --all-targets -- -D warnings` |
| FE-09 | `src/rs/vertebra-bundler/src/hmr.rs:140-142`; `Cargo.toml` `tokio`, `serde`; `config.rs:10,112` | over-abstraction / deps | `PySourceWatcher` puts `Mutex` around both the watcher and the receiver. The only channel use is a sync `tokio::sync::broadcast` (single consumer), which pulls in workspace `tokio` `full`. `BundleConfig`/`DevConfig` derive `Serialize/Deserialize` and are never serialized. `HmrEvent::connected` and `to_json` are used only in tests (`hmr.rs:49,55`). | Swap `broadcast` for `crossbeam_channel::bounded` (already a crossbeam dependency) and drop `tokio` from the crate. Remove the serde derives from the configs and the dead `HmrEvent` helpers. Fold `SourceWatcher` into `PySourceWatcher` behind a single `Mutex<State>`. | low | `cargo build -p vertebra-bundler`; `cargo test -p vertebra-bundler hmr` |
| FE-10 | `src/rs/vertebra-bundler/Cargo.toml` (`rquickjs` features), `ssr.rs:46-49` | build cost / unsafe | `rquickjs` enables `bindgen`, which needs libclang and regenerates bindings at build time, plus `classes` and `properties`, which `rg rquickjs::class` shows are unused. `parallel` is enabled, and that already makes `Runtime`/`Context` `Send`, so `unsafe impl Send for SlotEngine` looks redundant. | Use `features = ["parallel"]` only and delete the `unsafe impl Send`. Keep it only if the compiler rejects the removal. | med (target support for prebuilt bindings) | `cargo build -p vertebra-bundler` on Linux + macOS CI |
| FE-11 | `src/rs/vertebra-bundler/src/bundler.rs:108-117` | lock/GIL contention | `check_file` parses while holding the GIL; `transform_file` releases it with `py.detach`. | Wrap it in `py.detach`, or delete `check_file`/`transform_file`: there are no Python callers (`rg check_file\|transform_file src/py` → none). | low | `cargo test -p vertebra-bundler` |
| FE-12 | `src/py/vertebra/_bundler.py:1-15`; `src/rs/vertebra-bundler/Cargo.toml` `[lib] name = "_bundler"` | standards violation (shim) | A re-export shim exists because maturin installs the extension as the top-level module `_bundler`. Every consumer imports `vertebra._bundler` dynamically. | Add `src/rs/vertebra-bundler/pyproject.toml` with `[tool.maturin] module-name = "vertebra._bundler"`, or pass `-m ... --module-name` in the Makefile. Delete the shim. Do the same for `_storage` (the `pyproject.toml:112-113` mypy overrides list both `_storage` and `vertebra._storage`). | med (packaging) | `make dev && uv run python -c "import vertebra._bundler"` |

#### Python build

| ID | Location | Category | Evidence | Proposed change | Risk | Verify |
|---|---|---|---|---|---|---|
| FE-13 | `src/py/vertebra/build/_orchestrator.py:151-160`; `cli/dev.py:90-110` | stub/placeholder (functional bug) | `_run_native(dev=True)` creates a local `bundler`, calls `start_dev`, and returns. The `Bundler` goes out of scope, so its `SourceWatcher` and debouncer are dropped right away. `cli/dev.py` runs this on a daemon thread that then exits. Native frontend dev mode watches nothing, and no code polls HMR events. | Keep the watcher on the orchestrator (`self._native_dev: Bundler`). Run a poll loop that triggers rebuild + `HmrNotifier.notify()`, and add `BuildOrchestrator.stop()`. Use the same `CodegenWatcher`-style thread. | med | new test `test_native_dev_keeps_watcher_alive` in `tests/unit/build/test_orchestrator.py`; `uv run pytest src/py/tests/unit/build -k native_dev` |
| FE-14 | `src/py/vertebra/build/codegen/_watch.py:135-153` vs `src/rs/vertebra-bundler/src/hmr.rs:185` | functional bug (dead path) | `_native_watch_loop` calls `watcher.poll_events(timeout_ms)`, but the Rust signature is `poll_events(&self)` with no arguments. The `TypeError` is caught by `except Exception: break`, so the native path quits on its first iteration. Polling fallback never takes over because `start()` already chose the native target. Nothing tests `CodegenWatcher` (`test_codegen_watch.py` covers only `HmrNotifier`). | Add `timeout_ms: Option<u64>` to `PySourceWatcher::poll_events`, blocking via `recv_timeout` inside `py.detach`. Remove the busy `continue` loop and log the exception rather than silently breaking. | low | new `tests/unit/build/codegen/test_watch.py::test_native_watcher_invokes_callback` |
| FE-15 | `src/py/vertebra/build/codegen/zod.py:126`, `valibot.py:127`, `_typescript.py:40`, `protobuf.py:91`, `openapi.py:82`, `procedures.py:645` | duplication | `_collect_named_types` is copied byte-for-byte into 6 classes. `_render_interface`/`_type_ref` appear twice (`_typescript.py:47,87` vs `procedures.py:652,661`). Each emitter has its own `_primitive_to_*` switch (`zod.py:181`, `valibot.py:197`, `protobuf.py:133`, `openapi.py:186`). | Create `build/codegen/_emitter.py::SchemaEmitter` base with `declared`, `collect_named_types()`, and abstract `primitive(kind)`/`render_object()`. Have `ProcedureClientGenerator` reuse `TypeScriptGenerator` for interfaces. | low | `uv run pytest src/py/tests/unit/build/codegen` |
| FE-16 | `src/py/vertebra/build/codegen/procedures.py:1-668` | layering/organization | One module holds parameter markers + `Parameter()`/`Body()` factories (63-176), JSON-schema→IR conversion (192-414), the `ProcedureRegistry` singleton (438-515), the `@procedure` decorator (517-606), and a TS client emitter (608-668). `T = TypeVar("T")` sits at `:58`. | Split into `codegen/markers.py` (markers, `FromPath`… aliases), `codegen/_json_schema.py` (`json_schema_to_type_node`, `resolve_parameter_node`), and `codegen/procedures.py` (registry, decorator, client emitter). | low | `uv run pytest src/py/tests/unit/build/codegen -k procedure` |
| FE-17 | `src/py/vertebra/build/codegen/zod.py:37`, `valibot.py:37` | API surface bloat | `generate(routes: Iterable[RouteMetadata] \| dict[str, Any] \| TypeNode)` dispatches on type at runtime. `generate_from_node`, `generate_from_schema`, and `emit_schema` cover the same three paths again. | Make `generate(routes)` the route API and `emit_schema(name, node_or_schema)` the single schema API. Delete `generate_from_node`/`generate_from_schema`. | low | `uv run pytest src/py/tests/unit/build/codegen -k "zod or valibot"` |
| FE-18 | `src/py/vertebra/build/_import_map.py:72`, `_orchestrator.py:103,112`, `packaging/standalone.py:101`, `codegen/_manifest.py:33` | standards violation | Five call sites do `msgspec.json.format(encode_json_bytes(x), indent=2).decode()` while `vertebra.utils.serializers._json.format_json(value, indent=2)` exists (`utils/serializers/_json.py:142`). | Swap all five for `format_json(...)` and drop the `msgspec` imports. | low | `make lint`; `uv run pytest src/py/tests/unit/build` |
| FE-19 | `src/py/vertebra/build/_orchestrator.py:211` | stub/placeholder | Codegen writes `routes.ts` with the constant `export const routes = {};`. | Emit the real route tree from `ManifestGenerator` data (`operation_id` → `{method, path}`), or stop writing the file. | low | `uv run pytest src/py/tests/unit/build/test_orchestrator.py -k codegen_only` |
| FE-20 | `src/py/vertebra/build/_fallback.py:52-61,101-113,138-148` | duplication + standards | Vite binary resolution is written twice (`_resolve_command` and `run_vite_build`). The dev server uses `shell=True` with an f-string command (breaks on paths containing spaces) and `preexec_fn=os.setsid`, which is unsafe with threads; `cli/dev.py` runs it on a thread. | Add one `_resolve_vite_argv(*args) -> list[str]` helper, use `subprocess.Popen(argv, start_new_session=True)`, and merge both into a `ViteProcess` class (`start_dev()`, `build()`). | low | `uv run pytest src/py/tests/unit/build/test_vite_fallback.py` |
| FE-21 | `src/py/vertebra/build/__init__.py:3-82`; `codegen/__init__.py:1-123` | API surface bloat | `vertebra.build` eagerly imports and re-exports 38 names, including constants (`PBS_BASE_URL`, `ONEFILE_TRAILER_SIZE`) and internals (`HmrEventRouter`, `format_typescript`), so `import vertebra.build` loads the whole codegen + packaging tree. `cli/dev.py:12` imports it on every `vtb` startup via `cli/__init__.py:7`. | Shrink `vertebra.build.__all__` to `BuildConfig`, `BuildOrchestrator`, `JsConfig`. Import codegen and packaging from their subpackages. Use PEP 562 lazy `__getattr__`, as `vertebra/__init__` already does. | med (import paths in tests) | `uv run python -X importtime -c "import vertebra.cli" 2>&1 \| tail -5`; `make py-test` |
| FE-22 | `src/py/vertebra/build/codegen/_ir.py:27` | standards violation | `class TypeNode(msgspec.Struct, frozen=True, tag_field="_type", tag=True)` and its subclasses omit `gc=False`. | Add `gc=False` to the base (subclasses inherit it). | low | `uv run pytest src/py/tests/unit/build/codegen` |
| FE-23 | `src/py/vertebra/build/packaging/onefile.py:143-176` vs `:57-133` | duplication | The shell launcher embeds a second Python implementation of trailer parsing (`"<8sQ32s"`, `VTB1FILE`, ZSTD/ZLIB) that duplicates `OnefileBuilder.inspect_trailer`/`_decompress_payload`, so the two can drift. | Write the extractor once as `build/packaging/_onefile_extract.py` and inline its source into the launcher (`inspect.getsource`) at build time, with `ONEFILE_TRAILER_STRUCT.format` interpolated. | low | `uv run pytest src/py/tests/unit/build -k onefile` |
| FE-24 | `src/py/vertebra/build/packaging/standalone.py:42,98`; `cli/build.py:79-83` | stub/placeholder | `--pgo` / `pgo_enabled` only writes `"pgo_enabled": true` into `manifest.json`. No PGO happens, even though `tools/pgo/*.py` exists. | Either run `tools/pgo` flag generation, or rename the flag to `--record-pgo-intent` and say so in the help. Preferred: move `tools/pgo/{rust,python}_pgo.py` into `vertebra.build.packaging.pgo` and call them when `--pgo` is set. | low | `uv run pytest src/py/tests/unit/build -k pgo` |

#### Python CLI

| ID | Location | Category | Evidence | Proposed change | Risk | Verify |
|---|---|---|---|---|---|---|
| FE-25 | `src/py/vertebra/cli/init.py:11-15` | stub/placeholder | `vtb init NAME --template` prints "Initializing project…" and creates nothing. Tests only check that it shows up in help (`tests/unit/cli/test_cli.py:26`). | Implement scaffolding (pyproject with `[tool.vertebra]`, `app.py`, optional `web/` template) or remove the command until it exists. | low | new `tests/unit/cli/test_init.py` |
| FE-26 | `src/py/vertebra/cli/build.py:73`, `dev.py:48`, `banner/renderer.py:35`, `errors.py:41` | dead code | `_ = profile`, `_ = verbose`, `_ = log_level`, `_ = file`: the options are accepted and thrown away. | Wire `--profile` to `maturin --release/--profile` (or delete it). Wire `--verbose` to `configure_logging(traceback=True)`. Drop the unused `log_level` param from `render_banner` and the unused `file` from `CLIError.show`. | low | `uv run pytest src/py/tests/unit/cli` |
| FE-27 | `src/py/vertebra/cli/run.py:93-126` vs `cli/dev.py:53-64` | duplication + drift | Both build the server env dict by hand. `dev` sets `VERTEBRA_PYTHON_APP` but not `LITESTAR_APP`/`PYTHONPATH`. `run` sets `LITESTAR_APP`/`PYTHONPATH`/`VERTEBRA_THREADS`/`VERTEBRA_TOPOLOGY`. | Add `vertebra/cli/_launch.py::ServerLaunchEnv(msgspec.Struct, frozen=True, gc=False)` with `to_environ(base)`. `run` and `dev` construct it with mode-specific defaults. | low | `uv run pytest src/py/tests/unit/cli/test_cli.py -k env` |

#### TypeScript SDK

| ID | Location | Category | Evidence | Proposed change | Risk | Verify |
|---|---|---|---|---|---|---|
| FE-28 | `src/ts/vertebra-client/tsup.config.ts:10-22`; `package.json:41-48` | build/exports bug | The tsup entry list has `src/react/index.ts`, but the file is `src/react/index.tsx`. The existing `dist/` in the working checkout has no `react/` directory, which confirms the React export is never built. `package.json` exports `./wasm` → `dist/wasm.js` and `./arrow` → `dist/arrow.js`, but those are not tsup entries, so with `splitting: true` they turn into hashed chunks and the export paths 404. `src/hmr/index.ts` is built but has no `./hmr` export. | Generate tsup `entry` from `package.json` `exports` (or the reverse) in one object: `{ index, vite, react: "src/react/index.tsx", …, wasm, arrow, sse, websocket, hmr }`, and add `./hmr`. Add a `scripts.check-exports` step (`publint` or `attw`) to `ts-test`. | low | `bun run --cwd src/ts/vertebra-client build && ls dist/react dist/wasm.js dist/arrow.js` |
| FE-29 | `src/ts/vertebra-client/src/client/ws.ts:16-138` vs `src/websocket.ts:30-284` | duplication | `RealtimeChannel` (lazy connect, subscribe map, backoff reconnect) is a subset of `WebSocketClient` (the same plus heartbeat, idempotency, `publish`, `lastEventId`). Both are exported from `index.ts:19,46-50`. `HttpClient` uses `RealtimeChannel` (`client/client.ts:4,121`). | Delete `client/ws.ts` and have `HttpClient.subscribe` use `WebSocketClient`. Remove `RealtimeChannel` from exports (no compat alias). | low | `bun run --cwd src/ts/vertebra-client test` |
| FE-30 | `src/ts/vertebra-client/src/sse.ts:35-36,201-206`, `websocket.ts:45-46,277`, `hmr/index.ts:96-97,266-276`, `client/ws.ts:19-20,128-137` | duplication | Four classes each carry their own `reconnectAttempts`/`reconnectTimer`/`scheduleReconnect`/clear-on-close state machine around `computeBackoffDelay`. | Extract `src/client/reconnect.ts::ReconnectScheduler` (`schedule(fn)`, `reset()`, `cancel()`, `exhausted`) and compose it into `SseClient`, `WebSocketClient`, and `HotReloadClient`. | low | `bun run --cwd src/ts/vertebra-client test` |
| FE-31 | `src/ts/vertebra-client/src/react/index.tsx:33-97`, `vue/index.ts:25-85`, `svelte/index.ts:24-100`, `angular/index.ts:26-92`; `react/cache.ts:14-110` | duplication | Four framework bindings each rebuild the same `{data, loading, error, refresh}` query and `{mutate, loading, error}` mutation state machine over `client.call`. Only React gets `QueryCache`. No adapter has a test (`useQuery` tests=0). | Add `src/client/query.ts::QueryController` (framework-agnostic: subscribe/notify, cache via `QueryCache`, dedupe). Each adapter becomes a ~20-line reactive wrapper. | med (framework reactivity semantics) | `bun run --cwd src/ts/vertebra-client test` plus new `tests/query.test.ts` |
| FE-32 | `src/ts/vertebra-client/src/arrow.ts:12-49`, `src/wasm.ts:12-43` | stub/placeholder | `ArrowStreamClient` stores `endpoint` + `headers` and checks magic bytes, but has no fetch or stream method; its doc claims "Zero-copy Apache Arrow IPC stream client". `WasmClient.maxMemoryPages` only feeds a getter and is never passed to `WebAssembly.Memory`. | Implement `ArrowStreamClient.stream(): AsyncIterable<Uint8Array>` (fetch + `ReadableStream` reader splitting messages on the continuation marker), and pass a `WebAssembly.Memory({ maximum: maxMemoryPages })` import in `WasmClient`. If deferred, make the docs say "header helpers". | low | `bun run --cwd src/ts/vertebra-client test -- arrow wasm` |
| FE-33 | `src/ts/vertebra-client/src/client/types.ts:1`; `package.json:4,77-81`; `tsup.config.ts:47-51` | dead export / docs drift | `types.ts` re-exports `computeBackoffDelay` and nobody imports it from there. The package description says "Vertebra Ghost Framework". `svelte` is marked external but is not in `peerDependencies`. The tsup `onSuccess` catches and only logs copy failures, so the Python bundle can silently go stale. | Delete the re-export, fix the description, add `svelte` to `peerDependencies` + `peerDependenciesMeta`, and make `onSuccess` throw. | low | `bun run --cwd src/ts/vertebra-client build` |
| FE-34 | `src/ts/vertebra-client/src/live/index.ts:22-40` | hot-path allocation | `fnv1aStateId`/`fnv1aFieldId` allocate a `new TextEncoder()` per call, and the 64-bit variant uses BigInt arithmetic per byte. | Hoist a module-level `const encoder = new TextEncoder()`. Memoize field ids in a `Map<string, number>` (field names are a small fixed set). | low | `bun run --cwd src/ts/vertebra-client test -- live` |

#### Test suite

| ID | Location | Category | Evidence | Proposed change | Risk | Verify |
|---|---|---|---|---|---|---|
| TST-01 | `src/py/tests/unit/server/test_stage1_hotpath_and_314t.py:68-94`; `extensions/test_drop_in_names_and_generics.py:73-94` | test weakness | Import-only tests: `assert BuildOrchestrator is not None` ×12, `assert callable(...)`, `assert isinstance(MemoryStore(), MemoryStore)`, and 10 `assert not hasattr(module, "OldName")` checks for aliases that no longer exist. Repo-wide, 98 asserts match only `is not None`/`callable`/`hasattr`/bare `isinstance`; the worst files are `test_stage1_hotpath_and_314t.py` (16), `test_drop_in_names_and_generics.py` (9), `test_inertia_middleware.py` (8), `test_litestar_plugin.py` (7), `test_data_storage_integrations.py` (6), `test_codegen_extractor.py` (6). | Delete `test_subdomain_consolidation_and_shims` and `test_canonical_extension_exports`; import errors already fail collection. Replace the remaining weak asserts with behavioral assertions on returned values. | low | `uv run pytest src/py/tests -q` |
| TST-02 | `src/py/tests/unit/server/test_stage1_hotpath_and_314t.py:100-101` | test weakness (brittle) | `assert "to_thread" not in inspect.getsource(store_mod)` checks source text, not behavior. | Patch `asyncio.to_thread` with a `pytest.fail` side effect and run the async path. | low | `uv run pytest src/py/tests/unit/storage -k to_thread` |
| TST-03 | `src/py/tests/unit/server/test_stage1_hotpath_and_314t.py`, `extensions/test_drop_in_names_and_generics.py`, `telemetry/test_telemetry_and_platform.py`, `build/test_packaging_and_pgo.py`, `extensions/test_codegen_suite.py`, `framework/test_framework_subdomains.py`, `db/test_pg_dataplane_and_datafusion.py`, `extensions/test_oracle_apex_and_gateway.py`, `extensions/test_sqlalchemy_and_sqlspec_vtb.py` | ad-hoc test files | These files are named after PR waves ("stage1", "drop_in_names", "*_and_*") rather than modules, and each covers 3-6 unrelated domains. Example: `test_stage1_hotpath_and_314t.py` touches storage, alchemy, portal/sync_tools, GIL audit, SHM policy, and ASGI loop pool. | Redistribute by module under test: `tests/unit/storage/test_store.py`, `tests/unit/extensions/alchemy/test_storage.py`, `tests/unit/utils/test_sync_tools.py` (which already exists: `unit/utils/test_sync_tools.py`), `tests/unit/server/test_gil_audit.py`, `test_shm_policy.py`, `test_runtime_tuning.py` (exists), `tests/unit/telemetry/test_{logging,trace_context,sql_trace,genai}.py`, `tests/unit/server/platform/test_{cgroup,zpages,lame_duck}.py`, and so on. | low | `uv run pytest src/py/tests --co -q \| tail -1` (same test count minus deleted weak tests) |
| TST-04 | `src/py/tests/unit/framework/test_framework_subdomains.py:97,172`; `telemetry/test_telemetry_and_platform.py:130`; `db/test_pg_dataplane_and_datafusion.py:80`; `extensions/test_oracle_apex_and_gateway.py:153` | test weakness (mega-tests) | Single tests named `test_queues_channels_stores_email_and_procedures`, `test_mcp_a2a_wasm_and_litestar_ecosystem_bridge`, `test_span_ring_genai_metrics_cgroup_and_zpages`, `test_embedded_datafusion_and_memfd_scm_rights`: one failure hides the rest, and there is no clear owning module. | Split into one test per behavior inside the canonical files from TST-03. | low | `uv run pytest src/py/tests/unit/framework -q` |
| TST-05 | `src/py/tests/unit/framework/test_orchestrator.py`, `test_vite_fallback.py`, `test_js_config.py`, `test_import_map.py`, `test_hmr_routing.py`; `unit/codegen/*`; `unit/extensions/test_codegen_suite.py`; `unit/build/test_packaging_and_pgo.py:96`; `framework/test_orchestrator.py:204` | misplaced tests | `vertebra.build` tests are spread over `unit/framework/`, `unit/codegen/`, `unit/extensions/`, and `unit/build/`. CLI build tests live in `build/` and `framework/`. `unit/test_utils_{init,json,misc}.py` sit at the root even though `unit/utils/` exists. `unit/test_freethreaded_gil.py` is at the root. | Mirror the source tree: `tests/unit/build/{test_config,test_orchestrator,test_vite,test_import_map}.py`, `tests/unit/build/codegen/{test_extractor,test_manifest,test_typescript,test_zod,test_valibot,test_openapi,test_protobuf,test_procedures,test_watch}.py`, `tests/unit/build/packaging/test_{targets,standalone,onefile}.py`, `tests/unit/cli/test_build.py`, `tests/unit/utils/test_{init,json,misc}.py`, `tests/unit/ffi/test_freethreaded_gil.py`. | low | `uv run pytest src/py/tests --co -q` |
| TST-06 | `src/py/tests/unit/test_bench_infrastructure.py:8-118`; `build/test_packaging_and_pgo.py:142-172` | test weakness + standards | `sys.path.insert(0, repo_root)` hack, 17 function-scoped imports, and tautologies against constants (`target.port == 8012`, `len(ALL_TARGETS) >= 10`, `"granian-asgi" in BASELINE_TARGETS`, `support.cpython_pgo is True`). Tests for `tools/` live inside the product unit suite. | Keep only behavioral checks (`no_port_collisions`, `endpoint_coverage_*`, `expand_scenario_runs`, `oha` parse) in `tools/benchmarks/tests/test_benchmarks.py` and `tools/pgo/tests/test_pgo.py`. Add `tools` to `testpaths`, import at module top, and delete the constant-echo tests. | low | `uv run pytest tools -q` |
| TST-07 | `src/py/tests/conftest.py:29-38,41-44,47-56,59-82` | dead code + standards + perf | 5 of 6 fixtures are unused (`codegen_dir`, `shm_name`, `mock_asgi_app`, `asgi_send`, `asgi_receive`: 0 references). The autouse `_cleanup_shm` does `import glob, os` inside the function and globs `/dev/shm/*vertebra*` after every one of 571 tests, so it can delete segments owned by a concurrently running dev server or another test session. | Delete the unused fixtures. Make SHM cleanup a session-scoped fixture that removes only names created by this session (prefix `test-{session_uuid}-`), with top-level imports. | med (SHM tests relying on implicit cleanup) | `make py-test` |
| TST-08 | `src/py/tests/integration/storage/conftest.py:32-116`; `src/py/tests/integration/__init__.py` | stub/placeholder | The integration tree has 8 Floci emulator fixtures and zero tests. `pytest` has no `integration` marker registered. | Add `tests/integration/storage/test_floci_roundtrip.py` (skip when `floci_available` is false) covering put/get/list/delete per provider, and register `markers = ["integration: requires emulators"]`. Otherwise delete the tree. | low | `uv run pytest src/py/tests/integration -m integration` |
| TST-09 | `src/py/tests/unit/server/test_asgi.py` (42), `test_runner.py` (38), `test_e2e.py` (18), `test_wsgi.py` (10), `test_utils_misc.py` (4) | standards violation | 131 `PLC0415` function-scoped imports in tests (`ruff --select PLC0415`). | Move them to module top. Use `pytest.importorskip` at module level for optional dependencies. | low | `uv run ruff check --select PLC0415 src/py/tests` |
| TST-10 | `src/py/tests/unit/framework/test_orchestrator.py:69-107`; `unit/codegen/test_codegen_watch.py` | test weakness (mocks hide bugs) | Every orchestrator test patches `_run_bundler` or the bundler module, and no test covers `CodegenWatcher`. That is how FE-13 and FE-14 got through. | Add tests against the real `vertebra._bundler` extension (skip if not built): a native build writes `dist/main.js`, dev mode keeps the watcher alive, and `CodegenWatcher` fires the callback within 2 s of touching a `.py` file. | low | `uv run pytest src/py/tests/unit/build -k "native or watch"` |
| TST-11 | `src/rs/vertebra-bundler/src/config.rs:247-274`; `hmr.rs:244-251`; `ssr.rs` `test_ssr_pool_creation` | test weakness | The defaults tests re-assert literal defaults. `test_hmr_watcher_creation` asserts only `result.is_ok()`, and `test_ssr_pool_creation` asserts only `slots.len()`. No test covers watcher event delivery or concurrent SSR. | Replace them with: the watcher emits `Update` for a touched `.ts` file and filters `.md`; `render` from N > pool threads succeeds; the cache stays bounded. | low | `cargo test -p vertebra-bundler` |
| TST-12 | `src/rs/vertebra-http/tests/*.rs` (18 files) | test organization / build cost | 10 `*_tests.rs` + 8 `test_*.rs` files. Each one compiles and links as its own binary against hyper/quinn/rustls/tokio. | Consolidate into `tests/it/main.rs` with `mod router; mod server; mod sse_hub; …` (one link instead of 18) and settle on one naming convention. Do the same for `vertebra-core/tests` (4 files). | low | `cd src/rs && cargo test -p vertebra-http --features tls,compression` (compare wall time) |
| TST-13 | `src/ts/vertebra-client/tests/*.test.ts` vs `tests/unit/*.test.ts`; `tests/unit/client.test.ts:132,167` | test organization | Two layouts. `client.test.ts` also tests `QueryCache` (react) and `initHtmx` (htmx). `arrow.test.ts`/`wasm.test.ts` test only stub helpers (FE-32). There are no tests for `OfflineDeltaQueue` or the framework adapters. | Flatten into `tests/<module>.test.ts` mirroring `src/` (`client.test.ts`, `query.test.ts`, `htmx.test.ts`, `live.test.ts`, …). Add adapter tests alongside FE-31. | low | `bun run --cwd src/ts/vertebra-client test` |

#### Makefile, packaging config, CI, and benchmarks

| ID | Location | Category | Evidence | Proposed change | Risk | Verify |
|---|---|---|---|---|---|---|
| TOOL-01 | `.github/workflows/benchmark.yml:74-81`; `.gitignore:77-79` | broken CI step | The "Commit History" step runs `git add tools/benchmarks/results/*.duckdb`, but `.gitignore` ignores `*.duckdb`, so `git add` errors on ignored paths. It also pushes straight to `main`, which repo policy forbids. | Drop the commit step and keep the 90-day artifact. To build a history, push to a dedicated `bench-history` branch or attach to releases. | low | `act -j benchmark` or a dry run of `git add` against `.gitignore` |
| TOOL-02 | `.github/workflows/benchmark.yml:49-63`; `Makefile` `install` (deps `destroy clean`), `bench: build` | duplicated builds | The benchmark job runs `make install` (destroy venv, `clean` deletes `src/rs/target`, then `dev` debug build), then `make build` (release), then `make bench`, whose `build` prerequisite rebuilds again with a TS build and `uv build` sdist/wheel. On top of that, `cargo install oha` compiles every run with no cache. | CI: `uv sync --group bench && make build && make bench BENCH_SKIP_BUILD=1`, and install `oha` from the release binary (`taiki-e/install-action@v2 with: tool: oha`). Makefile: split `bench` (run only) from `bench-build`. | low | `make -n bench` |
| TOOL-03 | `Makefile:147-153,158-166` | duplicated builds | `dev` runs `cargo build --workspace --exclude vertebra-py --exclude vertebra-bundler --exclude vertebra-storage` and then three separate `maturin develop` runs. Each of those uses a different feature set (`extension-module`), so shared crates (`vertebra-core`, `pyo3`) compile up to four times. `build` also ends in `uv build`, which compiles the wheel a fifth time. `py-test: dev` and `check: lint test coverage` run the Python suite twice (`py-test`, then `coverage`). | Point all three maturin builds at a shared `CARGO_TARGET_DIR` with aligned features. Have `check` run `coverage` in place of `py-test` (`test-rs test-ts coverage`). Move `uv build` into a separate `dist` target. | med | `time make dev` before/after |
| TOOL-04 | `Makefile` (no target) | missing benchmark tooling | Criterion benches exist (`vertebra-core/benches/{spsc,mpmc,spin_wait,router,guards,ring_throughput,codec,shm_transport}.rs`, `vertebra-http/benches/server_throughput.rs`), but no Makefile target or CI job runs them. `codec` needs `--features compression`. The router is benchmarked twice under different crates (`vertebra-core/benches/router.rs` `router_match`, and `vertebra-http/benches/server_throughput.rs:23` `router`). | Add `bench-rs:` → `cd src/rs && cargo bench -p vertebra-core --features compression -- --save-baseline $${BASELINE:-current}` plus `cargo bench -p vertebra-http --bench server_throughput -- --save-baseline …`, and `bench-rs-compare:` → `-- --baseline $${BASELINE}`. Keep one router bench, owned by whichever router survives the core/http router consolidation. | low | `make bench-rs BASELINE=pre-debloat` |
| TOOL-05 | `pyproject.toml:32-41,43-60` | duplication + bogus dep | `[project.optional-dependencies].dev` (pytest, `pytest-anyio>=0.0.0`, ruff, mypy) duplicates `[dependency-groups].dev`. `pytest-anyio` is an unrelated third-party package; anyio ships its own pytest plugin. The `framework = ["msgspec>=0.18"]` extra repeats a base dependency, and `bench-ft` passes `--extra framework` for nothing (`Makefile` bench-ft). `make upgrade` runs `uv run pre-commit autoupdate`, but `pre-commit` is in no dependency group (not in `uv.lock`). | Delete the `dev` and `framework` extras, add `pre-commit` to the `lint` group, and drop `--extra framework` from `bench-ft`. | low | `uv lock && make upgrade -n` |
| TOOL-06 | `pyproject.toml:81-94`; `Makefile` `lint`/`lint-all` | weak gates | Ruff selects only `E,F,I,UP,B,SIM`, so standards the repo enforces by review (no inline comments, docstrings, no function-scoped imports, no `print`) go unchecked. `tools/` is never linted (`lint` covers only `src/py/vertebra src/py/tests`), and mypy skips tests and tools. There is no `[tool.coverage]` (`--cov` without `source`) and no `--strict-markers`. `ruff --select ALL` on `tools/` finds 18 `D103`, 15 `S603`, 9 `PLC0415`, and 2 `INP001` (missing `__init__.py` under `tools/pgo`, `tools/scripts`). | Add `D`, `PLC0415`, `ERA`, `T20`, `PERF`, `RUF`, `PTH`, `TC` with per-file ignores for tests (`S101`, `PLR2004`). Lint `tools/`, run mypy on `tools/`, and add `[tool.coverage.run] source=["vertebra"]` plus `[tool.pytest.ini_options] addopts="--strict-markers -ra"` and `markers`. | med (fix-up churn) | `make lint-all` |
| TOOL-07 | `.github/workflows/ci.yml` `test-rust` vs `test-rust-features` | duplication | `test-rust` runs `make rs-test` (`cargo test --workspace --features tls,compression`) on ubuntu, and the `test-rust-features` ubuntu matrix row runs the same command. | Remove `test-rust` and keep the matrix (it also covers macOS `macos-ulock`), or remove the ubuntu matrix row. | low | inspect workflow run |
| TOOL-08 | `Makefile:17-24` vs `tools/scripts/setup-env.sh` | duplication | Host registry detection and overrides are written in both the Makefile preamble and `setup-env.sh`. | Keep only `setup-env.sh` (it writes persistent config) and drop the Makefile exports, or source one `tools/scripts/registry.env`. | low | `make setup-env` on internal-mirror and standard hosts |
| TOOL-09 | `Makefile` `lint` (`ts-lint` → `biome check --write --unsafe`), `py-test` (`-v`) | ergonomics | `make lint` applies Biome and Ruff `--unsafe-fixes` automatically. `py-test -v` prints 571 lines per run. | Keep the auto-fix but make it opt-in for unsafe fixes (`LINT_UNSAFE=1`). Default `py-test` to `-q -ra`. | low | `make lint` |
| TOOL-10 | `src/ts/vertebra-client/package.json` scripts; CI `test-typescript` | missing gate | CI never runs `bun run build` for TS, so FE-28 (missing react/wasm/arrow outputs) cannot fail CI. There is no bundle-size budget. | Add `make ts-build` and an exports check (`bunx publint`) to the `test-typescript` job, plus `size-limit` budgets for `index`, `react`, `live`. | low | CI run |

## MCP, A2A, and agent alignment with litestar-mcp (prefix MCP)

#### MCP-01: Native `McpServer` is a partial, unmounted MCP implementation
- **Location:** `src/py/vertebra/framework/mcp/__init__.py:52-173`
- **Category:** stub/placeholder, reinvention
- **Evidence:**
  - No route or transport mounts it anywhere.
  - `initialize` advertises a `prompts` capability (`:117`), but there is no `prompts/list`, `prompts/get`, `resources/read`, `ping`, or `notifications/initialized` handler.
  - Notifications (no `id`) get an error response, which violates JSON-RPC 2.0. litestar-mcp returns nothing (`core/jsonrpc.py:105-111`).
  - Tool exceptions propagate unhandled (`:145`); results are flattened with `str(out)` (`:151`), with no `structuredContent` or `isError`.
  - Unknown tool returns `-32601` (method not found) instead of `-32602` (invalid params) or a tool error.
- **Proposed change:** Delete `McpServer`, `McpResource`, `McpPrompt`, and the three decorators. Under the Litestar extension, delegate to litestar-mcp's `LitestarMCP` (see Decision D1). Keep no native Python MCP dispatcher.
- **Risk:** Medium. Public names are removed (`vertebra.framework.mcp.*`), and `extensions/apex` imports `McpTool`/`McpServer` (handled in MCP-09).
- **Verification:**
  - `rg -n 'framework\.mcp|McpServer|McpTool|mcp_tool' src` (expect zero hits after the change)
  - `uv run pytest src/py/tests/unit/framework/test_framework_subdomains.py src/py/tests/unit/extensions -q`
  - `make py-test`

#### MCP-02: MCP protocol version drift across layers
- **Location:** `framework/mcp/__init__.py:115` (`2025-03-26`); `cli/mcp.py:20` and `discovery.rs:84` (`2026-07-28`)
- **Category:** misalignment
- **Evidence:** Three hardcoded strings that disagree. litestar-mcp pins `2026-07-28` (`mcp/routes.py:48`, `mcp/service.py:82`).
- **Proposed change:** Remove the Python copies along with MCP-01 and MCP-03. If the Rust discovery cache survives, set its header from the version the Python plugin reports at startup (`litestar_mcp.mcp.routes.MCP_PROTOCOL_VERSION`) instead of a literal.
- **Risk:** Low.
- **Verification:**
  - `rg -n '2025-03-26|2026-07-28' src`
  - `cargo test -p vertebra-http --test mcp_discovery_bridge_tests`

#### MCP-03: Duplicate stdio bridge, Python and Rust, both weaker than litestar-mcp's
- **Location:** `src/py/vertebra/cli/mcp.py:23-113`; `src/rs/vertebra-http/src/mcp_bridge.rs:1-78`
- **Category:** duplication, reinvention, stub/placeholder
- **Evidence:**
  - The Rust `McpStdioBridge` has no HTTP client, and `endpoint_url` is a dead field (`mcp_bridge.rs:32`). Its only consumer is a test.
  - The Python bridge:
    - uses blocking `urllib`;
    - sends `Accept: text/event-stream` but writes SSE bodies verbatim to stdout, which breaks newline-delimited JSON-RPC framing;
    - has no `Mcp-Session-Id` round-trip;
    - takes `Mcp-Name` only from `params.name`, which is wrong for `resources/read`, `tasks/*`, and `skills/get` (compare litestar-mcp `routes.py:58`);
    - does not encode header values;
    - has no message-size cap;
    - swallows a broad `except Exception` (`cli/mcp.py:30-33`).
  - litestar-mcp `run_bridge` (`mcp/bridge.py:150`) covers all of these and needs no Litestar app instance.
- **Proposed change:**
  - Delete `mcp_bridge.rs` and its `pub mod` (`lib.rs:25`), plus the bridge and extract cases in `tests/mcp_discovery_bridge_tests.rs`.
  - Make `vtb mcp bridge` a thin wrapper over `litestar_mcp.mcp.bridge.run_bridge` behind the extra, raising a hard error if the extra is missing. Alternatively, drop the command and document `litestar mcp bridge`.
  - If Rust access logs later need `Mcp-*` fields, add a tested extractor inside `access_log.rs` that uses the per-method field map.
- **Risk:** Low. No runtime consumers.
- **Verification:**
  - `cargo test -p vertebra-http`
  - `make rs-test`
  - `uv run pytest src/py/tests/unit/cli/test_mcp_cli.py -q`

#### MCP-04: Native A2A server and card are pre-1.0 and stateful in process
- **Location:** `src/py/vertebra/framework/a2a/__init__.py:25-167`
- **Category:** stub/placeholder, misalignment
- **Evidence:**
  - `AgentCard.to_dict` emits the old shape (`url`, `authentication.schemes`), while `discovery.rs:75` advertises `a2a-version: 1.0`.
  - It accepts legacy `tasks/send`.
  - The task dict is unbounded, and any caller can `tasks/get` or `tasks/cancel` any ID (no owner scoping).
  - A cancel of an unknown ID still reports `canceled` (`:154-162`).
  - There is no transport.
  - litestar-mcp `LitestarA2A` (`a2a/plugin.py:409`) wraps `a2a-sdk>=1.1.4`, with card validation, an ETag card route, guards, a `context_builder`, and streaming cleanup.
- **Proposed change:** Delete `A2AServer`, `A2ATask`, and `A2ATaskState`. Under the Litestar extension, delegate to `litestar_mcp.a2a.LitestarA2A`. Either replace `AgentSkill`/`AgentCard` with `a2a.types` or derive them from `SkillController.to_agent_skill()` (`skill_controller.py:58`).
- **Risk:** Medium. `extensions/apex/gateway.py` and `openapi.py` import `AgentCard`/`AgentSkill` (MCP-09).
- **Verification:**
  - `rg -n 'framework\.a2a|A2AServer|AgentCard|AgentSkill' src`
  - `uv run pytest src/py/tests/unit/extensions/test_oracle_apex_and_gateway.py -q`

#### MCP-05: `LitestarEcosystemBridge` is a placeholder, and its attach helper silently no-ops on real Litestar apps
- **Location:** `src/py/vertebra/extensions/litestar/ecosystem.py:1-93`
- **Category:** stub/placeholder, misalignment, layering
- **Evidence:**
  - The module docstring (`:1`) claims litestar-mcp, litestar-queues, litestar-security, and litestar-vite acceleration; none of those packages are imported.
  - It eagerly builds `QueueService()`/`EmailService()` (`:46-47`, `:71-72`) whether or not anything uses them.
  - `attach_litestar_ecosystem_bridges` guards on `isinstance(state, dict)` (`:84-85`), but Litestar `State` is not a `dict`, so nothing is ever attached on a real app (verified). The only test uses `SimpleNamespace(state={})`.
  - It is not a Litestar plugin and registers no routes.
- **Proposed change:** Delete `LitestarEcosystemBridge` and `attach_litestar_ecosystem_bridges`. Give `LitestarPlugin` (or a new `MCPExtension` helper) an opt-in that appends litestar-mcp's `LitestarMCP(MCPConfig(...))`, and optionally `LitestarA2A`, to `app_config.plugins` during `on_app_init`. If the extra is missing, raise a hard `MissingDependencyError`-style error rather than falling back silently.
- **Risk:** Low to medium. These are public exports in `extensions/litestar/__init__.py:10-13` and `:57,66`.
- **Verification:**
  - Add a test that builds a real `Litestar(plugins=[LitestarPlugin(...)])` and asserts `/mcp` responds to `initialize` through `litestar.testing.AsyncTestClient`.
  - `uv run pytest src/py/tests/unit/extensions/test_litestar_plugin.py -q`
  - `make py-test`

#### MCP-06: Decorator names collide with litestar-mcp but behave differently
- **Location:** `framework/mcp/__init__.py:176-232`
- **Category:** naming, reinvention
- **Evidence:**
  - Vertebra's `mcp_tool` returns an `McpTool` struct, so the decorated name is no longer callable, and it needs manual `register_tool`.
  - litestar-mcp's `mcp_tool` (`utils/__init__.py:143`) marks a route handler that is auto-discovered.
  - Vertebra tools advertise an empty `inputSchema` unless one is passed by hand (`:30`, `:190`); litestar-mcp derives the schema from the signature.
  - The `McpX` vs `MCPX` casing differs.
- **Proposed change:** Removed as part of MCP-01. If a native descriptor must stay for the non-Litestar App (D1 option B), name it per Vertebra's domain-naming rules without the `mcp_tool` collision, and derive the schema with `msgspec.json.schema` from the signature.
- **Risk:** Low.
- **Verification:** `rg -n 'def mcp_tool|def mcp_resource|def mcp_prompt' src/py`

#### MCP-07: Rust discovery cache and SPA exclusions are unwired and hardcoded
- **Location:** `src/rs/vertebra-http/src/discovery.rs:10`, `:28-120`
- **Category:** stub/placeholder, layering
- **Evidence:**
  - Neither `WellKnownDiscoveryCache` nor `is_excluded_from_spa_fallback` is referenced outside `tests/mcp_discovery_bridge_tests.rs`.
  - The prefixes are literals, while litestar-mcp paths are configurable.
  - litestar-mcp already serves the agent card with SHA-256 ETag/304 and `Cache-Control` (`a2a/plugin.py:433-457`), so the Rust cache duplicates it unless it is wired in front of Python.
- **Proposed change:** Pick one:
  - (a) Wire it into the `vertebra-http` ingress so that, at startup, the Litestar extension pushes card and manifest bytes plus the configured `base_path`/`path`/`agent_card_path`, and Rust answers `GET`/`304` without the GIL. Keep it only if `make bench` shows a measurable gain on `/.well-known/agent-card.json`.
  - (b) Delete it.

  Either way, the exclusion list must come from plugin config.
- **Risk:** Low.
- **Verification:**
  - `cargo test -p vertebra-http --test mcp_discovery_bridge_tests`
  - `make rs-test`
  - Bench before and after if (a) is chosen.

#### MCP-08: `vertebra.framework` eagerly imports protocol stubs
- **Location:** `src/py/vertebra/framework/__init__.py:5-22`
- **Category:** API surface bloat
- **Evidence:** `import vertebra.framework` loads `a2a` and `mcp` (and 14 other subdomains) even though top-level `vertebra` uses PEP 562 lazy exports. Removing the stubs (MCP-01, MCP-04) also removes them from this import list.
- **Proposed change:** Drop `a2a` and `mcp` from the subdomain list. Optionally convert the remaining subdomains to the same `_MODULE_MAP` lazy pattern that litestar-mcp uses.
- **Risk:** Low.
- **Verification:** `uv run python -X importtime -c "import vertebra.framework" 2>&1 | rg 'framework\.(mcp|a2a)'` (expect no hits). Then `make py-test`.

#### MCP-09: Apex extension produces non-executable MCP tools from stub types
- **Location:**
  - `src/py/vertebra/extensions/apex/openapi.py:9-10`, `:101-125`
  - `src/py/vertebra/extensions/apex/gateway.py:15-16`, `:133-156`
- **Category:** stub/placeholder, layering
- **Evidence:**
  - `to_mcp_tools` attaches `_noop_tool_handler` to every tool.
  - `ApexGateway` uses `McpServer` only as a dict holder.
  - Both depend on the types that MCP-01 and MCP-04 remove.
- **Proposed change:** Have `to_mcp_tools` return litestar-mcp `Tool` objects whose callables perform the ORDS request (or plain JSON tool descriptors, if execution belongs elsewhere). Have `ApexGateway` expose a `SkillController` subclass or a `list[Tool]` that the Litestar extension registers. Replace `AgentCard`/`AgentSkill` with `a2a.types` behind the extra.
- **Risk:** Medium. This is a behavior change in the apex extension's public methods.
- **Verification:** `uv run pytest src/py/tests/unit/extensions/test_oracle_apex_and_gateway.py -q`

#### MCP-10: Tests validate dead surface and fake state instead of wire behavior
- **Location:** `src/py/tests/unit/framework/test_framework_subdomains.py:173-235`; `src/rs/vertebra-http/tests/mcp_discovery_bridge_tests.rs`
- **Category:** stub/placeholder (weak gates)
- **Evidence:**
  - The Python test calls `handle_jsonrpc` directly and asserts a `dict` write that cannot happen on a real Litestar `State` (MCP-05).
  - The Rust tests cover types that nothing in production calls.
- **Proposed change:** Replace them with one integration test per surface: a real Litestar app with the extension enabled, exercising `initialize`, `tools/list`, `tools/call`, the agent card `200`/`304`, and the `vtb mcp bridge` wrapper against an in-process ASGI transport.
- **Risk:** Low.
- **Verification:**
  - `uv run pytest src/py/tests -k "mcp or a2a" -q`
  - `make test`

## OKF knowledge-audit code defects (prefix OKF)

Found while auditing the knowledge bundle against the code; verified against `edceea6`.

| ID | file:line | Category | Evidence | Proposed change | Verify |
|---|---|---|---|---|---|
| OKF-01 | `src/rs/vertebra-http/src/dispatcher/shm.rs:176`, `correlation.rs:63` | bug | `(internal_id << 8) \| shard_idx` where `internal_id = (generation << 32) \| index`; the shift drops the top 8 bits of the 32-bit generation, and decoders (`shm.rs:277,334`, `dispatcher/mod.rs:175,204`) recover a 24-bit generation, weakening stale-completion protection. | `CorrelationId` type that packs index, full generation, and shard without loss (HTTP-19). | `cargo test -p vertebra-http correlation_` with a generation-wrap regression test |
| OKF-02 | `src/rs/vertebra-core/src/channel.rs:1005-1014`, `:1666-1673` | bug | Zero-copy reads trust `SlotHeader::len` written by the worker process; a corrupted or hostile worker causes out-of-bounds reads in the server. | Clamp/validate `len <= payload_capacity()` before building slices; return a channel error. | `cargo test -p vertebra-core slot_len_out_of_range` |
| OKF-03 | `src/rs/vertebra-core/src/transport/ring_pair.rs:269-281` vs `channel.rs:312-317` | bug | `select_ring` picks the smallest tier with `total_size <= slot_size`, but `try_send_with` rejects `len > slot_size - 8`; frames within 8 bytes of a tier boundary fail with `PayloadTooLarge` instead of moving up a tier. | Compare against `payload_capacity()`. | `cargo test -p vertebra-core tier_boundary` |
| OKF-04 | `src/rs/vertebra-core/src/transport/ring_pair.rs:191` | bug | `headers_raw.len() as u16` truncates header blocks over 64 KiB (same as CORE-04). | Error on oversize (removed with the single response frame). | `cargo test -p vertebra-core tiered_ring_set` |
| OKF-05 | `src/ts/vertebra-client/package.json`, `tsup.config.ts` | bug | `./wasm` and `./arrow` exports have no tsup entries (same as FE-28). | Single entry map driving tsup and exports. | `bun run --cwd src/ts/vertebra-client build` and check `dist/` |
| OKF-06 | `src/rs/vertebra-http/tests/http3_quic_tests.rs:1`, `Makefile:176`, `.github/workflows/ci.yml:111,113` | tooling gap | Tests are gated on the `http3` feature, which no Makefile target or CI job enables. | CI job and Makefile flag running `--features http3`. | `cargo test -p vertebra-http --features http3 --test http3_quic_tests` |
| OKF-07 | `src/rs/vertebra-core/src/transport/ring_pair.rs` (`RingPair::reclaim`), `vertebra-http` `WorkerLifecycle`, `cleanup_stale_shm`, `install_panic_cleanup_hook` | stub/placeholder | Supervision and crash-recovery code has no non-test caller. | Wire into the server's worker supervision (HTTP-23). | `cargo test -p vertebra-http worker_crash_recovers` |
| OKF-08 | `tools/benchmarks/targets.py:95-100` | tooling bug | `vtb-ft-native-4w` silently becomes an `echo` placeholder when `src/rs/target-ft/release/vertebra-server` is missing, so `make bench-all` succeeds without measuring 3.14t. | Fail loudly. | `uv run --group bench python -m tools.benchmarks run -t vtb-ft-native-4w` without the binary exits non-zero |
