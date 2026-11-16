# Vertebra Review Checklist

Shared checklist with stable IDs across all 8 review domains. Every rule requires an observable verification method.

---

## RT: Rust runtime and IPC

- **RT-01**: Every new or modified `unsafe` block or `unsafe impl` carries a `// SAFETY:` comment naming the invariant it relies on. Verify: diff hunks containing `unsafe`; baseline count is a ratchet, not a failure.
- **RT-02**: Every slice built from SHM (`std::slice::from_raw_parts[_mut]`) is preceded by checked bounds validation (`offset.checked_add(len)` <= `data_len`, `slot_len <= max_payload`). Verify: grep `from_raw_parts` in `channel.rs`, `ipc.rs`, `tiered_ring.rs`, `buffer.rs`, and `transport/`.
- **RT-03**: Sleeper/waker handshakes keep the order: publish cursor (`Release`) -> unconditional `SeqCst` RMW on the wait word -> `SeqCst` load of `send_waiters`/`recv_waiters` -> `futex_wake`. Verify: `SlotView::release` and `wait_read_ptr`/`wait_write_ptr` call sites.
- **RT-04**: Each atomic ordering is justified by its pairing; `Relaxed` only for statistics or values never used to publish data. Verify: every changed `Ordering::` has a matching acquire/release partner.
- **RT-05**: Borrowed ring slots (`SlotView`, `try_send_with` closures) are released on every path, including early return and error; no double release. Verify: `?` and `return` inside slot scopes; `Drop` impl behavior.
- **RT-06**: `MpmcRing` capacity checks use the slot sequence (`seq as i64 - tail as i64 >= 0`), never SPSC index arithmetic. Verify: `channel.rs` `has_capacity`.
- **RT-07**: SHM header layout stays `#[repr(C)]`; producer cursor, consumer cursor, and waiter words sit on separate cache lines (`align(64)` / `align(128)`); layout changes update a version/magic constant and carry a `const` size/offset assertion. Verify: `buffer.rs` `ShmHeader`.
- **RT-08**: Platform code honors OS contracts: macOS `shm_open` names <= 31 bytes; `__ulock_wait` with `ULF_NO_ERRNO` treats `rc >= 0` as success; Linux futex handles `EAGAIN`/`EINTR`/`ETIMEDOUT`; non-Linux notifiers inherit both pipe ends. Verify: `platform/macos.rs`, `platform/linux.rs`, and `transport/ring_pair.rs`.
- **RT-09**: SHM segment lifecycle: names unique per pid/seq/attempt, unlinked on drop and on supervisor recovery, no leaked fds across `fork`/spawn beyond the inherited set. Verify: `shm.rs`, `cleanup.rs`, and `process.rs`.
- **RT-10**: No blocking syscall, futex wait, or `std::sync::Mutex` guard held across `.await`; spin waits are bounded and back off (`spin_wait`). Verify: async fns touching `sync.rs` or `notify.rs`.
- **RT-11**: `BufferPool` / `BatchBufferPool` acquire/release pairs are balanced and free-list claims are race-free. Verify: `buffer.rs` and `transport/mod.rs` tests.
- **RT-12**: Sharded structures use power-of-two masks only when `is_power_of_two()` holds, with a fallback; no new single global lock on the per-request path. Verify: mask calculations in ring and transport modules.

## HTTP: HTTP ingress

- **HTTP-01**: Request body and multipart limits are enforced before buffering; streaming bodies are bounded per chunk and in total. Verify: `payload.rs`, `multipart.rs`, and `streaming_body_tests.rs`.
- **HTTP-02**: Every bounded queue/ring on ingress has explicit full behavior (shed with `503` + `Retry-After`, or await a permit); no `unbounded_channel` on the request path. Verify: `backpressure.rs`, `batcher.rs`, and `dispatcher/`.
- **HTTP-03**: Correlation slots (`InFlightRequests`) are freed on client disconnect, timeout, worker death, and shutdown. Verify: `correlation.rs` exit paths; test that drops client connection mid-request.
- **HTTP-04**: h2/h2c/h3 honor protocol flow control and settings; h2c prior-knowledge detection matches the exact connection preface; `Upgrade: h2c` only when enabled. Verify: `h2c.rs`, `http3.rs`, and `h2c_and_mtls_tests.rs`.
- **HTTP-05**: gRPC framing: 5-byte prefix (flag byte + big-endian `u32` length), length capped by max message size before allocation, `grpc-status`/`grpc-message` trailers on every outcome including errors. Verify: `grpc.rs` and `grpc_wire_tests.rs`.
- **HTTP-06**: mTLS/identity: client-cert verification configured as documented; inbound `X-Forwarded-Client-Cert`/IAP identity headers are stripped from untrusted requests before injection. Verify: `tls.rs` identity extraction.
- **HTTP-07**: Secrets: token comparisons are constant-time (`profile.rs`); access logs never emit authorization headers, cookies, or query secrets. Verify: `access_log.rs`.
- **HTTP-08**: CORS regex origins are anchored; preflight validates method and requested headers; responses set `Vary: Origin`. Verify: `cors.rs`.
- **HTTP-09**: Compression skips already-compressed types, tiny bodies, `q=0` encodings, and SSE unless flushed per event. Verify: `compression.rs`.
- **HTTP-10**: Route normalization and FNV-1a handler ids stay identical between `vertebra-http/src/routing.rs` and `vertebra/framework/_router.py`; matchit 0.9 `{name}`/`{*rest}` syntax via `normalize_typed_path`. Verify: paired tests on both sides.
- **HTTP-11**: SSE/WebSocket/pubsub: per-subscriber buffers bounded, slow consumers evicted or dropped by policy, heartbeats configured, graceful shutdown drains. Verify: `sse.rs`, `ws_mount.rs`, and `pubsub.rs`.
- **HTTP-12**: Feature-gated modules compile alone and together (`tls`, `compression`, `http3`). Verify: `cargo check --manifest-path src/rs/Cargo.toml -p vertebra-http --no-default-features --features <f>`.

## FFI: PyO3 bridge

- **FFI-01**: Every `#[pymodule]` declares `gil_used = false`. Verify: `rg '#\[pymodule' src/rs`.
- **FFI-02**: Blocking waits, bulk copies, file I/O, subprocess waits, Oxc builds, and QuickJS renders run inside `py.detach(...)`. Verify: `get_batch`, `submit_*`, `Bundler::build`, `SsrPool::render`, and RSGI file I/O.
- **FFI-03**: No `Python<'_>`, `Bound`, or borrowed Python data crosses into a `py.detach` closure; no `unsafe impl Send/Sync` added to bypass that. Verify: closures and `unsafe impl`.
- **FFI-04**: Zero-copy views (`PyBuffer`, `body_view`, memoryviews over SHM) never outlive the slot or owner; views over released slots are invalidated or copied. Verify: `request.rs`, `transport.rs`, `lib.rs`, and `wsgi/input.rs`.
- **FFI-05**: Free-threaded safety: `#[pyclass]` with mutable state uses `frozen` + atomics/`Mutex`, never relies on the GIL for exclusion; no `static mut`; `OnceLock`/`PyOnceLock` used correctly under 3.14t. Verify: struct definitions in `vertebra-py/src`.
- **FFI-06**: No `unwrap`/`expect` on caller-controlled data in `#[pyfunction]`/`#[pymethods]`; errors map to typed `PyErr`. Verify: grep `unwrap()` in `vertebra-py/src`.
- **FFI-07**: Long-lived `Py<T>` held in Rust structs have a defined drop path; `clone_ref` is deliberate; no reference leak in loops. Verify: `sys.getrefcount` assertions in tests.
- **FFI-08**: Hot attribute/key names use `intern!`; no per-call `PyString::new` inside per-request loops. Verify: `asgi.rs`, `rsgi/scope.rs`, and `request.rs`.
- **FFI-09**: `{_vertebra,_bundler,_storage,_mysql,_bigquery,_mssql,_spanner,_duckdb,_oracle,_postgres,_sqlite}.pyi` match every added or changed `#[pyfunction]`/`#[pymethods]` signature and return type across every PyO3 extension. Verify: diff Rust signatures vs stubs; `uv run --no-sync python -m mypy.stubtest vertebra._vertebra vertebra._bundler vertebra._storage vertebra._mysql vertebra._bigquery vertebra._mssql vertebra._spanner vertebra._duckdb vertebra._oracle vertebra._postgres vertebra._sqlite` and `uv run --no-sync mypy src/py/vertebra`.
- **FFI-10**: Embedding registers `_vertebra` via `append_to_inittab!(pymodule_init)` (`use _vertebra::pymodule_init`) before Tokio/Python worker initialization. Verify: `src/rs/vertebra-http/src/main.rs`.

## PY: Python framework and server

- **PY-01**: Module layout: docstring -> grouped top-level imports -> `if TYPE_CHECKING:` -> `__all__` -> public classes/functions -> module-scoped `_...` helpers only when shared by several definitions. Verify: ruff and file layout inspection.
- **PY-02**: Zero inline `#` comments in `.py`; intent lives in Google-style docstrings. Verify: `tokenize` COMMENT tokens in changed files (shebang/encoding excluded).
- **PY-03**: PEP 695/604/585 typing, no `typing_extensions`; `uv run mypy src/py/vertebra` (strict) passes. Verify: `uv run mypy src/py/vertebra`.
- **PY-04**: Config, metadata, spec, and frame value objects are `msgspec.Struct(..., frozen=True, gc=False)` (or subclass `BaseStruct`/`CamelizedBaseStruct`/`FrozenBaseStruct`/`FrozenCamelizedStruct`), not `@dataclass`; `cache_hash=True` on hashable lookup/key structs; `array_like=True` strictly on internal binary/ring-buffer frames (`ChannelMessage`, `StateDeltaFrame`, `TaskEnvelope`, `DeadLetterEntry`), never on HTTP/SSE/JSON payloads. Verify: struct declarations.
- **PY-05**: JSON encode/decode goes through `vertebra.utils.serializers` (`encode_json`, `encode_json_bytes`, `encode_json_into`, `format_json`, `decode_json`); no direct `msgspec.json.*`, `json.dumps/loads` elsewhere. Verify: `rg 'msgspec\.json\.|json\.(dumps|loads)' src/py/vertebra`.
- **PY-06**: Package `__init__.py` files use PEP 562 lazy `__getattr__`; no eager submodule imports; `__all__` and `TYPE_CHECKING` imports list every lazy export. Verify: `uv run python -X importtime -c "import vertebra"`.
- **PY-07**: Canonical domain names; no brand-prefixed or implementation-leaking names, aliases, re-export shims, or compatibility wrappers. Verify: module naming inspection.
- **PY-08**: Imports at module top level; always import typing, struct bases, sentinels, and exceptions from the public `vertebra.typing` shim (never `vertebra._typing` outside `src/py/vertebra/typing.py`); optional/dynamic module imports must route through cached `vertebra.utils.module_loader` (`import_optional`, `ensure_module`, `import_app`, `import_string`) rather than raw `importlib.import_module`; no function-scoped imports. Verify: `make review-check` (`python_imports` rule) and AST inspection.
- **PY-09**: Class order: `__slots__`/class attributes -> `__init__` -> properties -> public -> private; single-class helpers are private methods; loose functions that belong to a domain class become static/class methods. Verify: class inspection.
- **PY-10**: Hot path: no `inspect.signature` per request, one loop wake per batch, no per-request `asyncio.create_task` unless the direct-drive path cannot apply. Verify: `_dispatch.py` and `_asgi/_runner.py`.
- **PY-11**: Async code never blocks the loop (file/network/subprocess via anyio or thread offload); worker shutdown cancels and awaits tasks. Verify: `server/worker.py` shutdown paths.
- **PY-12**: Public API changes update `__all__`, `.pyi` stubs, and knowledge `domains/` chapters in the same change. Verify: exports diff.

## TS: TypeScript client and bundler

- **TS-01**: Root `index.ts` exports only canonical domain symbols; framework adapters only via `package.json` `exports` subpaths. Verify: `src/ts/vertebra-client/src/index.ts` exports.
- **TS-02**: Core has zero runtime dependencies; framework libraries are optional `peerDependencies`. Verify: `package.json` dependencies.
- **TS-03**: `strict` + `noUncheckedIndexedAccess` hold; no `any`, `as unknown as`, or non-null assertions on network data. Verify: `bun run build` and `tsc --noEmit`.
- **TS-04**: Route contract changes update `RouteResponse`/client types and Python codegen (`_typescript.py`, `zod.py`, `_manifest.py`) together, with tests on both sides. Verify: paired test runs.
- **TS-05**: Realtime clients (`RealtimeChannel`/`WebSocketClient`, SSE, live state) use bounded backoff with jitter (`computeBackoffDelay`), remove listeners and timers on close, and resubscribe after reconnect. Verify: client reconnection tests.
- **TS-06**: Package stays tree-shakeable: ESM, `sideEffects: false`, one `tsup` entry per subpath, no top-level side effects in adapters. Verify: `tsup.config.ts` and bundle outputs.
- **TS-07**: Bundler/HMR/SSR: HMR messages typed end-to-end (`ServerMessage`), Oxc/rolldown config changes covered by Rust tests, SSR render runs off the GIL. Verify: bundler tests.
- **TS-08**: Biome clean (`bunx biome check .`); Vitest tests use fake timers, not real sleeps. Verify: `bun run test` and `bunx biome check .`.

## PERF: Performance

- **PERF-01**: A change on a hot path (rings, dispatch, batching, codec, router, `_dispatch.py`, `_fastpath_core.py`, `_asgi/_runner.py`) includes before/after numbers from `cargo bench --manifest-path src/rs/Cargo.toml -- --save-baseline`/`--baseline` or `make bench` + `make bench-diff` on the same machine. Verify: Criterion benchmark logs.
- **PERF-02**: No new per-request heap allocation (`Vec`, `String`, `Box`, `format!`, Python list/dict churn) where a pool, arena, or reusable buffer exists. Verify: allocations on request loop.
- **PERF-03**: No added cross-thread wakeup or lock acquisition per request; batch granularity preserved. Verify: futex calls and lock traces.
- **PERF-04**: No added copy across SHM or FFI (`try_send_with`, `try_recv_with`, `body_view` remain zero-copy). Verify: slice slicing and view lifetimes.
- **PERF-05**: `import vertebra` stays within the documented in-process budget. Verify: `-X importtime`.
- **PERF-06**: Benchmark-only packages stay in `[dependency-groups].bench` (or `[dependency-groups].postgres-bench` for PostgreSQL reference comparators); every `make bench*` passes `--group bench` (and `make postgres-bench*` passes `--group postgres-bench`). Verify: `pyproject.toml` dependency groups.
- **PERF-07**: Free-threaded mode: changes to dispatch, locks, or shared state are checked with `make bench-ft` or reasoned explicitly for 3.14t contention. Verify: `make bench-ft` execution.
- **PERF-08**: Benchmarks are valid: warmup, fixed iterations or duration, results recorded with machine/commit metadata under `tools/benchmarks/results/`, no cross-machine comparisons. Verify: benchmark results schema.
- **PERF-09**: Release profile (`lto = true`, `codegen-units = 1`, `opt-level = 3`) and PGO tooling are not regressed. Verify: `src/rs/Cargo.toml`.

## TEST: Tests

- **TEST-01**: New behavior or a defect fix has a test that fails without the change; red/green evidence is recorded in the task notes. Verify: test failure on unpatched baseline.
- **TEST-02**: Tests live in the canonical module for the unit (`src/py/tests/unit/<pkg>/test_<module>.py`, `src/rs/<crate>/tests/*.rs`, `src/ts/vertebra-client/tests/unit/*.test.ts`); no `test_*_fix.py`, `*remediation*`, `*audit*`, or stage-numbered files. Verify: test file path inspection.
- **TEST-03**: Assertions target observable behavior and named fields; no mock-echo assertions; `is not None`/`hasattr`/import-only assertions are never the sole check. Verify: assertion AST scan.
- **TEST-04**: Concurrency tests use real threads/processes, cross ring wrap-around and full/empty boundaries, and synchronize with barriers/channels, never sleeps. Verify: grep `sleep` in concurrency tests.
- **TEST-05**: Negative paths covered: timeouts, full rings, oversized payloads, malformed frames, disconnects, shutdown during work. Verify: error-path unit tests.
- **TEST-06**: SHM tests use unique names and clean up, and pass under default parallel `cargo test` threads. Verify: `cargo test` parallel execution.
- **TEST-07**: Async Python tests use `@pytest.mark.anyio` (no pytest-asyncio). Verify: `rg 'pytest\.mark\.asyncio' src/py/tests`.
- **TEST-08**: `#[cfg]`, `#[ignore]`, and `skipif` gates state a reason and never skip every assertion on a CI platform. Verify: test filter scan.
- **TEST-09**: No weakened assertions or deleted tests without an affected-coverage comparison. Verify: git diff on test files.
- **TEST-10**: Free-threaded behavior covered where relevant (`test_freethreaded_gil.py`: `sys._is_gil_enabled()` stays `False` after importing the extensions). Verify: `uv run python -X gil=0 -m pytest src/py/tests/unit/test_freethreaded_gil.py`.
- **TEST-11**: Tests do not pin incidental configuration (ports, private helper names, prompt phrases) without an operational contract. Verify: test inspection.

## DOC: Docs and OKF

- **DOC-01**: Every non-reserved `.agents/bundles/**/*.md` has OKF v0.2 frontmatter: non-empty `type`, `title`, `description`; `status in {draft, stable, deprecated}`; lowercase-hyphenated `tags`; root index declares `okf_version: "0.2"`. Verify: `make review-check`.
- **DOC-02**: Truth blocks are bounded and match the cited source (code excerpts compile against current signatures). Verify: truth block verification.
- **DOC-03**: Relative links and `sources: - resource:` paths resolve. Verify: link resolution check.
- **DOC-04**: Architecture, contract, command, or module-map changes update the owning knowledge chapter in the same change. Verify: diff inspection.
- **DOC-05**: Knowledge prose is current-state only; history goes in `log.md`. Verify: changelog/history check in knowledge files.
- **DOC-06**: No internal names, private paths, or count-prefixed labels in public docs (enforced by the validator's privacy rule). Verify: `make review-check`.
- **DOC-07**: `specs/` and `research/` stay lazy; no completed spec resident. Verify: spec state inspection.
- **DOC-08**: `AGENTS.md` and `workflow.md` commands match real `Makefile` targets. Verify: make target resolution.
- **DOC-09**: User-facing docs (`docs/`, `README.md`) updated for public API or configuration changes. Verify: documentation diff.
