---
title: "Chapter 7 — Frontend Bundler, Codegen, TypeScript SDK & Tooling Consolidation"
description: "Fix Oxc bundler transforms, QuickJS SSR pooling, source watchers, codegen emitters, @vertebra/client build/runtime defects, and repository CI/lint gates."
flow_id: frontend-and-tooling-consolidation
parent_spec: vertebra-design-quality
chapter: 7
state: closed
work_kind: refactor
state_revision: 1
plan_revision: 2
plan_commit: ""
updated_at: 2026-10-22T00:00:00Z
depends_on_chapters: [1, 4]
tags: [refactor, typescript, bundler, tooling]
---

# Chapter 7 — Frontend Bundler, Codegen, TypeScript SDK & Tooling Consolidation

## Problem & Context

The frontend, codegen, and tooling layers contain broken build steps, silent runtime failures, and duplicated emitters across Rust (`vertebra-bundler`), Python (`vertebra.build` and `vertebra.cli`), TypeScript (`@vertebra/client`), and repository CI/Makefile scripts:

1. **Rust `vertebra-bundler` (`src/rs/vertebra-bundler/`)**:
   - `BundleConfig` (`src/rs/vertebra-bundler/src/config.rs:11-108`) exposes 8 configuration fields (`entry`, `outdir`, `minify`, `sourcemap`, `target`, `external`, `define`, `format`, `tree_shaking`, `splitting`), but `Bundler::build_internal` (`src/rs/vertebra-bundler/src/bundler.rs:211-294`) ignores `sourcemap` and `target` (`TransformOptions::default()`) and never reads `external`, `define`, `format`, `tree_shaking`, or `splitting` (`FE-01`).
   - `Bundler` (`bundler.rs:25,68,93`) stores `hmr_rx: Option<broadcast::Receiver<HmrEvent>>` in `start_dev` and clears it in `stop_dev` without ever reading it (`FE-02`).
   - `IsolatedDeclarationsGenerator::emit_dts` (`src/rs/vertebra-bundler/src/dts.rs:34-92`) uses a naive line-by-line string scanner instead of `oxc_isolated_declarations`, breaking on multi-line function signatures; `dts.rs` also mixes `minify_css` and an unused `encode_ssr_worker_frame` function with broken `{component:?}` JSON escaping (`FE-03`).
   - `SsrPool` (`src/rs/vertebra-bundler/src/ssr.rs:63,152-159`) caches rendered HTML in an unbounded `DashMap<u64, CachedRender>` (`FE-04`), fails immediately with `BundlerError::PoolExhausted` instead of waiting when all QuickJS slots are busy (`FE-05`), clones the entire JS bundle `String` per slot and re-evaluates `__ssr_render` globals per call (`FE-06`), duplicates its API across `Bundler::init_ssr`/`ssr_render`/`ssr_invalidate`/`ssr_shutdown` and `SsrPool` while allocating an intermediate `HashMap<String, Py<PyAny>>` per render (`FE-07`), and enables unneeded `rquickjs` `bindgen`/`classes`/`properties` features alongside a redundant `unsafe impl Send for SlotEngine` (`FE-10`).
   - `Bundler`, `PySourceWatcher`, and `SsrPool` repeat `.map_err(|e| PyRuntimeError::new_err(e.to_string()))` across 18 sites instead of using `From<BundlerError> for PyErr` (`FE-08`), pull in `tokio`, `serde`, and `serde_json` solely for an internal watcher channel (`FE-09`), and run `Bundler::check_file` under the GIL (`FE-11`).
2. **File Watchers & Build Orchestration (`src/py/vertebra/build/`)**:
   - `PySourceWatcher::poll_events` (`hmr.rs:185`) takes 0 arguments in Rust, whereas `CodegenWatcher._watch_rust` (`src/py/vertebra/build/codegen/_watch.py:139`) calls `watcher.poll_events(timeout_ms)` and catches `Exception: break`, killing the watcher on iteration 1 (`FE-14`).
   - `BuildOrchestrator._run_native(dev=True)` (`src/py/vertebra/build/_orchestrator.py:151-160`) instantiates a local `Bundler()`, calls `start_dev`, and returns without storing `bundler`, dropping the watcher immediately (`FE-13`).
3. **Codegen, Packaging & CLI (`src/py/vertebra/build/`, `src/py/vertebra/cli/`)**:
   - Six codegen generators duplicate `_collect_objects` traversal (`FE-15`), `procedures.py` (668 lines) mixes marker definitions, JSON Schema conversion, runtime registry, and TypeScript client emission (`FE-16`), `ZodGenerator` and `ValibotGenerator` expose three overlapping entry points (`FE-17`), five build/codegen sites call `msgspec.json.format(encode_json_bytes(...))` instead of `format_json` (`FE-18`), and `_orchestrator.py:211` writes `export const routes = {};` after collecting route metadata (`FE-19`).
   - `build/_fallback.py` duplicates Vite binary discovery and uses `shell=True` + `preexec_fn=os.setsid` (`FE-20`), `build/__init__.py` eagerly imports 31 symbols across all submodules (`FE-21`), `TypeNode` omits `gc=False` (`FE-22`), `OnefileBuilder` duplicates trailer extraction logic between class methods and an embedded string stub (`FE-23`), `tools/pgo/{rust_pgo,python_pgo}.py` are disconnected from `StandaloneBuilder` (`FE-24`), `vtb init` is a stub that creates no files (`FE-25`), CLI options `--profile` and `--verbose` are dead (`FE-26`), and `cli/run.py` vs `cli/dev.py` duplicate server environment assembly (`FE-27`).
4. **TypeScript SDK (`src/ts/vertebra-client/`)**:
   - `tsup.config.ts` lists `src/react/index.ts` instead of `src/react/index.tsx` (`FE-28`), omits `src/wasm.ts` and `src/arrow.ts` (`FE-29`), and fails `dts` compilation on `src/wasm.ts:41` (`TS2739`) because `WebAssembly.instantiate(bytes, imports)` resolves to `WebAssembly.Instance` when given a `Uint8Array` (`FE-30`), while `.github/workflows/ci.yml` never runs `make ts-build` (`FE-31`).
   - `RealtimeChannel` (`src/client/ws.ts`) is a weaker duplicate of `WebSocketClient` (`src/websocket.ts`) (`FE-32`), `WebSocketClient` and `SseClient` duplicate FIFO idempotency sets with $O(N)$ `Array.shift()` (`FE-33`), `HotReloadClient` auto-connects on module import (`FE-34`), `HttpClient` mutates query/body parameters and returns `NaN` for `requestId` (`FE-35`), `QueryCache` is a module-global singleton with unstable `JSON.stringify` keys and unsubscribed `useQuery` (`FE-36`), Vue/Svelte/Angular `useQuery` composables lack stale-response guards and cleanups (`FE-37`), `ArrowStreamClient` and `WasmClient` ignore their options (`FE-38`), and `LiveStateClient` instantiates `TextEncoder` per hash call and drops `FieldDelete` (`FE-39`).
5. **Repository Tooling & CI (`Makefile`, `pyproject.toml`, `.github/workflows/`, `tools/scripts/`)**:
   - `make rs-test` and `src/py/tests/conftest.py` do not clean `/dev/shm/vtb-*` segments (`TOOL-01`), `ci.yml` never tests `--features http3` or `make ts-build` (`TOOL-02`), `Makefile` and `tools/scripts/setup-env.sh` duplicate mirror detection and `setup-env.sh` fails to remove stale `.npmrc` files (`TOOL-03`), `pyproject.toml` Ruff rules cover only `E,F,I,UP,B,SIM` on `src/py/vertebra` and `src/py/tests` (`TOOL-04`), `.gitignore` has duplicate patterns and ignores tracked `src/py/vertebra/server/_js/bundle/` (`TOOL-05`), `benchmark.yml` sets `UV_SYSTEM_PYTHON: "1"` and commits binary `.duckdb` files to `main` (`TOOL-06`), and three test directories lack `__init__.py` (`TOOL-07`).

## Scope

### In Scope
- Findings `FE-01`..`FE-11`, `FE-13`..`FE-39`, `TOOL-01`..`TOOL-07`, `TST-10`, `TST-11`, `OKF-05`, and `OKF-06`.
- Rust `vertebra-bundler` cleanup, `oxc_isolated_declarations` integration, bounded `SsrPool` with `Condvar` slot leasing, and Criterion benchmark `benches/ssr_render.rs`.
- Python `vertebra.build` (`_orchestrator.py`, `_fallback.py`, `_import_map.py`, `codegen/`, `packaging/`) and `vertebra.cli` (`init.py`, `build.py`, `dev.py`, `run.py`, `_launch.py`, `errors.py`, `banner/renderer.py`).
- TypeScript `@vertebra/client` build configuration (`tsup.config.ts`, `package.json`), transport/cache/adapter fixes, and Vitest unit coverage.
- Repository `Makefile`, `pyproject.toml`, `.github/workflows/{ci,benchmark}.yml`, `tools/scripts/setup-env.sh`, `.gitignore`, and OKF documentation updates (`workflow.md`, `polyglot-test-matrix.md`, `tech-stack.md`).

### Out of Scope
- `FE-12` (changing `vertebra-bundler` `Cargo.toml` `[package.metadata.maturin] name` to `"vertebra._bundler"` and deleting `src/py/vertebra/_bundler.py`), which is executed in Chapter 4 Task `4.1`.
- Framework and extension consolidations in `src/py/vertebra/framework/` and `src/py/vertebra/extensions/` (Chapter 5), except updating `NativeSsrClient` in `src/py/vertebra/framework/ssr/__init__.py` to use `SsrPool` directly in Task `7.4`.

## Applicable Requirements & Global Constraints

- **Requirements**: `R2` (Zero Runtime Defect Tolerance — watchers, SSR pool, `ts-build`), `R5` (Frontend Bundler, SSR & TypeScript SDK Integrity), `R6` (Python Framework & Extension Cohesion), `R8` (Tooling, CI & Lint Gate Completeness), `R10` (Test Quality & Coverage), `R11` (OKF Knowledge Base Synchronization).
- **Global Constraints**: Constraint 1 (Clean-Break Naming & Zero Shims), Constraint 2 (Zero Feature Removal), Constraint 3 (Strict Python Code Organization & Zero Inline Comments), Constraint 4 (`msgspec.Struct(frozen=True, gc=False)` Everywhere), Constraint 5 (Centralized Serialization via `vertebra.utils.serializers`), Constraint 6 (Top-Level Imports Only), Constraint 7 (Rust 2024 & PyO3 `0.30.1` Discipline), Constraint 8 (Shared-Memory Safety), Constraint 11 (Polyglot Rebuild Discipline).

## Architectural Design

1. **Synchronous Channel & Error Ergonomics in `vertebra-bundler`**:
   - Replace `tokio::sync::broadcast` in `hmr.rs` with `crossbeam_channel::bounded::<String>(256)` inside `PySourceWatcher`, dropping `tokio`, `serde`, and `serde_json` from `src/rs/vertebra-bundler/Cargo.toml`.
   - Extend `BundlerError` with `Poisoned(&'static str)` so every mutex/rwlock acquisition maps via `?` and `From<BundlerError> for PyErr`.
   - Expose `PySourceWatcher::poll_events(py, timeout_ms: Option<u64>) -> PyResult<Vec<String>>` using `py.detach` + `recv_timeout` + `try_recv` drain.
2. **Honored Transform Options & Oxc Isolated Declarations**:
   - Wire `BundleConfig.target` into `TransformOptions::from_target(&config.target)` and `BundleConfig.sourcemap` into `CodegenOptions { minify: config.minify, source_map_path: ... }`, writing `<stem>.js.map` and appending `//# sourceMappingURL=<stem>.js.map`. Remove unread `external`, `define`, `format`, `tree_shaking`, and `splitting` fields from `BundleConfig`.
   - Split `dts.rs` into `declarations.rs` (powered by `oxc_isolated_declarations::IsolatedDeclarations`) and `css.rs` (`minify_css`), deleting `encode_ssr_worker_frame`.
3. **Bounded `SsrPool` with Blocking Checkout**:
   - Replace the `ArrayQueue<usize>` immediate-fail checkout in `SsrPool` with a `Mutex<VecDeque<usize>>` + `Condvar` waiting up to `acquire_timeout` (default `500 ms`) before returning `BundlerError::PoolExhausted`.
   - Replace the unbounded `DashMap<u64, CachedRender>` with a bounded mutex-guarded LRU cache (capacity `256`, TTL `60 s`), store `bundle_source` as `Arc<str>`, and make `SsrPool` the single SSR pyclass used directly by Python `NativeSsrClient`.
4. **Shared `SchemaEmitter` Base & Split `procedures.py`**:
   - Extract `SchemaEmitter` in `src/py/vertebra/build/codegen/_emitter.py` and split `procedures.py` into `markers.py`, `_json_schema.py`, and `procedures.py`.
5. **Unified `@vertebra/client` Transport & Per-Tree `QueryCache`**:
   - Delete `src/client/ws.ts` (`RealtimeChannel`) and route `HttpClient.subscribe` through `WebSocketClient`.
   - Extract `BoundedKeySet` in `src/internal/lru_set.ts` ($O(1)$ `Set` eviction) shared by `WebSocketClient` and `SseClient`.
   - Scope `QueryCache` per `ClientProvider` tree with canonical key sorting and `onChange` re-render subscriptions in `useQuery`.

## File Ownership Boundary

| Path | Ownership |
| --- | --- |
| `src/rs/vertebra-bundler/**` | Exclusive to Chapter 7 (after Task `4.1` sets `name = "vertebra._bundler"`) |
| `src/py/vertebra/build/**` | Exclusive to Chapter 7 |
| `src/py/vertebra/cli/**` | Exclusive to Chapter 7 |
| `tools/pgo/**` | Exclusive to Chapter 7 (moved into `src/py/vertebra/build/packaging/pgo.py`) |
| `src/ts/vertebra-client/**` | Exclusive to Chapter 7 |
| `Makefile`, `pyproject.toml`, `.gitignore`, `.github/workflows/{ci,benchmark}.yml`, `tools/scripts/setup-env.sh` | Exclusive to Chapter 7 (Task `7.9` applies Ruff promotions prepared in Chapter 5 Task `5.2`) |

## Task Checklist

- [x] **7.1** — Clean up `vertebra-bundler` dead state, error mapping, and unused dependencies (`tasks/7.1.md`, `characterization`, `depends_on: []`)
- [x] **7.2** — Fix `PySourceWatcher.poll_events` signature mismatch and `BuildOrchestrator` dev watcher lifetime (`tasks/7.2.md`, `regression_tdd`, `depends_on: [7.1]`)
- [x] **7.3** — Wire `BundleConfig.sourcemap`/`target` and replace line-scanned `.d.ts` emitter with `oxc_isolated_declarations` (`tasks/7.3.md`, `regression_tdd`, `depends_on: [7.2]`)
- [x] **7.4** — Bound `SsrPool` cache, add Condvar slot checkout, and unify the SSR API on `SsrPool` (`tasks/7.4.md`, `regression_tdd`, `depends_on: [7.3]`)
- [x] **7.5** — Extract `SchemaEmitter`, split `procedures.py`, unify `ZodGenerator`/`ValibotGenerator`, and emit `routes.ts` (`tasks/7.5.md`, `characterization`, `depends_on: [7.2]`)
- [x] **7.6** — Harden `vertebra.build` (`_fallback.py`, lazy `__init__.py`, `_onefile_extract.py`, `pgo.py`) and complete `vertebra.cli` (`init.py`, `_launch.py`, flags) (`tasks/7.6.md`, `characterization`, `depends_on: [7.5]`)
- [x] **7.7** — Fix `@vertebra/client` `tsup.config.ts` entry points, `WasmClient` `.d.ts` return type, and CI `make ts-build` gate (`tasks/7.7.md`, `regression_tdd`, `depends_on: []`)
- [x] **7.8** — Consolidate `@vertebra/client` WebSocket transport, `BoundedKeySet`, `HttpClient`, framework `useQuery` hooks, `ArrowStreamClient`/`WasmClient`, and `LiveStateClient` (`tasks/7.8.md`, `regression_tdd`, `depends_on: [7.7]`)
- [x] **7.9** — Harden `Makefile`, `setup-env.sh`, `pyproject.toml` Ruff/Mypy coverage, `.gitignore`, CI `http3` feature gate, `benchmark.yml`, and OKF workflow/test-matrix docs (`tasks/7.9.md`, `static_validation`, `depends_on: [7.4, 7.6, 7.8]`)

## Verification

```bash
cd src/rs && cargo test -p vertebra-bundler
cd src/rs && cargo bench -p vertebra-bundler --bench ssr_render -- --test
make dev
uv run pytest src/py/tests/unit/framework/test_orchestrator.py src/py/tests/unit/framework/test_ssr.py src/py/tests/unit/codegen/ src/py/tests/unit/build/ src/py/tests/unit/cli/ -q
make ts-build
make ts-test
make lint-all
```

| TOOL-08 | 7.9 |
| TOOL-09 | 7.9 |
| TOOL-10 | 7.7 |
