---
type: Concept
title: "WASM Plugin Host & Browser Bridge"
description: "Capability-scoped WASMPluginHost and WASMSandboxPolicy in Python (vertebra.wasm) paired with the @vertebra/client/wasm WASMClient."
status: stable
tags:
  - domains
  - frontend
  - wasm
  - sandbox
  - plugins
generated:
  by: flow/0.26.0
  at: "2026-11-04T04:33:00Z"
verified:
  - by: agent:flow-executor
    at: "2026-11-17T08:02:00Z"
    commit: 5a833832
  - by: agent:flow-setup
    at: "2026-11-07T03:55:00Z"
    commit: 067fa901
sources:
  - id: src-wasm-init-py
    resource: src/py/vertebra/wasm/__init__.py
    title: "Python implementation module (src/py/vertebra/wasm/__init__.py)"
  - id: src-src-wasm-ts
    resource: src/ts/vertebra-client/src/wasm.ts
    title: "TypeScript client module (src/ts/vertebra-client/src/wasm.ts)"
  - id: src-tests-wasm-test-ts
    resource: src/ts/vertebra-client/tests/wasm.test.ts
    title: "TypeScript client module (src/ts/vertebra-client/tests/wasm.test.ts)"
  - id: src-pyproject-toml
    resource: pyproject.toml
    title: "Optional [wasmtime] dependency declaration in pyproject.toml"
---

# WASM Plugin Host & Browser Bridge

<!-- truth: start -->
- `WASMPluginHost`, `WASMModuleConfig`, and `WASMSandboxPolicy` validate the 8-byte WebAssembly magic header (`\x00asm\x01\x00\x00\x00`) and enforce per-call fuel, memory, and epoch-deadline budgets (`WASMExecutionError`), optionally executing guest modules via the `[wasmtime]` extra (`wasmtime>=25.0`, raising `MissingDependencyError` when absent).
- `_validate_policy_support` fails closed with `ImproperConfigurationError` at `WASMPluginHost()` creation and `register_module()` whenever `allow_network=True`, `allowed_preopen_dirs`, `env_vars`, or custom `epoch_timeout_ms != 500` cannot be enforced by the active runtime or `exports` callback mode.
- `@vertebra/client/wasm` exports `WASMClient` and `WASM_MAGIC_BYTES` for validated browser and worker module instantiation under bounded memory page limits.
<!-- truth: end -->

Vertebra provides a capability-scoped WebAssembly plugin host in Python ([`vertebra.wasm`](../../../../../src/py/vertebra/wasm/__init__.py)[^src-wasm-init-py], optional `[wasmtime]` extra in [`pyproject.toml`](../../../../../pyproject.toml)[^src-pyproject-toml]) and a matching browser/worker instantiation helper in TypeScript ([`@vertebra/client/wasm`](../../../../../src/ts/vertebra-client/src/wasm.ts)[^src-src-wasm-ts]).

## 1. Python Sandboxed Host (`vertebra.wasm`)

| Symbol | Responsibility |
|---|---|
| [`WASM_MAGIC_HEADER`](../../../../../src/py/vertebra/wasm/__init__.py) | Canonical 8-byte WebAssembly binary magic + version header (`b"\x00asm\x01\x00\x00\x00"`). |
| [`WASMSandboxPolicy`](../../../../../src/py/vertebra/wasm/__init__.py) | Frozen `msgspec.Struct(gc=False, kw_only=True)` specifying `max_memory_bytes` (default `64 MiB`), `max_fuel` (`10_000_000`), `epoch_timeout_ms` (`500`), `allow_network` (`False`), `allowed_preopen_dirs`, and `env_vars`; rejects non-positive limits with `ValueError`. |
| [`WASMModuleConfig`](../../../../../src/py/vertebra/wasm/__init__.py) | Frozen `msgspec.Struct(gc=False, kw_only=True)` pairing `name: str`, `wasm_source: str \| Path \| bytes`, and `policy: WASMSandboxPolicy`. |
| [`WASMPluginHost`](../../../../../src/py/vertebra/wasm/__init__.py) | Validates binaries via `validate_binary(wasm_bytes)`, enforces fail-closed policy support via `_validate_policy_support` on `__init__` and `register_module(config, exports=...)`, raises `MissingDependencyError("wasmtime", extra="wasmtime")` for guest Wasm registration when `[wasmtime]` is not installed, enforces fuel, memory limits, WASI capabilities (`WasiConfig`), and `threading.Timer` epoch deadlines on `call_export`, and provides zero-copy-buffered `call_json`, `call_msgpack`, and `dispatch_http` (`WASMHttpEnvelope` / `WASMHttpResponse`). |

- **Fail-closed sandbox enforcement**: `_validate_policy_support(policy, exports_mode=...)` raises `ImproperConfigurationError` at host creation (`WASMPluginHost(policy)` when `wasmtime` is present) and at `register_module(config, exports=...)` whenever `allow_network=True` (requires `WasiConfig.inherit_network`, `Linker.define_wasi`, `Store.set_wasi`), `allowed_preopen_dirs` (requires `WasiConfig.preopen_dir`), `env_vars` (requires `WasiConfig.set_env`), or `epoch_timeout_ms != 500` (requires `Config.epoch_interruption`, `Engine.increment_epoch`, `Store.set_epoch_deadline`) are enabled without runtime support or under Python `exports` callback mode (`exports_mode=True`).

## 2. TypeScript Client Loader (`@vertebra/client/wasm`)

- **[`WASMClient`](../../../../../src/ts/vertebra-client/src/wasm.ts) & [`WASM_MAGIC_BYTES`](../../../../../src/ts/vertebra-client/src/wasm.ts)**: Validates the 8-byte `0x00 0x61 0x73 0x6d 0x01 0x00 0x00 0x00` header via `WASMClient.isValidBinary(bytes)`, tracks `memoryLimitBytes` (`maxMemoryPages * 65536`, default `256` pages = `16 MiB`), and instantiates validated modules via `instantiateBytes(bytes)` with configured `WebAssembly.Imports`.
- **Packaging Note**: `WASMClient` and `WASM_MAGIC_BYTES` are re-exported from the `@vertebra/client` root entrypoint (`src/ts/vertebra-client/src/index.ts`) as well as `src/ts/vertebra-client/src/wasm.ts`.

Provenance sources: [^src-tests-wasm-test-ts].

[^src-wasm-init-py]: `src/py/vertebra/wasm/__init__.py` — Python implementation module (src/py/vertebra/wasm/__init__.py).
[^src-src-wasm-ts]: `src/ts/vertebra-client/src/wasm.ts` — TypeScript client module (src/ts/vertebra-client/src/wasm.ts).
[^src-tests-wasm-test-ts]: `src/ts/vertebra-client/tests/wasm.test.ts` — TypeScript client module (src/ts/vertebra-client/tests/wasm.test.ts).
[^src-pyproject-toml]: `pyproject.toml` — Optional `[wasmtime]` dependency declaration in `pyproject.toml`.
