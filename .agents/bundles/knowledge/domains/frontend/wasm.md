---
type: Concept
title: "WASM Plugin Host & Browser Bridge"
description: "Capability-scoped WASMPluginHost and WASMSandboxPolicy in Python (vertebra.wasm) paired with the @vertebra/client/wasm WASMClient."
status: stable
tags: [domains, frontend, wasm, sandbox, plugins]
generated:
  by: flow/0.26.0
  at: "2026-11-04T04:33:00Z"
verified:
  - by: agent:flow-setup
    at: "2026-11-04T04:33:00Z"
    commit: f741701
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
---

# WASM Plugin Host & Browser Bridge

<!-- truth: start -->
- `WASMPluginHost`, `WASMModuleConfig`, and `WASMSandboxPolicy` validate the 8-byte WebAssembly magic header (`\x00asm\x01\x00\x00\x00`) and enforce per-call fuel budgets (`WASMExecutionError`) without bundling an external Wasm engine.
- `@vertebra/client/wasm` exports `WASMClient` and `WASM_MAGIC_BYTES` for validated browser and worker module instantiation under bounded memory page limits.
<!-- truth: end -->

Vertebra provides a capability-scoped WebAssembly plugin host in Python ([`vertebra.wasm`](../../../../../src/py/vertebra/wasm/__init__.py)[^src-wasm-init-py]) and a matching browser/worker instantiation helper in TypeScript ([`@vertebra/client/wasm`](../../../../../src/ts/vertebra-client/src/wasm.ts)[^src-src-wasm-ts]).

## 1. Python Sandboxed Host (`vertebra.wasm`)

| Symbol | Responsibility |
|---|---|
| [`WASM_MAGIC_HEADER`](../../../../../src/py/vertebra/wasm/__init__.py) | Canonical 8-byte WebAssembly binary magic + version header (`b"\x00asm\x01\x00\x00\x00"`). |
| [`WASMSandboxPolicy`](../../../../../src/py/vertebra/wasm/__init__.py) | Frozen `msgspec.Struct(gc=False, kw_only=True)` specifying `max_memory_bytes` (default `64 MiB`), `max_fuel` (`10_000_000`), `epoch_timeout_ms` (`500`), `allow_network` (`False`), `allowed_preopen_dirs`, and `env_vars`. |
| [`WASMModuleConfig`](../../../../../src/py/vertebra/wasm/__init__.py) | Frozen `msgspec.Struct(gc=False, kw_only=True)` pairing `name: str`, `wasm_source: Path \| bytes`, and `policy: WASMSandboxPolicy`. |
| [`WASMPluginHost`](../../../../../src/py/vertebra/wasm/__init__.py) | Validates binaries via `validate_binary(wasm_bytes)`, registers guest modules and callable exports via `register_module(config, exports=...)`, and enforces `fuel_cost <= policy.max_fuel` on `call_export(module_name, export_name, *args, fuel_cost=100)`, raising [`WASMExecutionError`](../../../../../src/py/vertebra/wasm/__init__.py) when fuel is exceeded. |

## 2. TypeScript Client Loader (`@vertebra/client/wasm`)

- **[`WASMClient`](../../../../../src/ts/vertebra-client/src/wasm.ts) & [`WASM_MAGIC_BYTES`](../../../../../src/ts/vertebra-client/src/wasm.ts)**: Validates the 8-byte `0x00 0x61 0x73 0x6d 0x01 0x00 0x00 0x00` header via `WASMClient.isValidBinary(bytes)`, tracks `memoryLimitBytes` (`maxMemoryPages * 65536`, default `256` pages = `16 MiB`), and instantiates validated modules via `instantiateBytes(bytes)` with configured `WebAssembly.Imports`.
- **Packaging Note**: `WASMClient` and `WASM_MAGIC_BYTES` are re-exported from the `@vertebra/client` root entrypoint (`src/ts/vertebra-client/src/index.ts`) as well as `src/ts/vertebra-client/src/wasm.ts`.

Provenance sources: [^src-tests-wasm-test-ts].

[^src-wasm-init-py]: `src/py/vertebra/wasm/__init__.py` — Python implementation module (src/py/vertebra/wasm/__init__.py).
[^src-src-wasm-ts]: `src/ts/vertebra-client/src/wasm.ts` — TypeScript client module (src/ts/vertebra-client/src/wasm.ts).
[^src-tests-wasm-test-ts]: `src/ts/vertebra-client/tests/wasm.test.ts` — TypeScript client module (src/ts/vertebra-client/tests/wasm.test.ts).
