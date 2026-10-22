---
type: Concept
title: "WASM Plugin Host & Browser Bridge"
description: "Capability-scoped WasmPluginHost and WasmSandboxPolicy in Python (vertebra.wasm) paired with the @vertebra/client/wasm WasmClient."
status: stable
tags: [domains, frontend, wasm, sandbox, plugins]
sources:
  - resource: src/py/vertebra/wasm/__init__.py
  - resource: src/ts/vertebra-client/src/wasm.ts
  - resource: src/ts/vertebra-client/tests/wasm.test.ts
---

# WASM Plugin Host & Browser Bridge

Vertebra provides a capability-scoped WebAssembly plugin host in Python ([`vertebra.wasm`](../../../../../src/py/vertebra/wasm/__init__.py)) and a matching browser/worker instantiation helper in TypeScript ([`@vertebra/client/wasm`](../../../../../src/ts/vertebra-client/src/wasm.ts)).

## 1. Python Sandboxed Host (`vertebra.wasm`)

| Symbol | Responsibility |
|---|---|
| [`WASM_MAGIC_HEADER`](../../../../../src/py/vertebra/wasm/__init__.py) | Canonical 8-byte WebAssembly binary magic + version header (`b"\x00asm\x01\x00\x00\x00"`). |
| [`WasmSandboxPolicy`](../../../../../src/py/vertebra/wasm/__init__.py) | Frozen `msgspec.Struct(gc=False, kw_only=True)` specifying `max_memory_bytes` (default `64 MiB`), `max_fuel` (`10_000_000`), `epoch_timeout_ms` (`500`), `allow_network` (`False`), `allowed_preopen_dirs`, and `env_vars`. |
| [`WasmModuleConfig`](../../../../../src/py/vertebra/wasm/__init__.py) | Frozen `msgspec.Struct(gc=False, kw_only=True)` pairing `name: str`, `wasm_source: Path \| bytes`, and `policy: WasmSandboxPolicy`. |
| [`WasmPluginHost`](../../../../../src/py/vertebra/wasm/__init__.py) | Validates binaries via `validate_binary(wasm_bytes)`, registers guest modules and callable exports via `register_module(config, exports=...)`, and enforces `fuel_cost <= policy.max_fuel` on `call_export(module_name, export_name, *args, fuel_cost=100)`, raising [`WasmExecutionError`](../../../../../src/py/vertebra/wasm/__init__.py) when fuel is exceeded. |

## 2. TypeScript Client Loader (`@vertebra/client/wasm`)

- **[`WasmClient`](../../../../../src/ts/vertebra-client/src/wasm.ts) & [`WASM_MAGIC_BYTES`](../../../../../src/ts/vertebra-client/src/wasm.ts)**: Validates the 8-byte `0x00 0x61 0x73 0x6d 0x01 0x00 0x00 0x00` header via `WasmClient.isValidBinary(bytes)`, tracks `memoryLimitBytes` (`maxMemoryPages * 65536`, default `256` pages = `16 MiB`), and instantiates validated modules via `instantiateBytes(bytes)` with configured `WebAssembly.Imports`.
- **Packaging Note**: `WasmClient` and `WASM_MAGIC_BYTES` are re-exported from the `@vertebra/client` root entrypoint (`src/ts/vertebra-client/src/index.ts`) as well as `src/ts/vertebra-client/src/wasm.ts`.
