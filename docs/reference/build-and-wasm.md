# `vertebra.build`, `vertebra.wasm` & `vertebra.supervisor` — Build, Codegen, Wasm & Supervision

Build orchestration (`BuildConfig`, `BuildOrchestrator`), end-to-end TypeScript/Zod/Valibot/OpenAPI/Protobuf code generation, standalone and single-file binary packaging (`StandaloneBuilder`, `OnefileBuilder`), LLVM PGO pipelines (`RustPgoConfig`), WebAssembly plugin sandboxes (`WasmPluginHost`), and multi-worker process supervision (`run_supervisor`). See [Packaging & Deployment](../usage/packaging-and-deployment.md) for CLI and deployment workflows.

| Area | Classes / Functions | Purpose |
| --- | --- | --- |
| **Build Orchestration** | `JsConfig`, `BuildConfig`, `BuildOrchestrator` | Coordinate Rust extension builds, TypeScript codegen, and frontend asset bundling from `pyproject.toml` |
| **Schema & Client Codegen** | `CodegenConfig`, `TypeScriptGenerator`, `ZodGenerator`, `ValibotGenerator`, `OpenAPIGenerator`, `ProtobufGenerator`, `ProcedureClientGenerator` | Emit `.d.ts`, Zod/Valibot validators, OpenAPI 3.1 JSON, `.proto` schemas, and typed RPC clients from Python route signatures |
| **Standalone, Onefile & PGO** | `BuildTarget`, `StandaloneBuilder`, `OnefileBuilder`, `RustPgoConfig` | Bundle Python + Rust extensions into relocatable directories or self-extracting binaries and run two-phase PGO builds |
| **Wasm Sandbox & Supervisor** | `WasmSandboxPolicy`, `WasmModuleConfig`, `WasmExecutionError`, `WasmPluginHost`, `run_supervisor` | Execute fuel- and memory-bounded WASI plugins and supervise multi-worker OS processes |

## Build Configuration & Orchestration (`vertebra.build`)

::: vertebra.build._config.JsConfig
    options:
      heading_level: 4
      members: false

::: vertebra.build._config.BuildConfig
    options:
      heading_level: 4
      members: false

::: vertebra.build._orchestrator.BuildOrchestrator
    options:
      heading_level: 3

## TypeScript, Zod, Valibot, OpenAPI & Protobuf Codegen (`vertebra.build.codegen`)

::: vertebra.build.codegen.plugin.CodegenConfig
    options:
      heading_level: 4
      members: false

::: vertebra.build.codegen._typescript.TypeScriptGenerator
    options:
      heading_level: 3

::: vertebra.build.codegen.zod.ZodGenerator
    options:
      heading_level: 3

::: vertebra.build.codegen.valibot.ValibotGenerator
    options:
      heading_level: 3

::: vertebra.build.codegen.openapi.OpenAPIGenerator
    options:
      heading_level: 4
      members: false

::: vertebra.build.codegen.protobuf.ProtobufGenerator
    options:
      heading_level: 4
      members: false

::: vertebra.build.codegen.procedures.ProcedureClientGenerator
    options:
      heading_level: 4
      members: false

## Standalone, Onefile & PGO Packaging (`vertebra.build.packaging`)

::: vertebra.build.packaging.standalone.BuildTarget
    options:
      heading_level: 4
      members: false

::: vertebra.build.packaging.standalone.StandaloneBuilder
    options:
      heading_level: 3

::: vertebra.build.packaging.onefile.OnefileBuilder
    options:
      heading_level: 3

::: vertebra.build.packaging.pgo.RustPgoConfig
    options:
      heading_level: 4
      members: false

## WebAssembly Sandbox & Supervisor (`vertebra.wasm`, `vertebra.supervisor`)

::: vertebra.wasm.WasmSandboxPolicy
    options:
      heading_level: 4
      members: false

::: vertebra.wasm.WasmModuleConfig
    options:
      heading_level: 4
      members: false

::: vertebra.wasm.WasmExecutionError
    options:
      heading_level: 4
      members: false

::: vertebra.wasm.WasmPluginHost
    options:
      heading_level: 3

::: vertebra.supervisor._supervisor.run_supervisor
    options:
      heading_level: 4
