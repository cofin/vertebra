# Build, Codegen, Wasm & Supervision

Build orchestration (`vertebra.build`), schema and client code generation, reproducible standalone and onefile packaging, WebAssembly plugin sandboxes (`vertebra.wasm`), and multi-worker process supervision (`vertebra.supervisor`). See [Packaging & Deployment](../usage/packaging-and-deployment.md) for CLI and deployment workflows.

| Area | Purpose |
| --- | --- |
| **Build Orchestration** | Coordinate Rust extension builds, TypeScript codegen, and frontend asset bundling from `pyproject.toml`. |
| **Schema & Client Codegen** | Generate `.d.ts` types, Zod and Valibot validators, OpenAPI 3.1 JSON, `.proto` schemas, and typed RPC clients from Python routes. |
| **Standalone & Onefile Packaging** | Package an app into a relocatable directory or self-extracting binary with reproducible builds, independent of the package manager. |
| **Wasm Sandbox & Supervisor** | Run fuel- and memory-bounded WASI plugins and supervise multi-worker OS processes. |

## Build Configuration & Orchestration

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

::: vertebra.build._bundler.BundleConfig
    options:
      heading_level: 4
      members: false

::: vertebra.build._bundler.BundleDevConfig
    options:
      heading_level: 4
      members: false

::: vertebra.build._bundler.BundleResult
    options:
      heading_level: 4
      members: false

::: vertebra.build._bundler.BundleOutput
    options:
      heading_level: 4
      members: false

::: vertebra.build._bundler.Bundler
    options:
      heading_level: 3

## Schema & Client Codegen

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

## Standalone & Onefile Packaging

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

## WebAssembly Sandbox & Supervisor

::: vertebra.wasm.WASMSandboxPolicy
    options:
      heading_level: 4
      members: false

::: vertebra.wasm.WASMModuleConfig
    options:
      heading_level: 4
      members: false

::: vertebra.wasm.WASMExecutionError
    options:
      heading_level: 4
      members: false

::: vertebra.wasm.WASMPluginHost
    options:
      heading_level: 3

::: vertebra.supervisor.EmbeddedPythonRuntime
    options:
      heading_level: 4

::: vertebra.supervisor.run_supervisor
    options:
      heading_level: 4

::: vertebra.supervisor.build_server_command
    options:
      heading_level: 4

::: vertebra.supervisor.find_server_binary
    options:
      heading_level: 4

::: vertebra.supervisor.find_repo_root
    options:
      heading_level: 4

::: vertebra.supervisor.worker_main
    options:
      heading_level: 4
