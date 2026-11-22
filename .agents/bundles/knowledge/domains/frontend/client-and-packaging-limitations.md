---
type: Concept
title: "Client, Bundler, WASM, and Packaging Limitations"
description: "Operational boundaries across single-entry _bundler transforms, OnefileBuilder zstd extras, WASI preview1 allow_network enforcement, fail-closed WebMCP confirmation, BuildOrchestrator dev watch roots, and release.yaml git source pin blocks."
status: stable
tags:
  - domains
  - frontend
  - bundler
  - packaging
  - wasm
  - webmcp
  - limitations
generated:
  by: flow/0.30.1
  at: "2026-11-17T08:02:00Z"
verified:
  - by: agent:flow-executor
    at: "2026-11-17T08:02:00Z"
    commit: 5a833832
sources:
  - id: bundler-rs
    resource: src/rs/vertebra-bundler/src/bundler.rs
    title: "Single-entry Oxc Bundler.build implementation"
  - id: build-orchestrator-py
    resource: src/py/vertebra/build/_orchestrator.py
    title: "BuildOrchestrator native dev watch_dirs configuration"
  - id: onefile-py
    resource: src/py/vertebra/build/packaging/onefile.py
    title: "OnefileBuilder zlib default and optional zstandard extra"
  - id: cli-build-py
    resource: src/py/vertebra/cli/build.py
    title: "vtb build --compression option defaulting to zlib"
  - id: wasm-init-py
    resource: src/py/vertebra/wasm/__init__.py
    title: "WASMSandboxPolicy fail-closed _validate_policy_support enforcement"
  - id: webmcp-ts
    resource: src/ts/vertebra-client/src/webmcp.ts
    title: "WebMCP fail-closed requiresConfirmation execution gate"
  - id: release-yaml
    resource: .github/workflows/release.yaml
    title: "Release workflow validate_release_manifest git-pin blocker"
  - id: pyproject-toml
    resource: pyproject.toml
    title: "Root pyproject.toml [tool.uv.sources] and [zstandard] extra"
---

# Client, Bundler, WASM, and Packaging Limitations

<!-- truth: start -->
- `vertebra._bundler` (`Bundler.build`) runs single-entry Oxc TypeScript/JSX transforms into `<outdir>/<entry_stem>.js` without resolving or bundling imports, emitting source maps, or generating asset manifests; multi-file bundling requires `bundler="vite"` [^bundler-rs].
- `BuildOrchestrator._run_native(dev=True)` initializes `DevConfig` with `watch_dirs=[<project_dir>/src]` and default `extensions=["ts", "tsx", "js", "jsx"]`, so changes outside `project_dir/src` or in Python modules do not trigger native bundler rebuilds directly [^build-orchestrator-py].
- `OnefileBuilder` and `vtb build --onefile` default to stdlib `zlib` compression (`compression="zlib"`); selecting `--compression zstd` resolves `zstandard` lazily via `ensure_module` and raises `MissingDependencyError` unless the `[zstandard]` extra is installed [^onefile-py] [^cli-build-py].
- `_validate_policy_support` raises `ImproperConfigurationError` for `WASMSandboxPolicy(allow_network=True)` under WASI preview1 `wasmtime` runtimes lacking `WasiConfig.inherit_network`, when `Linker.define_wasi`/`Store.set_wasi` are absent, or in `exports` callback mode [^wasm-init-py].
- `registerWebMCPTools` fails closed for tools marked `requiresConfirmation: true`: when `onConfirm` is omitted and `globalThis.confirm` is unavailable (headless, SSR, or worker environments), `execute` throws a cancellation error instead of invoking the tool [^webmcp-ts].
- `.github/workflows/release.yaml` `validate_release_manifest` blocks `build_wheels` and `build_sdist` if `[tool.uv.sources]` in `pyproject.toml` contains any `git`, `rev`, `branch`, or `tag` source pins; `pyproject.toml` currently references only local workspace wheel paths alongside PyPI version specifiers (`sqlspec>=0.73.0`, `advanced-alchemy>=1.13.0b3`) [^release-yaml] [^pyproject-toml].
<!-- truth: end -->

## Operational Boundaries

- **Single-Entry Native Bundler**: [`Bundler.build`](../../../../../src/rs/vertebra-bundler/src/bundler.rs)[^bundler-rs] reads only `config.entry`, transforms TypeScript and JSX via `oxc_parser` and `oxc_transformer`, and writes `<outdir>/<entry_stem>.js`. Even though `BundleConfig` exposes `sourcemap` and `target` fields, `build_internal` emits neither source maps nor content-hashed asset manifests and never follows `import` graphs; applications needing multi-module bundling must select `bundler = "vite"`.
- **Native Dev Watch Scope**: [`BuildOrchestrator._run_native`](../../../../../src/py/vertebra/build/_orchestrator.py)[^build-orchestrator-py] restricts `DevConfig.watch_dirs` to `[str(self.project_dir / "src")]`. Frontend files in sibling directories (such as `resources/`) or Python route modules require Vite dev mode or `CodegenWatcher` to observe changes.
- **Onefile `zstd` Extra Requirement**: [`OnefileBuilder`](../../../../../src/py/vertebra/build/packaging/onefile.py)[^onefile-py] and [`vtb build`](../../../../../src/py/vertebra/cli/build.py)[^cli-build-py] default to stdlib `zlib` (`b"ZLIB"`, level 9) so single-binary builds work in base installs without compiled compression wheels. Passing `--compression zstd` (`compression="zstd"`) calls `ensure_module("zstandard", package="zstandard", extra="zstandard")` and fails with `MissingDependencyError` when `[zstandard]` is not installed.
- **WASI Preview1 `allow_network=True` Rejection**: [`_validate_policy_support`](../../../../../src/py/vertebra/wasm/__init__.py)[^wasm-init-py] enforces fail-closed capability checks at `WASMPluginHost` construction and `register_module` time. Because `wasmtime` WASI preview1 `WasiConfig` omits `inherit_network` (and Python `exports` callbacks have no WASI sandbox), `WASMSandboxPolicy(allow_network=True)` raises `ImproperConfigurationError`.
- **WebMCP Headless Confirmation Gate**: [`registerWebMCPTools`](../../../../../src/ts/vertebra-client/src/webmcp.ts)[^webmcp-ts] gates tools with `requiresConfirmation: true` through `resolveConfirmation`. Without an explicit `onConfirm: WebMCPConfirmationHandler` or a browser `globalThis.confirm` function, execution fails closed with `Error("WebMCP tool call '<name>' cancelled: user confirmation declined")`.
- **Release Git-Pin Check**: [`release.yaml`](../../../../../.github/workflows/release.yaml)[^release-yaml] runs `validate_release_manifest` before wheel and sdist builds, rejecting any `[tool.uv.sources]` git pin in [`pyproject.toml`](../../../../../pyproject.toml)[^pyproject-toml]. Both `sqlspec` (`>=0.73.0`) and `advanced-alchemy` (`>=1.13.0b3`) resolve from published PyPI specifiers with zero upstream git pins.

Related domain capabilities live in [Frontend Bundler, Codegen & Client SDK Domain](bundler-and-client-sdk.md), [Standalone & Onefile Packaging, Wheel Matrix, and PGO Tooling](packaging-and-pgo.md), and [WASM Plugin Host & Browser Bridge](wasm.md).

[^bundler-rs]: `src/rs/vertebra-bundler/src/bundler.rs` — Single-entry Oxc `Bundler.build` implementation.
[^build-orchestrator-py]: `src/py/vertebra/build/_orchestrator.py` — `BuildOrchestrator` native dev `watch_dirs` configuration.
[^onefile-py]: `src/py/vertebra/build/packaging/onefile.py` — `OnefileBuilder` `zlib` default and optional `zstandard` extra.
[^cli-build-py]: `src/py/vertebra/cli/build.py` — `vtb build --compression` option defaulting to `zlib`.
[^wasm-init-py]: `src/py/vertebra/wasm/__init__.py` — `WASMSandboxPolicy` fail-closed `_validate_policy_support` enforcement.
[^webmcp-ts]: `src/ts/vertebra-client/src/webmcp.ts` — WebMCP fail-closed `requiresConfirmation` execution gate.
[^release-yaml]: `.github/workflows/release.yaml` — Release workflow `validate_release_manifest` git-pin blocker.
[^pyproject-toml]: `pyproject.toml` — Root `pyproject.toml` `[tool.uv.sources]` and `[zstandard]` extra.
