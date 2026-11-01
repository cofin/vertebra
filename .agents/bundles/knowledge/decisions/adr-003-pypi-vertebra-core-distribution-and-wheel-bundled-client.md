---
type: Concept
title: "ADR-003: PyPI vertebra-core Distribution and Wheel-Bundled TypeScript Client"
description: "Architectural decision to publish the PyPI distribution as vertebra-core while preserving the vertebra Python namespace and bundling @vertebra/client inside the wheel."
status: stable
tags: [decisions, adr, packaging, pypi, typescript, importmap]
sources:
  - resource: pyproject.toml
  - resource: src/ts/vertebra-client/package.json
  - resource: src/ts/vertebra-client/tsup.config.ts
  - resource: src/py/vertebra/build/_import_map.py
  - resource: src/py/vertebra/framework/dev.py
---

# ADR-003: PyPI `vertebra-core` Distribution and Wheel-Bundled TypeScript Client

<!-- truth: start -->
- **Decision**: Publish the Python distribution on PyPI as `vertebra-core` (`[project] name = "vertebra-core"`) while keeping the import namespace `vertebra` (`[tool.maturin] python-packages = ["vertebra"]`), and ship the compiled `@vertebra/client` ESM bundle and `.d.ts` declarations directly inside the Python wheel under `src/py/vertebra/server/_js/bundle/`.
- **Status**: Accepted (`v0.2.0`).
<!-- truth: end -->

## Context

Full-stack Python + TypeScript applications frequently suffer from version skew between the backend Python package and the frontend NPM client package, or require Node.js/`node_modules` toolchains in production container images just to resolve client SDK imports. Furthermore, on PyPI the distribution name `vertebra-core` is the canonical published coordinate while ergonomic Python code expects `import vertebra` and `vtb` CLI commands.

## Alternatives Considered

| Option | Trade-offs | Outcome |
| :--- | :--- | :--- |
| **Separate PyPI and NPM releases (`npm install @vertebra/client`)** | Familiar to JS-only projects, but introduces version drift between Python codegen and TypeScript runtime types and requires NPM registry publishing for every release. | Rejected. |
| **Rename Python import namespace to `vertebra_core`** | Matches PyPI project name literally, but degrades import ergonomics across every Python module, extension, and user guide. | Rejected. |
| **`vertebra-core` PyPI distribution + `vertebra` import namespace + wheel-bundled `@vertebra/client`** | Single atomic install (`uv add vertebra-core` / `pip install vertebra-core`) delivers the Rust runtime, Python `vertebra` package, and lockstep `@vertebra/client` ESM + `.d.ts` bundle served via `/__vertebra/client/*` and `.vertebra/importmap.json` (or Vite aliases via `get_vertebra_client_vite_aliases`). | **Selected**. |

## Consequences

- `pyproject.toml` configures `[project] name = "vertebra-core"` and `[tool.maturin] python-packages = ["vertebra"]`, while `importlib.metadata.version("vertebra-core")` resolves the installed package version.
- `tsup.config.ts` and `ImportMapGenerator.sync_wheel_client` copy all 17 `@vertebra/client` entrypoints and `.d.ts` declarations into `src/py/vertebra/server/_js/bundle/` prior to building wheels, and user documentation never instructs running `npm install @vertebra/client` or `bun add @vertebra/client`.
