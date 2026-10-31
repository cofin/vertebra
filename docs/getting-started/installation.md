# Installation

Vertebra supports **Python 3.12 through 3.15**, including free-threaded (**GIL-disabled**) `3.14t` and `3.15t` builds across Linux and macOS.

## Install the Python Package

Install `vertebra-core` into your project environment:

=== "uv"
    ```bash
    uv add vertebra-core
    ```
=== "pip"
    ```bash
    pip install vertebra-core
    ```
=== "pdm"
    ```bash
    pdm add vertebra-core
    ```
=== "poetry"
    ```bash
    poetry add vertebra-core
    ```

Verify the installation and `vtb` CLI entrypoint:

```bash
vtb --help
```

## Optional Extras

Vertebra keeps its base dependency footprint minimal (`msgspec` and `rich-click`) and exposes optional extras in `pyproject.toml`:

| Extra | Install Command | Included Capabilities |
| --- | --- | --- |
| `framework` | `uv add "vertebra-core[framework]"` | Optional schema adapters (`pydantic`, `attrs`, `orjson`) alongside native `msgspec.Struct` |
| `sqlspec` | `uv add "vertebra-core[sqlspec]"` | `SQLSpecPlugin` (`vertebra.extensions.sqlspec`), `sqlspec` query builder, and `aiosqlite` |
| `dev` | `uv add "vertebra-core[dev]"` | Development, lint, type-checking, test, and `maturin` build dependencies |

## Free-Threaded Python (`3.14t` / `3.15t`)

All three native Rust extensions (`vertebra._vertebra`, `vertebra._bundler`, `vertebra._storage`) run without the GIL and support multi-core execution inside a single OS process under `--topology embedded`:

```bash
uv python install 3.14t
uv venv --python 3.14t
uv sync
```

## TypeScript Client SDK (Native Wheel Distribution)

Vertebra ships the complete `@vertebra/client` TypeScript SDK directly inside the Python wheel (`vertebra-core`), completely eliminating external NPM registry dependencies:

- **Browser Applications**: Import directly from the running Vertebra server via native browser ESM without Node.js or `node_modules`:
  ```html
  <script type="module" src="/__vertebra/client/index.js"></script>
  ```
- **Vite & Full-Stack Projects**: Manage ESM dependencies using `[tool.vtb.js]` in `pyproject.toml` and vendor or link the client bundle into your workspace:
  ```bash
  vtb js install
  ```
- **Existing NPM/Vite Workspaces (Optional)**: If integrating with an existing external Node.js or Bun build pipeline, the compiled client distribution can also be linked into your project.
