# Installation

Vertebra supports **Python 3.12 through 3.15**, including free-threaded (**GIL-disabled**) `3.14t` and `3.15t` builds across Linux and macOS.

## Install the Python Package

Install `vertebra-core` into your project environment with `uv`:

```bash
uv add vertebra-core
```

Verify the installation and `vtb` CLI entrypoint:

```bash
uv run vtb --help
```

## Optional Extras

Vertebra keeps its base dependency footprint minimal (`msgspec` and `rich-click`) and exposes optional extras for integrations:

| Extra | Install Command | Included Capabilities |
| --- | --- | --- |
| `litestar` | `uv add "vertebra-core[litestar]"` | `VertebraLitestarPlugin`, ASGI/RSGI mounting, and Litestar lifespan coordination |
| `otel` | `uv add "vertebra-core[otel]"` | OpenTelemetry trace and metrics exporters (`vertebra.telemetry`) |
| `adk` | `uv add "vertebra-core[adk]"` | Google ADK agent runtime integration (`vertebra.agent.adk`) |
| `pydantic` | `uv add "vertebra-core[pydantic]"` | Pydantic v2 schema adaptation alongside native `msgspec.Struct` |

## Free-Threaded Python (`3.14t` / `3.15t`)

All three native PyO3 extensions (`vertebra._vertebra`, `vertebra._bundler`, `vertebra._storage`) declare `gil_used = false` and support true multi-core execution inside a single OS process under `--topology embedded`:

```bash
uv python install 3.14t
uv venv --python 3.14t
uv sync
```

## Install the TypeScript Client SDK

For full-stack applications using Vite, React, Vue, Svelte, Angular, or HTMX, install `@vertebra/client`:

```bash
bun add @vertebra/client
```

## Local Contributor Setup

When developing on the Vertebra repository itself (Rust 2024 + Python + TypeScript):

```bash
make install   # Set up .venv, install Python/Bun deps, and compile debug PyO3 extensions
make dev       # Rebuild Rust crates and install debug PyO3 extensions (_vertebra, _bundler, _storage)
make test      # Run Rust, Python, and TypeScript test suites
```
