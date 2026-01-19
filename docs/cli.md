# CLI & Distribution

Vertebra features a unified CLI and a "Zero-Build" installation philosophy.

## Installation

```bash
pip install vertebra
```

This single command installs the complete framework, including:
- Python library
- Rust extensions (pre-compiled wheels)
- Embedded Bun engine (platform-specific binary)

No external dependencies (like Node.js, Bun, or Rust) are required on the user's machine.

## CLI Commands

### `vertebra init`

Scaffolds a new project with best practices.

```bash
vertebra init myproject
```

- Interactive project creation
- Supports `uv`, `poetry`, `hatch`, `pdm`, `pip`
- Optional TypeScript frontend integration

### `vertebra build`

Builds the project for production.

```bash
vertebra build --one-file
```

- **`--one-file`**: Creates a single, standalone executable (~80-150MB).
- **Offline Capable**: Bundles all dependencies (Python runtime, libs, Bun engine).
- **Cross-Platform**: Targets Linux, macOS, and Windows.

### `vertebra serve`

Runs the application server.

```bash
vertebra serve app:app
```

- Starts the Rust Supervisor
- Launches the Bun Broker and Engine
- Spawns Python workers
- Manages shared memory lifecycle

## Build Profiles

`pyproject.toml` configuration:

```toml
[tool.vertebra]
profile = "modern"  # or "baseline" for older CPUs
```

- **Modern**: Optimized for AVX2 (2013+ CPUs).
- **Baseline**: Maximum compatibility (x86-64 v1).
- **Native**: Uses system tools (development).
