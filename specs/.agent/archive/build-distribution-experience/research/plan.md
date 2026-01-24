# Research: Build System & Distribution Experience

## Executive Summary

This research explores native integration approaches for Vertebra's build and distribution system. The goal is to provide a seamless developer experience where `pip install vertebra` delivers a complete, ready-to-run framework without requiring users to build anything themselves.

Key findings:
1. **Native Python embedding** via `pyembed` crate (from PyOxidizer) instead of external PyApp patching
2. **Bun programmatic builds** via `Bun.build()` API with `compile: true` for standalone executables
3. **Rust-based supervisor** to avoid GIL issues and support custom event loops (uvloop, rloop)
4. **Cross-platform distribution** through platform-specific wheels with embedded binaries

---

## 1. Python Embedding: pyembed vs PyApp

### 1.1 Current Approach: External PyApp

The current design (from PRD-8) uses PyApp as an external tool:
- Clone PyApp repository
- Patch `src/app.rs` to customize installation directory
- Add `dirs` crate dependency
- Build with cargo

**Problems:**
- Fragile dependency on PyApp's internal structure
- Breaks when PyApp updates its source code
- Requires network access to clone
- Complex CI/CD pipeline

### 1.2 Native Approach: pyembed Crate

The `pyembed` crate from the PyOxidizer project provides a proper library interface for embedding Python in Rust applications.

**Key Types:**
```rust
use pyembed::{MainPythonInterpreter, OxidizedPythonInterpreterConfig};

// Configure the embedded interpreter
let config = OxidizedPythonInterpreterConfig::default();

// Create and run the interpreter
let interp = MainPythonInterpreter::new(config)?;
interp.with_gil(|py| {
    // Run Python code
    py.run("import vertebra; vertebra.main()", None, None)
})?;
```

**Advantages:**
- Stable API (crate versioning)
- Zero-copy module loading from memory
- Custom meta path importer for embedded resources
- Works with python-build-standalone distributions
- Memory allocator options (jemalloc, mimalloc, snmalloc)

**Integration with vertebra-py:**
```toml
# src/rs/vertebra-py/Cargo.toml
[dependencies]
pyo3 = { version = "0.22", features = ["auto-initialize"] }
pyembed = { version = "0.26", optional = true }

[features]
embedded = ["pyembed"]
```

### 1.3 python-build-standalone Distributions

The [python-build-standalone](https://github.com/indygreg/python-build-standalone/releases) project (now maintained by Astral) provides pre-built, redistributable Python distributions for multiple platforms:

| Platform | Architectures | Notes |
|----------|---------------|-------|
| Linux (glibc) | x86_64, aarch64 | glibc 2.17+ |
| Linux (musl) | x86_64, aarch64 | Fully static |
| macOS | x86_64, arm64 | 10.9+ |
| Windows | x86_64 | MSVC runtime |

These distributions include:
- Python interpreter and stdlib
- OpenSSL 3.0.x
- SQLite, bzip2, xz, zlib
- pip, setuptools (optional)

**Integration approach:**
1. Download platform-specific distribution at build time
2. Pre-install vertebra wheel into `site-packages`
3. Package as embedded resource in the Rust binary
4. Use `pyembed` to initialize interpreter with embedded resources

---

## 2. Bun Compilation: Programmatic API

### 2.1 Bun.build() API

Bun 1.3+ provides a programmatic API for building standalone executables:

```typescript
import { build } from "bun";

const result = await build({
  entrypoints: ["./src/ts/src/server/index.ts"],
  compile: true,
  outfile: "vertebra-engine",
  target: "bun-linux-x64",
  minify: true,
  bytecode: true,  // 2x faster startup
});
```

**CLI equivalent:**
```bash
bun build ./src/ts/src/server/index.ts \
  --compile \
  --outfile vertebra-engine \
  --target bun-linux-x64 \
  --minify \
  --bytecode
```

### 2.2 Cross-Platform Targets

| Target | OS | Architecture |
|--------|-----|--------------|
| `bun-linux-x64` | Linux | x86_64 |
| `bun-linux-arm64` | Linux | aarch64 |
| `bun-darwin-x64` | macOS | x86_64 |
| `bun-darwin-arm64` | macOS | arm64 (Apple Silicon) |
| `bun-windows-x64` | Windows | x86_64 |

**Baseline variants** (for older CPUs pre-2013):
- `bun-linux-x64-baseline`
- `bun-darwin-x64-baseline`
- `bun-windows-x64-baseline`

### 2.3 Asset Embedding

Bun can embed static files and databases directly:

```typescript
// Embed SQLite database
import db from "./data.db" with { type: "sqlite", embed: "true" };

// Embed static files
import config from "./config.json" with { type: "file" };
```

This is useful for embedding:
- Default broker configuration
- Static assets for MCP server
- Pre-compiled TypeScript definitions

### 2.4 Build Integration Strategy

The `vertebra build` command should:

1. **Compile Bun engine** using `Bun.build()` programmatically
2. **Embed the binary** in the Python wheel's `bin/` directory
3. **Or spawn as subprocess** if building from TypeScript source

```typescript
// tools/build-engine.ts
export async function buildEngine(target: string, outdir: string) {
  const result = await Bun.build({
    entrypoints: ["./src/ts/src/server/index.ts"],
    compile: true,
    outfile: `${outdir}/vertebra-engine`,
    target,
    minify: true,
    bytecode: true,
    define: {
      "process.env.NODE_ENV": '"production"',
    },
  });

  if (!result.success) {
    throw new Error(`Build failed: ${result.logs.join("\n")}`);
  }

  return result.outputs[0].path;
}
```

---

## 3. Supervisor Architecture: Rust vs Python

### 3.1 Problems with Python Supervisor

The current `supervisor.py` has several issues:

1. **GIL contention**: `subprocess.Popen.wait()` holds the GIL
2. **Signal handling**: Python signal handlers are limited
3. **Fork semantics**: Unclear pre-fork vs post-fork behavior
4. **Event loop compatibility**: Conflicts with uvloop/rloop

### 3.2 Rust Supervisor Design

Move supervision logic to `vertebra-py` Rust extension:

```rust
// src/rs/vertebra-py/src/supervisor.rs
use pyo3::prelude::*;
use std::process::{Child, Command};
use std::sync::Arc;
use tokio::sync::Notify;

#[pyclass]
pub struct Supervisor {
    bun_process: Option<Child>,
    shutdown: Arc<Notify>,
}

#[pymethods]
impl Supervisor {
    #[new]
    fn new() -> Self {
        Self {
            bun_process: None,
            shutdown: Arc::new(Notify::new()),
        }
    }

    fn start(&mut self, py: Python, engine_path: &str, config_path: &str) -> PyResult<()> {
        // Release GIL during blocking operations
        py.allow_threads(|| {
            self.bun_process = Some(
                Command::new(engine_path)
                    .arg("--config")
                    .arg(config_path)
                    .spawn()
                    .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?
            );
            Ok(())
        })
    }

    fn wait_for_shutdown(&self, py: Python) -> PyResult<i32> {
        py.allow_threads(|| {
            // Block on signal, return exit code
            // Rust handles SIGTERM/SIGINT properly
        })
    }

    fn stop(&mut self) -> PyResult<()> {
        if let Some(ref mut child) = self.bun_process {
            child.kill()?;
        }
        Ok(())
    }
}
```

### 3.3 Event Loop Compatibility

The Rust supervisor avoids event loop conflicts by:

1. **No Python async**: Supervisor runs in Rust threads, not Python coroutines
2. **GIL release**: All blocking operations use `py.allow_threads()`
3. **Signal handling**: Rust handles signals directly via `signal-hook` crate
4. **IPC is synchronous**: Shared-memory ring buffers are inherently sync

**uvloop/rloop users** can use their preferred event loop policy:

```python
import uvloop
uvloop.install()

# Supervisor starts in Rust thread, doesn't interfere
from vertebra import Vertebra
app = Vertebra()
```

### 3.4 Worker Forking Strategy

For ASGI workers, the correct forking strategy is:

1. **Pre-fork** (before workers): Start Rust supervisor, shared-memory regions
2. **Post-fork** (in each worker): Attach to existing shared-memory rings

```python
# vertebra/server/worker.py
def worker_main():
    # Attach to existing shared memory (created by supervisor)
    from vertebra._vertebra import Channel
    channel = Channel.open_named("/vertebra-broker", ...)

    # Now run ASGI app
    import asyncio
    asyncio.run(serve_asgi(channel))
```

The shared-memory ring buffer design is **fork-safe** because:
- Each side has its own atomic read/write counters
- No file descriptors need to be inherited
- Memory-mapped regions are preserved across fork

---

## 4. Project Scaffolding

### 4.1 Python Users: `vertebra init`

```bash
# After pip install vertebra
vertebra init myproject --package-manager uv

# Interactive mode
vertebra init
# ? Project name: myproject
# ? Package manager: (uv) / poetry / hatch / pdm / pip
# ? Include TypeScript client: (yes) / no
# ? Database: (postgres) / mysql / sqlite / none
```

**Generated structure:**
```
myproject/
├── pyproject.toml          # With vertebra dependency
├── vertebra.toml           # Broker configuration
├── src/
│   └── myproject/
│       ├── __init__.py
│       ├── app.py          # Vertebra application
│       └── procedures/     # RPC-like procedures
│           └── __init__.py
└── client/                 # Optional TypeScript client
    ├── package.json
    └── src/
        └── index.ts
```

### 4.2 TypeScript/Bun Users: `bun create vertebra`

```bash
bunx create-vertebra myproject
# or
bun create vertebra myproject
```

This creates a Bun-first project that can call Python procedures:

```
myproject/
├── package.json
├── bunfig.toml
├── src/
│   ├── index.ts           # Bun server
│   └── procedures/        # TypeScript procedures
├── python/                # Optional Python backend
│   ├── pyproject.toml
│   └── src/
│       └── myproject/
│           └── app.py
└── vertebra.toml
```

### 4.3 Package Manager Integrations

**uv (recommended):**
```bash
vertebra init myproject --package-manager uv
# Generates: uv.lock, uses `uv run vertebra serve`
```

**poetry:**
```bash
vertebra init myproject --package-manager poetry
# Generates: poetry.lock, uses `poetry run vertebra serve`
```

**hatch:**
```bash
vertebra init myproject --package-manager hatch
# Generates: hatch.toml, uses `hatch run vertebra serve`
```

**pdm:**
```bash
vertebra init myproject --package-manager pdm
# Generates: pdm.lock, uses `pdm run vertebra serve`
```

**pip (basic):**
```bash
vertebra init myproject --package-manager pip
# Generates: requirements.txt, uses `python -m vertebra serve`
```

---

## 5. Unified Build Command

### 5.1 Command Interface

```bash
# Development build
vertebra build

# Production single-file
vertebra build --one-file --output dist/myapp

# Cross-platform
vertebra build --one-file --target linux-x64 --output dist/myapp-linux
vertebra build --one-file --target darwin-arm64 --output dist/myapp-macos
vertebra build --one-file --target windows-x64 --output dist/myapp.exe
```

### 5.2 Build Pipeline

```
                    vertebra build
                          │
            ┌─────────────┼─────────────┐
            ▼             ▼             ▼
    [Type Generation]  [Bun Engine]  [Python Wheel]
            │             │             │
            ▼             ▼             ▼
    client/types.ts   bin/engine    dist/*.whl
            │             │             │
            └─────────────┼─────────────┘
                          ▼
                   [Single Binary]
                    (via pyembed)
                          │
                          ▼
                    dist/myapp
```

### 5.3 Programmatic Build API

```python
from vertebra.build import Builder

builder = Builder(config_path="vertebra.toml")

# Individual steps
builder.generate_types()     # TypeScript types from Python
builder.compile_engine()     # Bun engine binary
builder.build_wheel()        # Python wheel

# Or all-in-one
builder.build_one_file(
    output="dist/myapp",
    target="linux-x64",
)
```

### 5.4 Bun Integration

For TypeScript users, `bun build` should also work:

```bash
# In a TypeScript-first project
bun build --compile --outfile dist/myapp
```

This should:
1. Compile TypeScript server code
2. Embed any Python procedures via IPC bridge
3. Include the broker configuration

---

## 6. Distribution Strategy

### 6.1 PyPI: Platform-Specific Wheels

Build wheels for each platform with embedded binaries:

```
vertebra-1.0.0-cp312-cp312-manylinux_2_17_x86_64.whl
vertebra-1.0.0-cp312-cp312-manylinux_2_17_aarch64.whl
vertebra-1.0.0-cp312-cp312-macosx_10_12_x86_64.whl
vertebra-1.0.0-cp312-cp312-macosx_11_0_arm64.whl
vertebra-1.0.0-cp312-cp312-win_amd64.whl
```

**Wheel contents:**
```
vertebra/
├── __init__.py
├── cli.py
├── app.py
├── _vertebra.cpython-312-*.so  # Rust extension
└── bin/
    └── vertebra-engine         # Compiled Bun binary
```

### 6.2 npm: Optional TypeScript Package

For TypeScript-first users:

```bash
npm install vertebra
# or
bun add vertebra
```

This provides:
- TypeScript types for procedures
- Client SDK for calling Python procedures
- Bun server utilities

### 6.3 crates.io: Internal Only

The Rust crates are internal implementation details:
- `vertebra-core`: IPC primitives
- `vertebra-py`: Python bindings

Users don't need to interact with these directly.

### 6.4 Installation Locations

**Default locations:**
- Linux/macOS: `~/.vertebra/`
- Windows: `%LOCALAPPDATA%\vertebra\`

**Contents:**
```
~/.vertebra/
├── bin/
│   └── vertebra           # Main CLI (if installed globally)
├── cache/
│   └── engines/           # Cached Bun engine binaries
└── config/
    └── vertebra.toml      # Global configuration
```

---

## 7. CI/CD Pipeline

### 7.1 Build Matrix

```yaml
strategy:
  matrix:
    include:
      - os: ubuntu-latest
        target: x86_64-unknown-linux-gnu
        python: manylinux_2_17_x86_64
        bun: bun-linux-x64
      - os: ubuntu-latest
        target: aarch64-unknown-linux-gnu
        python: manylinux_2_17_aarch64
        bun: bun-linux-arm64
      - os: macos-latest
        target: aarch64-apple-darwin
        python: macosx_11_0_arm64
        bun: bun-darwin-arm64
      - os: windows-latest
        target: x86_64-pc-windows-msvc
        python: win_amd64
        bun: bun-windows-x64
```

### 7.2 Build Steps

1. **Compile Bun engine**: `bun build --compile --target ${{ matrix.bun }}`
2. **Build Rust extension**: `maturin build --target ${{ matrix.target }}`
3. **Create wheel**: Include Bun binary in `bin/`
4. **Test offline**: Run in isolated container without network
5. **Upload to PyPI**: `twine upload dist/*.whl`

---

## 8. Recommendations

### 8.1 Immediate Actions

1. **Add pyembed dependency** to `vertebra-py` Cargo.toml
2. **Implement Rust supervisor** with GIL-releasing methods
3. **Create build-engine.ts** for programmatic Bun compilation
4. **Update CI/CD** for cross-platform wheel builds

### 8.2 Architecture Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Python embedding | pyembed crate | Stable API, zero-copy loading |
| Bun compilation | Bun.build() API | Native, no subprocess needed |
| Supervisor language | Rust | GIL-free, signal handling |
| Distribution | Platform wheels | Single `pip install` UX |
| Install location | `~/.vertebra/` | User-writable, no sudo |

### 8.3 Open Questions

1. **Wheel size**: With embedded Bun (~50MB compressed), wheels will be large. Is this acceptable?
2. **Python version matrix**: Support 3.12+ only, or include 3.11?
3. **Windows support**: Priority for initial release?
4. **Offline mode**: Should `vertebra build --one-file` work fully offline?

---

## 9. Build Optimization & Legacy Compatibility

### 9.1 Optimization Tiers

| Tier | Target | Use Case | Trade-offs |
|------|--------|----------|------------|
| **Modern** (default) | 2013+ CPUs | Best performance | SSE4.2, AVX, AVX2 |
| **Baseline** | Pre-2013 CPUs | Maximum compatibility | Slower, larger |
| **Native** | Build machine | Development | Non-portable |

### 9.2 Bun Optimization Flags

```bash
# Modern (default) - SSE4.2, AVX support required
bun build --compile --target bun-linux-x64 ./server.ts

# Baseline - older CPU support
bun build --compile --target bun-linux-x64-baseline ./server.ts

# With bytecode compilation (2x faster startup)
bun build --compile --bytecode ./server.ts

# Full optimization
bun build --compile --minify --bytecode --sourcemap ./server.ts
```

**Target matrix:**
| Target | Modern | Baseline |
|--------|--------|----------|
| `bun-linux-x64` | AVX2 | SSE2 |
| `bun-linux-arm64` | ARMv8 | N/A |
| `bun-darwin-x64` | AVX2 | SSE2 |
| `bun-darwin-arm64` | Apple Silicon | N/A |
| `bun-windows-x64` | AVX2 | SSE2 |

### 9.3 Rust Optimization Flags

```toml
# .cargo/config.toml
[profile.release]
lto = "fat"           # Link-time optimization
codegen-units = 1     # Better optimization, slower build
panic = "abort"       # Smaller binary
strip = true          # Remove debug symbols

[profile.release.build-override]
opt-level = 3
```

**Target CPU options:**
```bash
# Modern (default)
RUSTFLAGS="-C target-cpu=x86-64-v3" cargo build --release

# Baseline (maximum compatibility)
RUSTFLAGS="-C target-cpu=x86-64" cargo build --release

# Native (build machine optimized)
RUSTFLAGS="-C target-cpu=native" cargo build --release
```

### 9.4 Python/glibc Compatibility

| Linux Target | glibc Version | Compatibility |
|--------------|---------------|---------------|
| `manylinux_2_17` | 2.17+ | CentOS 7+, Ubuntu 14.04+ |
| `manylinux_2_28` | 2.28+ | Ubuntu 18.04+, Debian 10+ |
| `manylinux_2_34` | 2.34+ | Ubuntu 22.04+, Fedora 34+ |
| `musllinux_1_2` | musl 1.2+ | Alpine 3.12+, fully static |

**Recommendation:**
- Default: `manylinux_2_28` (modern Linux)
- Legacy: `manylinux_2_17` (maximum compatibility)
- Static: `musllinux_1_2` (containerized deployments)

### 9.5 Build Profiles

```toml
# vertebra.toml
[build]
profile = "modern"  # modern | baseline | native

[build.profiles.modern]
bun_target_suffix = ""
rust_target_cpu = "x86-64-v3"
python_manylinux = "2_28"

[build.profiles.baseline]
bun_target_suffix = "-baseline"
rust_target_cpu = "x86-64"
python_manylinux = "2_17"

[build.profiles.native]
bun_target_suffix = ""
rust_target_cpu = "native"
python_manylinux = "2_28"
```

### 9.6 CLI Interface

```bash
# Default: modern optimized build
vertebra build --one-file

# Maximum compatibility
vertebra build --one-file --profile baseline

# Development (native CPU)
vertebra build --profile native

# Explicit targets
vertebra build --one-file \
  --target linux-x64 \
  --cpu-level baseline \
  --glibc 2.17
```

### 9.7 CI/CD Matrix with Profiles

```yaml
strategy:
  matrix:
    include:
      # Modern builds (default)
      - profile: modern
        os: ubuntu-latest
        bun_target: bun-linux-x64
        rust_flags: "-C target-cpu=x86-64-v3"
        manylinux: manylinux_2_28

      # Baseline builds (legacy support)
      - profile: baseline
        os: ubuntu-latest
        bun_target: bun-linux-x64-baseline
        rust_flags: "-C target-cpu=x86-64"
        manylinux: manylinux_2_17

      # Static builds (containers)
      - profile: static
        os: ubuntu-latest
        bun_target: bun-linux-x64
        rust_flags: "-C target-feature=+crt-static"
        manylinux: musllinux_1_2
```

### 9.8 Size vs Performance Trade-offs

| Optimization | Binary Size | Startup Time | Runtime Perf |
|--------------|-------------|--------------|--------------|
| Default | ~80MB | ~100ms | 100% |
| `--minify` | ~60MB | ~100ms | 100% |
| `--bytecode` | ~85MB | ~50ms | 100% |
| `--minify --bytecode` | ~65MB | ~50ms | 100% |
| LTO + strip | -20% | Same | +5-10% |
| Baseline CPU | Same | Same | -10-20% |

---

## References

- [Bun Single-file Executables](https://bun.com/docs/bundler/executables)
- [PyOxidizer Documentation](https://pyoxidizer.readthedocs.io/en/stable/)
- [pyembed Crate](https://docs.rs/pyembed)
- [python-build-standalone Releases](https://github.com/indygreg/python-build-standalone/releases)
- [PyApp GitHub](https://github.com/ofek/pyapp)
- [GoogleCloudPlatform/database-assessment bundler](https://github.com/GoogleCloudPlatform/database-assessment/tree/main/tools)
