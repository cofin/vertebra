# PRD: Distribution & Packaging for Vertebra 1.0

**Status:** Active
**Created:** 2026-01-19
**Author:** AI Assistant
**Archived From:** `specs/archive/build-distribution-experience/`
**Dependencies:** Core IPC (complete), Type Bridge (complete), Production Readiness (in progress)

**Related Documents:**
- [Tasks Breakdown](./tasks.md)
- [Recovery Guide](./recovery.md)
- [Research Plan](./research/plan.md)
- [Index](./index.md)

---

## Executive Summary

This PRD consolidates all distribution and packaging work for Vertebra 1.0, defining how developers install, scaffold, build, and deploy Vertebra applications. The goal is a seamless developer experience from `pip install vertebra` to production-ready single-file executables.

**Core Capabilities:**

1. **Zero-build installation**: `pip install vertebra` delivers a complete, working framework
2. **Project scaffolding**: `vertebra init` creates properly structured projects
3. **Native Python embedding**: `pyembed` crate integration (not external PyApp patching)
4. **Bun compilation**: `Bun.build({ compile: true })` for standalone executables
5. **Rust supervisor**: GIL-free process management supporting custom event loops
6. **Unified build command**: `vertebra build --one-file` produces portable binaries
7. **Cross-platform wheels**: Linux x64/arm64, macOS x64/arm64, Windows x64
8. **Optimization profiles**: Modern (default), baseline (legacy), native (development)

**Key Architectural Decisions:**

- **Offline builds**: All builds work without network access (complete dependency bundling)
- **Binary size**: ~80-150MB acceptable for superior developer experience
- **Split packages**: `vertebra` (main) + `vertebra-engine` (platform-specific) for PyPI limits
- **Build config**: `pyproject.toml` `[tool.vertebra]` section (not separate files)
- **Runtime config**: Python code for type safety and environment awareness
- **Python embedding**: `pyembed` crate (stable API) over PyApp patching (fragile)
- **Process spawning**: `sys.executable` from Python (works in venv and one-file mode)

---

## Problem Statement

### Current State

Vertebra's current distribution approach has several limitations:

1. **Complex installation**: Users must install Python, Bun, and potentially Rust separately
2. **No project scaffolding**: Users manually create project structure, easy to misconfigure
3. **External PyApp dependency**: Planned design patches PyApp source externally (fragile)
4. **Python supervisor limitations**: GIL contention, signal handling issues, event loop conflicts
5. **No unified build**: Separate commands for Python, Bun, and Rust components
6. **No single-file distribution**: No path to deploy standalone executables

### User Pain Points

**Python Developers:**
- "I ran `pip install vertebra` but then had to install Bun separately"
- "The server doesn't work with uvloop - I need custom event loop support"
- "I don't know how to structure my project - no templates or scaffolding"
- "How do I deploy this to production? Docker? Containers? Single binary?"

**TypeScript/Bun Developers:**
- "I want to use Vertebra from Bun but there's no npm package"
- "How do I integrate Python procedures into my Bun project?"
- "Can I use `bun build --compile` with Vertebra?"

**DevOps/Platform Engineers:**
- "I need a single binary I can deploy to production environments"
- "The build process is too complex for our CI/CD pipelines"
- "How do I build for older Linux systems? What about glibc compatibility?"
- "Our production environment has no internet access - can I build offline?"

### Impact

Without this work:
- **Adoption limited** by installation complexity and lack of scaffolding
- **Performance constrained** - uvloop/rloop users cannot use Vertebra
- **Production deployments** require complex multi-step builds
- **No path to single-binary distribution** for simplified deployment
- **Enterprise adoption blocked** by lack of offline build support

---

## Goals and Non-Goals

### Goals

**G1: Zero-Build Installation**
`pip install vertebra` delivers a complete, ready-to-run framework with embedded Bun engine. No additional tools required.

**G2: Intuitive Project Scaffolding**
`vertebra init` creates properly structured projects with package manager templates (uv, poetry, hatch, pdm, pip).

**G3: Single-File Distribution**
`vertebra build --one-file` produces portable standalone executables (~80-150MB) with embedded Python runtime.

**G4: GIL-Free Supervision**
Rust supervisor eliminates GIL issues and supports uvloop/rloop without conflicts.

**G5: Offline Build Support**
All builds work completely offline with configurable optimization profiles (modern/baseline/native).

**G6: TypeScript Integration**
TypeScript users can use `bun create vertebra` for Bun-first projects with Python procedures.

**G7: Native API Integration**
Use native APIs (`pyembed`, `Bun.build`) instead of external tool patching.

**G8: Cross-Platform Wheels**
Publish platform-specific wheels for Linux (x64/arm64), macOS (x64/arm64), Windows (x64).

### Non-Goals

**NG1: GUI Installer**
No desktop application or graphical installer. CLI-only distribution.

**NG2: Container Images**
No official Docker/Podman images. Users can build their own using the framework.

**NG3: ARM Windows Support**
x64 only initially. ARM64 Windows support deferred to future releases.

**NG4: Python 3.11 Support**
Python 3.12+ baseline. No backward compatibility with 3.11.

**NG5: Source-Based Installation**
Users should not need to build Rust/Bun from source. Pre-built binaries only.

**NG6: Dynamic Configuration Files**
No `vertebra.toml` or similar runtime configuration files. Python code for runtime config.

---

## User Stories

### US1: Python Developer Installation

**As a** Python developer
**I want to** run `pip install vertebra`
**So that** I can start using Vertebra immediately without installing other tools

**Acceptance Criteria:**
- Single pip command installs complete framework
- `vertebra serve app:app` works immediately after install
- No Bun, Rust, or other tool installation required
- Works on Linux, macOS, and Windows
- Embedded Bun engine is platform-appropriate (baseline for max compatibility)

### US2: Project Scaffolding

**As a** new Vertebra user
**I want to** run `vertebra init myproject`
**So that** I have a properly structured project to start from

**Acceptance Criteria:**
- Interactive prompt for project name, package manager, features
- Supports uv, poetry, hatch, pdm, and pip
- Generates `pyproject.toml` with `[tool.vertebra]` build config and `[tool.hatch.build] sources = ["src/py"]`
- Generates `src/py/myproject/app.py` with runtime configuration (pools, broker) as Python code
- Optional TypeScript client generation with `--with-typescript`
- Project structure: `src/py/<name>/`, `src/ts/` (monorepo pattern)

### US3: Production Build

**As a** DevOps engineer
**I want to** run `vertebra build --one-file`
**So that** I can deploy a single binary to production

**Acceptance Criteria:**
- Produces standalone executable (~80-150MB)
- Works offline (no network required after dependencies cached)
- Includes Python runtime from python-build-standalone
- Includes all Python dependencies (vertebra + user app deps)
- Includes Bun engine binary
- Configurable optimization profile (modern/baseline)
- Runs on target platform without any external dependencies

### US4: Custom Event Loop

**As a** performance-focused developer
**I want to** use uvloop with Vertebra
**So that** I get maximum async performance

**Acceptance Criteria:**
- `uvloop.install()` works before Vertebra startup
- Rust supervisor doesn't interfere with event loop
- Workers correctly inherit event loop policy
- No GIL contention during IPC operations
- Same compatibility for rloop and other event loop implementations

### US5: TypeScript-First Development

**As a** TypeScript developer
**I want to** run `bun create vertebra myproject`
**So that** I can build a Bun-first project with Python procedures

**Acceptance Criteria:**
- Creates Bun project with Vertebra integration
- TypeScript types for Python procedures
- `bun build --compile` produces standalone binary
- Optional Python backend generation
- TypeScript-first project structure

### US6: Legacy System Support

**As an** enterprise developer
**I want to** build for older Linux systems
**So that** I can deploy to our legacy infrastructure

**Acceptance Criteria:**
- `--profile baseline` targets pre-2013 CPUs (x86-64 baseline)
- `manylinux_2_17` wheels available for older glibc (CentOS 7+)
- Clear documentation of compatibility matrix
- No runtime CPU instruction crashes on older systems

### US7: Offline Build Pipeline

**As a** security-conscious enterprise
**I want to** build in air-gapped environments
**So that** our security policy is satisfied

**Acceptance Criteria:**
- All dependencies bundled in standalone executable
- No network calls during execution
- Cached python-build-standalone distributions
- Cached Bun binaries
- Build succeeds in network-isolated container

---

## Technical Specification

### 1. Native Python Embedding (pyembed)

Replace external PyApp patching with native `pyembed` crate integration from the PyOxidizer project.

#### 1.1 Cargo Integration

```toml
# src/rs/vertebra-py/Cargo.toml
[dependencies]
pyo3 = { version = "0.22", features = ["auto-initialize"] }
pyembed = { version = "0.26", optional = true }

[features]
default = []
embedded = ["pyembed"]
standalone = ["embedded"]
```

#### 1.2 Rust Implementation

```rust
// src/rs/vertebra-py/src/embedded.rs
use pyembed::{MainPythonInterpreter, OxidizedPythonInterpreterConfig};
use pyo3::prelude::*;

#[cfg(feature = "embedded")]
pub fn run_embedded_python(app_module: &str) -> Result<i32, String> {
    let config = OxidizedPythonInterpreterConfig::default();
    let interp = MainPythonInterpreter::new(config)
        .map_err(|e| format!("Failed to create interpreter: {}", e))?;

    interp.with_gil(|py| {
        let code = format!(
            "import {}; {}.main()",
            app_module, app_module
        );
        py.run(&code, None, None)
            .map_err(|e| format!("Python error: {}", e))?;
        Ok(0)
    })
}
```

#### 1.3 Benefits over PyApp Patching

- **Stable API**: Crate versioning, no source code dependency
- **No source patching**: No cloning external repos and modifying source
- **Zero-copy loading**: Modules loaded from memory without extraction
- **Custom meta path importer**: Embedded resources accessible via import
- **Memory allocator options**: jemalloc, mimalloc, snmalloc support

#### 1.4 Resource Layout

**Embedded Binary Structure:**
```
dist/myapp (standalone executable, ~80-150MB)
├── Python runtime (from python-build-standalone)
│   ├── Python interpreter
│   ├── Standard library (frozen/compressed)
│   └── Selected based on --profile and --target
├── Site-packages (ALL dependencies resolved offline)
│   ├── vertebra (framework)
│   ├── msgspec, nanoarrow, etc. (vertebra deps)
│   ├── User's deps from pyproject.toml
│   └── Transitive dependencies (fully resolved)
├── User's application code (from src/py/<project>/)
├── Bun engine binary
│   └── Selected based on --profile (baseline vs modern)
├── SSL certificates (bundled certifi)
└── Static resources (templates, assets)
```

**Import Strategy:**
- OxidizedFinder for embedded modules (in-memory imports)
- Fallback to filesystem for development mode
- No extraction to temp (everything in-memory where possible)
- Native extensions (.so/.dylib/.pyd) require extraction (dlopen needs filesystem path)

**Native Extension Policy:**
- Pre-compiled for each target platform via platform-specific wheels
- Distributed via platform-specific wheels
- SSL/TLS: bundled certificates (certifi-style)
- Extraction required for native extensions only

**Extraction Directory (uses app name, NOT "pyembed"):**
```
Example: $TMPDIR/cool-web-app-a1b2c3/ (if app named "cool-web-app")
Fallback: $TMPDIR/vertebra-a1b2c3/ (if no name available)
```

**Directory precedence (with fallback on write failure):**
1. `$VERTEBRA_CACHE_DIR` (explicit override)
2. `$XDG_CACHE_HOME/<app-name>-<hash>/` (Linux/macOS)
3. `~/.cache/<app-name>-<hash>/` (Linux/macOS fallback)
4. `tempfile.gettempdir()/<app-name>-<hash>/` (Python's platform temp)
5. `/tmp/<app-name>-<hash>/` (last resort)

Where:
- `<app-name>` = `[project].name` from pyproject.toml
- `<hash>` = short hash of binary path (for isolation when multiple versions)

**Caching behavior:**
- First run: Extract native extensions to cache directory (~50ms)
- Subsequent runs: Skip extraction, load directly from cache (~5ms)
- Cache invalidation: Hash changes if binary is replaced/updated

---

### 2. Rust Supervisor

Move process supervision from Python to Rust to eliminate GIL issues and support custom event loops.

#### 2.1 Python API

```python
# src/py/vertebra/supervisor.py
from vertebra._vertebra import Supervisor

def start_server(app: str, host: str, port: int, workers: int):
    import sys
    supervisor = Supervisor()

    # Runtime config is passed as Python object, NOT file path
    config = get_runtime_config()

    supervisor.start(
        engine_path=find_engine(),
        python_executable=sys.executable,  # Use actual interpreter (venv-aware)
        config=config,                     # Python object
        workers=workers,
    )
    return supervisor.wait_for_shutdown()
```

#### 2.2 Rust Implementation

```rust
// src/rs/vertebra-py/src/supervisor.rs
use pyo3::prelude::*;
use std::process::{Child, Command};
use std::sync::Arc;

#[pyclass]
pub struct Supervisor {
    bun_process: Option<Child>,
    workers: Vec<Child>,
}

#[pymethods]
impl Supervisor {
    #[new]
    fn new() -> Self {
        Self {
            bun_process: None,
            workers: Vec::new(),
        }
    }

    fn start(
        &mut self,
        py: Python,
        engine_path: &str,
        python_executable: &str,  // sys.executable from Python (venv-aware)
        config: PyObject,         // Runtime config as Python object
        workers: usize,
    ) -> PyResult<()> {
        // Release GIL during process spawning
        py.allow_threads(|| {
            // Start Bun engine (config passed via env/args, not file)
            self.bun_process = Some(
                Command::new(engine_path)
                    .spawn()
                    .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                        format!("Failed to start engine: {}", e)
                    ))?
            );

            // Start Python workers using actual interpreter (not "python")
            for _ in 0..workers {
                self.workers.push(
                    Command::new(python_executable)
                        .arg("-m")
                        .arg("vertebra.server.worker")
                        .spawn()
                        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                            format!("Failed to start worker: {}", e)
                        ))?
                );
            }
            Ok(())
        })
    }

    fn wait_for_shutdown(&mut self, py: Python) -> PyResult<i32> {
        py.allow_threads(|| {
            // Rust handles SIGTERM/SIGINT directly
            // Returns exit code when shutdown complete
            self.wait_for_signals()
        })
    }

    fn stop(&mut self) -> PyResult<()> {
        // Graceful shutdown
        if let Some(ref mut bun) = self.bun_process {
            bun.kill()?;
        }
        for worker in &mut self.workers {
            worker.kill()?;
        }
        Ok(())
    }
}
```

#### 2.3 One-File Mode Process Spawning

For standalone executables, workers re-exec the same binary:

```rust
// For one-file mode, re-exec self:
fn start_one_file(&mut self, py: Python) -> PyResult<()> {
    let current_exe = std::env::current_exe()
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            format!("Cannot determine current executable: {}", e)
        ))?;

    py.allow_threads(|| {
        for _ in 0..self.worker_count {
            self.workers.push(
                Command::new(&current_exe)
                    .arg("--worker")  // Special flag for worker mode
                    .spawn()
                    .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                        format!("Failed to spawn worker: {}", e)
                    ))?
            );
        }
        Ok(())
    })
}
```

#### 2.4 Event Loop Compatibility

The Rust supervisor avoids event loop conflicts:

1. **No Python async**: Supervisor runs in Rust threads, not Python coroutines
2. **GIL release**: All blocking operations use `py.allow_threads()`
3. **Signal handling**: Rust handles signals directly via `signal-hook` crate (Unix)
4. **IPC is synchronous**: Shared-memory ring buffers are inherently synchronous

**uvloop/rloop users** can install their preferred event loop:

```python
import uvloop
uvloop.install()

# Supervisor starts in Rust thread, doesn't interfere
from vertebra import Vertebra
app = Vertebra()
```

#### 2.5 Platform-Specific Process Management

**Unix (Linux/macOS):**
```rust
#[cfg(unix)]
fn setup_process_group(&self) -> Result<()> {
    use signal_hook::consts::signal::*;
    // Use process groups and SIGTERM propagation
    // signal-hook crate for async-safe signal handling
}
```

**Windows:**
```rust
#[cfg(windows)]
fn setup_process_group(&self) -> Result<()> {
    use windows::Win32::System::JobObjects::*;
    // Create job object to manage child processes
    // Ensures children terminate when parent exits (avoid orphan processes)
}
```

---

### 3. Bun Engine Compilation

Use `Bun.build()` programmatic API for engine compilation.

#### 3.1 TypeScript Build Script

```typescript
// tools/build-engine.ts
export interface BuildOptions {
  target: string;
  outdir: string;
  profile: "modern" | "baseline" | "native";
  minify: boolean;
  bytecode: boolean;
}

export async function buildEngine(options: BuildOptions): Promise<string> {
  const bunTarget = getBunTarget(options.target, options.profile);

  const result = await Bun.build({
    entrypoints: ["./src/ts/src/server/index.ts"],
    compile: true,
    outfile: `${options.outdir}/vertebra-engine`,
    target: bunTarget,
    minify: options.minify,
    bytecode: options.bytecode,
    define: {
      "process.env.NODE_ENV": '"production"',
    },
  });

  if (!result.success) {
    throw new Error(`Build failed: ${result.logs.join("\n")}`);
  }

  return result.outputs[0].path;
}

function getBunTarget(target: string, profile: string): string {
  const baseTarget = {
    "linux-x64": "bun-linux-x64",
    "linux-arm64": "bun-linux-arm64",
    "darwin-x64": "bun-darwin-x64",
    "darwin-arm64": "bun-darwin-arm64",
    "windows-x64": "bun-windows-x64",
  }[target];

  if (profile === "baseline" && target.includes("x64")) {
    return `${baseTarget}-baseline`;
  }

  return baseTarget;
}
```

#### 3.2 Cross-Platform Targets

| Target | OS | Architecture | Baseline Variant |
|--------|-----|--------------|------------------|
| `bun-linux-x64` | Linux | x86_64 | `bun-linux-x64-baseline` |
| `bun-linux-arm64` | Linux | aarch64 | N/A (ARM64 already optimized) |
| `bun-darwin-x64` | macOS | x86_64 | `bun-darwin-x64-baseline` |
| `bun-darwin-arm64` | macOS | arm64 (Apple Silicon) | N/A |
| `bun-windows-x64` | Windows | x86_64 | `bun-windows-x64-baseline` |

**Baseline variants** (for older CPUs pre-2013):
- Target pre-AVX2 CPUs with SSE2 only
- Larger binary, slower runtime performance (-10-20%)
- Maximum compatibility for legacy systems

---

### 4. Project Scaffolding

#### 4.1 CLI Interface

```bash
# Interactive mode
vertebra init

# Non-interactive mode
vertebra init myproject \
  --package-manager uv \
  --with-typescript \
  --database postgres
```

#### 4.2 Generated Project Structure

```
myproject/
├── pyproject.toml          # Build config in [tool.vertebra]
├── package.json            # If --with-typescript
├── tsconfig.json           # TypeScript configuration
├── src/
│   ├── py/
│   │   ├── myproject/      # Python package (same name as project)
│   │   │   ├── __init__.py
│   │   │   ├── app.py      # Runtime config (pools, broker) HERE
│   │   │   └── procedures/
│   │   │       ├── __init__.py
│   │   │       └── hello.py
│   │   └── tests/          # Python tests
│   │       └── test_app.py
│   └── ts/                 # If --with-typescript
│       ├── src/
│       │   └── index.ts
│       └── client/         # Generated TypeScript client
│           └── types.ts
└── dist/                   # Build outputs
    └── .gitkeep
```

#### 4.3 pyproject.toml Template

```toml
[project]
name = "myproject"
version = "0.1.0"
dependencies = ["vertebra"]

[project.scripts]
myproject = "myproject.__main__:main"

[build-system]
build-backend = "hatchling.build"
requires = ["hatchling"]

[tool.hatch.build]
sources = ["src/py"]

[tool.vertebra]
profile = "modern"  # Build config ONLY (not runtime config)
```

#### 4.4 Runtime Configuration (Python Code)

```python
# src/py/myproject/app.py
from vertebra import Vertebra
from vertebra.resources import PoolConfig, BrokerConfig
import os

app = Vertebra()

# Runtime configuration in Python code (NOT in config files)
app.configure_pool(
    name="postgres",
    driver="bun-postgres",
    url=os.environ.get("DATABASE_URL", "postgres://localhost/mydb"),
    max_connections=20,
)

app.configure_pool(
    name="redis",
    driver="bun-redis",
    url=os.environ.get("REDIS_URL", "redis://localhost:6379"),
)

# Or via config object
app.configure(
    broker=BrokerConfig(
        name="myproject-broker",
        slot_size=65536,
        slots=1024,
    ),
)
```

#### 4.5 Package Manager Templates

| Manager | Lock File | Run Command |
|---------|-----------|-------------|
| uv | `uv.lock` | `uv run vertebra serve` |
| poetry | `poetry.lock` | `poetry run vertebra serve` |
| hatch | - | `hatch run vertebra serve` |
| pdm | `pdm.lock` | `pdm run vertebra serve` |
| pip | `requirements.txt` | `python -m vertebra serve` |

---

### 5. Unified Build Command

#### 5.1 CLI Interface

```bash
# Development build
vertebra build

# Production single-file
vertebra build --one-file --output dist/myapp

# Cross-platform with profile
vertebra build --one-file \
  --target linux-x64 \
  --profile baseline \
  --output dist/myapp-linux

# All platforms
vertebra build --one-file --all-targets --output dist/
```

#### 5.2 Programmatic API

```python
from vertebra.build import Builder

builder = Builder(config_path="pyproject.toml")

# Individual steps
await builder.generate_types()      # TypeScript types
await builder.compile_engine()      # Bun binary
await builder.build_wheel()         # Python wheel

# All-in-one
await builder.build_one_file(
    output="dist/myapp",
    target="linux-x64",
    profile="modern",
)
```

---

### 6. Distribution Architecture

This section covers TWO distinct distribution scenarios.

#### 6.1 Vertebra Framework Distribution (PyPI)

**Goal:** `pip install vertebra` gives users a complete, working framework.

**Approach:** Follow sqlspec's cibuildwheel + hatch-mypyc pattern.

**Split Package Architecture:**

```
vertebra (main package, ~5MB)
├── Python code
├── Rust extension (.so)
└── Depends on: vertebra-engine

vertebra-engine (platform-specific, ~80MB)
├── Pre-compiled Bun engine binary
└── One wheel per platform
```

**This split keeps the main package small and allows aggressive caching of the engine.**

**Wheel Matrix:**

```
# Pure Python fallback (no embedded engine)
vertebra-{version}-py3-none-any.whl

# Platform-specific compiled wheels with embedded Bun engine
vertebra-engine-{version}-cp312-cp312-manylinux_2_17_x86_64.whl     # Linux x64
vertebra-engine-{version}-cp312-cp312-manylinux_2_17_aarch64.whl    # Linux arm64
vertebra-engine-{version}-cp312-cp312-macosx_10_12_x86_64.whl       # macOS x64
vertebra-engine-{version}-cp312-cp312-macosx_11_0_arm64.whl         # macOS arm64
vertebra-engine-{version}-cp312-cp312-win_amd64.whl                 # Windows x64

# Repeat for Python 3.13, 3.14
```

**CI Workflow (like sqlspec publish.yml):**

```yaml
build-wheels:
  runs-on: ubuntu-latest
  steps:
    - uses: docker/setup-qemu-action@v3
    - run: python -m cibuildwheel --output-dir wheelhouse
      env:
        HATCH_BUILD_HOOKS_ENABLE: "1"
        CIBW_ARCHS_LINUX: "x86_64 aarch64"
        CIBW_ARCHS_MACOS: "x86_64 arm64"
        CIBW_ARCHS_WINDOWS: "AMD64"
```

**PyPI wheels include BASELINE-ONLY Bun binaries for maximum compatibility.**

#### 6.2 User Application Distribution (vertebra build)

**Goal:** Users build standalone executables of THEIR apps.

**Scenario:**
```
cool-web-app/                    # User's project
├── pyproject.toml               # [tool.vertebra] build config
├── src/py/cool_web_app/
│   ├── app.py                   # Their Vertebra app (runtime config here)
│   └── procedures/
└── src/ts/                      # Optional TypeScript
```

**Build Command:**
```bash
# User runs from their project directory
vertebra build --one-file --profile modern --target linux-x64 --output dist/myapp
```

**What Gets Bundled (complete offline execution):**

```
dist/myapp (standalone executable, ~80-150MB)
├── Python runtime (from python-build-standalone)
│   ├── Python interpreter
│   ├── Standard library (frozen)
│   └── Selected based on --profile and --target
├── Site-packages (ALL dependencies resolved offline)
│   ├── vertebra (framework)
│   ├── msgspec, etc. (vertebra deps)
│   ├── User's deps from pyproject.toml
│   └── Transitive dependencies (fully resolved)
├── User's application code (cool_web_app/)
├── Bun engine binary
│   └── Selected based on --profile (baseline vs modern)
├── SSL certificates (bundled certifi)
└── Static resources (templates, assets)
```

**Offline Guarantee:** The resulting binary runs with ZERO network access. All dependencies are embedded. No `pip install` at runtime.

#### 6.3 Code Signing & Packaging

**macOS Code Signing & Notarization:**

Uses `rcodesign` (Rust-based Apple code signing):

```yaml
# CI workflow
- name: Sign macOS binary
  run: |
    rcodesign sign \
      --pem-source ${{ secrets.APPLE_CERT_APPLICATION }} \
      --code-signature-flags runtime \
      dist/myapp-darwin-arm64

- name: Notarize macOS binary
  run: |
    zip myapp.zip dist/myapp-darwin-arm64
    rcodesign notary-submit \
      --api-key-path ${{ secrets.APPLE_API_KEY }} \
      --max-wait-seconds 3600 \
      myapp.zip
```

**Windows Installer Generation:**

Uses PyOxidizer for MSI/EXE installers:

```yaml
- name: Build Windows installer
  run: |
    pyoxidizer build msi_installer exe_installer \
      --release \
      --var version ${{ env.VERSION }}
```

**Required CI Secrets:**

| Secret | Purpose |
|--------|---------|
| `APPLE_CERT_APPLICATION` | macOS binary signing certificate |
| `APPLE_CERT_INSTALLER` | macOS PKG installer certificate |
| `APPLE_API_KEY` | App Store Connect notarization |
| `WINDOWS_SIGNING_CERT` | Windows Authenticode certificate (optional) |

**User Builds (optional signing):**

```bash
# User can optionally sign their own builds
vertebra build --one-file --sign --output dist/myapp

# Or provide their own certificates
vertebra build --one-file \
  --sign-cert /path/to/cert.pem \
  --output dist/myapp
```

#### 6.4 python-build-standalone Integration

**Source:** https://github.com/astral-sh/python-build-standalone/releases (now under Astral)

```python
# vertebra/build/standalone.py

STANDALONE_VERSION = "20260114"  # Latest release
STANDALONE_BASE_URL = f"https://github.com/astral-sh/python-build-standalone/releases/download/{STANDALONE_VERSION}"

STANDALONE_RELEASES = {
    # Linux x64 (baseline vs modern)
    "linux-x64-baseline": "cpython-3.12.8+20260114-x86_64-unknown-linux-gnu-install_only.tar.gz",
    "linux-x64-modern": "cpython-3.12.8+20260114-x86_64_v3-unknown-linux-gnu-install_only.tar.gz",
    # Linux ARM64 (no baseline/modern distinction)
    "linux-arm64": "cpython-3.12.8+20260114-aarch64-unknown-linux-gnu-install_only.tar.gz",
    # macOS x64 (baseline vs modern)
    "darwin-x64-baseline": "cpython-3.12.8+20260114-x86_64-apple-darwin-install_only.tar.gz",
    "darwin-x64-modern": "cpython-3.12.8+20260114-x86_64_v3-apple-darwin-install_only.tar.gz",
    # macOS ARM64 / Apple Silicon (no baseline/modern distinction)
    "darwin-arm64": "cpython-3.12.8+20260114-aarch64-apple-darwin-install_only.tar.gz",
    # Windows x64
    "windows-x64": "cpython-3.12.8+20260114-x86_64-pc-windows-msvc-install_only.tar.gz",
}

async def download_standalone(target: str, profile: str, cache_dir: Path) -> Path:
    """Download python-build-standalone release for target platform."""
    key = f"{target}-{profile}" if "x64" in target and profile != "native" else target
    filename = STANDALONE_RELEASES[key]
    url = f"{STANDALONE_BASE_URL}/{filename}"
    # Download and cache...
```

---

### 7. Optimization Profiles

Build configuration lives in the user's `pyproject.toml` (primary) with optional override in `vertebra.build.toml`.

#### 7.1 Primary Configuration (pyproject.toml)

```toml
# User's pyproject.toml
[project]
name = "myapp"
dependencies = ["vertebra>=1.0"]

[tool.vertebra]
# Build configuration ONLY (NOT runtime config)
profile = "modern"  # modern | baseline | native

[tool.vertebra.build]
# Override specific settings
minify = true
bytecode = true
sourcemap = false

[tool.vertebra.build.targets]
# Platform-specific overrides
linux-x64 = { profile = "baseline" }  # Use baseline for Linux
darwin-arm64 = { profile = "modern" }

[tool.vertebra.profiles.modern]
bun_target_suffix = ""
rust_target_cpu = "x86-64-v3"
python_manylinux = "2_28"

[tool.vertebra.profiles.baseline]
bun_target_suffix = "-baseline"
rust_target_cpu = "x86-64"
python_manylinux = "2_17"

[tool.vertebra.profiles.native]
bun_target_suffix = ""
rust_target_cpu = "native"
python_manylinux = "2_28"
```

#### 7.2 Profile Selection Table

| Target | Profile | Python Standalone | Bun Target | Use Case |
|--------|---------|-------------------|------------|----------|
| `linux-x64` | `baseline` | `x86_64-unknown-linux-gnu` | `bun-linux-x64-baseline` | Max x64 compatibility |
| `linux-x64` | `modern` | `x86_64_v3-unknown-linux-gnu` | `bun-linux-x64` | x64 performance (2013+) |
| `linux-arm64` | (default) | `aarch64-unknown-linux-gnu` | `bun-linux-arm64` | ARM64 servers (Graviton) |
| `darwin-x64` | `baseline` | `x86_64-apple-darwin` | `bun-darwin-x64` | Intel Mac compatibility |
| `darwin-x64` | `modern` | `x86_64_v3-apple-darwin` | `bun-darwin-x64` | Intel Mac performance |
| `darwin-arm64` | (default) | `aarch64-apple-darwin` | `bun-darwin-arm64` | Apple Silicon (M1/M2/M3/M4) |
| `windows-x64` | `baseline` | `x86_64-pc-windows-msvc` | `bun-windows-x64` | Windows x64 |
| (any) | `native` | System Python | System Bun | Development only |

**Note:** ARM targets don't have baseline/modern distinction - ARM64 is already optimized.

#### 7.3 Built-in Profile Definitions

| Profile | Bun Target | Rust CPU | Python glibc | Use Case |
|---------|------------|----------|--------------|----------|
| `modern` | (default) | x86-64-v3 | 2.28 | 2013+ CPUs, best performance |
| `baseline` | -baseline | x86-64 | 2.17 | Pre-2013 CPUs, max compat |
| `native` | (default) | native | 2.28 | Development, build machine |

#### 7.4 Resolution Order (build config only)

1. CLI flags (`--profile baseline`)
2. `vertebra.build.toml` (if exists)
3. `pyproject.toml` `[tool.vertebra]` section
4. Built-in defaults (modern profile)

#### 7.5 Rust Build Flags

```toml
# .cargo/config.toml
[profile.release]
lto = "fat"
codegen-units = 1
panic = "abort"
strip = true
opt-level = 3
```

---

### 8. Configuration Philosophy

#### 8.1 Build vs Runtime Separation

Vertebra separates build-time configuration from runtime configuration:

| Configuration Type | Location | Purpose |
|--------------------|----------|---------|
| **Build config** | `pyproject.toml` `[tool.vertebra]` | Compilation targets, optimization profiles |
| **Runtime config** | Python code (`app.py`) | Pools, broker settings, procedures |

**Rationale:**
1. Build config rarely changes and belongs with project metadata
2. Runtime config often varies by environment (dev/staging/prod)
3. Python code allows dynamic configuration based on environment variables
4. Type checking works for Python config, not TOML

#### 8.2 Environment-Aware Runtime Config

```python
# src/myapp/app.py
import os
from vertebra import Vertebra
from vertebra.resources import PoolConfig

app = Vertebra()

# Environment-aware pool configuration
if os.getenv("ENV") == "production":
    app.configure_pool(
        name="postgres",
        driver="bun-postgres",
        url=os.environ["DATABASE_URL"],
        max_connections=50,
        ssl=True,
    )
else:
    app.configure_pool(
        name="postgres",
        driver="bun-postgres",
        url="postgres://localhost/mydb_dev",
        max_connections=5,
    )
```

---

## Implementation Phases

Implementation is organized into 7 phases. See [tasks.md](./tasks.md) for detailed task breakdowns.

| Phase | Goal | Duration | Dependencies |
|-------|------|----------|--------------|
| 1 | Rust Supervisor | Week 1 | None |
| 2 | Bun Compilation | Week 1-2 | None |
| 3 | Project Scaffolding | Week 2 | None (parallel) |
| 4 | Build Command | Week 2-3 | Phase 1, 2 |
| 5 | pyembed Integration | Week 3-4 | Phase 4 |
| 6 | CI/CD Pipeline | Week 4 | Phase 1-5 |
| 7 | npm Package (Optional) | Week 5 | Phase 1-6 |

**Critical Path:** Phase 1 -> Phase 2 -> Phase 5 -> Phase 4 -> Phase 6

**Total Estimated Effort:** 4-6 weeks (one developer)

---

## Testing Strategy

### Test Categories

| Category | Purpose | Location |
|----------|---------|----------|
| Unit Tests | Test individual components | `src/py/tests/test_build.py`, `src/rs/vertebra-py/src/*.rs` |
| Integration Tests | Test component interactions | `src/py/tests/integration/` |
| E2E Tests | Test complete workflows | `src/py/tests/e2e/` |
| Event Loop Tests | Verify uvloop/rloop compatibility | `src/py/tests/test_event_loop_compat.py` |
| Offline Tests | Verify network-isolated operation | CI workflows |

### Coverage Targets

| Component | Target |
|-----------|--------|
| `vertebra.build` | 90% |
| `vertebra.cli` | 85% |
| `vertebra-py/supervisor.rs` | 80% |
| Integration tests | All platforms |

### Key Test Scenarios

1. **Supervisor**: Verify GIL release, event loop compatibility, signal handling
2. **Build Command**: Verify one-file output, cross-platform targets, offline operation
3. **Scaffolding**: Verify all package managers, TypeScript integration
4. **Wheel Building**: Verify all platforms, size limits

See [research/plan.md](./research/plan.md) for detailed test patterns and examples.

---

## API Reference

### Python Build API

```python
from vertebra.build import Builder, BuildConfig, BuildTarget

class Builder:
    """Orchestrates Vertebra build process."""

    def __init__(
        self,
        project_dir: Path | None = None,
        config: BuildConfig | None = None,
    ) -> None: ...

    async def generate_types(self) -> Path:
        """Generate TypeScript types from Python procedures."""

    async def compile_engine(
        self,
        target: BuildTarget | None = None,
    ) -> Path:
        """Compile Bun engine binary for target platform."""

    async def build_wheel(self) -> Path:
        """Build Python wheel with embedded engine."""

    async def build_one_file(
        self,
        output: Path,
        target: BuildTarget | None = None,
        profile: str = "modern",
    ) -> Path:
        """Build standalone single-file executable."""


class BuildTarget(Enum):
    LINUX_X64 = "linux-x64"
    LINUX_ARM64 = "linux-arm64"
    DARWIN_X64 = "darwin-x64"
    DARWIN_ARM64 = "darwin-arm64"
    WINDOWS_X64 = "windows-x64"
```

### Rust Supervisor API

```rust
/// Process supervisor for Vertebra runtime.
#[pyclass]
pub struct Supervisor {
    // ...
}

#[pymethods]
impl Supervisor {
    /// Create a new supervisor instance.
    #[new]
    fn new() -> Self;

    /// Start the Bun engine and optional workers.
    /// Releases the GIL during process spawning.
    fn start(
        &mut self,
        py: Python,
        engine_path: &str,
        python_executable: &str,  // sys.executable from Python
        config: PyObject,         // Runtime config as Python object
        workers: usize,
    ) -> PyResult<()>;

    /// Check if all managed processes are running.
    fn is_running(&self) -> bool;

    /// Wait for shutdown signal (SIGTERM/SIGINT).
    /// Releases the GIL while waiting.
    fn wait_for_shutdown(&self, py: Python) -> PyResult<i32>;

    /// Stop all managed processes gracefully.
    fn stop(&mut self) -> PyResult<()>;
}
```

---

## Consensus Review Findings

This section documents critical issues identified through multi-model review (Gemini 3 Pro, GPT 5.2).

### Critical Issues Addressed

#### 1. Two Distribution Scenarios (CLARIFIED)

**Issue:** Original spec conflated Vertebra's PyPI publishing with user app builds.

**Resolution:** Clearly separate two scenarios:

| Scenario | Who | Command | Output |
|----------|-----|---------|--------|
| **Framework Distribution** | Vertebra maintainers | `cibuildwheel` in CI | PyPI wheels |
| **User App Distribution** | End users | `vertebra build --one-file` | Standalone binary |

**Framework Distribution (PyPI):**
- Follow sqlspec's cibuildwheel + hatch-mypyc pattern
- Embed baseline Bun binaries in wheels (max compatibility)
- Pip auto-selects best wheel for user's platform
- Split packages: `vertebra` (main) + `vertebra-engine` (platform-specific)

**User App Distribution:**
- User chooses `--profile baseline|modern|native`
- We bundle python-build-standalone + Bun binary matching their choice
- Full control over optimization vs compatibility tradeoff

#### 2. Supervisor Process Spawning (FIXED)

**Issue:** `Command::new("python")` ignores sys.executable, breaks in venv and one-file mode.

**Resolution:** Use sys.executable in wheel mode, re-exec pattern in one-file mode (see Section 2.2, 2.3).

#### 3. Config File Contradiction (RESOLVED)

**Issue:** PRD referenced `vertebra.toml` in supervisor code but stated "runtime config in Python code."

**Resolution:** Remove all `vertebra.toml` references. Config is passed as Python object (see Section 2.1).

#### 4. Cross-Platform Scope (CLARIFIED)

**Issue:** "Cross-platform from any OS" is infeasible without heavy toolchains.

**Resolution:** Two distinct scopes:
- **CI builds:** All targets (linux-x64, linux-arm64, darwin-x64, darwin-arm64, windows-x64)
- **Local builds:** Host platform only (offline supported)

Cross-compilation for one-file requires either:
1. CI pipeline (recommended)
2. Docker/QEMU for target platform
3. Pure-Python dependencies only

#### 5. Windows Process Management (ADDED)

**Issue:** Unix signals don't translate to Windows. Need job objects to avoid orphans.

**Resolution:** Platform-specific process management in Rust supervisor (see Section 2.5).

#### 6. pyembed Packaging Mechanics (SPECIFIED)

**Issue:** No specification for resource layout, import hooks, native extensions.

**Resolution:** Added specification in Section 1.4.

#### 7. PyPI Wheel Size Strategy (RESOLVED)

**Issue:** Bun binary (~90MB) may exceed PyPI limits (default 60MB, hard 100MB).

**Resolution:** Split package architecture (see Section 6.1):
- `vertebra` (main package, ~5MB)
- `vertebra-engine` (platform-specific, ~80MB)

---

## Success Metrics

| Metric | Target |
|--------|--------|
| `pip install vertebra` time | < 30s |
| `vertebra init` time | < 5s |
| `vertebra build --one-file` time | < 60s |
| Wheel size (main package) | < 10MB |
| Wheel size (engine package) | < 100MB |
| Standalone binary size | < 150MB |
| Offline build success | 100% |
| Platform coverage | Linux/macOS/Windows x64 + Linux/macOS arm64 |

---

## Risk Assessment

| Risk | Impact | Probability | Mitigation |
|------|--------|-------------|------------|
| pyembed API changes | Medium | Low | Pin version, monitor releases |
| Bun.build() API changes | Medium | Low | Pin Bun version in CI |
| Large wheel size | Low | High | Split into vertebra + vertebra-engine packages |
| Cross-compilation failures | Medium | Medium | CI-only cross-compilation, local = host only |
| uvloop compatibility | High | Low | GIL-free Rust supervisor |
| Windows orphan processes | High | Medium | Job objects for process management |
| Native extension compatibility | High | Medium | Strict platform wheel matrix, no cross-compile |
| PyPI size limits | Medium | Medium | Split package architecture |
| CPU instruction crashes | High | Low | Baseline wheels only on PyPI |
| SSL/certificate issues | Medium | Medium | Bundle certifi certificates |

---

## File Inventory

### New Files

**Rust:**
- `src/rs/vertebra-py/src/supervisor.rs`
- `src/rs/vertebra-py/src/embedded.rs` (optional)

**Python:**
- `src/py/vertebra/build/__init__.py`
- `src/py/vertebra/build/config.py`
- `src/py/vertebra/build/builder.py`
- `src/py/vertebra/build/standalone.py`
- `src/py/vertebra/cli/init.py`
- `src/py/vertebra/cli/build.py`
- `src/py/vertebra/templates/` (directory structure)

**TypeScript:**
- `tools/build-engine.ts`
- `tools/build-cli.ts`
- `tools/bundle-standalone.py`

**CI:**
- `.github/workflows/release.yaml`

**Optional:**
- `packages/vertebra/package.json`
- `packages/create-vertebra/`

### Modified Files

**Rust:**
- `src/rs/vertebra-py/Cargo.toml`
- `src/rs/vertebra-py/src/lib.rs`

**Python:**
- `src/py/vertebra/cli.py`
- `src/py/vertebra/supervisor.py`
- `pyproject.toml`

**CI:**
- `.github/workflows/ci.yaml`

**TypeScript:**
- `src/ts/package.json`

---

## Dependency Additions

**Rust:**
- `pyembed = "0.26"` (optional)
- `signal-hook = "0.3"` (Unix)
- `windows = { version = "0.52", features = ["Win32_System_JobObjects"] }` (Windows)

**Python:**
- `hatchling` (build backend)
- `click` (CLI framework, if not already)
- `cibuildwheel` (CI only)

**TypeScript:**
- (none additional - uses built-in Bun.build)

---

## References

- [Bun Single-file Executables](https://bun.sh/docs/bundler/executables)
- [PyOxidizer / pyembed Documentation](https://pyoxidizer.readthedocs.io/en/stable/)
- [python-build-standalone Releases (Astral)](https://github.com/astral-sh/python-build-standalone/releases)
- [Hatch Build Workflow (signing pattern)](https://github.com/pypa/hatch/blob/master/.github/workflows/build-hatch.yml)
- [rcodesign (Rust Apple signing)](https://github.com/indygreg/apple-platform-rs/tree/main/apple-codesign)
- [sqlspec publish.yml (cibuildwheel pattern)](https://github.com/litestar-org/sqlspec/blob/main/.github/workflows/publish.yml)
- [cibuildwheel Documentation](https://cibuildwheel.readthedocs.io/)
- [hatch-mypyc Plugin](https://github.com/ofek/hatch-mypyc)

---

## Appendix A: Open Questions (Resolved)

All critical questions have been resolved:

1. **Wheel size**: Large size (~80MB) acceptable; split packages for PyPI limits
2. **Offline builds**: Fully supported, required for production use cases
3. **Python version**: 3.12+ only (no 3.11 support)
4. **Windows priority**: Included in initial release (x64 only)
5. **Config location**: Build config in `pyproject.toml`, runtime config in Python code
6. **Process spawning**: Use `sys.executable` from Python, re-exec for one-file mode
