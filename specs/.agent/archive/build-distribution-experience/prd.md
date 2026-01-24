# PRD-9: Build System & Distribution Experience

**Status:** Draft
**Created:** 2026-01-19
**Author:** AI Assistant
**Dependencies:** PRD-1 (complete), PRD-2 (complete), PRD-8 (in progress)

---

## Intelligence Context

**Complexity:** Complex (12+ checkpoints)
**Stack:** Python 3.12+ / Rust / Bun / TypeScript
**Build Commands:** `make install`, `make test`, `make lint`
**Test Commands:** `uv run pytest`, `bun test`, `cargo test`

**Similar Features Referenced:**
- `specs/active/production-readiness/design/single-file-dist.md`
- `src/py/vertebra/supervisor.py` (to be replaced)
- `src/rs/vertebra-py/src/lib.rs` (to be extended)
- GoogleCloudPlatform/database-assessment bundler pattern

**Patterns to Follow:**
- Native Rust integration via PyO3 (existing pattern)
- `py.allow_threads()` for GIL-releasing operations
- Platform-specific wheel builds via maturin

---

## Executive Summary

This PRD specifies a comprehensive build system and distribution experience for Vertebra that enables:

1. **Zero-build installation**: `pip install vertebra` delivers a complete, ready-to-run framework
2. **Native integration**: Using `pyembed` crate for Python embedding (not external PyApp patching)
3. **Bun compilation**: Using `Bun.build()` programmatic API for standalone executables
4. **Rust supervisor**: GIL-free process management supporting custom event loops
5. **Project scaffolding**: Intuitive `vertebra init` with package manager choice
6. **Unified build command**: `vertebra build --one-file` produces portable binaries
7. **Cross-platform support**: Linux (x64/arm64), macOS (x64/arm64), Windows (x64)
8. **Optimization profiles**: Modern (default), baseline (legacy), and native (development)

**Key Decisions:**
- Offline builds are fully supported
- Large binary size (~80MB) is acceptable for better UX
- pyembed crate for native Python embedding
- Bun.build() API for programmatic Bun compilation

---

## Problem Statement

### Current State

Vertebra's current distribution approach has several issues:

1. **Complex installation**: Users must install Python, Bun, and potentially Rust separately
2. **External PyApp dependency**: Current design patches PyApp source externally, which is fragile
3. **Python supervisor limitations**: GIL contention, signal handling issues, event loop conflicts
4. **No project scaffolding**: Users must manually create project structure
5. **No unified build**: Separate commands for Python, Bun, and Rust

### User Pain Points

**Python developers:**
- "I ran `pip install vertebra` but then had to install Bun separately"
- "The server doesn't work with uvloop"
- "I don't know how to structure my project"

**TypeScript developers:**
- "I want to use Vertebra from Bun but there's no npm package"
- "How do I integrate Python procedures into my Bun project?"

**DevOps/Platform engineers:**
- "I need a single binary I can deploy to production"
- "The build process is too complex for our CI/CD"
- "How do I build for older Linux systems?"

### Impact

Without this work:
- Adoption is limited by installation complexity
- uvloop/rloop users cannot use Vertebra
- Production deployments require complex multi-step builds
- No path to single-binary distribution

---

## Goals and Non-Goals

### Goals

1. **G1**: `pip install vertebra` delivers complete framework with embedded Bun engine
2. **G2**: `vertebra init` creates properly structured projects for any package manager
3. **G3**: `vertebra build --one-file` produces portable single-file executables
4. **G4**: Rust supervisor eliminates GIL issues and supports uvloop/rloop
5. **G5**: Cross-platform builds work offline with configurable optimization profiles
6. **G6**: TypeScript users can use `bun create vertebra` for Bun-first projects
7. **G7**: Build system uses native APIs (pyembed, Bun.build) not external tools

### Non-Goals

1. **NG1**: GUI installer or desktop application
2. **NG2**: Docker/container image distribution (users can build their own)
3. **NG3**: ARM Windows support (x64 only initially)
4. **NG4**: Python 3.11 support (3.12+ only)
5. **NG5**: Source-based installation (users should not build Rust/Bun)

---

## User Stories

### US1: Python Developer Installation
**As a** Python developer
**I want to** run `pip install vertebra`
**So that** I can start using Vertebra immediately without installing other tools

**Acceptance Criteria:**
- Single pip command installs complete framework
- `vertebra serve app:app` works immediately
- No Bun, Rust, or other tool installation required
- Works on Linux, macOS, and Windows

### US2: Project Scaffolding
**As a** new Vertebra user
**I want to** run `vertebra init myproject`
**So that** I have a properly structured project to start from

**Acceptance Criteria:**
- Interactive prompt for project name, package manager, features
- Supports uv, poetry, hatch, pdm, and pip
- Generates `pyproject.toml` with correct dependencies and `[tool.vertebra]` build config
- Generates `app.py` with example runtime configuration (pools, broker)
- Optional TypeScript client generation

### US3: Production Build
**As a** DevOps engineer
**I want to** run `vertebra build --one-file`
**So that** I can deploy a single binary to production

**Acceptance Criteria:**
- Produces standalone executable
- Works offline (no network required)
- Cross-platform compilation from any OS
- Configurable optimization profile (modern/baseline)

### US4: Custom Event Loop
**As a** performance-focused developer
**I want to** use uvloop with Vertebra
**So that** I get maximum async performance

**Acceptance Criteria:**
- `uvloop.install()` works before Vertebra startup
- Supervisor doesn't interfere with event loop
- Workers correctly inherit event loop policy
- No GIL contention during IPC operations

### US5: TypeScript-First Development
**As a** TypeScript developer
**I want to** run `bun create vertebra myproject`
**So that** I can build a Bun-first project with Python procedures

**Acceptance Criteria:**
- Creates Bun project with Vertebra integration
- TypeScript types for Python procedures
- `bun build --compile` produces standalone binary
- Optional Python backend generation

### US6: Legacy System Support
**As a** enterprise developer
**I want to** build for older Linux systems
**So that** I can deploy to our legacy infrastructure

**Acceptance Criteria:**
- `--profile baseline` targets pre-2013 CPUs
- `manylinux_2_17` wheels available for older glibc
- Clear documentation of compatibility matrix

---

## Technical Specification

### 1. Native Python Embedding (pyembed)

Replace external PyApp patching with native `pyembed` crate integration.

**Cargo.toml addition:**
```toml
# src/rs/vertebra-py/Cargo.toml
[dependencies]
pyembed = { version = "0.26", optional = true }

[features]
default = []
embedded = ["pyembed"]
standalone = ["embedded"]
```

**Rust implementation:**
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

**Benefits over PyApp patching:**
- Stable API (crate versioning)
- No source patching required
- Zero-copy module loading
- Custom meta path importer for embedded resources

### 2. Rust Supervisor

Move process supervision from Python to Rust to eliminate GIL issues.

**Python API:**
```python
# src/py/vertebra/supervisor.py
from vertebra._vertebra import Supervisor

def start_server(app: str, host: str, port: int, workers: int):
    import sys
    supervisor = Supervisor()
    supervisor.start(
        engine_path=find_engine(),
        python_executable=sys.executable,  # Use actual interpreter
        config=get_runtime_config(),        # Python object, not file path
        workers=workers,
    )
    return supervisor.wait_for_shutdown()
```

**Rust implementation:**
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
        python_executable: &str,  // sys.executable from Python
        config: PyObject,         // Runtime config as Python object
        workers: usize,
    ) -> PyResult<()> {
        // Release GIL during process spawning
        py.allow_threads(|| {
            // Start Bun engine (config passed via env/args, not file)
            self.bun_process = Some(
                Command::new(engine_path)
                    .spawn()?
            );

            // Start Python workers using actual interpreter
            for _ in 0..workers {
                self.workers.push(
                    Command::new(python_executable)
                        .arg("-m")
                        .arg("vertebra.server.worker")
                        .spawn()?
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

**Event loop compatibility:**
- Supervisor runs in Rust threads, not Python async
- All blocking operations use `py.allow_threads()`
- Users can install uvloop/rloop before Vertebra startup
- IPC via shared memory is inherently synchronous

### 3. Bun Engine Compilation

Use `Bun.build()` programmatic API for engine compilation.

**TypeScript build script:**
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

### 4. Project Scaffolding

**CLI interface:**
```bash
# Interactive mode
vertebra init

# Non-interactive mode
vertebra init myproject \
  --package-manager uv \
  --with-typescript \
  --database postgres
```

**Template structure:**
```
myproject/
├── pyproject.toml          # Build config in [tool.vertebra], sources = ["src/py"]
├── package.json            # If --with-typescript, root package for TypeScript
├── tsconfig.json           # TypeScript configuration
├── src/
│   ├── py/
│   │   ├── myproject/      # Python package (same name as project)
│   │   │   ├── __init__.py
│   │   │   ├── app.py      # Runtime config (pools, broker) here
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

**pyproject.toml (generated):**
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
profile = "modern"
```

**Package manager templates:**

| Manager | Lock File | Run Command |
|---------|-----------|-------------|
| uv | `uv.lock` | `uv run vertebra serve` |
| poetry | `poetry.lock` | `poetry run vertebra serve` |
| hatch | - | `hatch run vertebra serve` |
| pdm | `pdm.lock` | `pdm run vertebra serve` |
| pip | `requirements.txt` | `python -m vertebra serve` |

### 5. Unified Build Command

**CLI interface:**
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

**Programmatic API:**
```python
from vertebra.build import Builder

builder = Builder(config_path="vertebra.toml")

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

### 6. Distribution Architecture

This section covers TWO distinct distribution scenarios:

#### 6.1 Vertebra Framework Distribution (PyPI)

**Goal:** `pip install vertebra` gives users a complete, working framework.

**Approach:** Follow sqlspec's cibuildwheel + hatch-mypyc pattern.

**PyPI wheel contents:**
```
vertebra-1.0.0-cp312-cp312-manylinux_2_17_x86_64.whl
├── vertebra/
│   ├── __init__.py
│   ├── cli.py
│   ├── app.py
│   ├── build/
│   │   ├── __init__.py
│   │   └── builder.py
│   ├── _vertebra.cpython-312-x86_64-linux-gnu.so  # PyO3 extension
│   └── bin/
│       └── vertebra-engine                         # Bun binary (baseline)
└── vertebra-1.0.0.dist-info/
    └── METADATA
```

**Wheel matrix (cibuildwheel with QEMU):**
```
# Pure Python fallback (no embedded engine)
vertebra-{version}-py3-none-any.whl

# Platform-specific compiled wheels with embedded Bun engine
vertebra-{version}-cp312-cp312-manylinux_2_17_x86_64.whl     # Linux x64
vertebra-{version}-cp312-cp312-manylinux_2_17_aarch64.whl    # Linux arm64
vertebra-{version}-cp312-cp312-macosx_10_12_x86_64.whl       # macOS x64
vertebra-{version}-cp312-cp312-macosx_11_0_arm64.whl         # macOS arm64
vertebra-{version}-cp312-cp312-win_amd64.whl                 # Windows x64

# Repeat for Python 3.13, 3.14
```

**CI workflow (like sqlspec publish.yml):**
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

---

#### 6.2 User Application Distribution (vertebra build)

**Goal:** Users build standalone executables of THEIR apps.

**Scenario:**
```
cool-web-app/                    # User's project
├── pyproject.toml               # [tool.vertebra] build config
├── src/py/cool_web_app/
│   ├── app.py                   # Their Vertebra app
│   └── procedures/
└── src/ts/                      # Optional TypeScript
```

**Build command:**
```bash
# User runs from their project directory
vertebra build --one-file --profile modern --target linux-x64 --output dist/myapp
```

**What gets bundled (complete offline execution):**
```
dist/myapp (standalone executable, ~80-150MB)
├── Python runtime (from python-build-standalone)
│   ├── Python interpreter
│   ├── Standard library (frozen)
│   └── Selected based on --profile and --target
├── Site-packages (ALL dependencies resolved offline)
│   ├── vertebra (framework)
│   ├── msgspec, cffi, etc. (vertebra deps)
│   ├── User's deps from pyproject.toml
│   └── Transitive dependencies (fully resolved)
├── User's application code (cool_web_app/)
├── Bun engine binary
│   └── Selected based on --profile (baseline vs modern)
├── SSL certificates (bundled certifi)
└── Static resources (templates, assets)
```

**Offline guarantee:** The resulting binary runs with ZERO network access. All dependencies are embedded. No `pip install` at runtime.

---

#### 6.3 Code Signing & Packaging (Following Hatch Pattern)

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

**User builds (optional signing):**
```bash
# User can optionally sign their own builds
vertebra build --one-file --sign --output dist/myapp

# Or provide their own certificates
vertebra build --one-file \
  --sign-cert /path/to/cert.pem \
  --output dist/myapp
```

**Profile selection for user builds:**

| Target | Profile | Python Standalone | Bun Target | Use Case |
|--------|---------|-------------------|------------|----------|
| `linux-x64` | `baseline` | `x86_64-unknown-linux-gnu` | `bun-linux-x64-baseline` | Max x64 compatibility |
| `linux-x64` | `modern` | `x86_64_v3-unknown-linux-gnu` | `bun-linux-x64` | x64 performance (2013+) |
| `linux-arm64` | (default) | `aarch64-unknown-linux-gnu` | `bun-linux-arm64` | ARM64 servers (Graviton, etc.) |
| `darwin-x64` | `baseline` | `x86_64-apple-darwin` | `bun-darwin-x64` | Intel Mac compatibility |
| `darwin-x64` | `modern` | `x86_64_v3-apple-darwin` | `bun-darwin-x64` | Intel Mac performance |
| `darwin-arm64` | (default) | `aarch64-apple-darwin` | `bun-darwin-arm64` | Apple Silicon (M1/M2/M3/M4) |
| `windows-x64` | `baseline` | `x86_64-pc-windows-msvc` | `bun-windows-x64` | Windows x64 |
| (any) | `native` | System Python | System Bun | Development only |

**Note:** ARM targets don't have baseline/modern distinction - ARM64 is already optimized.

**python-build-standalone integration (now under Astral):**
```python
# vertebra/build/standalone.py
# Source: https://github.com/astral-sh/python-build-standalone/releases

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

**Bun binary selection:**
```typescript
// tools/build-engine.ts
const BUN_TARGETS = {
  // Linux x64 (baseline vs modern)
  "linux-x64-baseline": "bun-linux-x64-baseline",
  "linux-x64-modern": "bun-linux-x64",
  // Linux ARM64
  "linux-arm64": "bun-linux-arm64",
  // macOS x64 (Bun doesn't have baseline for macOS, uses same binary)
  "darwin-x64-baseline": "bun-darwin-x64",
  "darwin-x64-modern": "bun-darwin-x64",
  // macOS ARM64 / Apple Silicon
  "darwin-arm64": "bun-darwin-arm64",
  // Windows x64
  "windows-x64": "bun-windows-x64",
};
```

### 7. Optimization Profiles

Build configuration lives in the user's `pyproject.toml` (primary) with optional override in `vertebra.toml`.

**Primary configuration (pyproject.toml):**
```toml
# User's pyproject.toml
[project]
name = "myapp"
dependencies = ["vertebra>=1.0"]

[tool.vertebra]
# Build configuration
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

**Optional build override file (vertebra.build.toml):**
```toml
# vertebra.build.toml - optional, overrides pyproject.toml build settings
# Only for build configuration, NOT runtime/pools

[build]
profile = "modern"
minify = true
bytecode = true
```

**Runtime configuration (Python code):**
```python
# src/myapp/app.py
from vertebra import Vertebra
from vertebra.resources import PoolConfig

app = Vertebra()

# Configure pools programmatically
app.configure_pool(
    name="postgres",
    driver="bun-postgres",
    url="postgres://localhost/mydb",
    max_connections=20,
)

app.configure_pool(
    name="redis",
    driver="bun-redis",
    url="redis://localhost:6379",
)

# Or via config object
app.configure(
    broker=BrokerConfig(
        name="myapp-broker",
        slot_size=65536,
        slots=1024,
    ),
)
```

**Resolution order (build config only):**
1. CLI flags (`--profile baseline`)
2. `vertebra.build.toml` (if exists)
3. `pyproject.toml` `[tool.vertebra]` section
4. Built-in defaults (modern profile)

**Built-in profile definitions:**
| Profile | Bun Target | Rust CPU | Python glibc | Use Case |
|---------|------------|----------|--------------|----------|
| `modern` | (default) | x86-64-v3 | 2.28 | 2013+ CPUs, best performance |
| `baseline` | -baseline | x86-64 | 2.17 | Pre-2013 CPUs, max compat |
| `native` | (default) | native | 2.28 | Development, build machine |

**Rust build flags:**
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

## Configuration Philosophy

### Build vs Runtime Separation

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

### Environment-Aware Runtime Config

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

### Build Config Examples

**Minimal (use defaults):**
```toml
# pyproject.toml
[tool.vertebra]
# Empty - uses modern profile defaults
```

**Custom profile:**
```toml
# pyproject.toml
[tool.vertebra]
profile = "baseline"  # For legacy system compatibility
```

**Full customization:**
```toml
# pyproject.toml
[tool.vertebra]
profile = "modern"

[tool.vertebra.build]
minify = true
bytecode = true
sourcemap = false

[tool.vertebra.build.targets]
linux-x64 = { profile = "baseline" }
windows-x64 = { profile = "modern" }
```

---

## Implementation Phases

### Phase 1: Rust Supervisor

**Tasks:**
1. Add `Supervisor` struct to `vertebra-py`
2. Implement process spawning with GIL release
3. Implement signal handling in Rust
4. Update `vertebra serve` to use Rust supervisor
5. Test with uvloop and rloop

**Files:**
- New: `src/rs/vertebra-py/src/supervisor.rs`
- Modify: `src/rs/vertebra-py/src/lib.rs`
- Modify: `src/py/vertebra/cli.py`

### Phase 2: Bun Engine Compilation

**Tasks:**
1. Create `tools/build-engine.ts`
2. Implement `Bun.build()` wrapper with target support
3. Add bytecode and minification options
4. Test cross-platform compilation

**Files:**
- New: `tools/build-engine.ts`
- Modify: `src/ts/package.json`

### Phase 3: Project Scaffolding

**Tasks:**
1. Create template directory structure
2. Implement `vertebra init` CLI command
3. Add package manager detection and templates
4. Add TypeScript client generation

**Files:**
- New: `src/py/vertebra/templates/`
- New: `src/py/vertebra/cli/init.py`
- Modify: `src/py/vertebra/cli.py`

### Phase 4: Unified Build Command

**Tasks:**
1. Implement `Builder` class
2. Add `vertebra build` CLI command
3. Integrate Bun compilation
4. Add wheel building with embedded engine

**Files:**
- New: `src/py/vertebra/build/`
- Modify: `src/py/vertebra/cli.py`
- Modify: `pyproject.toml`

### Phase 5: pyembed Integration

**Tasks:**
1. Add pyembed dependency
2. Implement embedded Python runner
3. Create standalone binary builder
4. Test offline operation

**Files:**
- Modify: `src/rs/vertebra-py/Cargo.toml`
- New: `src/rs/vertebra-py/src/embedded.rs`
- New: `tools/bundle-standalone.py`

### Phase 6: CI/CD Pipeline

**Tasks:**
1. Create wheel build workflow
2. Add cross-platform matrix
3. Implement offline testing
4. Configure PyPI publishing

**Files:**
- New: `.github/workflows/release.yaml`
- Modify: `.github/workflows/ci.yaml`

### Phase 7: npm Package (Optional)

**Tasks:**
1. Create npm package structure
2. Add TypeScript types
3. Create `create-vertebra` template
4. Publish to npm

**Files:**
- New: `packages/vertebra/package.json`
- New: `packages/create-vertebra/`

---

## Testing Strategy

### Unit Tests

**Python:**
```python
# tests/test_build.py
def test_builder_generates_types():
    builder = Builder(config_path="tests/fixtures/vertebra.toml")
    await builder.generate_types()
    assert Path("client/types.ts").exists()

def test_supervisor_starts_engine():
    supervisor = Supervisor()
    supervisor.start(engine_path="bin/vertebra-engine", ...)
    assert supervisor.is_running()
```

**Rust:**
```rust
#[test]
fn test_supervisor_spawn() {
    let mut supervisor = Supervisor::new();
    // Test process spawning
}
```

### Integration Tests

```python
# tests/integration/test_build_one_file.py
@pytest.mark.integration
def test_build_one_file_linux():
    result = subprocess.run([
        "vertebra", "build", "--one-file",
        "--target", "linux-x64",
        "--output", "dist/myapp"
    ])
    assert result.returncode == 0
    assert Path("dist/myapp").exists()

    # Test offline execution
    result = subprocess.run(
        ["./dist/myapp", "--help"],
        env={"HOME": "/tmp/isolated"}
    )
    assert result.returncode == 0
```

### End-to-End Tests

```python
# tests/e2e/test_full_workflow.py
@pytest.mark.e2e
def test_init_build_run_workflow(tmp_path):
    """Test complete workflow: init -> build -> run"""
    # 1. Initialize project
    result = subprocess.run([
        "vertebra", "init", "testapp",
        "--package-manager", "uv",
        "--no-interactive"
    ], cwd=tmp_path)
    assert result.returncode == 0

    project_dir = tmp_path / "testapp"

    # 2. Build single-file binary
    result = subprocess.run([
        "vertebra", "build", "--one-file",
        "--output", "dist/testapp"
    ], cwd=project_dir)
    assert result.returncode == 0
    assert (project_dir / "dist/testapp").exists()

    # 3. Run the binary (offline)
    result = subprocess.run(
        [str(project_dir / "dist/testapp"), "--help"],
        env={"HOME": str(tmp_path / "isolated")},
        capture_output=True,
    )
    assert result.returncode == 0
    assert b"vertebra" in result.stdout.lower()
```

### Event Loop Compatibility Tests

```python
# tests/test_event_loop_compat.py
@pytest.mark.parametrize("loop_impl", ["asyncio", "uvloop", "rloop"])
def test_supervisor_with_event_loop(loop_impl):
    """Verify supervisor works with different event loops"""
    if loop_impl == "uvloop":
        pytest.importorskip("uvloop")
        import uvloop
        uvloop.install()
    elif loop_impl == "rloop":
        pytest.importorskip("rloop")
        import rloop
        rloop.install()

    from vertebra._vertebra import Supervisor

    supervisor = Supervisor()
    # Supervisor should not interfere with event loop
    loop = asyncio.get_event_loop()
    assert loop is not None

    # Start should release GIL properly
    supervisor.start(engine_path="bin/vertebra-engine", config={})
    assert supervisor.is_running()
    supervisor.stop()
```

### Cross-Platform Build Tests

```python
# tests/test_cross_build.py
@pytest.mark.parametrize("target", [
    "linux-x64",
    "linux-arm64",
    "darwin-x64",
    "darwin-arm64",
    "windows-x64",
])
def test_cross_compile_target(target, tmp_path):
    """Test cross-compilation to different targets"""
    result = subprocess.run([
        "vertebra", "build", "--one-file",
        "--target", target,
        "--output", f"dist/myapp-{target}"
    ], cwd=tmp_path)

    # Build should succeed (may not run on current platform)
    assert result.returncode == 0

    expected_ext = ".exe" if "windows" in target else ""
    assert (tmp_path / f"dist/myapp-{target}{expected_ext}").exists()
```

### Coverage Targets

| Component | Target |
|-----------|--------|
| `vertebra.build` | 90% |
| `vertebra.cli` | 85% |
| `vertebra-py/supervisor.rs` | 80% |
| Integration tests | All platforms |

### Offline Build Verification

All builds must work completely offline. CI tests this by:

1. Building in a network-isolated container
2. Verifying no external network requests
3. Running the resulting binary in isolation

```yaml
# .github/workflows/ci.yaml
- name: Test offline build
  run: |
    # Create isolated network namespace
    unshare --net -- vertebra build --one-file --output dist/myapp
    # Verify binary runs offline
    unshare --net -- ./dist/myapp --version
```

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
        config: PyObject,
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

**Issue:** Original PRD conflated Vertebra's PyPI publishing with user app builds.

**Resolution:** Clearly separate two scenarios:

| Scenario | Who | Command | Output |
|----------|-----|---------|--------|
| **Framework Distribution** | Vertebra maintainers | `cibuildwheel` in CI | PyPI wheels |
| **User App Distribution** | End users | `vertebra build --one-file` | Standalone binary |

**Framework Distribution (PyPI):**
- Follow sqlspec's cibuildwheel + hatch-mypyc pattern
- Embed baseline Bun binaries in wheels (max compatibility)
- Pip auto-selects best wheel for user's platform

**User App Distribution:**
- User chooses `--profile baseline|modern|native`
- We bundle python-build-standalone + Bun binary matching their choice
- Full control over optimization vs compatibility tradeoff

#### 2. Supervisor Process Spawning (FIXED)

**Issue:** `Command::new("python")` ignores sys.executable, breaks in venv and one-file mode.

**Resolution:** Use sys.executable in wheel mode, re-exec pattern in one-file mode:

```rust
fn start(&mut self, py: Python, engine_path: &str, config: PyObject) -> PyResult<()> {
    // Get the correct Python interpreter
    let executable: String = py.import("sys")?.getattr("executable")?.extract()?;

    py.allow_threads(|| {
        // For workers, use the actual Python executable (not "python")
        for _ in 0..self.worker_count {
            self.workers.push(
                Command::new(&executable)
                    .arg("-m")
                    .arg("vertebra.server.worker")
                    .spawn()?
            );
        }
        Ok(())
    })
}

// For one-file mode, re-exec self:
fn start_one_file(&mut self, py: Python) -> PyResult<()> {
    let current_exe = std::env::current_exe()?;
    py.allow_threads(|| {
        for _ in 0..self.worker_count {
            self.workers.push(
                Command::new(&current_exe)
                    .arg("--worker")  // Special flag for worker mode
                    .spawn()?
            );
        }
        Ok(())
    })
}
```

#### 3. Config File Contradiction (RESOLVED)

**Issue:** PRD referenced `vertebra.toml` in supervisor code but stated "runtime config in Python code."

**Resolution:** Remove all `vertebra.toml` references. Config is passed as Python object:

```python
# Before (wrong)
supervisor.start(engine_path=..., config_path="vertebra.toml")

# After (correct)
supervisor.start(engine_path=..., config=app.get_supervisor_config())
```

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

**Resolution:** Platform-specific process management in Rust supervisor:

```rust
#[cfg(windows)]
fn setup_process_group(&self) -> Result<()> {
    use windows::Win32::System::JobObjects::*;
    // Create job object to manage child processes
    // Ensures children terminate when parent exits
}

#[cfg(unix)]
fn setup_process_group(&self) -> Result<()> {
    // Use process groups and SIGTERM propagation
}
```

#### 6. pyembed Packaging Mechanics (SPECIFIED)

**Issue:** No specification for resource layout, import hooks, native extensions.

**Resolution:** Added specification:

```
Embedded Binary Layout:
├── Python interpreter (from python-build-standalone)
├── Standard library (frozen/compressed)
├── Site-packages (user dependencies)
│   ├── Pure Python: embedded as-is
│   └── Native extensions: platform-specific .so/.dylib/.pyd
├── Application code (user's src/py/<project>/)
├── Bun engine binary
└── Static resources (templates, assets)

Import Strategy:
- OxidizedFinder for embedded modules
- Fallback to filesystem for development
- No extraction to temp (everything in-memory where possible)

Native Extension Policy:
- Pre-compiled for each target platform
- Distributed via platform-specific wheels
- SSL/TLS: bundled certificates (certifi-style)
- Extraction required for .so/.dylib/.pyd (dlopen needs filesystem path)

Extraction Directory (uses app name, NOT "pyembed"):
- Example: $TMPDIR/cool-web-app-a1b2c3/ (if app named "cool-web-app")
- Fallback: $TMPDIR/vertebra-a1b2c3/ (if no name available)

Directory precedence (with fallback on write failure):
1. $VERTEBRA_CACHE_DIR (explicit override)
2. $XDG_CACHE_HOME/<app-name>-<hash>/
3. ~/.cache/<app-name>-<hash>/
4. tempfile.gettempdir()/<app-name>-<hash>/  (Python's platform temp)
5. /tmp/<app-name>-<hash>/ (last resort)

If a directory is not writable, automatically try the next one.

Where:
- <app-name> = [project].name from pyproject.toml
- <hash> = short hash of binary path (for isolation when multiple versions)

Caching behavior:
- First run: Extract native extensions to cache directory
- Subsequent runs: Skip extraction, load directly from cache
- Cache invalidation: Hash changes if binary is replaced/updated
- Startup time: ~50ms first run (extraction), ~5ms subsequent (cached)
```

### PyPI Wheel Size Strategy

**Issue:** Bun binary (~90MB) may exceed PyPI limits (default 60MB, hard 100MB).

**Resolution:** Split package architecture:

```
vertebra (main package, ~5MB)
├── Python code
├── Rust extension (.so)
└── Depends on: vertebra-engine

vertebra-engine (platform-specific, ~80MB)
├── Pre-compiled Bun engine binary
└── One wheel per platform
```

This keeps the main package small and allows aggressive caching of the engine.

---

## Success Metrics

| Metric | Target |
|--------|--------|
| `pip install vertebra` time | < 30s |
| `vertebra init` time | < 5s |
| `vertebra build --one-file` time | < 60s |
| Wheel size (with Bun) | < 100MB |
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

## Open Questions (Resolved)

1. **Wheel size**: Large size (~80MB) is acceptable for better UX
2. **Offline builds**: Fully supported, required for production
3. **Python version**: 3.12+ only (no 3.11 support)
4. **Windows priority**: Included in initial release (x64 only)

---

## Appendix

### A. File Inventory

**New Files:**
- `src/rs/vertebra-py/src/supervisor.rs`
- `src/rs/vertebra-py/src/embedded.rs` (optional)
- `src/py/vertebra/build/__init__.py`
- `src/py/vertebra/build/builder.py`
- `src/py/vertebra/cli/init.py`
- `src/py/vertebra/templates/`
- `tools/build-engine.ts`
- `tools/bundle-standalone.py`
- `.github/workflows/release.yaml`

**Modified Files:**
- `src/rs/vertebra-py/Cargo.toml`
- `src/rs/vertebra-py/src/lib.rs`
- `src/py/vertebra/cli.py`
- `src/py/vertebra/supervisor.py`
- `pyproject.toml`
- `.github/workflows/ci.yaml`

### B. Dependency Additions

**Rust:**
- `pyembed = "0.26"` (optional)
- `signal-hook = "0.3"`

**Python:**
- `hatchling` (build backend)
- `click` (CLI framework, if not already)

**TypeScript:**
- (none additional - uses built-in Bun.build)

### C. References

- [Bun Single-file Executables](https://bun.sh/docs/bundler/executables)
- [PyOxidizer / pyembed Documentation](https://pyoxidizer.readthedocs.io/en/stable/)
- [python-build-standalone Releases (Astral)](https://github.com/astral-sh/python-build-standalone/releases)
- [Hatch Build Workflow (signing pattern)](https://github.com/pypa/hatch/blob/master/.github/workflows/build-hatch.yml)
- [rcodesign (Rust Apple signing)](https://github.com/indygreg/apple-platform-rs/tree/main/apple-codesign)
- [sqlspec publish.yml (cibuildwheel pattern)](https://github.com/litestar-org/sqlspec/blob/main/.github/workflows/publish.yml)
- [cibuildwheel Documentation](https://cibuildwheel.readthedocs.io/)
- [hatch-mypyc Plugin](https://github.com/ofek/hatch-mypyc)
