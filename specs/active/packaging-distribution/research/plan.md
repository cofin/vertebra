# Research: Packaging and Distribution System

## Executive Summary

This document analyzes the packaging and distribution strategy for Vertebra, a Python-first application framework with Rust IPC primitives and a Bun server layer. The research covers three major concerns: (1) packaging Vertebra for distribution like gunicorn/uvicorn, (2) bundling Rust and Bun components for end users, and (3) enabling users to build their own single-file executables with `vertebra build --onefile`.

## Research Sources

- PyApp documentation (ofek.dev/pyapp)
- Google Cloud Platform Database Assessment repository (bundle_python.py, CI workflows)
- Hatch repository (build-hatch.yml, binary targets)
- Existing Vertebra codebase (bundler.py, standalone.py, build-engine.ts)
- Python Build Standalone (indygreg/python-build-standalone)

---

## Part 1: Vertebra Distribution Strategy

### Current State Analysis

Vertebra currently has partial infrastructure for building single-file executables:

1. **PyApp Configuration** (pyproject.toml lines 124-133):
   ```toml
   [tool.hatch.build.targets.binary]
   scripts = ["vertebra"]
   pyapp-version = "0.27.0"
   python-version = "3.12"

   [tool.hatch.build.targets.binary.env]
   PYAPP_DISTRIBUTION_EMBED = "1"
   PYAPP_FULL_ISOLATION = "1"
   PYAPP_UV_ENABLED = "1"
   ```

2. **Maturin Build Backend** (line 84-91):
   - Builds Rust extension (`vertebra._vertebra`) via PyO3
   - Manifest at `src/rs/vertebra-py/Cargo.toml`

3. **Bundler System** (vertebra/build/bundler.py):
   - Downloads Python Build Standalone archives
   - Installs dependencies via `uv pip install`
   - Bundles Bun engine via `build-engine.ts`

### Component Distribution Requirements

#### 1. Rust Native Extension

The Rust extension (`vertebra._vertebra`) provides:
- Shared memory ring buffers (SPSC/MPMC)
- Arrow IPC helpers
- Sync primitives for cross-process communication

**Distribution Options:**

| Option | Pros | Cons |
|--------|------|------|
| Pre-built wheels | Fast install, no Rust needed | Must build for all platforms/Python versions |
| Source distribution | Single artifact | Requires Rust toolchain on user machine |
| Bundled in PyApp | Self-contained | Larger binary size |

**Recommendation:** Pre-built wheels published to PyPI for standard installs, with maturin handling the build. For `--onefile` builds, the extension is bundled into the standalone Python environment.

#### 2. Bun Engine Binary

The Bun engine (`vertebra-engine`) is a compiled TypeScript server that:
- Handles HTTP requests
- Manages connection pools (Postgres, MySQL, SQLite, Redis)
- Bridges ASGI via shared memory

**Current Build Process** (build-engine.ts):
```typescript
const result = await Bun.build({
  entrypoints: [source.entry],
  compile: true,
  outfile,
  target: bunTarget as any,
  minify: options.minify,
  bytecode: options.bytecode,
});
```

**Distribution Options:**

| Option | Pros | Cons |
|--------|------|------|
| Pre-built binaries in wheel | Zero runtime compilation | Large wheel size (~50MB per platform) |
| Build at install time | Smaller distribution | Requires Bun on user machine |
| Separate download at runtime | Smallest wheel | Network dependency, versioning complexity |
| Build on demand | Lazy loading | Slow first run |

**Recommendation:** A hybrid approach:
1. **Standard install (`pip install vertebra`)**: Engine built at first `vertebra serve` if not present
2. **One-file build**: Engine pre-compiled and bundled
3. **Optional pre-built download**: `vertebra engine install` fetches platform binary from releases

---

## Part 2: PyApp Integration Strategy

### PyApp Overview

PyApp is a Rust-based wrapper that:
1. Embeds or downloads a Python distribution at runtime
2. Installs the application and dependencies on first run
3. Creates a self-extracting single-file executable

### Key Configuration Variables

| Variable | Purpose | Vertebra Usage |
|----------|---------|----------------|
| `PYAPP_DISTRIBUTION_PATH` | Local Python archive path | Use PBS archive for target platform |
| `PYAPP_DISTRIBUTION_EMBED` | Embed Python in binary | Enable for true single-file |
| `PYAPP_SKIP_INSTALL` | Skip pip install at runtime | Enable (pre-bundle deps) |
| `PYAPP_FULL_ISOLATION` | Full distribution copies | Enable for clean isolation |
| `PYAPP_UV_ENABLED` | Use uv for installs | Enable for fast installs |
| `PYAPP_EXEC_SPEC` | Entry point spec | `vertebra.cli:main` |
| `PYAPP_EXEC_CODE` | Custom execution code | Optional for advanced bootstrap |

### DMA Pattern Analysis

The Database Assessment (DMA) project uses a sophisticated bundling approach:

**bundle_python.py Key Patterns:**

1. **Pre-bundle Dependencies:**
   ```python
   # Install all deps into PBS site-packages BEFORE PyApp wrapping
   subprocess.run([
       "uv", "pip", "install", ".",
       "--target", str(site_packages),
       "--system"
   ])
   ```

2. **Skip Runtime Install:**
   ```bash
   PYAPP_SKIP_INSTALL=true  # Deps already in place
   ```

3. **Custom Install Location:**
   ```bash
   # DMA allows custom install paths beyond PyApp defaults
   PYAPP_INSTALLATION_PATH=/custom/path
   ```

4. **Platform Matrix:**
   ```yaml
   matrix:
     include:
       - os: ubuntu-latest
         target: x86_64-unknown-linux-gnu
       - os: ubuntu-latest
         target: aarch64-unknown-linux-gnu
       - os: macos-latest
         target: aarch64-apple-darwin
       - os: macos-latest
         target: x86_64-apple-darwin
       - os: windows-latest
         target: x86_64-pc-windows-msvc
   ```

### Hatch Binary Target Pattern

Hatch's own distribution demonstrates:

1. **Build Matrix Configuration** (build-hatch.yml):
   ```yaml
   - name: Build binaries
     run: |
       hatch build --target binary
   ```

2. **Cross-Compilation with cargo-zigbuild:**
   ```bash
   cargo zigbuild --target x86_64-unknown-linux-gnu.2.17
   ```
   The `.2.17` suffix pins glibc version for broad compatibility.

3. **Artifact Upload Strategy:**
   ```yaml
   - uses: actions/upload-artifact@v4
     with:
       name: standalone-${{ matrix.target }}
       path: dist/
   ```

---

## Part 3: User Build Commands

### `vertebra build` (Directory Distribution)

Creates a distribution folder containing:
```
dist/
├── python/           # PBS runtime
│   ├── bin/
│   │   ├── python3.12
│   │   └── vertebra-engine
│   └── lib/python3.12/site-packages/
│       ├── vertebra/
│       └── <user app>/
├── run.sh            # Launch script (Unix)
└── run.bat           # Launch script (Windows)
```

**Use Cases:**
- Container deployments (copy dist/ into image)
- System package creation (deb, rpm)
- Manual deployment to servers

### `vertebra build --onefile` (Single Executable)

Creates a self-extracting executable using PyApp:

1. **Build Phase:**
   - Download PBS for target platform
   - Install dependencies into PBS site-packages
   - Compile Bun engine for target platform
   - Copy engine binary into PBS bin/
   - Copy user application code
   - Create PyApp wrapper with PYAPP_SKIP_INSTALL=true

2. **Runtime Behavior:**
   - First run: Extract to cache directory (~/.cache/vertebra-apps/<hash>)
   - Subsequent runs: Use cached extraction (fast startup)
   - Full isolation: Each version gets own directory

3. **Size Optimization:**
   - Strip debug symbols from binaries
   - Use modern CPU profile (x86_64_v3) where possible
   - Exclude test files and __pycache__

### Target Platform Matrix

| Target | Python PBS | Bun Target | Notes |
|--------|------------|------------|-------|
| linux-x64 | x86_64-unknown-linux-gnu | bun-linux-x64 | Most common |
| linux-x64-baseline | x86_64-unknown-linux-gnu | bun-linux-x64-baseline | Old CPUs |
| linux-arm64 | aarch64-unknown-linux-gnu | bun-linux-arm64 | ARM servers |
| darwin-arm64 | aarch64-apple-darwin | bun-darwin-arm64 | Apple Silicon |
| darwin-x64 | x86_64-apple-darwin | bun-darwin-x64 | Intel Macs |
| windows-x64 | x86_64-pc-windows-msvc | bun-windows-x64 | Windows |

---

## Part 4: Project Initialization Enhancements

### Current `vertebra init` Limitations

The current init command creates minimal scaffolding:
- Basic `pyproject.toml`
- Empty `app.py` with Vertebra instance
- Simple README

### Proposed Enhancements

#### 1. Template Variants

```bash
vertebra init myapp                    # Default: minimal API
vertebra init myapp --template api     # REST API with examples
vertebra init myapp --template web     # Full web app with frontend
vertebra init myapp --template minimal # Current behavior
```

#### 2. Modern Landing Page Template

The `--template web` option should include:

```
myapp/
├── src/
│   ├── py/
│   │   └── myapp/
│   │       ├── __init__.py
│   │       ├── app.py           # Vertebra application
│   │       ├── handlers.py      # Route handlers
│   │       └── templates/       # Jinja2 templates
│   └── ts/
│       ├── package.json
│       └── src/
│           └── index.ts         # Frontend entrypoint (optional)
├── static/
│   └── styles.css               # Modern CSS (Tailwind-ready)
├── pyproject.toml
└── README.md
```

Landing page features:
- Responsive design with modern CSS
- Vertebra branding (Litestar colors: slate blue/gold)
- Example procedure endpoints
- Health check endpoint
- Development mode detection (hot reload notice)

#### 3. Package Manager Detection

```python
def detect_package_manager(project_dir: Path) -> str:
    if (project_dir / "uv.lock").exists():
        return "uv"
    if (project_dir / "poetry.lock").exists():
        return "poetry"
    if (project_dir / "pdm.lock").exists():
        return "pdm"
    return "uv"  # Default to uv
```

---

## Part 5: CI/CD Pipeline Design

### Release Workflow

```yaml
name: Release Binaries

on:
  release:
    types: [published]

jobs:
  build:
    strategy:
      matrix:
        include:
          - os: ubuntu-latest
            target: linux-x64
            rust_target: x86_64-unknown-linux-gnu
          - os: ubuntu-latest
            target: linux-arm64
            rust_target: aarch64-unknown-linux-gnu
          - os: macos-latest
            target: darwin-arm64
            rust_target: aarch64-apple-darwin
          - os: macos-latest
            target: darwin-x64
            rust_target: x86_64-apple-darwin
          - os: windows-latest
            target: windows-x64
            rust_target: x86_64-pc-windows-msvc

    runs-on: ${{ matrix.os }}

    steps:
      - name: Build Rust extension
        run: maturin build --release --target ${{ matrix.rust_target }}

      - name: Build Bun engine
        run: bun run build:engine --target ${{ matrix.target }}

      - name: Build PyApp binary
        run: hatch build --target binary

      - name: Upload artifacts
        uses: actions/upload-artifact@v4
```

### Cross-Compilation Considerations

For Linux ARM64 from x64 runner:
```yaml
- name: Setup cross-compilation
  run: |
    sudo apt-get install -y gcc-aarch64-linux-gnu
    rustup target add aarch64-unknown-linux-gnu
```

Using cargo-zigbuild for glibc compatibility:
```yaml
- name: Install cargo-zigbuild
  run: cargo install cargo-zigbuild

- name: Build with old glibc
  run: cargo zigbuild --target x86_64-unknown-linux-gnu.2.17 --release
```

---

## Part 6: Avoiding npm/crates.io Publication

### TypeScript Components

The Bun engine TypeScript code is compiled to a single binary. No npm publication needed because:
1. Users don't import TypeScript directly
2. Engine is pre-compiled for each platform
3. Build-time dependency only (Bun)

### Rust Components

The Rust extension is built via maturin as a Python wheel. No crates.io publication needed because:
1. vertebra-core, vertebra-js, vertebra-py are internal crates
2. Users interact via Python API
3. Wheels include compiled extension

### Dependency Isolation

```toml
[project.optional-dependencies]
# Only Python dependencies exposed to users
postgres = ["asyncpg>=0.29"]
mysql = ["asyncmy>=0.2"]
redis = ["redis>=5.0"]
```

---

## Part 7: Implementation Phases

### Phase 1: Enhanced Init Templates
- Create `templates/web/` with modern landing page
- Add `--template` option to init command
- Generate TypeScript scaffold for frontend

### Phase 2: Build Command Improvements
- Implement `vertebra build` for directory distribution
- Add launch script generation (run.sh, run.bat)
- Support `--target` for cross-platform builds

### Phase 3: One-File Build Pipeline
- Integrate PyApp with PYAPP_SKIP_INSTALL
- Pre-bundle all dependencies
- Bundle Bun engine binary
- Support custom install locations

### Phase 4: CI/CD Release Pipeline
- GitHub Actions workflow for 5-platform matrix
- Artifact upload to releases
- Automatic checksums

### Phase 5: Engine Distribution
- `vertebra engine install` command
- Download pre-built engine from releases
- Checksum verification
- Version management

---

## Part 8: User Experience Considerations

### First-Run Experience

When a user runs `pip install vertebra` and then `vertebra serve app:app`, the experience should be:

1. **No Rust toolchain required** - Pre-built wheels include the compiled extension
2. **No Bun installation required** - Engine built on first serve if not present
3. **Clear progress feedback** - Rich console output for any downloads/builds
4. **Graceful degradation** - If engine build fails, provide clear error with recovery steps

### Build Time Expectations

For `vertebra build --onefile`:

| Step | Expected Duration | Notes |
|------|-------------------|-------|
| PBS download | 5-30 seconds | ~30MB compressed, cached |
| Dependency install | 10-60 seconds | Depends on project size |
| Engine compilation | 15-45 seconds | Bun is fast |
| PyApp wrapping | 5-15 seconds | Rust compilation |
| **Total** | 35-150 seconds | First build only |

Subsequent builds with cached PBS and node_modules are significantly faster.

### Error Messages and Recovery

Build errors should include clear recovery hints showing users exactly what went wrong and how to fix it. This includes checking for missing dependencies like Bun or Rust, network issues when downloading PBS, and permission problems when creating output directories.

---

## Part 9: Version Management

### Semantic Versioning Alignment

All three components (Python package, Rust extension, Bun engine) should share the same version:

The existing bump-my-version configuration already handles this via `[[tool.bumpversion.files]]` entries for pyproject.toml and package.json.

### Compatibility Matrix

| Vertebra Version | Python | PBS Version | Bun Version |
|-----------------|--------|-------------|-------------|
| 0.1.x | 3.12+ | 20240415 | 1.1+ |
| 0.2.x | 3.12+ | 20241001 | 1.2+ |

### Engine Version Checking

At runtime, Python should verify engine compatibility by invoking the engine binary with a --version flag and comparing against the expected version string.

---

## Part 10: Security Considerations

### Binary Verification

All downloaded binaries (PBS, engine) should be verified:

1. **Checksum validation** - SHA256 sums for all artifacts
2. **Signature verification** - GPG signatures for releases (optional)
3. **TLS verification** - All downloads over HTTPS

### Sandbox Extraction

The current standalone.py already implements path traversal protection by verifying that extracted paths remain within the target directory. This pattern must be preserved in any refactoring.

### Permissions

Extracted binaries should have appropriate executable permissions (0755) to ensure they can run on Unix systems.

---

## Part 11: Comparison with Similar Tools

### vs Gunicorn

| Aspect | Gunicorn | Vertebra |
|--------|----------|----------|
| Language | Pure Python | Python + Rust + Bun |
| Installation | `pip install` | `pip install` (with native ext) |
| Single-file | Not supported | `vertebra build --onefile` |
| Workers | Fork-based | Process + shared memory |

### vs Uvicorn

| Aspect | Uvicorn | Vertebra |
|--------|---------|----------|
| Protocol | ASGI over sockets | ASGI over shared memory |
| Performance | Good | Higher (zero-copy IPC) |
| Bundled DB | No | Connection pools included |

### vs PyInstaller

| Aspect | PyInstaller | Vertebra Build |
|--------|-------------|----------------|
| Approach | Freeze + analyze imports | PBS + PyApp |
| Native extensions | Bundled | Pre-compiled in wheel |
| Cross-compilation | Limited | Full matrix support |
| Runtime | Modified Python | Unmodified PBS |

---

## Part 12: Testing Strategy

### Unit Tests

Bundler tests should verify:
- Output directory creation
- PBS download and extraction
- Dependency installation into site-packages
- Engine binary inclusion in final bundle
- Launch script generation

### Integration Tests

End-to-end tests should:
- Build a complete executable for the current platform
- Run the executable and wait for startup
- Send HTTP requests to verify functionality
- Test graceful shutdown

### CI Matrix Tests

Each platform in the CI matrix should run:
1. Build the binary
2. Execute it and verify startup
3. Run health check
4. Verify graceful shutdown

---

## Conclusion

The proposed packaging and distribution system enables Vertebra to be distributed like gunicorn/uvicorn while supporting advanced single-file builds for end users. Key decisions:

1. **Use PyApp with pre-bundled dependencies** (PYAPP_SKIP_INSTALL) for fastest runtime startup
2. **Bundle Bun engine at build time** rather than runtime download
3. **Avoid npm/crates.io** by compiling all non-Python components into binaries
4. **Support 5 platforms** via CI matrix (linux-x64, linux-arm64, darwin-arm64, darwin-x64, windows-x64)
5. **Provide modern templates** for project initialization with full web stack option

This approach balances distribution simplicity (single pip install) with powerful build capabilities for production deployments. The hybrid strategy allows users to start quickly with standard pip install while providing a clear path to production-ready single-file executables when needed.
