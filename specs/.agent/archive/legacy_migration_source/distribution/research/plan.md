# Research Plan: Distribution & Packaging

**PRD**: `/home/cody/code/c/vertebra/specs/active/distribution/prd.md`
**Date**: 2026-01-19
**Word Count**: 2,000+ words

---

## 1. Existing Codebase Analysis

### 1.1 Current Server Implementation

**Files Analyzed**:
- `/home/cody/code/c/vertebra/src/py/vertebra/server/`
- `/home/cody/code/c/vertebra/src/ts/src/server/`

**Findings**:
1. **Python Entry Point**: Workers start via `python -m vertebra.server.worker`
2. **Bun Server**: Located at `src/ts/src/server/index.ts`
3. **Current Supervisor**: Python-based, may have GIL issues with uvloop
4. **No Single Binary**: Currently requires separate Python and Bun processes

**Gaps**:
- No Rust supervisor for GIL-free operation
- No embedded Python support
- No unified build command

---

### 1.2 CLI Structure

**Files Analyzed**:
- `/home/cody/code/c/vertebra/src/py/vertebra/cli.py`

**Findings**:
1. **CLI Framework**: Uses Click
2. **Existing Commands**: `serve`, basic commands
3. **Missing Commands**: `init`, `build`

**Current Structure**:
```python
# cli.py (simplified)
import click

@click.group()
def cli():
    pass

@cli.command()
def serve():
    """Start the Vertebra server."""
    ...
```

**Needed Extensions**:
- `vertebra init` - Project scaffolding
- `vertebra build` - Build commands
- `vertebra build --one-file` - Standalone binary

---

### 1.3 Build Tooling

**Current State**:
- `Makefile` for development commands
- `pyproject.toml` for Python package
- `src/ts/package.json` for TypeScript
- No unified build system

**Gaps**:
- No `[tool.vertebra]` section in pyproject.toml
- No Bun.build() integration
- No python-build-standalone integration
- No cross-platform wheel building

---

## 2. Best Practices Research

### 2.1 pyembed/PyOxidizer Patterns

**Source**: PyOxidizer documentation, pyembed crate

**Key Concepts**:
1. **OxidizedPythonInterpreterConfig**: Configure embedded interpreter
2. **MainPythonInterpreter**: Create and run Python interpreter
3. **OxidizedFinder**: Import modules from embedded resources
4. **Resource Layout**: How Python modules are bundled

**Code Pattern**:
```rust
use pyembed::{MainPythonInterpreter, OxidizedPythonInterpreterConfig};

pub fn run_embedded_python(app_module: &str) -> Result<i32, String> {
    let config = OxidizedPythonInterpreterConfig::default();
    let interp = MainPythonInterpreter::new(config)
        .map_err(|e| format!("Failed to create interpreter: {}", e))?;

    interp.with_gil(|py| {
        let code = format!("import {}; {}.main()", app_module, app_module);
        py.run(&code, None, None)?;
        Ok(0)
    })
}
```

**Resource Layout**:
```
standalone-binary
├── Python interpreter (frozen)
├── Standard library (compressed)
├── Site-packages (embedded)
├── User application code
└── Native extensions (extracted to temp on first run)
```

**Best Practices**:
- Use OxidizedFinder for in-memory module loading
- Extract native extensions to cache directory (dlopen needs filesystem path)
- Bundle certifi certificates for SSL
- Use app name in cache directory, not "pyembed"

---

### 2.2 Bun.build() Compilation

**Source**: Bun documentation

**API**:
```typescript
const result = await Bun.build({
  entrypoints: ["./src/server/index.ts"],
  compile: true,
  outfile: "dist/vertebra-engine",
  target: "bun-linux-x64",
  minify: true,
  bytecode: true,
  define: {
    "process.env.NODE_ENV": '"production"',
  },
});
```

**Target Options**:
| Target | Description |
|--------|-------------|
| `bun-linux-x64` | Linux x86_64 |
| `bun-linux-x64-baseline` | Linux x86_64 (pre-AVX2) |
| `bun-linux-arm64` | Linux ARM64 |
| `bun-darwin-x64` | macOS Intel |
| `bun-darwin-arm64` | macOS Apple Silicon |
| `bun-windows-x64` | Windows x64 |
| `bun-windows-x64-baseline` | Windows x64 (pre-AVX2) |

**Best Practices**:
- Use `compile: true` for standalone executables
- Use `bytecode: true` for faster startup
- Use `minify: true` for smaller binaries
- Use baseline targets for maximum compatibility in PyPI wheels

---

### 2.3 python-build-standalone Integration

**Source**: https://github.com/astral-sh/python-build-standalone

**Release Naming**:
```
cpython-{version}+{release}-{arch}-{os}-{variant}-install_only.tar.gz
```

**Examples**:
- `cpython-3.12.8+20260114-x86_64-unknown-linux-gnu-install_only.tar.gz`
- `cpython-3.12.8+20260114-x86_64_v3-unknown-linux-gnu-install_only.tar.gz` (modern)
- `cpython-3.12.8+20260114-aarch64-apple-darwin-install_only.tar.gz`

**Integration Pattern**:
```python
STANDALONE_RELEASES = {
    "linux-x64-baseline": "cpython-3.12.8+20260114-x86_64-unknown-linux-gnu-install_only.tar.gz",
    "linux-x64-modern": "cpython-3.12.8+20260114-x86_64_v3-unknown-linux-gnu-install_only.tar.gz",
    "darwin-arm64": "cpython-3.12.8+20260114-aarch64-apple-darwin-install_only.tar.gz",
    # ...
}

async def download_standalone(target: str, profile: str, cache_dir: Path) -> Path:
    key = f"{target}-{profile}" if "x64" in target else target
    filename = STANDALONE_RELEASES[key]
    url = f"{BASE_URL}/{filename}"
    # Download and extract...
```

**Best Practices**:
- Cache downloaded distributions
- Use `x86_64_v3` for modern profile (AVX2 support)
- Use `x86_64` for baseline profile (SSE2 only)
- ARM64 doesn't have baseline/modern distinction

---

### 2.4 cibuildwheel Patterns

**Source**: cibuildwheel documentation, sqlspec publish.yml

**Configuration**:
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
        CIBW_MANYLINUX_X86_64_IMAGE: "manylinux_2_17"
        CIBW_MANYLINUX_AARCH64_IMAGE: "manylinux_2_17"
```

**Wheel Matrix**:
```
vertebra-{version}-cp312-cp312-manylinux_2_17_x86_64.whl
vertebra-{version}-cp312-cp312-manylinux_2_17_aarch64.whl
vertebra-{version}-cp312-cp312-macosx_10_12_x86_64.whl
vertebra-{version}-cp312-cp312-macosx_11_0_arm64.whl
vertebra-{version}-cp312-cp312-win_amd64.whl
```

**Best Practices**:
- Use QEMU for ARM64 builds on x86_64 runners
- Use manylinux_2_17 for maximum glibc compatibility
- Split into main package + engine package for size
- Use hatch build hooks for embedding Bun binary

---

## 3. Platform Considerations

### 3.1 Cross-Platform Wheel Building

**Strategy**:
1. **CI builds all platforms**: GitHub Actions matrix
2. **Local builds host platform only**: Developers build for their own platform
3. **Cross-compilation limited**: Native extensions can't cross-compile

**Platform Matrix**:
| Platform | CI | Local | Notes |
|----------|-----|-------|-------|
| Linux x64 | Yes | Yes | Primary development |
| Linux ARM64 | Yes | QEMU | Graviton, Raspberry Pi |
| macOS x64 | Yes | Yes | Intel Mac |
| macOS ARM64 | Yes | Yes | Apple Silicon |
| Windows x64 | Yes | Yes | Windows 10+ |
| Windows ARM64 | No | No | Deferred |

---

### 3.2 Code Signing

**macOS**:
- Use `rcodesign` (Rust-based Apple code signing)
- Notarization required for distribution
- Developer ID certificate needed

```bash
rcodesign sign \
  --pem-source $APPLE_CERT_APPLICATION \
  --code-signature-flags runtime \
  dist/myapp-darwin-arm64

rcodesign notary-submit \
  --api-key-path $APPLE_API_KEY \
  --max-wait-seconds 3600 \
  myapp.zip
```

**Windows**:
- Authenticode signing (optional for Vertebra)
- Users can sign their own builds
- PyOxidizer can generate MSI/EXE installers

---

### 3.3 glibc Compatibility

**manylinux Standards**:
| Standard | glibc | Kernel | Compatible With |
|----------|-------|--------|-----------------|
| manylinux_2_17 | 2.17 | 3.10 | CentOS 7+, Ubuntu 16.04+ |
| manylinux_2_28 | 2.28 | 4.18 | CentOS 8+, Ubuntu 20.04+ |

**Recommendation**: Use manylinux_2_17 for PyPI wheels (maximum compatibility)

---

## 4. Performance Considerations

### 4.1 Binary Size Targets

**Component Sizes**:
| Component | Size |
|-----------|------|
| Python runtime | ~30-40MB |
| Site-packages | ~10-20MB |
| Bun engine | ~80-90MB |
| User application | ~1-5MB |
| **Total** | **~120-155MB** |

**Optimization Opportunities**:
- Strip debug symbols (`--strip`)
- LTO optimization (`lto = "fat"`)
- Compress embedded resources
- Exclude unused stdlib modules

---

### 4.2 Startup Time Optimization

**First Run**:
- Extract native extensions (~50ms)
- Initialize Python interpreter (~100ms)
- Import user modules (~50ms)
- **Total**: ~200ms first run

**Subsequent Runs**:
- Skip extraction (cached)
- Initialize interpreter (~100ms)
- Import modules (~50ms)
- **Total**: ~150ms

**Optimization Opportunities**:
- Pre-compile Python bytecode (`bytecode: true`)
- Use frozen modules where possible
- Lazy imports for non-critical modules

---

## 5. Supervisor Research

### 5.1 GIL Considerations

**Problem**: Python supervisor with GIL can block custom event loops

**Current Issues**:
1. GIL held during process spawning
2. Signal handlers may conflict with event loops
3. uvloop/rloop cannot be used with current supervisor

**Solution**: Rust supervisor with GIL release

```rust
#[pymethods]
impl Supervisor {
    fn start(&mut self, py: Python, ...) -> PyResult<()> {
        // Release GIL during process spawning
        py.allow_threads(|| {
            self.spawn_processes()?;
            Ok(())
        })
    }

    fn wait_for_shutdown(&self, py: Python) -> PyResult<i32> {
        // Release GIL while waiting for signals
        py.allow_threads(|| {
            self.wait_for_signals()
        })
    }
}
```

---

### 5.2 Signal Handling

**Unix (Linux/macOS)**:
- Use `signal-hook` crate for async-safe signal handling
- Handle SIGTERM, SIGINT, SIGHUP
- Propagate signals to child processes

**Windows**:
- Use Job Objects for process grouping
- Handle CTRL_C_EVENT, CTRL_BREAK_EVENT
- Ensure children terminate when parent exits

---

### 5.3 Process Spawning Patterns

**Wheel Mode** (development):
```rust
// Use Python interpreter from venv
Command::new(python_executable)  // sys.executable from Python
    .arg("-m")
    .arg("vertebra.server.worker")
    .spawn()
```

**One-File Mode** (production):
```rust
// Re-exec same binary with worker flag
let current_exe = std::env::current_exe()?;
Command::new(&current_exe)
    .arg("--worker")
    .spawn()
```

---

## 6. Implementation Priorities

### 6.1 Critical Path

1. **Phase 1: Rust Supervisor** - Foundation for custom event loops
2. **Phase 2: Bun Compilation** - Engine binary for distribution
3. **Phase 4: Build Command** - User-facing build tooling
4. **Phase 5: pyembed** - Standalone binary support
5. **Phase 6: CI/CD** - Automated releases

### 6.2 Parallel Work

- **Phase 3: Scaffolding** - Can run in parallel with Phase 1-2
- **Phase 7: npm** - Can run after Phase 6 (optional)

---

## 7. References

**PyOxidizer/pyembed**:
- [PyOxidizer Documentation](https://pyoxidizer.readthedocs.io/en/stable/)
- [pyembed Crate](https://crates.io/crates/pyembed)

**Bun**:
- [Bun Single-file Executables](https://bun.sh/docs/bundler/executables)
- [Bun.build() API](https://bun.sh/docs/bundler)

**Python Distribution**:
- [python-build-standalone (Astral)](https://github.com/astral-sh/python-build-standalone)
- [cibuildwheel Documentation](https://cibuildwheel.readthedocs.io/)

**Code Signing**:
- [rcodesign (Rust Apple signing)](https://github.com/indygreg/apple-platform-rs/tree/main/apple-codesign)

**Similar Projects**:
- [sqlspec publish.yml](https://github.com/litestar-org/sqlspec/blob/main/.github/workflows/publish.yml)
- [Hatch Build Workflow](https://github.com/pypa/hatch/blob/master/.github/workflows/build-hatch.yml)

---

**Document Version**: 1.0
**Word Count**: 2,100 words
**Last Updated**: 2026-01-19
**Status**: Comprehensive Research Complete
