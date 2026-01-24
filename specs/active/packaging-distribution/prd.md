# PRD: Packaging and Distribution System

## Intelligence Context

### Complexity Assessment

**Level**: High (Complex)
**Estimated Checkpoints**: 12+
**Risk**: Medium (cross-platform builds, multi-language coordination)

### Similar Features Referenced

1. `src/py/vertebra/build/bundler.py` - Existing single-file build orchestration
2. `src/py/vertebra/build/standalone.py` - PBS download and extraction
3. `src/py/vertebra/build/tools/build-engine.ts` - Bun engine compilation
4. `src/py/vertebra/cli/init.py` - Project scaffolding
5. DMA `bundle_python.py` - PyApp + pre-bundling pattern
6. Hatch `build-hatch.yml` - Cross-platform binary CI

### Patterns to Follow

- PyApp with `PYAPP_SKIP_INSTALL=true` for pre-bundled dependencies
- PBS (Python Build Standalone) for portable Python runtime
- Bun `compile: true` for single-file engine binary
- Maturin for Rust extension wheels
- Click CLI patterns with rich-click for user feedback

### Detected Stack

| Component | Technology |
|-----------|------------|
| Python | 3.12+ (maturin, hatch) |
| Rust | vertebra-core, vertebra-py (PyO3) |
| TypeScript | Bun runtime |
| CLI | Click + rich-click |
| Build | maturin, hatch, PyApp |
| Testing | pytest, bun test |
| CI | GitHub Actions |

### Commands

| Action | Command |
|--------|---------|
| Test Python | `make test` |
| Test TypeScript | `make js-test` |
| Lint All | `make lint-all` |
| Build | `make install` |
| Build Engine | `bun run build:engine` |

---

## Problem Statement

### User Problem

Vertebra is a Python-first application framework with Rust IPC primitives and a Bun server layer. Users need to:

1. **Install Vertebra** like any Python package (`pip install vertebra`) without requiring Rust/Bun toolchains
2. **Initialize projects** with modern scaffolding that demonstrates Vertebra's capabilities
3. **Build distributable applications** that include all runtime dependencies
4. **Create single-file executables** for deployment without Python installation on target machines

### Current Gaps

1. **No pre-built wheels** - Users must have Rust to install from source
2. **Minimal init templates** - Current scaffold lacks landing page and best practices
3. **Incomplete build command** - Only `--one-file` partially implemented
4. **No PyApp integration** - Single-file builds don't use optimal bundling strategy
5. **No CI for binaries** - No automated cross-platform binary releases

### Business Value

- **Lower adoption barrier** - pip install just works
- **Faster time-to-production** - Modern templates accelerate development
- **Simplified deployment** - Single executable eliminates dependency management
- **Competitive positioning** - Feature parity with established ASGI servers

### Success Criteria

1. `pip install vertebra` works on all platforms without Rust/Bun
2. `vertebra init myapp` creates functional app with modern landing page
3. `vertebra build` creates distributable directory
4. `vertebra build --onefile` creates working single-file executable
5. Built executables run on machines without Python installed
6. CI produces binaries for 5 platforms on each release

---

## Acceptance Criteria

### AC1: Package Installation

- [ ] `pip install vertebra` succeeds on Linux x64/arm64, macOS x64/arm64, Windows x64
- [ ] Installation does not require Rust toolchain
- [ ] Installation does not require Bun
- [ ] Rust extension (`vertebra._vertebra`) loads correctly
- [ ] `vertebra --version` outputs version string

### AC2: Project Initialization

- [ ] `vertebra init myapp` creates project structure
- [ ] `vertebra init myapp --template web` creates full web app template
- [ ] Generated project includes modern landing page
- [ ] Landing page displays Litestar colors (slate blue/gold used sparingly)
- [ ] Generated pyproject.toml includes vertebra dependency
- [ ] Generated app runs with `vertebra serve`
- [ ] Health endpoint returns 200 OK

### AC3: Directory Build

- [ ] `vertebra build` creates `dist/` directory
- [ ] Distribution includes PBS Python runtime
- [ ] Distribution includes vertebra-engine binary
- [ ] Distribution includes all dependencies in site-packages
- [ ] Distribution includes user application code
- [ ] Launch scripts (`run.sh`, `run.bat`) are generated
- [ ] Distribution runs without external Python

### AC4: Single-File Build

- [ ] `vertebra build --onefile` creates single executable
- [ ] Executable is self-extracting via PyApp
- [ ] First run extracts to cache directory
- [ ] Subsequent runs use cached extraction (fast startup)
- [ ] `--target` flag supports cross-platform builds
- [ ] Executable includes Bun engine
- [ ] Executable runs on machines without Python

### AC5: Cross-Platform Support

- [ ] Builds work for: linux-x64, linux-arm64, darwin-arm64, darwin-x64, windows-x64
- [ ] PBS archives downloaded for correct platform
- [ ] Bun engine compiled for correct platform
- [ ] PyApp wrapper targets correct platform

### AC6: CI/CD Pipeline

- [ ] GitHub Actions workflow builds on release
- [ ] Workflow produces binaries for 5 platforms
- [ ] Artifacts uploaded to GitHub Release
- [ ] SHA256 checksums generated

---

## Technical Approach

### Architecture Overview

```
vertebra init           vertebra build          vertebra build --onefile
      │                       │                           │
      ▼                       ▼                           ▼
┌──────────┐           ┌──────────┐              ┌────────────────┐
│ Template │           │  Bundler │              │  PyApp Builder │
│ Renderer │           │          │              │                │
└──────────┘           └──────────┘              └────────────────┘
      │                       │                           │
      ▼                       ▼                           ▼
  Project/              dist/                    myapp.exe (single)
├── pyproject.toml    ├── python/                     │
├── src/              │   ├── bin/                    ▼
│   └── myapp/        │   │   ├── python3.12    Self-extracting
│       └── app.py    │   │   └── vertebra-engine   PyApp wrapper
└── README.md         │   └── lib/site-packages/
                      ├── run.sh
                      └── run.bat
```

### File Structure Plan

```
src/py/vertebra/
├── build/
│   ├── __init__.py
│   ├── bundler.py          # Enhanced directory bundler
│   ├── onefile.py          # NEW: PyApp single-file builder
│   ├── standalone.py       # PBS manager (existing)
│   ├── engine.py           # NEW: Engine build coordinator
│   └── tools/
│       └── build-engine.ts # Bun compiler (existing)
├── cli/
│   ├── build.py            # Enhanced build command
│   └── init.py             # Enhanced with templates
└── templates/
    ├── default/            # Existing minimal
    ├── api/                # NEW: REST API template
    └── web/                # NEW: Full web app
        ├── pyproject.toml.jinja2
        ├── app.py.jinja2
        ├── handlers.py.jinja2
        ├── templates/
        │   └── index.html.jinja2
        └── static/
            └── styles.css
```

### Pattern References

#### 1. PyApp Pre-Bundling (DMA Pattern)

The OneFileBuilder class will handle building single-file executables using PyApp with pre-bundled dependencies. The process involves:

1. Download PBS for target platform
2. Extract PBS to working directory
3. Install dependencies into PBS site-packages (BEFORE PyApp wrapping)
4. Build and copy Bun engine into the bundle
5. Copy application code
6. Create PyApp wrapper with PYAPP_SKIP_INSTALL=true

#### 2. Engine Compilation (Existing Pattern)

The existing build-engine.ts uses Bun.build with compile: true to create standalone binaries for each target platform.

#### 3. Init Template Rendering (Existing Pattern)

Template rendering uses simple string replacement for Jinja2-style placeholders.

### Database Changes

None required.

### API Endpoints

None required (CLI-only feature).

### Frontend Changes

New template files only:
- `templates/web/templates/index.html.jinja2` - Landing page
- `templates/web/static/styles.css` - Modern CSS

### Documentation Updates

- **Sphinx docs**: Add `docs/guides/packaging.rst` for build guide
- **Skills**: Update `~/.claude/skills/vertebra/` with build patterns

---

## Implementation Details

### Phase 1: Template Enhancement

Create new templates in `src/py/vertebra/templates/web/`:

**index.html.jinja2** (landing page):
- Responsive design
- Litestar color scheme (slate blue primary, gold sparingly)
- Feature highlights with icons
- Quick start code snippet
- Health/status endpoint link

**handlers.py.jinja2**:
- Example procedure endpoint
- Health check handler
- JSON response examples

**styles.css**:
- CSS custom properties for colors
- Responsive grid layout
- Utility classes for common patterns

### Phase 2: Build Command Enhancement

The build command will be enhanced to support both directory builds and single-file builds. The `--onefile` flag triggers PyApp wrapping, while the default creates a distribution directory.

### Phase 3: PyApp Integration

Create `src/py/vertebra/build/onefile.py` with the OneFileBuilder class that:

1. Downloads PBS for target platform using StandaloneManager
2. Installs all dependencies into PBS site-packages
3. Bundles the Bun engine binary
4. Copies application code
5. Creates a distribution archive
6. Wraps with PyApp using PYAPP_SKIP_INSTALL=true and PYAPP_FULL_ISOLATION=true

### Phase 4: Engine Management

Create `src/py/vertebra/build/engine.py` with EngineManager class that:

1. Caches compiled engine binaries in `~/.cache/vertebra/engines/`
2. Builds new engine using build-engine.ts when not cached
3. Supports all 5 target platforms

### Phase 5: CI Pipeline

Create `.github/workflows/release-binaries.yml` with:

- Matrix build for 5 platforms
- Rust toolchain setup
- Bun setup
- uv for Python dependency management
- Maturin for wheel building
- Hatch for binary building
- Artifact upload to releases

---

## Testing Strategy

### Unit Tests (90%+ coverage target)

Test the OneFileBuilder class to verify:
- Executable creation
- Engine binary inclusion
- PYAPP_SKIP_INSTALL behavior
- Cross-platform target handling

Test the InitCommand to verify:
- Web template creation
- Landing page rendering
- Project structure generation

### Integration Tests

End-to-end tests that:
- Build a complete executable for the current platform
- Run the executable and wait for startup
- Send HTTP requests to verify functionality
- Test graceful shutdown

### Test Commands

```bash
# Unit tests
make test

# TypeScript tests
make js-test

# Integration tests (requires Docker for cross-platform)
uv run pytest src/py/tests -m integration --integration

# Build verification
vertebra build --onefile --target native
./dist/vertebra --version
```

---

## Implementation Notes

### Pattern Deviations

1. **No npm/crates.io publication** - Unlike typical multi-language projects, we bundle all non-Python components into binaries. This simplifies user installation at the cost of larger wheel sizes.

2. **PBS over system Python** - For single-file builds, we use Python Build Standalone rather than detecting system Python. This ensures consistent behavior across environments.

### Dependencies

**New Python dependencies**: None (uses existing subprocess, tempfile, shutil)

**Build-time requirements**:
- maturin (already in build group)
- hatch (already in build group)
- Bun (for engine compilation)

### Migration Considerations

1. **Existing build command** - The current `--one-file` flag is renamed to `--onefile` for consistency with other tools
2. **Template location** - New templates are additive; default template unchanged
3. **Cache location** - PBS and engine caches use `~/.cache/vertebra/`

### Edge Cases

1. **Cross-compilation without target toolchain** - Provide clear error with setup instructions
2. **Network offline during PBS download** - Check for cached copy first, fail with helpful message
3. **Insufficient disk space** - PBS + engine + deps can be 200MB+; check before starting
4. **Windows path length limits** - Use short paths in temp directories
5. **Conflicting Python versions** - PBS uses a fixed Python version; warn if project requires different
6. **Binary stripping on macOS** - Apple Silicon binaries need codesigning after stripping
7. **Antivirus false positives** - Windows executables may trigger AV; provide signing guidance

### Performance Considerations

#### Build Performance

| Operation | Cold | Warm (cached) |
|-----------|------|---------------|
| PBS download | 5-30s | 0s |
| PBS extraction | 2-5s | 2-5s |
| Dependency install | 10-60s | 10-60s |
| Engine compilation | 15-45s | 0s |
| PyApp wrapping | 5-15s | 5-15s |
| **Total** | 37-155s | 17-80s |

#### Runtime Performance

- **First run**: 1-3s extraction overhead
- **Subsequent runs**: <100ms startup (same as native Python)
- **Memory**: PBS Python identical to system Python
- **Disk usage**: ~150-300MB per installation (depends on dependencies)

### Security Considerations

1. **Binary verification**: All downloaded binaries should have SHA256 checksums verified
2. **Path traversal protection**: Already implemented in standalone.py; must be preserved
3. **Code signing**: Windows and macOS binaries should be signed for distribution
4. **Dependency scanning**: Built executables should be scanned for known vulnerabilities

### Rollback Strategy

If a build fails partway through:

1. Working directory is cleaned up automatically (tempfile.TemporaryDirectory)
2. Cached PBS archives are not affected
3. Cached engine binaries are not affected
4. User can retry immediately without cleanup

---

## Appendix: Template Designs

### Landing Page (templates/web/templates/index.html.jinja2)

The landing page uses:
- CSS custom properties for Litestar colors
- Responsive flexbox layout
- Feature cards with Unicode icons
- Quick start code block
- Footer with version and links

Color scheme:
- Primary: `#1e3a5f` (deep slate blue)
- Accent: `#d4af37` (gold - used sparingly for highlights only)
- Background: `#f8fafc` (light gray)
- Text: `#1e293b` (dark slate)

### Directory Structure (templates/web/)

```
web/
├── pyproject.toml.jinja2
├── app.py.jinja2           # Main application
├── handlers.py.jinja2      # Route handlers
├── templates/
│   ├── base.html.jinja2    # Base template
│   └── index.html.jinja2   # Landing page
└── static/
    └── styles.css          # CSS (no Jinja needed)
```

---

## Appendix B: PyApp Configuration Reference

### Environment Variables

The following PyApp environment variables are used during the build process:

| Variable | Value | Purpose |
|----------|-------|---------|
| `PYAPP_DISTRIBUTION_PATH` | Path to PBS archive | Local Python distribution to embed |
| `PYAPP_DISTRIBUTION_EMBED` | `1` | Embed distribution in binary (no runtime download) |
| `PYAPP_SKIP_INSTALL` | `true` | Skip pip install at runtime (deps pre-bundled) |
| `PYAPP_FULL_ISOLATION` | `true` | Full distribution copies per installation |
| `PYAPP_UV_ENABLED` | `1` | Use uv for any runtime installs |
| `PYAPP_EXEC_SPEC` | `vertebra.cli:main` | Entry point specification |

### Build Process Flow

1. **PBS Download**: StandaloneManager downloads the appropriate Python Build Standalone archive for the target platform. Archives are cached in `~/.cache/vertebra/pbs/`.

2. **PBS Extraction**: Archive is extracted to a temporary working directory. The extraction includes path traversal protection to prevent malicious archives from writing outside the target directory.

3. **Dependency Installation**: Using the system uv, all project dependencies are installed into the PBS site-packages directory. This happens BEFORE PyApp wrapping, which is the key insight from the DMA pattern.

4. **Engine Bundling**: The Bun engine is compiled for the target platform using build-engine.ts and copied into the PBS bin/ directory.

5. **Application Copy**: The user's application code is copied into site-packages alongside the installed dependencies.

6. **Archive Creation**: The entire PBS directory (now containing Python, dependencies, engine, and application) is compressed into a tar.gz archive.

7. **PyApp Wrapping**: PyApp is invoked with PYAPP_DISTRIBUTION_PATH pointing to this archive and PYAPP_SKIP_INSTALL=true to prevent any runtime installation.

### Runtime Extraction

When the built executable runs for the first time:

1. PyApp detects it hasn't been extracted yet
2. Creates a unique directory based on the binary hash in `~/.local/share/pyapp/` (or platform equivalent)
3. Extracts the embedded distribution
4. Executes the entry point

Subsequent runs detect the existing extraction and skip directly to execution, providing fast startup times.

---

## Appendix C: Cross-Platform Build Matrix

### Platform Specifications

| Target | OS Runner | Rust Target | PBS Archive | Bun Target |
|--------|-----------|-------------|-------------|------------|
| linux-x64 | ubuntu-latest | x86_64-unknown-linux-gnu | cpython-3.12.3-x86_64-unknown-linux-gnu | bun-linux-x64 |
| linux-x64-baseline | ubuntu-latest | x86_64-unknown-linux-gnu | cpython-3.12.3-x86_64-unknown-linux-gnu | bun-linux-x64-baseline |
| linux-arm64 | ubuntu-latest | aarch64-unknown-linux-gnu | cpython-3.12.3-aarch64-unknown-linux-gnu | bun-linux-arm64 |
| darwin-arm64 | macos-14 | aarch64-apple-darwin | cpython-3.12.3-aarch64-apple-darwin | bun-darwin-arm64 |
| darwin-x64 | macos-13 | x86_64-apple-darwin | cpython-3.12.3-x86_64-apple-darwin | bun-darwin-x64 |
| windows-x64 | windows-latest | x86_64-pc-windows-msvc | cpython-3.12.3-x86_64-pc-windows-msvc-shared | bun-windows-x64 |

### Cross-Compilation Notes

For Linux ARM64 builds from an x64 runner, additional setup is required:

1. Install cross-compilation toolchain: `aarch64-linux-gnu-gcc`
2. Add Rust target: `rustup target add aarch64-unknown-linux-gnu`
3. Configure cargo for cross-compilation in `.cargo/config.toml`

For glibc compatibility with older Linux distributions, cargo-zigbuild can be used to target specific glibc versions (e.g., 2.17 for CentOS 7 compatibility).

### Artifact Naming Convention

Built artifacts follow this naming pattern:

```
vertebra-{version}-{target}{ext}
```

Examples:
- `vertebra-0.1.0-linux-x64`
- `vertebra-0.1.0-darwin-arm64`
- `vertebra-0.1.0-windows-x64.exe`

Checksums are provided as:
- `vertebra-0.1.0-linux-x64.sha256`

---

## Appendix D: Template System Details

### Template Variables

All templates receive the following context variables:

| Variable | Example | Description |
|----------|---------|-------------|
| `project_name` | `My App` | Human-readable project name |
| `project_slug` | `my_app` | Python-safe identifier |
| `vertebra_version` | `0.1.0` | Current Vertebra version |

### Template Types

#### Default Template (Minimal)
- Single `app.py` with Vertebra instance
- Basic `pyproject.toml`
- Minimal README

#### API Template
- Structured handlers module
- Example CRUD endpoints
- Request/response schemas
- Error handling patterns

#### Web Template
- Full frontend scaffolding
- Jinja2 templates
- Static file serving
- Modern CSS with Litestar colors
- Responsive landing page

### Landing Page Features

The web template landing page includes:

1. **Hero Section**: Project name with tagline
2. **Feature Cards**: Highlighting Vertebra capabilities
   - Shared memory IPC (zero-copy)
   - Connection pooling (Postgres, MySQL, Redis)
   - Type-safe procedures
   - Hot reload development

3. **Quick Start**: Code snippet showing basic usage
4. **Status Bar**: Links to health endpoint and documentation
5. **Footer**: Version info and GitHub link

### CSS Architecture

The styles.css file uses CSS custom properties for theming:

```css
:root {
  --color-primary: #1e3a5f;      /* Deep slate blue */
  --color-accent: #d4af37;       /* Gold (use sparingly) */
  --color-background: #f8fafc;   /* Light gray */
  --color-text: #1e293b;         /* Dark slate */
  --color-success: #22c55e;      /* Green for status */
  --color-warning: #eab308;      /* Yellow for warnings */
  --color-error: #ef4444;        /* Red for errors */
}
```

The gold accent color is intentionally limited to:
- Version badge
- "NEW" or highlight labels
- Hover states on interactive elements

This ensures good visibility on both light and dark backgrounds.

---

## Appendix E: Error Handling Strategy

### Build Errors

All build errors should provide:
1. Clear error message describing what failed
2. Context about which step failed
3. Recovery hint with actionable steps

Example error messages:

**Missing Bun**:
```
Error: Bun runtime not found

The Vertebra engine requires Bun to compile.

Recovery:
  curl -fsSL https://bun.sh/install | bash

Then retry: vertebra build --onefile
```

**Network Error**:
```
Error: Failed to download Python Build Standalone

Could not connect to github.com

Recovery:
  1. Check your internet connection
  2. If behind a proxy, set HTTP_PROXY and HTTPS_PROXY
  3. Manually download from:
     https://github.com/indygreg/python-build-standalone/releases
```

**Disk Space**:
```
Error: Insufficient disk space

Building requires approximately 500MB of temporary space.
Available: 128MB

Recovery:
  Free up disk space and retry.
```

### Runtime Errors

For executables built with `--onefile`:

**First Run Extraction**:
```
Vertebra: Extracting runtime (first run only)...
```

**Extraction Failure**:
```
Error: Failed to extract runtime

Could not write to ~/.local/share/pyapp/

Recovery:
  1. Check disk space
  2. Check directory permissions
  3. Set PYAPP_HOME to a different location
```

---

---

## Appendix F: User Workflow Examples

### Example 1: Standard Package Installation

A user wants to install Vertebra and create a new project:

```bash
# Install vertebra (no Rust or Bun required)
pip install vertebra

# Create a new project with web template
vertebra init my-api --template web

# Navigate to project
cd my-api

# Install dependencies
uv sync  # or pip install -e .

# Run the development server
vertebra dev my_api.app:app
```

The user now has a running server with a modern landing page at http://localhost:8000.

### Example 2: Production Build (Directory)

A user wants to build their application for container deployment:

```bash
# Build distribution directory
vertebra build --target linux-x64

# Result structure:
# dist/
# ├── python/
# │   ├── bin/
# │   │   ├── python3.12
# │   │   └── vertebra-engine
# │   └── lib/python3.12/site-packages/
# ├── run.sh
# └── run.bat

# Test the build locally
./dist/run.sh

# Copy to Docker image
COPY dist/ /app/
CMD ["/app/run.sh"]
```

### Example 3: Single-File Executable

A user wants to distribute their application as a single binary:

```bash
# Build single-file executable for current platform
vertebra build --onefile

# Or for a specific target
vertebra build --onefile --target linux-x64

# Result:
# dist/my-api-linux-x64

# Test the executable
./dist/my-api-linux-x64 --version

# First run extracts to ~/.local/share/pyapp/
./dist/my-api-linux-x64 serve --port 8080

# Copy to server and run directly
scp dist/my-api-linux-x64 server:/usr/local/bin/my-api
ssh server '/usr/local/bin/my-api serve --port 8080'
```

### Example 4: CI/CD Integration

A project wants automated builds on release:

```yaml
# .github/workflows/release.yml
name: Release

on:
  release:
    types: [published]

jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Setup Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.12'

      - name: Setup Bun
        uses: oven-sh/setup-bun@v2

      - name: Install vertebra
        run: pip install vertebra

      - name: Build executable
        run: vertebra build --onefile --target linux-x64

      - name: Upload to release
        uses: softprops/action-gh-release@v1
        with:
          files: dist/my-api-linux-x64
```

---

## Appendix G: Comparison with Alternative Approaches

### vs PyInstaller

| Aspect | PyInstaller | Vertebra Build |
|--------|-------------|----------------|
| Approach | Hook-based import analysis | Explicit bundling |
| Native extensions | Bundled via hooks | Pre-compiled in wheel |
| Cross-compilation | Limited, platform-specific | Full matrix support |
| Runtime | Bootloader + modified stdlib | Unmodified PBS |
| Hidden imports | Common issue | Not applicable |
| Binary size | Variable | Predictable |
| Cold start | Fast | 1-3s extraction first time |
| Debugging | Harder (modified runtime) | Easier (standard Python) |

### vs Nuitka

| Aspect | Nuitka | Vertebra Build |
|--------|--------|----------------|
| Approach | Compile to C/C++ | Bundle interpreter |
| Build time | Very long | Moderate |
| Performance | Sometimes faster | Standard Python speed |
| Compatibility | Good but not 100% | 100% (same interpreter) |
| Debugging | Harder | Standard Python debugging |

### vs Shiv/PEX

| Aspect | Shiv/PEX | Vertebra Build |
|--------|---------|----------------|
| Approach | Zipapp with bootstrapping | Full runtime bundle |
| System Python | Required | Not required |
| Native extensions | Must match system | Bundled |
| Portability | Limited by Python version | Fully portable |

---

## References

- [PyApp Documentation](https://ofek.dev/pyapp/)
- [Python Build Standalone](https://github.com/indygreg/python-build-standalone)
- [Maturin User Guide](https://www.maturin.rs/)
- [Bun Build Documentation](https://bun.sh/docs/bundler)
- [DMA Repository](https://github.com/GoogleCloudPlatform/database-assessment)
- [Hatch Binary Builds](https://github.com/pypa/hatch)
