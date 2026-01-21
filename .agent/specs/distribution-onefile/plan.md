# Tasks: Distribution & Packaging

**PRD**: `/home/cody/code/c/vertebra/specs/active/distribution/prd.md`
**Status**: Active - Critical Path to 1.0
**Estimated Duration**: 4-6 weeks

---

## Phase 1: Rust Supervisor (Week 1)

### Task Group 1.1: Supervisor Implementation

**Priority**: P0 (Foundation for all other phases)
**Dependencies**: None

- [ ] **Task 1.1.1**: Create supervisor module structure
  - Create `src/rs/vertebra-py/src/supervisor.rs`
  - Add module declaration to `src/rs/vertebra-py/src/lib.rs`
  - Add dependencies to `src/rs/vertebra-py/Cargo.toml`:
    - `signal-hook = "0.3"` (Unix signal handling)
    - `windows = { version = "0.52", features = ["Win32_System_JobObjects"] }` (Windows)

- [ ] **Task 1.1.2**: Implement `Supervisor` struct
  - Create `Supervisor` PyClass with `bun_process` and `workers` fields
  - Implement `#[new]` constructor
  - Export to Python module

- [ ] **Task 1.1.3**: Implement process spawning with GIL release
  - Implement `start()` method with `py.allow_threads()`
  - Accept `python_executable` parameter (sys.executable from Python)
  - Accept `config` as Python object (not file path)
  - Spawn Bun engine process
  - Spawn Python worker processes

- [ ] **Task 1.1.4**: Implement Unix signal handling
  - Use `signal-hook` crate for SIGTERM/SIGINT
  - Implement `wait_for_shutdown()` with GIL release
  - Implement graceful child process termination

- [ ] **Task 1.1.5**: Implement Windows process management
  - Use Windows Job Objects for process grouping
  - Ensure child processes terminate when parent exits
  - Handle Windows-specific signal equivalents

- [ ] **Task 1.1.6**: Update Python supervisor wrapper
  - Modify `src/py/vertebra/supervisor.py`
  - Import Rust `Supervisor` class
  - Pass `sys.executable` to Rust supervisor
  - Pass runtime config as Python object

- [ ] **Task 1.1.7**: Test with uvloop and rloop
  - Create test that installs uvloop before Vertebra startup
  - Verify supervisor doesn't interfere with event loop
  - Test same compatibility for rloop
  - File: `src/py/tests/test_supervisor_eventloop.py` (NEW)

**Acceptance**:
- Supervisor spawns Bun engine and workers
- GIL is released during all blocking operations
- uvloop/rloop compatibility verified
- Graceful shutdown works on Linux, macOS, Windows

---

## Phase 2: Bun Engine Compilation (Week 1-2)

### Task Group 2.1: Build Script Implementation

**Priority**: P0
**Dependencies**: None (can run in parallel with Phase 1)

- [ ] **Task 2.1.1**: Create build-engine.ts script
  - File: `tools/build-engine.ts`
  - Implement `BuildOptions` interface
  - Implement `buildEngine()` function using `Bun.build()`

- [ ] **Task 2.1.2**: Implement target mapping
  - Implement `getBunTarget()` function
  - Map to: bun-linux-x64, bun-linux-arm64, bun-darwin-x64, bun-darwin-arm64, bun-windows-x64
  - Handle baseline variants for x64 targets

- [ ] **Task 2.1.3**: Add bytecode and minification options
  - Add `minify: boolean` option
  - Add `bytecode: boolean` option
  - Configure production build settings

- [ ] **Task 2.1.4**: Create CLI wrapper
  - File: `tools/build-cli.ts`
  - Parse command-line arguments
  - Call `buildEngine()` with options
  - Output build results

- [ ] **Task 2.1.5**: Add to package scripts
  - Update `src/ts/package.json`
  - Add `build:engine` script
  - Add `build:engine:all` script for all platforms

**Acceptance**:
- `bun run build:engine` produces working binary
- All platform targets build successfully
- Baseline variants work on older CPUs
- Binary size within acceptable range (~80MB)

---

## Phase 3: Project Scaffolding (Week 2)

### Task Group 7.1: CLI Implementation

**Priority**: P0
**Dependencies**: None

- [ ] **Task 7.1.0**: Refactor CLI structure
  - Move `src/py/vertebra/cli.py` to `src/py/vertebra/cli/__init__.py` (or main entry point)
  - Split `serve` command into `src/py/vertebra/cli/serve.py`
  - Split `build` command into `src/py/vertebra/cli/build.py`
  - Ensure `vertebra` command still works

- [ ] **Task 7.1.1**: Create template structure
  - Create `src/py/vertebra/templates/default/`
  - Add `pyproject.toml.jinja2`
  - Add `app.py.jinja2`
  - File: `src/py/vertebra/templates/`

- [ ] **Task 7.1.2**: Implement `vertebra init`
  - File: `src/py/vertebra/cli/init.py`
  - Interactive prompts (rich/click)
  - Template rendering
  - Update `src/py/vertebra/cli/__init__.py` to register command

- [ ] **Task 3.1.7**: Add scaffolding tests
  - Test `vertebra init myapp` creates valid project
  - Test all package managers produce working projects
  - Test TypeScript integration compiles
  - File: `src/py/tests/test_cli_init.py` (NEW)

**Acceptance**:
- `vertebra init myproject` creates working project
- All package managers supported (uv, poetry, hatch, pdm, pip)
- Generated project structure matches spec
- TypeScript integration works

---

## Phase 4: Unified Build Command (Week 2-3)

### Task Group 4.1: Builder Module Implementation

**Priority**: P0
**Dependencies**: Phase 1 (Supervisor), Phase 2 (Bun Compilation)

- [ ] **Task 4.1.1**: Create builder module structure
  - Create `src/py/vertebra/build/__init__.py`
  - Create `src/py/vertebra/build/config.py`
  - Create `src/py/vertebra/build/builder.py`

- [ ] **Task 4.1.2**: Implement BuildConfig
  - Parse `[tool.vertebra]` from pyproject.toml
  - Support profile (modern/baseline/native)
  - Support target platform overrides
  - File: `config.py`

- [ ] **Task 4.1.3**: Implement type generation
  - `builder.generate_types()` method
  - Generate TypeScript types from Python procedures
  - Output to `client/types.ts`
  - File: `builder.py`

- [ ] **Task 4.1.4**: Implement engine compilation
  - `builder.compile_engine()` method
  - Call Bun build script
  - Handle cross-platform targets
  - File: `builder.py`

- [ ] **Task 4.1.5**: Implement wheel building
  - `builder.build_wheel()` method
  - Use hatchling as build backend
  - Embed compiled engine in wheel
  - File: `builder.py`

- [ ] **Task 4.1.6**: Implement one-file build
  - `builder.build_one_file()` method
  - Bundle Python runtime (python-build-standalone)
  - Bundle all dependencies
  - Bundle Bun engine
  - File: `builder.py`

- [ ] **Task 4.1.7**: Create build CLI command
  - File: `src/py/vertebra/cli/build.py`
  - Support `--one-file`, `--target`, `--profile`, `--output`
  - Support `--all-targets` for CI

- [ ] **Task 4.1.8**: Add builder tests
  - Test `vertebra build` succeeds
  - Test `vertebra build --one-file` produces working binary
  - Test cross-platform targets
  - Test profile options affect build
  - File: `src/py/tests/test_build.py` (NEW)

**Acceptance**:
- `vertebra build` succeeds
- `vertebra build --one-file` produces working binary (~80-150MB)
- Cross-platform targets work
- Profile options affect build output

---

## Phase 5: pyembed Integration (Week 3-4)

### Task Group 5.1: Embedded Python

**Priority**: P0 (Required for `--one-file`)
**Dependencies**: Phase 4 (Build Command)

- [ ] **Task 5.1.1**: Add pyembed dependency
  - Update `src/rs/vertebra-py/Cargo.toml`
  - Add `pyembed = { version = "0.26", optional = true }`
  - Add `embedded` and `standalone` features

- [ ] **Task 5.1.2**: Implement embedded Python runner
  - Create `src/rs/vertebra-py/src/embedded.rs`
  - Use `MainPythonInterpreter` from pyembed
  - Implement `run_embedded_python()` function
  - Support module loading from embedded resources

- [ ] **Task 5.1.3**: Create standalone binary builder
  - File: `tools/bundle-standalone.py`
  - Download python-build-standalone release
  - Bundle Python runtime + site-packages + user app
  - Bundle Bun engine binary
  - Bundle SSL certificates (certifi)

- [ ] **Task 5.1.4**: Implement resource layout
  - Configure OxidizedFinder for embedded modules
  - Handle native extension extraction
  - Implement cache directory with app name

- [ ] **Task 5.1.5**: Test offline operation
  - Test binary runs in network-isolated environment
  - Test no external Python required
  - Test binary size under 150MB
  - File: `src/py/tests/test_standalone.py` (NEW)

**Acceptance**:
- Standalone binary includes Python runtime
- No external Python required to run
- Offline operation verified
- Binary size under 150MB

---

## Phase 6: CI/CD Pipeline (Week 4)

### Task Group 6.1: Release Workflow

**Priority**: P1
**Dependencies**: Phases 1-5

- [ ] **Task 6.1.1**: Create wheel build workflow
  - File: `.github/workflows/release.yaml`
  - Use cibuildwheel for cross-platform builds
  - Build for Linux x64/arm64, macOS x64/arm64, Windows x64

- [ ] **Task 6.1.2**: Add cross-platform matrix
  - Configure matrix for all target platforms
  - Use QEMU for ARM64 builds
  - Configure manylinux_2_17 for glibc compatibility

- [ ] **Task 6.1.3**: Implement offline testing
  - Test builds in network-isolated containers
  - Verify no network calls during execution
  - Test standalone binary runs offline

- [ ] **Task 6.1.4**: Configure PyPI publishing
  - Split packages: `vertebra` + `vertebra-engine`
  - Publish main package (~5MB)
  - Publish engine package per platform (~80MB each)

- [ ] **Task 6.1.5**: Add npm publishing (optional)
  - Publish TypeScript types to npm
  - Publish `create-vertebra` template

- [ ] **Task 6.1.6**: Add code signing
  - macOS: Use rcodesign for signing and notarization
  - Windows: Use PyOxidizer for MSI/EXE installers
  - Configure secrets for certificates

**Acceptance**:
- All platform wheels build in CI
- Wheels install correctly via pip
- Offline tests pass
- PyPI publishing works on tag
- macOS binaries signed and notarized

---

## Phase 7: npm Package (Week 5, Optional)

### Task Group 7.1: TypeScript Package

**Priority**: P2
**Dependencies**: Phases 1-6

- [ ] **Task 7.1.1**: Create npm package structure
  - File: `packages/vertebra/package.json`
  - Configure exports and types

- [ ] **Task 7.1.2**: Add TypeScript types
  - File: `packages/vertebra/src/index.ts`
  - Export types for Python procedure integration

- [ ] **Task 7.1.3**: Create `create-vertebra` template
  - Directory: `packages/create-vertebra/`
  - Support `bun create vertebra myproject`

- [ ] **Task 7.1.4**: Publish to npm
  - Configure npm publishing in CI
  - Version sync with Python package

**Acceptance**:
- `bun create vertebra myapp` works
- TypeScript types are accurate
- npm package installs correctly

---

## Summary

**Total Tasks**: 42+
**Estimated Duration**: 4-6 weeks
**Critical Path**: Phase 1 -> Phase 2 -> Phase 4 -> Phase 5 -> Phase 6

**Dependencies**:
- Phase 1 (Supervisor): No dependencies
- Phase 2 (Bun Compilation): No dependencies
- Phase 3 (Scaffolding): No dependencies (parallel)
- Phase 4 (Build Command): Depends on Phase 1, 2
- Phase 5 (pyembed): Depends on Phase 4
- Phase 6 (CI/CD): Depends on Phase 1-5
- Phase 7 (npm): Depends on Phase 1-6 (optional)

**Testing Strategy**:
- Unit tests after each task group
- Integration tests after each phase
- Platform-specific tests in CI
- Offline verification tests

---

**Document Version**: 1.0
**Last Updated**: 2026-01-19
**Status**: Ready for Implementation
