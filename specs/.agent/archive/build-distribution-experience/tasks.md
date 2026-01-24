# PRD-9 Task Breakdown: Build System & Distribution Experience

**Status:** Pending Implementation
**PRD Reference:** [prd.md](./prd.md)
**Research Reference:** [research/plan.md](./research/plan.md)

---

## Implementation Order

Tasks are ordered by dependency chain. Each phase must pass its quality gates before proceeding.

---

## Phase 1: Rust Supervisor

**Goal:** Replace Python-based supervisor with GIL-free Rust implementation

### Tasks

#### 1.1 Create Supervisor Struct
- **File:** `src/rs/vertebra-py/src/supervisor.rs`
- **Description:** Implement core `Supervisor` struct with process management
- **Dependencies:** None
- **Tests:** Unit tests for struct creation and state management

```rust
#[pyclass]
pub struct Supervisor {
    bun_process: Option<Child>,
    workers: Vec<Child>,
    config: SupervisorConfig,
}
```

#### 1.2 Implement Process Spawning with GIL Release
- **File:** `src/rs/vertebra-py/src/supervisor.rs`
- **Description:** Implement `start()` method using `py.allow_threads()`. **IMPORTANT:** Use `sys.executable` passed from Python, NOT `Command::new("python")`. For one-file mode, use re-exec pattern with `std::env::current_exe()`.
- **Dependencies:** 1.1
- **Tests:** Integration test verifying GIL is released during spawn, test in venv, test one-file mode

#### 1.3 Implement Signal Handling (Unix)
- **File:** `src/rs/vertebra-py/src/supervisor.rs`
- **Description:** Handle SIGTERM/SIGINT in Rust for graceful shutdown on Unix
- **Dependencies:** 1.2
- **Cargo additions:** `signal-hook = "0.3"`
- **Tests:** Test signal propagation to child processes

#### 1.3b Implement Process Management (Windows)
- **File:** `src/rs/vertebra-py/src/supervisor.rs`
- **Description:** Use Windows Job Objects to manage child processes. Ensures children terminate when parent exits, avoiding orphan processes.
- **Dependencies:** 1.2
- **Cargo additions:** `windows = { version = "0.52", features = ["Win32_System_JobObjects"] }`
- **Tests:** Test Ctrl+C handling, test process cleanup on exit

#### 1.4 Export to Python Module
- **File:** `src/rs/vertebra-py/src/lib.rs`
- **Description:** Add `Supervisor` to PyO3 module exports
- **Dependencies:** 1.3
- **Tests:** Python import test

#### 1.5 Update Python Supervisor Wrapper
- **File:** `src/py/vertebra/supervisor.py`
- **Description:** Update to use Rust `Supervisor` class
- **Dependencies:** 1.4
- **Tests:** Existing supervisor tests should pass

#### 1.6 Test with Event Loops
- **File:** `src/py/tests/test_supervisor.py`
- **Description:** Add tests for uvloop and rloop compatibility
- **Dependencies:** 1.5
- **Tests:** Parametrized tests with asyncio/uvloop/rloop

### Phase 1 Quality Gates
- [ ] All unit tests pass
- [ ] uvloop compatibility verified
- [ ] GIL release verified with timing tests
- [ ] Graceful shutdown works on all platforms

---

## Phase 2: Bun Engine Compilation

**Goal:** Implement programmatic Bun compilation using `Bun.build()`

### Tasks

#### 2.1 Create Build Engine Script
- **File:** `tools/build-engine.ts`
- **Description:** Implement `buildEngine()` function with target support
- **Dependencies:** None
- **Tests:** Unit test for build configuration generation

```typescript
export async function buildEngine(options: BuildOptions): Promise<string>
```

#### 2.2 Add Target Mapping
- **File:** `tools/build-engine.ts`
- **Description:** Implement `getBunTarget()` for cross-platform targets
- **Dependencies:** 2.1
- **Tests:** Test all target combinations

#### 2.3 Add Bytecode and Minification Options
- **File:** `tools/build-engine.ts`
- **Description:** Wire bytecode and minify options to Bun.build
- **Dependencies:** 2.2
- **Tests:** Verify minified output is smaller

#### 2.4 Create CLI Wrapper
- **File:** `tools/build-cli.ts`
- **Description:** CLI entry point for `bun run tools/build-cli.ts`
- **Dependencies:** 2.3
- **Tests:** CLI integration test

#### 2.5 Add to Package Scripts
- **File:** `src/ts/package.json`
- **Description:** Add `build:engine` script
- **Dependencies:** 2.4
- **Tests:** `bun run build:engine` works

### Phase 2 Quality Gates
- [ ] All platforms build successfully
- [ ] Baseline targets work on older CPUs
- [ ] Build output is a valid executable
- [ ] Cross-compilation from macOS/Linux/Windows works

---

## Phase 3: Project Scaffolding

**Goal:** Implement `vertebra init` with proper monorepo structure

### Tasks

#### 3.1 Create Template Directory
- **Files:**
  - `src/py/vertebra/templates/default/`
  - `src/py/vertebra/templates/default/src/py/__project__/`
  - `src/py/vertebra/templates/default/src/ts/`
- **Description:** Create template files with `__project__` placeholders
- **Dependencies:** None

#### 3.2 Create pyproject.toml Template
- **File:** `src/py/vertebra/templates/default/pyproject.toml.jinja2`
- **Description:** Template with hatch.build.sources = ["src/py"]
- **Dependencies:** 3.1

```toml
[project]
name = "{{ project_name }}"
dependencies = ["vertebra"]

[tool.hatch.build]
sources = ["src/py"]

[tool.vertebra]
profile = "{{ profile }}"
```

#### 3.3 Create Package Manager Templates
- **Files:**
  - `src/py/vertebra/templates/managers/uv.toml.jinja2`
  - `src/py/vertebra/templates/managers/poetry.toml.jinja2`
  - `src/py/vertebra/templates/managers/hatch.toml.jinja2`
  - `src/py/vertebra/templates/managers/pdm.toml.jinja2`
- **Description:** Additional config for each package manager
- **Dependencies:** 3.2

#### 3.4 Create TypeScript Templates
- **Files:**
  - `src/py/vertebra/templates/typescript/package.json.jinja2`
  - `src/py/vertebra/templates/typescript/tsconfig.json`
  - `src/py/vertebra/templates/typescript/src/ts/src/index.ts`
- **Description:** TypeScript project setup
- **Dependencies:** 3.1

#### 3.5 Implement Init Command
- **File:** `src/py/vertebra/cli/init.py`
- **Description:** Implement `vertebra init` with prompts
- **Dependencies:** 3.1-3.4
- **Tests:** Test template rendering

```python
@click.command()
@click.argument("name", required=False)
@click.option("--package-manager", type=click.Choice(["uv", "poetry", "hatch", "pdm", "pip"]))
@click.option("--with-typescript", is_flag=True)
def init(name: str | None, package_manager: str, with_typescript: bool):
    """Initialize a new Vertebra project."""
```

#### 3.6 Register CLI Command
- **File:** `src/py/vertebra/cli.py`
- **Description:** Add `init` to main CLI group
- **Dependencies:** 3.5
- **Tests:** `vertebra init --help` works

#### 3.7 Add Non-Interactive Mode
- **File:** `src/py/vertebra/cli/init.py`
- **Description:** Support `--no-interactive` flag for CI
- **Dependencies:** 3.6
- **Tests:** Test non-interactive project creation

### Phase 3 Quality Gates
- [ ] `vertebra init myapp` creates valid project
- [ ] All package managers produce working projects
- [ ] TypeScript integration compiles
- [ ] Generated project structure matches PRD spec

---

## Phase 4: Unified Build Command

**Goal:** Implement `vertebra build` with one-file output support

### Tasks

#### 4.1 Create Builder Module
- **File:** `src/py/vertebra/build/__init__.py`
- **Description:** Export Builder class and types
- **Dependencies:** Phase 2

#### 4.2 Implement BuildConfig
- **File:** `src/py/vertebra/build/config.py`
- **Description:** Config loading from pyproject.toml `[tool.vertebra]`
- **Dependencies:** 4.1
- **Tests:** Test config resolution order

```python
@dataclass
class BuildConfig:
    profile: str = "modern"
    minify: bool = True
    bytecode: bool = True
    targets: dict[str, TargetConfig] = field(default_factory=dict)
```

#### 4.3 Implement Type Generation
- **File:** `src/py/vertebra/build/builder.py`
- **Description:** Implement `generate_types()` method
- **Dependencies:** 4.2, PRD-2 (types)
- **Tests:** Generated types are valid TypeScript

#### 4.4 Implement Engine Compilation
- **File:** `src/py/vertebra/build/builder.py`
- **Description:** Implement `compile_engine()` calling Bun build
- **Dependencies:** 4.3, Phase 2
- **Tests:** Engine binary is executable

#### 4.5 Implement Wheel Building
- **File:** `src/py/vertebra/build/builder.py`
- **Description:** Implement `build_wheel()` with embedded engine
- **Dependencies:** 4.4
- **Tests:** Wheel installs and runs

#### 4.6 Implement One-File Build
- **File:** `src/py/vertebra/build/builder.py`
- **Description:** Implement `build_one_file()` combining all artifacts
- **Dependencies:** 4.5
- **Tests:** Standalone binary works offline

#### 4.7 Create Build CLI Command
- **File:** `src/py/vertebra/cli/build.py`
- **Description:** Implement `vertebra build` command
- **Dependencies:** 4.6

```python
@click.command()
@click.option("--one-file", is_flag=True)
@click.option("--target", type=click.Choice([...]))
@click.option("--profile", type=click.Choice(["modern", "baseline", "native"]))
@click.option("--output", type=click.Path())
def build(one_file: bool, target: str | None, profile: str, output: str | None):
    """Build Vertebra application."""
```

#### 4.8 Register CLI Command
- **File:** `src/py/vertebra/cli.py`
- **Description:** Add `build` to main CLI group
- **Dependencies:** 4.7
- **Tests:** `vertebra build --help` works

### Phase 4 Quality Gates
- [ ] `vertebra build` succeeds
- [ ] `vertebra build --one-file` produces working binary
- [ ] Cross-platform targets work
- [ ] Profile options affect build

---

## Phase 5: pyembed Integration

**Goal:** Enable fully embedded Python distribution for standalone single-file binaries (required for `--one-file`)

### Tasks

#### 5.1 Add pyembed Dependency
- **File:** `src/rs/vertebra-py/Cargo.toml`
- **Description:** Add optional pyembed feature
- **Dependencies:** Phase 1

```toml
[dependencies]
pyembed = { version = "0.26", optional = true }

[features]
embedded = ["pyembed"]
standalone = ["embedded"]
```

#### 5.2 Implement Embedded Python Runner
- **File:** `src/rs/vertebra-py/src/embedded.rs`
- **Description:** Implement `run_embedded_python()` function
- **Dependencies:** 5.1
- **Tests:** Embedded interpreter runs Python code

#### 5.3 Create Standalone Binary Builder
- **File:** `tools/bundle-standalone.py`
- **Description:** Script to package Python + app + engine
- **Dependencies:** 5.2
- **Tests:** Bundle includes all dependencies

#### 5.4 Test Offline Operation
- **File:** `src/py/tests/integration/test_standalone.py`
- **Description:** Test standalone binary in network-isolated environment
- **Dependencies:** 5.3
- **Tests:** Binary runs without network access

### Phase 5 Quality Gates
- [ ] Standalone binary includes Python runtime
- [ ] No external Python required to run
- [ ] Offline operation verified
- [ ] Size under 150MB

---

## Phase 6: CI/CD Pipeline

**Goal:** Automate wheel building and publishing for all platforms

### Tasks

#### 6.1 Create Wheel Build Workflow
- **File:** `.github/workflows/release.yaml`
- **Description:** Matrix build for all platform wheels
- **Dependencies:** Phases 1-4

#### 6.2 Add Cross-Platform Matrix
- **File:** `.github/workflows/release.yaml`
- **Description:** Build matrix for Linux/macOS/Windows × x64/arm64
- **Dependencies:** 6.1

```yaml
strategy:
  matrix:
    os: [ubuntu-latest, macos-latest, windows-latest]
    arch: [x64, arm64]
    profile: [modern, baseline]
```

#### 6.3 Implement Offline Testing
- **File:** `.github/workflows/ci.yaml`
- **Description:** Add network-isolated build and run tests
- **Dependencies:** 6.2

#### 6.4 Configure PyPI Publishing
- **File:** `.github/workflows/release.yaml`
- **Description:** Publish wheels to PyPI on release
- **Dependencies:** 6.3
- **Secrets:** `PYPI_API_TOKEN`

#### 6.5 Add npm Publishing (Optional)
- **File:** `.github/workflows/release.yaml`
- **Description:** Publish TypeScript package to npm
- **Dependencies:** 6.4
- **Secrets:** `NPM_TOKEN`

#### 6.6 Add Code Signing (macOS/Windows)
- **File:** `.github/workflows/release.yaml`
- **Description:** Add rcodesign for macOS, optional Authenticode for Windows
- **Dependencies:** 6.4
- **Secrets:** `APPLE_CERT_APPLICATION`, `APPLE_API_KEY`, optionally `WINDOWS_SIGNING_CERT`

### Phase 6 Quality Gates
- [ ] All platform wheels build in CI (cibuildwheel)
- [ ] Wheels install correctly via pip
- [ ] Offline tests pass
- [ ] PyPI publishing works on tag
- [ ] macOS binaries are signed and notarized
- [ ] Windows installers generated (MSI/EXE)

---

## Phase 7: npm Package (Optional)

**Goal:** Enable `bun create vertebra` for TypeScript-first projects

### Tasks

#### 7.1 Create npm Package Structure
- **File:** `packages/vertebra/package.json`
- **Description:** npm package with TypeScript client
- **Dependencies:** Phase 2

#### 7.2 Add TypeScript Types
- **File:** `packages/vertebra/src/index.ts`
- **Description:** Export generated types and client utilities
- **Dependencies:** 7.1

#### 7.3 Create create-vertebra Template
- **Directory:** `packages/create-vertebra/`
- **Description:** Template for `bun create vertebra`
- **Dependencies:** 7.2

#### 7.4 Publish to npm
- **File:** `.github/workflows/release.yaml`
- **Description:** Add npm publish step
- **Dependencies:** 7.3, 6.5

### Phase 7 Quality Gates
- [ ] `bun create vertebra myapp` works
- [ ] TypeScript types are accurate
- [ ] npm package installs correctly

---

## Verification Checklist

### Pre-Implementation
- [ ] All dependencies identified
- [ ] File paths verified
- [ ] Test strategy documented

### Post-Implementation
- [ ] All tests pass (`make test-all`)
- [ ] Documentation updated
- [ ] CHANGELOG entry added
- [ ] PRD marked as complete

---

## Estimated Effort

| Phase | Tasks | Complexity | Notes |
|-------|-------|------------|-------|
| 1 | 7 | Medium | Core dependency, includes Windows job objects |
| 2 | 5 | Low | Uses existing Bun.build API |
| 3 | 7 | Medium | Many templates to create |
| 4 | 8 | High | Integration with all components |
| 5 | 5 | High | Required for single-file builds, resource layout |
| 6 | 6 | Medium | CI configuration, split packages |
| 7 | 4 | Low | Optional, npm ecosystem |

**Critical Path:** Phase 1 → Phase 2 → Phase 5 → Phase 4 → Phase 6

**Parallelizable:** Phase 3 (after Phase 1)

---

## Codebase Patterns (from review)

The codebase has migrated from dataclasses to msgspec. Follow these patterns:

```python
# Domain models: msgspec.Struct with frozen=True
class BuildConfig(msgspec.Struct, frozen=True):
    profile: str = "baseline"
    minify: bool = True

# Internal tools/state: dataclasses allowed
@dataclass
class ProcessHandle:
    pid: int
    started_at: float
```

**Key resources discovered:**
- [python-build-standalone](https://github.com/astral-sh/python-build-standalone/releases) - Now under Astral
- [Hatch build-hatch.yml](https://github.com/pypa/hatch/blob/master/.github/workflows/build-hatch.yml) - Signing pattern
- [sqlspec publish.yml](https://github.com/litestar-org/sqlspec/blob/main/.github/workflows/publish.yml) - cibuildwheel pattern

---

## Consensus Review Changes

The following changes were incorporated based on Gemini 3 Pro and GPT 5.2 review:

1. **Wheel strategy:** PyPI gets baseline-only wheels (x86-64, manylinux_2_17). Modern profile for CI artifacts only.
2. **Supervisor spawning:** Use `sys.executable` not `"python"`. Re-exec pattern for one-file mode.
3. **Windows process management:** Added job objects task (1.3b).
4. **Config approach:** Removed all `vertebra.toml` references. Config passed as Python object.
5. **Cross-platform scope:** CI builds all targets; local builds host-only.
6. **Split packages:** `vertebra` (small) + `vertebra-engine` (platform binary) for PyPI size limits.
7. **pyembed mechanics:** Added resource layout specification (Phase 5).
