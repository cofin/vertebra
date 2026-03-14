# Spec: Scaffold Base 0.1.0

**Beads ID:** vertebra-w4i.2
**Flow ID:** scaffold-base
**Chapter:** 2 of the Vertebra 0.1.0 Ghost Framework Rewrite
**Depends on:** Ch1 `wipe-slate` (scratch/ must exist)
**Status:** planned

---

## Goal

Create the complete, working 0.1.0 project skeleton from an empty repository root. After Ch1 all source
code lives under `scratch/`; this chapter writes every file from scratch. When this chapter is done the
repository compiles, imports cleanly, and all three toolchains (Python via `uv`, Rust via Cargo, and
linting via ruff/mypy/clippy) report no errors.

---

## Scope and Non-Goals

**In scope:**
- `pyproject.toml` with maturin build backend, core dependencies, and tool configuration
- `Makefile` with the canonical target set
- `src/py/vertebra/__init__.py` exposing `__version__ = "0.1.0"` and `py.typed`
- `src/py/tests/` with empty `__init__.py` and minimal `conftest.py`
- `src/rs/Cargo.toml` workspace (three members)
- Three Rust crate stubs: `vertebra-core`, `vertebra-http`, `vertebra-py`
- Verification gate: `make install`, `make lint`, `make test`, `cargo test`

**Not in scope:**
- Any business logic, CLI code, server code, or FFI bindings (those belong to later chapters)
- Bun/TypeScript toolchain (introduced in a later chapter)
- CI/CD configuration beyond what already exists in `.github/`

---

## Pre-conditions

- `scratch/` exists (created by Ch1)
- `.agent/`, `.beads/`, `.github/`, `.gitignore` are intact
- The working directory root contains none of: `pyproject.toml`, `Makefile`, `src/`
- `uv` >= 0.4, `cargo` >= 1.78, `maturin` >= 1.5 are available on PATH

---

## Post-conditions (Pass/Fail Gate)

All of the following commands succeed (exit 0) against the scaffolded tree:

```
make install          # uv sync succeeds, cargo build succeeds
make lint             # ruff format + ruff check + cargo fmt: all exit 0
make test             # pytest collects 0 items, exits 0
cd src/rs && cargo test   # 0 failures across all crates
python -c "import vertebra; print(vertebra.__version__)"  # prints: 0.1.0
```

---

## File Tree (Complete)

```
vertebra/
├── pyproject.toml
├── Makefile
├── src/
│   ├── py/
│   │   ├── vertebra/
│   │   │   ├── __init__.py
│   │   │   └── py.typed
│   │   └── tests/
│   │       ├── __init__.py
│   │       └── conftest.py
│   └── rs/
│       ├── Cargo.toml
│       ├── vertebra-core/
│       │   ├── Cargo.toml
│       │   └── src/
│       │       └── lib.rs
│       ├── vertebra-http/
│       │   ├── Cargo.toml
│       │   └── src/
│       │       └── lib.rs
│       └── vertebra-py/
│           ├── Cargo.toml
│           └── src/
│               └── lib.rs
├── scratch/                    # from Ch1, untouched
├── .agent/                     # preserved
├── .beads/                     # preserved
└── .github/                    # preserved
```

---

## Phase 1: Python Toolchain Files

### Task 1.1 — Write `pyproject.toml`

**File:** `pyproject.toml` (repository root)

**Exact content:**

```toml
[build-system]
requires = ["maturin>=1.5,<2"]
build-backend = "maturin"

[project]
name = "vertebra"
version = "0.1.0"
description = "Python-first application backbone with Rust IPC primitives"
readme = "README.md"
requires-python = ">=3.12"
license = { text = "MIT" }
dependencies = [
    "msgspec>=0.18",
    "anyio>=4.0",
    "rich>=13.0",
    "rich-click>=1.7",
]

[project.scripts]
vertebra = "vertebra.cli:cli"
vtb = "vertebra.cli:cli"

[project.optional-dependencies]
dev = [
    "pytest>=8.0",
    "pytest-anyio>=0.0.0",
    "ruff>=0.4",
    "mypy>=1.10",
]

[tool.uv]
dev-dependencies = [
    "pytest>=8.0",
    "pytest-anyio>=0.0.0",
    "ruff>=0.4",
    "mypy>=1.10",
]

[tool.maturin]
python-source = "src/py"
module-name = "vertebra._vertebra"
features = ["pyo3/extension-module"]

[tool.ruff]
line-length = 120
target-version = "py312"
src = ["src/py"]

[tool.ruff.lint]
select = ["E", "F", "I", "UP", "B", "SIM"]
ignore = ["E501"]

[tool.ruff.lint.isort]
known-first-party = ["vertebra"]

[tool.pytest.ini_options]
testpaths = ["src/py/tests"]
asyncio_mode = "auto"

[tool.mypy]
python_version = "3.12"
strict = true
files = ["src/py/vertebra"]
```

**Pass criteria:**
- `uv sync` reads the file without error
- `python -c "import tomllib; tomllib.loads(open('pyproject.toml').read())"` exits 0
- File is valid TOML (no syntax errors)

**Fail criteria:**
- `uv sync` reports a metadata parse error
- Any key is misspelled or missing from the required sections listed above

---

### Task 1.2 — Write `src/py/vertebra/__init__.py`

**File:** `src/py/vertebra/__init__.py`

**Exact content:**

```python
"""Vertebra — Python-first application backbone."""

__version__ = "0.1.0"
__all__ = ["__version__"]
```

**Pass criteria:**
- `python -c "import vertebra; print(vertebra.__version__)"` prints `0.1.0`
- `python -c "from vertebra import __version__; assert __version__ == '0.1.0'"` exits 0

**Fail criteria:**
- Import raises `ModuleNotFoundError`
- `__version__` is absent or has the wrong value

---

### Task 1.3 — Write `src/py/vertebra/py.typed`

**File:** `src/py/vertebra/py.typed`

**Exact content:** (empty file — PEP 561 marker)

```

```

**Pass criteria:**
- File exists and is empty (0 bytes or a single newline)
- `mypy` does not emit "missing py.typed" warnings for the `vertebra` package

**Fail criteria:**
- File is absent (mypy will treat the package as untyped)

---

### Task 1.4 — Write `src/py/tests/__init__.py`

**File:** `src/py/tests/__init__.py`

**Exact content:** (empty)

```python
```

**Pass criteria:**
- File exists
- `pytest src/py/tests` does not emit "not a package" warnings

**Fail criteria:**
- File is absent

---

### Task 1.5 — Write `src/py/tests/conftest.py`

**File:** `src/py/tests/conftest.py`

**Exact content:**

```python
"""Pytest configuration for Vertebra test suite."""
```

**Pass criteria:**
- `pytest --co -q` collects 0 items with exit code 5 (no tests found) OR 0 (when `--ignore-glob` is not set)
- No import errors during collection

**Fail criteria:**
- conftest raises a syntax error or import error on collection

---

## Phase 2: Rust Workspace

### Task 2.1 — Write `src/rs/Cargo.toml` (workspace root)

**File:** `src/rs/Cargo.toml`

**Exact content:**

```toml
[workspace]
resolver = "2"
members = [
    "vertebra-core",
    "vertebra-http",
    "vertebra-py",
]

[workspace.package]
version = "0.1.0"
edition = "2021"
authors = ["Vertebra Contributors"]
license = "MIT"
repository = "https://github.com/vertebra-io/vertebra"

[workspace.dependencies]
# Shared dependency versions — pin here, reference with { workspace = true } in crates
pyo3 = { version = "0.21", features = ["extension-module"] }
```

**Pass criteria:**
- `cargo metadata --manifest-path src/rs/Cargo.toml --no-deps` exits 0
- All three workspace members are listed

**Fail criteria:**
- `cargo metadata` reports an unknown member or resolver error

---

### Task 2.2 — Write `src/rs/vertebra-core/Cargo.toml`

**File:** `src/rs/vertebra-core/Cargo.toml`

**Exact content:**

```toml
[package]
name = "vertebra-core"
version.workspace = true
edition.workspace = true
authors.workspace = true
license.workspace = true

[lib]
name = "vertebra_core"
crate-type = ["lib"]
```

**Pass criteria:**
- `cargo build -p vertebra-core` exits 0
- `cargo test -p vertebra-core` exits 0 (0 tests, 0 failures)

**Fail criteria:**
- Package is unknown to the workspace
- `crate-type` is wrong (must be `["lib"]`, not `["cdylib"]` for this crate)

---

### Task 2.3 — Write `src/rs/vertebra-core/src/lib.rs`

**File:** `src/rs/vertebra-core/src/lib.rs`

**Exact content:**

```rust
//! vertebra-core — IPC primitives, ring buffers, and arena allocators.
//!
//! This crate is a stub. Implementation arrives in the `ipc-substrate` chapter.

pub mod placeholder {
    /// Placeholder to confirm the crate compiles.
    pub fn version() -> &'static str {
        env!("CARGO_PKG_VERSION")
    }
}

#[cfg(test)]
mod tests {
    use super::placeholder;

    #[test]
    fn version_is_set() {
        assert_eq!(placeholder::version(), "0.1.0");
    }
}
```

**Pass criteria:**
- `cargo test -p vertebra-core` runs 1 test (`version_is_set`) and exits 0
- No compiler warnings (or only allowed `dead_code` lint if stub items are unused)

**Fail criteria:**
- Compilation error
- Test failure (`version_is_set` fails — means workspace version is wrong)

---

### Task 2.4 — Write `src/rs/vertebra-http/Cargo.toml`

**File:** `src/rs/vertebra-http/Cargo.toml`

**Exact content:**

```toml
[package]
name = "vertebra-http"
version.workspace = true
edition.workspace = true
authors.workspace = true
license.workspace = true

[dependencies]
vertebra-core = { path = "../vertebra-core" }

[lib]
name = "vertebra_http"
crate-type = ["lib"]
```

**Pass criteria:**
- `cargo build -p vertebra-http` exits 0
- `cargo test -p vertebra-http` exits 0

**Fail criteria:**
- The `vertebra-core` path dependency cannot be resolved

---

### Task 2.5 — Write `src/rs/vertebra-http/src/lib.rs`

**File:** `src/rs/vertebra-http/src/lib.rs`

**Exact content:**

```rust
//! vertebra-http — HTTP/2 and HTTP/3 server primitives.
//!
//! This crate is a stub. Implementation arrives in the `http-server-h2` chapter.

pub mod placeholder {
    /// Confirm the crate links against vertebra-core.
    pub fn core_version() -> &'static str {
        vertebra_core::placeholder::version()
    }
}

#[cfg(test)]
mod tests {
    use super::placeholder;

    #[test]
    fn links_to_core() {
        assert_eq!(placeholder::core_version(), "0.1.0");
    }
}
```

**Pass criteria:**
- `cargo test -p vertebra-http` runs 1 test (`links_to_core`) and exits 0
- `vertebra_core` is successfully linked

**Fail criteria:**
- Compilation error due to unresolved `vertebra_core`
- Test failure

---

### Task 2.6 — Write `src/rs/vertebra-py/Cargo.toml`

**File:** `src/rs/vertebra-py/Cargo.toml`

**Exact content:**

```toml
[package]
name = "vertebra-py"
version.workspace = true
edition.workspace = true
authors.workspace = true
license.workspace = true

[dependencies]
vertebra-core = { path = "../vertebra-core" }
pyo3 = { workspace = true }

[lib]
name = "_vertebra"
crate-type = ["cdylib", "rlib"]
```

**Pass criteria:**
- `cargo build -p vertebra-py` exits 0 (may require `PYO3_PYTHON` or active venv)
- `cargo test -p vertebra-py --lib` exits 0

**Fail criteria:**
- `crate-type` is missing `"cdylib"` (required for Python extension module)
- PyO3 version not found (workspace dependency must be defined in workspace `Cargo.toml`)

---

### Task 2.7 — Write `src/rs/vertebra-py/src/lib.rs`

**File:** `src/rs/vertebra-py/src/lib.rs`

**Exact content:**

```rust
//! vertebra-py — PyO3 extension module for Vertebra.
//!
//! This crate is a stub. FFI bindings arrive in the `pyo3-request-proxy` chapter.

use pyo3::prelude::*;

/// Minimal PyO3 module. Exposes `__version__` to Python.
#[pymodule]
fn _vertebra(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn module_compiles() {
        // Compilation of this file is the test.
        assert!(true);
    }
}
```

**Pass criteria:**
- `cargo test -p vertebra-py --lib` exits 0 (`module_compiles` passes)
- `cargo build -p vertebra-py` produces a `.so` / `.dylib` / `.dll` artifact (or succeeds without one in CI without a Python interpreter if `PYO3_PYTHON` is unset — `rlib` crate-type covers this)

**Fail criteria:**
- PyO3 macro expansion fails (wrong PyO3 version or missing feature flag)
- `#[pymodule]` signature does not match the current PyO3 0.21 API

---

## Phase 3: Makefile

### Task 3.1 — Write `Makefile`

**File:** `Makefile` (repository root)

**Exact content:**

```makefile
.PHONY: install test rs-test rs-build test-all lint lint-all clean

# ── Python ──────────────────────────────────────────────────────────────────
install:
	uv sync
	cd src/rs && cargo build

test:
	uv run pytest src/py/tests -v

# ── Rust ────────────────────────────────────────────────────────────────────
rs-build:
	cd src/rs && cargo build

rs-test:
	cd src/rs && cargo test

# ── Combined ────────────────────────────────────────────────────────────────
test-all: test rs-test

# ── Lint / Format ───────────────────────────────────────────────────────────
lint:
	uv run ruff format .
	uv run ruff check --fix .
	cd src/rs && cargo fmt

lint-all: lint
	uv run mypy src/py
	cd src/rs && cargo clippy

# ── Housekeeping ─────────────────────────────────────────────────────────────
clean:
	rm -rf build dist *.egg-info
```

**Pass criteria:**
- `make install` exits 0 (uv sync succeeds, cargo build succeeds)
- `make test` exits 0 (pytest exits 0 or 5 — no test failures)
- `make lint` exits 0 (ruff and cargo fmt find no violations)
- `make rs-test` exits 0
- `make test-all` exits 0
- `make lint-all` exits 0 (mypy and clippy find no errors)
- `make clean` exits 0

**Fail criteria:**
- Any target uses tabs inconsistently (Makefile syntax requires tab indentation)
- A `.PHONY` target is missing and `make` tries to treat it as a file

---

## Phase 4: Verification

### Task 4.1 — Full Gate Check

Run the following commands in order. Every command must exit 0 (except `pytest` which may exit 5 when no tests are collected — this is treated as pass).

```bash
# 1. Dependency installation
make install

# 2. Linting (format + check + rustfmt)
make lint

# 3. Python test suite
make test

# 4. Rust test suite (all crates)
cd src/rs && cargo test

# 5. Version probe
python -c "import vertebra; print(vertebra.__version__)"
# Expected output: 0.1.0

# 6. Strict type check
uv run mypy src/py

# 7. Clippy (no warnings)
cd src/rs && cargo clippy -- -D warnings
```

**Pass criteria for each step:**

| Step | Pass | Fail |
|------|------|------|
| `make install` | exit 0, lock file written | uv/cargo error |
| `make lint` | exit 0, no diffs reported | ruff or cargo fmt exits non-zero |
| `make test` | exit 0 or 5, 0 failures | any test failure or collection error |
| `cargo test` | all tests pass, 0 failures | any test fails |
| version probe | stdout is exactly `0.1.0\n` | wrong version or ImportError |
| `mypy` | 0 errors | any type error |
| `cargo clippy` | 0 warnings | any warning or error |

---

### Task 4.2 — Smoke Check: Import Isolation

Confirm the package is importable without any optional dependencies installed:

```bash
uv run python -c "
import vertebra
assert vertebra.__version__ == '0.1.0', f'bad version: {vertebra.__version__!r}'
print('OK')
"
```

**Pass criteria:** prints `OK`, exits 0
**Fail criteria:** any exception, wrong output

---

### Task 4.3 — Smoke Check: Rust Workspace Metadata

```bash
cargo metadata --manifest-path src/rs/Cargo.toml --no-deps --format-version 1 \
  | python -c "
import json, sys
meta = json.load(sys.stdin)
names = {p['name'] for p in meta['packages']}
assert names == {'vertebra-core', 'vertebra-http', 'vertebra-py'}, names
print('Workspace OK:', names)
"
```

**Pass criteria:** prints `Workspace OK: {'vertebra-core', 'vertebra-http', 'vertebra-py'}`, exits 0
**Fail criteria:** any missing crate, JSON parse error, assertion failure

---

## Implementation Order

Tasks must be applied in the following sequence to avoid dependency errors:

1. Task 2.1 — `src/rs/Cargo.toml` (workspace root must exist before crate Cargo.toml files)
2. Task 2.2 + 2.3 — `vertebra-core` (no dependencies, needed by -http and -py)
3. Task 2.4 + 2.5 — `vertebra-http` (depends on vertebra-core path)
4. Task 2.6 + 2.7 — `vertebra-py` (depends on vertebra-core and pyo3)
5. Task 1.1 — `pyproject.toml` (needed before `uv sync`)
6. Task 1.2 — `src/py/vertebra/__init__.py`
7. Task 1.3 — `src/py/vertebra/py.typed`
8. Task 1.4 — `src/py/tests/__init__.py`
9. Task 1.5 — `src/py/tests/conftest.py`
10. Task 3.1 — `Makefile`
11. Task 4.1–4.3 — Verification gate

---

## Notes and Caveats

### PyO3 and Python Interpreter Discovery

`maturin develop` or `cargo build -p vertebra-py` requires a Python interpreter. During scaffolding,
ensure a `uv`-managed virtual environment is active:

```bash
uv sync         # creates .venv
source .venv/bin/activate   # or let uv run handle it
```

If CI runs without an active venv, set `PYO3_PYTHON=$(which python3)` or use `maturin develop`
which handles interpreter discovery automatically.

### pytest Exit Code 5

`pytest` exits with code 5 when no tests are collected. The `Makefile` `test` target must treat
exit code 5 as success until real tests are added. If this is a concern, add `|| true` or use
`--ignore-glob` — but the simplest fix is to add a placeholder test:

```python
# src/py/tests/test_smoke.py  (optional, created during CLI chapter)
def test_import():
    import vertebra
    assert vertebra.__version__ == "0.1.0"
```

This is deferred to the CLI chapter (Ch3) to keep this chapter minimal.

### `project.scripts` Entry Points

`pyproject.toml` declares `vertebra.cli:cli` but that module does not exist yet. The scripts
declaration is safe at this stage — `uv sync` installs the metadata but the scripts are not
invocable until `vertebra.cli` is created in Ch3. Attempting to run `vertebra` before Ch3 will
produce a clean `ModuleNotFoundError`, which is acceptable.

### Cargo.lock

`src/rs/Cargo.lock` will be generated automatically by `cargo build`. Do not commit it unless the
team policy changes — for a library workspace, `Cargo.lock` is conventionally gitignored.

### ruff `E501` Suppression

`E501` (line too long) is ignored because the project uses 120-character lines and some generated
or comment lines may exceed that. All other `E`, `F`, `I`, `UP`, `B`, `SIM` rules are active.

---

## Rollback

If any task fails:

1. Delete the files created in that task
2. Do NOT touch `scratch/` — archive is immutable
3. Re-read the exact file content from this spec and recreate

The chapter produces no side effects beyond creating new files; rollback is always safe.

---

## Handoff to Ch3

When all verification gates pass, signal readiness for Ch3 `cli-entrypoints`:

```
Chapter 2 complete. Workspace at repository root is clean.
Next: /implement cli-entrypoints
```

Ch3 will add:
- `src/py/vertebra/cli.py` (the `cli` entry point referenced in `pyproject.toml`)
- `src/py/vertebra/app.py` (the VertebraApp skeleton)
- `src/py/tests/test_smoke.py` (resolves the pytest exit-5 issue)
