---
type: Spec
parent_flow: server-runtime-correctness
chapter_id: ch06-relocatable-server-binary
title: "Chapter 06: Per-ABI Relocatable Server Binary Build and Embedded Runtime Resolution"
description: Build vertebra-server for the running CPython ABI against a shared libpython without absolute rpaths, stage it under src/py/vertebra/bin with platform repair helpers, and resolve PYTHONHOME and the libpython search path in the launcher with a fail-loud check for interpreters that lack a shared libpython.
status: draft
state: planned
planning_status: complete
depends_on_chapters:
  - ch05-supervisor-exports-and-worker-lanes
replan_trigger: null
current_task: null
plan_revision: 1
plan_commit: null
state_revision: 0
last_verified_checkpoint: null
created_at: "2026-11-12T00:45:00Z"
updated_at: "2026-11-12T00:45:00Z"
generated:
  by: agent:plan-generator
  at: "2026-11-12T00:45:00Z"
sources:
  - id: http-build-rs
    resource: src/rs/vertebra-http/build.rs
    title: Build script that adds the build interpreter's absolute libpython directory as rpath
  - id: supervisor-py
    resource: src/py/vertebra/supervisor/_supervisor.py::find_server_binary
    title: Packaged and development server binary resolution and run_supervisor
  - id: main-rs
    resource: src/rs/vertebra-http/src/main.rs::prepare_embedded_python_env
    title: Embedded interpreter PATH preparation and sys.path extension
  - id: release-binaries
    resource: .github/workflows/release-binaries.yml
    title: Standalone binary build command and feature set
tags:
  - packaging
  - binary
  - pyo3
  - libpython
---

# Chapter 06: Per-ABI Relocatable Server Binary Build and Embedded Runtime Resolution

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

`vertebra-server` embeds CPython through PyO3 `auto-initialize` and `append_to_inittab!(pymodule_init)`, so it links a specific libpython ABI. `src/rs/vertebra-http/build.rs` adds `-Wl,-rpath,<build lib_dir>`, an absolute path that only exists on the build machine [^http-build-rs]. `find_server_binary` already prefers `vertebra/bin/vertebra-server` inside the installed package, but nothing builds a relocatable binary for that location, and the launcher exports no `PYTHONHOME` or library search path for an embedded interpreter whose prefix differs from its build prefix [^supervisor-py] [^main-rs]. manylinux build images ship CPython without a shared or static libpython, so the binary must link a shared libpython from a python-build-standalone interpreter of the same version and ABI. This chapter delivers the build and runtime halves of [#82](https://github.com/cofin/vertebra/issues/82); `ch07` wires them into wheels and CI.

### 1.2 Non-Goals

- Static libpython embedding.
- Serving HTTP from inside the `_vertebra` extension.
- Supporting interpreters without a shared libpython for the packaged binary (they fail loud with guidance; developer binaries built locally keep working).

### 1.3 Upstream Baseline & Code Analysis

`release-binaries.yml` builds with `cargo build --manifest-path src/rs/Cargo.toml --bin vertebra-server --release --features vertebra-http/python,vertebra-http/tls,vertebra-http/compression` and `PYO3_PYTHON` [^release-binaries]. `prepare_embedded_python_env` prepends the interpreter directory and `VIRTUAL_ENV/bin` to `PATH`; `ensure_embedded_python_path` adds `PYTHONPATH` entries and venv `site-packages`; `ServerLaunchEnv.to_environ` already sets `PYTHONPATH` to cwd, `purelib`, and `platlib`. `.gitignore` already ignores `src/py/vertebra/bin/`.

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Chapter Requirements

- **R14** (`t01`): `tools/scripts/build_server_binary.py` and relocatable `build.rs`.
- **R15** (`t02`): `EmbeddedPythonRuntime` in `_supervisor.py`, fail-loud validation in `build_server_command`, and environment export in `run_supervisor`.

### 2.2 Concrete Type & Signature Contracts

```python
# tools/scripts/build_server_binary.py
class ServerBinaryTarget(msgspec.Struct, frozen=True, gc=False):
    python_version: str
    free_threaded: bool
    platform: Literal["linux", "darwin", "win32"]
    rust_target: str | None
    @property
    def abi_suffix(self) -> str: ...            # "" or "t"
    @property
    def binary_name(self) -> str: ...           # "vertebra-server" or "vertebra-server.exe"
    @property
    def libpython_soname(self) -> str: ...      # "libpython3.12.so.1.0" / "libpython3.14t.so.1.0"
    @property
    def libpython_dylib(self) -> str: ...       # "libpython3.12.dylib"
    @classmethod
    def for_running_interpreter(cls, *, version_info: tuple[int, int], config_vars: Mapping[str, object],
                                platform: str, environ: Mapping[str, str]) -> Self: ...
    @staticmethod
    def resolve_rust_target(platform: str, environ: Mapping[str, str]) -> str | None: ...

class ServerBinaryBuilder:
    def __init__(self, repo_root: Path, target: ServerBinaryTarget, *, build_python: Path) -> None: ...
    @staticmethod
    def select_build_python(target: ServerBinaryTarget, *, running_python: Path,
                            config_vars: Mapping[str, object], uv_find: Callable[[str], Path]) -> Path: ...
    def cargo_command(self) -> list[str]: ...
    def cargo_environ(self, base: Mapping[str, str]) -> dict[str, str]: ...
    @staticmethod
    def macos_install_name_changes(otool_output: str, target: ServerBinaryTarget) -> list[tuple[str, str]]: ...
    def build(self) -> Path: ...
    @staticmethod
    def repair_command(platform: str, wheel: Path, dest_dir: Path, *, wheel_tag: str, delocate_archs: str | None) -> list[str]: ...

def main(argv: Sequence[str] | None = None) -> int: ...
```

```python
# src/py/vertebra/supervisor/_supervisor.py
class EmbeddedPythonRuntime(msgspec.Struct, frozen=True, gc=False):
    home: str
    library_dir: str
    library_file: str | None
    @classmethod
    def detect(cls) -> Self: ...
    def require_shared_library(self, binary: Path) -> None: ...
    def apply(self, env: dict[str, str]) -> None: ...

def is_packaged_server_binary(binary: Path) -> bool: ...
```

### 2.3 Caller -> Seam -> Collaborator Map

| Caller | Seam | Collaborator |
| --- | --- | --- |
| cibuildwheel `before-build` / `make core-wheel-check` | `build_server_binary.py build` | `uv python find`, `cargo build`, `otool`/`install_name_tool`/`codesign` (macOS) |
| cibuildwheel repair | `build_server_binary.py repair` | `auditwheel repair --exclude <soname>`, `delocate-wheel --exclude libpython` |
| `vtb run`, `vtb dev`, `litestar run` | `build_server_command` | `find_server_binary`, `EmbeddedPythonRuntime.detect().require_shared_library` |
| same | `run_supervisor` | `EmbeddedPythonRuntime.detect().apply(full_env)` |

### 2.4 Edge-Case & Failure-Mode Matrix

| Input | Behaviour |
| --- | --- |
| Running interpreter has `Py_ENABLE_SHARED=1` or `PYTHONFRAMEWORK` set, or `win32` | Build against it. |
| Static interpreter (manylinux, pyenv default) | Build against `uv python find cpython-<ver><t>` (python-build-standalone); `uv` missing → `SystemExit` naming the requirement. |
| macOS `ARCHFLAGS="-arch x86_64"` | `--target x86_64-apple-darwin`; `-arch arm64` → `aarch64-apple-darwin`; both or none → host target. |
| Packaged binary + interpreter lacking `LIBDIR/LDLIBRARY` (POSIX) | `SystemExit("vertebra-server from the wheel requires a Python built with a shared libpython; <exe> has none (pyenv: PYTHON_CONFIGURE_OPTS=--enable-shared; uv-managed and official installers qualify)")`; `vtb run` turns it into `CLIError` exit `2`. |
| Development binary under `CARGO_TARGET_DIR`, `target`, `src/rs/target`, or `/tmp/vtb-cargo-target` | No shared-library requirement; environment applied when a library exists. |
| Existing `LD_LIBRARY_PATH`/`DYLD_LIBRARY_PATH`/`PATH` | Runtime directory prepended, existing entries preserved. |
| `VERTEBRA_SERVER_BINARY` override | Treated as a development binary. |

### 2.5 Alternatives Considered

Building against the cibuildwheel interpreter on Linux was rejected (no libpython in manylinux images). Static embedding was rejected (no static library in manylinux images and extension modules would need `--export-dynamic`). Bundling libpython inside the wheel was rejected because it would shadow the user's interpreter and break `sys.prefix`-relative stdlib discovery.

## 3. Documentation & OKF Knowledge Plan

Progress: `[0/0 files documented in ch06]`; the build and runtime contract is documented in `docs/usage/packaging-and-deployment.md`, `docs/getting-started/installation.md` (`ch10:t02`), `packaging-and-pgo.md`, and `pyo3-and-python-runtime.md` (`ch11:t01`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test / Gate |
| --- | --- | --- | --- |
| R14 | [t01](tasks/t01-server-binary-build-script.md) | `behavior_tdd` | `src/py/tests/unit/build/test_packaging_and_pgo.py` |
| R15 | [t02](tasks/t02-launcher-runtime-library-resolution.md) | `behavior_tdd` | `src/py/tests/unit/cli/test_cli_commands.py` |

## 5. Implementation Plan

- [ ] server-runtime-correctness:ch06:t01 — Build a per-ABI relocatable vertebra-server and stage it under src/py/vertebra/bin (tasks/t01-server-binary-build-script.md)
- [ ] server-runtime-correctness:ch06:t02 — Resolve the embedded interpreter runtime in the launcher with fail-loud checks (tasks/t02-launcher-runtime-library-resolution.md)

**Chapter Recovery Checkpoint:** `uv run --no-sync pytest src/py/tests/unit/build/test_packaging_and_pgo.py src/py/tests/unit/cli/test_cli_commands.py -q && uv run --no-sync python -m tools.scripts.build_server_binary build && test -x src/py/vertebra/bin/vertebra-server` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Baseline**: `plan_commit: null`; chapter `state: planned`.
- **Remaining Tasks**: `t01`, `t02` (`0/2`).
- **Downstream Dependents**: `ch07:t01`–`ch07:t03`.

[^http-build-rs]: `src/rs/vertebra-http/build.rs` — Build script that adds the build interpreter's absolute libpython directory as rpath.
[^supervisor-py]: `src/py/vertebra/supervisor/_supervisor.py::find_server_binary` — Packaged and development server binary resolution and `run_supervisor`.
[^main-rs]: `src/rs/vertebra-http/src/main.rs::prepare_embedded_python_env` — Embedded interpreter `PATH` preparation and `sys.path` extension.
[^release-binaries]: `.github/workflows/release-binaries.yml` — Standalone binary build command and feature set.
