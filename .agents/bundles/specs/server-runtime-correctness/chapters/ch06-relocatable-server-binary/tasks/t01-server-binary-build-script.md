---
type: Task
id: "server-runtime-correctness:ch06:t01"
chapter_id: ch06-relocatable-server-binary
title: Build a per-ABI relocatable vertebra-server and stage it under src/py/vertebra/bin
description: Add tools/scripts/build_server_binary.py with build and repair subcommands that select a shared-libpython build interpreter for the running ABI, run cargo with relocatable link settings, rewrite macOS libpython install names, stage the binary under src/py/vertebra/bin, and wrap auditwheel/delocate with libpython exclusions; make vertebra-http/build.rs skip absolute rpaths for relocatable builds.
status: draft
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "server-runtime-correctness:ch05:t03"
read_set:
  - .agents/bundles/specs/server-runtime-correctness/chapters/ch06-relocatable-server-binary/spec.md
  - src/rs/vertebra-http/build.rs
  - src/rs/vertebra-http/Cargo.toml
  - .github/workflows/release-binaries.yml
  - tools/scripts/verify_oracle_wheel.py
  - src/py/tests/unit/build/test_packaging_and_pgo.py
files:
  - tools/scripts/build_server_binary.py
  - src/rs/vertebra-http/build.rs
tests:
  - src/py/tests/unit/build/test_packaging_and_pgo.py
estimated_loc: 240
plan_revision: 1
plan_commit: null
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-12T00:45:00Z"
updated_at: "2026-11-12T00:45:00Z"
commit: null
generated:
  by: agent:plan-generator
  at: "2026-11-12T00:45:00Z"
sources:
  - id: http-build-rs
    resource: src/rs/vertebra-http/build.rs
    title: Absolute rpath emission from pyo3_build_config lib_dir
  - id: http-cargo
    resource: src/rs/vertebra-http/Cargo.toml
    title: vertebra-server bin target and python/tls/compression features
  - id: oracle-wheel-script
    resource: tools/scripts/verify_oracle_wheel.py
    title: Existing tools/scripts module style for wheel tooling
tags:
  - packaging
  - binary
  - tooling
---

# Task t01: Build a Per-ABI Relocatable vertebra-server and Stage It under src/py/vertebra/bin

## Objective & Scope Boundary

Create `tools/scripts/build_server_binary.py` (module docstring, `__all__`, msgspec value objects, classes with public methods before private helpers, Google docstrings, no inline comments, `sys.stdout.write` for output) exposing `build` and `repair` subcommands, and update `src/rs/vertebra-http/build.rs` so `VERTEBRA_SERVER_RELOCATABLE=1` suppresses the absolute rpath and adds `-Wl,-headerpad_max_install_names` on macOS. Out of scope: pyproject/CI wiring (`ch07`) and launcher runtime (`t02`).

## Bounded Read Manifest

- [spec.md](../spec.md) — chapter contracts.
- `src/rs/vertebra-http/build.rs` [^http-build-rs].
- `src/rs/vertebra-http/Cargo.toml` — `[[bin]] vertebra-server`, `python`, `tls`, `compression` features [^http-cargo].
- `.github/workflows/release-binaries.yml` — existing cargo invocation.
- `tools/scripts/verify_oracle_wheel.py` — script layout precedent [^oracle-wheel-script].
- `src/py/tests/unit/build/test_packaging_and_pgo.py` — test module importing `tools.*`.

## Symbol & Signature Contracts

- `ServerBinaryTarget` (fields and properties exactly as in the chapter spec): `for_running_interpreter` reads `Py_GIL_DISABLED` from `config_vars` for `free_threaded`, normalises `platform` (`linux*` → `linux`, `darwin`, `win32`), and calls `resolve_rust_target(platform, environ)` which maps macOS `ARCHFLAGS` containing exactly one of `-arch x86_64` / `-arch arm64` to `x86_64-apple-darwin` / `aarch64-apple-darwin`, else `None`.
- `ServerBinaryBuilder.select_build_python(target, *, running_python, config_vars, uv_find)`: returns `running_python` when `platform == "win32"` or `config_vars.get("Py_ENABLE_SHARED") in (1, "1")` or `config_vars.get("PYTHONFRAMEWORK")`; otherwise returns `uv_find(f"cpython-{target.python_version}{target.abi_suffix}")`. The production `uv_find` runs `uv python install <spec>` then `uv python find --managed-python <spec>`; a missing `uv` raises `SystemExit("uv is required to build vertebra-server against a shared libpython")`.
- `cargo_command()` → `["cargo", "build", "--manifest-path", "src/rs/Cargo.toml", "--bin", "vertebra-server", "--release", "--features", "vertebra-http/python,vertebra-http/tls,vertebra-http/compression", *(["--target", t] if t else [])]`.
- `cargo_environ(base)` adds `PYO3_PYTHON=<build_python>`, `VERTEBRA_SERVER_RELOCATABLE=1`, `CARGO_TARGET_DIR=<repo>/target/server-binary/<python_version><abi_suffix>`.
- `macos_install_name_changes(otool_output, target)` returns `(old, f"@rpath/{target.libpython_dylib}")` for each `otool -L` line whose path ends with `Python.framework/Versions/<ver>/Python` or `libpython<ver><t>.dylib` and is not already `@rpath/`.
- `build()` runs cargo, applies `install_name_tool -change` pairs plus `codesign --force --sign -` on macOS, copies to `<repo>/src/py/vertebra/bin/<binary_name>`, sets mode `0o755`, writes the path to stdout, returns it.
- `repair_command(platform, wheel, dest_dir, *, wheel_tag, delocate_archs)`: Linux → `["auditwheel", "repair", "--exclude", <soname from wheel_tag>, "-w", dest_dir, wheel]` where `cp314t` → `libpython3.14t.so.1.0`; macOS → `["delocate-wheel", "--require-archs", delocate_archs, "--exclude", "libpython", "-w", dest_dir, "-v", wheel]`; Windows → `SystemExit("no wheel repair on win32")`.
- `main(argv)`: `build` and `repair <wheel> <dest_dir> [--delocate-archs A]` subcommands via `argparse`; `repair` derives `wheel_tag` from the wheel filename.
- `src/rs/vertebra-http/build.rs`: `println!("cargo:rerun-if-env-changed=VERTEBRA_SERVER_RELOCATABLE");` emit the absolute rpath only when the variable is unset; when set on macOS emit `cargo:rustc-link-arg=-Wl,-headerpad_max_install_names`.

## Edge Cases & Error Semantics

- `ARCHFLAGS="-arch x86_64 -arch arm64"` → `None` (universal builds are not produced; cibuildwheel builds per arch).
- Wheel filename without a `cpXY` tag → `SystemExit("cannot derive CPython tag from <name>")`.
- `cargo` failure → `subprocess.CalledProcessError` propagates (no retry, no fallback).
- `otool` output without any libpython reference on macOS → `SystemExit("vertebra-server does not link libpython")`.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/unit/build/test_packaging_and_pgo.py`, dependency category `unit` (pure functions; no cargo). Parametrize all tables.

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Target GIL Linux | `version_info=(3, 12)`, `config_vars={"Py_GIL_DISABLED": 0}`, `platform="linux"`, `environ={}` | `binary_name == "vertebra-server"`, `libpython_soname == "libpython3.12.so.1.0"`, `rust_target is None` |
| Target free-threaded | `(3, 14)`, `{"Py_GIL_DISABLED": 1}` | `abi_suffix == "t"`, `libpython_soname == "libpython3.14t.so.1.0"`, `libpython_dylib == "libpython3.14t.dylib"` |
| Windows name | `platform="win32"` | `binary_name == "vertebra-server.exe"` |
| macOS ARCHFLAGS | `{"ARCHFLAGS": "-arch x86_64"}` / `"-arch arm64"` / `"-arch x86_64 -arch arm64"` | `"x86_64-apple-darwin"` / `"aarch64-apple-darwin"` / `None` |
| Build python shared | `config_vars={"Py_ENABLE_SHARED": 1}` | Returns `running_python`; `uv_find` not called |
| Build python framework | `{"Py_ENABLE_SHARED": 0, "PYTHONFRAMEWORK": "Python"}` | Returns `running_python` |
| Build python static | `{"Py_ENABLE_SHARED": 0}`, `uv_find=lambda spec: Path(f"/pbs/{spec}")` | `Path("/pbs/cpython-3.12")` (or `cpython-3.14t` for free-threaded) |
| Cargo command target | Target with `rust_target="aarch64-apple-darwin"` | Command ends with `["--target", "aarch64-apple-darwin"]` and includes `"vertebra-http/python,vertebra-http/tls,vertebra-http/compression"` |
| Cargo env | `cargo_environ({})` | Contains `VERTEBRA_SERVER_RELOCATABLE == "1"` and `PYO3_PYTHON` equal to the build python |
| macOS rewrite | otool text containing `\t/Library/Frameworks/Python.framework/Versions/3.12/Python (compatibility version 3.12.0, ...)` | `[("/Library/Frameworks/Python.framework/Versions/3.12/Python", "@rpath/libpython3.12.dylib")]` |
| Linux repair | `repair_command("linux", Path("vertebra_core-0.3.0-cp314-cp314t-manylinux_2_28_aarch64.whl"), Path("out"), wheel_tag="cp314t", delocate_archs=None)` | `["auditwheel", "repair", "--exclude", "libpython3.14t.so.1.0", "-w", "out", "vertebra_core-...whl"]` |
| Bad wheel name | `main(["repair", "vertebra_core-0.3.0-py3-none-any.whl", "out"])` | `SystemExit` with `"cannot derive CPython tag"` |

## Verification

- Initial Red baseline: `uv run --no-sync pytest src/py/tests/unit/build/test_packaging_and_pgo.py -q -k server_binary`
- Final Green + lint:
  ```bash
  uv run --no-sync pytest src/py/tests/unit/build/test_packaging_and_pgo.py -q
  uv run --no-sync ruff check tools/scripts/build_server_binary.py
  uv run --no-sync mypy tools/scripts/build_server_binary.py
  cargo check --manifest-path src/rs/Cargo.toml -p vertebra-http --bin vertebra-server --features python,tls,compression
  VERTEBRA_SERVER_RELOCATABLE=1 uv run --no-sync python -m tools.scripts.build_server_binary build
  ```
- Planned local commit: `build(server): add relocatable per-ABI vertebra-server build script`

## Acceptance Criteria

- [ ] `tools/scripts/build_server_binary.py build` stages an executable `src/py/vertebra/bin/vertebra-server` linked against a shared libpython without an absolute rpath (R14).
- [ ] `repair` wraps auditwheel and delocate with libpython exclusions derived from the wheel tag (R14, R16).
- [ ] `build.rs` keeps today's development rpath when `VERTEBRA_SERVER_RELOCATABLE` is unset (R14).

## Notes & Discoveries

[^http-build-rs]: `src/rs/vertebra-http/build.rs` — Absolute rpath emission from `pyo3_build_config` `lib_dir`.
[^http-cargo]: `src/rs/vertebra-http/Cargo.toml` — `vertebra-server` bin target and `python`/`tls`/`compression` features.
[^oracle-wheel-script]: `tools/scripts/verify_oracle_wheel.py` — Existing `tools/scripts` module style for wheel tooling.
