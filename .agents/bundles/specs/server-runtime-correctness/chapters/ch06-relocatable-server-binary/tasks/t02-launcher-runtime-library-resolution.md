---
type: Task
id: "server-runtime-correctness:ch06:t02"
chapter_id: ch06-relocatable-server-binary
title: Resolve the embedded interpreter runtime in the launcher with fail-loud checks
description: Add EmbeddedPythonRuntime and is_packaged_server_binary to the supervisor module, make build_server_command reject packaged binaries on interpreters without a shared libpython, and make run_supervisor export PYTHONHOME and the platform libpython search path for every launch.
status: draft
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "server-runtime-correctness:ch06:t01"
read_set:
  - .agents/bundles/specs/server-runtime-correctness/chapters/ch06-relocatable-server-binary/spec.md
  - src/py/vertebra/supervisor/_supervisor.py
  - src/py/vertebra/cli/run.py
  - src/py/vertebra/cli/_launch.py
  - src/rs/vertebra-http/src/main.rs
  - src/py/tests/unit/cli/test_cli_commands.py
files:
  - src/py/vertebra/supervisor/_supervisor.py
tests:
  - src/py/tests/unit/cli/test_cli_commands.py
estimated_loc: 150
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
  - id: supervisor-py
    resource: src/py/vertebra/supervisor/_supervisor.py::run_supervisor
    title: run_supervisor, find_server_binary, and build_server_command
  - id: run-cli-py
    resource: src/py/vertebra/cli/run.py::run
    title: SystemExit to CLIError conversion around build_server_command
  - id: main-rs
    resource: src/rs/vertebra-http/src/main.rs::ensure_embedded_python_path
    title: Embedded interpreter sys.path extension
tags:
  - supervisor
  - libpython
  - fail-loud
---

# Task t02: Resolve the Embedded Interpreter Runtime in the Launcher with Fail-Loud Checks

## Objective & Scope Boundary

Add `EmbeddedPythonRuntime` and `is_packaged_server_binary` to `src/py/vertebra/supervisor/_supervisor.py`. `build_server_command` validates packaged binaries with `require_shared_library` (raising `SystemExit`, matching the existing missing-binary contract that `cli/run.py` converts to `CLIError`), and `run_supervisor` applies the runtime environment to `full_env` before spawning. Out of scope: CLI command changes (the three call sites in `cli/run.py`, `cli/dev.py`, and `extensions/litestar/plugin.py` already route through these two functions).

## Bounded Read Manifest

- [spec.md](../spec.md) — chapter contracts.
- `src/py/vertebra/supervisor/_supervisor.py::{run_supervisor,find_repo_root,find_server_binary,build_server_command,_run_with_tui}` [^supervisor-py].
- `src/py/vertebra/cli/run.py::run` — `except SystemExit as e: raise CLIError(...)` around `build_server_command()` [^run-cli-py].
- `src/py/vertebra/cli/_launch.py::ServerLaunchEnv.to_environ` — existing `PYTHONPATH` assembly.
- `src/rs/vertebra-http/src/main.rs::{prepare_embedded_python_env,ensure_embedded_python_path}` [^main-rs].
- `src/py/tests/unit/cli/test_cli_commands.py` — existing `find_server_binary` tests.

## Symbol & Signature Contracts

- `class EmbeddedPythonRuntime(msgspec.Struct, frozen=True, gc=False)` with `home: str`, `library_dir: str`, `library_file: str | None`:
  - `@classmethod def detect(cls) -> Self`: `home = sys.base_prefix`; POSIX: `library_dir = sysconfig.get_config_var("LIBDIR")`, `library_file` = `LIBDIR/LDLIBRARY` when that file exists, or (macOS framework) `LIBDIR/libpython<ver><abiflags>.dylib` when it exists, else `None`; Windows: `library_dir = sys.base_prefix`, `library_file = <base_prefix>/python3XY[t].dll` when it exists.
  - `def require_shared_library(self, binary: Path) -> None`: raises `SystemExit` with the chapter-spec message when `library_file is None`.
  - `def apply(self, env: dict[str, str]) -> None`: sets `PYTHONHOME = home` and prepends `library_dir` to `LD_LIBRARY_PATH` (Linux), `DYLD_LIBRARY_PATH` (macOS), or `PATH` (Windows) when `library_file` is not `None`, preserving existing entries and avoiding duplicates.
- `def is_packaged_server_binary(binary: Path) -> bool`: `True` when `binary.resolve().parent == Path(__file__).resolve().parent.parent / "bin"`.
- `build_server_command(binary: str | None = None) -> list[str]`: after resolving `server_bin`, when `binary is None` and `is_packaged_server_binary(Path(server_bin))`, call `EmbeddedPythonRuntime.detect().require_shared_library(Path(server_bin))`.
- `run_supervisor(...)`: after building `full_env`, call `EmbeddedPythonRuntime.detect().apply(full_env)` (both TUI and non-TUI paths).
- `__all__` gains `EmbeddedPythonRuntime` and `is_packaged_server_binary`; `msgspec`, `sys`, `sysconfig` imported at module top.

## Edge Cases & Error Semantics

- `VERTEBRA_SERVER_BINARY` override or development target binaries skip the shared-library requirement.
- `PYTHONHOME` set by the user is overwritten with `sys.base_prefix` because the embedded interpreter must match the launching interpreter; documented in `ch10:t02`.
- Free-threaded interpreters resolve `libpython3.14t.so.1.0` through `LDLIBRARY`.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/unit/cli/test_cli_commands.py`, dependency category `unit`; monkeypatch `sysconfig.get_config_var`, `sys.platform`, `sys.base_prefix`, and use `tmp_path` fake library files. Parametrize platforms.

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Linux shared | `LIBDIR=tmp/lib`, `LDLIBRARY=libpython3.12.so.1.0` (file exists), `sys.platform="linux"`; `apply(env={"LD_LIBRARY_PATH": "/opt/x"})` | `env["LD_LIBRARY_PATH"] == f"{tmp}/lib:/opt/x"`, `env["PYTHONHOME"] == sys.base_prefix` |
| macOS framework | `LIBDIR=tmp/lib`, `LDLIBRARY="Python.framework/Versions/3.12/Python"` (absent), `tmp/lib/libpython3.12.dylib` exists, `sys.platform="darwin"` | `library_file` ends with `libpython3.12.dylib`; `DYLD_LIBRARY_PATH` starts with `tmp/lib` |
| Static interpreter | `LDLIBRARY=libpython3.12.a` (present), no shared file | `library_file is None`; `require_shared_library(Path("x"))` raises `SystemExit` containing `"shared libpython"` |
| Packaged binary check | Monkeypatch `find_server_binary` to return `<pkg>/bin/vertebra-server` and a static runtime | `build_server_command()` raises `SystemExit` |
| Dev binary skip | `find_server_binary` returns `/tmp/vtb-cargo-target/release/vertebra-server`, static runtime | Returns `[that path]` |
| No duplicates | `apply` twice on the same env | Library dir appears once |
| `vtb run` conversion | `CliRunner().invoke(run, ["app:app"])` with the packaged/static setup | Exit code `2`; output contains `"shared libpython"` |

## Verification

- Initial Red baseline: `uv run --no-sync pytest src/py/tests/unit/cli/test_cli_commands.py -q -k "runtime or packaged"`
- Final Green + lint:
  ```bash
  uv run --no-sync pytest src/py/tests/unit/cli/test_cli_commands.py src/py/tests/unit/cli/test_run_with_workers.py -q
  uv run --no-sync ruff check src/py/vertebra/supervisor
  uv run --no-sync mypy src/py/vertebra/supervisor
  ```
- Planned local commit: `fix(supervisor): resolve embedded libpython runtime for packaged server binary`

## Acceptance Criteria

- [ ] `run_supervisor` exports `PYTHONHOME` and the platform library path for the launching interpreter (R15).
- [ ] Packaged binaries on interpreters without a shared libpython fail before launch with an actionable message and exit code `2` from `vtb run` (R15).
- [ ] Development binaries keep working on static interpreters (R15).

## Notes & Discoveries

[^supervisor-py]: `src/py/vertebra/supervisor/_supervisor.py::run_supervisor` — `run_supervisor`, `find_server_binary`, and `build_server_command`.
[^run-cli-py]: `src/py/vertebra/cli/run.py::run` — `SystemExit` to `CLIError` conversion around `build_server_command`.
[^main-rs]: `src/rs/vertebra-http/src/main.rs::ensure_embedded_python_path` — Embedded interpreter `sys.path` extension.
