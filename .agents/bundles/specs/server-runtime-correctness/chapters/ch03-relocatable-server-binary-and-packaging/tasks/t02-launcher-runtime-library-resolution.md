---
type: Task
id: "server-runtime-correctness:ch03:t02"
flow_id: server-runtime-correctness
chapter_id: ch03-relocatable-server-binary-and-packaging
title: Resolve Embedded Python Runtime Library Paths in Launcher
description: Implement EmbeddedPythonRuntime and is_packaged_server_binary in _supervisor.py to export PYTHONHOME and shared library search paths and fail loud on static interpreters.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "server-runtime-correctness:ch03:t01"
files:
  - src/py/vertebra/supervisor/_supervisor.py
tests:
  - src/py/tests/unit/cli/test_cli_commands.py
read_set:
  - src/py/vertebra/supervisor/_supervisor.py
  - src/rs/vertebra-http/src/main.rs
  - src/py/tests/unit/cli/test_cli_commands.py
estimated_loc: 145
tags:
  - supervisor
  - pyo3
  - libpython
  - packaging
sources:
  - id: supervisor-py
    resource: "src/py/vertebra/supervisor/_supervisor.py::find_server_binary"
    title: Packaged vs workspace server binary resolution
  - id: main-rs
    resource: "src/rs/vertebra-http/src/main.rs::prepare_embedded_python_env"
    title: Embedded Python sys.path and PATH initialization
---

# Task `t02`: Resolve Embedded Python Runtime Library Paths in Launcher

## Objective & Scope Boundary

Implement `EmbeddedPythonRuntime` (`detect`, `require_shared_library`, `apply`) and `is_packaged_server_binary` in `src/py/vertebra/supervisor/_supervisor.py` per [`../spec.md` §2](../spec.md) [^supervisor-py] [^main-rs].

## Bounded Read Manifest

- `src/py/vertebra/supervisor/_supervisor.py` — `find_server_binary`, `build_server_command`, and `run_supervisor`.
- `src/rs/vertebra-http/src/main.rs` — embedded interpreter startup contract.

## Contracts & Edge Cases

- `is_packaged_server_binary(binary)` returns `True` only when `binary` resolves inside `<vertebra>/bin/` and `VERTEBRA_SERVER_BINARY` was not explicitly set.
- `require_shared_library(binary)` raises `SystemExit` with actionable remediation (`uv`-managed or `--enable-shared` CPython) when a packaged POSIX binary runs on a static-only Python lacking `LIBDIR/LDLIBRARY`; developer target binaries skip the check.
- `apply(env)` exports `PYTHONHOME` (`sys.base_prefix`) and prepends `library_dir` to `LD_LIBRARY_PATH` (Linux), `DYLD_LIBRARY_PATH` (macOS), or `PATH` (Windows), preserving existing entries.

## Test Matrix

- Parameterized unit tests in `test_cli_commands.py` covering Linux/macOS/Windows `apply(env)` prepending, static-interpreter `SystemExit` -> `CLIError` exit `2` on packaged binaries, and bypassed check for `VERTEBRA_SERVER_BINARY` / Cargo target binaries.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/supervisor/_supervisor.py && uv run --no-sync mypy src/py/vertebra/supervisor/_supervisor.py && uv run --no-sync pytest src/py/tests/unit/cli/test_cli_commands.py -q
```

## Acceptance Criteria

- [ ] Packaged `vertebra-server` fails loud with exit `2` on static-only Python interpreters.
- [ ] `run_supervisor` exports `PYTHONHOME` and platform library search paths cleanly.

[^supervisor-py]: `src/py/vertebra/supervisor/_supervisor.py::find_server_binary` — Packaged vs workspace server binary resolution.
[^main-rs]: `src/rs/vertebra-http/src/main.rs::prepare_embedded_python_env` — Embedded Python sys.path and PATH initialization.
