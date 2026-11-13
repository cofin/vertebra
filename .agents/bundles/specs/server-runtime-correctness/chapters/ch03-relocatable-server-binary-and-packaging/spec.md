---
type: Spec
flow_id: server-runtime-correctness
chapter_id: ch03-relocatable-server-binary-and-packaging
title: "Chapter 03: Relocatable Server Binary, Wheel Packaging, and Standalone Skills Staging"
description: "Build relocatable per-ABI vertebra-server binaries, resolve shared libpython at launch, include vertebra/bin/* in core wheels, and add opt-in StandaloneBuilder .agents/skills staging."
status: stable
state: completed
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
depends_on_chapters:
  - ch02-lifespan-supervisor-and-worker-lanes
replan_trigger: null
plan_commit: 18baadcd
last_reviewed_commit: 0e03d7a1
updated_at: "2026-11-13T05:08:50Z"
generated:
  by: flow-plan
  at: "2026-11-12T23:45:00Z"
verified:
  - by: agent:code-reviewer
    at: "2026-11-13T05:08:50Z"
    commit: 0e03d7a1
tags:
  - packaging
  - wheel
  - binary
  - pyo3
  - skills
sources:
  - id: http-build-rs
    resource: src/rs/vertebra-http/build.rs
    title: Current build script embedding absolute build-host rpaths
  - id: supervisor-py
    resource: "src/py/vertebra/supervisor/_supervisor.py::find_server_binary"
    title: Server binary lookup and environment preparation
  - id: pyproject-toml
    resource: pyproject.toml
    title: Maturin wheel include configuration for vertebra-core
  - id: standalone-py
    resource: "src/py/vertebra/build/packaging/standalone.py::StandaloneBuilder"
    title: Standalone and onefile staging builder
  - id: cli-build-py
    resource: "src/py/vertebra/cli/build.py::build"
    title: vtb build CLI command
---

# Chapter 03: Relocatable Server Binary, Wheel Packaging, and Standalone Skills Staging

<!-- truth: start -->
- `src/rs/vertebra-http/build.rs` drops build-machine absolute `-Wl,-rpath` entries, and `tools/scripts/build_server_binary.py` builds relocatable `vertebra-server` binaries against a shared `libpython` (`python-build-standalone` via `uv python find` when the host CPython is static) and stages `src/py/vertebra/bin/vertebra-server[.exe]` ([#82](https://github.com/cofin/vertebra/issues/82)) [^http-build-rs].
- `EmbeddedPythonRuntime` in `_supervisor.py` validates shared `libpython` presence for packaged binaries and exports `PYTHONHOME` plus platform library search paths (`LD_LIBRARY_PATH`, `DYLD_LIBRARY_PATH`, `PATH`), while `pyproject.toml` packages `src/py/vertebra/bin/*` in wheels only [^supervisor-py] [^pyproject-toml].
- `StandaloneBuilder(..., include_agent_skills=False)` and `vtb build --include-agent-skills` stage user `<project>/.agents/skills` into `app/.agents/skills` (never staging Vertebra's own repository `.agents/` or non-`skills` `.agents/` subdirectories) and anchor relative skill resolution to `app_dir` ([#115](https://github.com/cofin/vertebra/issues/115)) [^standalone-py] [^cli-build-py].
<!-- truth: end -->

## 1. Specification, Context & Provenance

`vertebra-http/build.rs` bakes the build machine's `LIBDIR` into the binary rpath [^http-build-rs], `_supervisor.py` exports neither `PYTHONHOME` nor library paths for embedded CPython [^supervisor-py], `pyproject.toml` omits `src/py/vertebra/bin/*` from wheels [^pyproject-toml], and `StandaloneBuilder.build` skips every dot-prefixed directory (`child.name.startswith(".")`), dropping `.agents/skills` in standalone and `OnefileBuilder` distributions ([#115](https://github.com/cofin/vertebra/issues/115)) [^standalone-py] [^cli-build-py].

## 2. Requirements, Concrete Contracts & Architecture

- **`R6a` (`t01`)**: Update `src/rs/vertebra-http/build.rs` to link `libpython` without absolute build-machine rpaths (emitting relocatable `@rpath` metadata only as needed) and implement `ServerBinaryTarget` + `ServerBinaryBuilder` (`select_build_python`, `cargo_command`, `macos_install_name_changes`, `build`, `repair_command`) in `tools/scripts/build_server_binary.py` (compiling with `vertebra-http/python,vertebra-http/tls,vertebra-http/compression`).
- **`R6b` (`t02`)**: Implement `EmbeddedPythonRuntime` (`detect`, `require_shared_library`, `apply`) and `is_packaged_server_binary` in `src/py/vertebra/supervisor/_supervisor.py`, failing loud (`CLIError` exit `2`) when a packaged binary runs under a static-only Python interpreter while allowing developer target binaries.
- **`R6c` (`t03`)**: Add `{ path = "src/py/vertebra/bin/*", format = "wheel" }` to `pyproject.toml` `[tool.maturin].include` and verify `auditwheel --exclude <soname>` / `delocate-wheel --exclude libpython` repair command generation so wheels ship `vertebra/bin/vertebra-server[.exe]` without bundling `libpython` and sdists omit prebuilt binaries.
- **`R7` (`t04`, [#115](https://github.com/cofin/vertebra/issues/115))**: Add `include_agent_skills: bool = False` to `StandaloneBuilder.__init__` and `--include-agent-skills` to `vtb build`:
  - When `include_agent_skills=True` and `(project_root / ".agents" / "skills").is_dir()`, copy only `.agents/skills` into `app_stage / ".agents" / "skills"` (ignoring `__pycache__`, `*.pyc`, `.DS_Store`), never copying sibling `.agents/` directories.
  - Refuse with `ValueError` if `include_agent_skills=True` is invoked against Vertebra's own repository root (`(project_root / "src" / "rs" / "vertebra-core").exists()` and `(project_root / "src" / "py" / "vertebra").exists()`), preventing accidental bundling of Vertebra's internal engineering skills.
  - Set `VERTEBRA_APP_ROOT` (and resolve default relative `.agents/skills` against `app_dir` / `chdir` safe bootstrap) in `_render_bootstrap` so standalone and `VTB1FILE` onefile bundles discover `app/.agents/skills` regardless of caller working directory.

## 3. Documentation & OKF Knowledge Plan

Wheel binary installation, shared-libpython requirements, and `--include-agent-skills` standalone packaging are documented in `ch07` (`docs/usage/packaging-and-deployment.md`, `docs/getting-started/installation.md`, `docs/reference/cli.md`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task | Files | Verification |
| --- | --- | --- | --- |
| `R6a` | `t01` | `src/rs/vertebra-http/build.rs`, `tools/scripts/build_server_binary.py` | `pytest src/py/tests/unit/build/test_packaging_and_pgo.py` |
| `R6b` | `t02` | `src/py/vertebra/supervisor/_supervisor.py` | `pytest src/py/tests/unit/cli/test_cli_commands.py` |
| `R6c` | `t03` | `pyproject.toml`, `tools/scripts/build_server_binary.py` | `pytest src/py/tests/unit/build/test_packaging_and_pgo.py` |
| `R7` (`#115`) | `t04` | `src/py/vertebra/build/packaging/standalone.py`, `src/py/vertebra/cli/build.py` | `pytest src/py/tests/unit/build/test_packaging_and_pgo.py` |

## 5. Implementation Plan

- [x] [7a5cbc61] [t01-relocatable-build-rs-and-target.md](tasks/t01-relocatable-build-rs-and-target.md) — Remove absolute rpaths in `build.rs` and implement `ServerBinaryTarget` / `ServerBinaryBuilder`
- [x] [7a5cbc61] [t02-launcher-runtime-library-resolution.md](tasks/t02-launcher-runtime-library-resolution.md) — Add `EmbeddedPythonRuntime` shared-libpython check and runtime env export in `_supervisor.py`
- [x] [7a5cbc61] [t03-core-wheel-maturin-binary-include.md](tasks/t03-core-wheel-maturin-binary-include.md) — Include `src/py/vertebra/bin/*` in core wheels and verify libpython-excluding wheel repair commands
- [x] [7a5cbc61] [t04-standalone-builder-skills-staging.md](tasks/t04-standalone-builder-skills-staging.md) — Add opt-in `StandaloneBuilder` staging of user `.agents/skills` with Vertebra-repo guard ([#115](https://github.com/cofin/vertebra/issues/115))

## 6. Current State & Remaining Plan Snapshot

- **Status**: `ch03-relocatable-server-binary-and-packaging` is completed at `7a5cbc61` (`sync` `0e03d7a1`); next frontier is [`ch04-wheel-smoke-and-ci`](../ch04-wheel-smoke-and-ci/spec.md).
- **Fast Chapter Gate (`< 25s`)**: `uv run --no-sync ruff check src/py/vertebra/supervisor src/py/vertebra/build/packaging/standalone.py src/py/vertebra/cli/build.py tools/scripts/build_server_binary.py && uv run --no-sync mypy src/py/vertebra/supervisor src/py/vertebra/build/packaging/standalone.py src/py/vertebra/cli/build.py tools/scripts/build_server_binary.py && uv run --no-sync pytest src/py/tests/unit/build/test_packaging_and_pgo.py src/py/tests/unit/cli/test_cli_commands.py -q`.

[^http-build-rs]: `src/rs/vertebra-http/build.rs` — Current build script embedding absolute build-host rpaths.
[^supervisor-py]: `src/py/vertebra/supervisor/_supervisor.py::find_server_binary` — Server binary lookup and environment preparation.
[^pyproject-toml]: `pyproject.toml` — Maturin wheel include configuration for vertebra-core.
[^standalone-py]: `src/py/vertebra/build/packaging/standalone.py::StandaloneBuilder` — Standalone and onefile staging builder.
[^cli-build-py]: `src/py/vertebra/cli/build.py::build` — vtb build CLI command.
