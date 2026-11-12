---
type: Task
id: "server-runtime-correctness:ch07:t01"
chapter_id: ch07-wheel-packaging-and-smoke
title: Package vertebra-server into every core wheel with libpython-excluding repairs
description: Add the staged server binary to the vertebra-core wheel include list, run the build script as a core-only cibuildwheel before-build hook, route core Linux and macOS repairs through the libpython-excluding repair wrapper, and assert the binary in the core cibuildwheel test command.
status: draft
state: pending
priority: P1
verification_strategy: static_validation
depends_on:
  - "server-runtime-correctness:ch06:t02"
read_set:
  - .agents/bundles/specs/server-runtime-correctness/chapters/ch07-wheel-packaging-and-smoke/spec.md
  - pyproject.toml
  - .github/workflows/release.yaml
  - tools/scripts/build_server_binary.py
files:
  - pyproject.toml
  - .github/workflows/release.yaml
tests: []
estimated_loc: 60
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
  - id: pyproject
    resource: pyproject.toml
    title: "[tool.maturin] include list"
  - id: release-yaml
    resource: .github/workflows/release.yaml
    title: build_wheels cibuildwheel environment
tags:
  - packaging
  - wheel
  - ci
---

# Task t01: Package vertebra-server into Every Core Wheel with libpython-Excluding Repairs

## Objective & Scope Boundary

Edit `pyproject.toml` and the `build_wheels` job of `.github/workflows/release.yaml` so every core wheel contains `vertebra/bin/vertebra-server` (`.exe` on Windows). Out of scope: the smoke script and Makefile (`t02`), the `test_wheels` job and `release-binaries.yml` (`t03`).

## Bounded Read Manifest

- [spec.md](../spec.md) — chapter contracts.
- `pyproject.toml` — `[tool.maturin]` `include`/`exclude` [^pyproject].
- `.github/workflows/release.yaml` — `build_wheels` matrix and `pypa/cibuildwheel@v4.3.5` env [^release-yaml].
- `tools/scripts/build_server_binary.py` — `build` and `repair` subcommands (`ch06:t01`).

## Symbol & Signature Contracts

- `pyproject.toml` `[tool.maturin].include` gains `{ path = "src/py/vertebra/bin/*", format = "wheel" }`.
- `.github/workflows/release.yaml` `build_wheels` → `Build wheels` step env additions:
  - `CIBW_BEFORE_BUILD_LINUX: ${{ matrix.package.name == 'core' && 'python -m pip install uv && python tools/scripts/build_server_binary.py build' || '' }}`
  - `CIBW_BEFORE_BUILD_MACOS: ${{ matrix.package.name == 'core' && 'python tools/scripts/build_server_binary.py build' || '' }}`
  - `CIBW_BEFORE_BUILD_WINDOWS: ${{ matrix.package.name == 'core' && 'python tools/scripts/build_server_binary.py build' || '' }}`
  - `CIBW_REPAIR_WHEEL_COMMAND_LINUX: ${{ matrix.package.name == 'core' && 'python tools/scripts/build_server_binary.py repair {wheel} {dest_dir}' || 'auditwheel repair -w {dest_dir} {wheel}' }}`
  - `CIBW_REPAIR_WHEEL_COMMAND_MACOS: ${{ matrix.package.name == 'core' && 'python tools/scripts/build_server_binary.py repair {wheel} {dest_dir} --delocate-archs {delocate_archs}' || 'delocate-wheel --require-archs {delocate_archs} -w {dest_dir} -v {wheel}' }}`
  - `CIBW_TEST_COMMAND` for core additionally asserts `importlib.resources.files('vertebra').joinpath('bin', 'vertebra-server' + ('.exe' if sys.platform == 'win32' else '')).is_file()`.
- The `CIBW_ENVIRONMENT_LINUX` `PATH` already includes `$HOME/.cargo/bin` for cargo inside the container; `uv` installs into the build interpreter's scripts directory, which cibuildwheel places on `PATH`.

## Edge Cases & Error Semantics

- Non-core packages keep cibuildwheel's default repair commands verbatim (stated explicitly as the expression fallbacks).
- A failed binary build fails the wheel build (no fallback to a binary-less wheel).
- macOS `x86_64` wheels built on arm64 runners use `ARCHFLAGS` to select `x86_64-apple-darwin`; the job's rust toolchain already installs both targets.

## Public-Seam Test Blueprint

- **Verification category**: `static_validation` (archive inspection + workflow schema).

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Local wheel contains binary | `uv run --no-sync python -m tools.scripts.build_server_binary build && uv build --wheel --out-dir /tmp/vertebra-core-wheel-check` | `zipfile.ZipFile(whl).namelist()` contains `vertebra/bin/vertebra-server` with external attr mode `0o755` |
| sdist excludes binary | `uv build --sdist --out-dir /tmp/vertebra-core-sdist-check` | No `vertebra/bin/vertebra-server` entry |
| Workflow schema | `check-jsonschema --builtin-schema vendor.github-workflows .github/workflows/release.yaml` | Exit `0` |
| Expression fallbacks | Read `release.yaml` with `yaml.safe_load` | Non-core repair defaults equal cibuildwheel's documented defaults above |

## Verification

- Initial Red baseline: `uv build --wheel --out-dir /tmp/vertebra-core-wheel-check && uv run --no-sync python -c "import glob, zipfile; w = glob.glob('/tmp/vertebra-core-wheel-check/vertebra_core-*.whl')[0]; assert 'vertebra/bin/vertebra-server' in zipfile.ZipFile(w).namelist()"`
- Final Green:
  ```bash
  rm -rf /tmp/vertebra-core-wheel-check
  uv run --no-sync python -m tools.scripts.build_server_binary build
  uv build --wheel --out-dir /tmp/vertebra-core-wheel-check
  uv run --no-sync python -c "import glob, zipfile; w = glob.glob('/tmp/vertebra-core-wheel-check/vertebra_core-*.whl')[0]; info = zipfile.ZipFile(w).getinfo('vertebra/bin/vertebra-server'); assert (info.external_attr >> 16) & 0o111"
  uvx --from check-jsonschema check-jsonschema --builtin-schema vendor.github-workflows .github/workflows/release.yaml
  ```
- Planned local commit: `build(wheel): ship vertebra-server in vertebra-core wheels`

## Acceptance Criteria

- [ ] Locally built core wheels contain an executable `vertebra/bin/vertebra-server`; sdists do not (R16).
- [ ] Release core wheels build the binary per ABI and repair with libpython excluded on Linux and macOS (R16).

## Notes & Discoveries

[^pyproject]: `pyproject.toml` — `[tool.maturin]` include list.
[^release-yaml]: `.github/workflows/release.yaml` — `build_wheels` cibuildwheel environment.
