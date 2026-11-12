---
type: Spec
parent_flow: server-runtime-correctness
chapter_id: ch07-wheel-packaging-and-smoke
title: "Chapter 07: Core Wheel Server Binary, vtb run Smoke Test, CI, and Release Matrix"
description: Ship vertebra/bin/vertebra-server in every vertebra-core wheel through maturin include and cibuildwheel build and repair hooks, add a smoke test that installs the built wheel and runs vtb run against native, streaming, SHM, and worker-only targets via make core-wheel-check and CI, and run it across the release wheel matrix including linux-aarch64.
status: draft
state: planned
planning_status: complete
depends_on_chapters:
  - ch06-relocatable-server-binary
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
  - id: pyproject
    resource: pyproject.toml
    title: "[tool.maturin] include and exclude lists for vertebra-core"
  - id: release-yaml
    resource: .github/workflows/release.yaml
    title: cibuildwheel build_wheels matrix and test_wheels job
  - id: release-binaries
    resource: .github/workflows/release-binaries.yml
    title: Standalone binary matrix without linux-arm64
  - id: ci-yaml
    resource: .github/workflows/ci.yml
    title: Pull request CI jobs
  - id: makefile
    resource: Makefile
    title: Wheel check targets (postgres-wheel-check, oracle-wheel-check)
tags:
  - packaging
  - wheel
  - ci
  - smoke
---

# Chapter 07: Core Wheel Server Binary, vtb run Smoke Test, CI, and Release Matrix

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

`[tool.maturin]` includes only the TypeScript bundle and framework assets alongside `_vertebra` [^pyproject]; `release.yaml` builds core wheels on `ubuntu-latest` (x86_64), `ubuntu-24.04-arm` (aarch64), `macos-latest` (`x86_64 arm64`), and `windows-latest` (AMD64) without a server binary, and its `test_wheels` job only imports modules on `ubuntu-latest` [^release-yaml]. `release-binaries.yml` lacks a linux-arm64 entry [^release-binaries]. No check anywhere runs `vtb run` from an installed wheel. This chapter closes [#82](https://github.com/cofin/vertebra/issues/82) using `ch06`'s build script and runtime resolution.

### 1.2 Non-Goals

- Publishing changes (`upload_release`, `upload_pypi` keep their `needs`).
- Removing the standalone binaries workflow.

### 1.3 Upstream Baseline & Code Analysis

`build_wheels` sets one cibuildwheel environment for all packages, so core-only hooks use expressions on `matrix.package.name`. cibuildwheel exposes `{wheel}`, `{dest_dir}`, and `{delocate_archs}` placeholders to repair commands and sets `ARCHFLAGS` for macOS builds. Existing wheel checks (`postgres-wheel-check`, `oracle-wheel-check`) build into `/tmp/<name>` and verify with inline Python [^makefile].

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Chapter Requirements

- **R16** (`t01`, `t03`): Binary in every core wheel; repairs exclude libpython; linux-arm64 standalone binary.
- **R17** (`t02`, `t03`): Smoke test locally, in CI, and across the release matrix.

### 2.2 Concrete Type & Signature Contracts

```toml
# pyproject.toml [tool.maturin] include addition
{ path = "src/py/vertebra/bin/*", format = "wheel" },
```

```python
# tools/scripts/smoke_vtb_run.py
class SmokeScenario(msgspec.Struct, frozen=True, gc=False):
    name: str
    args: tuple[str, ...]
    probe_path: str
    expected_body: bytes
    streaming: bool = False
    posix_only: bool = False

class VtbRunSmoke:
    def __init__(self, vtb_executable: Path, workdir: Path) -> None: ...
    def write_project(self) -> None: ...
    def run(self, scenario: SmokeScenario) -> None: ...
    @classmethod
    def default_scenarios(cls) -> tuple[SmokeScenario, ...]: ...

def main(argv: Sequence[str] | None = None) -> int: ...
```

```make
core-wheel-check:  ## Build the core wheel with vertebra-server and smoke-test vtb run from a clean venv
```

### 2.3 Caller -> Seam -> Collaborator Map

| Caller | Seam | Collaborator |
| --- | --- | --- |
| cibuildwheel (core) | `CIBW_BEFORE_BUILD` | `python -m pip install uv` (Linux) + `python tools/scripts/build_server_binary.py build` |
| cibuildwheel repair (core, Linux/macOS) | `CIBW_REPAIR_WHEEL_COMMAND_{LINUX,MACOS}` | `build_server_binary.py repair {wheel} {dest_dir} [--delocate-archs {delocate_archs}]` |
| `make core-wheel-check`, CI `core-wheel-smoke`, release `test_wheels` | `tools/scripts/smoke_vtb_run.py --vtb <venv vtb>` | installed `vtb run` → packaged `vertebra-server` |

### 2.4 Edge-Case & Failure-Mode Matrix

| Input | Behaviour |
| --- | --- |
| Core wheel missing `vertebra/bin/vertebra-server[.exe]` | `CIBW_TEST_COMMAND` and the smoke script fail. |
| Optional engine wheels | No server hooks; unchanged. |
| sdist | Binary not included (`format = "wheel"`). |
| Smoke on Windows | SHM scenario skipped (`posix_only`); embedded, streaming, and worker-only run. |
| Smoke interpreter static | Smoke venv is created from `uv python` (shared libpython); `vtb run` on a static interpreter is covered by the `ch06:t02` unit test. |
| `vtb run` exits early | Smoke prints captured stderr and fails with the exit code. |
| Port collision | Smoke binds port `0` probe to pick a free port before launch. |

### 2.5 Alternatives Considered

A separate `vertebra-server` wheel per platform was rejected because the binary must match the core wheel's CPython ABI exactly and would duplicate the core matrix. Running the smoke only in the release workflow was rejected; PR CI must catch packaging regressions before release.

## 3. Documentation & OKF Knowledge Plan

Progress: `[0/0 files documented in ch07]`; packaging and smoke gates are documented in `docs/usage/packaging-and-deployment.md` (`ch10:t02`), `packaging-and-pgo.md` (`ch11:t01`), and `tech-stack.md` (`ch11:t02`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test / Gate |
| --- | --- | --- | --- |
| R16 | [t01](tasks/t01-core-wheel-binary-packaging.md) | `static_validation` | `uv build --wheel` archive listing; `CIBW_TEST_COMMAND` |
| R17 | [t02](tasks/t02-vtb-run-smoke-and-ci.md) | `integration` | `make core-wheel-check`; CI `core-wheel-smoke` |
| R16, R17 | [t03](tasks/t03-release-wheel-smoke-matrix.md) | `static_validation` | release `test_wheels` matrix; `release-binaries.yml` linux-arm64 |

## 5. Implementation Plan

- [ ] server-runtime-correctness:ch07:t01 — Package vertebra-server into every core wheel with libpython-excluding repairs (tasks/t01-core-wheel-binary-packaging.md)
- [ ] server-runtime-correctness:ch07:t02 — Smoke-test vtb run from an installed core wheel locally and in CI (tasks/t02-vtb-run-smoke-and-ci.md)
- [ ] server-runtime-correctness:ch07:t03 — Run the vtb run smoke across the release wheel matrix and add linux-arm64 standalone binaries (tasks/t03-release-wheel-smoke-matrix.md)

**Chapter Recovery Checkpoint:** `make core-wheel-check && uv run --no-sync python -c "import tomllib, pathlib; tomllib.loads(pathlib.Path('pyproject.toml').read_text())" && uvx --from check-jsonschema check-jsonschema --builtin-schema vendor.github-workflows .github/workflows/release.yaml .github/workflows/release-binaries.yml .github/workflows/ci.yml` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Baseline**: `plan_commit: null`; chapter `state: planned`.
- **Remaining Tasks**: `t01`–`t03` (`0/3`).
- **Downstream Dependents**: `ch10:t02`, `ch11:t01`–`ch11:t03`.

[^pyproject]: `pyproject.toml` — `[tool.maturin]` include and exclude lists for `vertebra-core`.
[^release-yaml]: `.github/workflows/release.yaml` — cibuildwheel `build_wheels` matrix and `test_wheels` job.
[^release-binaries]: `.github/workflows/release-binaries.yml` — Standalone binary matrix without linux-arm64.
[^ci-yaml]: `.github/workflows/ci.yml` — Pull request CI jobs.
[^makefile]: `Makefile` — Wheel check targets (`postgres-wheel-check`, `oracle-wheel-check`).
