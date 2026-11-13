---
type: Task
id: "server-runtime-correctness:ch03:t03"
flow_id: server-runtime-correctness
chapter_id: ch03-relocatable-server-binary-and-packaging
title: "Package vertebra/bin/* in Core Wheels and Exclude libpython from Wheel Repairs"
description: "Configure pyproject.toml [tool.maturin] wheel-only binary inclusion and add auditwheel/delocate repair command helpers in tools/scripts/build_server_binary.py."
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
  - pyproject.toml
  - tools/scripts/build_server_binary.py
tests:
  - src/py/tests/unit/build/test_packaging_and_pgo.py
read_set:
  - pyproject.toml
  - tools/scripts/build_server_binary.py
  - .github/workflows/release.yaml
  - src/py/tests/unit/build/test_packaging_and_pgo.py
estimated_loc: 110
tags:
  - packaging
  - wheel
  - maturin
  - cibuildwheel
sources:
  - id: pyproject-toml
    resource: pyproject.toml
    title: Maturin include configuration for vertebra-core
  - id: release-yaml
    resource: .github/workflows/release.yaml
    title: cibuildwheel repair hooks
---

# Task `t03`: Package vertebra/bin/* in Core Wheels and Exclude libpython from Wheel Repairs

## Objective & Scope Boundary

Ship `src/py/vertebra/bin/*` (`format = 'wheel'`) in `pyproject.toml` and implement `ServerBinaryBuilder.repair_command` + CLI `repair` subcommand in `tools/scripts/build_server_binary.py` ([#82](https://github.com/cofin/vertebra/issues/82)) per [`../spec.md` §2](../spec.md) [^pyproject-toml] [^release-yaml].

## Bounded Read Manifest

- `pyproject.toml` — `[tool.maturin]` `include` and `exclude` tables.
- `tools/scripts/build_server_binary.py` — `repair_command` CLI subcommand.

## Contracts & Edge Cases

- Add `{ path = 'src/py/vertebra/bin/*', format = 'wheel' }` to `[tool.maturin].include` so wheels include `vertebra/bin/vertebra-server[.exe]` while sdists never include precompiled binaries.
- `ServerBinaryBuilder.repair_command` generates `auditwheel repair -w <dest> --exclude <libpython_soname> <wheel>` on Linux and `delocate-wheel --require-archs <archs> -w <dest> -v --exclude libpython <wheel>` on macOS.

## Test Matrix

- Unit assertions in `test_packaging_and_pgo.py` parsing `pyproject.toml` `[tool.maturin].include` (`format == 'wheel'`) and parameterized tests for Linux `auditwheel` (`3.12`, `3.14t`) and macOS `delocate-wheel` exclusion flags.

## Verification

```bash
uv run --no-sync ruff check tools/scripts/build_server_binary.py && uv run --no-sync mypy tools/scripts/build_server_binary.py && uv run --no-sync pytest src/py/tests/unit/build/test_packaging_and_pgo.py -q
```

## Acceptance Criteria

- [ ] `pyproject.toml` includes `src/py/vertebra/bin/*` strictly for `wheel` format.
- [ ] Linux and macOS wheel repair commands exclude `libpython` across GIL and free-threaded ABIs.

[^pyproject-toml]: `pyproject.toml` — Maturin include configuration for vertebra-core.
[^release-yaml]: `.github/workflows/release.yaml` — cibuildwheel repair hooks.
