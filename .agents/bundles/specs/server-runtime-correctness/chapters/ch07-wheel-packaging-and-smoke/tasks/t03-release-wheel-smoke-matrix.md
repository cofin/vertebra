---
type: Task
id: "server-runtime-correctness:ch07:t03"
chapter_id: ch07-wheel-packaging-and-smoke
title: Run the vtb run smoke across the release wheel matrix and add linux-arm64 standalone binaries
description: Turn the release test_wheels job into a matrix over Linux x86_64, Linux aarch64, macOS arm64, and Windows AMD64 with CPython 3.12 and 3.14t that installs the built core wheel and runs tools/scripts/smoke_vtb_run.py alongside the existing namespace checks, and add a linux-arm64 entry to release-binaries.yml.
status: draft
state: pending
priority: P1
verification_strategy: static_validation
depends_on:
  - "server-runtime-correctness:ch07:t02"
read_set:
  - .agents/bundles/specs/server-runtime-correctness/chapters/ch07-wheel-packaging-and-smoke/spec.md
  - .github/workflows/release.yaml
  - .github/workflows/release-binaries.yml
  - tools/scripts/smoke_vtb_run.py
files:
  - .github/workflows/release.yaml
  - .github/workflows/release-binaries.yml
tests: []
estimated_loc: 90
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
  - id: release-yaml
    resource: .github/workflows/release.yaml
    title: test_wheels job and artifact naming cibw-wheels-<os>-<package>-<index>
  - id: release-binaries
    resource: .github/workflows/release-binaries.yml
    title: Standalone binary matrix
tags:
  - release
  - ci
  - aarch64
---

# Task t03: Run the vtb run Smoke across the Release Wheel Matrix and Add linux-arm64 Standalone Binaries

## Objective & Scope Boundary

Rewrite `test_wheels` in `.github/workflows/release.yaml` as a matrix job and add linux-arm64 to `.github/workflows/release-binaries.yml`. Out of scope: wheel build hooks (`t01`) and publishing jobs.

## Bounded Read Manifest

- [spec.md](../spec.md) — chapter contracts.
- `.github/workflows/release.yaml::test_wheels` and artifact names `cibw-wheels-${{ matrix.platform.os }}-${{ matrix.package.name }}-${{ strategy.job-index }}` [^release-yaml].
- `.github/workflows/release-binaries.yml` — `matrix.include` entries [^release-binaries].
- `tools/scripts/smoke_vtb_run.py` — CLI (`--vtb`, `--workdir`).

## Symbol & Signature Contracts

- `test_wheels`:
  - `strategy.fail-fast: false`; `matrix.platform: [{os: ubuntu-latest}, {os: ubuntu-24.04-arm}, {os: macos-latest}, {os: windows-latest}]`; `matrix.python: ["3.12", "3.14t"]`.
  - `runs-on: ${{ matrix.platform.os }}`; downloads `cibw-wheels-${{ matrix.platform.os }}-*` (merge-multiple) and checks out the repository (for `tools/scripts/smoke_vtb_run.py` only; the smoke runs from `$RUNNER_TEMP`).
  - Steps: `uv venv --python ${{ matrix.python }} --managed-python "$RUNNER_TEMP/wheel-smoke"`; install core and optional wheels as today (`--no-index --no-deps` then extras with `--only-binary :all:`); keep the existing namespace/export Python check on `ubuntu-latest` + `3.12` only (`if:`), since it imports every optional wheel; run `uv run --no-project --python ${{ matrix.python }} python tools/scripts/smoke_vtb_run.py --vtb "$RUNNER_TEMP/wheel-smoke/<bin|Scripts>/vtb" --workdir "$RUNNER_TEMP/smoke-project"` with `shell: bash`.
- `release-binaries.yml` adds `{target: linux-arm64, os: ubuntu-24.04-arm, bin_src: target/release/vertebra-server, bin_name: vertebra-server-linux-arm64}`.

## Edge Cases & Error Semantics

- `3.14t` wheels exist for all four platforms (`CIBW_BUILD` includes `cp314t-*`); a missing wheel fails the install step.
- Windows paths use `Scripts/vtb.exe`; the step computes the path with a bash conditional on `RUNNER_OS`.
- The smoke's POSIX-only SHM scenario self-skips on Windows.

## Public-Seam Test Blueprint

- **Verification category**: `static_validation` (schema + structural assertions; the smoke itself runs on release).

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Schema | `check-jsonschema --builtin-schema vendor.github-workflows release.yaml release-binaries.yml` | Exit `0` |
| Matrix shape | `yaml.safe_load(release.yaml)["jobs"]["test_wheels"]["strategy"]["matrix"]` | `platform` has `4` OS entries incl. `ubuntu-24.04-arm`; `python == ["3.12", "3.14t"]` |
| Smoke step present | Steps of `test_wheels` | One step runs `tools/scripts/smoke_vtb_run.py --vtb` |
| Publishing gated | `upload_release.needs`, `upload_pypi.needs` | Both still contain `test_wheels` |
| linux-arm64 binary | `release-binaries.yml` matrix | Entry with `target: linux-arm64` and `os: ubuntu-24.04-arm` |

## Verification

- Initial Red baseline: `uv run --no-sync python -c "import yaml, pathlib; m = yaml.safe_load(pathlib.Path('.github/workflows/release.yaml').read_text())['jobs']['test_wheels'].get('strategy'); assert m"`
- Final Green:
  ```bash
  uvx --from check-jsonschema check-jsonschema --builtin-schema vendor.github-workflows .github/workflows/release.yaml .github/workflows/release-binaries.yml
  uv run --no-sync python -c "import yaml, pathlib; wf = yaml.safe_load(pathlib.Path('.github/workflows/release.yaml').read_text()); m = wf['jobs']['test_wheels']['strategy']['matrix']; assert {p['os'] for p in m['platform']} == {'ubuntu-latest', 'ubuntu-24.04-arm', 'macos-latest', 'windows-latest'} and m['python'] == ['3.12', '3.14t']; assert 'test_wheels' in wf['jobs']['upload_pypi']['needs']"
  uv run --no-sync python -c "import yaml, pathlib; inc = yaml.safe_load(pathlib.Path('.github/workflows/release-binaries.yml').read_text())['jobs']['build']['strategy']['matrix']['include']; assert any(e['target'] == 'linux-arm64' and e['os'] == 'ubuntu-24.04-arm' for e in inc)"
  ```
- Planned local commit: `ci(release): smoke-test vtb run across release wheels and add linux-arm64 binaries`

## Acceptance Criteria

- [ ] Release `test_wheels` runs the `vtb run` smoke on Linux x86_64, Linux aarch64, macOS, and Windows for CPython 3.12 and 3.14t (R17).
- [ ] `release-binaries.yml` publishes a linux-arm64 standalone binary (R16).

## Notes & Discoveries

[^release-yaml]: `.github/workflows/release.yaml` — `test_wheels` job and artifact naming `cibw-wheels-<os>-<package>-<index>`.
[^release-binaries]: `.github/workflows/release-binaries.yml` — Standalone binary matrix.
