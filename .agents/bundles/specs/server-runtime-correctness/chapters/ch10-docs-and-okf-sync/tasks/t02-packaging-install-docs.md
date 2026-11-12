---
type: Task
id: "server-runtime-correctness:ch10:t02"
chapter_id: ch10-docs-and-okf-sync
title: Document the wheel-shipped server binary, shared-libpython requirement, and platform matrix
description: Update docs/usage/packaging-and-deployment.md, docs/getting-started/installation.md, and docs/getting-started/cli.md to state that vertebra-core wheels ship vertebra/bin/vertebra-server for Linux x86_64 and aarch64, macOS, and Windows, that vtb run needs an interpreter with a shared libpython, how PYTHONHOME and library paths are set, and how to verify an install.
status: draft
state: pending
priority: P2
verification_strategy: documentation_validation
depends_on:
  - "server-runtime-correctness:ch10:t01"
read_set:
  - .agents/bundles/specs/server-runtime-correctness/chapters/ch06-relocatable-server-binary/spec.md
  - docs/usage/packaging-and-deployment.md
  - docs/getting-started/installation.md
  - docs/getting-started/cli.md
  - src/py/vertebra/supervisor/_supervisor.py
files:
  - docs/usage/packaging-and-deployment.md
  - docs/getting-started/installation.md
  - docs/getting-started/cli.md
tests: []
estimated_loc: 120
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
  - id: packaging-docs
    resource: docs/usage/packaging-and-deployment.md
    title: Packaging and deployment guide
  - id: install-docs
    resource: docs/getting-started/installation.md
    title: Installation page with extras table
  - id: cli-docs
    resource: docs/getting-started/cli.md
    title: Getting-started CLI guide
tags:
  - docs
  - packaging
---

# Task t02: Document the Wheel-Shipped Server Binary, Shared-libpython Requirement, and Platform Matrix

## Objective & Scope Boundary

Update three public pages for operators and new users. Out of scope: internal OKF chapters.

## Bounded Read Manifest

- [ch06 spec](../../ch06-relocatable-server-binary/spec.md) — build and runtime contract.
- `docs/usage/packaging-and-deployment.md` [^packaging-docs].
- `docs/getting-started/installation.md` [^install-docs].
- `docs/getting-started/cli.md` [^cli-docs].
- `src/py/vertebra/supervisor/_supervisor.py::{EmbeddedPythonRuntime,find_server_binary}`.

## Symbol & Signature Contracts

- `docs/usage/packaging-and-deployment.md`: section "Server Binary in the Wheel" — every `vertebra-core` wheel carries `vertebra/bin/vertebra-server` built for its CPython ABI (`3.12`–`3.15t`) on Linux x86_64 and aarch64 (manylinux), macOS x86_64 and arm64, and Windows AMD64; `vtb run`, `vtb dev`, and `litestar run` resolve it first (`VERTEBRA_SERVER_BINARY` overrides); the launcher sets `PYTHONHOME` to the running interpreter's base prefix and prepends its library directory to `LD_LIBRARY_PATH`, `DYLD_LIBRARY_PATH`, or `PATH`; container note (official `python:` images and uv-managed interpreters ship a shared libpython; Debian/Ubuntu need `libpython3.X`); deployment snippet installing with `uv pip install vertebra-core` and running `vtb run app:app`.
- `docs/getting-started/installation.md`: "Platform Support" table (OS, architecture, wheel tag family, server binary ✓) and an admonition: "`vtb run` needs a Python interpreter built with a shared libpython; pyenv users set `PYTHON_CONFIGURE_OPTS=--enable-shared`"; verification command `python -c "import importlib.resources as r; print(r.files('vertebra') / 'bin')"`.
- `docs/getting-started/cli.md`: `vtb run` section notes it starts the bundled server binary and documents `--worker-only`; error example for the shared-libpython requirement (exit code `2`).
- Installation commands added in these pages use linked `pymdownx.tabbed` blocks (`=== "uv"`, `=== "pip"`, `=== "pdm"`, `=== "poetry"`).

## Edge Cases & Error Semantics

- No mention of internal repository paths beyond the installed `vertebra/bin` location.
- The standalone release binaries are described as requiring a matching CPython installation.

## Public-Seam Test Blueprint

- **Verification category**: `documentation_validation`.

| Check | Concrete Input | Expected Output |
| --- | --- | --- |
| Build | `make docs-check` | Exit `0` |
| Platform table | `git grep -n "aarch64" -- docs/getting-started/installation.md` | At least one match in the table |
| Requirement text | `git grep -n "shared libpython" -- docs/usage/packaging-and-deployment.md docs/getting-started/installation.md docs/getting-started/cli.md` | Matches in all three |
| Tabbed installs | `git grep -n '=== "uv"' -- docs/usage/packaging-and-deployment.md` | At least one match |

## Verification

- Initial baseline: `git grep -n "shared libpython" -- docs`
- Final:
  ```bash
  make docs-check
  git grep -n "shared libpython" -- docs/usage/packaging-and-deployment.md docs/getting-started/installation.md docs/getting-started/cli.md
  ```
- Planned local commit: `docs(packaging): document the wheel-shipped vertebra-server binary`

## Acceptance Criteria

- [ ] Packaging, installation, and CLI getting-started pages describe the wheel binary, platforms, runtime environment, and interpreter requirement (R18).

## Notes & Discoveries

[^packaging-docs]: `docs/usage/packaging-and-deployment.md` — Packaging and deployment guide.
[^install-docs]: `docs/getting-started/installation.md` — Installation page with extras table.
[^cli-docs]: `docs/getting-started/cli.md` — Getting-started CLI guide.
