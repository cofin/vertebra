---
type: Task
id: "strict-typing-hygiene:t04"
flow_id: strict-typing-hygiene
title: "Synchronize Tech Stack, Polyglot Test Matrix, and Workflow OKF Chapters"
description: "Update product/tech-stack.md, knowledge/testing/polyglot-test-matrix.md (keeping <= 200L), and knowledge/workflow.md to reflect pruned mypy overrides and benchmark/test typing gates."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: documentation_validation
plan_commit: 18baadcd
depends_on:
  - "strict-typing-hygiene:t03"
files:
  - .agents/bundles/product/tech-stack.md
  - .agents/bundles/knowledge/testing/polyglot-test-matrix.md
  - .agents/bundles/knowledge/workflow.md
tests: []
read_set:
  - .agents/bundles/product/tech-stack.md
  - .agents/bundles/knowledge/testing/polyglot-test-matrix.md
  - .agents/bundles/knowledge/workflow.md
  - pyproject.toml
  - Makefile
estimated_loc: 120
tags:
  - okf
  - typing
  - testing
  - workflow
sources:
  - id: okf-tech-stack
    resource: .agents/bundles/product/tech-stack.md
    title: "Product technology stack Mypy Overrides and [tool.mypy].files bullets"
  - id: okf-test-matrix
    resource: .agents/bundles/knowledge/testing/polyglot-test-matrix.md
    title: "Polyglot test matrix OKF chapter (198 lines on HEAD)"
  - id: okf-workflow
    resource: .agents/bundles/knowledge/workflow.md
    title: "Project workflow Makefile lint and lint-all commands"
---

# Task `t04`: Synchronize Tech Stack, Polyglot Test Matrix, and Workflow OKF Chapters

## Objective & Scope Boundary

Update `.agents/bundles/product/tech-stack.md` (`<= 200L`), `.agents/bundles/knowledge/testing/polyglot-test-matrix.md` (`198L <= 200L`), and `.agents/bundles/knowledge/workflow.md` (`<= 200L`) in present tense to match the pruned `[tool.mypy.overrides]` list and expanded `tools/benchmarks` + `src/py/tests/{unit,integration}` Ruff/Mypy gates per [`../spec.md` §2](../spec.md) [^okf-tech-stack] [^okf-test-matrix] [^okf-workflow].

## Bounded Read Manifest

- `.agents/bundles/product/tech-stack.md` — `Verification & Documentation Toolchain` and `Mypy Overrides` bullets (`<= 200L`) [^okf-tech-stack].
- `.agents/bundles/knowledge/testing/polyglot-test-matrix.md` — `make lint` / `make lint-all` table and test standards (`198L`) [^okf-test-matrix].
- `.agents/bundles/knowledge/workflow.md` — Fast Chapter Gate and `Makefile` command summary (`<= 200L`) [^okf-workflow].
- `pyproject.toml` — shipped `[tool.mypy]` configuration from `t01` and `t03`.
- `Makefile` — shipped `lint` and `lint-all` targets from `t03`.

## Contracts & Edge Cases

- `product/tech-stack.md` (`<= 200` lines, expanded YAML `tags:` list): update `[tool.mypy].files` (`src/py/vertebra`, `src/py/tests/unit`, `src/py/tests/integration`, `tools/benchmarks`, `tools/scripts`, `tools/pgo`, `tools/docs`, `docs/examples`) and replace the `40+`-module `Mypy Overrides` list with the minimal untyped/optional C-package list; re-anchor `verified` commits to ancestors of `HEAD`.
- `knowledge/testing/polyglot-test-matrix.md` (`<= 200` lines, `<= 40`-line truth block): update `make lint` / `make lint-all` paths (`src/py/tests/{unit,integration}`) and 10-extension `stubtest` list while keeping `<= 200` lines, and re-anchor `verified` commits.
- `knowledge/workflow.md` (`<= 200` lines): update `lint`/`lint-all` descriptions and companion `*-wheel-check` references.

## Test Matrix

- Assert `wc -l <= 200` on `tech-stack.md`, `polyglot-test-matrix.md`, and `workflow.md`; run `make review-check`.

## Verification

```bash
python3 -c 'from pathlib import Path; [(_ for _ in ()).throw(AssertionError(f"{p}: {len(Path(p).read_text().splitlines())}L")) for p in (".agents/bundles/product/tech-stack.md", ".agents/bundles/knowledge/testing/polyglot-test-matrix.md", ".agents/bundles/knowledge/workflow.md") if len(Path(p).read_text().splitlines()) > 200]' && make review-check
```

## Acceptance Criteria

- [ ] `tech-stack.md`, `polyglot-test-matrix.md`, and `workflow.md` are all `<= 200` lines, accurately reflect the pruned `mypy` overrides and expanded `tools/benchmarks` + `src/py/tests` gates, and pass `make review-check` (`R4`).
- [ ] Zero regressions in OKF validation.

[^okf-tech-stack]: `.agents/bundles/product/tech-stack.md` — Product technology stack `Mypy Overrides` and `[tool.mypy].files` bullets.
[^okf-test-matrix]: `.agents/bundles/knowledge/testing/polyglot-test-matrix.md` — Polyglot test matrix OKF chapter (`198` lines on `HEAD`).
[^okf-workflow]: `.agents/bundles/knowledge/workflow.md` — Project workflow `Makefile` `lint` and `lint-all` commands.
