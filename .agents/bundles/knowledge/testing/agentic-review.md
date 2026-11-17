---
type: Guide
title: Agentic Review and Benchmark Gates
description: Manifest-driven review workflow, checklist validation, and benchmark regression gates.
status: stable
tags:
  - review
  - testing
  - benchmarks
  - quality
generated:
  by: flow/0.26.0
  at: "2026-11-04T04:33:00Z"
verified:
  - by: agent:flow-executor
    at: "2026-11-17T08:15:00Z"
    commit: 5a833832
  - by: agent:flow-setup
    at: "2026-11-07T03:55:00Z"
    commit: 067fa901
  - by: agent:flow-setup
    at: "2026-11-04T04:33:00Z"
    commit: f741701
sources:
  - id: src-tools-scripts-review-setup-py
    resource: tools/scripts/review_setup.py
    title: "Repository tooling module (tools/scripts/review_setup.py)"
  - id: src-review-core-references-areas-toml
    resource: .agents/skills/review-core/references/areas.toml
    title: "Repository source (.agents/skills/review-core/references/areas.toml)"
  - id: src-tools-benchmarks-cli-py
    resource: tools/benchmarks/cli.py
    title: "Repository tooling module (tools/benchmarks/cli.py)"
  - id: src-makefile
    resource: Makefile
    title: "Canonical polyglot build, test, lint, docs, and benchmark targets"
  - id: src-check-extras-py
    resource: tools/scripts/check_extras.py
    title: "Bidirectional pyproject.toml optional extras parity checker"
---

# Agentic Review and Benchmark Gates

<!-- truth: start -->
- `make review-route BASE=origin/main` maps modified files to owning areas and shards (covering all 13 PyO3 extensions under `[areas.pyo3-bridge]`).
- `make lint` / `make lint-all` enforce `tsc --noEmit`, `pyright src/py/vertebra`, `tools/scripts/check_extras.py`, fail-loud 13-extension `mypy.stubtest`, and `make review-check`.
- `make bench-check BASELINE=pre-refactor` enforces `<= 3%` throughput regression and `<= 5%` latency increase.
- Every false-positive suppression is cataloged in `false-positive-suppressions.md` with concrete evidence.
<!-- truth: end -->

## Overview

The Vertebra agentic review system provides automated, manifest-driven static and architectural quality gates across Rust, Python, and TypeScript subsystems. It pairs specialized review skills with strict finding ledgers, workspace type/extras parity gates, and automated performance regression checks.

## Review Architecture

1. **Manifest and Routing**:
   - `areas.toml` defines area manifests, ownership patterns, shard paths, validation budgets, and static subagent tool lists (omitting runtime-only dynamic MCP multiplexer tools `call_mcp_tool`, `list_resources`, and `read_resource` from `.agents/agents/*.md` frontmatter so custom subagents launch cleanly against the static tool registry). `[areas.pyo3-bridge]` owns all 13 native extensions (`vertebra-py`, `vertebra-bundler`, `vertebra-storage`, `vertebra-mysql`, `vertebra-bigquery`, `vertebra-mssql`, `vertebra-postgres`, `vertebra-spanner`, `vertebra-duckdb`, `vertebra-oracle`, `vertebra-sqlite`, `vertebra-redis`, `vertebra-valkey`) and their `.pyi` stubs.
   - `make review-route BASE=origin/main` routes changed repository files to their designated area reviewers.
2. **Review Execution, Static Type/Extras Gates, and Ledger Validation**:
   - Area reviewers audit code against authoritative checklists (`RT-01..12`, `HTTP-01..12`, `FFI-01..10`, `PY-01..12`, etc.).
   - `make review-check` enforces AST-level Python structural invariants in [`tools/scripts/review_setup.py`](../../../../tools/scripts/review_setup.py)[^src-tools-scripts-review-setup-py], including `PY-07` (clean domain naming without `Vertebra*`, `Native*`, `*VtbDialect`, or `X = Y` compatibility aliases) and `PY-08` (centralized `vertebra.typing` re-exports without `vertebra._typing` imports outside `typing.py` and `vertebra.utils.module_loader` usage).
   - `make lint` and `make lint-all` ([`Makefile`](../../../../Makefile)[^src-makefile]) run `bunx tsc --noEmit` alongside Biome, `uv run --no-sync` across Ruff, Mypy, and `pyright src/py/vertebra`, bidirectional extras verification via [`tools/scripts/check_extras.py`](../../../../tools/scripts/check_extras.py)[^src-check-extras-py], fail-loud extension presence checks + `mypy.stubtest` across all 13 extensions, Rust `cargo fmt` + `cargo clippy --workspace --all-targets --features tls,http3,compression,gcp -- -D warnings`, and `make review-check`.
   - Findings adhere to machine-validated Markdown ledgers checked via `uv run --no-sync python -m tools.scripts.review_setup ledger <path>`.
3. **Benchmark Regression Gate**:
   - `make rs-bench`: Runs Rust Criterion benchmarks.
   - `make bench-micro`: Measures hot-path primitive and warm import latencies.
   - `make bench-baseline NAME=pre-refactor`: Records authoritative baseline metrics.
   - `make bench-check BASELINE=pre-refactor`: Ensures zero regressions exceeding 3% throughput drop or 5% latency increase.

Provenance sources: [^src-review-core-references-areas-toml], [^src-tools-benchmarks-cli-py].

[^src-tools-scripts-review-setup-py]: `tools/scripts/review_setup.py` — Repository tooling module (tools/scripts/review_setup.py).
[^src-review-core-references-areas-toml]: `.agents/skills/review-core/references/areas.toml` — Repository source (.agents/skills/review-core/references/areas.toml).
[^src-tools-benchmarks-cli-py]: `tools/benchmarks/cli.py` — Repository tooling module (tools/benchmarks/cli.py).
[^src-makefile]: `Makefile` — Canonical polyglot build, test, lint, docs, and benchmark targets.
[^src-check-extras-py]: `tools/scripts/check_extras.py` — Bidirectional `pyproject.toml` optional extras parity checker.
