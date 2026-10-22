---
type: Guide
title: Agentic Review and Benchmark Gates
description: Manifest-driven review workflow, checklist validation, and benchmark regression gates.
status: stable
tags: [review, testing, benchmarks, quality]
sources:
  - resource: tools/scripts/review_setup.py
  - resource: .agents/skills/review-core/references/areas.toml
  - resource: tools/benchmarks/cli.py
---

<!-- truth: start -->
- **Review routing**: `make review-route BASE=origin/main` maps modified files to owning areas and shards.
- **Review validation**: `make review-check` validates skills, manifests, agents, and privacy constraints.
- **Benchmark gate**: `make bench-check BASELINE=pre-refactor` enforces <= 3% throughput regression and <= 5% latency increase.
- **Suppression triage**: Every false-positive must be cataloged in `false-positive-suppressions.md` with concrete evidence.
<!-- truth: end -->

# Agentic Review and Benchmark Gates

## Overview

The Vertebra agentic review system provides automated, manifest-driven static and architectural quality gates across Rust, Python, and TypeScript subsystems. It pairs specialized review skills with strict finding ledgers and automated performance regression checks.

## Review Architecture

1. **Manifest and Routing**:
   - `areas.toml` defines area manifests, ownership patterns, shard paths, and validation budgets.
   - `make review-route BASE=origin/main` routes changed repository files to their designated area reviewers.
2. **Review Execution and Ledger Validation**:
   - Area reviewers audit code against authoritative checklists (`RT-01..12`, `HTTP-01..12`, `FFI-01..10`, etc.).
   - Findings adhere to machine-validated Markdown ledgers checked via `uv run python -m tools.scripts.review_setup ledger <path>`.
3. **Benchmark Regression Gate**:
   - `make rs-bench`: Runs Rust Criterion benchmarks.
   - `make bench-micro`: Measures hot-path primitive and warm import latencies.
   - `make bench-baseline NAME=pre-refactor`: Records authoritative baseline metrics.
   - `make bench-check BASELINE=pre-refactor`: Ensures zero regressions exceeding 3% throughput drop or 5% latency increase.
