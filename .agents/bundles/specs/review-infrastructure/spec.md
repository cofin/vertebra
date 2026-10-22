---
type: Spec
flow_id: review-infrastructure
title: "Chapter 1: Agentic review infrastructure, benchmark harness, and baseline ledger"
state: closed
plan_revision: 2
plan_commit: ""
state_revision: 0
current_task: null
last_operation: null
operation_targets: []
last_verified_checkpoint: null
created_at: 2026-10-22T03:30:00Z
updated_at: 2026-10-22T03:30:00Z
description: "Install a manifest-driven agentic review system (8 area review skills, 16 paired reviewer/specialist agents, structural validator, and pre-commit/CI gates), add Criterion and Python micro-benchmark harnesses with a committed pre-refactor baseline, and run the baseline review."
tags: [feature, tooling, review, benchmarks]
parent_prd: vertebra-design-quality
research: [../vertebra-design-quality/findings.md, ../vertebra-design-quality/review-infra-design.md]
---

# Flow: Chapter 1: Agentic review infrastructure, benchmark harness, and baseline ledger

**Flow ID:** `review-infrastructure`

## Specification

### Parent Context

This chapter is Chapter 1 of the [`vertebra-design-quality`](../vertebra-design-quality/spec.md) master roadmap. It executes first on branch `feat/review-infrastructure` branched from `feat/stack-followups` (commit `edceea6`) before any code refactor or hot-path optimization chapter runs. It owns roadmap requirements:

- **R1 (Repo-native review system):** Manifest-driven review configuration (`areas.toml`), shared checklist (`RT`, `HTTP`, `FFI`, `PY`, `TS`, `PERF`, `TEST`, `DOC`) and false-positive catalog, 8 area review skills (`review-<area>`), 16 paired custom subagents (`<area>-reviewer` read-only + `<area>-specialist` write-enabled), and structural validator (`tools/scripts/review_setup.py`) wired into `make review-check`, `make review-route`, `make lint-all`, `.pre-commit-config.yaml`, and pytest.
- **R2 (Baseline review ledger):** Full-tree fan-out of the 8 area reviewers across `feat/stack-followups`, merged and validated into `.agents/bundles/specs/review-infrastructure/research/baseline-ledger.md` and deduplicated against `findings.md`.
- **R3 (Committed benchmark baseline and regression gate):** Makefile targets `rs-bench`, `bench-micro`, `bench-baseline`, and `bench-check` (`TOOL-04`, `CORE-40` Makefile part, `HTTP-41` Makefile part), fix for silent 3.14t benchmark fallback (`OKF-08`), and the committed `tools/benchmarks/baselines/pre-refactor.json` baseline used by Chapters 2–8.

### Code Analysis Summary

The repository tooling, agent configuration, and benchmark harness were inspected at commit `edceea6`:

- **`.gitignore:66-75`:** Ignores `.agents/*` and allow-lists only `!.agents/bundles/`, `!.agents/skills/`, and `!.agents/config.json`. `.agents/agents/` is currently gitignored and must be allow-listed (`!.agents/agents/`, `!.agents/agents/**`).
- **`.agents/skills/`:** Contains only `.agents/skills/flow-memory-keeper/SKILL.md` (no `<!-- truth: start -->` block). The validator must scope line-count (`<= 200`) and truth-block (`<= 40`) rules to `review-*` skills so `flow-memory-keeper` is untouched.
- **`Makefile:175-272` & `pyproject.toml:76-116`:**
  - `py-lint` (`Makefile:191-196`) and `lint-all` (`Makefile:204-213`) run Ruff and Mypy only on `src/py/vertebra` and `src/py/tests`; `tools/scripts/` has no `__init__.py` and is unlinted.
  - `[tool.pytest.ini_options]` (`pyproject.toml:88-90`) sets `testpaths = ["src/py/tests"]` without `pythonpath = ["."]`, requiring `sys.path` hacks to import `tools.*`.
  - No `Makefile` target runs Rust Criterion benchmarks (`src/rs/vertebra-core/benches/*.rs`, `src/rs/vertebra-http/benches/server_throughput.rs`) (`TOOL-04`, `CORE-40`, `HTTP-41`).
  - `tools/benchmarks/targets.py:95-100` `_ft_run` returns `["echo", "3.14t server binary not found at ..."]` when `src/rs/target-ft/release/vertebra-server` is missing, causing `make bench-all` to succeed silently without benchmarking free-threaded Python (`OKF-08`).

#### Findings not carried in this chapter

- **`CORE-40` (Rust benchmark file deletions and additions):** Deleting `benches/router.rs`, `benches/guards.rs`, and `benches/mpmc.rs` and merging `spin_wait.rs` into `spsc.rs` belong to Chapter 2 Task 2.2 (when `Router` and `guards.rs` are deleted); adding `benches/mpmc_contention.rs` and `benches/bridge.rs` belongs to Chapter 8 Task 8.1. This chapter adds the `make rs-bench` target (`Task 1.6`).
- **`HTTP-41` (New HTTP Criterion benchmark groups):** Adding `shm_dispatch/roundtrip_1000`, `response_decode/slot_20_headers`, and `compression` Criterion groups belongs to Chapter 8 Task 8.1; this chapter wires `cargo bench -p vertebra-http --bench server_throughput` into `make rs-bench` (`Task 1.6`).

### Relevant Patterns

- [Agentic Review Infrastructure Design](../vertebra-design-quality/review-infra-design.md) — Complete specification for `areas.toml`, `review_setup.py` rule groups, checklist IDs, false-positive catalog, agent frontmatter schema, and baseline review procedure.
- [Product and Engineering Guidelines](../../product/product-guidelines.md) — `msgspec.Struct(frozen=True, gc=False)`, module/class ordering, zero inline `#` comments in Python, `sys.stdout.write`/`sys.stderr.write`, and strict privacy rules.
- [Project Workflow](../../knowledge/workflow.md) — Verification strategies (`static_validation`, `documentation_validation`) and Makefile gate discipline.

### Requirements

| ID | Requirement |
| --- | --- |
| C1-R1 | Implement `tools/scripts/review_setup.py` (`ReviewIssue`, `ReviewManifest`, `MarkdownDocument`, `ReviewSetupValidator`, `ReviewRouter`, `LedgerValidator`) with `validate`, `route`, and `ledger` CLI subcommands covering rule groups `manifest`, `checklist`, `suppressions`, `skills`, `agents`, `index`, `files`, `bundles`, `privacy`, and `ledger`, backed by unit tests in `src/py/tests/unit/test_review_setup.py`. |
| C1-R2 | Create `.agents/skills/review-core/` containing `SKILL.md` and `references/{areas.toml,checklist.md,false-positive-suppressions.md,test-strength.md,ledger-format.md,suppression-playbook.md}` with stable checklist IDs (`RT-01..12`, `HTTP-01..12`, `FFI-01..10`, `PY-01..12`, `TS-01..08`, `PERF-01..09`, `TEST-01..11`, `DOC-01..09`) and verified seed false-positive entries (`FP-*`). |
| C1-R3 | Create the 8 area review skills (`.agents/skills/review-{runtime-ipc,http-ingress,pyo3-bridge,python-framework,typescript-client,performance,tests,docs-okf}/SKILL.md` + `references/`) and `.agents/skills/index.md` linking existing OKF knowledge chapters without duplicating rule prose. |
| C1-R4 | Allow-list `.agents/agents/` in `.gitignore` and create 16 paired custom subagents (`.agents/agents/<area>-{reviewer,specialist}.md`) with full research/verification tools (`search_web`, `moma_search`, `code_search`, `skill_search`, `call_mcp_tool`, `run_command`), read-only enforcement on reviewers, and write tools on specialists. |
| C1-R5 | Wire `make review-check` and `make review-route` into `Makefile`, add `review-check` and `tools/scripts` linting/type-checking to `make lint` and `make lint-all`, and register the `review-setup` local hook in `.pre-commit-config.yaml`. |
| C1-R6 | Add `make rs-bench`, `make bench-micro` (`tools/benchmarks/micro.py`), `make bench-baseline` (`tools/benchmarks/baseline.py`), and `make bench-check` to `Makefile`, fix `tools/benchmarks/targets.py:95-100` (`OKF-08`) to raise `FileNotFoundError` when the 3.14t binary is missing, and commit `tools/benchmarks/baselines/pre-refactor.json`. |
| C1-R7 | Run the 8 area reviewers across `feat/stack-followups`, validate and merge their outputs into `.agents/bundles/specs/review-infrastructure/research/baseline-ledger.md`, deduplicate against `findings.md`, and route any newly discovered Critical/Important findings into Chapters 2–9 via `/flow:revise`. |
| C1-R8 | Document the agentic review and benchmark gate workflow in `.agents/bundles/knowledge/testing/agentic-review.md` and update `knowledge/index.md`, `testing/index.md`, `workflow.md`, and `AGENTS.md`. |

### Target Layout

```text
.gitignore                                         allow-list !.agents/agents/ and !.agents/agents/**
.pre-commit-config.yaml                            local review-setup hook
Makefile                                           review-check, review-route, rs-bench, bench-micro, bench-baseline, bench-check
pyproject.toml                                     pythonpath = ["."] in pytest, tools/scripts in ruff/mypy
tools/
  scripts/
    __init__.py                                    package marker for ruff INP001 and mypy
    review_setup.py                                manifest, validator, router, and ledger CLI
  benchmarks/
    targets.py                                     _ft_run raises FileNotFoundError when 3.14t binary is missing (OKF-08)
    micro.py                                       bridge FFI and import-time micro-benchmarks
    baseline.py                                    baseline JSON writer and regression comparator
    baselines/
      pre-refactor.json                            committed pre-refactor benchmark baseline
src/py/tests/unit/
  test_review_setup.py                             real-repo check + isolated fixture tests for every validator rule
.agents/
  agents/
    <area>-reviewer.md (x8)                        read-only domain reviewers
    <area>-specialist.md (x8)                      write-enabled domain specialists
  skills/
    index.md                                       domain -> skill -> reviewer -> specialist -> checklist -> FP index
    review-core/
      SKILL.md
      references/
        areas.toml
        checklist.md
        false-positive-suppressions.md
        test-strength.md
        ledger-format.md
        suppression-playbook.md
    review-{runtime-ipc,http-ingress,pyo3-bridge,python-framework,typescript-client,performance,tests,docs-okf}/
      SKILL.md
      references/*.md
  bundles/knowledge/testing/
    agentic-review.md                              OKF testing chapter for review and benchmark gates
```

### Risks

| Risk | Mitigation |
| --- | --- |
| Concurrent area reviewers corrupt shared `/dev/shm` segments or `.venv` `.so` files if they run `make rs-test` or `make py-test`. | `areas.toml` `reviewer_forbidden_commands` and reviewer `## Allowed Commands` forbid `make rs-test`, `make py-test`, `make dev`, and `maturin develop`; preflight runs once before fan-out, and reviewers run only `cargo test -p <crate> --test <name>` or `uv run pytest <file>`. |
| Privacy scan leaks internal project names if the forbidden term list is hardcoded in the repo. | `areas.toml` commits only generic structural regexes (`/(?:home|Users)/`, `/usr/local/\w+/home/`, count-prefixed labels); project-specific terms load from `VERTEBRA_REVIEW_FORBIDDEN_TERMS` or gitignored `.agents/scratch/forbidden-terms.txt` and are reported by index only. |
| Micro-benchmark jitter causes false regression failures in `make bench-check`. | `tools/benchmarks/micro.py` runs warmup iterations and records the median of 5 timed rounds; `baseline.py` enforces the roadmap Global Constraint 9 thresholds (>3% throughput drop or >5% p99 latency increase). |

## Implementation Plan

### Phase 1: Validator, Shared Review Core, Skills, and Agents
- [x] 1.1 Review setup validator and tests ([worksheet](tasks/1.1.md))
- [x] 1.2 Shared review core ([worksheet](tasks/1.2.md))
- [x] 1.3 Area review skills ([worksheet](tasks/1.3.md))
- [x] 1.4 Paired reviewer and specialist agents ([worksheet](tasks/1.4.md))
- [x] 1.5 Make, pre-commit, and lint wiring ([worksheet](tasks/1.5.md))
- [x] Checkpoint: `make review-check && make lint-all && uv run pytest src/py/tests/unit/test_review_setup.py -q`

### Phase 2: Benchmark Harness, Pre-Refactor Baseline, and Baseline Review Run
- [x] 1.6 Benchmark harness and `pre-refactor` baseline ([worksheet](tasks/1.6.md))
- [x] 1.7 Baseline review run and triage ([worksheet](tasks/1.7.md))
- [x] 1.8 Review workflow knowledge chapter ([worksheet](tasks/1.8.md))
- [x] Chapter gate: `make review-check && make lint-all && make rs-test && make py-test && make ts-test`

## Requirement-to-task trace

| Requirement | Tasks | Verification |
| --- | --- | --- |
| C1-R1 | 1.1 | `uv run pytest src/py/tests/unit/test_review_setup.py -q` |
| C1-R2 | 1.2 | `uv run python -m tools.scripts.review_setup validate --rules manifest,checklist,suppressions` |
| C1-R3 | 1.3 | `uv run python -m tools.scripts.review_setup validate --rules skills,index` |
| C1-R4 | 1.4 | `uv run python -m tools.scripts.review_setup validate --rules agents,files,privacy` |
| C1-R5 | 1.5 | `make review-check && make lint-all && uv run pre-commit run review-setup --all-files` |
| C1-R6 | 1.6 | `make bench-check BASELINE=pre-refactor` |
| C1-R7 | 1.7 | `uv run python -m tools.scripts.review_setup ledger .agents/bundles/specs/review-infrastructure/research/baseline-ledger.md` |
| C1-R8 | 1.8 | `uv run python -m tools.scripts.review_setup validate --rules bundles,privacy` |

## Finding-to-task trace

| Finding | Task |
| --- | --- |
| TOOL-04 | 1.6 |
| OKF-08 | 1.6 |
| CORE-40 (Makefile `rs-bench` target part) | 1.6 |
| HTTP-41 (Makefile `rs-bench` target part) | 1.6 |

## Continuity Snapshot

- Active flow: `review-infrastructure`, state `planned`. Current task: none. Next step: `/flow:implement review-infrastructure` after roadmap approval.
- Plan identity: plan_revision 2, plan_commit "". State identity: state_revision 0.
- Blockers: none. Decisions: inherited D1–D4 from the roadmap.
- Knowledge paths: `.agents/bundles/product/product-guidelines.md`, `.agents/bundles/knowledge/workflow.md`, `.agents/bundles/knowledge/testing/verification-matrix.md`.
