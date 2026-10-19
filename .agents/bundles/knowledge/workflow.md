---
type: Guide
title: Project Workflow
description: Canonical polyglot build, lint, test, and benchmark workflow plus Flow OKF v0.2 state and verification rules for Vertebra.
status: stable
tags: [workflow, commands, verification, testing, ci]
sources:
  - resource: Makefile
  - resource: tools/scripts/setup-env.sh
  - resource: pyproject.toml
  - resource: src/rs/Cargo.toml
  - resource: src/ts/vertebra-client/package.json
---

# Project Workflow

<!-- truth: start -->
- Task files under `.agents/bundles/specs/<flow_id>/tasks/` are authoritative; `spec.md` is the synchronized checklist and continuity view.
- Use Vertebra's canonical [`Makefile`](../../../Makefile) targets (`make setup-env`, `make install`, `make dev`, `make rs-test`, `make py-test`, `make ts-test`, `make test`, `make lint`, `make lint-all`, `make check`, `make bench`, `make bench-ft`) and rebuild PyO3 extensions via `make dev` after any Rust edit before running `pytest`.
- Every task declares and justifies one `verification_strategy`; only observable behavior and defect correction require an initial failing test.
- Record durable findings in the task's `## Notes & Discoveries` and preserve unrelated worktree changes.
- Consumer operational skills install only under `.agents/skills/`. Product, knowledge, research, and specs remain under `.agents/bundles/` (`specs/` and `research/` are created lazily only when needed).
- Flow never creates, moves, force-updates, or deletes Git tags, and never pushes automatically. All changes in Vertebra must be made on a dedicated feature/fix branch with signed conventional commits (`commit.gpgsign=true`) and submitted via pull request—never directly on `main`.
<!-- truth: end -->

## Canonical Polyglot Commands (`Makefile`)

[`tools/scripts/setup-env.sh`](../../../tools/scripts/setup-env.sh) (`make setup-env`, run automatically by `make install` and `make upgrade`) detects internal Linux (`rodete` in `/etc/os-release`) and configures public registries in gitignored `uv.toml`, `.npmrc`, `src/ts/vertebra-client/.npmrc`, and `src/rs/.cargo/config.toml` while removing overrides on external local/CI machines.

```bash
# 1. Environment & Native PyO3 Extension Setup
make setup-env                                            # Auto-configure Rodete vs standard registries (uv, bun/npm, cargo)
make install                                              # Full fresh setup: venv (3.12), uv sync, bun install/build, maturin develop
make dev                                                  # Build Rust workspace + debug PyO3 extensions (_vertebra, _bundler)

# 2. Focused Language Test Suites
make rs-test                                              # Clean /dev/shm/*vertebra* and run cargo test --workspace --features tls,compression
make py-test                                              # Run make dev + uv run pytest src/py/tests -v
uv run pytest src/py/tests/unit/server/test_runner.py -q  # Focused Python test file (run make dev first if Rust changed)
make ts-test                                              # Run Vitest in src/ts/vertebra-client with isolated --localstorage-file

# 3. Aggregate Test, Lint & Pre-PR Verification Gates
make test                                                 # Run rs-test + py-test + ts-test
make lint                                                 # Auto-fix TS (Biome), Python (Ruff check/format), Rust (fmt/clippy), and run mypy
make lint-all                                             # Read-only CI gate: Biome + Ruff + mypy + cargo fmt --check + clippy -D warnings
make coverage                                             # Run pytest with XML/HTML coverage reports
make check                                                # Full verification gate: make lint + make test + make coverage

# 4. Release Builds & Comparative / Free-Threaded Benchmarks (--group bench isolated from dev)
make build                                                # Release build of @vertebra/client, Rust workspace, maturin wheels, and uv build
make bench                                                # Run tools.benchmarks (override with TARGETS=vtb SCENARIOS=default)
make bench-all                                            # Run full benchmark matrix across all targets and scenarios
make bench-ft                                             # Build .venv-ft + src/rs/target-ft against Python 3.14t and benchmark free-threaded mode
make bench-report                                         # Render latest benchmark summary report (make bench-diff to compare runs)
```

## Polyglot Rebuild & Hygiene Rules

1. **Rust $\to$ Python FFI Rebuild (`make dev`)**: `uv run pytest` loads compiled `.so` binaries (`vertebra._vertebra` and `vertebra._bundler`) from `.venv`. Whenever any `.rs` file in `vertebra-core`, `vertebra-py`, or `vertebra-bundler` changes, run `make dev` (or `make py-test`) before invoking `uv run pytest`.
2. **Codegen $\to$ TypeScript Sync**: When modifying route schemas or [`src/py/vertebra/builder/_codegen/`](../../../src/py/vertebra/builder/_codegen/__init__.py), verify both `make py-test` (`src/py/tests/unit/codegen/`) and `make ts-test`.
3. **Shared-Memory Cleanup**: Interrupted SHM tests can leave `/dev/shm/vertebra-*` or `/dev/shm/vtb-*` segments behind. `make rs-test` purges them automatically before running `cargo test`.
4. **`--group bench` Isolation**: Keep `duckdb`, `spikard`, `granian`, and `uvicorn` strictly inside `[dependency-groups].bench` in [`pyproject.toml`](../../../pyproject.toml) so `Python 3.14t` test runs never compile `duckdb` C++ from source.

## Direct-Read Continuity

1. Resolve `.agents/setup-state.json:root_directory`, defaulting to `.agents/`.
2. Before selecting a flow or doing normal work, scan `<configured-root>/transactions/*/journal.md` and the retired `<configured-root>/tasks/transactions/*/journal.md`. Jointly arbitrate every nonterminal journal (`prepared`, `task_writes_started`, `recovery_required`, `contended`, or `rollback_in_progress`); recover the selected transaction from its recorded fragments, or stop on unresolved conflict.
3. Resolve the configured bundle root, then read its index and candidate spec frontmatter under `specs/<flow_id>/`.
4. Read authoritative task frontmatter in `tasks/*.md`.
5. Verify plan/state identity, task dependencies, claims, and checklist agreement.
6. Select an explicit task, the sole in-progress task, or the first ready task.
7. Read the complete worksheet, direct dependencies, newest discoveries, and relevant nested knowledge chapters (`architecture/`, `domains/`, `patterns/`, `testing/`, `gotchas/`).

Hooks and prior conversation are routing hints, not authority. Task files are the single authority for task state.

## Task and State Operations

A task is ready when `state: open`, all `depends_on` tasks are `closed`, its worksheet is complete, and its plan identity matches the spec.

Resolve the canonical state authority through the active Flow installation. In plugin/default-skip mode, load the active packaged `flow-state` skill and its sibling `references/state.md`; do not require or synthesize a project-local skill path. In a standalone project install, load `.agents/skills/flow-state/SKILL.md` and resolve `references/state.md` relative to it. Every existing-flow mutation must carry the exact observed `expected_plan_revision`, `expected_plan_commit`, and `expected_state_revision`. Create the prepared transaction journal before tracked state changes, write in canonical task-first/spec-last order, reread the journal and semantic read set around each write, and record final validation before marking the journal terminal.

| Operation | Purpose |
| --- | --- |
| `claim` | Move one ready task to `in_progress`. |
| `note` | Append investigation findings to `## Notes & Discoveries`. |
| `block` / `unblock` | Record or resolve an exact blocker and next step. |
| `release` | Return an in-progress task to open when the claimant stops. |
| `close` | Close the sole claimed task with recorded commit SHA. |
| `reconcile` | Update spec checklist markers from task-file frontmatter via `/flow:sync`. |
| `complete` | Move an active spec whose tasks are all closed or skipped to `completed` after verification and review. |
| `archive` | Finish a completed spec: elevate knowledge into nested chapters, log summary in `log.md`, then delete the spec directory. |
| `recover` | Resume an unresolved transaction journal in its recorded finish-or-rollback direction. |

## Verification Strategies

| Strategy | Select for | Required evidence |
| --- | --- | --- |
| `behavior_tdd` | New observable behavior | Focused behavior fails because it is absent; minimal implementation makes it green. |
| `regression_tdd` | Defect correction | Focused reproduction demonstrates the defect; the narrow fix makes it green. |
| `characterization` | Behavior-preserving refactor/deletion | Green focused baseline before and unchanged behavior after. |
| `static_validation` | Manifest, config, generated surface, tooling | Native parser/lint/type/build; isolated representative violation proves a new/replacement gate fails with the expected diagnostic. |
| `documentation_validation` | Links, examples, docs structure | Docs-native baseline and final link/example/build/structure checks. |
| `integration_acceptance` | Composition of existing contracts | Green focused baseline; end-to-end scenario plus injected negative states proving refusal paths. |

Never manufacture a failing unit test for documentation, configuration, generated output, prose, or behavior-preserving cleanup.

## Execution Sequence & Quality Gates

1. Preflight the worksheet, live targets, dependencies, strategy, plan identity, state revision, and worktree.
2. Claim the task through the state contract and record discoveries in `## Notes & Discoveries`.
3. Obtain the strategy's required initial evidence, make the minimum worksheet-scoped change, and obtain focused green evidence.
4. Run relevant aggregate gates (`make lint-all`, `make rs-test`, `make py-test`, `make ts-test`) and confirm `git diff --check` is clean.
5. Stage exact task-owned paths only and create one signed conventional commit (`<type>(<scope>): <description>`) on the feature/fix branch.
6. Close the task with the commit SHA, exact commands/results, and checked acceptance criteria; reconcile `spec.md` in the same state transaction.
7. During `/flow:archive`, synthesize reusable learnings into the nested `.agents/bundles/knowledge/{architecture,domains,patterns,testing,gotchas}/**/*.md` tree, append the archive summary to `.agents/bundles/log.md`, and remove the completed spec directory.
