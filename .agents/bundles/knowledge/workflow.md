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
- Use Vertebra's canonical [`Makefile`](../../../Makefile) targets (`make setup-env`, `make install`, `make dev`, `make rs-test`, `make py-test`, `make ts-test`, `make test`, `make lint`, `make lint-all`, `make review-check`, `make review-route`, `make coverage`, `make check`, `make build`, `make build-pgo`, `make bench`, `make bench-all`, `make bench-ft`, `make bench-report`, `make rs-bench`, `make bench-micro`, `make bench-baseline`, `make bench-check`, `make upgrade`) and rebuild the PyO3 extensions (`_vertebra`, `_bundler`, `_storage`) via `make dev` after any Rust edit before running `pytest`.
- Every task declares and justifies one `verification_strategy`; only observable behavior and defect correction require an initial failing test.
- Record durable findings in the task's `## Notes & Discoveries` and preserve unrelated worktree changes.
- Consumer operational skills install only under `.agents/skills/`. Product, knowledge, research, and specs remain under `.agents/bundles/` (`specs/` and `research/` are created lazily only when needed).
- Flow never creates, moves, force-updates, or deletes Git tags, and never pushes automatically. All changes in Vertebra are made on a dedicated feature/fix branch with signed conventional commits (`commit.gpgsign=true`) and submitted via pull request, never directly on `main`.
<!-- truth: end -->

## Canonical Polyglot Commands (`Makefile`)

[`tools/scripts/setup-env.sh`](../../../tools/scripts/setup-env.sh) (`make setup-env`, run automatically by `make install` and `make upgrade`) first deletes any existing `uv.toml` and `src/rs/.cargo/config.toml`. On internal corporate Linux workstations whose package mirrors replace the public registries (detected by an `/etc/os-release` match in the script), it then writes `uv.toml` (default index `https://pypi.org/simple`), appends `registry=https://registry.npmjs.org` to `.npmrc` and `src/ts/vertebra-client/.npmrc`, and writes `src/rs/.cargo/config.toml` (sparse crates.io protocol, `git-fetch-with-cli = true`). On other hosts and in CI it writes nothing. The `Makefile` applies the same detection and exports `NPM_CONFIG_REGISTRY`, `PIP_INDEX_URL`, `UV_INDEX_URL`, `UV_DEFAULT_INDEX`, `CARGO_REGISTRIES_CRATES_IO_PROTOCOL`, and `CARGO_NET_GIT_FETCH_WITH_CLI`.

```bash
# 1. Environment & Native PyO3 Extension Setup
make setup-env                                            # Configure public registries on hosts that need them; no-op elsewhere
make install                                              # destroy + clean + setup-env, then uv python pin 3.12, uv sync --all-extras --dev, ts-install, ts-build, dev
make dev                                                  # cargo build of non-extension crates + maturin develop for _vertebra, _bundler, _storage
make upgrade                                              # uv lock --upgrade, cargo update, bun update, pre-commit autoupdate
make clean                                                # Remove Python/Rust/TS build artifacts and caches (keeps .venv)

# 2. Focused Language Test Suites
make rs-test                                              # rm -rf /dev/shm/test-* /dev/shm/*vertebra*, then cargo test --workspace --features tls,compression
make py-test                                              # make dev, mypy.stubtest (_vertebra, _bundler, _storage), then uv run pytest src/py/tests -v
uv run pytest src/py/tests/unit/server/test_runner.py -q  # Focused Python test file (run make dev first if Rust changed)
make ts-test                                              # bun run test (vitest run) with an isolated --localstorage-file

# 3. Aggregate Test, Lint & Pre-PR Verification Gates
make test                                                 # rs-test + py-test + ts-test
make lint                                                 # Biome --write --unsafe, ruff (src/py/vertebra src/py/tests tools/scripts tools/pgo), cargo fmt + clippy --fix, mypy + stubtest, review-check
make lint-all                                             # Read-only CI gate: biome check, ruff check + format --check, mypy + stubtest, cargo fmt --check, clippy -D warnings, review-check
make review-check                                         # Validate review capability manifests, skills, agents, and privacy constraints
make review-route                                         # Route changed files to owning review areas and shards (BASE=origin/main)
make coverage                                             # uv run pytest src/py/tests --cov, then coverage.xml and htmlcov/ (Python only)
make check                                                # lint + test + coverage

# 4. Release Builds & Benchmarks (--group bench kept out of dev)
make build                                                # ts-build, release cargo build of non-extension crates, maturin develop -r for the three extensions, uv build
make build-pgo                                            # Two-phase Rust PGO instrumentation + profile-use build via tools/pgo/rust_pgo.py (or PROFDATA=<path>)
make bench                                                # make build, then tools.benchmarks run -t ${TARGETS:-vtb} -s ${SCENARIOS:-default} ${BENCH_ARGS}
make bench-all                                            # make build, then tools.benchmarks run -t all -s full
make bench-ft                                             # Free-threaded Python (FT_PYTHON=3.15t fallback 3.14t): sync .venv-ft, build src/rs/target-ft, run -t freethreaded -s ${SCENARIOS:-multiworker}
make rs-bench                                             # Run Rust Criterion microbenchmarks across core and http
make bench-micro                                          # Run Python microbenchmarks across hot-path primitives and warm import times
make bench-baseline                                       # Record baseline performance metrics (NAME=pre-refactor by default)
make bench-check                                          # Compare current benchmarks against baseline and enforce regression thresholds
make bench-report                                         # Latest benchmark report (make bench-diff compares the latest run with the previous one)
```

`make dev` and `make build` exclude `vertebra-py`, `vertebra-bundler`, and `vertebra-storage` from `cargo build --workspace` because `maturin develop` builds and installs them into `.venv`. `make build` installs the release extensions into `.venv` and then runs `uv build`; it does not produce separate extension wheels.

## Polyglot Rebuild & Hygiene Rules

1. **Rust to Python FFI Rebuild (`make dev`)**: `uv run pytest` loads the compiled `vertebra._vertebra`, `vertebra._bundler`, and `vertebra._storage` extensions from `.venv`. After any `.rs` change in `vertebra-core`, `vertebra-py`, `vertebra-bundler`, or `vertebra-storage`, run `make dev` (or `make py-test`) before `uv run pytest`. `vertebra-http` changes need `make dev` only to rebuild the `vertebra-server` binary.
2. **Codegen to TypeScript Sync**: When modifying route schemas or [`src/py/vertebra/build/codegen/`](../../../src/py/vertebra/build/codegen/__init__.py), run `make py-test` (`src/py/tests/unit/codegen/`) and `make ts-test`.
3. **Shared-Memory Cleanup**: `make rs-test` and the autouse `_cleanup_shm` fixture in [`src/py/tests/conftest.py`](../../../src/py/tests/conftest.py) remove `/dev/shm/test-*`, `/dev/shm/*vertebra*`, and `/dev/shm/vtb-*`.
4. **`--group bench` Isolation**: Keep `duckdb`, `uvicorn`, `httptools`, `uvloop`, `granian`, and `spikard` inside `[dependency-groups].bench` in [`pyproject.toml`](../../../pyproject.toml) so Python `3.14t`/`3.15t` test runs never compile `duckdb` from source.
5. **TypeScript Build**: `make ts-build` compiles all 13 subpath entrypoints (`index`, `react`, `vue`, `svelte`, `angular`, `htmx`, `vite`, `store`, `live`, `apex`, `telemetry`, `wasm`, `arrow`, plus `hmr`) and generates `.d.ts` declarations cleanly.

## Direct-Read Continuity

1. Resolve `.agents/setup-state.json:root_directory` (local-only, not tracked), falling back to `.agents/config.json` and then `.agents/`.
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

## Low-Signal Test & Gate Policy

Reject tests that lock incidental prompt phrases, private implementation shape, duplicate snapshots, or file existence without an operational contract. Prefer native parser, lint, type, and build contracts to source scanners when they express the complete rule. Retain tests for observable behavior, public contracts, proven regressions, error paths, interoperability, and operationally meaningful structure such as signatures, exports, hashing, memory layout, compilation, serialization, and isolation.

When replacing a gate, prove the replacement against an isolated violation before removing the old check. Confirm aggregate discovery includes new and untracked files where relevant.

## Commits & Checkpoints

Use `<type>(<scope>): <description>` with signed commits (`commit.gpgsign=true`) on a dedicated feature/fix branch and stage exact paths. Do not force-add ignored Flow artifacts. A phase checkpoint records affected task ids, the last functional commit, and fresh aggregate evidence; never create an empty checkpoint commit.

Flow may append supplementary Git notes under `refs/notes/flow` only after the canonical Markdown transaction succeeds. Notes are optional, stay local by default, and never become state authority. Git tags are prohibited as evidence or fallback transport.

## Knowledge Lifecycle

1. **Capture:** append dated discoveries to the owning task and reusable learnings to the flow's `learnings.md`.
2. **Synthesize:** integrate reusable current-state guidance into the best matching chapter anywhere under `.agents/bundles/knowledge/**/*.md`, preserving project-shaped nesting (`architecture/`, `domains/`, `patterns/`, `testing/`, `gotchas/`).
3. **Log:** history belongs in `.agents/bundles/log.md`, not in current-state knowledge prose.
4. **Contract:** after verified completion and archive review, delete the archived spec directory; Git history is the archive.

Operational project skills live only at `.agents/skills/`. Never create `.agents/bundles/skills/`.

## Quality Gates

Before close or checkpoint, require:

- worksheet acceptance criteria satisfied with fresh evidence;
- selected verification strategy followed, including isolated gate proof when required;
- focused and relevant aggregate commands green (`make lint-all`, `make rs-test`, `make py-test`, `make ts-test`);
- repository-defined lint, type, build, docs, coverage, security, and performance gates run when applicable;
- no accidental public API, typing, performance, import-boundary, or behavior change;
- no unrelated paths staged and `git diff --check` clean;
- discoveries and verification limitations recorded.

Coverage follows repository and worksheet requirements. Compare affected lines/branches when deleting behavioral tests; there is no universal percentage or one-test-file-per-module mandate.
