---
type: Task
id: "driver-fail-loud:ch13:t01"
chapter_id: ch13-agent-knowledge-and-embeddings-docs
title: State the GIL-only DuckDB dependency group in AGENTS.md, workflow, and benchmark knowledge
description: Update the AGENTS.md make install line, the workflow command block and hygiene rule, and the benchmark chapter's dependency-group section so they describe the dedicated duckdb group, its GIL-only Makefile wiring, and bench's inclusion of it.
status: stable
state: pending
priority: P2
verification_strategy: documentation_validation
depends_on: []
read_set:
  - AGENTS.md
  - .agents/bundles/knowledge/workflow.md
  - .agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md
  - Makefile
  - pyproject.toml
files:
  - AGENTS.md
  - .agents/bundles/knowledge/workflow.md
  - .agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md
tests: []
estimated_loc: 40
plan_revision: 1
plan_commit: 1e1f028a
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-08T00:00:00Z"
updated_at: "2026-11-08T00:00:00Z"
commit: null
sources:
  - id: agents-md
    resource: AGENTS.md
    title: Agent instructions Quick Verification Commands block
  - id: okf-workflow
    resource: .agents/bundles/knowledge/workflow.md
    title: OKF project workflow commands and hygiene rules
  - id: okf-benchmarks
    resource: .agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md
    title: OKF benchmark suite and free-threaded test gate chapter
  - id: makefile
    resource: Makefile
    title: install, py-test, coverage, and upgrade targets with PY_TEST_GROUPS
  - id: pyproject
    resource: pyproject.toml
    title: duckdb and bench dependency groups
tags:
  - docs
  - okf
  - agents
  - duckdb
---

# Task t01: DuckDB Group in Agent Instructions, Workflow, and Benchmark Knowledge

## Objective & Scope Boundary

Rewrite three agent-facing files so they describe the post-ch02 dependency layout: `duckdb>=1.0` lives in a dedicated `duckdb` group that `bench` includes and `dev` excludes; `make install` syncs it; `make py-test`, `make coverage`, and `make upgrade` add it only on GIL interpreters.[^agents-md][^okf-workflow][^okf-benchmarks][^makefile][^pyproject]

In scope: the `AGENTS.md` "Quick Verification Commands" `make install` line; the `workflow.md` "Canonical Polyglot Commands (`Makefile`)" block lines for `make install`, `make upgrade`, `make py-test`, and `make coverage`, the `# 4. Release Builds & Benchmarks` comment, and "Polyglot Rebuild & Hygiene Rules" rule 5; the `benchmarks-and-free-threaded.md` truth block bullet on benchmark-only packages and section "5. Isolating `--group bench` Dependencies (`pyproject.toml`)".

Out of scope: every other `AGENTS.md` line (agent-facing file with its own terse style); `tech-stack.md` and `polyglot-test-matrix.md` (t02); `Makefile` and `pyproject.toml` (read only; owned by ch02 t01); `make bench-ft` behavior.

## Bounded Read Manifest

- `AGENTS.md` — "Quick Verification Commands" fenced block, `make install` line.[^agents-md]
- `.agents/bundles/knowledge/workflow.md` — frontmatter `verified`/`sources`, "Canonical Polyglot Commands (`Makefile`)" fenced block, "Polyglot Rebuild & Hygiene Rules" rule 5.[^okf-workflow]
- `.agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md` — frontmatter, truth block, section 5.[^okf-benchmarks]
- `Makefile` — `PY_GIL_DISABLED`, `PY_TEST_GROUPS`, and the `install`, `upgrade`, `py-test`, `coverage` recipes.[^makefile]
- `pyproject.toml` — `[dependency-groups]` `dev`, `bench`, `duckdb`.[^pyproject]

## Symbol & Signature Contracts

Quote the Makefile and `pyproject.toml` as found; the post-ch02 values are:

```make
PY_GIL_DISABLED = $(shell uv run --no-sync python -c "import sysconfig; print(int(bool(sysconfig.get_config_var('Py_GIL_DISABLED'))))" 2>/dev/null)
PY_TEST_GROUPS = $(if $(filter 0,$(PY_GIL_DISABLED)),--group duckdb)
```

`PY_TEST_GROUPS` is `--group duckdb` only when the interpreter probe reports `Py_GIL_DISABLED=0` (GIL build); a free-threaded build or a failed probe adds no group.

| Target | Recipe line |
| --- | --- |
| `install` | `@uv sync --all-extras --dev --group doc --group duckdb` |
| `upgrade` | `@uv lock --upgrade && uv sync --all-extras --dev --group doc $(PY_TEST_GROUPS)` |
| `py-test` | `@uv run $(PY_TEST_GROUPS) pytest src/py/tests -v` |
| `coverage` | `@uv run $(PY_TEST_GROUPS) pytest src/py/tests --cov --quiet` |

`pyproject.toml`: `duckdb = ["duckdb>=1.0"]`; `bench` starts with `{ include-group = "duckdb" }`; `dev` includes `test`, `lint`, `build` only.

Target text (adapt wording; keep every fact):

- **`AGENTS.md` `make install` line**: `make install     # Fresh setup: destroy .venv + clean, setup-env, pin Python 3.12, uv sync --all-extras --dev --group doc --group duckdb, bun install/build, make dev`. Keep the column alignment of the block.
- **`workflow.md` command block**:
  - `make install` comment: `... uv python pin 3.12, uv sync --all-extras --dev --group doc --group duckdb, ts-install, ts-build, dev`.
  - `make upgrade` comment: `uv lock --upgrade + uv sync (--group duckdb on GIL interpreters), cargo update, bun update, pre-commit autoupdate`.
  - `make py-test` comment ends `then uv run $(PY_TEST_GROUPS) pytest src/py/tests -v (--group duckdb on GIL interpreters only)`.
  - `make coverage` comment starts `uv run $(PY_TEST_GROUPS) pytest src/py/tests --cov`.
  - `# 4. Release Builds & Benchmarks` comment keeps `--group bench kept out of dev`.
- **`workflow.md` rule 5** — heading `--group bench`, `--group duckdb` & `--group doc` Isolation; body: keep `duckdb>=1.0` in the dedicated `[dependency-groups].duckdb` group, which `bench` includes and `dev` does not; keep `uvicorn`, `httptools`, `uvloop`, `granian`, and `spikard` in `bench` and `zensical`, `mkdocstrings-python`, `pymdown-extensions` in `doc`; `make install` syncs `--group duckdb` and `make py-test`, `make coverage`, and `make upgrade` add it through `PY_TEST_GROUPS` only when the interpreter probe reports `Py_GIL_DISABLED=0` (GIL build); a free-threaded build or a failed probe adds no group. Reason line: DuckDB publishes only `cp312-abi3` wheels, which free-threaded `3.14t` / `3.15t` cannot load, and `zensical` would build Rust sdists on those interpreters. Cite `[^src-pyproject-toml]` and `[^src-makefile]`.
- **`benchmarks-and-free-threaded.md` truth bullet**: `duckdb>=1.0` lives in the GIL-only `[dependency-groups].duckdb` group that `bench` includes; `uvicorn`, `granian`, and `spikard` stay in `bench`; free-threaded (`3.14t` / `3.15t`) test runs install neither group.
- **`benchmarks-and-free-threaded.md` section 5**: heading becomes `5. Isolating `--group bench` and `--group duckdb` Dependencies (`pyproject.toml`)`; the opening sentence lists the `duckdb` results store as reaching `bench` through `{ include-group = "duckdb" }`; replace the `default-groups` bullet with: `pyproject.toml` sets no `[tool.uv] default-groups`; `make py-test` and `make coverage` run `uv run $(PY_TEST_GROUPS)`, so GIL interpreters sync `dev` plus `duckdb` and the CI `3.14t` / `3.15t` jobs sync `dev` only, where DuckDB tests skip through `pytest.importorskip("duckdb")`; keep the `make bench*` `--group bench` bullet.
- **Frontmatter of both OKF files**: prepend `{by: agent:flow-executor, at: <UTC now>, commit: <HEAD short sha>}` as the first `verified:` entry, keeping earlier entries. Both files already carry `src-makefile` and `src-pyproject-toml`; add no source unless a new file is cited.

## Edge Cases & Error Semantics

- If the `install` recipe is not `@uv sync --all-extras --dev --group doc --group duckdb` or `PY_TEST_GROUPS` is absent, stop and set `blocked_reason` (ch02 t01 incomplete).
- `AGENTS.md` has no frontmatter and no footnotes; change only the `make install` line.
- The `workflow.md` truth block lists target names only; leave it unchanged.
- `benchmarks-and-free-threaded.md` truth block stays `<= 40` lines; edit the existing bullet instead of adding one.
- Zero standalone `---` body rules; `status` unchanged; no `state:` key.

## Public-Seam Test Blueprint

Canonical gates: stale-claim grep, required-claim grep, OKF pairing check, relative-link check, `make review-check`.

| Case | File / Section | Expected Text / Claim |
| --- | --- | --- |
| AGENTS install | `AGENTS.md` `make install` line | Contains `uv sync --all-extras --dev --group doc --group duckdb` |
| AGENTS untouched elsewhere | `AGENTS.md` | `git diff --stat AGENTS.md` shows `1 insertion(+), 1 deletion(-)` |
| Workflow install | `workflow.md` command block | `make install` comment contains `--group doc --group duckdb` |
| Workflow test lines | `workflow.md` command block | `make py-test` and `make coverage` comments contain `PY_TEST_GROUPS` |
| Workflow rule | `workflow.md` rule 5 | Contains `[dependency-groups].duckdb` and `include`; `grep -c 'Keep \`duckdb\`, \`uvicorn\`'` = `0` |
| Benchmark truth | benchmarks truth block | Contains `[dependency-groups].duckdb`; no `(\`duckdb\`, \`uvicorn\`` run |
| Benchmark section 5 | benchmarks section 5 | Contains `include-group`, `PY_TEST_GROUPS`, and `importorskip` |
| Provenance | both OKF files | Pairing check prints `OK`; first `verified:` entry is `agent:flow-executor` |

## Verification

Baseline (expect matches before editing):

```bash
grep -nF 'PY_TEST_GROUPS = $(if $(filter 0,$(PY_GIL_DISABLED)),--group duckdb)' Makefile
grep -n 'uv sync --all-extras --dev --group doc --group duckdb' Makefile
grep -nE 'uv sync --all-extras --dev --group doc,|Keep `duckdb`, `uvicorn`|\(`duckdb`, `uvicorn`' AGENTS.md .agents/bundles/knowledge/workflow.md .agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md
```

The first two commands must match before editing (ch02 t01 complete); if either does not, stop and set `blocked_reason`.

Final (all must hold):

```bash
! grep -nE 'uv sync --all-extras --dev --group doc,|Keep `duckdb`, `uvicorn`|\(`duckdb`, `uvicorn`' AGENTS.md .agents/bundles/knowledge/workflow.md .agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md
grep -n 'uv sync --all-extras --dev --group doc --group duckdb' AGENTS.md .agents/bundles/knowledge/workflow.md
grep -n 'PY_TEST_GROUPS' .agents/bundles/knowledge/workflow.md .agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md
grep -n 'dependency-groups\].duckdb' .agents/bundles/knowledge/workflow.md .agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md
grep -n 'importorskip' .agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md
test "$(git diff --numstat AGENTS.md | cut -f1,2)" = "$(printf '1\t1')"
for f in .agents/bundles/knowledge/workflow.md .agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md; do sed -n '/^verified:/{n;p;q}' "$f" | grep -q 'agent:flow-executor' || echo "first verified entry is not agent:flow-executor: $f"; done | (! grep .)
uv run --no-sync python -I - .agents/bundles/knowledge/workflow.md .agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md <<'PY'
import pathlib, re, sys
import yaml
failed = False
for p in map(pathlib.Path, sys.argv[1:]):
    _, front, body = p.read_text().split("---\n", 2)
    srcs = yaml.safe_load(front)["sources"]
    ids = {s["id"] for s in srcs}
    defs = set(re.findall(r"(?m)^\[\^([^\]]+)\]:", body))
    refs = set(re.findall(r"\[\^([^\]]+)\]", re.sub(r"(?m)^\[\^[^\]]+\]:.*$", "", body)))
    truth = re.search(r"<!-- truth: start -->(.*?)<!-- truth: end -->", body, re.S)
    problems = {
        "sources_without_footnote": sorted(ids - defs),
        "footnotes_without_source": sorted(defs - ids),
        "sources_never_cited": sorted(ids - refs),
        "body_horizontal_rules": len(re.findall(r"(?m)^---\s*$", body)),
        "truth_block_missing_or_over_40": truth is None or len(truth.group(1).strip().splitlines()) > 40,
        "missing_resources": [s["resource"] for s in srcs if not pathlib.Path(s["resource"].split("::")[0]).exists()],
    }
    bad = {k: v for k, v in problems.items() if v}
    failed |= bool(bad)
    print(p, "OK" if not bad else bad)
sys.exit(1 if failed else 0)
PY
uv run --no-sync python -I - AGENTS.md .agents/bundles/knowledge/workflow.md .agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md <<'PY'
import pathlib, re, sys
bad = []
for p in map(pathlib.Path, sys.argv[1:]):
    for target in re.findall(r"\]\(([^)#\s]+)(?:#[^)]*)?\)", p.read_text()):
        if "://" in target or target.startswith("mailto:"):
            continue
        if not (p.parent / target).resolve().exists():
            bad.append(f"{p}: {target}")
print("\n".join(bad) or "all relative links resolve")
sys.exit(1 if bad else 0)
PY
make review-check
```

## Acceptance Criteria

- [ ] `AGENTS.md` `make install` line shows `--group duckdb`; no other `AGENTS.md` line changes (R18.1, R3).
- [ ] `workflow.md` command block and rule 5 describe the dedicated GIL-only `duckdb` group, `bench`'s inclusion, and `PY_TEST_GROUPS` (R18.1, R3).
- [ ] `benchmarks-and-free-threaded.md` truth block and section 5 describe the same layout and the free-threaded `importorskip` skip (R18.1, R3).
- [ ] Both OKF files keep 1:1 provenance with a prepended `agent:flow-executor` `verified:` entry.
- [ ] Every Verification command exits `0`.

## Notes & Discoveries

- [2026-11-08 00:00] Update `[k/11 files documented]` in the chapter spec and root spec when closing this task.

[^agents-md]: `AGENTS.md` — Agent instructions Quick Verification Commands block.
[^okf-workflow]: `.agents/bundles/knowledge/workflow.md` — OKF project workflow commands and hygiene rules.
[^okf-benchmarks]: `.agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md` — OKF benchmark suite and free-threaded test gate chapter.
[^makefile]: `Makefile` — install, py-test, coverage, and upgrade targets with PY_TEST_GROUPS.
[^pyproject]: `pyproject.toml` — duckdb and bench dependency groups.
