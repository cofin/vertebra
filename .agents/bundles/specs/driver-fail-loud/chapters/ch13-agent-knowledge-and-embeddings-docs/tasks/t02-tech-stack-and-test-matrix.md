---
type: Task
id: "driver-fail-loud:ch13:t02"
chapter_id: ch13-agent-knowledge-and-embeddings-docs
title: State vertebra-db dependencies, the DuckDB group, the importorskip rule, and resp3_server in the tech stack and test matrix
description: Drop sha2 and rmp-serde from the tech-stack vertebra-db bullet, list the duckdb dependency group and the install command, and add the GIL-only DuckDB group, the pytest.importorskip("duckdb") rule, and the resp3_server loopback fixture to the polyglot test matrix outside its conformance section.
status: stable
state: pending
priority: P2
verification_strategy: documentation_validation
depends_on: []
read_set:
  - .agents/bundles/product/tech-stack.md
  - .agents/bundles/knowledge/testing/polyglot-test-matrix.md
  - pyproject.toml
  - src/rs/vertebra-db/Cargo.toml
  - src/py/tests/unit/conftest.py
  - src/py/tests/unit/db/test_enterprise_olap_drivers.py
files:
  - .agents/bundles/product/tech-stack.md
  - .agents/bundles/knowledge/testing/polyglot-test-matrix.md
tests: []
estimated_loc: 45
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
  - id: okf-tech-stack
    resource: .agents/bundles/product/tech-stack.md
    title: Product tech stack crate dependencies and dependency groups
  - id: okf-test-matrix
    resource: .agents/bundles/knowledge/testing/polyglot-test-matrix.md
    title: OKF polyglot test matrix
  - id: pyproject
    resource: pyproject.toml
    title: duckdb and bench dependency groups
  - id: db-cargo
    resource: src/rs/vertebra-db/Cargo.toml
    title: vertebra-db crate dependencies
  - id: unit-conftest
    resource: src/py/tests/unit/conftest.py::resp3_server
    title: Loopback RESP3 server fixture for Redis and Valkey unit tests
  - id: duckdb-tests
    resource: src/py/tests/unit/db/test_enterprise_olap_drivers.py
    title: DuckDB tests guarded by pytest.importorskip
tags:
  - okf
  - testing
  - duckdb
  - redis
---

# Task t02: Tech Stack and Test Matrix

## Objective & Scope Boundary

Rewrite two OKF files so the tech stack lists the post-ch07 `vertebra-db` dependencies and the post-ch02 dependency groups, and the test matrix describes the GIL-only DuckDB group, the DuckDB test guard, and the `resp3_server` fixture.[^okf-tech-stack][^okf-test-matrix]

In scope: `tech-stack.md` "Rust Workspace Crates (`src/rs/Cargo.toml`)" `vertebra-db` dependency bullet, "Python Package Configuration (`pyproject.toml`)" "Optional Extras" bullet (install command clause) and "Dependency Groups" bullets; `polyglot-test-matrix.md` "2. Test & Lint Targets" `make py-test` and `make coverage` rows, "3. CI Matrix" `test-python` row, "4. Suite Inventory & Test Quality Standards" Python bullets, and "Driver regression boundaries".

Out of scope: the `polyglot-test-matrix.md` "Database driver conformance" section, which ch11 t03 owns and this task leaves byte-identical; other `tech-stack.md` crates; `pyproject.toml`, `Cargo.toml`, and test files (read only).

## Bounded Read Manifest

- `.agents/bundles/product/tech-stack.md` — frontmatter, "Rust Workspace Crates" `vertebra-db` bullet, "Python Package Configuration" bullets.[^okf-tech-stack]
- `.agents/bundles/knowledge/testing/polyglot-test-matrix.md` — frontmatter, sections 2, 3, 4, "Driver regression boundaries", footnotes.[^okf-test-matrix]
- `pyproject.toml` — `[dependency-groups]` `dev`, `bench`, `duckdb`.[^pyproject]
- `src/rs/vertebra-db/Cargo.toml` — `[dependencies]`.[^db-cargo]
- `src/py/tests/unit/conftest.py` — `Resp3FakeServer` and `resp3_server`.[^unit-conftest]
- `src/py/tests/unit/db/test_enterprise_olap_drivers.py` — `_DUCKDB_SKIP_REASON` and its `pytest.importorskip("duckdb", ...)` calls.[^duckdb-tests]

## Symbol & Signature Contracts

Quote the files as found; the post-ch02/ch03/ch07 values are:

- `src/rs/vertebra-db/Cargo.toml` has no `sha2` or `rmp-serde` dependency line; `rsa = { version = "0.9", features = ["sha2"] }` stays.
- `pyproject.toml`: `duckdb = ["duckdb>=1.0"]`; `bench = [{ include-group = "duckdb" }, "uvicorn[standard]>=0.30", "httptools>=0.6", "uvloop>=0.23.0; sys_platform != 'win32'", "granian>=1.6", "spikard>=0.12"]`.
- `_DUCKDB_SKIP_REASON = "duckdb publishes no free-threaded wheel; DuckDB tests run on GIL interpreters"`.
- `class Resp3FakeServer` with `port: int`, `redis_url: str`, `valkey_url: str`, `commands: list[list[bytes]]`, `close() -> None`; `@pytest.fixture def resp3_server() -> Iterator[Resp3FakeServer]`.

Target text (adapt wording; keep every fact):

- **tech-stack `vertebra-db` bullet**: remove `sha2 0.10` and `rmp-serde 1.3` from the dependency list; keep every other entry.
- **tech-stack "Optional Extras" bullet** tail: `make install` syncs all extras plus the `doc` and `duckdb` groups (`uv sync --all-extras --dev --group doc --group duckdb`).
- **tech-stack "Dependency Groups"**: add `duckdb`: separate GIL-only group (`duckdb>=1.0`), synced by `make install` and added by `make py-test`, `make coverage`, and `make upgrade` through `PY_TEST_GROUPS = $(if $(filter 0,$(PY_GIL_DISABLED)),--group duckdb)` only when the interpreter probe reports `Py_GIL_DISABLED=0` (GIL build), so a free-threaded build or a failed probe adds no group, because DuckDB publishes only `cp312-abi3` wheels; `bench` reads `separate group ({ include-group = "duckdb" }, uvicorn[standard]>=0.30, …)`; `dev` stays `test`, `lint`, `build`.
- **test-matrix section 2**: `make py-test` row command reads `make dev` + stubtest + `uv run $(PY_TEST_GROUPS) pytest src/py/tests -v`; `make coverage` row reads `uv run $(PY_TEST_GROUPS) pytest src/py/tests --cov --quiet` + `coverage xml` + `coverage html`; add one sentence after the table: `PY_TEST_GROUPS` is `--group duckdb` only when the interpreter probe reports `Py_GIL_DISABLED=0` (GIL build); a free-threaded build (`3.14t` / `3.15t`) or a failed probe adds no group.
- **test-matrix section 3 `test-python` row**: append that the `3.14t` and `3.15t` jobs install no `duckdb`, so DuckDB tests skip there.
- **test-matrix section 4 Python bullets**: add a bullet "**DuckDB Test Guard**": tests that construct `SyncDuckDBDriver` / `AsyncDuckDBDriver` or open a `duckdb+vtb` connection call `pytest.importorskip("duckdb", reason=_DUCKDB_SKIP_REASON)` first; parametrized tests call it inside the DuckDB branch only so other engines still run on free-threaded interpreters; config-only DuckDB tests carry no guard. Cite `[^src-tests-unit-db-enterprise-olap-py]`.
- **test-matrix "Driver regression boundaries"**: add two sentences: Redis, Valkey, and store unit tests run against the function-scoped `resp3_server` fixture in `src/py/tests/unit/conftest.py`, a threaded loopback `Resp3FakeServer` exposing `redis_url`, `valkey_url`, and recorded `commands`, which exercises the real RESP3 wire path of `SyncRedisDriver` and `AsyncRedisDriver`; no unit test builds a `memory://` Redis or Valkey config. Cite `[^src-tests-unit-conftest-py]`.
- **Frontmatter of both files**: prepend `{by: agent:flow-executor, at: <UTC now>, commit: <HEAD short sha>}` as the first `verified:` entry, keeping earlier entries. Add to `polyglot-test-matrix.md` sources `src-tests-unit-conftest-py` (`src/py/tests/unit/conftest.py::resp3_server`, "Loopback RESP3 server fixture for Redis and Valkey unit tests") and `src-tests-unit-db-enterprise-olap-py` (`src/py/tests/unit/db/test_enterprise_olap_drivers.py`, "DuckDB driver tests guarded by pytest.importorskip"), each with a matching `[^id]:` footnote. `tech-stack.md` already cites `src-vertebra-db-cargo-toml` and `src-pyproject-toml`.

## Edge Cases & Error Semantics

- If `src/py/tests/unit/conftest.py` does not exist or lacks `resp3_server`, stop and set `blocked_reason` (ch03 t02 incomplete).
- If `src/rs/vertebra-db/Cargo.toml` still lists `sha2` or `rmp-serde` as a dependency, stop and report a ch07 contradiction.
- Do not touch the "Database driver conformance" section; the Verification diff check enforces it.
- `tech-stack.md` truth block stays `<= 40` lines; no truth edit is required.
- Zero standalone `---` body rules; `status` unchanged; no `state:` key.

## Public-Seam Test Blueprint

Canonical gates: stale-claim grep, required-claim grep, section-isolation check, OKF pairing check, relative-link check, `make review-check`.

| Case | File / Section | Expected Text / Claim |
| --- | --- | --- |
| vertebra-db deps | tech-stack `vertebra-db` bullet | Bullet contains neither `sha2 0.10` nor `rmp-serde` |
| Other crates keep sha2 | tech-stack `vertebra-core` / `vertebra-storage` bullets | Still contain `sha2 0.10` |
| Groups | tech-stack Dependency Groups | Contains `` `duckdb`: `` and `include-group = "duckdb"` |
| Install | tech-stack Optional Extras | Contains `--group doc --group duckdb` |
| Test targets | test-matrix section 2 | `make py-test` and `make coverage` rows contain `PY_TEST_GROUPS` |
| CI free-threaded | test-matrix section 3 | `test-python` row names `3.14t`, `3.15t`, and `duckdb` skip |
| Guard rule | test-matrix section 4 | Contains `pytest.importorskip("duckdb"` and `_DUCKDB_SKIP_REASON` |
| Fixture | test-matrix "Driver regression boundaries" | Contains `resp3_server` and `src/py/tests/unit/conftest.py` |
| Section isolation | test-matrix "Database driver conformance" | Text identical before and after this task |
| Provenance | both files | Pairing check prints `OK`; first `verified:` entry is `agent:flow-executor`; in `polyglot-test-matrix.md` its `commit:` equals `git rev-parse --short HEAD`, which distinguishes it from the entry ch11 t03 prepends |

## Verification

Baseline (expect matches before editing; capture the conformance section):

```bash
grep -n 'def resp3_server' src/py/tests/unit/conftest.py
! grep -nE '^(sha2|rmp-serde)\b' src/rs/vertebra-db/Cargo.toml
grep -n 'include-group = "duckdb"' pyproject.toml
grep -nE 'rmp-serde 1\.3|`bench`: separate group \(`duckdb' .agents/bundles/product/tech-stack.md
before="$(mktemp)"
sed -n '/^## Database driver conformance/,/^## Driver regression boundaries/p' .agents/bundles/knowledge/testing/polyglot-test-matrix.md > "$before"
```

The first three commands must succeed before editing; if any does not, stop and set `blocked_reason`. Run Baseline and Final in one shell session so `$before` still names the snapshot.

Final (all must hold):

```bash
! grep -n 'rmp-serde' .agents/bundles/product/tech-stack.md
! grep -n '`vertebra-db`: `rlib`' .agents/bundles/product/tech-stack.md | grep 'sha2'
test "$(grep -c 'sha2 0.10' .agents/bundles/product/tech-stack.md)" -ge 2
grep -n 'include-group = "duckdb"' .agents/bundles/product/tech-stack.md
grep -n -- '--group doc --group duckdb' .agents/bundles/product/tech-stack.md
grep -n 'PY_TEST_GROUPS' .agents/bundles/knowledge/testing/polyglot-test-matrix.md
grep -n 'pytest.importorskip("duckdb"' .agents/bundles/knowledge/testing/polyglot-test-matrix.md
grep -n 'resp3_server' .agents/bundles/knowledge/testing/polyglot-test-matrix.md
sed -n '/^## Database driver conformance/,/^## Driver regression boundaries/p' .agents/bundles/knowledge/testing/polyglot-test-matrix.md | diff "$before" -
for f in .agents/bundles/product/tech-stack.md .agents/bundles/knowledge/testing/polyglot-test-matrix.md; do sed -n '/^verified:/{n;p;q}' "$f" | grep -q 'agent:flow-executor' || echo "first verified entry is not agent:flow-executor: $f"; done | (! grep .)
uv run --no-sync python -I - "$(git rev-parse --short HEAD)" .agents/bundles/knowledge/testing/polyglot-test-matrix.md <<'PY'
import pathlib, sys
import yaml
head, path = sys.argv[1], pathlib.Path(sys.argv[2])
first = yaml.safe_load(path.read_text().split("---\n", 2)[1])["verified"][0]
ok = first.get("by") == "agent:flow-executor" and str(first.get("commit")) == head
print(path, "OK" if ok else f"first verified entry {first} does not carry commit {head}")
sys.exit(0 if ok else 1)
PY
uv run --no-sync python -I - .agents/bundles/product/tech-stack.md .agents/bundles/knowledge/testing/polyglot-test-matrix.md <<'PY'
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
uv run --no-sync python -I - .agents/bundles/product/tech-stack.md .agents/bundles/knowledge/testing/polyglot-test-matrix.md <<'PY'
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

- [ ] `tech-stack.md` lists no `sha2` or `rmp-serde` for `vertebra-db`, keeps `sha2` for `vertebra-core` and `vertebra-storage`, and lists the `duckdb` group, `bench`'s inclusion, and the install command (R18.2, R3, R8).
- [ ] `polyglot-test-matrix.md` sections 2–4 and "Driver regression boundaries" describe `PY_TEST_GROUPS`, free-threaded DuckDB skips, the `importorskip` rule, and `resp3_server` (R18.2, R3, R7).
- [ ] The "Database driver conformance" section is unchanged by this task.
- [ ] Both files keep 1:1 provenance with a prepended `agent:flow-executor` `verified:` entry.
- [ ] Every Verification command exits `0`.

## Notes & Discoveries

- [2026-11-08 00:00] ch11 t03 rewrites the "Database driver conformance" section of `polyglot-test-matrix.md` and prepends its own `verified:` entry first; this task prepends another above it.
- [2026-11-08 00:00] Update `[k/11 files documented]` in the chapter spec and root spec when closing this task.

[^okf-tech-stack]: `.agents/bundles/product/tech-stack.md` — Product tech stack crate dependencies and dependency groups.
[^okf-test-matrix]: `.agents/bundles/knowledge/testing/polyglot-test-matrix.md` — OKF polyglot test matrix.
[^pyproject]: `pyproject.toml` — duckdb and bench dependency groups.
[^db-cargo]: `src/rs/vertebra-db/Cargo.toml` — vertebra-db crate dependencies.
[^unit-conftest]: `src/py/tests/unit/conftest.py::resp3_server` — Loopback RESP3 server fixture for Redis and Valkey unit tests.
[^duckdb-tests]: `src/py/tests/unit/db/test_enterprise_olap_drivers.py` — DuckDB tests guarded by pytest.importorskip.
