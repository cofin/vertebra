---
type: Task
id: "driver-fail-loud:ch13:t04"
chapter_id: ch13-agent-knowledge-and-embeddings-docs
title: State the Oracle embedding pushdown requirement in the embeddings guide, usage index, and OKF embeddings chapter
description: Rewrite the embeddings guide selection matrix and pushdown section, the usage index embeddings row, and the OKF embeddings truth block and embedder table so OracleEmbeddingPushdown reads as requiring the pending native Oracle driver, whose construction raises MissingDependencyError.
status: stable
state: completed
priority: P2
verification_strategy: documentation_validation
depends_on: []
read_set:
  - docs/usage/embeddings-and-vectors.md
  - docs/usage/index.md
  - .agents/bundles/knowledge/domains/agent/embeddings-and-vectors.md
  - src/py/vertebra/db/adapters/oracle/embeddings.py
  - src/py/vertebra/db/adapters/oracle/driver.py
  - docs/usage/databases.md
files:
  - docs/usage/embeddings-and-vectors.md
  - docs/usage/index.md
  - .agents/bundles/knowledge/domains/agent/embeddings-and-vectors.md
tests: []
estimated_loc: 25
plan_revision: 2
plan_commit: c2b09b9b
state_revision: 1
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: "2026-11-09T15:25:00Z"
last_verified_commit: 5c95a733
verification_evidence: "grepok, okf provenance ok, relative-links ok, make docs ok, review_setup validate ok, pytest test_docs_tooling.py 28 passed"
created_at: "2026-11-08T00:00:00Z"
updated_at: "2026-11-09T15:25:00Z"
commit: 5c95a733
sources:
  - id: docs-embeddings
    resource: docs/usage/embeddings-and-vectors.md
    title: Vector embeddings and SQL pushdown usage guide
  - id: docs-usage-index
    resource: docs/usage/index.md
    title: Usage guide landing page
  - id: okf-embeddings
    resource: .agents/bundles/knowledge/domains/agent/embeddings-and-vectors.md
    title: OKF vector slabs, embeddings, and SQL pushdown chapter
  - id: oracle-embeddings
    resource: src/py/vertebra/db/adapters/oracle/embeddings.py::OracleEmbeddingPushdown
    title: Oracle SQL embedding pushdown over an Oracle driver
  - id: oracle-driver
    resource: src/py/vertebra/db/adapters/oracle/driver.py::SyncOracleDriver
    title: Oracle driver native-module guard
  - id: docs-databases
    resource: docs/usage/databases.md
    title: Databases guide phrasing for pending Oracle and SQL Server drivers
tags:
  - docs
  - okf
  - embeddings
  - oracle
  - fail-loud
---

# Task t04: Oracle Embedding Pushdown Docs

## Objective & Scope Boundary

Rewrite three files so `OracleEmbeddingPushdown` reads as requiring an Oracle driver that the native Oracle driver package, not yet published, would provide; constructing `SyncOracleDriver` or `AsyncOracleDriver` raises `MissingDependencyError`.[^docs-embeddings][^docs-usage-index][^okf-embeddings][^oracle-embeddings][^oracle-driver]

In scope: `docs/usage/embeddings-and-vectors.md` "Embedder Selection Matrix" `PushdownEmbedder` row and the "Google Cloud & In-Database SQL Pushdown" paragraph that lists the pushdown classes; the `docs/usage/index.md` "Vector Embeddings & Slabs" row; the OKF embeddings chapter frontmatter, truth bullet "Engine-Owned Pushdown", and the `OracleEmbeddingPushdown` row of "4. Embedder Hierarchy (`vertebra.embeddings`)".

Out of scope: the Oracle SQL-builder facts (`DBMS_VECTOR.UTL_TO_EMBEDDINGS`, `VECTOR_ARRAY_T`, 128-byte URL guard, credential rotation), which describe shipped code and stay; the mermaid diagram (`Oracle 26ai` there names a `VECTOR` byte format that `to_oracle_bytes` encodes); `docs/usage/databases.md` (read only, owned by ch11 t01); every other row of `docs/usage/index.md`.

## Bounded Read Manifest

- `docs/usage/embeddings-and-vectors.md` — "Embedder Selection Matrix", "Google Cloud & In-Database SQL Pushdown".[^docs-embeddings]
- `docs/usage/index.md` — "Vector Embeddings & Slabs" table row.[^docs-usage-index]
- `.agents/bundles/knowledge/domains/agent/embeddings-and-vectors.md` — frontmatter, truth block, section 4 table, footnotes.[^okf-embeddings]
- `src/py/vertebra/db/adapters/oracle/embeddings.py::OracleEmbeddingPushdown.__init__` — `driver: AsyncOracleDriver | SyncOracleDriver` parameter.[^oracle-embeddings]
- `src/py/vertebra/db/adapters/oracle/driver.py::_require_oracle_native` — guard and error.[^oracle-driver]
- `docs/usage/databases.md` — the post-ch11 Oracle wording ("not yet published", `MissingDependencyError`) to match.[^docs-databases]

## Symbol & Signature Contracts

`OracleEmbeddingPushdown.__init__(self, driver: AsyncOracleDriver | SyncOracleDriver, model: str = "gemini-embedding-001", *, dimensions: int = 768, ...)` is unchanged; the class builds SQL against the driver it receives and constructs no driver itself.

Target text (adapt wording; keep every fact):

- **Guide matrix `PushdownEmbedder` row, "Extra Required" cell**: `Driver extra for BigQuery/Spanner; Oracle needs the native Oracle driver, which is not yet published`.
- **Guide pushdown paragraph**: directly after the sentence listing `BigQueryEmbeddingPushdown`, `SpannerEmbeddingPushdown`, `PostgresEmbeddingPushdown`, and `OracleEmbeddingPushdown`, and before the Python code block, add: `OracleEmbeddingPushdown` takes a `SyncOracleDriver` or `AsyncOracleDriver`. The native Oracle driver package is not yet published, so constructing either driver raises `MissingDependencyError`. Leave the code block and the Oracle SQL paragraph after it unchanged.
- **Usage index row**: `... PushdownEmbedder with BigQuery, Spanner, PostgreSQL/AlloyDB, and Oracle EmbeddingPushdown (Oracle requires the native Oracle driver, not yet published), ...`.
- **OKF truth bullet "Engine-Owned Pushdown"**: append one sentence: `OracleEmbeddingPushdown` needs an Oracle driver, and `SyncOracleDriver` / `AsyncOracleDriver` raise `MissingDependencyError` until the native Oracle driver ships, so the Oracle path is verified as SQL text and binds only. Cite `[^src-adapters-oracle-driver-py]`. Keep the bullet count.
- **OKF section 4 `OracleEmbeddingPushdown` row**: append `Requires an Oracle driver; Oracle drivers raise MissingDependencyError until the native Oracle driver ships.`
- **OKF frontmatter**: prepend `{by: agent:flow-executor, at: <UTC now>, commit: <HEAD short sha>}` as the first `verified:` entry, keeping earlier entries; add source `src-adapters-oracle-driver-py` (`src/py/vertebra/db/adapters/oracle/driver.py::SyncOracleDriver`, "Oracle driver native-module guard") with a matching footnote and add it to the "Provenance sources:" line.

## Edge Cases & Error Semantics

- If `oracle/driver.py` has no `_require_oracle_native` or the guard raises something other than `MissingDependencyError`, stop and report a ch01 contradiction.
- User docs never quote the guard's `uv add vertebra-core[oracle]` hint; it names a planned extra that is not installable.
- No new fenced code blocks; the existing PostgreSQL example stays byte-identical.
- OKF truth block stays `<= 40` lines; zero standalone `---` body rules; `status` unchanged; no `state:` key.
- No internal identifiers, `Previously` / `now` sediment, or exclamation marks in `docs/**`.

## Public-Seam Test Blueprint

Canonical gates: required-claim grep, leak grep, planned-extra grep, OKF pairing check, relative-link check, `make docs`, `make review-check`, `src/py/tests/unit/tools/test_docs_tooling.py`.

| Case | File / Section | Expected Text / Claim |
| --- | --- | --- |
| Matrix | guide `PushdownEmbedder` row | Contains `native Oracle driver` and `not yet published` |
| Pushdown paragraph | guide pushdown section | Contains `SyncOracleDriver`, `AsyncOracleDriver`, and `MissingDependencyError` before the first code block of the section |
| Kept SQL facts | guide Oracle paragraph | Still contains `DBMS_VECTOR.UTL_TO_EMBEDDINGS` and `128-byte` |
| Kept example | guide code block | `PostgresEmbeddingPushdown(driver, dimensions=768)` present |
| Usage index | `docs/usage/index.md` embeddings row | Contains `not yet published` |
| OKF truth | embeddings truth block | Contains `MissingDependencyError` next to `OracleEmbeddingPushdown` |
| OKF table | embeddings section 4 | `OracleEmbeddingPushdown` row contains `MissingDependencyError` |
| No leaks | both `docs/**` files | Leak grep and planned-extra grep return no match |
| Provenance | OKF chapter | Pairing check prints `OK`; first `verified:` entry is `agent:flow-executor` |

## Verification

Baseline (expect matches before editing):

```bash
grep -n '_require_oracle_native' src/py/vertebra/db/adapters/oracle/driver.py
grep -n 'not yet published' docs/usage/databases.md
grep -n 'OracleEmbeddingPushdown' docs/usage/embeddings-and-vectors.md docs/usage/index.md .agents/bundles/knowledge/domains/agent/embeddings-and-vectors.md
```

The first two commands must match before editing (ch01 and ch11 t01 complete); if either does not, stop and set `blocked_reason`.

Final (all must hold):

```bash
grep -n 'MissingDependencyError' docs/usage/embeddings-and-vectors.md .agents/bundles/knowledge/domains/agent/embeddings-and-vectors.md
grep -n 'not yet published' docs/usage/embeddings-and-vectors.md docs/usage/index.md
grep -n 'DBMS_VECTOR.UTL_TO_EMBEDDINGS' docs/usage/embeddings-and-vectors.md
grep -n 'PostgresEmbeddingPushdown(driver, dimensions=768)' docs/usage/embeddings-and-vectors.md
test "$(grep -c 'MissingDependencyError' .agents/bundles/knowledge/domains/agent/embeddings-and-vectors.md)" -ge 2
! grep -rnE 'native-[a-z-]+-drivers?|pending_flow_id|driver work|driver-fail-loud|ch[0-9]{2}' docs/usage/embeddings-and-vectors.md docs/usage/index.md
! grep -nE 'vertebra-core\[(oracle|mssql)\]' docs/usage/embeddings-and-vectors.md docs/usage/index.md
sed -n '/^verified:/{n;p;q}' .agents/bundles/knowledge/domains/agent/embeddings-and-vectors.md | grep 'agent:flow-executor'
uv run --no-sync python -I - .agents/bundles/knowledge/domains/agent/embeddings-and-vectors.md <<'PY'
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
uv run --no-sync python -I - docs/usage/embeddings-and-vectors.md docs/usage/index.md .agents/bundles/knowledge/domains/agent/embeddings-and-vectors.md <<'PY'
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
make docs
make review-check
uv run --no-sync pytest src/py/tests/unit/tools/test_docs_tooling.py -q
```

## Acceptance Criteria

- [x] The embeddings guide matrix and pushdown section state that `OracleEmbeddingPushdown` needs the native Oracle driver, which is not yet published, and that Oracle drivers raise `MissingDependencyError`, without quoting an install command (R18.4, R1, R13).
- [x] `docs/usage/index.md` embeddings row marks the Oracle pushdown as requiring the unpublished native driver (R18.4).
- [x] The OKF embeddings truth bullet and section 4 row state the requirement with 1:1 provenance and a prepended `agent:flow-executor` `verified:` entry (R18.4).
- [x] Every Verification command exits `0`.

## Notes & Discoveries

- [2026-11-08 00:00] At `0acdf576` `OracleEmbeddingPushdown.__init__` takes `driver: AsyncOracleDriver | SyncOracleDriver` and constructs no driver.
- [2026-11-08 00:00] Update `[k/11 files documented]` in the chapter spec and root spec when closing this task.

[^docs-embeddings]: `docs/usage/embeddings-and-vectors.md` — Vector embeddings and SQL pushdown usage guide.
[^docs-usage-index]: `docs/usage/index.md` — Usage guide landing page.
[^okf-embeddings]: `.agents/bundles/knowledge/domains/agent/embeddings-and-vectors.md` — OKF vector slabs, embeddings, and SQL pushdown chapter.
[^oracle-embeddings]: `src/py/vertebra/db/adapters/oracle/embeddings.py::OracleEmbeddingPushdown` — Oracle SQL embedding pushdown over an Oracle driver.
[^oracle-driver]: `src/py/vertebra/db/adapters/oracle/driver.py::SyncOracleDriver` — Oracle driver native-module guard.
[^docs-databases]: `docs/usage/databases.md` — Databases guide phrasing for pending Oracle and SQL Server drivers.
