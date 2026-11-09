---
type: Task
id: "driver-fail-loud:ch13:t03"
chapter_id: ch13-agent-knowledge-and-embeddings-docs
title: State the DB-API InterfaceError and pending Oracle and SQL Server drivers in dialect knowledge, the domain index, and the docs home page
description: Rewrite the SQLSpec/SQLAlchemy dialect chapter's adapter and DB-API rows, the domain index driver and embeddings entries, and the docs home page database row so unparsable +vtb URLs raise InterfaceError, Oracle and MSSQL sessions raise MissingDependencyError, and no engine count is advertised.
status: stable
state: completed
priority: P2
verification_strategy: documentation_validation
depends_on: []
read_set:
  - .agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md
  - .agents/bundles/knowledge/domains/index.md
  - docs/index.md
  - src/py/vertebra/extensions/sqlalchemy/dialects.py
  - src/py/vertebra/extensions/sqlspec/adapters.py
  - src/py/vertebra/db/adapters/oracle/driver.py
files:
  - .agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md
  - .agents/bundles/knowledge/domains/index.md
  - docs/index.md
tests: []
estimated_loc: 30
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
  - id: okf-dialects
    resource: .agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md
    title: OKF SQLSpec, Advanced Alchemy, and SQLAlchemy dialect chapter
  - id: okf-domains-index
    resource: .agents/bundles/knowledge/domains/index.md
    title: OKF domain vocabulary index
  - id: docs-index
    resource: docs/index.md
    title: Documentation home page capability table
  - id: sqla-dialects
    resource: src/py/vertebra/extensions/sqlalchemy/dialects.py::SyncDBAPIConnection
    title: DB-API connection URL parsing and InterfaceError
  - id: sqlspec-adapters
    resource: src/py/vertebra/extensions/sqlspec/adapters.py::SyncAdapterConfig
    title: SQLSpec adapter driver construction
  - id: oracle-driver
    resource: src/py/vertebra/db/adapters/oracle/driver.py::SyncOracleDriver
    title: Oracle driver native-module guard
tags:
  - docs
  - okf
  - sqlalchemy
  - fail-loud
---

# Task t03: Dialect Knowledge, Domain Index, and Docs Home Page

## Objective & Scope Boundary

Rewrite three files so the dialect knowledge states the post-ch01/ch02 error contract and the domain index and home page stop presenting Oracle and SQL Server drivers as usable.[^okf-dialects][^okf-domains-index][^docs-index]

In scope: `sqlspec-alchemy-and-dialects.md` frontmatter, the `SyncAdapterConfig` / `AsyncAdapterConfig` row in "1. Plugins, Tier-1 Native Configs & Tier-2 Universal Broker Proxy", and the `SyncDBAPIModule` / `SyncDBAPIConnection` / `SyncDBAPICursor` row in "4. `+vtb` Dialects, Types, Alembic"; `domains/index.md` "Shared-Memory Vector Slabs…" entry under "Agent Protocols & Harness (`agent/`)" and "Database Drivers & Cloud Dialers" entry under "Storage, Data, Telemetry & Extensions"; `docs/index.md` "Databases & Analytics" capability row.

Out of scope: `docs/usage/**` and the OKF embeddings chapter (t04); OKF drivers chapter (ch11 t03); every other domain-index entry; other `docs/index.md` rows.

## Bounded Read Manifest

- `.agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md` — frontmatter, sections 1 and 4, footnotes.[^okf-dialects]
- `.agents/bundles/knowledge/domains/index.md` — the two entries above (frontmatter-free reserved index).[^okf-domains-index]
- `docs/index.md` — capability table row "Databases & Analytics".[^docs-index]
- `src/py/vertebra/extensions/sqlalchemy/dialects.py::SyncDBAPIConnection.__init__`, `::AsyncDBAPIConnection.__init__`, `::InterfaceError` — post-ch02 parse block and message.[^sqla-dialects]
- `src/py/vertebra/extensions/sqlspec/adapters.py::SyncAdapterConfig.create_driver`, `::OracleSyncDriver` — session driver construction path.[^sqlspec-adapters]
- `src/py/vertebra/db/adapters/oracle/driver.py::_require_oracle_native` — guard and error.[^oracle-driver]

## Symbol & Signature Contracts

Post-ch02 parse block in both DB-API connections (quote the message as found):

```python
except ValueError as exc:
    msg = f"Cannot parse Vertebra database URL: {exc}"
    raise InterfaceError(msg) from exc
```

Target text (adapt wording; keep every fact):

- **Dialect chapter, `SyncAdapterConfig` row**: keep the sentence that `OracleSyncDriver` / `OracleAsyncDriver` build `OracleCore.build_session_context_sql` and return `predicate.to_sql_fragment(":tenant_id")`, and add that Oracle and MSSQL configs construct while `provide_session()` raises `MissingDependencyError` at driver construction because the native Oracle and SQL Server driver packages are not yet published, so the Oracle tenant path executes in no session today. Cite `[^src-oracle-driver-py]`.
- **Dialect chapter, `SyncDBAPIModule` row**: replace the sentence "Non-supported URLs raise `UnsupportedBackendError`" (and its link to `vertebra/typing.py`) with: when `DatabaseRegistry.parse_url` raises `ValueError`, `SyncDBAPIConnection` / `AsyncDBAPIConnection` raise the module's PEP 249 `InterfaceError` (`Cannot parse Vertebra database URL: …`) chained from it, with no SQLite substitution; `oracle+vtb` / `mssql+vtb` connections raise `MissingDependencyError`; `duckdb+vtb` requires the `duckdb` package. Remove the `typing.py` link.
- **Dialect chapter frontmatter**: prepend `{by: agent:flow-executor, at: <UTC now>, commit: <HEAD short sha>}` as the first `verified:` entry, keeping earlier entries; add source `src-oracle-driver-py` (`src/py/vertebra/db/adapters/oracle/driver.py::SyncOracleDriver`, "Oracle driver native-module guard") with a matching `[^src-oracle-driver-py]:` footnote in the existing `` `resource` — title. `` format.
- **Domain index, embeddings entry**: after `OracleEmbeddingPushdown` add `(requires the pending native Oracle driver)`.
- **Domain index, drivers entry**: the driver list reads `` `SyncPostgreSQLDriver`, `AsyncPostgreSQLDriver`, MySQL, SQLite, DuckDB (requires `duckdb`), BigQuery, Spanner, Redis, and Valkey drivers, Oracle and MSSQL drivers (native drivers pending; construction raises `MissingDependencyError`) ``, followed by the unchanged `ArrowCursor` … `DatabaseRegistry`, `UnsupportedBackendError` symbols. `UnsupportedBackendError` stays: `src/py/vertebra/_typing.py::UnsupportedBackendError` exists at `d2b168dc` and no chapter removes it.
- **docs/index.md "Databases & Analytics" row**: replace "Nine native database drivers" with "PostgreSQL, MySQL, SQLite, DuckDB, BigQuery, Spanner, Redis, and Valkey drivers (native Oracle and SQL Server drivers are not yet published)"; keep the rest of the cell.

## Edge Cases & Error Semantics

- If `SyncDBAPIConnection.__init__` lacks the `InterfaceError` URL-parse guard (ch02 t02 incomplete), stop and set `blocked_reason`.
- `domains/index.md` is a reserved subdirectory index: no frontmatter, no footnotes; edit list text only.
- `docs/index.md`: no `vertebra-core[oracle]` / `[mssql]` command, no internal identifiers, no exclamation marks.
- Dialect chapter truth block stays unchanged (it states pool sharing and entry-point counts only).
- Zero standalone `---` body rules; `status` unchanged; no `state:` key.

## Public-Seam Test Blueprint

Canonical gates: stale-claim grep, leak grep, planned-extra grep, OKF pairing check, relative-link check, `make docs`, `make review-check`.

| Case | File / Section | Expected Text / Claim |
| --- | --- | --- |
| No UnsupportedBackendError claim | dialect chapter section 4 | `grep -c 'Non-supported URLs raise'` = `0`; row contains `InterfaceError` |
| Oracle sessions | dialect chapter section 1 | Row contains `MissingDependencyError` next to Oracle and MSSQL |
| DuckDB dialect | dialect chapter section 4 | Row contains `duckdb+vtb` and the `duckdb` package |
| Domain index embeddings | `domains/index.md` | `OracleEmbeddingPushdown` followed by the pending-driver note |
| Domain index drivers | `domains/index.md` | Drivers entry contains `MissingDependencyError` |
| Home page | `docs/index.md` | `grep -ci 'nine native'` = `0`; contains `not yet published` |
| No leaks | `docs/index.md` | Leak grep and planned-extra grep return no match |
| Provenance | dialect chapter | Pairing check prints `OK`; first `verified:` entry is `agent:flow-executor` |

## Verification

Baseline (expect matches before editing):

```bash
grep -n 'raise InterfaceError(msg) from exc' src/py/vertebra/extensions/sqlalchemy/dialects.py
grep -nE 'Non-supported URLs raise|Nine native database drivers' .agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md docs/index.md
```

The first command must match before editing (ch02 t02 complete); if it does not, stop and set `blocked_reason`.

Final (all must hold):

```bash
! grep -niE 'Non-supported URLs raise|nine native database drivers' .agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md docs/index.md
grep -n 'InterfaceError' .agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md
grep -n 'MissingDependencyError' .agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md .agents/bundles/knowledge/domains/index.md
grep -n 'OracleEmbeddingPushdown` (requires the pending native Oracle driver)' .agents/bundles/knowledge/domains/index.md
grep -n 'not yet published' docs/index.md
! grep -rnE 'native-[a-z-]+-drivers?|pending_flow_id|driver work|driver-fail-loud|ch[0-9]{2}' docs/index.md
! grep -nE 'vertebra-core\[(oracle|mssql)\]' docs/index.md
! head -1 .agents/bundles/knowledge/domains/index.md | grep -q '^---$'
sed -n '/^verified:/{n;p;q}' .agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md | grep 'agent:flow-executor'
uv run --no-sync python -I - .agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md <<'PY'
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
uv run --no-sync python -I - .agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md .agents/bundles/knowledge/domains/index.md docs/index.md <<'PY'
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

- [x] The dialect chapter states the DB-API `InterfaceError` for unparsable URLs, Oracle/MSSQL `MissingDependencyError`, and the `duckdb` requirement for `duckdb+vtb`, with no `UnsupportedBackendError` claim (R18.3, R1, R3, R4).
- [x] The domain index marks Oracle and MSSQL drivers and `OracleEmbeddingPushdown` as pending and stays frontmatter-free (R18.3).
- [x] `docs/index.md` lists engines without a count and marks native Oracle and SQL Server drivers as not yet published (R18.3, R13).
- [x] The dialect chapter keeps 1:1 provenance with a prepended `agent:flow-executor` `verified:` entry.
- [x] Every Verification command exits `0`.

## Notes & Discoveries

- [2026-11-08 00:00] At `d2b168dc` `src/py/vertebra/extensions/sqlalchemy/dialects.py` contained no `UnsupportedBackendError`; the dialect chapter replaces that stale claim with `InterfaceError`.
- [2026-11-08 00:00] Update `[k/11 files documented]` in the chapter spec and root spec when closing this task.

[^okf-dialects]: `.agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md` — OKF SQLSpec, Advanced Alchemy, and SQLAlchemy dialect chapter.
[^okf-domains-index]: `.agents/bundles/knowledge/domains/index.md` — OKF domain vocabulary index.
[^docs-index]: `docs/index.md` — Documentation home page capability table.
[^sqla-dialects]: `src/py/vertebra/extensions/sqlalchemy/dialects.py::SyncDBAPIConnection` — DB-API connection URL parsing and InterfaceError.
[^sqlspec-adapters]: `src/py/vertebra/extensions/sqlspec/adapters.py::SyncAdapterConfig` — SQLSpec adapter driver construction.
[^oracle-driver]: `src/py/vertebra/db/adapters/oracle/driver.py::SyncOracleDriver` — Oracle driver native-module guard.
