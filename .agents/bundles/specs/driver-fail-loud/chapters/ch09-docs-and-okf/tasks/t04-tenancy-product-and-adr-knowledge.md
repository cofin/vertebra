---
type: Task
id: "driver-fail-loud:ch09:t04"
chapter_id: ch09-docs-and-okf
title: Remove the SQLite stand-in, TNS/TDS framing, and deleted vertebra-db exports from tenancy, product, and ADR-002 chapters
description: Rewrite the OKF tenancy chapter Oracle paragraph, the product overview data-access bullet, tenant paragraph, and vertebra-db component row, and the ADR-002 consequences in present tense with matching provenance.
status: draft
state: pending
priority: P2
verification_strategy: documentation_validation
depends_on:
  - "driver-fail-loud:ch09:t03"
read_set:
  - .agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md
  - .agents/bundles/product/product.md
  - .agents/bundles/knowledge/decisions/adr-002-two-tier-shm-database-and-arrow-data-plane.md
  - src/rs/vertebra-db/src/lib.rs
  - src/py/vertebra/db/adapters/oracle/core.py
  - src/py/vertebra/db/adapters/oracle/driver.py
files:
  - .agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md
  - .agents/bundles/product/product.md
  - .agents/bundles/knowledge/decisions/adr-002-two-tier-shm-database-and-arrow-data-plane.md
tests: []
estimated_loc: 50
plan_revision: 1
plan_commit: 0acdf576
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-07T00:00:00Z"
updated_at: "2026-11-08T00:00:00Z"
commit: null
sources:
  - id: okf-tenancy
    resource: .agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md
    title: OKF query pipeline, CDC, and tenancy chapter
  - id: okf-product
    resource: .agents/bundles/product/product.md
    title: Product overview
  - id: okf-adr-002
    resource: .agents/bundles/knowledge/decisions/adr-002-two-tier-shm-database-and-arrow-data-plane.md
    title: ADR-002 shared-memory database broker and Arrow data plane
  - id: db-lib
    resource: src/rs/vertebra-db/src/lib.rs
    title: vertebra-db public re-exports
  - id: oracle-core
    resource: src/py/vertebra/db/adapters/oracle/core.py::OracleCore
    title: Oracle session context SQL builder
  - id: oracle-driver
    resource: src/py/vertebra/db/adapters/oracle/driver.py::SyncOracleDriver
    title: Oracle driver native-module guard
tags:
  - okf
  - product
  - database
  - fail-loud
---

# Task t04: Tenancy, Product, and ADR-002 Knowledge

## Objective & Scope Boundary

Rewrite three OKF files so they carry no SQLite stand-in, Thin TNS/TTC, or TDS framing claims and list only `vertebra-db` exports present after ch06, with provenance intact.[^okf-tenancy][^okf-product][^okf-adr-002]

In scope: `query-pipeline-cdc-and-tenancy.md` paragraph starting "The Python Oracle adapter records context values"; `product.md` "Data Access (`vertebra.db`, `vertebra-db`)" bullet, tenant paragraph starting "Tenant configuration is engine-neutral", and the "Rust Database & Wire Engine" component-table row; ADR-002 "Consequences" first bullet.

Out of scope: files owned by t01–t03; ADR-002 "Context", "Decision", and options table; `product.md` sections unrelated to data access.

## Bounded Read Manifest

- `.agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md` — tenancy section around `OracleCore` and the stand-in paragraph; frontmatter `verified`/`sources`.[^okf-tenancy]
- `.agents/bundles/product/product.md` — Data Access bullet, tenant paragraph, component table, frontmatter.[^okf-product]
- `.agents/bundles/knowledge/decisions/adr-002-two-tier-shm-database-and-arrow-data-plane.md` — "Consequences", frontmatter.[^okf-adr-002]
- `src/rs/vertebra-db/src/lib.rs` — post-ch06 `pub use` lists for the component row.[^db-lib]
- `src/py/vertebra/db/adapters/oracle/core.py::OracleCore.build_session_context_sql` — SQL and binds.[^oracle-core]
- `src/py/vertebra/db/adapters/oracle/driver.py::_require_oracle_native` — guard behavior.[^oracle-driver]

## Symbol & Signature Contracts

- **Tenancy paragraph**: replace "The Python Oracle adapter records context values in its in-process SQLite stand-in." with: Oracle drivers raise `MissingDependencyError` until `vertebra._oracle` is installed, so `OracleCore.build_session_context_sql` output is verified as SQL text and binds only. Keep the trusted-package / `CREATE CONTEXT` sentence and the Oracle reference link. Add source `src-oracle-driver` (`src/py/vertebra/db/adapters/oracle/driver.py::SyncOracleDriver`) with footnote and citation.
- **product.md Data Access bullet**: Oracle reads `Oracle (native driver pending; Autonomous Database wallet/tnsnames.ora parsing + 26ai VECTOR/OSON codecs)`; SQL Server reads `SQL Server / MSSQL (native driver pending; @P1 parameter rewriting)`; DuckDB reads `DuckDB (requires duckdb; single-writer SHM broker host with Arrow IPC)`; Redis/Valkey text unchanged.
- **product.md tenant paragraph**: replace "Oracle context support currently has helper/SQLite-stand-in acceptance; trusted-package provisioning, session reset, and live Oracle acceptance belong to native Oracle driver work." with: Oracle context SQL is builder-verified; Oracle drivers raise `MissingDependencyError`, so no Vertebra session executes it.
- **product.md component row "Rust Database & Wire Engine"**: rebuild the symbol list from post-ch06 `lib.rs`. Remove `COM_STMT_EXECUTE`/`encode_com_stmt_prepare`, `SqliteConfig`/`SqlitePool` becomes `SqliteConfig`, `OracleWalletMaterial::from_directory`, `DuckdbConfig`/`DuckdbBrokerPool` becomes `DuckdbConfig`. Keep `CloudSqlInstanceUri`, `AlloyDbClusterUri`, `PoolerMode`/`compute_worker_pool_budget`, PGCOPY codecs, `parse_tnsnames_ora`, `OracleConnectDescriptor`, `encode_oracle_vector_{f32,f64,i8,binary}`, `rewrite_mssql_placeholders`, `parse_resp3_frame`, `crc16_xmodem`/`redis_hash_slot`, `ShmArrowFrameHeader`/`ShmArrowStreamWriter` only when `lib.rs` still exports them or they are reachable `pub` items in kept modules.
- **ADR-002 Consequences bullet**: replace "Oracle Thin TNS/TTC + Wallet, TDS `@P1` rewriting" with "Oracle wallet/`tnsnames.ora` parsing and 26ai `VECTOR`/OSON codecs, SQL Server `@P1` rewriting"; replace "non-GCP Python driver adapters execute through Python client bindings or the shared-memory broker proxy" with a statement that PostgreSQL, MySQL, SQLite, DuckDB, Redis, and Valkey adapters execute through Python client bindings or the broker proxy, while Oracle and SQL Server drivers raise `MissingDependencyError` until their native modules ship.
- **Frontmatter on all three files**: prepend `{by: agent:flow-executor, at: <UTC now>, commit: <HEAD short sha>}` as the first `verified:` entry, keeping earlier entries; every added `sources` id gets a footnote and a citation.

## Edge Cases & Error Semantics

- If a symbol named in the product row is not in `lib.rs` and not a `pub` item of a kept module, remove it; never add a symbol absent from code.
- `product.md` truth block is 26 lines today; edits stay `<= 40`.
- `product.md` and ADR-002 relative links use `../../../src/...` and `../../../../src/...` depths respectively; keep them resolving.
- Zero standalone `---` body rules; `status` unchanged; no `state:` key.

## Public-Seam Test Blueprint

Canonical gates: OKF pairing check, stale-claim grep, relative-link check, `make review-check`.

Every stale-claim check in this task uses one pattern, `T04_STALE`, defined once in Verification and used verbatim by the Baseline and Final commands and by the rows below:

```bash
T04_STALE='stand-in|Thin TNS/TTC|\bTDS\b|SqlitePool|DuckdbBrokerPool|from_directory|COM_STMT_EXECUTE|encode_com_stmt_prepare'
```

| Case | File / Section | Expected Text / Claim |
| --- | --- | --- |
| No stale claims | tenancy chapter, product.md, ADR-002 | `grep -cE "$T04_STALE"` = `0` for each file |
| Oracle tenant status | tenancy chapter | Contains `MissingDependencyError` and `build_session_context_sql` |
| Data Access status | product.md Data Access bullet | Contains `native driver pending` twice and `requires duckdb` |
| Kept exports | product.md component row | Contains `rewrite_mssql_placeholders`, `encode_oracle_vector`, `parse_resp3_frame` |
| ADR framing | ADR-002 Consequences | `grep -cE "$T04_STALE"` = `0`; contains `@P1` and `MissingDependencyError` |
| Provenance | all three files | OKF pairing check prints `OK` for each |

## Verification

Baseline (expect matches before editing):

```bash
T04_STALE='stand-in|Thin TNS/TTC|\bTDS\b|SqlitePool|DuckdbBrokerPool|from_directory|COM_STMT_EXECUTE|encode_com_stmt_prepare'
grep -nE "$T04_STALE" .agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md .agents/bundles/product/product.md .agents/bundles/knowledge/decisions/adr-002-two-tier-shm-database-and-arrow-data-plane.md
```

Final (all must hold):

```bash
T04_STALE='stand-in|Thin TNS/TTC|\bTDS\b|SqlitePool|DuckdbBrokerPool|from_directory|COM_STMT_EXECUTE|encode_com_stmt_prepare'
! grep -nE "$T04_STALE" .agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md .agents/bundles/product/product.md .agents/bundles/knowledge/decisions/adr-002-two-tier-shm-database-and-arrow-data-plane.md
for f in .agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md .agents/bundles/product/product.md .agents/bundles/knowledge/decisions/adr-002-two-tier-shm-database-and-arrow-data-plane.md; do sed -n '/^verified:/{n;p;q}' "$f" | grep -q 'agent:flow-executor' || echo "first verified entry is not agent:flow-executor: $f"; done | (! grep .)
uv run --no-sync python -I - .agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md .agents/bundles/product/product.md .agents/bundles/knowledge/decisions/adr-002-two-tier-shm-database-and-arrow-data-plane.md <<'PY'
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
uv run --no-sync python -I - .agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md .agents/bundles/product/product.md .agents/bundles/knowledge/decisions/adr-002-two-tier-shm-database-and-arrow-data-plane.md <<'PY'
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

- [ ] Tenancy chapter and product.md describe Oracle context as builder-verified with drivers raising `MissingDependencyError` (R1, R13).
- [ ] product.md Data Access bullet and component row match post-ch06 `lib.rs` and state Oracle/SQL Server/DuckDB status (R3, R8, R13).
- [ ] ADR-002 Consequences carry no Thin TNS/TTC or TDS framing claim (R8, R13).
- [ ] Every Verification command exits `0`.

## Notes & Discoveries

- [2026-11-07 00:00] Update `[k/11 files documented]` in the chapter spec and root spec when closing this task.

[^okf-tenancy]: `.agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md` — OKF query pipeline, CDC, and tenancy chapter.
[^okf-product]: `.agents/bundles/product/product.md` — Product overview.
[^okf-adr-002]: `.agents/bundles/knowledge/decisions/adr-002-two-tier-shm-database-and-arrow-data-plane.md` — ADR-002 shared-memory database broker and Arrow data plane.
[^db-lib]: `src/rs/vertebra-db/src/lib.rs` — vertebra-db public re-exports.
[^oracle-core]: `src/py/vertebra/db/adapters/oracle/core.py::OracleCore` — Oracle session context SQL builder.
[^oracle-driver]: `src/py/vertebra/db/adapters/oracle/driver.py::SyncOracleDriver` — Oracle driver native-module guard.
