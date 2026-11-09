---
type: Task
id: "driver-fail-loud:ch11:t03"
chapter_id: ch11-docs-and-okf
title: Sync the OKF drivers chapter and test matrix with adapter status, codecs, install hints, and conformance outcomes
description: Rewrite database-drivers-and-dialers.md and the polyglot-test-matrix conformance section in present tense with matching sources, footnotes, and refreshed verified entries.
status: stable
state: pending
priority: P2
verification_strategy: documentation_validation
depends_on: []
read_set:
  - .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
  - .agents/bundles/knowledge/testing/polyglot-test-matrix.md
  - src/rs/vertebra-db/src/lib.rs
  - src/py/vertebra/_typing.py
  - src/py/tests/integration/db/test_driver_conformance.py
  - src/py/tests/unit/build/test_packaging_and_pgo.py
files:
  - .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
  - .agents/bundles/knowledge/testing/polyglot-test-matrix.md
tests: []
estimated_loc: 130
plan_revision: 2
plan_commit: fd571e3a
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
  - id: okf-drivers
    resource: .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
    title: OKF database drivers and dialers chapter
  - id: okf-test-matrix
    resource: .agents/bundles/knowledge/testing/polyglot-test-matrix.md
    title: OKF polyglot test matrix
  - id: db-lib
    resource: src/rs/vertebra-db/src/lib.rs
    title: vertebra-db public re-exports
  - id: missing-dep
    resource: src/py/vertebra/_typing.py::MissingDependencyError
    title: Missing dependency error and install hint
  - id: conformance
    resource: src/py/tests/integration/db/test_driver_conformance.py::_resolve_live_config
    title: Conformance live config resolution
  - id: packaging-test
    resource: src/py/tests/unit/build/test_packaging_and_pgo.py
    title: Packaging extras gate
  - id: conformance-cache
    resource: src/py/tests/integration/db/test_driver_conformance.py::_resolve_cache_config
    title: Conformance Redis and Valkey live-only resolution
  - id: oracle-wallet
    resource: src/py/vertebra/db/adapters/oracle/config.py::OracleWallet
    title: Python Autonomous Database wallet parser
tags:
  - okf
  - database
  - conformance
  - fail-loud
---

# Task t03: OKF Drivers Chapter and Test Matrix

## Objective & Scope Boundary

Rewrite `database-drivers-and-dialers.md` and the "Database driver conformance" section of `polyglot-test-matrix.md` so agents read the post-ch09 adapter status, `vertebra-db` codec inventory, install-hint contract, extras gate, and conformance outcomes, with OKF v0.2 provenance intact.[^okf-drivers][^okf-test-matrix]

Out of scope: user docs (t01, t02); tenancy, product, and ADR chapters (t04); BigQuery/Spanner chapter; any section of `polyglot-test-matrix.md` other than "Database driver conformance" (the DuckDB dependency-group, `pytest.importorskip("duckdb")`, and `resp3_server` additions to its "Test & Lint Targets", "CI Matrix", and "Driver regression boundaries" sections belong to ch13-agent-knowledge-and-embeddings-docs t02, which runs after this chapter).

## Bounded Read Manifest

- `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` — frontmatter, truth block, "Polymorphic Registry & Driver Contracts", "Relational, Analytical & Cache Adapters", "Database Driver Conformance".[^okf-drivers]
- `.agents/bundles/knowledge/testing/polyglot-test-matrix.md` — frontmatter `verified`/`sources`, "Database driver conformance".[^okf-test-matrix]
- `src/rs/vertebra-db/src/lib.rs` — post-ch07 `pub mod` and `pub use` lists; the only source for the codec inventory.[^db-lib]
- `src/py/vertebra/_typing.py::MissingDependencyError` — post-ch08 default `install_package`.[^missing-dep]
- `src/py/tests/integration/db/test_driver_conformance.py::_resolve_live_config` and `::_resolve_cache_config` — post-ch09 skip/fail logic, the Redis/Valkey live-only rule, env vars.[^conformance][^conformance-cache]
- `src/py/tests/unit/build/test_packaging_and_pgo.py` — the extras-gate test name and what it scans.[^packaging-test]

Fact outside the read set, verified at `0acdf576` and unchanged by ch01–ch09: `src/py/vertebra/db/adapters/oracle/config.py::OracleWallet` parses Autonomous Database wallets (`from_zip_bytes`, `from_path`, `resolve_alias`); after ch07 deletes the `OracleWalletMaterial` struct with `from_directory`, Rust `src/rs/vertebra-db/src/oracle/tns.rs` keeps `parse_tnsnames_ora`, `resolve_tns_alias`, and `OracleConnectDescriptor` only.[^oracle-wallet]

## Symbol & Signature Contracts

`database-drivers-and-dialers.md`:

- **Truth block** (stay `<= 40` lines): add a bullet "Fail-loud engine availability": Oracle and MSSQL drivers resolve `vertebra._oracle` / `vertebra._mssql` through `ensure_module` and raise `MissingDependencyError`; DuckDB requires `duckdb`, and DuckDB publishes no free-threaded wheel, so `duckdb` is not installed on `3.14t` / `3.15t` and DuckDB drivers raise `MissingDependencyError` there; no non-SQLite adapter opens `sqlite3`; PostgreSQL, MySQL, Redis, and Valkey drivers raise `ImproperConfigurationError` for `memory://` while their configs construct. Add a bullet "Install hints": `MissingDependencyError` defaults `install_package` to `vertebra-core[<extra>]`, the packaging gate rejects undeclared extras, and the Oracle and MSSQL guards name the planned, unpublished `vertebra-core[oracle]` / `vertebra-core[mssql]` extras. Rewrite the "Binary Vector Wire Codecs" bullet to say `vertebra-db` holds codecs, parsers, and dialers only, with no `*Pool` lease types.
- **Registry bullet**: scheme list reads `redis`/`rediss`, with `memory` removed.
- **Adapter table**: add the status to each "Wire & Runtime Behavior" cell and derive Rust claims from `lib.rs`:
  - PostgreSQL: keep v3 frame encoders, `PGCOPY`, `pgvector`, `PoolerMode`, `compute_worker_pool_budget`; drop `PostgresPool` if mentioned; add "rejects `memory://`".
  - MySQL: remove the `mysql/mod.rs` link and handshake/scramble/`COM_STMT_*` codec claims; keep Python-config claims (`:3306` IAM token auth, `to_cloud_dialer_config()` `:3307`, summed `execute_many`); state the clients `pymysql` (sync) and `asyncmy` (async); add "rejects `memory://`".
  - SQLite: keep `SqliteConfig` pragmas (WAL, `synchronous`, `busy_timeout`, `mmap_size`); drop `SqlitePool`.
  - Oracle: "Native driver pending; drivers raise `MissingDependencyError`"; attribute Autonomous Database wallet parsing to Python `OracleWallet` (`oracle/config.py`, cite `[^src-oracle-config]`) and keep `oracle/tns.rs` for `tnsnames.ora` and connect-descriptor parsing only; keep 26ai `VECTOR`, OSON, `OracleSessionAttrs`; drop "Thin TNS/TTC framing", `O5LOGON`, wallet zip parsing in `tns.rs`, TNS packet codecs.
  - MSSQL: "Native driver pending; drivers raise `MissingDependencyError`"; keep `@P1` placeholder rewriting through Rust `vertebra_db::rewrite_mssql_placeholders`, its Python binding `rewrite_mssql_placeholders`, and `MSSQLConfig.rewrite_placeholders`; drop `PRELOGIN`, `LOGIN7`, `SQL_BATCH`, `RPC_REQUEST`, TDS packet codecs.
  - DuckDB: "Requires `duckdb`; DuckDB publishes no free-threaded wheel, so `duckdb` is not installed on `3.14t` / `3.15t` and DuckDB drivers raise `MissingDependencyError` there"; keep `DuckdbConfig` bootstrap pragmas and `vtb_gcs_adc` rotation; drop `DuckdbBrokerPool`.
  - Redis/Valkey: keep RESP3 parsing, `CLIENT TRACKING`, Sentinel, Cluster CRC16; add "reject `memory://`".
- **New subsection "Missing Dependencies & Install Hints"** after "Shared Connector, Async Drainer & Arrow Cursor": `ensure_module(module_name, *, package=None, extra=None, install_package=None)` raises `MissingDependencyError(package or module_name, install_package=..., extra=...)`; default hint `vertebra-core[<extra>]`; the packaging test fails when a `src/py/vertebra` `extra=` literal names an extra missing from `[project.optional-dependencies]`. Cite `[^src-missing-dependency]` and `[^src-packaging-extras-gate]`.
- **"Database Driver Conformance" paragraph**: transports list reads `psycopg`/`asyncpg`, `pymysql`/`asyncmy`, `sqlite3`, `duckdb`, and RESP3 streams (drop `aiomysql`, `oracledb`, and `pymssql`) and states Oracle/MSSQL have no transport until their native modules ship; add the conformance outcome sentence (native module resolves before credential skips; a missing native module skips; `VERTEBRA_CONFORMANCE_DOCKER=1` fails the case only for engines without `pending_flow_id` (`bigquery`, `spanner`); pending engines skip in both modes).
- **Frontmatter**: add sources `src-oracle-config` (`src/py/vertebra/db/adapters/oracle/config.py::OracleWallet`), `src-missing-dependency` (`src/py/vertebra/_typing.py::MissingDependencyError`), `src-module-loader` (`src/py/vertebra/utils/module_loader.py::ensure_module`), `src-packaging-extras-gate` (`src/py/tests/unit/build/test_packaging_and_pgo.py`), `src-oracle-driver` (`src/py/vertebra/db/adapters/oracle/driver.py::SyncOracleDriver`), `src-mssql-driver` (`src/py/vertebra/db/adapters/mssql/driver.py::SyncMSSQLDriver`), `src-duckdb-driver` (`src/py/vertebra/db/adapters/duckdb/driver.py::SyncDuckDBDriver`), `src-redis-config` (`src/py/vertebra/db/adapters/redis/config.py::RedisConfig`), each with a matching `[^id]:` footnote and at least one body citation. Update the `src-vertebra-db-src-oracle-rs` title to drop "Thin TNS/TTC framing". Prepend a `verified:` entry `{by: agent:flow-executor, at: <UTC now>, commit: <HEAD short sha>}` above the existing entries.

`polyglot-test-matrix.md` "Database driver conformance":

- Replace the strict-`xfail` sentence with: the native module resolves before credential skips; a spec whose `native_extension_module` is missing skips; under `VERTEBRA_CONFORMANCE_DOCKER=1` a missing module fails a non-pending spec (`bigquery`, `spanner`) and still skips a pending spec; strict `xfail` (`Owned by <pending_flow_id>`) applies only when a pending engine's native module imports.
- Replace "Redis and Valkey default to `memory://` (`VERTEBRA_CONFORMANCE_DOCKER=1` selects `pytest-databases` fixtures)" with the `_resolve_cache_config` rule: Redis and Valkey cases use `VERTEBRA_CONFORMANCE_<ENGINE>_DSN` (`VERTEBRA_CONFORMANCE_REDIS_DSN`, `VERTEBRA_CONFORMANCE_VALKEY_DSN`) or, under `VERTEBRA_CONFORMANCE_DOCKER=1`, the `pytest-databases` fixtures, and skip otherwise.
- Replace "Report passed, skipped, and expected-failure cases separately" with "Report passed, skipped, and expected-failure (pending engine with its native module present) cases separately".
- Update the `src-db-conformance` title if needed and prepend a `verified:` entry `{by: agent:flow-executor, at: <UTC now>, commit: <HEAD short sha>}` above the existing entries.

## Edge Cases & Error Semantics

- If `lib.rs` still exports any `*Pool`, `mysql`, TDS, or TNS packet symbol, stop: ch05, ch06, or ch07 is incomplete; report instead of documenting it.
- If `_build_live_engine_params` marks a pending engine `xfail` while its native module is missing, or `_require_native_module` fails a pending engine under `VERTEBRA_CONFORMANCE_DOCKER=1`, stop and report a ch09 contradiction.
- Removing the `mysql/mod.rs` link must not leave a dangling footnote; the MySQL row has no footnote today.
- Keep frontmatter `type: Guide` and `status: stable`; never add `state:`.
- Zero standalone `---` body rules; truth block `<= 40` lines.

## Public-Seam Test Blueprint

Canonical gates: OKF pairing check, stale-claim grep, relative-link check, `make review-check`.

| Case | File / Section | Expected Text / Claim |
| --- | --- | --- |
| Scheme list | drivers chapter registry bullet | `grep -c 'rediss\`/\`memory'` = `0` |
| Deleted exports | drivers chapter | `grep -cE 'DuckdbBrokerPool\|SqlitePool\|PostgresPool\|MysqlPool\|OraclePool\|MssqlPool\|mysql/mod.rs'` = `0` |
| No framing | drivers chapter | `grep -cE 'Thin TNS/TTC\|TDS 7.4\|PRELOGIN\|LOGIN7\|O5LOGON'` = `0` |
| Kept codecs | drivers chapter Oracle/MSSQL/Redis rows | Contains `VECTOR`, `OSON`, `tnsnames`, `@P1`, `RESP3` |
| Wallet attribution | drivers chapter Oracle row | Contains `OracleWallet` linked to `oracle/config.py`; the `tns.rs` clause names only `tnsnames.ora` and connect-descriptor parsing |
| Status | drivers chapter Oracle/MSSQL rows | Each contains `MissingDependencyError` |
| DuckDB | drivers chapter DuckDB row | Contains `duckdb` requirement, `3.14t`, and `3.15t` |
| Transports | drivers chapter conformance paragraph | Contains `asyncmy`; `grep -c aiomysql` = `0` |
| Install hints | drivers chapter new subsection | Contains `vertebra-core[<extra>]` and the packaging test path |
| Conformance | test matrix section | `grep -c 'default to \`memory://\`'` = `0`; contains `VERTEBRA_CONFORMANCE_DOCKER=1`, `VERTEBRA_CONFORMANCE_<ENGINE>_DSN`, `skip`, `pending_flow_id`, and `passed, skipped, and expected-failure`; every `xfail` mention is qualified by "native module imports" |
| Provenance order | both files | first `verified:` entry is `agent:flow-executor` |
| Provenance | both files | OKF pairing check prints `OK` for both |

## Verification

Baseline (expect matches before editing):

```bash
grep -nE 'Thin TNS/TTC|TDS 7\.4|O5LOGON|mysql/mod\.rs|rediss`/`memory|pymssql|strict=True|xfail' .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md .agents/bundles/knowledge/testing/polyglot-test-matrix.md
```

Final (all must hold):

```bash
! grep -nE 'Thin TNS/TTC|TDS 7\.4|PRELOGIN|LOGIN7|O5LOGON|mysql/mod\.rs|rediss`/`memory|pymssql|oracledb|aiomysql|DuckdbBrokerPool|SqlitePool|PostgresPool|MysqlPool|OraclePool|MssqlPool|stand-in|currently `bigquery`' .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
! grep -n 'rewrite_sql_to_tds_params' .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
grep -n 'OracleWallet' .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
grep -n 'asyncmy' .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
grep -nE '3\.14t' .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
! grep -n 'default to `memory://`' .agents/bundles/knowledge/testing/polyglot-test-matrix.md
grep -n 'pending_flow_id' .agents/bundles/knowledge/testing/polyglot-test-matrix.md
grep -nE 'VERTEBRA_CONFORMANCE_(<ENGINE>|REDIS)_DSN' .agents/bundles/knowledge/testing/polyglot-test-matrix.md
grep -n 'passed, skipped, and expected-failure' .agents/bundles/knowledge/testing/polyglot-test-matrix.md
sed -n '/^verified:/{n;p;q}' .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md | grep 'agent:flow-executor'
sed -n '/^verified:/{n;p;q}' .agents/bundles/knowledge/testing/polyglot-test-matrix.md | grep 'agent:flow-executor'
! grep -n 'xfail' .agents/bundles/knowledge/testing/polyglot-test-matrix.md | grep -v 'native module imports'
uv run --no-sync python -I - .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md .agents/bundles/knowledge/testing/polyglot-test-matrix.md <<'PY'
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
uv run --no-sync python -I - .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md .agents/bundles/knowledge/testing/polyglot-test-matrix.md <<'PY'
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

- [ ] Adapter table states Oracle/MSSQL pending status, the DuckDB requirement and free-threaded caveat, and `memory://` rejection (R1, R3, R6, R7, R13).
- [ ] Wallet parsing is attributed to Python `OracleWallet`, `tns.rs` to `tnsnames.ora` / descriptor parsing, and async MySQL to `asyncmy` (R8, R13).
- [ ] Codec inventory matches post-ch07 `lib.rs`: kept `tnsnames.ora` parsing, `VECTOR`, `pgvector`, OSON, session attrs, `rewrite_mssql_placeholders` `@P1` rewriter, SQLite pragmas, DuckDB bootstrap, RESP3; no `*Pool`, `OracleWalletMaterial` struct, MySQL module, or TDS/TNS framing (R8, R9).
- [ ] Install-hint contract and extras gate are documented with provenance (R10, R11).
- [ ] Conformance outcomes (skip on a missing module, Docker-mode failure only for non-pending engines, strict `xfail` only when a pending module imports, Redis/Valkey live-only through `_resolve_cache_config`) replace the unconditional strict `xfail` and `memory://` defaults in both files, and reporting keeps passed, skipped, and expected-failure cases separate (R7, R12).
- [ ] Every Verification command exits `0`.

## Notes & Discoveries

- [2026-11-07 00:00] Update `[k/11 files documented]` in the chapter spec and root spec when closing this task.

[^okf-drivers]: `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` — OKF database drivers and dialers chapter.
[^okf-test-matrix]: `.agents/bundles/knowledge/testing/polyglot-test-matrix.md` — OKF polyglot test matrix.
[^db-lib]: `src/rs/vertebra-db/src/lib.rs` — vertebra-db public re-exports.
[^missing-dep]: `src/py/vertebra/_typing.py::MissingDependencyError` — Missing dependency error and install hint.
[^conformance]: `src/py/tests/integration/db/test_driver_conformance.py::_resolve_live_config` — Conformance live config resolution.
[^packaging-test]: `src/py/tests/unit/build/test_packaging_and_pgo.py` — Packaging extras gate.
[^conformance-cache]: `src/py/tests/integration/db/test_driver_conformance.py::_resolve_cache_config` — Conformance Redis and Valkey live-only resolution.
[^oracle-wallet]: `src/py/vertebra/db/adapters/oracle/config.py::OracleWallet` — Python Autonomous Database wallet parser.
