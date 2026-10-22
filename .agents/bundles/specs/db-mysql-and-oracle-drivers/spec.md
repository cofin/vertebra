---
type: Spec
flow_id: db-mysql-and-oracle-drivers
title: "Chapter 2: MySQL and Oracle 26ai drivers, OracleWallet mTLS, DBMS_CLOUD staging, and +vtb dialects"
state: planned
plan_revision: 2
plan_commit: ""
state_revision: 0
current_task: null
last_operation: null
operation_targets: []
last_verified_checkpoint: null
created_at: 2026-10-22T04:45:00Z
updated_at: 2026-10-22T04:45:00Z
description: "Implement pooled SyncMysqlDriver (pymysql) and AsyncMysqlDriver (asyncmy), SyncOracleDriver and AsyncOracleDriver (oracledb Thin/Thick mode) with OracleWallet sqlnet.ora + mTLS, DBMS_CLOUD staging, OracleHybridSearch execution, and mysql+vtb / oracle+vtb SQLAlchemy 2.1 dialects."
tags: [feature, database, mysql, oracle, sqlalchemy, sqlspec]
parent_prd: vertebra-native-db-drivers
research: [../vertebra-design-quality/findings.md]
---

# Flow: Chapter 2: MySQL and Oracle 26ai drivers, OracleWallet mTLS, DBMS_CLOUD staging, and +vtb dialects

**Flow ID:** `db-mysql-and-oracle-drivers`

## Specification

### Parent Context

This chapter is Chapter 2 of [`vertebra-native-db-drivers`](../vertebra-native-db-drivers/spec.md) and depends on Chapter 1 (`db-postgres-and-cloud-dialers`). It owns requirements **R4** (pooled MySQL sync/async drivers with Cloud SQL `:3307` dialer integration), **R5** (pooled Oracle 26ai Thin/Thick drivers, `OracleWallet` `sqlnet.ora` + `tnsnames.ora` + `ewallet.pem` mTLS, `DBMS_CLOUD` object store staging, and live `OracleHybridSearch` execution), **R3** (the `mysql+vtb` and `oracle+vtb` SQLAlchemy 2.1 dialects and `AlembicImpl` DDL migration hooks), and **R9** (MySQL and Oracle unit and container-backed acceptance suite).

### Code Analysis Summary

- **`src/py/vertebra/db/mysql.py` (80 lines):**
  - `MysqlConfig` (`:19-43`) defines pool bounds, `cloud_sql_instance`, `enable_iam_auth`, `charset`, `autocommit`, and `to_cloud_dialer_config` (`:35-42`).
  - `SyncMysqlDriver.execute_tuples` (`:58-61`) ignores `sql` and `parameters` and returns `[]`.
  - `AsyncMysqlDriver.execute_tuples` (`:78-80`) delegates synchronously to `self._sync.execute_tuples`.
- **`src/py/vertebra/db/oracle/driver.py` (183 lines):**
  - `OracleDataFrame.to_dicts` (`:47-50`) calls `zip(cols, row, strict=False)` (Ruff `B905`).
  - `SyncOracleDriver` (`:69-116`) and `AsyncOracleDriver` (`:118-139`) have no `oracledb` pool creation, `execute_tuples`, `execute_with_description`, `stage_to_dbms_cloud`, or `close` methods (`FW-03`).
  - `SyncOracleDriver.encode_vector` (`:89-97`) accepts both `type_code: str = "f"` and `dtype: str | None = None` (`FW-56`).
- **`src/py/vertebra/db/oracle/wallet.py` (228 lines) & `containers.py` (50 lines):**
  - `OracleWallet` (`:21-57`) extracts `tnsnames.ora`, `ewallet.pem`, and `ewallet.p12`, but does not parse `sqlnet.ora` (`WALLET_LOCATION`, `SSL_SERVER_DN_MATCH`, `SSL_VERSION`), and `extract` (`:59-75`) accepts duplicate `target_dir` and `cache_root` parameters (`FW-56`).
  - `OracleContainer` (`containers.py:13-35`) hardcodes `admin_password: str = "WelcomeOracle26ai!"` (`:19`, Ruff `S105`, `FW-48`, `FW-56`).
- **`src/py/vertebra/extensions/sqlspec/oracle.py` (157 lines):**
  - `OracleHybridSearch.build_rrf_sql` (`:108-157`) interpolates `int(candidate_limit)` (`:130,137`) and `int(rrf_k)` (`:140,141`) into SQL while also placing `"rrf_k"` and `"candidate_limit"` in the returned `binds` dictionary (`:154-155`), which causes `oracledb` `ORA-01036: illegal variable name/number` on execution because `:rrf_k` and `:candidate_limit` do not appear as bind placeholders in the SQL string.
- **`src/py/vertebra/extensions/sqlalchemy/dialects.py` (291 lines) & `alembic.py` (145 lines):**
  - `DBAPIConnection` does not route `mysql+vtb://` or `oracle+vtb://` URLs to `SyncMysqlDriver` or `SyncOracleDriver`.
  - `AlembicImpl` (`alembic.py:20-110`) only handles `PostgresConfig` in `resolve_migration_url` (`:105-109`) and does not emit Oracle/MySQL DDL migration statements for `Vector` HNSW/IVF indexes or `CreateViewOp`/`DropViewOp` execution.

#### Findings carried in this chapter

- `FW-01` (MySQL portion): Replace no-op `SyncMysqlDriver.execute_tuples` and `AsyncMysqlDriver.execute_tuples` with pooled `pymysql` / `asyncmy` execution.
- `FW-03`: Implement pooled `SyncOracleDriver` and `AsyncOracleDriver` query execution, `DBMS_CLOUD` staging, and `OracleHybridSearch` execution.
- `FW-44` (MySQL and Oracle dialect portions): Wire `MysqlVtbDialect` and `OracleVtbDialect` to `SyncMysqlDriver` and `SyncOracleDriver`, and complete `AlembicImpl` DDL migration hooks.
- `FW-48` & `FW-56`: Fix `B905` in `OracleDataFrame.to_dicts`, remove duplicate `dtype` from `SyncOracleDriver.encode_vector` and `cache_root` from `OracleWallet.extract`, and remove hardcoded `admin_password` from `OracleContainer`.

### Relevant Patterns

- [Product Guidelines](../../product/product-guidelines.md) — `msgspec.Struct(frozen=True, gc=False)`, zero inline `#` comments, `vertebra.utils.serializers` for JSON/OSON serialization, and explicit optional dependency boundaries (`vertebra[mysql]`, `vertebra[oracle]`).

### Requirements

| ID | Requirement |
| --- | --- |
| C2-R1 | Add `mysql` optional extra (`pymysql>=1.1`, `asyncmy>=0.2.9`) in `pyproject.toml`, implement pooled `SyncMysqlDriver` and `AsyncMysqlDriver` in `src/py/vertebra/db/mysql.py` with Cloud SQL `:3307` dialer socket support, and wire `MysqlVtbDialect` (`mysql+vtb`) in `src/py/vertebra/extensions/sqlalchemy/dialects.py`. |
| C2-R2 | Add `oracle` optional extra (`oracledb>=2.5`, `cryptography>=43.0`) in `pyproject.toml`, implement pooled `SyncOracleDriver` and `AsyncOracleDriver` (Thin and Thick mode, `DB_TYPE_VECTOR` array binds, OSON decoding, `DBMS_CLOUD.COPY_DATA` object store staging), add `sqlnet.ora` parsing to `OracleWallet`, fix `B905`/`S105`/`FW-56` in `src/py/vertebra/db/oracle/`, and fix `ORA-01036` bind parameterization plus live execution in `OracleHybridSearch`. |
| C2-R3 | Wire `OracleVtbDialect` (`oracle+vtb`) in `src/py/vertebra/extensions/sqlalchemy/dialects.py` (`Vector(dim, storage_format)`, `BinaryJson` `OSON`, `SelectInLoadSpec` `<=1000` chunking, `INSERT ... RETURNING INTO`) and extend `AlembicImpl` in `src/py/vertebra/extensions/sqlalchemy/alembic.py` with `CreateVectorIndexOp`, Oracle/MySQL `resolve_migration_url`, and DDL execution hooks. |
| C2-R4 | Add unit and container-backed acceptance tests for MySQL and Oracle drivers, wallets, hybrid RRF search, and `mysql+vtb` / `oracle+vtb` dialects in `src/py/tests/unit/db/test_mysql_and_oracle.py`. |

### Target Layout

```text
pyproject.toml                                          add vertebra[mysql] and vertebra[oracle] extras
src/py/vertebra/db/
  mysql.py                                              SyncMysqlDriver (pymysql pool) + AsyncMysqlDriver (asyncmy pool) + CloudSqlDialer socket hook
  oracle/
    config.py                                           OracleConfig thin/thick mode + resolve_wallet(target_dir=...)
    containers.py                                       OracleContainer without hardcoded admin_password
    driver.py                                           SyncOracleDriver + AsyncOracleDriver (oracledb pool, execute_tuples, stage_to_dbms_cloud)
    wallet.py                                           OracleWallet sqlnet.ora parser + tnsnames.ora + ewallet.pem mTLS
src/py/vertebra/extensions/
  sqlalchemy/
    __init__.py                                         export CreateVectorIndexOp
    alembic.py                                          AlembicImpl Oracle/MySQL DDL hooks + CreateVectorIndexOp
    dialects.py                                         MysqlVtbDialect + OracleVtbDialect + DBAPIConnection routing for MySQL/Oracle
  sqlspec/
    oracle.py                                           OracleHybridSearch with parameterized :rrf_k / :candidate_limit and execute_rrf
src/py/tests/unit/
  data/
    test_mysql_driver.py                                focused unit tests for SyncMysqlDriver, AsyncMysqlDriver, and mysql+vtb dialect
    test_oracle_driver.py                               focused unit tests for SyncOracleDriver, AsyncOracleDriver, OracleWallet, and OracleHybridSearch
  extensions/
    test_sqlalchemy.py                                  focused unit tests for MysqlVtbDialect, OracleVtbDialect, and AlembicImpl
```

### Risks

| Risk | Mitigation |
| --- | --- |
| `OracleHybridSearch.build_rrf_sql` includes `rrf_k` and `candidate_limit` in `binds` without matching `:rrf_k` and `:candidate_limit` placeholders in the SQL string, causing `oracledb` to fail with `ORA-01036`. | Task `2.2` replaces the interpolated `{int(candidate_limit)}` and `{int(rrf_k)}` fragments in `build_rrf_sql` with `:candidate_limit` and `:rrf_k` bind placeholders so every key in `binds` matches a SQL placeholder. |
| Multiple Oracle pools in one process overwrite `os.environ["TNS_ADMIN"]`. | `OracleWallet.extract` extracts each wallet into an isolated `0o700` directory and passes `config_dir` and `wallet_location` per pool without mutating `os.environ`. |

## Implementation Plan

### Phase 1: MySQL and Oracle Drivers, Wallets, and `+vtb` Dialects
- [ ] 2.1 Pooled `SyncMysqlDriver`, `AsyncMysqlDriver`, and `mysql+vtb` SQLAlchemy 2.1 dialect ([worksheet](tasks/2.1.md))
- [ ] 2.2 Pooled `SyncOracleDriver` and `AsyncOracleDriver`, `OracleWallet` `sqlnet.ora` mTLS, `DBMS_CLOUD` staging, and `OracleHybridSearch` execution ([worksheet](tasks/2.2.md))
- [ ] 2.3 SQLAlchemy 2.1 `OracleVtbDialect` (`oracle+vtb`) and `AlembicImpl` DDL migration hooks ([worksheet](tasks/2.3.md))
- [ ] 2.4 Unit and container-backed acceptance tests for MySQL and Oracle drivers, wallets, and `+vtb` dialects ([worksheet](tasks/2.4.md))
- [ ] Chapter gate: `make lint-all && make py-test`

## Requirement-to-task trace

| Requirement | Tasks | Verification |
| --- | --- | --- |
| C2-R1 | 2.1 | `uv run pytest src/py/tests/unit/data/test_mysql_driver.py -q` |
| C2-R2 | 2.2 | `uv run pytest src/py/tests/unit/data/test_oracle_driver.py -q` |
| C2-R3 | 2.3 | `uv run pytest src/py/tests/unit/extensions/test_sqlalchemy.py -k "oracle or alembic" -q` |
| C2-R4 | 2.4 | `uv run pytest src/py/tests/unit/data/test_mysql_driver.py src/py/tests/unit/data/test_oracle_driver.py src/py/tests/unit/extensions/test_sqlalchemy.py -q` |

## Finding-to-task trace

| Finding | Task |
| --- | --- |
| FW-01 (MySQL driver) | 2.1 |
| FW-03 | 2.2 |
| FW-48 (Oracle `B905` / `S105`) | 2.2 |
| FW-56 | 2.2 |
| FW-44 (MySQL and Oracle `+vtb` dialects & Alembic) | 2.1, 2.3 |

## Continuity Snapshot

- Active flow: `db-mysql-and-oracle-drivers`, state `planned`. Current task: none. Next step: `/flow:implement db-mysql-and-oracle-drivers` after Chapter 1 completes.
- Plan identity: `plan_revision: 2`, `plan_commit: ""`. State identity: `state_revision: 0`.
- Blockers: depends on Chapter 1 (`db-postgres-and-cloud-dialers`).
- Knowledge paths: `.agents/bundles/product/product-guidelines.md`, `.agents/bundles/knowledge/workflow.md`.
