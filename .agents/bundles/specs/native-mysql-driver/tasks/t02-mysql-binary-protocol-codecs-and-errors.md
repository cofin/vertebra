---
type: Task
flow_id: native-mysql-driver
task_id: t02
title: "Implement MySQL binary prepared-statement protocol, codecs, multi-result sets, and DatabaseError mapping"
description: "Implement COM_QUERY and COM_STMT_PREPARE/EXECUTE/CLOSE execution, per-connection LRU statement cache, multi-result set iteration, full binary/text codecs (DECIMAL, fractional temporal types, JSON, BLOB/VECTOR, BIT, ENUM/SET), and MySQL errno/SQLSTATE to DatabaseError hierarchy mapping."
status: stable
state: completed
verification_strategy: tdd
Verification-Exempt-Reason: null
dependencies:
  - t01
files_to_modify:
  - src/rs/vertebra-mysql/src/protocol.rs
  - src/rs/vertebra-mysql/src/codecs.rs
  - src/rs/vertebra-mysql/src/loopback.rs
  - src/rs/vertebra-mysql/src/errors.rs
  - src/rs/vertebra-mysql/src/client.rs
  - src/rs/vertebra-mysql/src/lib.rs
  - src/py/vertebra/_mysql.pyi
read_set:
  - .agents/bundles/specs/native-mysql-driver/spec.md
  - src/rs/vertebra-mysql/src/protocol.rs
  - src/rs/vertebra-mysql/src/client.rs
  - src/rs/vertebra-mysql/src/errors.rs
  - src/py/vertebra/db/exceptions.py
commands_to_run:
  - "cargo test --manifest-path src/rs/Cargo.toml -p vertebra-mysql --locked"
  - "cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-mysql --all-targets --locked -- -D warnings"
  - "uv run python -m mypy.stubtest vertebra._mysql"
  - "uv run python -m tools.scripts.review_setup validate"
commit_sha: null
created_at: "2026-11-09T03:25:00Z"
updated_at: "2026-11-09T03:25:00Z"
sources:
  - id: mysql-spec
    resource: .agents/bundles/specs/native-mysql-driver/spec.md
    title: Native MySQL driver specification
  - id: py-exceptions
    resource: src/py/vertebra/db/exceptions.py
    title: Canonical Python DatabaseError exception hierarchy
---

# Task t02: Implement MySQL binary prepared-statement protocol, codecs, multi-result sets, and `DatabaseError` mapping

<!-- truth: start -->
- `SyncMySQLClient` and `AsyncMySQLClient` execute parameterized statements via `COM_STMT_PREPARE`/`COM_STMT_EXECUTE`/`COM_STMT_CLOSE` with a bounded per-connection LRU statement cache and `COM_QUERY` text protocol fallback for DDL and multi-statement scripts [^mysql-spec].
- Binary and text codecs losslessly round-trip `DECIMAL`/`NEWDECIMAL`, fractional-second `DATETIME`/`TIMESTAMP`/`TIME`, `DATE`, `JSON`, `BLOB`/`TEXT`/`VECTOR`, `BIT`, and `ENUM`/`SET`, and map MySQL `errno` / `SQLSTATE` codes to `vertebra.db.exceptions` subclasses [^mysql-spec] [^py-exceptions].
<!-- truth: end -->

## Context

Task `t02` completes the MySQL binary prepared-statement protocol, multi-result sets, statement caching, binary/text value codecs, transaction/savepoint methods, and deterministic MySQL `errno` / `SQLSTATE` error mapping onto `vertebra.db.exceptions` [^mysql-spec] [^py-exceptions].

## Public Seam & TDD Plan

1. **Red**:
   - Write Rust and Python wire/loopback tests covering:
     - Binary `COM_STMT_PREPARE` (`0x16`), `COM_STMT_EXECUTE` (`0x17`) with NULL-bitmap (`(num_params + 7) / 8` at offset 0) and new-params-bound flag, and `COM_STMT_CLOSE` (`0x19`) on LRU eviction.
     - Binary row (`0x00` header + NULL bitmap with `+2` bit offset) and text row decoding for signed/unsigned integers (`TINY`, `SHORT`, `INT24`, `LONG`, `LONGLONG`), `FLOAT`/`DOUBLE`, `DECIMAL`/`NEWDECIMAL` -> `decimal.Decimal`, `DATE` -> `datetime.date`, fractional-second `DATETIME`/`TIMESTAMP` (0, 4, 7, and 11-byte encodings) -> `datetime.datetime`, signed `TIME` (0, 8, and 12-byte encodings) -> `datetime.time` / `datetime.timedelta`, `JSON`, `BIT`, `ENUM`/`SET`, and `BLOB`/`VARBINARY`/MySQL 9 `VECTOR`.
     - Multi-result set iteration (`SERVER_MORE_RESULTS_EXISTS` `0x0008`), `execute`, `execute_many`, `execute_with_description`, `fetch_all`, `fetch_one`, `fetch_optional`, `select_value`, `begin`, `commit`, `rollback`, `savepoint`, `release_savepoint`, and `rollback_to_savepoint`.
     - Error packet (`0xFF`) mapping for `1062`/`1452` (`IntegrityError`), `1213`/`1205` (`SerializationFailure`), `1317`/`3024` (`QueryCancelledError`), `1040`/`2006`/`2013` (`ConnectionFailure`), `1264`/`1366`/`1406` (`DataError`), `1054`/`1064`/`1146` (`ProgrammingError`), and generic server errors (`OperationalError`).
2. **Green**:
   - Implement `src/rs/vertebra-mysql/src/codecs.rs`, extend `src/rs/vertebra-mysql/src/protocol.rs`, `src/rs/vertebra-mysql/src/errors.rs`, and `src/rs/vertebra-mysql/src/client.rs`, and keep `src/py/vertebra/_mysql.pyi` synchronized.
3. **Refactor & Verify**:
   - Run `cargo test -p vertebra-mysql --locked`, `cargo clippy -p vertebra-mysql --all-targets --locked -- -D warnings`, `mypy.stubtest vertebra._mysql`, and `review_setup validate`.

## Acceptance Criteria

- [x] Binary prepared statements (`COM_STMT_PREPARE`/`EXECUTE`/`CLOSE`) and per-connection LRU statement cache pass loopback wire tests, including `COM_STMT_CLOSE` eviction when capacity is exceeded.
- [x] All MySQL value types (`DECIMAL`, fractional `DATETIME`/`TIMESTAMP`/`TIME`, `DATE`, `JSON`, `BLOB`/`VECTOR`, `BIT`, `ENUM`/`SET`, signed/unsigned ints, floats, `NULL`) round-trip accurately in both binary and text protocols.
- [x] Multi-result sets and savepoint lifecycle methods (`begin`, `commit`, `rollback`, `savepoint`, `release_savepoint`, `rollback_to_savepoint`) pass.
- [x] MySQL `errno` and `SQLSTATE` codes map to the exact `vertebra.db.exceptions` subclasses with `sqlstate` and `vendor_code` populated.
- [x] `cargo test`, `cargo clippy`, `mypy.stubtest vertebra._mysql`, and `review_setup validate` pass.

[^mysql-spec]: `.agents/bundles/specs/native-mysql-driver/spec.md` — Native MySQL driver specification.
[^py-exceptions]: `src/py/vertebra/db/exceptions.py` — Canonical Python DatabaseError exception hierarchy.
