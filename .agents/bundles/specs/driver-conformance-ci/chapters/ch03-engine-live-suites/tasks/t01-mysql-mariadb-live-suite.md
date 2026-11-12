---
type: Task
id: "driver-conformance-ci:ch03:t01"
chapter_id: ch03-engine-live-suites
title: Add the live MySQL and MariaDB edge suite
description: Create a family-parametrized live suite covering utf8mb4, DECIMAL(65,30), DATETIME(6), JSON, lastrowid, multi-result CALL, KILL QUERY cancellation, Arrow load and stream, and duplicate-key IntegrityError across MySQL and MariaDB.
status: draft
state: pending
priority: P1
verification_strategy: integration_acceptance
depends_on:
  - "driver-conformance-ci:ch01:t02"
read_set:
  - src/py/vertebra/db/adapters/mysql/driver.py
  - src/py/vertebra/db/adapters/mysql/config.py
  - src/py/tests/integration/conftest.py
  - src/py/tests/integration/db/test_oracle_native.py
  - src/py/vertebra/_vertebra.pyi
files: []
tests:
  - src/py/tests/integration/db/test_mysql_native.py
estimated_loc: 240
plan_revision: 1
plan_commit: null
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-12T00:30:00Z"
updated_at: "2026-11-12T00:30:00Z"
commit: null
sources:
  - id: mysql-driver
    resource: src/py/vertebra/db/adapters/mysql/driver.py::SyncMySQLDriver.fetch_all_sets
    title: Multi-result CALL, cancel_query, lastrowid, load_from_arrow, stream_arrow_batches
  - id: mysql-config
    resource: src/py/vertebra/db/adapters/mysql/config.py::MySQLConfig.from_url
    title: MySQL and MariaDB DSN parsing
  - id: fixtures
    resource: src/py/tests/integration/conftest.py::vtb_mariadb_config
    title: Versioned MySQL and MariaDB container fixtures
tags:
  - mysql
  - mariadb
  - integration
---

# Task t01: Add the Live MySQL and MariaDB Edge Suite

## Objective & Scope Boundary

Create `src/py/tests/integration/db/test_mysql_native.py` with a `mysql_family_config` fixture parametrized over `mysql` and `mariadb` (each `pytest.param` carrying `pytest.mark.driver_engine(<family>)`), a `mode` parametrization (`"sync"`, `"async"`) where both drivers apply, and the cases in the Worked Test Matrix [^mysql-driver] [^fixtures].

Resolution: `ensure_module("vertebra._mysql", package="vertebra-core[mysql]", install_package="vertebra-core[mysql]", extra="mysql")` → `VERTEBRA_CONFORMANCE_<FAMILY>_DSN` → `request.getfixturevalue("vtb_<family>_config")` under `VERTEBRA_CONFORMANCE_DOCKER == "1"` → `pytest.skip("<family> live tests require VERTEBRA_CONFORMANCE_<FAMILY>_DSN or VERTEBRA_CONFORMANCE_DOCKER=1")`.

Out of scope: production code; driver failures are recorded as `blocked` with the observed error.

## Bounded Read Manifest

- `src/py/vertebra/db/adapters/mysql/driver.py` — `SyncMySQLDriver.fetch_all_sets`, `cancel_query`, `connection_id`, `lastrowid`, `load_from_arrow`, `stream_arrow_batches`, `select_to_arrow`, `AsyncMySQLDriver` counterparts [^mysql-driver].
- `src/py/vertebra/db/adapters/mysql/config.py` — `MySQLConfig.from_url` [^mysql-config].
- `src/py/tests/integration/conftest.py` — `vtb_mysql_config`, `vtb_mariadb_config` [^fixtures].
- `src/py/tests/integration/db/test_oracle_native.py` — live-suite gate precedent.
- `src/py/vertebra/_vertebra.pyi` — `DatabaseError.vendor_code`, `IntegrityError`.

## Symbol & Signature Contracts

```python
@pytest.fixture(params=[pytest.param("mysql", marks=pytest.mark.driver_engine("mysql")), pytest.param("mariadb", marks=pytest.mark.driver_engine("mariadb"))])
def mysql_family_config(request: pytest.FixtureRequest) -> MySQLConfig: ...

@pytest.fixture
def sync_mysql(mysql_family_config: MySQLConfig) -> Iterator[SyncMySQLDriver]: ...

@pytest.fixture
async def async_mysql(mysql_family_config: MySQLConfig) -> AsyncIterator[AsyncMySQLDriver]: ...
```

- Each test creates `vtb_live_<case>` and drops it in `finally` (`DROP TABLE IF EXISTS`).
- Cancellation uses a `threading.Thread` (sync) or `asyncio.create_task` (async) running `SELECT SLEEP(30)` on driver A, polls `information_schema.PROCESSLIST` from driver B until `INFO LIKE 'SELECT SLEEP(30)%'`, then calls `driver_b.cancel_query(driver_a.connection_id)`.

## Edge Cases & Error Semantics

- `KILL QUERY` on `SLEEP` returns `1` instead of an error on both MySQL and MariaDB.
- `DECIMAL(65,30)` must round-trip as an exact `Decimal` (no float coercion).
- `DATETIME(6)` returns naive `datetime` with microseconds intact.
- `CALL` returning two result sets needs no multi-statement flag.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/integration/db/test_mysql_native.py`. Dependency category: remote-owned MySQL `8.0`/`8.4`/`9.7` and MariaDB `11.4`/`11.8` via `pytest-databases`.

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| `utf8mb4` text (sync/async) | `CREATE TABLE vtb_live_text (id INT PRIMARY KEY, v VARCHAR(64) CHARACTER SET utf8mb4)`; insert `(1, "🚀𝄞 café 脊椎")` | `select_value("SELECT v FROM vtb_live_text WHERE id = ?", (1,), value_type=str) == "🚀𝄞 café 脊椎"` |
| `DECIMAL(65,30)` | insert `Decimal("12345678901234567890123456789012345.123456789012345678901234567890")` | Selected value is `Decimal` and equal |
| `DATETIME(6)` | insert `datetime(2026, 11, 12, 1, 2, 3, 456789)` | Equal naive `datetime` with `microsecond == 456789` |
| `JSON` | insert `encode_json({"k": [1, 2, {"z": None}]})` into `JSON` column | `decode_json(value) == {"k": [1, 2, {"z": None}]}` |
| `lastrowid` | `AUTO_INCREMENT` table; two inserts | `driver.lastrowid == 1` then `2` |
| Multi-result `CALL` | `CREATE PROCEDURE vtb_live_two_sets() BEGIN SELECT 1 AS a; SELECT 2 AS b; END`; `fetch_all_sets("CALL vtb_live_two_sets()")` | `[[{"a": 1}], [{"b": 2}]]` (any trailing empty status set excluded or asserted explicitly as `[]`) |
| `KILL QUERY` cancel (sync/async) | `SELECT SLEEP(30)` cancelled via `cancel_query(conn_id)` | `cancel_query` returns `True`; sleeper returns `1` in under `10 s` |
| Arrow load + stream | `pa.table({"id": pa.array(range(10_000), pa.int64()), "label": [f"r{i}" for i in range(10_000)]})`; `load_from_arrow("vtb_live_arrow", table)`; `stream_arrow_batches("SELECT id, label FROM vtb_live_arrow ORDER BY id", batch_size=4096)` | `load_from_arrow` returns `10_000`; batch row counts `[4096, 4096, 1808]` |
| Duplicate key | insert `id=1` twice | `IntegrityError` with `vendor_code == 1062` |

## Verification

- Offline: `make dev && uv run --no-sync pytest src/py/tests/integration/db/test_mysql_native.py -q -rs` exits `0` with every case skipped.
- Live MySQL: `VERTEBRA_CONFORMANCE_DOCKER=1 VERTEBRA_CONFORMANCE_ENGINES=mysql VERTEBRA_CONFORMANCE_MYSQL_VERSION=8.4 uv run --no-sync pytest src/py/tests/integration/db -q -rs` exits `0`.
- Live MariaDB: `VERTEBRA_CONFORMANCE_DOCKER=1 VERTEBRA_CONFORMANCE_ENGINES=mariadb VERTEBRA_CONFORMANCE_MARIADB_VERSION=11.8 uv run --no-sync pytest src/py/tests/integration/db -q -rs` exits `0`.
- Lint: `uv run --no-sync ruff check src/py/tests/integration/db/test_mysql_native.py && uv run --no-sync ruff format --check src/py/tests/integration/db/test_mysql_native.py`.

## Acceptance Criteria

- [ ] Every Worked Test Matrix row passes on MySQL `8.0`, `8.4`, `9.7` and MariaDB `11.4`, `11.8` (R6).
- [ ] Cases are tagged per family and selected only in their lane (R6).

## Notes & Discoveries

- [2026-11-12 00:30] `MySQLWireTestServer` remains the offline wire-contract double in unit tests; this suite is the live evidence.

[^mysql-driver]: `src/py/vertebra/db/adapters/mysql/driver.py::SyncMySQLDriver.fetch_all_sets` — MySQL driver surface.
[^mysql-config]: `src/py/vertebra/db/adapters/mysql/config.py::MySQLConfig.from_url` — DSN parsing.
[^fixtures]: `src/py/tests/integration/conftest.py::vtb_mariadb_config` — Versioned container fixtures.
