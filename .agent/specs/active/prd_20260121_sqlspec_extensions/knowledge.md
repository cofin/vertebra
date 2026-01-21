# Knowledge Capture: Optimized SQLSpec Extensions for Vertebra Drivers

## Patterns Discovered
- **Adapter-First Structure:** Mirroring internal library structure (`sqlspec/adapters/<dialect>/<extension>/store.py`) inside `vertebra/extensions/sqlspec/<dialect>/<extension>/store.py` ensures logical consistency and easier upstream parity.
- **Vertebra Result Mapping:** `VertebraAsyncConnection.fetchrow` returns a tuple, while `sqlspec` often expects dict-like access. Using `VertebraAsyncDriver.execute` (which uses `_collect_rows` to map results to dicts) or manually mapping tuples to named fields is necessary.
- **Async Context Manager Awaiting:** Vertebra's `provide_connection` is an async method that returns an async context manager, requiring `async with (await self.config.provide_connection()) as conn:`.
- **MySQL Procedural DDL:** MySQL doesn't support `IF NOT EXISTS` for indexes. The pattern used in `sqlspec` (and mirrored here) involves checking `information_schema.statistics` using procedural SQL variables and prepared statements.
- **Julian Day Timestamps:** SQLite doesn't have a native timestamp type with timezone support that works well with `julianday()`. Using `REAL` to store Julian Day numbers is the most compatible pattern for efficient expiry queries in SQLite.

## Lessons Learned
- **Dependency Awareness:** `sqlspec` has optional extras like `adk` which are required for specific extensions. These must be explicitly added to `pyproject.toml`.
- **Import Side Effects:** Importing `sqlspec.extensions.adk` triggers imports of `google.adk`, which might be missing if the extra wasn't installed.
- **Test File Naming:** Unique basenames for test files are required when running pytest across multiple directories without `__init__.py` files to avoid collection collisions.
- **Timezone Naivety:** MySQL `DATETIME` is timezone-naive. UTC datetimes should be stored as naive and handled in the Python layer for consistency.

## Decisions Made
- **Manual Tuple Mapping in Litestar Store:** Manually mapped tuple indices to named fields in `get()` methods to avoid complex wrapper objects while maintaining performance.
- **Async/Sync Parity:** Implemented both async and sync versions for all stores to ensure full driver support.
- **Strict Casting:** Used `cast` and explicit type checks to satisfy mypy's strictness with `TypedDict` and database result tuples.

## Reusable Components
- `_datetime_to_julian` and `_julian_to_datetime` helpers for SQLite.
- `_to_sqlite_bool` and `_from_sqlite_bool` for SQLite boolean handling.
- `_parse_owner_id_column_for_mysql` for MySQL foreign key extraction.

## Gotchas & Warnings
- Always `await` the result of `config.provide_connection()` before using it in `async with`.
- Use `result.one_or_none()` or `result.all()` instead of `fetchone()`/`fetchall()` when using Vertebra's `Driver.execute`.
- Be careful with `sqlspec` extras; they are not always installed by default.