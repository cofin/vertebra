# Knowledge Capture: Optimized SQLSpec Extensions for Vertebra Drivers

## Patterns Discovered
- **Julian Day Timestamps:** SQLite doesn't have a native timestamp type with timezone support that works well with `julianday()`. Using `REAL` to store Julian Day numbers (as seen in `sqlspec`'s internal adapters) is the most compatible pattern for efficient expiry queries.
- **SQL Parameter Parity:** PostgreSQL uses `$1, $2`, while SQLite uses `?`. Vertebra's `VertebraAsyncConnection.execute` handles both if passed correctly, but the implementations must be dialect-aware.

## Lessons Learned
- **SQLite Booleans:** SQLite lacks a native `BOOLEAN` type. Using `INTEGER` (0/1) and explicit conversion helpers (`_to_sqlite_bool`, `_from_sqlite_bool`) ensures consistency across drivers.
- **JSON as TEXT:** SQLite stores JSON as `TEXT`. Explicit serialization to string before insertion and deserialization after fetching is required.

## Decisions Made
- **Mirroring Aiosqlite Logic:** Followed the Julian Day conversion logic from `sqlspec.adapters.aiosqlite` to ensure identical behavior when using Vertebra's SQLite driver.
- **Async/Sync Parity for SQLite:** Implemented both `SqliteADKStore` and `SyncSqliteADKStore`.

## Reusable Components
[Code, utilities, or approaches that could be reused]

## Gotchas & Warnings
[Pitfalls to avoid in similar work]
