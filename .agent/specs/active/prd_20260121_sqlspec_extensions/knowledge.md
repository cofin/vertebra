# Knowledge Capture: Optimized SQLSpec Extensions for Vertebra Drivers

## Patterns Discovered
- **MySQL Procedural DDL:** MySQL doesn't support `IF NOT EXISTS` for indexes. The pattern used in `sqlspec` (and mirrored here) involves checking `information_schema.statistics` using procedural SQL variables and prepared statements.
- **MySQL Parameter Style:** Uses `%s` consistently across all implementations.

## Lessons Learned
- **Timezone Naivety:** MySQL `DATETIME` is timezone-naive. Following `sqlspec`'s lead, we store UTC as naive datetime and handle the "awareness" in the Python layer using `.replace(tzinfo=timezone.utc)`.
- **JSON Type Requirement:** Requires MySQL 5.7.8+ for the native `JSON` column support used in the stores.

## Decisions Made
- **Mirroring Asyncmy logic:** Used `UTC_TIMESTAMP(6)` for microsecond precision current time in MySQL, consistent with `sqlspec.adapters.asyncmy`.
- **Async/Sync Parity for MySQL:** Implemented both `MysqlADKStore` and `SyncMysqlADKStore`.

## Reusable Components
[Code, utilities, or approaches that could be reused]

## Gotchas & Warnings
[Pitfalls to avoid in similar work]
