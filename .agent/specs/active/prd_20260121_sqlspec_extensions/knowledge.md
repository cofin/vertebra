# Knowledge Capture: Optimized SQLSpec Extensions for Vertebra Drivers

## Patterns Discovered
- **Adapter-First Structure:** Mirroring internal library structure (`sqlspec/adapters/<dialect>/<extension>/store.py`) inside `vertebra/extensions/sqlspec/<dialect>/<extension>/store.py` ensures logical consistency and easier upstream parity.
- **Vertebra Result Mapping:** `VertebraAsyncConnection.fetchrow` returns a tuple, while `sqlspec` often expects dict-like access. Using `VertebraAsyncDriver.execute` (which uses `_collect_rows` to map results to dicts) or manually mapping tuples to named fields is necessary.

## Lessons Learned
- **Dependency Awareness:** `sqlspec` has optional extras like `adk` which are required for specific extensions. These must be explicitly added to `pyproject.toml`.
- **Import Side Effects:** Importing `sqlspec.extensions.adk` triggers imports of `google.adk`, which might be missing if the extra wasn't installed.

## Decisions Made
- **Manual Tuple Mapping in Litestar Store:** In `PostgresLitestarStore.get`, I manually mapped the tuple indices `row[0]` (data) and `row[1]` (expires_at) to avoid complex wrapper objects while maintaining performance.
- **Async/Sync Parity:** Implemented both `PostgresADKStore` and `SyncPostgresADKStore` to ensure support for both driver types as requested.

## Reusable Components
[Code, utilities, or approaches that could be reused]

## Gotchas & Warnings
[Pitfalls to avoid in similar work]
