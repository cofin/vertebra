# Planner Notes — ch03-asyncpg-parity-and-verification

## Files Written

- `chapters/ch03-asyncpg-parity-and-verification/spec.md`
- `chapters/ch03-asyncpg-parity-and-verification/tasks/t07-postgres-driver-parity-and-rsgi-target.md`
- `chapters/ch03-asyncpg-parity-and-verification/tasks/t08-live-sqlite-and-postgres-asyncpg-verification.md`
- `chapters/ch03-asyncpg-parity-and-verification/tasks/t09-okf-knowledge-sync-and-final-gate.md`

## Task Summaries

- `t07`: Align `NativePostgresWorldDriver` to single-checkout/single-lease per multi-query call and register `granian-rsgi-data-stdlib` (port `8170`) with offline unit tests.
- `t08`: Sync `--group postgres-bench`, run release `make build`, and execute live Suite B (SQLite) and Suite C (Docker PostgreSQL 17 vs `asyncpg`/`psycopg` HTTP + standalone comparators).
- `t09`: Update 4 OKF knowledge chapters in present tense and pass `make lint-all`, `make rs-test`, `make review-check`, and Flow validation.

## Root Spec Issues

None

## Open Questions

None
