# Chapters — db-pool-broker-perf

- [x] [b14aa2c3] **ch01-sqlite-sqlalchemy-fastpath** (`[completed]`, `3` tasks) — Skip `BEGIN IMMEDIATE` on read-only SQLite `lazy_begin` queries, default SQLAlchemy SQLite `max_readers=0`, and remove dead `mpsc::channel()` allocations in `SqlitePool`. ([spec](ch01-sqlite-sqlalchemy-fastpath/spec.md))
- [x] [f1496e28] **ch02-async-shm-broker-contention** (`[completed]`, `3` tasks) — Single-waiter SHM futex wake, `LeaseBroker.wait_for_wake_async`, and per-loop local handoff + single-leader watcher in `AsyncSHMBrokerTransport`. ([spec](ch02-async-shm-broker-contention/spec.md))
- [~] **ch03-asyncpg-parity-and-verification** (`[active]`, `3` tasks) — `NativePostgresWorldDriver` single-checkout parity, `granian-rsgi-data-stdlib`, live SQLite + PostgreSQL 17 `asyncpg`/`psycopg` verification, and OKF doc sync. ([spec](ch03-asyncpg-parity-and-verification/spec.md))
