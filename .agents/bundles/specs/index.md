# Active Specifications

- **[SQLite SQLAlchemy Fastpath, Lock-Free Async SHM Broker, and Local asyncpg Parity](db-pool-broker-perf/spec.md)** (`db-pool-broker-perf`, `status: stable`, `plan_revision: 2`, `3` chapters / `9` tasks) — Read-only SQLite `lazy_begin` lexical scan and SQLAlchemy `max_readers=0`, single-waiter SHM futex wake and lock-free `AsyncSHMBrokerTransport` local handoff, and 1:1 local `asyncpg`/`psycopg` PostgreSQL 17 benchmarks.
