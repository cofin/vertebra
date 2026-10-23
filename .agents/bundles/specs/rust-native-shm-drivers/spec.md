---
type: Spec
flow_id: rust-native-shm-drivers
title: "Rust-Native Shared-Memory Database Drivers, Cloud Dialers & SQLSpec/Alchemy Adapters (vertebra-db)"
state: planned
plan_revision: 1
plan_commit: null
state_revision: 0
current_task: null
last_operation: null
operation_targets: []
last_verified_checkpoint: null
created_at: 2026-10-23T20:18:00Z
updated_at: 2026-10-23T20:18:00Z
description: "Restore and expand src/rs/vertebra-db with native Rust PostgreSQL, MySQL, SQLite, and Cloud SQL / AlloyDB mTLS dialers, wire zero-copy shared-memory ring and Arrow IPC execution into vertebra-py and vertebra.db, and expose first-class Vertebra* SQLSpec, SQLAlchemy +vtb, and Advanced Alchemy adapters."
tags: [feature, rust, database, shm, arrow, postgres, mysql, sqlite, cloudsql, alloydb]
parent_prd: shm-native-drivers-and-broker-proxy
research: []
---

# Flow: Rust-Native Shared-Memory Database Drivers, Cloud Dialers & SQLSpec/Alchemy Adapters (`vertebra-db`)

**Flow ID:** `rust-native-shm-drivers`

## Specification

### Code Analysis Summary

- **Historical `src/rs/vertebra-db` (`6737c0c` / [PR #27](https://github.com/cofin/vertebra/pull/27)) and `src/rs/vertebra-broker` (`809c8d77` / `a5918811`)**:
  - `src/rs/vertebra-db` was added to the Rust workspace in `6737c0c` with `CloudSqlInstanceUri`, `AlloyDbClusterUri`, `AlloyDbAuthType`, `encode_alloydb_metadata_exchange`, `PoolerMode`, `compute_worker_pool_budget`, `strip_pg_jsonb_header`, `encode_resp_command`, and `encode_hello3_handshake`, while `809c8d77` and `a5918811` implemented SHM-backed `VertebraSyncConnection`, `VertebraAsyncConnection`, `VertebraSyncCursor`, `VertebraArrowCursor`, and first-class `VertebraPostgres*Config`, `VertebraMysql*Config`, and `VertebraSqlite*Config` adapters.
  - Currently on `feat/wave2`, `src/py/vertebra/db/{postgres,mysql,sqlite}.py` and `src/py/vertebra/db/dialer/{cloudsql,alloydb}.py` are pure-Python implementations that neither execute in Rust nor route through the shared-memory broker.
- **[`src/rs/vertebra-core/src/broker.rs`](../../../../src/rs/vertebra-core/src/broker.rs) & [`src/rs/vertebra-core/src/channel.rs`](../../../../src/rs/vertebra-core/src/channel.rs)**:
  - Provides `ShmLeaseTable`, `ShmStatementCache`, `SpscRing`, `MpmcRing`, and `ShmRegion` primitives for cross-process shared-memory leasing, statement caching, and zero-copy payload transfer.

### Relevant Patterns

- [PyO3 0.30.1 FFI & GIL Discipline](../../knowledge/patterns/rust/ffi-and-gil-discipline.md)
- [Lock-Free Shared Memory & Sharded Concurrency](../../knowledge/patterns/rust/lock-free-shm-and-sharding.md)
- [Database Drivers & Cloud Dialers](../../knowledge/domains/data/database-drivers-and-dialers.md)
- [SQLSpec, Advanced Alchemy & SQLAlchemy Dialect Extensions](../../knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md)

### Requirements

1. **Workspace Crate `src/rs/vertebra-db` & PyO3 Bindings (`vertebra-py`)**:
   - Restore `src/rs/vertebra-db` in the Cargo workspace (`edition = "2024"`) and bind its native types into `vertebra._vertebra` (`src/rs/vertebra-py` with `gil_used = false` and `py.detach(...)` around all I/O and SHM waits).
   - Implement native Rust Cloud SQL (`:3307` ephemeral X.509 mTLS via Cloud SQL Admin API + `rustls`) and AlloyDB (`:5433` mTLS + 4-byte BE length-prefixed protobuf `MetadataExchangeRequest` / `MetadataExchangeResponse`) dialers in `src/rs/vertebra-db/src/dialer.rs` so both Rust-native drivers and Python `CloudSqlDialer` / `AlloyDbDialer` use the same zero-sidecar Rust TLS/socket engine.
2. **Native Rust Database Pools & Shared-Memory Execution Engine**:
   - Implement Rust-backed driver execution and SHM result framing in `src/rs/vertebra-db` for:
     - **PostgreSQL (`postgres.rs`)**: Pooler-aware (`direct`, `transaction`, `session`, `statement`, `auto`), automatic `direct_dsn` routing for `LISTEN/NOTIFY` and DDL, binary `JSONB` `\x01` prefix stripping (`strip_pg_jsonb_header`), and binary `PGCOPY` parser producing compact row frames or Arrow IPC streams in `ShmRegion`.
     - **MySQL (`mysql.rs`)**: Prepared statement caching, Cloud SQL `:3307` mTLS dialer integration, and binary row decoding into SHM slots / Arrow IPC regions.
     - **SQLite (`sqlite.rs`)**: Native WAL mode, `mmap_size`, `busy_timeout`, `synchronous=NORMAL`, and multi-reader connection pool safe under free-threaded Python `3.14t`.
   - When running in multi-worker SHM topology, worker processes acquire connection leases from `ShmLeaseTable`, check `ShmStatementCache`, dispatch queries over the SHM broker ring, and read results zero-copy from `SpscRing` slots (below `arrow_threshold_rows=1000` / `arrow_threshold_bytes=65536`) or named/fd `ShmRegion` Arrow IPC buffers (`VertebraArrowCursor`).
3. **First-Class SQLSpec, SQLAlchemy `+vtb`, and Advanced Alchemy Native Adapters**:
   - Expose `VertebraPostgresSyncConfig` / `VertebraPostgresAsyncConfig`, `VertebraMysqlSyncConfig` / `VertebraMysqlAsyncConfig`, and `VertebraSqliteSyncConfig` / `VertebraSqliteAsyncConfig` in `src/py/vertebra/extensions/sqlspec/` backed by the SHM-native drivers.
   - Wire `DBAPIConnection` and `DBAPICursor` in [`src/py/vertebra/extensions/sqlalchemy/dialects.py`](../../../../src/py/vertebra/extensions/sqlalchemy/dialects.py) (`postgresql+vtb`, `mysql+vtb`, `sqlite+vtb`) and `AlchemyPlugin` (`NullPool` when broker-backed) to execute through the Rust-native SHM connections.

## Implementation Plan

### Phase 1: `vertebra-db` Crate, Cloud SQL / AlloyDB Rust Dialers & SHM Result Framing
- [ ] 1.1 Create `src/rs/vertebra-db` with native Cloud SQL (`:3307`) and AlloyDB (`:5433` `MetadataExchangeRequest`) mTLS dialers, `PGCOPY` binary parser, `JSONB` header stripping, and SHM row/Arrow IPC framing
- [ ] 1.2 Expose `vertebra-db` dialers, pools, and SHM cursor primitives in `src/rs/vertebra-py` (`gil_used = false`, `py.detach`) and wire `src/py/vertebra/db/dialer/` to the Rust implementation

### Phase 2: Native SHM Drivers (`Postgres`, `MySQL`, `SQLite`) & SQLSpec / SQLAlchemy `+vtb` Adapters
- [ ] 2.1 Wire `SyncPostgresDriver`/`AsyncPostgresDriver`, `SyncMysqlDriver`/`AsyncMysqlDriver`, and `SyncSqliteDriver`/`AsyncSqliteDriver` in `src/py/vertebra/db/` to `vertebra-db` and the SHM broker transport with `ShmRegion` Arrow IPC streaming
- [ ] 2.2 Add `VertebraPostgres*Config`, `VertebraMysql*Config`, and `VertebraSqlite*Config` in `src/py/vertebra/extensions/sqlspec/` and wire SQLAlchemy `+vtb` dialects with `NullPool` broker leasing

## Continuity Snapshot

- **Active Flow**: `rust-native-shm-drivers` (`planned`)
- **Current Task & Claimant**: `null`
- **Last Verified Checkpoint**: `null`
- **Decisions**:
  - Keep `CloudSqlDialer` and `AlloyDbDialer` in Vertebra and move their core URI parsing, metadata-exchange framing, and mTLS socket dialing into `src/rs/vertebra-db` so they power the Rust-native PostgreSQL and MySQL SHM drivers directly.
- **Recent Discoveries**:
  - `6737c0c` already contains the initial Rust `src/rs/vertebra-db/src/lib.rs` implementation of `CloudSqlInstanceUri`, `AlloyDbClusterUri`, `encode_alloydb_metadata_exchange`, `PoolerMode`, and `strip_pg_jsonb_header`.
- **Blockers & Unblock Conditions**: `none`
- **Next Exact Step**: Activate flow and claim Task `1.1` when ready to implement `src/rs/vertebra-db`.
- **Plan Identity**: `plan_revision: 1`, `plan_commit: null`
- **State Identity**: `state_revision: 0`, `last_operation: null`, `operation_targets: []`
- **Relevant Knowledge**:
  - `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md`
  - `.agents/bundles/knowledge/domains/data/broker-and-shm-leases.md`
  - `.agents/bundles/knowledge/patterns/rust/ffi-and-gil-discipline.md`
