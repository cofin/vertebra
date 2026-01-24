# Tasks: ORM Extensions

**Transport**: Shared-memory C ABI via broker (no sockets/UDS/TCP).

## Phase 1: SQLSpec + Storage (Broker-Backed)
- [x] Create `src/py/vertebra/extensions/` package structure. <!-- id: 0 -->
- [x] Move SQLSpec integration to `vertebra.extensions.sqlspec`. <!-- id: 1 -->
- [ ] Implement broker-backed SQLSpec drivers (sync/async) for postgres/mysql/sqlite/redis. <!-- id: 2 -->
- [ ] Implement generic proxy adapter + provider host for external SQLSpec adapters. <!-- id: 3 -->
- [ ] Add storage pipeline bridge (fsspec-compatible) routed through broker. <!-- id: 4 -->
- [ ] Add Arrow decoding + threshold handling. <!-- id: 5 -->
- [ ] Add integration tests for SQLSpec + storage. <!-- id: 6 -->

## Phase 2: Extension API + Registration
- [x] Provide plugin registration via `Vertebra(plugins=[...])` / `register_plugin(...)`. <!-- id: 7 -->
- [ ] Wire extension hooks for worker startup and broker registration. <!-- id: 8 -->

## Phase 3: SQLAlchemy + Advanced Alchemy (Deferred; last)
- [ ] Implement `vertebra.extensions.sqlalchemy.dbapi` (PEP 249). <!-- id: 9 -->
- [ ] Implement `vertebra.extensions.sqlalchemy.dialect`. <!-- id: 10 -->
- [ ] Verify `advanced-alchemy` compatibility. <!-- id: 11 -->
- [ ] Benchmarks + docs. <!-- id: 12 -->
