# Tasks: Vertebra SQLAlchemy Integration

## Phase 1: DBAPI Skeleton & Exceptions
- [ ] Create `vertebra/dbapi` package structure.
- [ ] Implement PEP 249 Exceptions in `vertebra/dbapi/exceptions.py`.
- [ ] Define `connect`, `Connection`, `Cursor` stubs in `vertebra/dbapi/module.py`.
- [ ] Define `TypeObjects` (STRING, BINARY, NUMBER, DATETIME, ROWID).
- [ ] Test: Verify `vertebra.dbapi` is importable and has required attributes.

## Phase 2: IPC Connection & Execution
- [ ] Implement `Connection.__init__` to establish IPC channel (mock/stub for now if kernel not ready).
- [ ] Implement `Cursor.execute` to serialize SQL + args to IPC buffer.
- [ ] Implement `Cursor.fetchall` to read from shared memory response.
  - [ ] **Critical**: Implement Zero-copy buffer protocol wrapper.
- [ ] Implement Transaction methods (`commit`, `rollback`) sending IPC opcodes.

## Phase 3: SQLAlchemy Dialect
- [ ] Create `vertebra/sqlalchemy.py`.
- [ ] Implement `VertebraDialect` class.
- [ ] Register `vertebra.ipc` entry point in `pyproject.toml`.
- [ ] Implement type mapping (SQL -> Vertebra types).
- [ ] Test: `create_engine("vertebra+ipc://")` works.
- [ ] Test: Basic reflection (if supported) or raw SQL execution.

## Phase 4: Advanced Alchemy Integration
- [ ] Create `vertebra/integrations/advanced_alchemy.py`.
- [ ] Implement `VertebraConfig` class.
- [ ] Add integration test with a minimal Litestar app.

## Phase 5: Verification
- [ ] Run full test suite.
- [ ] Verify memory usage (zero-copy check).
