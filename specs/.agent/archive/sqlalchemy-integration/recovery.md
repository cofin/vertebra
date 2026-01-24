# Recovery Notes: Vertebra SQLAlchemy Integration

## Potential Issues

### IPC Protocol Mismatch
- **Risk**: The Python IPC client definitions might drift from the Rust kernel definitions.
- **Mitigation**: Ensure shared constants/schema (possibly via `vertebra-common` package or generated code).

### Zero-Copy Limitations
- **Risk**: Python objects (int, str) require allocation. True zero-copy is only possible for buffer interfaces (bytes, arrow).
- **Mitigation**:
  - For numeric/string data, minimal copying is acceptable.
  - For large datasets, expose an `arrow()` method on the cursor to bypass row-by-row conversion if users want raw speed.
  - The "Zero-copy" requirement in PRD mainly applies to the *transport* layer (reading from shmem) before converting to Python objects.

### Advanced Alchemy Async Compatibility
- **Risk**: If the DBAPI is blocking, using it with `AdvancedAlchemyAsyncConfig` might block the event loop.
- **Mitigation**: Run in a threadpool (standard SQLAlchemy `asyncio` engine behavior) or implement a true `async` DBAPI later.
