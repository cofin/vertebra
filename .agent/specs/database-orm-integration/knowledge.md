# Knowledge Capture: Consolidated Database & ORM Integration

## Patterns Discovered
- **Polyglot Configuration**: Using a centralized Bun broker to own connection pools and exposing them to Python via shared-memory C ABI ring buffers.
- **Dialect Redirection**: Registering custom SQLAlchemy dialects (e.g., `postgresql+vertebra`) that act as thin wrappers around a shared DBAPI implementation which routes to the broker.
- **Dual-Mode Registration**: Supporting both sync and async dialects by having separate registration names (`+vertebra` for async, `+vertebra_sync` for sync) but potentially sharing core logic.
- **Type-Safe Serialization Hooks**: Using `msgspec` hooks (`enc_hook`, `dec_hook`) to handle 30+ non-primitive types (Path, UUID, IP, etc.) uniformly across JSON and MessagePack.
- **Litestar Plugin Layering**: `VertebraPlugin` acts as a foundation by registering types, while ORM-specific plugins (`VertebraSQLSpecPlugin`, etc.) inherit from native plugins and leverage the foundation.

## Lessons Learned
- **SQLAlchemy Dialect Complexity**: Native dialects often perform many initialization queries (version check, schema check, isolation level). Mocking these requires careful side-effect setup or a more robust fake connection.
- **Async DBAPI Wrapping**: SQLAlchemy's `create_async_engine` requires a dialect with `is_async = True`. Even if the underlying DBAPI is sync, SQLAlchemy can wrap it, but for native async support, a dedicated `_async_dbapi.py` with `async def` methods is preferred for clarity and performance.
- **Litestar Import Paths**: Litestar's plugin locations can change between versions (e.g., `contrib` vs `plugins`). Always verify imports in the target version.
- **Dataclass Inheritance in AA**: Advanced Alchemy config classes are dataclasses. Overriding `__init__` requires careful handling of base class initialization (calling `BaseClass.__init__` directly if `super()` is ambiguous).

## Decisions Made
- **Prefix Standard**: Settled on `Vertebra*` prefix for all public-facing database and ORM classes to maintain brand consistency.
- **Dialect Naming**: Adopted `postgresql+vertebra` for the primary (async) dialect and `postgresql+vertebra_sync` for the synchronous variant.
- **Serialization Strategy**: Arrow IPC for large results (>=1000 rows/1MB) and MessagePack for everything else in the data plane.
- **Storage Unification**: Routed all storage operations (local, S3, GCS) through the same broker process using a unified `ObjectStoreBase`/`Backend` protocol.

## Reusable Components
- `VertebraCodec`: A highly compatible MessagePack/JSON codec supporting 30+ Python types.
- `_dbapi.py` and `_async_dbapi.py`: Generic PEP 249-like implementations for routing any SQL protocol through shared memory.
- `VertebraStorageBackend`: A storage abstraction that can be used with `sqlspec` or `advanced-alchemy`.

## Gotchas & Warnings
- **SHM Cleanup**: Shared memory regions created for Arrow batches must be explicitly released by the client once consumed to avoid leaks in `/dev/shm`.
- **Typing Registry**: When adding support for optional dependencies (Pydantic, etc.), always update the `vertebra.typing` facade to ensure consistency across the codebase.
- **DBAPI fetchone() return type**: SQLAlchemy's version check regexes expect strings. Ensure mocks or real drivers return strings for metadata queries, not ints or other types.