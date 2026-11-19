"""Shared pluggable database, cache, and object storage backend for benchmark apps.

Provides matched connection pool budget parity across worker topologies,
multi-cloud object storage configuration (blocking MinIO), deterministic offline
memory and SQLite backends, and free-threaded GIL enforcement.
"""

from __future__ import annotations

import os
import random
import sqlite3
import sys
import tempfile
import threading
from collections.abc import Mapping
from pathlib import Path
from typing import Final

import msgspec

from vertebra.storage import (
    AsyncObjectStore,
    AzureConfig,
    GCSConfig,
    LocalConfig,
    MemoryConfig,
    OCIConfig,
    R2Config,
    S3Config,
    StorageConfig,
    SyncObjectStore,
)

__all__ = (
    "FORBIDDEN_STORAGE_BACKENDS",
    "JSON_HELLO_PAYLOAD",
    "WORLD_ROW_COUNT",
    "DataBackend",
    "DataBackendConfig",
    "WorldRow",
    "assert_free_threaded_runtime",
    "clamp_query_count",
    "get_default_backend",
    "load_backend_config",
    "resolve_pool_budget",
)

WORLD_ROW_COUNT: Final[int] = 10_000
JSON_HELLO_PAYLOAD: Final[bytes] = b'{"message":"Hello, World!"}'
FORBIDDEN_STORAGE_BACKENDS: Final[frozenset[str]] = frozenset({"minio"})
_FORBIDDEN_STORAGE_ERROR: Final[str] = "MinIO is forbidden; use rustfs, floci_s3, floci_gcs, azurite, local, or memory"
_TRUTHY_VALUES: Final[frozenset[str]] = frozenset({"1", "true", "yes", "on"})


class WorldRow(msgspec.Struct, frozen=True, gc=False):
    """Immutable TechEmpower World table row."""

    id: int
    randomNumber: int


class DataBackendConfig(msgspec.Struct, frozen=True, gc=False):
    """Configuration for the shared benchmark data, cache, and storage backend."""

    db_backend: str = "memory"
    db_dsn: str = ""
    cache_backend: str = "memory"
    cache_dsn: str = ""
    storage_backend: str = "memory"
    storage_endpoint: str = ""
    storage_bucket: str = "vertebra-bench"
    storage_root: str = ""
    workers: int = 1
    broker_mode: str = "none"
    total_pool_budget: int = 40
    strict_free_threading: bool = False


def _parse_bool(raw: str | None) -> bool:
    """Return True when an environment flag string represents a truthy value."""
    if raw is None:
        return False
    return raw.strip().lower() in _TRUTHY_VALUES


def _parse_positive_int(raw: str | None, default: int) -> int:
    """Parse a positive integer string with a safe fallback."""
    if raw is None:
        return default
    try:
        parsed = int(raw.strip())
    except ValueError:
        return default
    return max(1, parsed)


def _validate_storage_backend(backend: str) -> str:
    """Normalize storage backend name and reject forbidden backends."""
    normalized = backend.strip().lower() or "memory"
    if normalized in FORBIDDEN_STORAGE_BACKENDS:
        raise ValueError(_FORBIDDEN_STORAGE_ERROR)
    return normalized


def load_backend_config(env: Mapping[str, str] | None = None) -> DataBackendConfig:
    """Load benchmark backend configuration from environment variables.

    Args:
        env: Optional mapping of environment variables. Defaults to ``os.environ``.

    Returns:
        Frozen ``DataBackendConfig`` populated from ``VTB_BENCH_*`` and
        ``VERTEBRA_STRICT_FREE_THREADING``.

    Raises:
        ValueError: If ``VTB_BENCH_STORAGE_BACKEND`` selects a forbidden backend.
    """
    source = os.environ if env is None else env
    storage_backend = _validate_storage_backend(source.get("VTB_BENCH_STORAGE_BACKEND", "memory"))
    db_backend = source.get("VTB_BENCH_DB_BACKEND", "memory").strip().lower() or "memory"
    cache_backend = source.get("VTB_BENCH_CACHE_BACKEND", "memory").strip().lower() or "memory"
    broker_mode = source.get("VTB_BENCH_BROKER_MODE", "none").strip().lower() or "none"
    return DataBackendConfig(
        db_backend=db_backend,
        db_dsn=source.get("VTB_BENCH_DB_DSN", "").strip(),
        cache_backend=cache_backend,
        cache_dsn=source.get("VTB_BENCH_CACHE_DSN", "").strip(),
        storage_backend=storage_backend,
        storage_endpoint=source.get("VTB_BENCH_STORAGE_ENDPOINT", "").strip(),
        storage_bucket=source.get("VTB_BENCH_STORAGE_BUCKET", "vertebra-bench").strip() or "vertebra-bench",
        storage_root=source.get("VTB_BENCH_STORAGE_ROOT", "").strip(),
        workers=_parse_positive_int(source.get("VTB_BENCH_WORKERS"), 1),
        broker_mode=broker_mode,
        total_pool_budget=_parse_positive_int(source.get("VTB_BENCH_TOTAL_POOL_BUDGET"), 40),
        strict_free_threading=_parse_bool(source.get("VERTEBRA_STRICT_FREE_THREADING")),
    )


def resolve_pool_budget(workers: int, broker_mode: str, total_budget: int = 40) -> int:
    """Compute per-process connection pool size preserving total cluster budget parity.

    Args:
        workers: Number of worker processes.
        broker_mode: Pooling topology (``"shm_broker"``, ``"per_worker"``, or ``"none"``).
        total_budget: Total target connection budget across all workers.

    Returns:
        Connection pool size for the current process.
    """
    budget = max(1, total_budget)
    if broker_mode.strip().lower() == "shm_broker":
        return budget
    return max(1, budget // max(1, workers))


def clamp_query_count(raw: str | int | None, *, default: int = 1, maximum: int = 500) -> int:
    """Parse a TechEmpower query count parameter and clamp it to ``[1, maximum]``.

    Args:
        raw: Raw query parameter value from HTTP request or caller.
        default: Fallback value when ``raw`` is missing or invalid.
        maximum: Inclusive upper bound for the returned count.

    Returns:
        Clamped integer within ``[1, maximum]``.
    """
    upper = max(1, maximum)
    fallback = min(max(1, default), upper)
    if raw is None or isinstance(raw, bool):
        return fallback
    if isinstance(raw, int):
        parsed = raw
    else:
        try:
            parsed = int(raw.strip())
        except (ValueError, AttributeError):
            return fallback
    if parsed < 1:
        return 1
    if parsed > upper:
        return upper
    return parsed


def assert_free_threaded_runtime(*, strict: bool | None = None) -> bool:
    """Verify whether CPython is running with the GIL disabled.

    Args:
        strict: When True, raise ``RuntimeError`` if the GIL is enabled.
            When None, falls back to ``VERTEBRA_STRICT_FREE_THREADING``.

    Returns:
        True when the GIL is disabled, False otherwise.

    Raises:
        RuntimeError: If strict enforcement is active and the GIL is enabled.
    """
    is_gil_enabled_fn = getattr(sys, "_is_gil_enabled", None)
    gil_enabled = bool(is_gil_enabled_fn()) if callable(is_gil_enabled_fn) else True
    enforce_strict = _parse_bool(os.environ.get("VERTEBRA_STRICT_FREE_THREADING")) if strict is None else strict
    if gil_enabled and enforce_strict:
        msg = (
            "Free-threaded benchmark execution requires CPython with the GIL disabled "
            "(sys._is_gil_enabled() returned True while strict free-threading is enabled)"
        )
        raise RuntimeError(msg)
    return not gil_enabled


class DataBackend:
    """Thread-safe pluggable data, cache, and object storage backend for benchmark apps."""

    __slots__ = (
        "_async_store",
        "_cache",
        "_cache_lock",
        "_db_lock",
        "_memory_rows",
        "_rng",
        "_sqlite_conn",
        "_store_lock",
        "_sync_store",
        "config",
    )

    def __init__(self, config: DataBackendConfig | None = None) -> None:
        """Initialize the benchmark backend from an explicit or environment config."""
        resolved = load_backend_config() if config is None else config
        _validate_storage_backend(resolved.storage_backend)
        self.config: DataBackendConfig = resolved
        self._db_lock = threading.Lock()
        self._cache_lock = threading.Lock()
        self._store_lock = threading.Lock()
        self._rng = random.Random(42)
        self._cache: dict[int, WorldRow] = {}
        self._memory_rows: list[int] = []
        self._sqlite_conn: sqlite3.Connection | None = None
        self._sync_store: SyncObjectStore | None = None
        self._async_store: AsyncObjectStore | None = None
        self._init_database()

    def _init_database(self) -> None:
        """Initialize the configured database store with 10,000 World rows."""
        backend = self.config.db_backend.strip().lower() or "memory"
        if backend == "memory":
            self._memory_rows = [
                ((idx * 1_103_515_245 + 12_345) % WORLD_ROW_COUNT) + 1 for idx in range(WORLD_ROW_COUNT)
            ]
            return
        if backend == "sqlite":
            dsn = self.config.db_dsn.strip()
            if not dsn or dsn == ":memory:":
                db_target = ":memory:"
            elif dsn.startswith("sqlite:///"):
                db_target = dsn.removeprefix("sqlite:///")
            else:
                db_target = dsn
            conn = sqlite3.connect(db_target, check_same_thread=False)
            conn.execute("CREATE TABLE IF NOT EXISTS world (id INTEGER PRIMARY KEY, randomnumber INTEGER NOT NULL)")
            cursor = conn.execute("SELECT COUNT(*) FROM world")
            existing = int(cursor.fetchone()[0])
            if existing < WORLD_ROW_COUNT:
                rows = [
                    (idx, ((idx * 1_103_515_245 + 12_345) % WORLD_ROW_COUNT) + 1)
                    for idx in range(1, WORLD_ROW_COUNT + 1)
                ]
                conn.executemany("INSERT OR REPLACE INTO world (id, randomnumber) VALUES (?, ?)", rows)
                conn.commit()
            self._sqlite_conn = conn
            return
        msg = f"Unsupported db_backend: {self.config.db_backend!r}"
        raise ValueError(msg)

    @property
    def pool_size(self) -> int:
        """Return the resolved per-process connection pool size."""
        return resolve_pool_budget(
            self.config.workers,
            self.config.broker_mode,
            self.config.total_pool_budget,
        )

    def build_storage_config(self) -> StorageConfig:
        """Build the Vertebra ``StorageConfig`` matching ``self.config.storage_backend``.

        Returns:
            Provider-specific ``StorageConfig`` struct.

        Raises:
            ValueError: If the configured storage backend is forbidden or unsupported.
        """
        backend = _validate_storage_backend(self.config.storage_backend)
        bucket = self.config.storage_bucket or "vertebra-bench"
        endpoint = self.config.storage_endpoint
        access_key = os.environ.get("AWS_ACCESS_KEY_ID", "test-token-placeholder")
        secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "test-token-placeholder")

        if backend == "memory":
            return MemoryConfig(namespace=bucket)
        if backend == "local":
            root = self.config.storage_root or str(Path(tempfile.gettempdir()) / "vertebra-bench-storage")
            return LocalConfig(root=root, create_dirs=True)
        if backend == "rustfs":
            return S3Config(
                bucket=bucket,
                region="us-east-1",
                endpoint=endpoint or "http://127.0.0.1:9000",
                access_key_id=access_key,
                secret_access_key=secret_key,
                allow_http=True,
            )
        if backend in {"floci_s3", "floci_aws"}:
            return S3Config(
                bucket=bucket,
                region="us-east-1",
                endpoint=endpoint or "http://127.0.0.1:4566",
                access_key_id=access_key,
                secret_access_key=secret_key,
                allow_http=True,
            )
        if backend == "floci_gcs":
            return GCSConfig(
                bucket=bucket,
                endpoint=endpoint or "http://127.0.0.1:4566",
                allow_http=True,
                skip_signature=True,
            )
        if backend == "floci_oci":
            return OCIConfig(
                bucket=bucket,
                namespace="vertebra",
                endpoint=endpoint or "http://127.0.0.1:4566",
            )
        if backend in {"r2", "floci_r2"}:
            return R2Config(
                bucket=bucket,
                account_id="0" * 32,
                endpoint=endpoint or "http://127.0.0.1:4566",
                access_key_id=access_key,
                secret_access_key=secret_key,
                allow_http=True,
            )
        if backend == "azurite":
            azure_key = os.environ.get("AZURE_STORAGE_KEY", "test-token-placeholder")
            return AzureConfig(
                container=bucket,
                account="devstoreaccount1",
                access_key=azure_key,
                endpoint=endpoint or None,
                use_emulator=not bool(endpoint),
            )
        msg = f"Unsupported storage backend: {backend!r}"
        raise ValueError(msg)

    def _ensure_stores(self) -> tuple[SyncObjectStore, AsyncObjectStore]:
        """Lazily initialize paired sync and async object stores sharing one backend."""
        with self._store_lock:
            if self._sync_store is None:
                self._sync_store = SyncObjectStore(self.build_storage_config())
                self._async_store = self._sync_store.to_async()
            assert self._async_store is not None
            return self._sync_store, self._async_store

    def _next_world_id(self) -> int:
        """Select a random World row ID in ``[1, WORLD_ROW_COUNT]``."""
        return self._rng.randint(1, WORLD_ROW_COUNT)

    def _fetch_world_locked(self, row_id: int) -> WorldRow:
        """Read a single World row while holding ``self._db_lock``."""
        if self._sqlite_conn is not None:
            cursor = self._sqlite_conn.execute(
                "SELECT id, randomnumber FROM world WHERE id = ?",
                (row_id,),
            )
            found = cursor.fetchone()
            if found is None:
                return WorldRow(id=row_id, randomNumber=1)
            return WorldRow(id=int(found[0]), randomNumber=int(found[1]))
        return WorldRow(id=row_id, randomNumber=self._memory_rows[row_id - 1])

    def _update_world_locked(self, row_id: int, new_value: int) -> WorldRow:
        """Update a single World row while holding ``self._db_lock``."""
        if self._sqlite_conn is not None:
            self._sqlite_conn.execute(
                "UPDATE world SET randomnumber = ? WHERE id = ?",
                (new_value, row_id),
            )
            return WorldRow(id=row_id, randomNumber=new_value)
        self._memory_rows[row_id - 1] = new_value
        return WorldRow(id=row_id, randomNumber=new_value)

    def get_world_sync(self) -> WorldRow:
        """Synchronously fetch a single random World row."""
        with self._db_lock:
            row_id = self._next_world_id()
            return self._fetch_world_locked(row_id)

    def get_worlds_sync(self, count: int) -> list[WorldRow]:
        """Synchronously fetch ``count`` random World rows."""
        resolved_count = clamp_query_count(count)
        with self._db_lock:
            return [self._fetch_world_locked(self._next_world_id()) for _ in range(resolved_count)]

    def update_worlds_sync(self, count: int) -> list[WorldRow]:
        """Synchronously read and update ``count`` random World rows."""
        resolved_count = clamp_query_count(count)
        updated: list[WorldRow] = []
        with self._db_lock:
            for _ in range(resolved_count):
                row_id = self._next_world_id()
                _ = self._fetch_world_locked(row_id)
                new_value = self._rng.randint(1, WORLD_ROW_COUNT)
                updated.append(self._update_world_locked(row_id, new_value))
            if self._sqlite_conn is not None:
                self._sqlite_conn.commit()
        with self._cache_lock:
            for row in updated:
                if row.id in self._cache:
                    self._cache[row.id] = row
        return updated

    def get_cached_worlds_sync(self, count: int) -> list[WorldRow]:
        """Synchronously fetch ``count`` World rows using the process-local cache."""
        resolved_count = clamp_query_count(count)
        results: list[WorldRow] = []
        for _ in range(resolved_count):
            with self._db_lock:
                row_id = self._next_world_id()
            with self._cache_lock:
                cached = self._cache.get(row_id)
            if cached is not None:
                results.append(cached)
                continue
            with self._db_lock:
                fetched = self._fetch_world_locked(row_id)
            with self._cache_lock:
                self._cache[row_id] = fetched
            results.append(fetched)
        return results

    def get_object_sync(self, key: str) -> bytes | None:
        """Synchronously read an object payload by key, returning None when absent."""
        sync_store, _ = self._ensure_stores()
        try:
            return sync_store.get(key)
        except (FileNotFoundError, KeyError):
            return None

    def put_object_sync(self, key: str, data: bytes) -> int:
        """Synchronously store an object payload and return the written byte count."""
        sync_store, _ = self._ensure_stores()
        result = sync_store.put(key, data)
        return int(result.size)

    async def get_world(self) -> WorldRow:
        """Asynchronously fetch a single random World row."""
        return self.get_world_sync()

    async def get_worlds(self, count: int) -> list[WorldRow]:
        """Asynchronously fetch ``count`` random World rows."""
        return self.get_worlds_sync(count)

    async def update_worlds(self, count: int) -> list[WorldRow]:
        """Asynchronously read and update ``count`` random World rows."""
        return self.update_worlds_sync(count)

    async def get_cached_worlds(self, count: int) -> list[WorldRow]:
        """Asynchronously fetch ``count`` World rows using the cache."""
        return self.get_cached_worlds_sync(count)

    async def get_object(self, key: str) -> bytes | None:
        """Asynchronously read an object payload by key, returning None when absent."""
        _, async_store = self._ensure_stores()
        try:
            return await async_store.get(key)
        except (FileNotFoundError, KeyError):
            return None

    async def put_object(self, key: str, data: bytes) -> int:
        """Asynchronously store an object payload and return the written byte count."""
        _, async_store = self._ensure_stores()
        result = await async_store.put(key, data)
        return int(result.size)

    def close(self) -> None:
        """Release any open SQLite or object storage resources."""
        with self._db_lock:
            if self._sqlite_conn is not None:
                self._sqlite_conn.close()
                self._sqlite_conn = None
        with self._store_lock:
            if self._async_store is not None:
                self._async_store.close()
                self._async_store = None
            if self._sync_store is not None:
                self._sync_store.close()
                self._sync_store = None


_DEFAULT_BACKEND_LOCK = threading.Lock()
_DEFAULT_BACKEND: DataBackend | None = None


def get_default_backend() -> DataBackend:
    """Return the lazy process-local default ``DataBackend`` singleton."""
    global _DEFAULT_BACKEND
    with _DEFAULT_BACKEND_LOCK:
        if _DEFAULT_BACKEND is None:
            _DEFAULT_BACKEND = DataBackend(load_backend_config())
        return _DEFAULT_BACKEND
