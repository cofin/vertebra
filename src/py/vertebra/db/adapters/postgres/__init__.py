"""PostgreSQL driver, PgBouncer/PgCat/pgdog pooler config, and zero-copy JSONB passthrough."""

from __future__ import annotations

from typing import TYPE_CHECKING

from vertebra.utils.module_loader import lazy_exports

if TYPE_CHECKING:
    from vertebra.db.adapters.postgres.config import PoolerMode, PostgreSQLConfig
    from vertebra.db.adapters.postgres.core import (
        PGCOPY_BINARY_SIGNATURE,
        PostgreSQLCore,
        decode_pgcopy_rows,
        encode_pg_jsonb,
        encode_pgcopy_rows,
        strip_pg_jsonb_header,
    )
    from vertebra.db.adapters.postgres.driver import AsyncPostgreSQLDriver, SyncPostgreSQLDriver
    from vertebra.db.adapters.postgres.embeddings import PostgresEmbeddingPushdown
    from vertebra.db.adapters.postgres.notify import PostgresNotificationSource, PostgresNotifyConfig
    from vertebra.db.arrow import (
        ARROW_SHM_THRESHOLD_ROWS,
        ArrowCursor,
        _hydrate_rows_with_schema,
        _table_to_rows_and_cols,
        read_arrow_shm,
        write_arrow_shm,
    )

__all__ = (
    "PostgresEmbeddingPushdown",
    "ARROW_SHM_THRESHOLD_ROWS",
    "PGCOPY_BINARY_SIGNATURE",
    "ArrowCursor",
    "AsyncPostgreSQLDriver",
    "PoolerMode",
    "PostgreSQLConfig",
    "PostgreSQLCore",
    "PostgresNotificationSource",
    "PostgresNotifyConfig",
    "SyncPostgreSQLDriver",
    "_hydrate_rows_with_schema",
    "_table_to_rows_and_cols",
    "decode_pgcopy_rows",
    "encode_pg_jsonb",
    "encode_pgcopy_rows",
    "read_arrow_shm",
    "strip_pg_jsonb_header",
    "write_arrow_shm",
)

__lazy_modules__ = [
    "vertebra.db.adapters.postgres.embeddings",
    "vertebra.db.adapters.postgres.config",
    "vertebra.db.adapters.postgres.core",
    "vertebra.db.adapters.postgres.driver",
    "vertebra.db.adapters.postgres.notify",
    "vertebra.db.arrow",
]

_EXPORT_MAP: dict[str, tuple[str, str]] = {
    "PostgresEmbeddingPushdown": ("vertebra.db.adapters.postgres.embeddings", "PostgresEmbeddingPushdown"),
    "ARROW_SHM_THRESHOLD_ROWS": ("vertebra.db.arrow", "ARROW_SHM_THRESHOLD_ROWS"),
    "PGCOPY_BINARY_SIGNATURE": ("vertebra.db.adapters.postgres.core", "PGCOPY_BINARY_SIGNATURE"),
    "ArrowCursor": ("vertebra.db.arrow", "ArrowCursor"),
    "AsyncPostgreSQLDriver": ("vertebra.db.adapters.postgres.driver", "AsyncPostgreSQLDriver"),
    "PoolerMode": ("vertebra.db.adapters.postgres.config", "PoolerMode"),
    "PostgreSQLConfig": ("vertebra.db.adapters.postgres.config", "PostgreSQLConfig"),
    "PostgreSQLCore": ("vertebra.db.adapters.postgres.core", "PostgreSQLCore"),
    "PostgresNotificationSource": ("vertebra.db.adapters.postgres.notify", "PostgresNotificationSource"),
    "PostgresNotifyConfig": ("vertebra.db.adapters.postgres.notify", "PostgresNotifyConfig"),
    "SyncPostgreSQLDriver": ("vertebra.db.adapters.postgres.driver", "SyncPostgreSQLDriver"),
    "_hydrate_rows_with_schema": ("vertebra.db.arrow", "_hydrate_rows_with_schema"),
    "_table_to_rows_and_cols": ("vertebra.db.arrow", "_table_to_rows_and_cols"),
    "decode_pgcopy_rows": ("vertebra.db.adapters.postgres.core", "decode_pgcopy_rows"),
    "encode_pg_jsonb": ("vertebra.db.adapters.postgres.core", "encode_pg_jsonb"),
    "encode_pgcopy_rows": ("vertebra.db.adapters.postgres.core", "encode_pgcopy_rows"),
    "read_arrow_shm": ("vertebra.db.arrow", "read_arrow_shm"),
    "strip_pg_jsonb_header": ("vertebra.db.adapters.postgres.core", "strip_pg_jsonb_header"),
    "write_arrow_shm": ("vertebra.db.arrow", "write_arrow_shm"),
}

__getattr__, __dir__ = lazy_exports(__name__, __all__, _EXPORT_MAP)
