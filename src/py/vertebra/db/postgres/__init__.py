"""PostgreSQL driver, PgBouncer/PgCat/pgdog pooler config, and zero-copy JSONB passthrough."""

from __future__ import annotations

from vertebra.db._arrow import (
    ARROW_SHM_THRESHOLD_ROWS,
    ArrowCursor,
    _hydrate_rows_with_schema,
    _table_to_rows_and_cols,
    read_arrow_shm,
    write_arrow_shm,
)
from vertebra.db.driver import _normalize_driver_params
from vertebra.db.postgres.config import PoolerMode, PostgreSQLConfig
from vertebra.db.postgres.core import (
    PGCOPY_BINARY_SIGNATURE,
    PostgreSQLCore,
    decode_pgcopy_rows,
    encode_pg_jsonb,
    encode_pgcopy_rows,
    strip_pg_jsonb_header,
)
from vertebra.db.postgres.driver import AsyncPostgreSQLDriver, SyncPostgreSQLDriver

__all__ = (
    "ARROW_SHM_THRESHOLD_ROWS",
    "PGCOPY_BINARY_SIGNATURE",
    "ArrowCursor",
    "AsyncPostgreSQLDriver",
    "PoolerMode",
    "PostgreSQLConfig",
    "PostgreSQLCore",
    "SyncPostgreSQLDriver",
    "_hydrate_rows_with_schema",
    "_normalize_driver_params",
    "_table_to_rows_and_cols",
    "decode_pgcopy_rows",
    "encode_pg_jsonb",
    "encode_pgcopy_rows",
    "read_arrow_shm",
    "strip_pg_jsonb_header",
    "write_arrow_shm",
)
