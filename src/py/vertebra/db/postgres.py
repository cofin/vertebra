"""PostgreSQL driver, PgBouncer/PgCat/pgdog pooler config, and zero-copy JSONB passthrough."""

from __future__ import annotations

import contextlib
import inspect
import mmap
import struct
from collections.abc import Awaitable, Buffer, Callable, Iterator, Mapping, Sequence
from typing import Any, Literal

import msgspec

from vertebra.db.dialer.alloydb import AlloyDBConfig
from vertebra.db.dialer.cloudsql import CloudSQLConfig
from vertebra.typing import FrozenBaseStruct, ImproperConfigurationError
from vertebra.utils.module_loader import ensure_module, import_optional
from vertebra.utils.serializers import (
    ZeroCopyCodecRegistry,
    decode_json,
    encode_json_bytes,
)

__all__ = (
    "ARROW_SHM_THRESHOLD_ROWS",
    "ArrowCursor",
    "AsyncPostgreSQLDriver",
    "PGCOPY_BINARY_SIGNATURE",
    "PoolerMode",
    "PostgreSQLConfig",
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

PGCOPY_BINARY_SIGNATURE: bytes = b"PGCOPY\n\xff\r\n\x00"
ARROW_SHM_THRESHOLD_ROWS: int = 256

type PoolerMode = Literal["direct", "transaction", "session", "statement", "auto"]


class ArrowCursor:
    """Zero-copy shared-memory Arrow IPC cursor for bulk analytical result sets."""

    __slots__ = ("_buffer", "_columns", "_mmap", "_rows", "_shm_cursor", "_used_shm")

    def __init__(
        self,
        buffer: bytes | bytearray | memoryview,
        *,
        rows: Sequence[tuple[Any, ...]] = (),
        columns: Sequence[str] = (),
        used_shm: bool = False,
        mmap_obj: mmap.mmap | None = None,
        shm_cursor: Any = None,
    ) -> None:
        self._buffer = buffer
        self._rows = list(rows)
        self._columns = tuple(columns)
        self._used_shm = used_shm
        self._mmap = mmap_obj
        self._shm_cursor = shm_cursor

    def __len__(self) -> int:
        """Return the number of rows in the cursor."""
        return len(self._rows)

    @property
    def num_rows(self) -> int:
        """Return the number of rows in the cursor."""
        return len(self._rows)

    @property
    def row_count(self) -> int:
        """Return the number of rows in the cursor."""
        return len(self._rows)

    @property
    def used_shm(self) -> bool:
        """Return True when the payload was transferred over a shared-memory region."""
        return self._used_shm

    @property
    def shm_region(self) -> Any:
        """Return the underlying shared-memory region or mmap object when SHM transfer is active."""
        if self._shm_cursor is not None:
            return self._shm_cursor
        if self._mmap is not None:
            return self._mmap
        return self._buffer if self._used_shm else None

    def buffer_view(self) -> memoryview:
        """Return a zero-copy memoryview over the serialized Arrow IPC stream."""
        if self._mmap is not None:
            return memoryview(self._mmap)[: len(self._buffer)]
        return memoryview(self._buffer)

    def to_arrow_table(self) -> Any:
        """Deserialize the Arrow IPC buffer into a pyarrow.Table with zero copy."""
        pa = import_optional("pyarrow")
        if pa is not None and len(self._buffer) > 0:
            with contextlib.suppress(TypeError, ValueError, OSError):
                reader = pa.ipc.open_stream(pa.py_buffer(self.buffer_view()))
                return reader.read_all()
        if pa is not None:
            cols = self._columns or tuple(f"col_{i}" for i in range(len(self._rows[0]) if self._rows else 0))
            pydict = {col: [r[idx] if idx < len(r) else None for r in self._rows] for idx, col in enumerate(cols)}
            return pa.Table.from_pydict(pydict)
        return self

    def to_pylist(self) -> list[dict[str, Any]]:
        """Convert the buffered rows into a list of dictionaries."""
        pa = import_optional("pyarrow")
        if pa is not None and len(self._buffer) > 0 and self._rows:
            tbl = self.to_arrow_table()
            if hasattr(tbl, "to_pylist"):
                res: list[dict[str, Any]] = tbl.to_pylist()
                return res
        if not self._rows and len(self._buffer) > 0:
            with contextlib.suppress(TypeError, ValueError):
                decoded = decode_json(bytes(self.buffer_view()))
                if isinstance(decoded, list):
                    return decoded
        cols = self._columns or tuple(f"col_{i}" for i in range(len(self._rows[0]) if self._rows else 0))
        return [{col: (r[idx] if idx < len(r) else None) for idx, col in enumerate(cols)} for r in self._rows]

    def close(self) -> None:
        """Release any underlying anonymous mmap region."""
        if self._mmap is not None:
            self._mmap.close()
            self._mmap = None

    @classmethod
    def from_rows(
        cls,
        rows: Sequence[Any],
        columns: Sequence[str] | None = None,
        *,
        threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS,
    ) -> ArrowCursor:
        """Build an ArrowCursor from tabular rows, using SHM Arrow IPC when threshold is met."""
        if rows and isinstance(rows[0], Mapping):
            dict_rows = [dict(r) for r in rows]
            cols = tuple(columns) if columns is not None else tuple(dict_rows[0].keys())
            tuple_rows = [tuple(d.get(c) for c in cols) for d in dict_rows]
        else:
            tuple_rows = [tuple(r) for r in rows]
            if columns is not None:
                cols = tuple(columns)
            elif tuple_rows:
                cols = tuple(f"col_{i}" for i in range(len(tuple_rows[0])))
            else:
                cols = ()

        pa = import_optional("pyarrow")
        ipc_bytes = b""
        if pa is not None:
            pydict: dict[str, list[Any]] = {
                col: [r[idx] if idx < len(r) else None for r in tuple_rows] for idx, col in enumerate(cols)
            }
            table = pa.Table.from_pydict(pydict)
            sink = pa.BufferOutputStream()
            with pa.ipc.new_stream(sink, table.schema) as writer:
                writer.write_table(table)
            ipc_bytes = sink.getvalue().to_pybytes()
        else:
            dict_list = [{col: (r[idx] if idx < len(r) else None) for idx, col in enumerate(cols)} for r in tuple_rows]
            ipc_bytes = encode_json_bytes(dict_list)

        use_shm = len(tuple_rows) >= threshold_rows or len(ipc_bytes) >= 65_536
        mmap_region: mmap.mmap | None = None
        shm_cursor_obj: Any = None
        if use_shm and len(ipc_bytes) > 0:
            native_mod = import_optional("vertebra._vertebra")
            native_cursor_cls = getattr(native_mod, "NativeShmCursor", None) if native_mod is not None else None
            if native_cursor_cls is not None:
                with contextlib.suppress(TypeError, ValueError, OSError, RuntimeError):
                    shm_cursor_obj = native_cursor_cls(ipc_bytes)
            mmap_region = mmap.mmap(-1, max(4096, len(ipc_bytes)))
            mmap_region[: len(ipc_bytes)] = ipc_bytes

        return cls(
            ipc_bytes,
            rows=tuple_rows,
            columns=cols,
            used_shm=use_shm,
            mmap_obj=mmap_region,
            shm_cursor=shm_cursor_obj,
        )


def write_arrow_shm(table: Any) -> memoryview:
    """Serialize a PyArrow Table, RecordBatch, or row sequence into a shared-memory Arrow IPC buffer."""
    native_mod = import_optional("vertebra._vertebra")
    if native_mod is not None:
        native_fn = getattr(native_mod, "write_arrow_shm", None)
        if callable(native_fn):
            with contextlib.suppress(TypeError, ValueError, OSError, RuntimeError):
                raw_res = native_fn(table)
                if isinstance(raw_res, (bytes, bytearray, memoryview)):
                    return memoryview(raw_res)

    if isinstance(table, (bytes, bytearray, memoryview)):
        payload = bytes(table)
    else:
        pa = import_optional("pyarrow")
        if pa is not None and hasattr(table, "schema"):
            sink = pa.BufferOutputStream()
            with pa.ipc.new_stream(sink, table.schema) as writer:
                if hasattr(table, "to_batches"):
                    writer.write_table(table)
                else:
                    writer.write_batch(table)
            payload = sink.getvalue().to_pybytes()
        elif isinstance(table, ArrowCursor):
            return table.buffer_view()
        elif isinstance(table, Sequence) and not isinstance(table, str):
            cur = ArrowCursor.from_rows(table, threshold_rows=1)
            return cur.buffer_view()
        else:
            payload = encode_json_bytes(table)

    region = mmap.mmap(-1, max(4096, len(payload)))
    region[: len(payload)] = payload
    return memoryview(region)[: len(payload)]


def read_arrow_shm(region: Any, length: int | None = None) -> ArrowCursor:
    """Wrap a shared-memory Arrow IPC buffer or memoryview in a zero-copy `ArrowCursor`."""
    if isinstance(region, ArrowCursor):
        return region
    raw_view = memoryview(region)
    if length is not None:
        raw_view = raw_view[:length]
    raw_bytes = bytes(raw_view)
    rows: list[tuple[Any, ...]] = []
    cols: tuple[str, ...] = ()
    pa = import_optional("pyarrow")
    if pa is not None and len(raw_bytes) > 0:
        with contextlib.suppress(TypeError, ValueError, OSError):
            reader = pa.ipc.open_stream(pa.py_buffer(raw_view))
            tbl = reader.read_all()
            pylist = tbl.to_pylist()
            if pylist:
                cols = tuple(pylist[0].keys())
                rows = [tuple(d.get(c) for c in cols) for d in pylist]
    if not rows and len(raw_bytes) > 0:
        with contextlib.suppress(TypeError, ValueError):
            decoded = decode_json(raw_bytes)
            if isinstance(decoded, list) and decoded and isinstance(decoded[0], dict):
                cols = tuple(decoded[0].keys())
                rows = [tuple(d.get(c) for c in cols) for d in decoded]
    return ArrowCursor(raw_view, rows=rows, columns=cols, used_shm=True)


def strip_pg_jsonb_header(payload: Buffer) -> memoryview:
    """Strip the 1-byte PostgreSQL binary JSONB version prefix (`\\x01`) with zero copy."""
    native_mod = import_optional("vertebra._vertebra")
    if native_mod is not None:
        for fn_name in ("strip_pg_jsonb_header", "strip_pg_jsonb_header_native"):
            native_fn = getattr(native_mod, fn_name, None)
            if callable(native_fn):
                res = native_fn(bytes(payload))
                if isinstance(res, (bytes, bytearray, memoryview)):
                    return memoryview(res)

    view = memoryview(payload).cast("B")
    if len(view) == 0:
        msg = "Empty PostgreSQL binary JSONB payload"
        raise ValueError(msg)
    if view[0] != 0x01:
        msg = f"Unsupported PostgreSQL binary JSONB version byte: 0x{view[0]:02x}"
        raise ValueError(msg)
    return view[1:]


def encode_pg_jsonb(value: Any) -> bytes:
    """Encode a Python value or UTF-8 JSON buffer with the 1-byte PostgreSQL `\\x01` JSONB header."""
    native_mod = import_optional("vertebra._vertebra")
    if native_mod is not None:
        for fn_name in ("encode_pg_jsonb", "encode_pg_jsonb_native"):
            native_fn = getattr(native_mod, fn_name, None)
            if callable(native_fn):
                with contextlib.suppress(TypeError, ValueError, RuntimeError):
                    raw_arg = (
                        value
                        if isinstance(value, (bytes, bytearray))
                        else (value.encode("utf-8") if isinstance(value, str) else encode_json_bytes(value))
                    )
                    res = native_fn(raw_arg)
                    if isinstance(res, (bytes, bytearray)):
                        return bytes(res)

    if isinstance(value, (bytes, bytearray, memoryview)):
        raw_bytes = bytes(value)
        if raw_bytes.startswith(b"\x01"):
            return raw_bytes
        return b"\x01" + raw_bytes
    if isinstance(value, str):
        return b"\x01" + value.encode("utf-8")
    return b"\x01" + encode_json_bytes(value)


def encode_pgcopy_rows(
    rows: Sequence[Sequence[Any]],
    flags: int = 0,
    ext_bytes: bytes = b"",
) -> bytes:
    """Encode a sequence of rows into PostgreSQL binary COPY format."""
    normalized_rows: list[list[bytes | None]] = [
        [
            None
            if field is None
            else (
                field.encode("utf-8")
                if isinstance(field, str)
                else (bytes(field) if isinstance(field, (bytes, bytearray, memoryview)) else str(field).encode("utf-8"))
            )
            for field in row
        ]
        for row in rows
    ]
    native_mod = import_optional("vertebra._vertebra")
    if native_mod is not None:
        for fn_name in ("encode_pgcopy_rows", "encode_pgcopy_rows_native"):
            native_fn = getattr(native_mod, fn_name, None)
            if callable(native_fn) and flags == 0 and not ext_bytes:
                with contextlib.suppress(TypeError, ValueError, RuntimeError):
                    res = native_fn(normalized_rows)
                    if isinstance(res, (bytes, bytearray)):
                        return bytes(res)

    buf = bytearray(PGCOPY_BINARY_SIGNATURE)
    buf.extend(struct.pack(">II", flags, len(ext_bytes)))
    buf.extend(ext_bytes)
    for row in normalized_rows:
        buf.extend(struct.pack(">h", len(row)))
        for field in row:
            if field is None:
                buf.extend(struct.pack(">i", -1))
            else:
                buf.extend(struct.pack(">i", len(field)))
                buf.extend(field)
    buf.extend(struct.pack(">h", -1))
    return bytes(buf)


def decode_pgcopy_rows(payload: Buffer) -> list[tuple[bytes | None, ...]]:
    """Decode a PostgreSQL binary COPY stream into row tuples of bytes or None."""
    native_mod = import_optional("vertebra._vertebra")
    if native_mod is not None:
        for fn_name in ("decode_pgcopy_rows", "parse_pgcopy_binary_native", "parse_pgcopy_binary_rows"):
            native_fn = getattr(native_mod, fn_name, None)
            if callable(native_fn):
                with contextlib.suppress(TypeError, ValueError):
                    res = native_fn(bytes(payload))
                    return [tuple(None if f is None else bytes(f) for f in r) for r in res]

    raw = memoryview(payload)
    sig_len = len(PGCOPY_BINARY_SIGNATURE)
    if len(raw) < sig_len or bytes(raw[:sig_len]) != PGCOPY_BINARY_SIGNATURE:
        msg = "Invalid PGCOPY binary signature"
        raise ValueError(msg)

    if len(raw) < sig_len + 8:
        msg = "Truncated PGCOPY header"
        raise ValueError(msg)

    _flags, ext_len = struct.unpack_from(">II", raw, sig_len)
    pos = sig_len + 8 + ext_len
    if pos > len(raw):
        msg = "Corrupted PGCOPY header extension"
        raise ValueError(msg)

    rows: list[tuple[bytes | None, ...]] = []
    while pos + 2 <= len(raw):
        (field_count,) = struct.unpack_from(">h", raw, pos)
        pos += 2
        if field_count == -1:
            return rows
        if field_count < -1:
            msg = f"Invalid PGCOPY field count: {field_count}"
            raise ValueError(msg)

        fields: list[bytes | None] = []
        for _ in range(field_count):
            if pos + 4 > len(raw):
                msg = "Truncated PGCOPY field length"
                raise ValueError(msg)
            (field_len,) = struct.unpack_from(">i", raw, pos)
            pos += 4
            if field_len == -1:
                fields.append(None)
            elif field_len >= 0:
                if pos + field_len > len(raw):
                    msg = "Truncated PGCOPY field data"
                    raise ValueError(msg)
                fields.append(bytes(raw[pos : pos + field_len]))
                pos += field_len
            else:
                msg = f"Invalid PGCOPY field length: {field_len}"
                raise ValueError(msg)
        rows.append(tuple(fields))

    msg = "Unexpected EOF before PGCOPY trailer"
    raise ValueError(msg)


class PostgreSQLConfig(FrozenBaseStruct, frozen=True, gc=False, kw_only=True):
    """PostgreSQL configuration supporting Cloud SQL, AlloyDB, and PgBouncer/PgCat/pgdog poolers."""

    dsn: str = "postgresql://localhost:5432/postgres"
    """Primary PostgreSQL connection URI (may point to a PgBouncer, PgCat, or pgdog pooler)."""
    direct_dsn: str | None = None
    """Optional unpooled direct PostgreSQL URI used for `LISTEN/NOTIFY`, migrations, and DDL."""
    pooler_mode: PoolerMode = "direct"
    """Connection pooler multiplexing mode (`"direct"`, `"transaction"`, `"session"`, `"statement"`, or `"auto"`)."""
    pgbouncer: bool = False
    """When True, configure transaction-mode PgBouncer settings (`statement_cache_size = 0`)."""
    pgbouncer_mode: bool = False
    """Alias for `pgbouncer` matching `sqlspec` driver features."""
    protocol_level_pooler_cache: bool = False
    """When True, retain prepared statements in transaction-mode poolers supporting protocol-level statement caching."""
    max_prepared_statements: int | None = None
    """Maximum per-connection prepared statement LRU capacity (`None` defaults to `256` when safe)."""
    min_size: int = 1
    """Minimum number of pooled connections maintained per worker."""
    max_size: int = 10
    """Maximum number of pooled connections permitted per worker."""
    enable_cloud_sql: bool = False
    """When True, enable zero-sidecar Google Cloud SQL (:3307) mTLS connection dialing."""
    cloud_sql_instance: str | None = None
    """Optional Google Cloud SQL instance connection name (`project:region:instance`) for zero-sidecar port 3307 mTLS."""
    cloud_sql_ip_type: Literal["PUBLIC", "PRIVATE", "PSC"] = "PRIVATE"
    """Preferred Google Cloud SQL IP connectivity mode."""
    enable_alloydb: bool = False
    """When True, enable zero-sidecar Google Cloud AlloyDB (:5433) mTLS and MetadataExchange dialing."""
    alloydb_instance_uri: str | None = None
    """Optional Google Cloud AlloyDB instance URI (`projects/.../instances/...`) for zero-sidecar port 5433 mTLS."""
    alloydb_ip_type: Literal["PUBLIC", "PRIVATE", "PSC"] = "PRIVATE"
    """Preferred Google Cloud AlloyDB IP connectivity mode."""
    enable_iam_auth: bool = False
    """When True, authenticate using short-lived Google Cloud IAM OAuth2 tokens."""
    enable_alloydb_columnar_engine: bool = False
    """When True, enable `google_columnar_engine.enabled = on` in session startup GUCs."""
    application_name: str = "vertebra"
    """PostgreSQL `application_name` GUC reported in `pg_stat_activity`."""
    statement_timeout_ms: int | None = None
    """Optional per-statement execution timeout in milliseconds (`statement_timeout` GUC)."""
    idle_in_transaction_session_timeout_ms: int | None = None
    """Optional timeout in milliseconds before aborting idle open transactions."""
    session_gucs: Mapping[str, str] = msgspec.field(default_factory=dict)
    """Additional PostgreSQL session GUC key-value pairs applied on connection startup."""
    broker: Any = None
    """Optional shared-memory `DatabaseBroker` or `SHMBrokerTransport` for cross-worker connection leasing."""

    def __post_init__(self) -> None:
        if self.min_size < 0 or self.max_size < 1 or self.min_size > self.max_size:
            msg = f"Invalid pool bounds: min_size={self.min_size}, max_size={self.max_size}"
            raise ValueError(msg)
        has_csql = bool(self.enable_cloud_sql or self.cloud_sql_instance)
        has_alloy = bool(self.enable_alloydb or self.alloydb_instance_uri)
        if has_csql and has_alloy:
            raise ImproperConfigurationError("Cannot enable both Cloud SQL and AlloyDB on the same PostgreSQLConfig")
        if self.pgbouncer or self.pgbouncer_mode:
            msgspec.structs.force_setattr(self, "pgbouncer", True)
            msgspec.structs.force_setattr(self, "pgbouncer_mode", True)
            if self.pooler_mode in ("direct", "session", "auto"):
                msgspec.structs.force_setattr(self, "pooler_mode", "transaction")
        msgspec.structs.force_setattr(self, "session_gucs", dict(self.session_gucs))

    @property
    def effective_statement_cache_size(self) -> int:
        """Return the prepared statement cache capacity safe for the configured pooler mode.

        In transaction-mode or statement-mode PgBouncer/PgCat/pgdog, prepared statements must be disabled (`0`)
        unless protocol-level statement caching (`PgBouncer 1.21+` / `PgCat` / `pgdog`) is enabled.
        """
        if self.pooler_mode in ("transaction", "statement") and not self.protocol_level_pooler_cache:
            return 0
        return 256 if self.max_prepared_statements is None else max(0, self.max_prepared_statements)

    @property
    def effective_direct_dsn(self) -> str:
        """Return the direct primary DSN for LISTEN/NOTIFY, logical replication, and migrations."""
        return self.direct_dsn or self.dsn

    def derive_direct_config(self) -> PostgreSQLConfig:
        """Return a session-mode PostgreSQLConfig targeting ``effective_direct_dsn`` (`sqlspec>=0.68.0`)."""
        if not self.direct_dsn:
            return self
        base = msgspec.structs.asdict(self)
        base["dsn"] = self.direct_dsn
        base["direct_dsn"] = None
        base["pooler_mode"] = "session"
        base["pgbouncer"] = False
        base["pgbouncer_mode"] = False
        base["min_size"] = min(1, self.max_size)
        base["max_size"] = min(2, self.max_size)
        base["session_gucs"] = dict(self.session_gucs)
        return PostgreSQLConfig(**base)

    @staticmethod
    def compute_worker_pool_budget(
        max_connections: int,
        worker_count: int = 1,
        *,
        replica_count: int = 1,
        workers_per_replica: int | None = None,
        reserved_admin_connections: int = 5,
    ) -> int:
        """Compute per-worker pool size from a cluster-wide database connection budget."""
        usable = max(0, max_connections - reserved_admin_connections)
        per_replica = workers_per_replica if workers_per_replica is not None else worker_count
        total_shards = max(1, replica_count) * max(1, per_replica)
        return max(1, usable // total_shards)

    def build_session_gucs(self) -> dict[str, str]:
        """Build the startup/session GUC parameters for the PostgreSQL connection."""
        gucs: dict[str, str] = {"application_name": self.application_name}
        if self.enable_alloydb_columnar_engine or self.alloydb_instance_uri is not None:
            gucs["google_columnar_engine.enabled"] = "on"
            gucs["alloydb.enable_columnar_engine"] = "on"
        if self.statement_timeout_ms is not None:
            gucs["statement_timeout"] = str(self.statement_timeout_ms)
        if self.idle_in_transaction_session_timeout_ms is not None:
            gucs["idle_in_transaction_session_timeout"] = str(self.idle_in_transaction_session_timeout_ms)
        gucs.update(self.session_gucs)
        return gucs

    def to_cloud_dialer_config(self) -> CloudSQLConfig | AlloyDBConfig | None:
        """Return the zero-sidecar CloudSQLConfig or AlloyDBConfig when configured."""
        if self.alloydb_instance_uri:
            return AlloyDBConfig(
                instance_uri=self.alloydb_instance_uri,
                enable_iam_auth=self.enable_iam_auth,
                enable_columnar_engine=self.enable_alloydb_columnar_engine or True,
            )
        if self.cloud_sql_instance:
            return CloudSQLConfig(
                instance_connection_name=self.cloud_sql_instance,
                enable_iam_auth=self.enable_iam_auth,
            )
        return None


class SyncPostgreSQLDriver:
    """Synchronous PostgreSQL driver facade supporting plain-tuple, zero-copy JSONB, and COPY streaming."""

    __slots__ = ("_broker", "_closed", "_config", "_dialer", "_executor", "_in_memory_rows", "_pool")

    def __init__(
        self,
        config: PostgreSQLConfig | None = None,
        *,
        pool: Any = None,
        dialer: Any = None,
        broker: Any = None,
        executor: Callable[[str, Sequence[Any]], list[tuple[Any, ...]]] | None = None,
    ) -> None:
        self._config = config or PostgreSQLConfig()
        self._pool = pool
        self._dialer = dialer
        self._broker = broker if broker is not None else self._config.broker
        self._executor = executor
        self._closed = False
        self._in_memory_rows: list[tuple[Any, ...]] = []

    @property
    def config(self) -> PostgreSQLConfig:
        """Return the active PostgreSQLConfig."""
        return self._config

    @property
    def dialer(self) -> Any:
        """Return the attached Cloud SQL or AlloyDB dialer if present."""
        return self._dialer

    @property
    def broker(self) -> Any:
        """Return the attached SHM connection broker transport if present."""
        return self._broker

    @property
    def pool(self) -> Any:
        """Return the underlying connection pool if present."""
        return self._pool

    @property
    def closed(self) -> bool:
        """Return True if the driver has been closed."""
        return self._closed

    def _format_guc_options(self) -> str:
        """Format session GUC options for psycopg connection parameters."""
        gucs = self._config.build_session_gucs()
        return " ".join(f"-c {k}={v}" for k, v in gucs.items())

    def _ensure_pool(self) -> Any:
        """Ensure connection pool is initialized or raise MissingDependencyError."""
        if self._pool is not None:
            return self._pool
        if self._config.dsn.startswith("memory://"):
            return None
        psycopg_pool = ensure_module(
            "psycopg_pool",
            install_package="psycopg[binary,pool]",
            extra="postgres",
        )

        kwargs: dict[str, Any] = {
            "autocommit": False,
            "prepare_threshold": None if self._config.effective_statement_cache_size == 0 else 5,
        }
        guc_opts = self._format_guc_options()
        if guc_opts:
            kwargs["options"] = guc_opts

        self._pool = psycopg_pool.ConnectionPool(
            conninfo=self._config.dsn,
            min_size=self._config.min_size,
            max_size=self._config.max_size,
            kwargs=kwargs,
        )
        return self._pool

    @staticmethod
    def strip_jsonb_header(payload: Buffer) -> memoryview:
        """Strip the 1-byte PostgreSQL binary JSONB version prefix (`\\x01`) with zero copy."""
        return strip_pg_jsonb_header(payload)

    @staticmethod
    def encode_jsonb(value: Any) -> bytes:
        """Encode a Python value with the 1-byte PostgreSQL `\\x01` JSONB version header."""
        return encode_pg_jsonb(value)

    @staticmethod
    def encode_pgcopy_binary_rows(
        rows: Sequence[Sequence[bytes | str | None]],
        flags: int = 0,
        ext_bytes: bytes = b"",
    ) -> bytes:
        """Encode rows into PostgreSQL binary COPY format."""
        return encode_pgcopy_rows(rows, flags=flags, ext_bytes=ext_bytes)

    @staticmethod
    def parse_pgcopy_binary_rows(payload: Buffer) -> list[tuple[bytes | None, ...]]:
        """Parse PostgreSQL binary COPY format stream into row tuples of byte values."""
        return decode_pgcopy_rows(payload)

    def _acquire_broker_lease(self, sql: str) -> Any:
        """Acquire a lease and statement cache plan from the attached broker if present."""
        if self._broker is None:
            return None
        if hasattr(self._broker, "get_or_compile_statement") and self._config.effective_statement_cache_size > 0:
            self._broker.get_or_compile_statement("postgresql", sql, lambda s: (s, ()))
        if hasattr(self._broker, "acquire"):
            return self._broker.acquire(bind_key="postgresql")
        return None

    def execute_with_description(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] = (),
    ) -> tuple[list[tuple[Any, ...]], tuple[tuple[str, Any, None, None, None, None, None], ...] | None]:
        """Execute a SQL statement returning rows and column descriptions."""
        lease = self._acquire_broker_lease(sql)
        try:
            if self._executor is not None:
                seq_params = tuple(parameters) if isinstance(parameters, Sequence) else ()
                exec_rows = [tuple(row) for row in self._executor(sql, seq_params)]
                return exec_rows, None
            if self._pool is not None:
                if hasattr(self._pool, "connection"):
                    conn = self._pool.connection()
                    conn_ctx = conn if hasattr(conn, "__enter__") else None
                    try:
                        active_conn = conn.__enter__() if conn_ctx is not None else conn
                        cur = active_conn.cursor()
                        try:
                            cur.execute(sql, parameters)
                            rows = cur.fetchall() if hasattr(cur, "fetchall") else []
                            desc = (
                                tuple((d[0], d[1], None, None, None, None, None) for d in cur.description)
                                if getattr(cur, "description", None)
                                else None
                            )
                            return [tuple(r) for r in rows], desc
                        finally:
                            if hasattr(cur, "close"):
                                cur.close()
                    finally:
                        if conn_ctx is not None:
                            conn_ctx.__exit__(None, None, None)
                if hasattr(self._pool, "execute_with_description"):
                    res: tuple[
                        list[tuple[Any, ...]],
                        tuple[tuple[str, Any, None, None, None, None, None], ...] | None,
                    ] = self._pool.execute_with_description(sql, parameters)
                    return res
                if hasattr(self._pool, "execute"):
                    exec_res = self._pool.execute(sql, parameters)
                    rows = exec_res.fetchall() if hasattr(exec_res, "fetchall") else list(exec_res)
                    desc = (
                        tuple((d[0], d[1], None, None, None, None, None) for d in exec_res.description)
                        if hasattr(exec_res, "description") and exec_res.description
                        else None
                    )
                    return [tuple(r) for r in rows], desc

            if not self._config.dsn.startswith("memory://"):
                pool = self._ensure_pool()
                if pool is not None and hasattr(pool, "connection"):
                    with pool.connection() as conn, conn.cursor() as cur:
                        cur.execute(sql, parameters)
                        rows = cur.fetchall() if hasattr(cur, "fetchall") else []
                        desc = (
                            tuple((d[0], d[1], None, None, None, None, None) for d in cur.description)
                            if getattr(cur, "description", None)
                            else None
                        )
                        return [tuple(r) for r in rows], desc

            _ = (sql, parameters)
            desc = (
                tuple((f"col_{i}", None, None, None, None, None, None) for i in range(len(self._in_memory_rows[0])))
                if self._in_memory_rows
                else None
            )
            return list(self._in_memory_rows), desc
        finally:
            if lease is not None and hasattr(lease, "release"):
                lease.release()

    def execute_tuples(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] = (),
    ) -> list[tuple[Any, ...]]:
        """Execute a SQL statement and return plain Python tuples."""
        rows, _ = self.execute_with_description(sql, parameters)
        return rows

    def execute(
        self,
        sql: str,
        *params: Any,
        parameters: Sequence[Any] | Mapping[str, Any] = (),
    ) -> list[tuple[Any, ...]]:
        """Execute a SQL statement with positional or sequence parameters."""
        resolved = _normalize_driver_params(params, parameters)
        return self.execute_tuples(sql, resolved)

    def select(
        self,
        sql: str,
        *params: Any,
        parameters: Sequence[Any] | Mapping[str, Any] = (),
        schema_type: type[Any] | None = None,
    ) -> list[Any]:
        """Execute a SELECT query and return rows as dictionaries or hydrated schema_type instances."""
        resolved = _normalize_driver_params(params, parameters)
        rows, desc = self.execute_with_description(sql, resolved)
        if not rows:
            return []
        cols = [d[0] for d in desc] if desc else [f"col_{i}" for i in range(len(rows[0]))]
        return _hydrate_rows_with_schema(rows, cols, schema_type)

    def select_one(
        self,
        sql: str,
        *params: Any,
        parameters: Sequence[Any] | Mapping[str, Any] = (),
        schema_type: type[Any] | None = None,
    ) -> Any:
        """Execute a SELECT query and return the first row or raise ValueError."""
        rows = self.select(sql, *params, parameters=parameters, schema_type=schema_type)
        if not rows:
            msg = "No row found for select_one"
            raise ValueError(msg)
        return rows[0]

    def select_one_or_none(
        self,
        sql: str,
        *params: Any,
        parameters: Sequence[Any] | Mapping[str, Any] = (),
        schema_type: type[Any] | None = None,
    ) -> Any | None:
        """Execute a SELECT query and return the first row or None."""
        rows = self.select(sql, *params, parameters=parameters, schema_type=schema_type)
        return rows[0] if rows else None

    def load_from_arrow(
        self,
        table_name: str,
        data: Any,
        *,
        columns: Sequence[str] | None = None,
    ) -> int:
        """Bulk-load an Arrow Table, RecordBatch, ArrowCursor, or row sequence into table_name."""
        tuple_rows, col_names = _table_to_rows_and_cols(data, columns)
        if not tuple_rows:
            return 0
        placeholders = ", ".join("%s" for _ in col_names)
        cols_sql = ", ".join(col_names)
        sql = f"INSERT INTO {table_name} ({cols_sql}) VALUES ({placeholders})"
        for row in tuple_rows:
            self.execute_tuples(sql, row)
        return len(tuple_rows)

    def select_to_arrow(
        self,
        sql: str,
        *params: Any,
        parameters: Sequence[Any] | Mapping[str, Any] = (),
    ) -> Any:
        """Execute a query and return a PyArrow Table backed by ShmRegion Arrow IPC when >= 256 rows."""
        resolved = _normalize_driver_params(params, parameters)
        rows, desc = self.execute_with_description(sql, resolved)
        cols = [d[0] for d in desc] if desc else [f"col_{i}" for i in range(len(rows[0]) if rows else 0)]
        cursor = ArrowCursor.from_rows(rows, cols, threshold_rows=ARROW_SHM_THRESHOLD_ROWS)
        return cursor.to_arrow_table()

    def write_arrow_shm(self, table: Any) -> memoryview:
        """Write an Arrow Table into a shared-memory Arrow IPC buffer and return a memoryview."""
        return write_arrow_shm(table)

    def stream_arrow_batches(
        self,
        sql: str,
        columns: Sequence[str],
        *,
        batch_size: int = 1024,
    ) -> Iterator[Any]:
        """Execute a query via binary COPY and yield PyArrow RecordBatch or tuple batch chunks."""
        rows: list[tuple[Any, ...]] = []
        if self._pool is not None:
            if hasattr(self._pool, "copy_binary_stream"):
                stream = self._pool.copy_binary_stream(sql)
                rows = self.parse_pgcopy_binary_rows(stream)
            elif hasattr(self._pool, "connection"):
                with self._pool.connection() as conn, conn.cursor() as cur:
                    buf = bytearray()
                    with cur.copy(f"COPY ({sql}) TO STDOUT (FORMAT BINARY)") as copy:
                        for chunk in copy:
                            buf.extend(chunk)
                    rows = self.parse_pgcopy_binary_rows(buf)
            elif hasattr(self._pool, "execute_tuples"):
                rows = self._pool.execute_tuples(sql)
            else:
                rows = list(self._in_memory_rows)
        elif not self._config.dsn.startswith("memory://"):
            pool = self._ensure_pool()
            if pool is not None and hasattr(pool, "connection"):
                with pool.connection() as conn, conn.cursor() as cur:
                    buf = bytearray()
                    with cur.copy(f"COPY ({sql}) TO STDOUT (FORMAT BINARY)") as copy:
                        for chunk in copy:
                            buf.extend(chunk)
                    rows = self.parse_pgcopy_binary_rows(buf)
            else:
                rows = list(self._in_memory_rows)
        else:
            rows = list(self._in_memory_rows)

        pa = import_optional("pyarrow")

        cols = tuple(columns)
        if not rows:
            if pa is not None:
                pydict_empty: dict[str, list[Any]] = {col: [] for col in cols}
                yield pa.RecordBatch.from_pydict(pydict_empty)
            else:
                yield []
            return

        for i in range(0, len(rows), batch_size):
            chunk = rows[i : i + batch_size]
            if pa is not None:
                pydict_chunk: dict[str, list[Any]] = {
                    col: [r[j] if j < len(r) else None for r in chunk] for j, col in enumerate(cols)
                }
                yield pa.RecordBatch.from_pydict(pydict_chunk)
            else:
                yield chunk

    def commit(self) -> None:
        """Commit the active transaction."""
        if self._pool is not None and hasattr(self._pool, "commit"):
            self._pool.commit()

    def rollback(self) -> None:
        """Roll back the active transaction."""
        if self._pool is not None and hasattr(self._pool, "rollback"):
            self._pool.rollback()

    def close(self) -> None:
        """Close the driver and release the connection pool."""
        self._closed = True
        if self._pool is not None and hasattr(self._pool, "close"):
            self._pool.close()

    def decode_binary_jsonb(self, raw: Buffer) -> memoryview:
        """Return a zero-copy UTF-8 JSON memoryview from a binary PostgreSQL JSONB column."""
        return self.strip_jsonb_header(raw)


class AsyncPostgreSQLDriver:
    """Asynchronous PostgreSQL driver facade supporting plain-tuple, zero-copy JSONB, and COPY streaming."""

    __slots__ = ("_async_executor", "_broker", "_closed", "_config", "_dialer", "_pool", "_sync")

    def __init__(
        self,
        config: PostgreSQLConfig | None = None,
        *,
        pool: Any = None,
        dialer: Any = None,
        broker: Any = None,
        executor: Callable[[str, Sequence[Any]], list[tuple[Any, ...]]] | None = None,
        async_executor: Callable[[str, Sequence[Any]], Awaitable[list[tuple[Any, ...]]]] | None = None,
    ) -> None:
        self._config = config or PostgreSQLConfig()
        self._pool = pool
        self._dialer = dialer
        self._broker = broker if broker is not None else self._config.broker
        self._async_executor = async_executor
        self._closed = False
        self._sync = SyncPostgreSQLDriver(
            self._config,
            pool=pool,
            dialer=dialer,
            broker=self._broker,
            executor=executor,
        )

    @property
    def config(self) -> PostgreSQLConfig:
        """Return the active PostgreSQLConfig."""
        return self._config

    @property
    def dialer(self) -> Any:
        """Return the attached Cloud SQL or AlloyDB dialer if present."""
        return self._dialer

    @property
    def broker(self) -> Any:
        """Return the attached SHM connection broker transport if present."""
        return self._broker

    @property
    def pool(self) -> Any:
        """Return the underlying connection pool if present."""
        return self._pool

    @property
    def closed(self) -> bool:
        """Return True if the driver has been closed."""
        return self._closed

    def _ensure_async_pool(self) -> Any:
        """Ensure async connection pool is initialized or raise MissingDependencyError."""
        if self._pool is not None:
            return self._pool
        if self._config.dsn.startswith("memory://"):
            return None
        asyncpg_mod = import_optional("asyncpg")
        if asyncpg_mod is not None:
            return asyncpg_mod
        return ensure_module(
            "psycopg_pool",
            package="asyncpg",
            install_package="asyncpg",
            extra="postgres",
        )

    async def execute_with_description(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] = (),
    ) -> tuple[list[tuple[Any, ...]], tuple[tuple[str, Any, None, None, None, None, None], ...] | None]:
        """Asynchronously execute a SQL statement returning rows and column descriptions."""
        lease: Any = None
        if self._broker is not None:
            if hasattr(self._broker, "get_or_compile_statement") and self._config.effective_statement_cache_size > 0:
                self._broker.get_or_compile_statement("postgresql", sql, lambda s: (s, ()))
            if hasattr(self._broker, "acquire_async"):
                lease = await self._broker.acquire_async(bind_key="postgresql")
            elif hasattr(self._broker, "acquire"):
                lease = self._broker.acquire(bind_key="postgresql")
        try:
            if self._async_executor is not None:
                seq_params = tuple(parameters) if isinstance(parameters, Sequence) else ()
                exec_rows = await self._async_executor(sql, seq_params)
                return [tuple(r) for r in exec_rows], None
            if self._pool is not None:
                if hasattr(self._pool, "acquire"):
                    async with self._pool.acquire() as conn:
                        if hasattr(conn, "fetch"):
                            params = tuple(parameters) if isinstance(parameters, Sequence) else ()
                            records = await conn.fetch(sql, *params)
                            return [tuple(r.values() if hasattr(r, "values") else r) for r in records], None
                        if hasattr(conn, "cursor"):
                            async with conn.cursor() as cur:
                                await cur.execute(sql, parameters)
                                rows = await cur.fetchall() if hasattr(cur, "fetchall") else []
                                desc = (
                                    tuple((d[0], d[1], None, None, None, None, None) for d in cur.description)
                                    if getattr(cur, "description", None)
                                    else None
                                )
                                return [tuple(r) for r in rows], desc
                if hasattr(self._pool, "execute_with_description"):
                    res = self._pool.execute_with_description(sql, parameters)
                    if inspect.iscoroutine(res):
                        awaited_res: tuple[
                            list[tuple[Any, ...]],
                            tuple[tuple[str, Any, None, None, None, None, None], ...] | None,
                        ] = await res
                        return awaited_res
                    typed_res: tuple[
                        list[tuple[Any, ...]],
                        tuple[tuple[str, Any, None, None, None, None, None], ...] | None,
                    ] = res
                    return typed_res
                if hasattr(self._pool, "execute_tuples"):
                    res_tuples = self._pool.execute_tuples(sql, parameters)
                    tuple_rows: list[tuple[Any, ...]]
                    if inspect.iscoroutine(res_tuples):
                        tuple_rows = await res_tuples
                    else:
                        tuple_rows = res_tuples
                    return tuple_rows, None

            if not self._config.dsn.startswith("memory://") and self._sync._executor is None:
                self._ensure_async_pool()

            return self._sync.execute_with_description(sql, parameters)
        finally:
            if lease is not None and hasattr(lease, "release"):
                lease.release()

    async def execute_tuples(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] = (),
    ) -> list[tuple[Any, ...]]:
        """Asynchronously execute a SQL statement and return plain Python tuples."""
        rows, _ = await self.execute_with_description(sql, parameters)
        return rows

    async def execute(
        self,
        sql: str,
        *params: Any,
        parameters: Sequence[Any] | Mapping[str, Any] = (),
    ) -> list[tuple[Any, ...]]:
        """Asynchronously execute a SQL statement."""
        resolved = _normalize_driver_params(params, parameters)
        return await self.execute_tuples(sql, resolved)

    async def select(
        self,
        sql: str,
        *params: Any,
        parameters: Sequence[Any] | Mapping[str, Any] = (),
        schema_type: type[Any] | None = None,
    ) -> list[Any]:
        """Asynchronously execute a SELECT query and return rows as dictionaries or hydrated schema_type instances."""
        resolved = _normalize_driver_params(params, parameters)
        rows, desc = await self.execute_with_description(sql, resolved)
        if not rows:
            return []
        cols = [d[0] for d in desc] if desc else [f"col_{i}" for i in range(len(rows[0]))]
        return _hydrate_rows_with_schema(rows, cols, schema_type)

    async def select_one(
        self,
        sql: str,
        *params: Any,
        parameters: Sequence[Any] | Mapping[str, Any] = (),
        schema_type: type[Any] | None = None,
    ) -> Any:
        """Asynchronously execute a SELECT query and return the first row or raise ValueError."""
        rows = await self.select(sql, *params, parameters=parameters, schema_type=schema_type)
        if not rows:
            msg = "No row found for select_one"
            raise ValueError(msg)
        return rows[0]

    async def select_one_or_none(
        self,
        sql: str,
        *params: Any,
        parameters: Sequence[Any] | Mapping[str, Any] = (),
        schema_type: type[Any] | None = None,
    ) -> Any | None:
        """Asynchronously execute a SELECT query and return the first row or None."""
        rows = await self.select(sql, *params, parameters=parameters, schema_type=schema_type)
        return rows[0] if rows else None

    async def load_from_arrow(
        self,
        table_name: str,
        data: Any,
        *,
        columns: Sequence[str] | None = None,
    ) -> int:
        """Asynchronously bulk-load an Arrow Table, RecordBatch, ArrowCursor, or row sequence into table_name."""
        tuple_rows, col_names = _table_to_rows_and_cols(data, columns)
        if not tuple_rows:
            return 0
        placeholders = ", ".join("%s" for _ in col_names)
        cols_sql = ", ".join(col_names)
        sql = f"INSERT INTO {table_name} ({cols_sql}) VALUES ({placeholders})"
        for row in tuple_rows:
            await self.execute_tuples(sql, row)
        return len(tuple_rows)

    async def select_to_arrow(
        self,
        sql: str,
        *params: Any,
        parameters: Sequence[Any] | Mapping[str, Any] = (),
    ) -> Any:
        """Asynchronously execute a query and return a PyArrow Table over ShmRegion Arrow IPC."""
        resolved = _normalize_driver_params(params, parameters)
        rows, desc = await self.execute_with_description(sql, resolved)
        cols = [d[0] for d in desc] if desc else [f"col_{i}" for i in range(len(rows[0]) if rows else 0)]
        cursor = ArrowCursor.from_rows(rows, cols, threshold_rows=ARROW_SHM_THRESHOLD_ROWS)
        return cursor.to_arrow_table()

    def write_arrow_shm(self, table: Any) -> memoryview:
        """Write an Arrow Table into a shared-memory Arrow IPC buffer and return a memoryview."""
        return write_arrow_shm(table)

    async def stream_arrow_batches(
        self,
        sql: str,
        columns: Sequence[str],
        *,
        batch_size: int = 1024,
    ) -> Iterator[Any]:
        """Asynchronously execute a query via binary COPY and yield batch chunks."""
        return self._sync.stream_arrow_batches(sql, columns, batch_size=batch_size)

    async def commit(self) -> None:
        """Commit the active transaction."""
        self._sync.commit()

    async def rollback(self) -> None:
        """Roll back the active transaction."""
        self._sync.rollback()

    async def close(self) -> None:
        """Close the driver and underlying resources."""
        self._closed = True
        if self._pool is not None and hasattr(self._pool, "close"):
            res = self._pool.close()
            if inspect.iscoroutine(res):
                await res
        self._sync.close()

    def decode_binary_jsonb(self, raw: Buffer) -> memoryview:
        """Return a zero-copy UTF-8 JSON memoryview from a binary PostgreSQL JSONB column."""
        return SyncPostgreSQLDriver.strip_jsonb_header(raw)


def _normalize_driver_params(
    params: tuple[Any, ...],
    parameters: Sequence[Any] | Mapping[str, Any],
) -> Sequence[Any] | Mapping[str, Any]:
    """Normalize variadic positional parameters and explicit parameters argument."""
    if parameters:
        return parameters
    if len(params) == 1:
        if params[0] is None:
            return ()
        if isinstance(params[0], (tuple, list, dict)):
            return params[0]
    return params


def _hydrate_rows_with_schema(
    rows: Sequence[Any],
    columns: Sequence[str],
    schema_type: type[Any] | None = None,
) -> list[Any]:
    """Hydrate tabular rows into dictionaries or typed schema_type instances via C-API vectorcall."""
    if not rows:
        return []
    if schema_type is None or schema_type is dict:
        if isinstance(rows[0], Mapping):
            return [dict(r) for r in rows]
        return [{col: (r[i] if i < len(r) else None) for i, col in enumerate(columns)} for r in rows]
    plan = ZeroCopyCodecRegistry.compile(schema_type)
    if isinstance(rows[0], Mapping):
        col_tuple = tuple(columns) if columns else tuple(rows[0].keys())
        tuple_rows = [tuple(r.get(c) for c in col_tuple) for r in rows]
        return plan.hydrate_rows(tuple_rows, col_tuple)
    return plan.hydrate_rows(rows, columns)


def _table_to_rows_and_cols(
    data: Any,
    columns: Sequence[str] | None = None,
) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
    """Extract tuple rows and column names from a PyArrow Table, ArrowCursor, Polars DataFrame, or row sequence."""
    if isinstance(data, ArrowCursor) or hasattr(data, "to_pylist") and callable(data.to_pylist):
        pylist = data.to_pylist()
    elif hasattr(data, "to_dicts") and callable(data.to_dicts):
        pylist = data.to_dicts()
    elif isinstance(data, Sequence) and not isinstance(data, (str, bytes, bytearray)):
        if not data:
            return [], tuple(columns or ())
        if isinstance(data[0], Mapping):
            pylist = [dict(r) for r in data]
        else:
            tuple_rows = [tuple(r) for r in data]
            cols = tuple(columns) if columns is not None else tuple(f"col_{i}" for i in range(len(tuple_rows[0])))
            return tuple_rows, cols
    else:
        return [], tuple(columns or ())

    if not pylist:
        return [], tuple(columns or ())
    cols = tuple(columns) if columns is not None else tuple(pylist[0].keys())
    return [tuple(d.get(c) for c in cols) for d in pylist], cols
