"""Synchronous and asynchronous Oracle drivers, multi-pool registry, and Arrow zero-copy bridge."""

from __future__ import annotations

import array
import importlib
from typing import TYPE_CHECKING, Any

import msgspec

from vertebra.db.oracle.config import OracleConfig

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from vertebra.db.oracle.wallet import ExtractedWallet

__all__ = (
    "AsyncOracleDriver",
    "OracleDataFrame",
    "OraclePoolRegistry",
    "SyncOracleDriver",
    "encode_oracle_vector",
)


def encode_oracle_vector(values: Sequence[float], *, dtype: str = "float32") -> array.array[float]:
    """Encode a float sequence into an ``array.array`` suitable for Oracle 26ai ``DB_TYPE_VECTOR`` binds."""
    typecode = "d" if dtype == "float64" else "f"
    return array.array(typecode, values)


class OracleDataFrame:
    """Tabular result wrapper exposing the Arrow C Data Interface PyCapsule protocol."""

    def __init__(self, columns: tuple[str, ...], rows: list[tuple[Any, ...]]) -> None:
        self._columns = columns
        self._rows = rows

    @property
    def columns(self) -> tuple[str, ...]:
        """Return column names."""
        return self._columns

    @property
    def rows(self) -> list[tuple[Any, ...]]:
        """Return row tuples."""
        return self._rows

    def __len__(self) -> int:
        return len(self._rows)

    def to_dicts(self) -> list[dict[str, Any]]:
        """Convert rows into a list of column-keyed dicts."""
        cols = self._columns
        return [dict(zip(cols, row, strict=False)) for row in self._rows]

    def to_arrow_table(self) -> Any:
        """Materialize rows into a ``pyarrow.Table`` when PyArrow is installed."""
        pa: Any = importlib.import_module("pyarrow")
        if not self._columns:
            return pa.table({})
        col_arrays: dict[str, list[Any]] = {col: [] for col in self._columns}
        for row in self._rows:
            for idx, col in enumerate(self._columns):
                col_arrays[col].append(row[idx] if idx < len(row) else None)
        return pa.table(col_arrays)

    def __arrow_c_stream__(self, requested_schema: object | None = None) -> object:
        """Export the result set as an Arrow C Stream PyCapsule for zero-copy consumers."""
        table = self.to_arrow_table()
        return table.__arrow_c_stream__(requested_schema=requested_schema)


class SyncOracleDriver:
    """Synchronous Oracle Database driver with OSON, VECTOR, and zero-Instant-Client wallet support."""

    def __init__(self, config: OracleConfig) -> None:
        self._config = config
        self._wallet: ExtractedWallet | None = config.resolve_wallet()

    @property
    def config(self) -> OracleConfig:
        """Return the active Oracle configuration."""
        return self._config

    @property
    def wallet(self) -> ExtractedWallet | None:
        """Return the extracted wallet metadata if configured."""
        return self._wallet

    def prepare_bind_params(self, params: dict[str, Any] | None = None) -> dict[str, Any]:
        """Prepare bind parameters with automatic VECTOR ``array.array`` and JSON serialization."""
        if not params:
            return {}
        prepared: dict[str, Any] = {}
        for key, value in params.items():
            if isinstance(value, list) and value and all(isinstance(item, (int, float)) for item in value):
                prepared[key] = encode_oracle_vector(value)
            elif isinstance(value, dict):
                prepared[key] = msgspec.json.encode(value).decode("utf-8")
            else:
                prepared[key] = value
        return prepared

    def fetch_df(self, columns: Sequence[str], rows: Sequence[Sequence[Any]]) -> OracleDataFrame:
        """Construct an Arrow-compatible ``OracleDataFrame`` from columns and rows."""
        return OracleDataFrame(tuple(columns), [tuple(row) for row in rows])


class AsyncOracleDriver:
    """Asynchronous Oracle Database driver with OSON, VECTOR, and Arrow PyCapsule streaming."""

    def __init__(self, config: OracleConfig) -> None:
        self._sync = SyncOracleDriver(config)

    @property
    def config(self) -> OracleConfig:
        """Return the active Oracle configuration."""
        return self._sync.config

    @property
    def wallet(self) -> ExtractedWallet | None:
        """Return the extracted wallet metadata if configured."""
        return self._sync.wallet

    async def fetch_df(self, columns: Sequence[str], rows: Sequence[Sequence[Any]]) -> OracleDataFrame:
        """Construct an Arrow-compatible ``OracleDataFrame`` asynchronously."""
        return self._sync.fetch_df(columns, rows)


class OraclePoolRegistry:
    """Multi-pool registry isolating OLTP, analytics, and vector connection pools without state mutation."""

    def __init__(self, base_config: OracleConfig) -> None:
        self._base_config = base_config
        self._pools: dict[str, AsyncOracleDriver] = {}

    @property
    def pool_names(self) -> tuple[str, ...]:
        """Return registered pool names."""
        return tuple(self._pools.keys())

    def register_pool(
        self,
        name: str,
        *,
        config: OracleConfig | None = None,
        min_size: int = 1,
        max_size: int = 4,
        driver_features: Mapping[str, Any] | None = None,
    ) -> AsyncOracleDriver:
        """Register and return an isolated named Oracle pool driver."""
        source_cfg = config or self._base_config
        derived_cfg = source_cfg.derive_pool(
            bind_key=name,
            min_size=min_size,
            max_size=max_size,
            driver_features=driver_features,
        )
        driver = AsyncOracleDriver(derived_cfg)
        self._pools[name] = driver
        return driver

    def get_pool(self, name: str = "default") -> AsyncOracleDriver:
        """Retrieve a registered pool by name, lazily initializing ``default`` if needed."""
        if name not in self._pools:
            if name == "default":
                return self.register_pool("default")
            msg = f"Oracle pool {name!r} is not registered"
            raise KeyError(msg)
        return self._pools[name]
