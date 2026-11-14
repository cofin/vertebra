"""Consistency bounds, table mutations, and mutation groups for Cloud Spanner."""

from __future__ import annotations

import base64
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Final, Literal, Self

from vertebra.typing import FrozenBaseStruct
from vertebra.utils.serializers import encode_json

__all__ = (
    "Mutation",
    "MutationGroup",
    "StalenessBound",
)

_MAX_MUTATION_CELLS_PER_BATCH: Final[int] = 80_000


class StalenessBound(FrozenBaseStruct, frozen=True, gc=False):
    """Timestamp bound controlling Cloud Spanner `ReadOnly` snapshot consistency."""

    mode: Literal[
        "strong",
        "exact_staleness",
        "max_staleness",
        "read_timestamp",
        "min_read_timestamp",
    ] = "strong"
    """Timestamp bound mode passed to `TransactionOptions.ReadOnly`."""

    milliseconds: int = 0
    """Staleness duration in milliseconds for `exact_staleness` and `max_staleness`."""

    timestamp_micros: int = 0
    """Unix epoch microseconds for `read_timestamp` and `min_read_timestamp`."""

    @classmethod
    def strong(cls) -> Self:
        """Return a strong consistency timestamp bound."""
        return cls(mode="strong")

    @classmethod
    def exact_staleness(cls, milliseconds: int) -> Self:
        """Return an `exact_staleness` bound for lock-free local replica reads."""
        return cls(mode="exact_staleness", milliseconds=max(0, int(milliseconds)))

    @classmethod
    def max_staleness(cls, milliseconds: int) -> Self:
        """Return a `max_staleness` bounded-staleness read bound."""
        return cls(mode="max_staleness", milliseconds=max(0, int(milliseconds)))

    @classmethod
    def read_timestamp(cls, timestamp_micros: int) -> Self:
        """Return an exact `read_timestamp` bound in Unix epoch microseconds."""
        return cls(mode="read_timestamp", timestamp_micros=int(timestamp_micros))

    @classmethod
    def min_read_timestamp(cls, timestamp_micros: int) -> Self:
        """Return a `min_read_timestamp` bound in Unix epoch microseconds."""
        return cls(mode="min_read_timestamp", timestamp_micros=int(timestamp_micros))

    def effective_value(self) -> int:
        """Return the integer payload expected by the Rust `SpannerClient` staleness parser."""
        if self.mode in {"read_timestamp", "min_read_timestamp"}:
            return int(self.timestamp_micros)
        return int(self.milliseconds)


class Mutation(FrozenBaseStruct, frozen=True, gc=False):
    """Single Cloud Spanner table mutation (`insert`, `update`, `insert_or_update`, `replace`, `delete`)."""

    op: Literal["insert", "update", "insert_or_update", "replace", "delete"]
    """Mutation operation type."""

    table: str
    """Target Spanner table name."""

    columns: tuple[str, ...] = ()
    """Column names populated by `Write` mutations."""

    values: tuple[tuple[Any, ...], ...] = ()
    """Row value tuples aligned with `columns`."""

    keys: tuple[tuple[Any, ...], ...] = ()
    """Primary key tuples targeted by `delete` mutations."""

    all_rows: bool = False
    """Whether a `delete` mutation deletes every row in `table`."""

    @classmethod
    def insert(
        cls,
        table: str,
        columns: Sequence[str],
        values: Sequence[Sequence[Any]],
    ) -> Self:
        """Construct an `insert` mutation."""
        return cls(
            op="insert",
            table=table,
            columns=tuple(str(c) for c in columns),
            values=tuple(tuple(row) for row in values),
        )

    @classmethod
    def update(
        cls,
        table: str,
        columns: Sequence[str],
        values: Sequence[Sequence[Any]],
    ) -> Self:
        """Construct an `update` mutation."""
        return cls(
            op="update",
            table=table,
            columns=tuple(str(c) for c in columns),
            values=tuple(tuple(row) for row in values),
        )

    @classmethod
    def insert_or_update(
        cls,
        table: str,
        columns: Sequence[str],
        values: Sequence[Sequence[Any]],
    ) -> Self:
        """Construct an `insert_or_update` (upsert) mutation."""
        return cls(
            op="insert_or_update",
            table=table,
            columns=tuple(str(c) for c in columns),
            values=tuple(tuple(row) for row in values),
        )

    @classmethod
    def replace(
        cls,
        table: str,
        columns: Sequence[str],
        values: Sequence[Sequence[Any]],
    ) -> Self:
        """Construct a `replace` mutation."""
        return cls(
            op="replace",
            table=table,
            columns=tuple(str(c) for c in columns),
            values=tuple(tuple(row) for row in values),
        )

    @classmethod
    def delete(
        cls,
        table: str,
        keys: Sequence[Sequence[Any] | Any] = (),
        *,
        all_rows: bool = False,
    ) -> Self:
        """Construct a `delete` mutation targeting `keys` or `all_rows`."""
        normalized_keys: list[tuple[Any, ...]] = []
        for k in keys:
            if isinstance(k, (list, tuple)):
                normalized_keys.append(tuple(k))
            else:
                normalized_keys.append((k,))
        return cls(
            op="delete",
            table=table,
            keys=tuple(normalized_keys),
            all_rows=all_rows,
        )

    @classmethod
    def coerce(cls, item: Mutation | Mapping[str, Any]) -> Mutation:
        """Coerce a `Mutation` or dictionary representation into a typed `Mutation` struct."""
        if isinstance(item, Mutation):
            return item
        op_raw = str(item.get("op", "insert_or_update")).lower()
        table = str(item.get("table", ""))
        if op_raw == "delete":
            return cls.delete(
                table,
                item.get("keys", ()),
                all_rows=bool(item.get("all", item.get("all_rows", False))),
            )
        cols = [str(c) for c in item.get("columns", ())]
        vals = [tuple(r) for r in item.get("values", ())]
        if op_raw == "insert":
            return cls.insert(table, cols, vals)
        if op_raw == "update":
            return cls.update(table, cols, vals)
        if op_raw == "replace":
            return cls.replace(table, cols, vals)
        return cls.insert_or_update(table, cols, vals)

    @staticmethod
    def chunk_by_cell_limit(
        mutations: Sequence[Mutation],
        *,
        max_cells: int = _MAX_MUTATION_CELLS_PER_BATCH,
    ) -> list[list[Mutation]]:
        """Partition `mutations` into batches that respect Spanner's per-commit cell limit."""
        batches: list[list[Mutation]] = []
        current: list[Mutation] = []
        current_cells = 0
        for mut in mutations:
            cells = mut.cell_count()
            if current and current_cells + cells > max_cells:
                batches.append(current)
                current = []
                current_cells = 0
            current.append(mut)
            current_cells += cells
        if current:
            batches.append(current)
        return batches

    def cell_count(self) -> int:
        """Return the estimated Spanner mutation cell count for batch sizing."""
        if self.op == "delete":
            return max(1, len(self.keys))
        return max(1, len(self.columns) * len(self.values))

    def to_dict(self) -> dict[str, Any]:
        """Serialize this mutation into the JSON dictionary structure consumed by Rust."""
        if self.op == "delete":
            return {
                "op": "delete",
                "table": self.table,
                "keys": [[self._normalize_cell(c) for c in row] for row in self.keys],
                "all": self.all_rows,
            }
        return {
            "op": self.op,
            "table": self.table,
            "columns": list(self.columns),
            "values": [[self._normalize_cell(c) for c in row] for row in self.values],
        }

    @classmethod
    def _normalize_cell(cls, val: Any) -> Any:
        if val is None or isinstance(val, (bool, int, float, str)):
            return val
        if isinstance(val, Decimal):
            return str(val)
        if isinstance(val, (bytes, bytearray, memoryview)):
            return base64.b64encode(bytes(val)).decode("ascii")
        if isinstance(val, (datetime, date)):
            return val.isoformat()
        if isinstance(val, (list, tuple)):
            return [cls._normalize_cell(item) for item in val]
        if isinstance(val, Mapping):
            return encode_json(val)
        return str(val)


class MutationGroup(FrozenBaseStruct, frozen=True, gc=False):
    """Independent group of mutations committed atomically via Cloud Spanner `BatchWrite`."""

    mutations: tuple[Mutation, ...]
    """Mutations belonging to this independent `BatchWrite` group."""

    @classmethod
    def coerce(
        cls,
        group: MutationGroup | Sequence[Mutation | Mapping[str, Any]],
    ) -> MutationGroup:
        """Coerce a `MutationGroup` or sequence of mutations into a `MutationGroup` struct."""
        if isinstance(group, MutationGroup):
            return group
        return cls(mutations=tuple(Mutation.coerce(m) for m in group))

    def to_dict(self) -> dict[str, Any]:
        """Serialize this mutation group into a dictionary for `SpannerClient.batch_write`."""
        return {"mutations": [m.to_dict() for m in self.mutations]}
