"""ArrowQuerySetMixin and ArrowQuerySet for zero-copy Arrow, Polars, and Struct projections in Django."""

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, cast

from vertebra.extensions.django.orm import _struct_fields, models_to_structs, rows_to_structs
from vertebra.utils.module_loader import import_optional

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

__all__ = (
    "ArrowQuerySet",
    "ArrowQuerySetMixin",
)


class ArrowQuerySetMixin:
    """Mixin adding ``to_arrow()``, ``to_polars()``, ``to_schema()``, and ``from_arrow()`` to Django QuerySets."""

    def _extract_records(self, fields: Sequence[str] | None = None) -> tuple[list[str], list[dict[str, Any]]]:
        """Extract column names and row dictionaries from this QuerySet."""
        values_fn = getattr(self, "values", None)
        if callable(values_fn):
            rows_iter = cast("Iterable[Mapping[str, Any]]", values_fn(*fields) if fields else values_fn())
            records = [dict(r) for r in rows_iter]
            if records:
                cols = list(fields) if fields else list(records[0].keys())
                return cols, records
            return list(fields or ()), []
        items = list(self) if isinstance(self, Iterable) else getattr(self, "_records", [])
        records = []
        for item in items:
            if isinstance(item, dict):
                records.append(dict(item))
            elif hasattr(item, "__dict__"):
                records.append({k: v for k, v in item.__dict__.items() if not k.startswith("_")})
        cols = list(fields) if fields else (list(records[0].keys()) if records else [])
        return cols, records

    def to_arrow(self, *, fields: Sequence[str] | None = None) -> Any:
        """Execute this QuerySet and return a ``pyarrow.Table``."""
        pa = import_optional("pyarrow")
        if pa is None:
            msg = "pyarrow is required for QuerySet.to_arrow()"
            raise RuntimeError(msg)
        cols, records = self._extract_records(fields=fields)
        if not records:
            arrays = {col: pa.array([]) for col in cols}
            return pa.Table.from_pydict(arrays)
        return pa.Table.from_pylist(records)

    def to_polars(self, *, fields: Sequence[str] | None = None) -> Any:
        """Execute this QuerySet and return a ``polars.DataFrame`` via Arrow zero-copy handoff."""
        pl = import_optional("polars")
        arrow_table = self.to_arrow(fields=fields)
        if pl is not None:
            return pl.from_arrow(arrow_table)
        return arrow_table

    def to_schema[T](self, schema_type: type[T], *, fields: Sequence[str] | None = None) -> list[T]:
        """Project QuerySet rows directly into ``schema_type`` bypassing Django Model instantiation."""
        target_fields = tuple(fields) if fields is not None else _struct_fields(schema_type)
        values_list_fn = getattr(self, "values_list", None)
        if callable(values_list_fn) and target_fields:
            raw_rows = list(cast("Iterable[Any]", values_list_fn(*target_fields)))
            return rows_to_structs(raw_rows, schema_type, columns=target_fields)
        cols, records = self._extract_records(fields=target_fields or None)
        if records:
            return rows_to_structs(records, schema_type, columns=target_fields or cols)
        if isinstance(self, Iterable):
            return models_to_structs(self, schema_type, fields=target_fields or None)
        return []

    def from_arrow(self, arrow_table: Any, *, batch_size: int | None = None) -> list[Any]:
        """Bulk-insert rows from a ``pyarrow.Table`` or ``RecordBatch`` into this QuerySet's model."""
        rows: list[dict[str, Any]]
        if hasattr(arrow_table, "to_pylist"):
            rows = list(arrow_table.to_pylist())
        elif isinstance(arrow_table, list):
            rows = [dict(r) for r in arrow_table]
        else:
            rows = []

        model_cls = getattr(self, "model", None)
        if model_cls is not None and callable(model_cls):
            instances = [model_cls(**r) for r in rows]
            bulk_create_fn = getattr(self, "bulk_create", None)
            if callable(bulk_create_fn):
                return list(cast("Iterable[Any]", bulk_create_fn(instances, batch_size=batch_size)))
            return instances
        records_attr = getattr(self, "_records", None)
        if isinstance(records_attr, list):
            records_attr.extend(rows)
        return rows


class ArrowQuerySet(ArrowQuerySetMixin):
    """Django-compatible ``QuerySet`` with native Arrow, Polars, and ``msgspec.Struct`` projection."""

    def __init__(self, model: Any = None, query: Any = None, using: str | None = None, hints: Any = None) -> None:
        self.model = model
        self.query = query
        self._db = using
        self._hints = hints
        self._records: list[dict[str, Any]] = []

    @classmethod
    def from_records(cls, records: Iterable[dict[str, Any]], *, model: Any = None) -> ArrowQuerySet:
        """Construct an in-memory ``ArrowQuerySet`` seeded with ``records``."""
        qs = cls(model=model)
        qs._records = [dict(r) for r in records]
        return qs

    def values(self, *fields: str) -> list[dict[str, Any]]:
        """Return projected dictionaries for ``fields``."""
        records = getattr(self, "_records", [])
        if not fields:
            return [dict(r) for r in records]
        return [{k: r.get(k) for k in fields} for r in records]

    def values_list(self, *fields: str, flat: bool = False) -> list[Any]:
        """Return projected tuples (or scalar values when ``flat=True``) for ``fields``."""
        records = getattr(self, "_records", [])
        if flat and len(fields) == 1:
            key = fields[0]
            return [r.get(key) for r in records]
        return [tuple(r.get(k) for k in fields) for r in records]
