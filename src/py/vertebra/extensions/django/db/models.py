"""ArrowManager and ArrowModel base classes for Django ORM models."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from vertebra.extensions.django.db.queryset import ArrowQuerySet

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

__all__ = (
    "ArrowManager",
    "ArrowModel",
)


class ArrowManager:
    """Django ``Manager`` returning ``ArrowQuerySet`` instances and exposing Arrow/Struct methods."""

    def __init__(
        self,
        records: Iterable[dict[str, Any]] | None = None,
        *,
        model: Any = None,
    ) -> None:
        self.model = model
        self._db: str | None = None
        self._records: list[dict[str, Any]] = [dict(r) for r in (records or ())]

    def contribute_to_class(self, model: type[Any], name: str) -> None:
        """Attach this manager to a Django ``Model`` class."""
        self.model = model
        setattr(model, name, self)

    def get_queryset(self) -> ArrowQuerySet:
        """Return an ``ArrowQuerySet`` for the managed model."""
        if self._db is not None:
            return ArrowQuerySet(self.model, using=self._db)
        return ArrowQuerySet.from_records(self._records, model=self.model)

    def to_arrow(self, *, fields: Sequence[str] | None = None) -> Any:
        """Project all managed records into a ``pyarrow.Table``."""
        return self.get_queryset().to_arrow(fields=fields)

    def to_polars(self, *, fields: Sequence[str] | None = None) -> Any:
        """Project all managed records into a ``polars.DataFrame``."""
        return self.get_queryset().to_polars(fields=fields)

    def to_schema[T](self, schema_type: type[T], *, fields: Sequence[str] | None = None) -> list[T]:
        """Project all managed records directly into ``list[schema_type]``."""
        return self.get_queryset().to_schema(schema_type, fields=fields)

    def from_arrow(self, arrow_table: Any, *, batch_size: int | None = None) -> list[Any]:
        """Bulk-load an Arrow table into the managed model and update internal records."""
        loaded = self.get_queryset().from_arrow(arrow_table, batch_size=batch_size)
        if hasattr(arrow_table, "to_pylist"):
            self._records.extend(arrow_table.to_pylist())
        elif isinstance(arrow_table, list):
            self._records.extend(dict(r) for r in arrow_table)
        return loaded


class ArrowModel:
    """Mixin/base class pre-configured with ``objects = ArrowManager()``."""

    objects = ArrowManager()

    class Meta:
        abstract = True
