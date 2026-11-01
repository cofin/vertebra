"""Zero-copy C-API ORM-to-Struct projection bridge for Django Models and QuerySets."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from typing import Any, cast

import msgspec

from vertebra._vertebra import hydrate_rows_vectorcall, orm_to_struct_vectorcall

__all__ = (
    "ORMBridge",
    "model_to_struct",
    "models_to_structs",
    "rows_to_structs",
)


def _struct_fields(struct_type: type[Any]) -> tuple[str, ...]:
    """Return the ordered field names of a ``msgspec.Struct`` or Pydantic ``BaseModel``."""
    if hasattr(struct_type, "__struct_fields__"):
        return tuple(struct_type.__struct_fields__)
    if hasattr(struct_type, "model_fields"):
        return tuple(struct_type.model_fields.keys())
    return ()


def model_to_struct[T](instance: Any, struct_type: type[T], *, fields: Sequence[str] | None = None) -> T:
    """Project a single Django Model instance or mapping into ``struct_type`` via ``PyObject_Vectorcall``."""
    field_names = tuple(fields) if fields is not None else _struct_fields(struct_type)
    if isinstance(instance, Mapping):
        row = tuple(instance.get(name) for name in field_names)
        if isinstance(struct_type, type) and issubclass(struct_type, msgspec.Struct):
            return cast("T", hydrate_rows_vectorcall(struct_type, [row], field_names)[0])
        data = dict(zip(field_names, row, strict=False))
        validate_fn = getattr(struct_type, "model_validate", None)
        if callable(validate_fn):
            return cast("T", validate_fn(data))
        return struct_type(**data)
    if isinstance(struct_type, type) and issubclass(struct_type, msgspec.Struct):
        return cast("T", orm_to_struct_vectorcall(struct_type, instance, field_names))
    data = {name: getattr(instance, name) for name in field_names}
    validate_fn = getattr(struct_type, "model_validate", None)
    if callable(validate_fn):
        return cast("T", validate_fn(data))
    return struct_type(**data)


def models_to_structs[T](
    instances: Iterable[Any],
    struct_type: type[T],
    *,
    fields: Sequence[str] | None = None,
) -> list[T]:
    """Project an iterable of Django Model instances into a list of ``struct_type``."""
    field_names = tuple(fields) if fields is not None else _struct_fields(struct_type)
    return [model_to_struct(inst, struct_type, fields=field_names) for inst in instances]


def rows_to_structs[T](
    rows: Sequence[Sequence[Any] | dict[str, Any]],
    struct_type: type[T],
    *,
    columns: Sequence[str] | None = None,
) -> list[T]:
    """Hydrate raw database tuples or dicts into ``struct_type`` via ``hydrate_rows_vectorcall``."""
    if not rows:
        return []
    first = rows[0]
    field_names = tuple(columns) if columns is not None else _struct_fields(struct_type)
    if isinstance(first, Mapping):
        tuple_rows = [tuple(cast("Mapping[str, Any]", r).get(col) for col in field_names) for r in rows]
    else:
        tuple_rows = [tuple(r) for r in rows]
    if isinstance(struct_type, type) and issubclass(struct_type, msgspec.Struct):
        return list(hydrate_rows_vectorcall(struct_type, tuple_rows, field_names))
    validate_fn = getattr(struct_type, "model_validate", None)
    if callable(validate_fn):
        return [cast("T", validate_fn(dict(zip(field_names, row, strict=False)))) for row in tuple_rows]
    return [struct_type(**dict(zip(field_names, row, strict=False))) for row in tuple_rows]


class ORMBridge:
    """High-level helper binding Django Model and QuerySet projections to ``msgspec.Struct``."""

    __slots__ = ()

    @staticmethod
    def to_struct[T](instance: Any, struct_type: type[T], *, fields: Sequence[str] | None = None) -> T:
        """Convert one Django model instance into ``struct_type``."""
        return model_to_struct(instance, struct_type, fields=fields)

    @staticmethod
    def to_structs[T](
        instances: Iterable[Any],
        struct_type: type[T],
        *,
        fields: Sequence[str] | None = None,
    ) -> list[T]:
        """Convert multiple Django model instances into ``list[struct_type]``."""
        return models_to_structs(instances, struct_type, fields=fields)

    @staticmethod
    def from_rows[T](
        rows: Sequence[Sequence[Any] | dict[str, Any]],
        struct_type: type[T],
        *,
        columns: Sequence[str] | None = None,
    ) -> list[T]:
        """Hydrate raw rows into ``list[struct_type]``."""
        return rows_to_structs(rows, struct_type, columns=columns)
