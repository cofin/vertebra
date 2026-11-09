"""Unit tests for free-threaded Python GIL declaration and immutable PyO3 types on database extensions."""

from __future__ import annotations

import pytest


@pytest.mark.parametrize(
    ("module_name", "class_names"),
    [
        ("vertebra._bigquery", ("SyncBigQueryClient", "AsyncBigQueryClient")),
        ("vertebra._mssql", ("SyncMSSQLClient", "AsyncMSSQLClient")),
        ("vertebra._spanner", ("SyncSpannerClient", "AsyncSpannerClient")),
    ],
)
def test_db_extension_modules_declare_gil_used_false_and_immutable_types(
    module_name: str,
    class_names: tuple[str, ...],
) -> None:
    """Verify optional database PyO3 extensions declare `GIL_USED = False` and immutable client classes."""
    mod = pytest.importorskip(module_name)
    assert getattr(mod, "GIL_USED", None) is False
    attr_name = "_mutated_type_attr"
    for class_name in class_names:
        cls = getattr(mod, class_name)
        with pytest.raises(TypeError):
            setattr(cls, attr_name, 1)
