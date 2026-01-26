"""Tests for Arrow typing protocols and conditional imports."""

import pytest

from vertebra.core.protocols import ArrowTableProtocol
from vertebra.typing import PYARROW_INSTALLED, ArrowTable
from vertebra.utils.module_loader import reset_dependency_cache


class TestPyarrowInstalledFlag:
    def test_flag_evaluates_to_bool(self) -> None:
        """Test that PYARROW_INSTALLED evaluates to a boolean."""
        reset_dependency_cache()
        # pyarrow is installed in test env
        assert bool(PYARROW_INSTALLED) is True

    def test_flag_works_in_if_statement(self) -> None:
        """Test that flag works correctly in conditional."""
        reset_dependency_cache()
        result = "installed" if PYARROW_INSTALLED else "not_installed"
        assert result == "installed"


class TestArrowTableProtocol:
    def test_real_table_satisfies_protocol(self) -> None:
        """Test that a real pyarrow Table satisfies the Protocol."""
        import pyarrow as pa

        table = pa.Table.from_pydict({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        # Protocol check
        assert isinstance(table, ArrowTableProtocol)

    def test_protocol_attributes(self) -> None:
        """Test that Protocol defines expected attributes."""
        import pyarrow as pa

        table = pa.Table.from_pydict({"a": [1, 2, 3]})
        # These should all work since they're defined in the Protocol
        assert hasattr(table, "column_names")
        assert hasattr(table, "num_rows")
        assert hasattr(table, "to_pylist")
        assert hasattr(table, "to_pydict")


class TestArrowTableConditionalImport:
    def test_arrow_table_is_real_type_when_installed(self) -> None:
        """Test that ArrowTable is the real pyarrow.Table when installed."""
        import pyarrow as pa

        # When pyarrow is installed, ArrowTable should be the real type
        assert ArrowTable is pa.Table

    def test_table_can_be_created_via_type(self) -> None:
        """Test that ArrowTable can be used to reference the pyarrow.Table class."""
        # Create a table using the conditionally imported type
        table = ArrowTable.from_pydict({"col": [1, 2, 3]})
        assert table.num_rows == 3
        assert table.column_names == ["col"]


class TestArrowTypingIntegration:
    def test_type_annotation_pattern(self) -> None:
        """Test the recommended pattern for type-annotated arrow code."""

        def process_table(table: ArrowTable) -> list[dict[str, object]]:
            """Example function with ArrowTable annotation."""
            return table.to_pylist()

        import pyarrow as pa

        test_table = pa.Table.from_pydict({"id": [1, 2], "name": ["a", "b"]})
        result = process_table(test_table)

        assert result == [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]

    def test_conditional_import_pattern(self) -> None:
        """Test the recommended pattern for conditional arrow imports."""
        if PYARROW_INSTALLED:
            import pyarrow as pa

            table = pa.Table.from_pydict({"test": [1]})
            assert table.num_rows == 1
        else:
            # This branch won't execute since pyarrow is installed
            pytest.skip("pyarrow not installed")
