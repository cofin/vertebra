"""Tests for SQLSpec Arrow integration."""

import unittest.mock
from typing import Any

import pytest

from vertebra.extensions.sqlspec._driver import VertebraSyncDriverBase
from vertebra.extensions.sqlspec._shared import _decode_arrow
from vertebra.typing import PYARROW_INSTALLED

if PYARROW_INSTALLED:
    import pyarrow as pa


@pytest.fixture
def mock_connection() -> unittest.mock.Mock:
    return unittest.mock.Mock()


@pytest.fixture
def mock_driver(mock_connection: unittest.mock.Mock) -> VertebraSyncDriverBase:
    return VertebraSyncDriverBase(
        connection=mock_connection,
        driver_features={"data_dictionary_factory": unittest.mock.Mock(), "arrow_threshold_rows": 10},
    )


@pytest.mark.skipif(not PYARROW_INSTALLED, reason="pyarrow not installed")
def test_should_use_arrow(mock_driver: VertebraSyncDriverBase) -> None:
    # Below threshold
    rows: list[list[Any]] = [[1, "test"]]
    columns = ["id", "name"]
    assert not mock_driver._should_use_arrow(rows, columns)

    # Above threshold
    rows = [[i, "test"] for i in range(15)]
    assert mock_driver._should_use_arrow(rows, columns)


class FakeRegion(bytearray):
    def close(self) -> None:
        pass


@pytest.mark.skipif(not PYARROW_INSTALLED, reason="pyarrow not installed")
def test_encode_arrow(mock_driver: VertebraSyncDriverBase) -> None:
    rows: list[list[Any]] = [[1, "test"], [2, "foo"]]
    columns = ["id", "name"]

    with unittest.mock.patch("vertebra.extensions.sqlspec._driver.ShmRegion") as mock_shm:
        # Create a fake region that supports buffer protocol
        # We need it large enough to hold the arrow data
        fake_region = FakeRegion(4096)
        mock_shm.create_named.return_value = fake_region

        result = mock_driver._encode_arrow(rows, columns)

        assert "arrow" in result
        assert result["arrow"]["name"].startswith("/vertebra.client.arrow.")
        assert result["arrow"]["size"] > 0

        # Verify data was written to the region (check for Arrow magic header "ARROW")
        # IPC stream format starts with 0xFFFFFFFF then size then "ARROW"?
        # Or simply check it's not all zeros.
        assert any(b != 0 for b in fake_region)


@pytest.mark.skipif(not PYARROW_INSTALLED, reason="pyarrow not installed")
def test_decode_arrow_bytes() -> None:
    # Create sample arrow bytes
    data = [{"id": 1, "name": "test"}]
    table = pa.Table.from_pylist(data)
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    arrow_bytes = sink.getvalue().to_pybytes()

    # Test decoding
    result = _decode_arrow(arrow_bytes)
    assert len(result) == 1
    assert result[0]["id"] == 1
    assert result[0]["name"] == "test"


@pytest.mark.skipif(not PYARROW_INSTALLED, reason="pyarrow not installed")
def test_decode_arrow_table() -> None:
    data = [{"id": 1, "name": "test"}]
    table = pa.Table.from_pylist(data)

    result = _decode_arrow(table)
    assert len(result) == 1
    assert result[0]["id"] == 1
