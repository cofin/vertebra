"""Integration tests for SQLSpec Arrow transport."""

import sys
import unittest.mock
import uuid
from typing import Any

import pytest

# Mock the native extension BEFORE any other imports
mock_vertebra_native = unittest.mock.MagicMock()
sys.modules["vertebra._vertebra"] = mock_vertebra_native


# Define Mock classes
class MockShmRegion(bytearray):
    def close(self) -> None:
        pass

    @classmethod
    def open_named(cls, name: str, size: int) -> "MockShmRegion":
        msg = "Should be patched"
        raise NotImplementedError(msg)


mock_vertebra_native.ShmRegion = MockShmRegion
mock_vertebra_native.AsyncRing = unittest.mock.MagicMock()
mock_vertebra_native.SpscRing = unittest.mock.MagicMock()

try:
    import pyarrow as pa

    from vertebra.drivers.broker import VertebraAsyncConnection
    from vertebra.extensions.sqlspec.sqlite import VertebraSqliteAsyncConfig
    from vertebra.resources.broker import AsyncBrokerClient

    PYARROW_INSTALLED = True
except ImportError:
    PYARROW_INSTALLED = False


@pytest.mark.anyio
@pytest.mark.skipif(not PYARROW_INSTALLED, reason="pyarrow not installed")
async def test_sqlspec_arrow_integration_flow() -> None:
    """Test SQLSpec client Arrow decoding flow."""
    config = VertebraSqliteAsyncConfig(pool="test_pool")
    import pyarrow as pa

    # Prepare Arrow data
    data = [{"id": i, "name": f"row_{i}"} for i in range(1000)]
    table = pa.Table.from_pylist(data)
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    arrow_bytes = sink.getvalue().to_pybytes()

    # Setup Mock Region
    arrow_region_name = f"/vertebra.client.arrow.{uuid.uuid4().hex}"
    mock_region_instance = MockShmRegion(len(arrow_bytes))
    mock_region_instance[:] = arrow_bytes

    mock_open_named = unittest.mock.MagicMock(return_value=mock_region_instance)
    MockShmRegion.open_named = mock_open_named

    # Setup Real Client with Mocked Request
    client = AsyncBrokerClient()

    async def mock_request(payload: dict[str, Any]) -> dict[str, Any]:
        return {"ok": True, "arrow": {"name": arrow_region_name, "size": len(arrow_bytes)}, "rowcount": 1000}

    # We patch the instance methods
    client.request = mock_request  # type: ignore
    client.release = unittest.mock.AsyncMock(return_value=True)  # type: ignore

    # Create Connection with Real Client
    fake_conn = VertebraAsyncConnection("test_pool", client, "fake_token")

    # Patch the pool acquire to return our connection
    with unittest.mock.patch("vertebra.drivers.broker.VertebraAsyncPool.acquire", return_value=fake_conn):
        async with config.provide_session() as session:
            rows = await session.select("SELECT * FROM large_table")

            assert len(rows) == 1000
            assert rows[0]["id"] == 0
            assert rows[0]["name"] == "row_0"
            assert rows[999]["id"] == 999

            # Verify decoding happened via our mock region
            mock_open_named.assert_called_with(arrow_region_name, len(arrow_bytes))
