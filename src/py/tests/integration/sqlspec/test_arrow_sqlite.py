"""Integration tests for SQLSpec Arrow transport."""

import unittest.mock
import uuid
from typing import Any

import pytest

pa = pytest.importorskip("pyarrow")

from vertebra.database.backends.broker import VertebraAsyncConnection  # noqa: E402
from vertebra.extensions.sqlspec.sqlite import VertebraSqliteAsyncConfig  # noqa: E402
from vertebra.messaging import broker  # noqa: E402
from vertebra.messaging.broker import AsyncBrokerClient  # noqa: E402


class MockShmRegion(bytearray):
    def close(self) -> None:
        pass


@pytest.mark.anyio
async def test_sqlspec_arrow_integration_flow(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test SQLSpec client Arrow decoding flow."""
    config = VertebraSqliteAsyncConfig(pool="test_pool")

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
    monkeypatch.setattr(broker.ShmRegion, "open_named", mock_open_named)

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
    with unittest.mock.patch("vertebra.database.backends.broker.VertebraAsyncPool.acquire", return_value=fake_conn):
        async with config.provide_session() as session:
            rows = await session.select("SELECT * FROM large_table")

            assert len(rows) == 1000
            assert rows[0]["id"] == 0
            assert rows[0]["name"] == "row_0"
            assert rows[999]["id"] == 999

            # Verify decoding happened via our mock region
            mock_open_named.assert_called_with(arrow_region_name, len(arrow_bytes))
