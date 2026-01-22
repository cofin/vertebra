"""Tests for Vertebra SQLAlchemy dialects."""

import sys
import unittest.mock

import pytest
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

# Mock native extension
mock_vertebra_native = unittest.mock.MagicMock()
sys.modules["vertebra._vertebra"] = mock_vertebra_native
mock_vertebra_native.ShmRegion = unittest.mock.MagicMock()
mock_vertebra_native.AsyncRing = unittest.mock.MagicMock()
mock_vertebra_native.SpscRing = unittest.mock.MagicMock()

# Import dialects to register them


def test_postgres_engine() -> None:
    # Use + separator to avoid parsing issues
    engine = create_engine("postgresql+vertebra://test_pool")
    assert engine.dialect.name == "vertebra_postgres"

    # Mock the client used by DBAPI
    with unittest.mock.patch("vertebra.extensions.sqlalchemy._dbapi.SyncBrokerClient") as MockClientClass:
        mock_client = MockClientClass.return_value
        mock_client.acquire.return_value = "token"
        mock_client.begin.return_value = {"ok": True}
        # Dialect initialize calls version check
        mock_client.execute.return_value = {"rows": [["PostgreSQL 15.0"]], "columns": ["version"], "rowcount": 1}

        conn = engine.connect()
        raw_conn = conn.connection.dbapi_connection
        assert raw_conn.pool_name == "test_pool"
        conn.close()


@pytest.mark.anyio
async def test_postgres_async_engine() -> None:
    # Use +vertebra_async for async dialect
    # Use NullPool to avoid QueuePool mismatch
    engine = create_async_engine("postgresql+vertebra_async://test_pool", poolclass=NullPool)
    assert engine.dialect.name == "vertebra_postgres_async"


def test_mysql_engine() -> None:
    engine = create_engine("mysql+vertebra://test_pool")
    assert engine.dialect.name == "vertebra_mysql"


def test_sqlite_engine() -> None:
    engine = create_engine("sqlite+vertebra://test_pool")
    assert engine.dialect.name == "vertebra_sqlite"
