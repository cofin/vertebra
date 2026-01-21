from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import MagicMock, AsyncMock

import pytest

if TYPE_CHECKING:
    from vertebra.extensions.sqlspec.sqlite.litestar.store import SqliteLitestarStore


@pytest.fixture
def mock_driver_session():
    mock = AsyncMock()
    mock.execute_script = AsyncMock()
    return mock


@pytest.fixture
def mock_driver_connection():
    mock = AsyncMock()
    mock.execute = AsyncMock()
    mock.fetchrow = AsyncMock(return_value=None)
    mock.fetchval = AsyncMock(return_value=None)
    return mock


@pytest.fixture
def mock_config(mock_driver_session, mock_driver_connection):
    config = MagicMock()
    config.extension_config = {"litestar": {"session_table": "test_litestar_sessions"}}
    
    session_cm = AsyncMock()
    session_cm.__aenter__.return_value = mock_driver_session
    config.provide_session.return_value = session_cm

    conn_cm = AsyncMock()
    conn_cm.__aenter__.return_value = mock_driver_connection
    config.provide_connection = AsyncMock(return_value=conn_cm)
    
    return config


async def test_sqlite_litestar_store_initialization(mock_config):
    from vertebra.extensions.sqlspec.sqlite.litestar.store import SqliteLitestarStore

    store = SqliteLitestarStore(mock_config)
    assert store.table_name == "test_litestar_sessions"


async def test_create_table(mock_config, mock_driver_session):
    from vertebra.extensions.sqlspec.sqlite.litestar.store import SqliteLitestarStore

    store = SqliteLitestarStore(mock_config)
    await store.create_table()

    mock_driver_session.execute_script.assert_called_once()
    assert "CREATE TABLE IF NOT EXISTS test_litestar_sessions" in mock_driver_session.execute_script.call_args[0][0]


async def test_get_session(mock_config, mock_driver_connection):
    from vertebra.extensions.sqlspec.sqlite.litestar.store import SqliteLitestarStore

    # Mock fetchrow to return a tuple (data, expires_at)
    mock_driver_connection.fetchrow.return_value = (b"session_data", None)

    store = SqliteLitestarStore(mock_config)
    data = await store.get("sess_1")

    assert data == b"session_data"
    mock_driver_connection.fetchrow.assert_called_once()
    assert "SELECT data, expires_at" in mock_driver_connection.fetchrow.call_args[0][0]
