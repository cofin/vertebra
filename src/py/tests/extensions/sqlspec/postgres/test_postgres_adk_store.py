from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import MagicMock, AsyncMock

import pytest
from sqlspec.extensions.adk import SessionRecord

if TYPE_CHECKING:
    from vertebra.extensions.sqlspec.postgres.adk.store import PostgresADKStore
    from vertebra.extensions.sqlspec.postgres.config import PostgresConfig


@pytest.fixture
def mock_driver_session():
    mock = AsyncMock()
    mock.execute_script = AsyncMock()
    
    # Mock execute to return a result object
    mock_result = MagicMock()
    mock_result.one_or_none.return_value = None
    mock_result.all.return_value = []
    mock.execute = AsyncMock(return_value=mock_result)
    
    return mock, mock_result


@pytest.fixture
def mock_driver_connection():
    mock = AsyncMock()
    mock.execute = AsyncMock()
    return mock


@pytest.fixture
def mock_config(mock_driver_session, mock_driver_connection):
    driver_session, _ = mock_driver_session
    config = MagicMock()
    config.extension_config = {"adk": {"session_table": "test_sessions", "events_table": "test_events"}}
    
    # Mock provide_session context manager
    session_cm = AsyncMock()
    session_cm.__aenter__.return_value = driver_session
    config.provide_session.return_value = session_cm

    # Mock provide_connection context manager
    conn_cm = AsyncMock()
    conn_cm.__aenter__.return_value = mock_driver_connection
    config.provide_connection = AsyncMock(return_value=conn_cm)
    
    return config


async def test_postgres_adk_store_initialization(mock_config):
    from vertebra.extensions.sqlspec.postgres.adk.store import PostgresADKStore

    store = PostgresADKStore(mock_config)
    assert store.session_table == "test_sessions"
    assert store.events_table == "test_events"


async def test_create_tables(mock_config, mock_driver_session):
    driver_session, _ = mock_driver_session
    from vertebra.extensions.sqlspec.postgres.adk.store import PostgresADKStore

    store = PostgresADKStore(mock_config)
    await store.create_tables()

    assert driver_session.execute_script.call_count == 2
    # Check if CREATE TABLE SQL contains expected strings (simple check)
    call_args = driver_session.execute_script.call_args_list
    assert "CREATE TABLE IF NOT EXISTS test_sessions" in call_args[0][0][0]
    assert "CREATE TABLE IF NOT EXISTS test_events" in call_args[1][0][0]


async def test_create_session(mock_config, mock_driver_connection, mock_driver_session):
    _, mock_result = mock_driver_session
    from vertebra.extensions.sqlspec.postgres.adk.store import PostgresADKStore

    # Mock one_or_none to return the created session
    mock_result.one_or_none.return_value = {
        "id": "sess_1",
        "app_name": "app",
        "user_id": "user",
        "state": {"foo": "bar"},
        "create_time": "now",
        "update_time": "now",
    }

    store = PostgresADKStore(mock_config)
    session = await store.create_session("sess_1", "app", "user", {"foo": "bar"})

    assert session["id"] == "sess_1"
    assert session["state"] == {"foo": "bar"}
    mock_driver_connection.execute.assert_called_once()
    assert "INSERT INTO test_sessions" in mock_driver_connection.execute.call_args[0][0]


async def test_postgres_adk_memory_store_initialization(mock_config):
    from vertebra.extensions.sqlspec.postgres.adk.store import PostgresADKMemoryStore

    store = PostgresADKMemoryStore(mock_config)
    assert store._memory_table == "adk_memory_entries"  # Default


async def test_postgres_adk_memory_store_insert(mock_config, mock_driver_connection):
    from vertebra.extensions.sqlspec.postgres.adk.store import PostgresADKMemoryStore

    mock_driver_connection.execute.return_value = 1

    store = PostgresADKMemoryStore(mock_config)
    count = await store.insert_memory_entries([{
        "id": "mem_1",
        "session_id": "sess_1",
        "app_name": "app",
        "user_id": "user",
        "event_id": "evt_1",
        "author": "user",
        "timestamp": "now",
        "content_json": {"text": "hello"},
        "content_text": "hello",
        "metadata_json": {},
        "inserted_at": "now"
    }])

    assert count == 1
    mock_driver_connection.execute.assert_called_once()
    assert "INSERT INTO adk_memory_entries" in mock_driver_connection.execute.call_args[0][0]
