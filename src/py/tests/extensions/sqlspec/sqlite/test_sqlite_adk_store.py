from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest


@pytest.fixture
def mock_driver_session() -> tuple[AsyncMock, MagicMock]:

    mock = AsyncMock()

    mock.execute_script = AsyncMock()

    # Mock execute to return a result object

    mock_result = MagicMock()

    mock_result.one_or_none.return_value = None

    mock_result.all.return_value = []

    mock.execute = AsyncMock(return_value=mock_result)

    return mock, mock_result


@pytest.fixture
def mock_driver_connection() -> AsyncMock:

    mock = AsyncMock()

    mock.execute = AsyncMock()

    mock.fetchrow = AsyncMock(return_value=None)

    mock.fetchval = AsyncMock(return_value=None)

    return mock


@pytest.fixture
def mock_config(mock_driver_session: tuple[AsyncMock, MagicMock], mock_driver_connection: AsyncMock) -> MagicMock:

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


async def test_sqlite_adk_store_initialization(mock_config) -> None:
    from vertebra.extensions.sqlspec.sqlite.adk.store import SqliteADKStore

    store = SqliteADKStore(mock_config)
    assert store.session_table == "test_sessions"
    assert store.events_table == "test_events"


async def test_create_tables(mock_config, mock_driver_session) -> None:
    driver_session, _ = mock_driver_session
    from vertebra.extensions.sqlspec.sqlite.adk.store import SqliteADKStore

    store = SqliteADKStore(mock_config)
    await store.create_tables()

    assert driver_session.execute_script.call_count == 2
    call_args = driver_session.execute_script.call_args_list
    assert "CREATE TABLE IF NOT EXISTS test_sessions" in call_args[0][0][0]
    assert "CREATE TABLE IF NOT EXISTS test_events" in call_args[1][0][0]


async def test_create_session(mock_config, mock_driver_connection, mock_driver_session) -> None:
    _, mock_result = mock_driver_session
    from vertebra.extensions.sqlspec.sqlite.adk.store import SqliteADKStore

    # Mock one_or_none to return the created session
    # SQLite implementation uses REAL for timestamps, so we mock floats
    mock_result.one_or_none.return_value = {
        "id": "sess_1",
        "app_name": "app",
        "user_id": "user",
        "state": '{"foo": "bar"}',
        "create_time": 2440587.5,
        "update_time": 2440587.5,
    }

    store = SqliteADKStore(mock_config)
    session = await store.create_session("sess_1", "app", "user", {"foo": "bar"})

    assert session["id"] == "sess_1"
    assert session["state"] == {"foo": "bar"}
    mock_driver_connection.execute.assert_called_once()
    assert "INSERT INTO test_sessions" in mock_driver_connection.execute.call_args[0][0]
