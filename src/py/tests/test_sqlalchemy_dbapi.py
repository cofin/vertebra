"""Tests for SQLAlchemy DBAPI implementation."""

import sys
import unittest.mock

# Mock native extension
mock_vertebra_native = unittest.mock.MagicMock()
sys.modules["vertebra._vertebra"] = mock_vertebra_native
mock_vertebra_native.ShmRegion = unittest.mock.MagicMock()
mock_vertebra_native.AsyncRing = unittest.mock.MagicMock()
mock_vertebra_native.SpscRing = unittest.mock.MagicMock()

from vertebra.extensions.sqlalchemy._dbapi import Connection, connect


def test_connect() -> None:
    conn = connect("test_pool")
    assert isinstance(conn, Connection)
    assert conn.pool_name == "test_pool"


def test_cursor_execution() -> None:
    conn = connect("test_pool")

    # Mock the client
    mock_client = unittest.mock.MagicMock()
    mock_client.acquire.return_value = "token"
    mock_client.execute.return_value = {"ok": True, "result": {"rows": [[1]], "columns": ["id"], "rowcount": 1}}
    conn._client = mock_client

    cursor = conn.cursor()

    # We need to mock VertebraSyncConnection._execute to return the response directly
    # because our mock client returns the raw response structure which the connection parses.
    # Actually Connection uses _execute which calls client.execute which calls _decode_result.
    # Let's mock _decode_result on the client instance if possible?
    # Or just mock connection._execute.

    with unittest.mock.patch("vertebra.drivers.broker.VertebraSyncConnection._execute") as mock_exec:
        mock_exec.return_value = {"rows": [[1]], "columns": ["id"], "rowcount": 1}

        cursor.execute("SELECT 1")

        assert cursor.rowcount == 1
        assert cursor.fetchone() == (1,)

        # Verify transaction started (lazy begin)
        assert conn._transaction_in_progress
        # Verify begin called on underlying connection
        # We need to check if conn.connection.begin() was called.
        # But conn.connection creates a new VertebraSyncConnection.
        # We can't easily access that instance unless we mock VertebraSyncConnection class.


def test_transaction_lifecycle() -> None:
    conn = connect("test_pool")
    conn._client = unittest.mock.MagicMock()
    conn._client.acquire.return_value = "token"

    with unittest.mock.patch("vertebra.extensions.sqlalchemy._dbapi.VertebraSyncConnection") as MockConnClass:
        mock_inner_conn = MockConnClass.return_value
        # Setup inner cursor
        mock_inner_cursor = unittest.mock.MagicMock()
        mock_inner_cursor.execute.return_value = 1
        mock_inner_conn.cursor.return_value = mock_inner_cursor

        cursor = conn.cursor()
        cursor.execute("SELECT 1")

        # Check begin called
        mock_inner_conn.begin.assert_called_once()
        assert conn._transaction_in_progress

        conn.commit()
        mock_inner_conn.commit.assert_called_once()
        assert not conn._transaction_in_progress

        cursor.execute("SELECT 2")
        mock_inner_conn.begin.call_count == 2

        conn.rollback()
        mock_inner_conn.rollback.assert_called_once()
        assert not conn._transaction_in_progress
