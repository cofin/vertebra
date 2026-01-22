"""Tests for Vertebra SQLAlchemy DBAPI."""

from unittest.mock import MagicMock, patch
import pytest
from vertebra.extensions.sqlalchemy._dbapi import connect, Connection, Cursor, DatabaseError


class TestDBAPI:
    @patch("vertebra.extensions.sqlalchemy._dbapi.SyncBrokerClient")
    def test_connection_lifecycle(self, mock_client_cls):
        mock_client = mock_client_cls.return_value
        mock_client.acquire.return_value = "fake-token"
        
        conn = connect("test-pool")
        assert isinstance(conn, Connection)
        assert conn.pool_name == "test-pool"
        
        # Trigger lazy connect
        _ = conn.connection
        mock_client.acquire.assert_called_once_with("test-pool")
        
        conn.close()
        assert conn._connection is None

    @patch("vertebra.extensions.sqlalchemy._dbapi.SyncBrokerClient")
    def test_cursor_execute(self, mock_client_cls):
        mock_client = mock_client_cls.return_value
        mock_client.acquire.return_value = "fake-token"
        
        # Mock the inner connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        
        with patch("vertebra.extensions.sqlalchemy._dbapi.VertebraSyncConnection", return_value=mock_conn):
            conn = connect("test-pool")
            cursor = conn.cursor()
            assert isinstance(cursor, Cursor)
            
            mock_cursor.execute.return_value = 1
            mock_cursor._columns = ["id"]
            
            cursor.execute("SELECT 1")
            
            # Verify auto-begin
            mock_conn.begin.assert_called_once()
            assert conn._transaction_in_progress is True
            
            mock_cursor.execute.assert_called_once_with("SELECT 1", None)
            assert cursor.rowcount == 1
            assert cursor.description == [("id", None, None, None, None, None, None)]

    @patch("vertebra.extensions.sqlalchemy._dbapi.SyncBrokerClient")
    def test_cursor_execute_numeric_conversion(self, mock_client_cls):
        mock_client = mock_client_cls.return_value
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        
        with patch("vertebra.extensions.sqlalchemy._dbapi.VertebraSyncConnection", return_value=mock_conn):
            # Explicitly set numeric style
            conn = Connection("test-pool", paramstyle="numeric")
            cursor = conn.cursor()
            
            cursor.execute("SELECT * FROM users WHERE id = :1 AND status = :2", [1, "active"])
            
            # Should have converted to $1, $2
            mock_cursor.execute.assert_called_once_with(
                "SELECT * FROM users WHERE id = $1 AND status = $2", 
                [1, "active"]
            )

    @patch("vertebra.extensions.sqlalchemy._dbapi.SyncBrokerClient")
    def test_cursor_executemany(self, mock_client_cls):
        mock_client = mock_client_cls.return_value
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        
        with patch("vertebra.extensions.sqlalchemy._dbapi.VertebraSyncConnection", return_value=mock_conn):
            conn = connect("test-pool")
            cursor = conn.cursor()
            
            params = [[1], [2], [3]]
            mock_cursor.executemany.return_value = 3
            
            cursor.executemany("INSERT INTO t VALUES (?)", params)
            
            mock_cursor.executemany.assert_called_once_with("INSERT INTO t VALUES (?)", params)
            assert cursor.rowcount == 3

    @patch("vertebra.extensions.sqlalchemy._dbapi.SyncBrokerClient")
    def test_transaction_commit(self, mock_client_cls):
        mock_client = mock_client_cls.return_value
        mock_conn = MagicMock()
        
        with patch("vertebra.extensions.sqlalchemy._dbapi.VertebraSyncConnection", return_value=mock_conn):
            conn = connect("test-pool")
            conn._transaction_in_progress = True
            
            conn.commit()
            mock_conn.commit.assert_called_once()
            assert conn._transaction_in_progress is False

    @patch("vertebra.extensions.sqlalchemy._dbapi.SyncBrokerClient")
    def test_transaction_rollback(self, mock_client_cls):
        mock_client = mock_client_cls.return_value
        mock_conn = MagicMock()
        
        with patch("vertebra.extensions.sqlalchemy._dbapi.VertebraSyncConnection", return_value=mock_conn):
            conn = connect("test-pool")
            conn._transaction_in_progress = True
            
            conn.rollback()
            mock_conn.rollback.assert_called_once()
            assert conn._transaction_in_progress is False