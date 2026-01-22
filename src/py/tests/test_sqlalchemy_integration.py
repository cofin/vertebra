"""Integration tests for Vertebra SQLAlchemy transactions."""

from unittest.mock import MagicMock, patch
import pytest
from sqlalchemy import create_engine, text

# Ensure dialects are registered
import vertebra.extensions.sqlalchemy.postgres  # noqa: F401


class TestSQLAlchemyTransactions:
    @patch("vertebra.extensions.sqlalchemy._dbapi.SyncBrokerClient")
    def test_nested_transactions_savepoints(self, mock_client_cls):
        mock_client = mock_client_cls.return_value
        mock_client.acquire.return_value = "fake-token"
        
        # Mock connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        
        # Mock result for version check etc
        mock_cursor.fetchone.return_value = ("PostgreSQL 15.0",)
        
        with patch("vertebra.extensions.sqlalchemy._dbapi.VertebraSyncConnection", return_value=mock_conn):
            engine = create_engine("postgresql+vertebra_sync://main")
            
            with engine.begin() as conn:
                # Top level transaction already began
                
                # Create a savepoint
                with conn.begin_nested() as sp:
                    # SQLAlchemy should have called our savepoint method
                    mock_conn.savepoint.assert_called()
                    savepoint_name = mock_conn.savepoint.call_args[0][0]
                    assert savepoint_name is not None
                    
                    conn.execute(text("SELECT 1"))
                
                # Verify release_savepoint was called
                mock_conn.release_savepoint.assert_called_with(savepoint_name)
                
            # Verify commit was called for top-level
            mock_conn.commit.assert_called()

    @patch("vertebra.extensions.sqlalchemy._dbapi.SyncBrokerClient")
    def test_savepoint_rollback(self, mock_client_cls):
        mock_client = mock_client_cls.return_value
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = ("PostgreSQL 15.0",)
        
        with patch("vertebra.extensions.sqlalchemy._dbapi.VertebraSyncConnection", return_value=mock_conn):
            engine = create_engine("postgresql+vertebra_sync://main")
            
            with engine.connect() as conn:
                trans = conn.begin()
                
                # Execute something to ensure the transaction flag is set
                # Wait, begin() should set it too.
                conn.execute(text("SELECT 1"))
                
                try:
                    with conn.begin_nested():
                        mock_conn.savepoint.assert_called()
                        savepoint_name = mock_conn.savepoint.call_args[0][0]
                        raise ValueError("force rollback")
                except ValueError:
                    pass
                
                # Verify rollback_savepoint was called
                mock_conn.rollback_savepoint.assert_called_with(savepoint_name)
                
                trans.commit()
                mock_conn.commit.assert_called()