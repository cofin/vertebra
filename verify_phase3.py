
import asyncio
from unittest.mock import MagicMock, patch
from sqlalchemy import create_engine, text
from sqlalchemy.ext.asyncio import create_async_engine

# Ensure dialects are registered
import vertebra.extensions.sqlalchemy.postgres
import vertebra.extensions.sqlalchemy.mysql
import vertebra.extensions.sqlalchemy.sqlite

def verify_dialects():
    print("Verifying SQLAlchemy Dialects...")
    
    # Postgres
    e_pg_sync = create_engine("postgresql+vertebra_sync://main")
    print("Sync engine (Postgres) created successfully.")
    e_pg_async = create_async_engine("postgresql+vertebra://main")
    print("Async engine (Postgres) created successfully.")
    
    # MySQL
    e_my_sync = create_engine("mysql+vertebra_sync://main")
    print("Sync engine (MySQL) created successfully.")
    e_my_async = create_async_engine("mysql+vertebra://main")
    print("Async engine (MySQL) created successfully.")
    
    # SQLite
    e_sq_sync = create_engine("sqlite+vertebra_sync://main")
    print("Sync engine (SQLite) created successfully.")
    e_sq_async = create_async_engine("sqlite+vertebra://main")
    print("Async engine (SQLite) created successfully.")

async def verify_transactions():
    print("\nVerifying Transaction Lifecycle...")
    
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.fetchone.return_value = ("PostgreSQL 15.0",)
    
    with patch("vertebra.extensions.sqlalchemy._dbapi.SyncBrokerClient") as mock_client_cls:
        mock_client = mock_client_cls.return_value
        mock_client.acquire.return_value = "fake-token"
        
        with patch("vertebra.extensions.sqlalchemy._dbapi.VertebraSyncConnection", return_value=mock_conn):
            engine = create_engine("postgresql+vertebra_sync://main")
            
            # Test top-level transaction
            with engine.begin() as conn:
                mock_conn.begin.assert_called()
                conn.execute(text("SELECT 1"))
            mock_conn.commit.assert_called()
            print("Transaction begin/commit sequence verified.")
            
            # Test nested transaction
            mock_conn.reset_mock()
            with engine.connect() as conn:
                trans = conn.begin()
                conn.execute(text("SELECT 1"))
                with conn.begin_nested():
                    mock_conn.savepoint.assert_called()
                    conn.execute(text("SELECT 2"))
                mock_conn.release_savepoint.assert_called()
                trans.commit()
            print("Nested transaction (savepoint) sequence verified.")

if __name__ == "__main__":
    verify_dialects()
    asyncio.run(verify_transactions())
    print("\nPhase 3 verification passed!")
