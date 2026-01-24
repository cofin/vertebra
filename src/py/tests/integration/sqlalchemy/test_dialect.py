"""Tests for Vertebra SQLAlchemy dialects."""

from unittest.mock import patch

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.ext.asyncio import create_async_engine

import vertebra.extensions.sqlalchemy.mysql

# Ensure dialects are registered
import vertebra.extensions.sqlalchemy.postgres
import vertebra.extensions.sqlalchemy.sqlite  # noqa: F401


class TestPostgresDialect:
    @patch("vertebra.extensions.sqlalchemy._dbapi.SyncBrokerClient")
    def test_create_engine_sync(self, mock_client_cls) -> None:
        mock_client = mock_client_cls.return_value
        mock_client.acquire.return_value = "fake-token"

        with patch("vertebra.extensions.sqlalchemy._dbapi.Cursor") as mock_cursor_cls:
            mock_cursor = mock_cursor_cls.return_value
            mock_cursor.description = [("val", None, None, None, None, None, None)]
            mock_cursor.rowcount = 1

            mock_cursor.fetchone.return_value = ("PostgreSQL 15.0",)

            engine = create_engine("postgresql+vertebra_sync://main")
            with engine.connect() as conn:
                result = conn.execute(text("SELECT 1")).fetchone()
                assert result is not None

    @pytest.mark.anyio
    async def test_create_engine_async(self) -> None:
        engine = create_async_engine("postgresql+vertebra://main")
        assert engine.dialect.name == "postgresql"
        assert engine.dialect.driver == "vertebra"
        assert engine.dialect.is_async is True


class TestMySQLDialect:
    @patch("vertebra.extensions.sqlalchemy._dbapi.SyncBrokerClient")
    def test_create_engine_sync(self, mock_client_cls) -> None:
        mock_client = mock_client_cls.return_value
        mock_client.acquire.return_value = "fake-token"

        with patch("vertebra.extensions.sqlalchemy._dbapi.Cursor") as mock_cursor_cls:
            mock_cursor = mock_cursor_cls.return_value
            mock_cursor.description = [("val", None, None, None, None, None, None)]
            mock_cursor.rowcount = 1

            # Simple stateful mock
            state = {"version_done": False}

            def side_effect() -> tuple[str]:
                if not state["version_done"]:
                    state["version_done"] = True
                    return ("8.0.30",)
                return ("0",)  # Default for other initialization checks

            mock_cursor.fetchone.side_effect = side_effect

            engine = create_engine("mysql+vertebra_sync://main")
            assert engine.dialect.name == "mysql"

            with engine.connect() as conn:
                result = conn.execute(text("SELECT 1")).fetchone()
                assert result is not None

    @pytest.mark.anyio
    async def test_create_engine_async(self) -> None:
        engine = create_async_engine("mysql+vertebra://main")
        assert engine.dialect.name == "mysql"
        assert engine.dialect.driver == "vertebra"
        assert engine.dialect.is_async is True


class TestSQLiteDialect:
    @patch("vertebra.extensions.sqlalchemy._dbapi.SyncBrokerClient")
    def test_create_engine_sync(self, mock_client_cls) -> None:
        mock_client = mock_client_cls.return_value
        mock_client.acquire.return_value = "fake-token"

        with patch("vertebra.extensions.sqlalchemy._dbapi.Cursor") as mock_cursor_cls:
            mock_cursor = mock_cursor_cls.return_value
            mock_cursor.description = [("val", None, None, None, None, None, None)]
            mock_cursor.rowcount = 1

            mock_cursor.fetchone.return_value = (0,)

            engine = create_engine("sqlite+vertebra_sync://main")
            assert engine.dialect.name == "sqlite"

            with engine.connect() as conn:
                result = conn.execute(text("SELECT 1")).fetchone()
                assert result is not None

    @pytest.mark.anyio
    async def test_create_engine_async(self) -> None:
        engine = create_async_engine("sqlite+vertebra://main")
        assert engine.dialect.name == "sqlite"
        assert engine.dialect.driver == "vertebra"
        assert engine.dialect.is_async is True
