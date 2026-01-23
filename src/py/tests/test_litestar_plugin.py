"""Tests for Vertebra Litestar Plugin."""

from typing import Any
import pytest
from unittest.mock import MagicMock, patch

try:
    from litestar import Litestar, get
    from litestar.status_codes import HTTP_200_OK
    from litestar.testing import TestClient
    from vertebra.extensions.litestar import VertebraPlugin
    # Ensure dialects are registered
    import vertebra.extensions.sqlalchemy.postgres # noqa: F401
    HAS_LITESTAR = True
except ImportError:
    HAS_LITESTAR = False

try:
    import advanced_alchemy
    HAS_AA = True
except ImportError:
    HAS_AA = False

try:
    import sqlspec
    HAS_SQLSPEC = True
except ImportError:
    HAS_SQLSPEC = False


@pytest.mark.skipif(not HAS_LITESTAR, reason="Litestar not installed")
def test_plugin_registration() -> None:
    import uuid
    @get("/", sync_to_thread=False)
    def handler() -> dict[str, Any]:
        return {"id": uuid.uuid4()}

    app = Litestar(route_handlers=[handler], plugins=[VertebraPlugin()])
    with TestClient(app) as client:
        res = client.get("/")
        assert res.status_code == 200
        assert "id" in res.json()


@pytest.mark.skipif(not HAS_LITESTAR or not HAS_SQLSPEC, reason="Litestar or SQLSpec not installed")
def test_sqlspec_plugin_registration() -> None:
    from sqlspec.extensions.litestar import SQLSpecPlugin, LitestarConfig
    from vertebra.extensions.litestar.sqlspec import VertebraSQLSpecPlugin
    from vertebra.extensions.sqlspec.postgres import VertebraPostgresAsyncConfig
    
    # Mock the client to avoid SHM connection attempts
    with patch("vertebra.resources.broker.AsyncBrokerClient"):
        config = LitestarConfig(config=VertebraPostgresAsyncConfig(pool="main"))
        plugin = VertebraSQLSpecPlugin(config=config)
        
        app = Litestar(route_handlers=[], plugins=[plugin])
        assert app.type_encoders


@pytest.mark.skipif(not HAS_LITESTAR or not HAS_AA, reason="Litestar or Advanced Alchemy not installed")
def test_sqlalchemy_plugin_registration() -> None:
    from vertebra.extensions.litestar.sqlalchemy import VertebraSQLAlchemyPlugin
    from sqlalchemy.ext.asyncio import create_async_engine
    
    # SQLAlchemyPlugin requires a config
    # We need to mock the client/SHM
    with patch("vertebra.resources.broker.SyncBrokerClient"):
        mock_engine = create_async_engine("postgresql+vertebra://main")
        
        from advanced_alchemy.extensions.litestar import SQLAlchemyAsyncConfig
        config = SQLAlchemyAsyncConfig(engine_instance=mock_engine)
        
        plugin = VertebraSQLAlchemyPlugin(config=config)
        
        app = Litestar(route_handlers=[], plugins=[plugin])
        assert app.type_encoders


@pytest.mark.skipif(not HAS_LITESTAR or not HAS_AA, reason="Litestar or Advanced Alchemy not installed")
def test_advanced_alchemy_plugin_registration() -> None:
    from vertebra.extensions.litestar.advanced_alchemy import VertebraAdvancedAlchemyPlugin
    from vertebra.extensions.advanced_alchemy.postgres import VertebraPostgresAlchemyAsyncConfig
    
    with patch("vertebra.resources.broker.SyncBrokerClient"):
        config = VertebraPostgresAlchemyAsyncConfig(pool="main")
        plugin = VertebraAdvancedAlchemyPlugin(config=config)
        
        app = Litestar(route_handlers=[], plugins=[plugin])
        assert app.type_encoders