"""Tests for Vertebra Litestar Plugin."""

from typing import Any
from unittest.mock import patch

import pytest

try:
    from litestar import Litestar, get
    from litestar.testing import TestClient

    # Ensure dialects are registered
    import vertebra.extensions.sqlalchemy.postgres  # noqa: F401
    from vertebra.extensions.litestar import VertebraPlugin

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
    """Test VertebraPlugin registers type encoders correctly."""
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
def test_sqlspec_config_instantiation() -> None:
    """Test Vertebra SQLSpec configs can be instantiated."""
    from vertebra.extensions.sqlspec.postgres import VertebraPostgresAsyncConfig

    # Mock the client to avoid SHM connection attempts
    with patch("vertebra.resources.broker.AsyncBrokerClient"):
        config = VertebraPostgresAsyncConfig(pool="main")
        assert config is not None
        assert config.pool == "main"


@pytest.mark.skipif(not HAS_LITESTAR or not HAS_AA, reason="Litestar or Advanced Alchemy not installed")
def test_sqlalchemy_with_vertebra_config() -> None:
    """Test standard SQLAlchemyPlugin works with Vertebra configs."""
    from advanced_alchemy.extensions.litestar import SQLAlchemyAsyncConfig, SQLAlchemyPlugin
    from sqlalchemy.ext.asyncio import create_async_engine

    # Use Vertebra's dialect URL format
    with patch("vertebra.resources.broker.SyncBrokerClient"):
        mock_engine = create_async_engine("postgresql+vertebra://main")
        config = SQLAlchemyAsyncConfig(engine_instance=mock_engine)

        # Use standard SQLAlchemyPlugin with Vertebra-based engine
        app = Litestar(route_handlers=[], plugins=[VertebraPlugin(), SQLAlchemyPlugin(config=config)])
        assert app.type_encoders


@pytest.mark.skipif(not HAS_LITESTAR or not HAS_AA, reason="Litestar or Advanced Alchemy not installed")
def test_advanced_alchemy_with_vertebra_config() -> None:
    """Test standard SQLAlchemyPlugin works with Vertebra Advanced Alchemy configs."""
    from advanced_alchemy.extensions.litestar import SQLAlchemyPlugin

    from vertebra.extensions.advanced_alchemy.postgres import VertebraPostgresAsyncConfig

    with patch("vertebra.resources.broker.SyncBrokerClient"):
        config = VertebraPostgresAsyncConfig(pool="main")

        # Use standard SQLAlchemyPlugin with Vertebra config
        app = Litestar(route_handlers=[], plugins=[VertebraPlugin(), SQLAlchemyPlugin(config=config)])
        assert app.type_encoders
