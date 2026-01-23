from unittest.mock import MagicMock, patch
from litestar import Litestar
from vertebra.extensions.litestar import VertebraPlugin
from vertebra.extensions.litestar.sqlalchemy import VertebraSQLAlchemyPlugin
from vertebra.extensions.litestar.advanced_alchemy import VertebraAdvancedAlchemyPlugin
from sqlalchemy.ext.asyncio import create_async_engine
from advanced_alchemy.extensions.litestar import SQLAlchemyAsyncConfig

# Register dialects
import vertebra.extensions.sqlalchemy.postgres # noqa: F401

def verify_plugins():
    print("Verifying Litestar Plugins...")
    
    # 1. VertebraPlugin
    app1 = Litestar(route_handlers=[], plugins=[VertebraPlugin()])
    assert app1.type_encoders
    print("VertebraPlugin registered successfully.")
    
    # 2. VertebraSQLAlchemyPlugin
    with patch("vertebra.resources.broker.SyncBrokerClient"):
        mock_engine = create_async_engine("postgresql+vertebra://main")
        config2 = SQLAlchemyAsyncConfig(engine_instance=mock_engine)
        plugin2 = VertebraSQLAlchemyPlugin(config=config2)
        app2 = Litestar(route_handlers=[], plugins=[plugin2])
        assert app2.type_encoders
        print("VertebraSQLAlchemyPlugin registered successfully.")
        
    # 3. VertebraAdvancedAlchemyPlugin
    with patch("vertebra.resources.broker.SyncBrokerClient"):
        from vertebra.extensions.advanced_alchemy.postgres import VertebraPostgresAlchemyAsyncConfig
        config3 = VertebraPostgresAlchemyAsyncConfig(pool="main")
        plugin3 = VertebraAdvancedAlchemyPlugin(config=config3)
        app3 = Litestar(route_handlers=[], plugins=[plugin3])
        assert app3.type_encoders
        print("VertebraAdvancedAlchemyPlugin registered successfully.")

if __name__ == "__main__":
    verify_plugins()
    print("\nPhase 6 verification passed!")