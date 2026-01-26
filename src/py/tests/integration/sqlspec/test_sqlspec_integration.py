"""Tests for SQLSpec broker integration.

These tests verify the broker-backed SQLSpec adapter configuration
and proxy wrappers work correctly.
"""

from unittest.mock import MagicMock

import pytest

# Check if sqlspec is available
try:
    import sqlspec  # noqa: F401

    HAS_SQLSPEC = True
except ImportError:
    HAS_SQLSPEC = False

sqlspec_required = pytest.mark.skipif(not HAS_SQLSPEC, reason="sqlspec not installed")

pytestmark = pytest.mark.integration


@sqlspec_required
class TestBrokerConfigBase:
    """Test the base broker config classes."""

    def test_sync_config_defaults(self) -> None:
        """Test VertebraSyncConfigBase uses correct defaults."""
        from sqlspec.core import get_default_config

        from vertebra.extensions.sqlspec._config import VertebraSyncConfigBase
        from vertebra.extensions.sqlspec._driver import VertebraSyncDriverBase
        from vertebra.resources import BrokerClientConfig

        # Create a concrete implementation
        class TestSyncConfig(VertebraSyncConfigBase):
            driver_type = VertebraSyncDriverBase
            default_statement_config = get_default_config()
            default_pool_name = "test-pool"

        config = TestSyncConfig()

        assert config.pool == "test-pool"
        assert isinstance(config.client_config, BrokerClientConfig)
        assert config.max_clients == 5
        assert config.connection_config == {"pool": "test-pool"}

    def test_sync_config_custom_values(self) -> None:
        """Test VertebraSyncConfigBase accepts custom values."""
        from sqlspec.core import get_default_config

        from vertebra.extensions.sqlspec._config import VertebraSyncConfigBase
        from vertebra.extensions.sqlspec._driver import VertebraSyncDriverBase
        from vertebra.resources import BrokerClientConfig

        class TestSyncConfig(VertebraSyncConfigBase):
            driver_type = VertebraSyncDriverBase
            default_statement_config = get_default_config()
            default_pool_name = "default-pool"

        custom_client = BrokerClientConfig(name="custom-broker")
        config = TestSyncConfig(pool="custom-pool", client_config=custom_client, max_clients=10)

        assert config.pool == "custom-pool"
        assert config.client_config.name == "custom-broker"
        assert config.max_clients == 10

    def test_async_config_defaults(self) -> None:
        """Test VertebraAsyncConfigBase uses correct defaults."""
        from sqlspec.core import get_default_config

        from vertebra.extensions.sqlspec._config import VertebraAsyncConfigBase
        from vertebra.extensions.sqlspec._driver import VertebraAsyncDriverBase
        from vertebra.resources import BrokerClientConfig

        class TestAsyncConfig(VertebraAsyncConfigBase):
            driver_type = VertebraAsyncDriverBase
            default_statement_config = get_default_config()
            default_pool_name = "async-pool"

        config = TestAsyncConfig()

        assert config.pool == "async-pool"
        assert isinstance(config.client_config, BrokerClientConfig)
        assert config.max_clients == 5


@sqlspec_required
class TestBrokerProxyConfig:
    """Test the broker proxy config wrappers."""

    def test_sync_proxy_wraps_target_config(self) -> None:
        """Test VertebraProxySyncConfig correctly wraps a target config."""
        from sqlspec.config import SyncDatabaseConfig
        from sqlspec.core import get_default_config

        from vertebra.extensions.sqlspec.proxy import VertebraProxySyncConfig

        # Create a mock target config
        mock_config = MagicMock(spec=SyncDatabaseConfig)
        mock_config.statement_config = get_default_config().replace(dialect="postgres")
        mock_config.driver_features = {}
        mock_config.bind_key = None
        mock_config.migration_config = None
        mock_config.extension_config = {}

        proxy = VertebraProxySyncConfig(mock_config, pool="test-proxy-pool")

        assert proxy.target_config is mock_config
        assert proxy.pool == "test-proxy-pool"

    def test_async_proxy_wraps_target_config(self) -> None:
        """Test VertebraProxyAsyncConfig correctly wraps a target config."""
        from sqlspec.config import AsyncDatabaseConfig
        from sqlspec.core import get_default_config

        from vertebra.extensions.sqlspec.proxy import VertebraProxyAsyncConfig

        mock_config = MagicMock(spec=AsyncDatabaseConfig)
        mock_config.statement_config = get_default_config().replace(dialect="postgres")
        mock_config.driver_features = {}
        mock_config.bind_key = None
        mock_config.migration_config = None
        mock_config.extension_config = {}

        proxy = VertebraProxyAsyncConfig(mock_config, pool="async-proxy-pool")

        assert proxy.target_config is mock_config
        assert proxy.pool == "async-proxy-pool"

    def test_proxy_uses_bind_key_as_default_pool(self) -> None:
        """Test proxy uses target config's bind_key if pool not specified."""
        from sqlspec.config import SyncDatabaseConfig
        from sqlspec.core import get_default_config

        from vertebra.extensions.sqlspec.proxy import VertebraProxySyncConfig

        mock_config = MagicMock(spec=SyncDatabaseConfig)
        mock_config.statement_config = get_default_config().replace(dialect="postgres")
        mock_config.driver_features = {}
        mock_config.bind_key = "my-database"
        mock_config.migration_config = None
        mock_config.extension_config = {}

        proxy = VertebraProxySyncConfig(mock_config)

        assert proxy.pool == "my-database"


class TestProviderConfig:
    """Test the ProviderConfig for SQLSpec providers."""

    def test_provider_config_defaults(self) -> None:
        """Test ProviderConfig has correct defaults."""
        from vertebra.resources.provider import ProviderConfig

        config = ProviderConfig(pool="test-pool", driver="test-driver")

        assert config.pool == "test-pool"
        assert config.driver == "test-driver"
        assert config.max_size == 10
        assert config.name == "vertebra-broker"
        assert config.slot_size == 65536
        assert config.slots == 1024
        assert config.arrow_threshold_rows == 1_000
        assert config.arrow_threshold_bytes == 1_048_576

    def test_provider_config_custom_values(self) -> None:
        """Test ProviderConfig accepts custom values."""
        from vertebra.resources.provider import ProviderConfig

        config = ProviderConfig(
            pool="custom-pool",
            driver="custom-driver",
            max_size=20,
            name="custom-broker",
            slot_size=32768,
            slots=512,
            arrow_threshold_rows=500,
            arrow_threshold_bytes=512_000,
        )

        assert config.pool == "custom-pool"
        assert config.driver == "custom-driver"
        assert config.max_size == 20
        assert config.name == "custom-broker"
        assert config.slot_size == 32768
        assert config.slots == 512
        assert config.arrow_threshold_rows == 500
        assert config.arrow_threshold_bytes == 512_000


@sqlspec_required
class TestBrokerConnection:
    """Test broker connection types."""

    def test_sync_broker_connection_token(self) -> None:
        """Test VertebraSyncConnection stores token correctly."""
        from vertebra.database.backends.broker import VertebraSyncConnection
        from vertebra.resources import AsyncBrokerClient, BrokerClientConfig

        client = AsyncBrokerClient(BrokerClientConfig(name="test-broker"))
        conn = VertebraSyncConnection(pool="test-pool", client=client, token="test-token-123")
        assert conn.token == "test-token-123"

    def test_async_broker_connection_token(self) -> None:
        """Test VertebraAsyncConnection stores token correctly."""
        from vertebra.database.backends.broker import VertebraAsyncConnection
        from vertebra.resources import AsyncBrokerClient, BrokerClientConfig

        client = AsyncBrokerClient(BrokerClientConfig(name="test-broker"))
        conn = VertebraAsyncConnection(pool="test-pool", client=client, token="async-token-456")
        assert conn.token == "async-token-456"


@sqlspec_required
class TestDataDictionaryResolution:
    """Test dialect-specific data dictionary resolution."""

    def test_sync_postgres_data_dictionary(self) -> None:
        """Test sync postgres data dictionary is resolved."""
        from vertebra.extensions.sqlspec._dictionary import resolve_sync_data_dictionary

        factory = resolve_sync_data_dictionary("postgresql")
        # Should return a callable or class
        assert callable(factory)

    def test_sync_mysql_data_dictionary(self) -> None:
        """Test sync mysql data dictionary is resolved."""
        from vertebra.extensions.sqlspec._dictionary import resolve_sync_data_dictionary

        factory = resolve_sync_data_dictionary("mysql")
        assert callable(factory)

    def test_sync_sqlite_data_dictionary(self) -> None:
        """Test sync sqlite data dictionary is resolved."""
        from vertebra.extensions.sqlspec._dictionary import resolve_sync_data_dictionary

        factory = resolve_sync_data_dictionary("sqlite")
        assert callable(factory)

    def test_async_postgres_data_dictionary(self) -> None:
        """Test async postgres data dictionary is resolved."""
        from vertebra.extensions.sqlspec._dictionary import resolve_async_data_dictionary

        factory = resolve_async_data_dictionary("postgresql")
        assert callable(factory)

    def test_unknown_dialect_returns_none(self) -> None:
        """Test unknown dialect raises error."""
        from sqlspec.exceptions import ImproperConfigurationError

        from vertebra.extensions.sqlspec._dictionary import resolve_sync_data_dictionary

        with pytest.raises(ImproperConfigurationError):
            resolve_sync_data_dictionary("unknown-dialect")


@sqlspec_required
class TestSpecificConfigs:
    """Test dialect-specific broker configs."""

    def test_postgres_config_exists(self) -> None:
        """Test PostgreSQL broker config can be imported."""
        from vertebra.extensions.sqlspec.postgres import VertebraPostgresAsyncConfig, VertebraPostgresSyncConfig

        assert VertebraPostgresSyncConfig is not None
        assert VertebraPostgresAsyncConfig is not None

    def test_mysql_config_exists(self) -> None:
        """Test MySQL broker config can be imported."""
        from vertebra.extensions.sqlspec.mysql import VertebraMysqlAsyncConfig, VertebraMysqlSyncConfig

        assert VertebraMysqlSyncConfig is not None
        assert VertebraMysqlAsyncConfig is not None

    def test_sqlite_config_exists(self) -> None:
        """Test SQLite broker config can be imported."""
        from vertebra.extensions.sqlspec.sqlite import VertebraSqliteAsyncConfig, VertebraSqliteSyncConfig

        assert VertebraSqliteSyncConfig is not None
        assert VertebraSqliteAsyncConfig is not None

    def test_postgres_config_dialect(self) -> None:
        """Test PostgreSQL config has correct dialect."""
        from vertebra.extensions.sqlspec.postgres import VertebraPostgresSyncConfig

        config = VertebraPostgresSyncConfig()
        assert config.statement_config.dialect == "postgres"

    def test_mysql_config_dialect(self) -> None:
        """Test MySQL config has correct dialect."""
        from vertebra.extensions.sqlspec.mysql import VertebraMysqlSyncConfig

        config = VertebraMysqlSyncConfig()
        assert config.statement_config.dialect == "mysql"

    def test_sqlite_config_dialect(self) -> None:
        """Test SQLite config has correct dialect."""
        from vertebra.extensions.sqlspec.sqlite import VertebraSqliteSyncConfig

        config = VertebraSqliteSyncConfig()
        assert config.statement_config.dialect == "sqlite"


@sqlspec_required
class TestDriverBase:
    """Test broker driver base classes."""

    def test_sync_driver_base_dialect(self) -> None:
        """Test sync driver base has dialect attribute."""
        from vertebra.extensions.sqlspec._driver import VertebraSyncDriverBase

        assert hasattr(VertebraSyncDriverBase, "dialect")

    def test_async_driver_base_dialect(self) -> None:
        """Test async driver base has dialect attribute."""
        from vertebra.extensions.sqlspec._driver import VertebraAsyncDriverBase

        assert hasattr(VertebraAsyncDriverBase, "dialect")
