"""Unit tests for broker provider registration and configuration."""

from vertebra.common.exceptions import BrokerError, VertebraError
from vertebra.messaging.broker import AsyncBrokerClient, BrokerClientConfig


class TestBrokerClientConfig:
    """Test BrokerClientConfig defaults and ring naming."""

    def test_config_defaults(self) -> None:
        """Test BrokerClientConfig has correct defaults."""
        config = BrokerClientConfig()
        assert config.name == "vertebra-broker"
        assert config.slot_size == 65536
        assert config.slots == 1024
        assert config.timeout_ms == 5000
        assert config.weight == 1

    def test_config_custom(self) -> None:
        """Test BrokerClientConfig accepts custom values."""
        config = BrokerClientConfig(name="custom-broker", slot_size=4096, slots=64, timeout_ms=1000, weight=10)
        assert config.name == "custom-broker"
        assert config.slot_size == 4096
        assert config.slots == 64
        assert config.timeout_ms == 1000
        assert config.weight == 10

    def test_ring_names(self) -> None:
        """Test ring naming logic."""
        config = BrokerClientConfig(name="my-broker")
        client_id = "test-client"
        assert config.control_ring().name == "/my-broker.control"
        assert config.request_ring(client_id).name == "/my-broker.client.test-client.req"
        assert config.response_ring(client_id).name == "/my-broker.client.test-client.resp"

    def test_custom_control_name(self) -> None:
        """Test custom control ring name."""
        config = BrokerClientConfig(name="my-broker", control_name="/custom-control")
        assert config.control_ring().name == "/custom-control"


class TestAsyncBrokerClient:
    """Test AsyncBrokerClient initialization."""

    def test_client_init_defaults(self) -> None:
        """Test AsyncBrokerClient with default config."""
        client = AsyncBrokerClient()
        assert client._config.name == "vertebra-broker"

    def test_client_init_custom(self) -> None:
        """Test AsyncBrokerClient with custom config."""
        config = BrokerClientConfig(name="custom-broker", client_id="my-client")
        client = AsyncBrokerClient(config)
        assert client._config.name == "custom-broker"
        assert client._client_id == "my-client"

    def test_client_id_generation(self) -> None:
        """Test AsyncBrokerClient generates unique client IDs."""
        client1 = AsyncBrokerClient()
        client2 = AsyncBrokerClient()
        assert client1._client_id != client2._client_id


class TestBrokerError:
    """Test BrokerError exception."""

    def test_error_subclass(self) -> None:
        """Test BrokerError is a VertebraError subclass."""
        assert issubclass(BrokerError, VertebraError)

    def test_error_message(self) -> None:
        """Test BrokerError preserves message."""
        error = BrokerError("Connection failed")
        assert str(error) == "Connection failed"
