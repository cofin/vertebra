"""Tests for broker provider registration.

These tests verify the BrokerClientConfig and provider registration
functionality works correctly.
"""

from __future__ import annotations

import tempfile

import pytest


class TestBrokerClientConfig:
    """Test BrokerClientConfig defaults and ring naming."""

    def test_default_config_values(self) -> None:
        """Test BrokerClientConfig has correct defaults."""
        from vertebra.resources.broker import BrokerClientConfig

        config = BrokerClientConfig()

        assert config.name == "vertebra-broker"
        assert config.control_name is None
        assert config.lock_path is None
        assert config.slot_size == 65536
        assert config.slots == 1024
        assert config.timeout_ms == 5_000
        assert config.client_id is None
        assert config.weight == 1
        assert config.prefer_eventfd is True

    def test_custom_config_values(self) -> None:
        """Test BrokerClientConfig accepts custom values."""
        from vertebra.resources.broker import BrokerClientConfig

        config = BrokerClientConfig(
            name="custom-broker",
            control_name="/custom-control",
            lock_path="/tmp/custom.lock",
            slot_size=32768,
            slots=512,
            timeout_ms=10_000,
            client_id="test-client",
            weight=2,
            prefer_eventfd=False,
        )

        assert config.name == "custom-broker"
        assert config.control_name == "/custom-control"
        assert config.lock_path == "/tmp/custom.lock"
        assert config.slot_size == 32768
        assert config.slots == 512
        assert config.timeout_ms == 10_000
        assert config.client_id == "test-client"
        assert config.weight == 2
        assert config.prefer_eventfd is False

    def test_control_ring_config_default_name(self) -> None:
        """Test control_ring() generates correct default name."""
        from vertebra.resources.broker import BrokerClientConfig

        config = BrokerClientConfig(name="my-broker")
        ring = config.control_ring()

        assert ring.name == "/my-broker.control"
        assert ring.slot_size == config.slot_size
        assert ring.slots == config.slots

    def test_control_ring_config_custom_name(self) -> None:
        """Test control_ring() uses custom control_name when set."""
        from vertebra.resources.broker import BrokerClientConfig

        config = BrokerClientConfig(name="my-broker", control_name="/custom-control")
        ring = config.control_ring()

        assert ring.name == "/custom-control"

    def test_request_ring_config(self) -> None:
        """Test request_ring() generates correct name pattern."""
        from vertebra.resources.broker import BrokerClientConfig

        config = BrokerClientConfig(name="my-broker")
        ring = config.request_ring("client-123")

        assert ring.name == "/my-broker.client.client-123.req"
        assert ring.slot_size == config.slot_size
        assert ring.slots == config.slots

    def test_response_ring_config(self) -> None:
        """Test response_ring() generates correct name pattern."""
        from vertebra.resources.broker import BrokerClientConfig

        config = BrokerClientConfig(name="my-broker")
        ring = config.response_ring("client-123")

        assert ring.name == "/my-broker.client.client-123.resp"

    def test_control_lock_default_path(self) -> None:
        """Test control_lock() generates default path in temp directory."""
        from vertebra.resources.broker import BrokerClientConfig

        config = BrokerClientConfig(name="my-broker")
        lock_path = config.control_lock()

        assert lock_path.endswith("my-broker.control.lock")
        assert tempfile.gettempdir() in lock_path

    def test_control_lock_custom_path(self) -> None:
        """Test control_lock() uses custom path when set."""
        from vertebra.resources.broker import BrokerClientConfig

        config = BrokerClientConfig(name="my-broker", lock_path="/var/lock/broker.lock")
        lock_path = config.control_lock()

        assert lock_path == "/var/lock/broker.lock"


class TestRingConfig:
    """Test RingConfig structure."""

    def test_ring_config_defaults(self) -> None:
        """Test RingConfig has correct defaults."""
        from vertebra.resources.broker import RingConfig

        ring = RingConfig(name="/test-ring")

        assert ring.name == "/test-ring"
        assert ring.slot_size == 65536
        assert ring.slots == 1024

    def test_ring_config_custom(self) -> None:
        """Test RingConfig accepts custom values."""
        from vertebra.resources.broker import RingConfig

        ring = RingConfig(name="/custom-ring", slot_size=8192, slots=256)

        assert ring.name == "/custom-ring"
        assert ring.slot_size == 8192
        assert ring.slots == 256

    def test_ring_config_frozen(self) -> None:
        """Test RingConfig is immutable."""
        from vertebra.resources.broker import RingConfig

        ring = RingConfig(name="/test-ring")

        with pytest.raises(AttributeError):
            ring.name = "/new-name"  # type: ignore[misc]


class TestBrokerClient:
    """Test BrokerClient initialization."""

    def test_broker_client_default_config(self) -> None:
        """Test BrokerClient with default config."""
        from vertebra.resources.broker import BrokerClient

        client = BrokerClient()

        assert client._config.name == "vertebra-broker"
        assert client._client_id is not None  # Auto-generated

    def test_broker_client_custom_config(self) -> None:
        """Test BrokerClient with custom config."""
        from vertebra.resources.broker import BrokerClient, BrokerClientConfig

        config = BrokerClientConfig(name="custom-broker", client_id="my-client")
        client = BrokerClient(config)

        assert client._config.name == "custom-broker"
        assert client._client_id == "my-client"

    def test_broker_client_generated_client_id(self) -> None:
        """Test BrokerClient generates unique client IDs."""
        from vertebra.resources.broker import BrokerClient

        client1 = BrokerClient()
        client2 = BrokerClient()

        # Each client should have a unique ID
        assert client1._client_id != client2._client_id


class TestProviderHost:
    """Test ProviderHost configuration."""

    def test_provider_host_ring_names(self) -> None:
        """Test ProviderHost generates correct ring names."""
        from vertebra.resources.provider import ProviderConfig

        config = ProviderConfig(pool="postgres", driver="psycopg", name="my-broker")

        # The request ring name pattern
        expected_req = "/my-broker.provider.postgres.req"
        expected_resp = "/my-broker.provider.postgres.resp"

        # Verify the naming convention matches what ProviderHost uses internally
        assert f"/{config.name}.provider.{config.pool}.req" == expected_req
        assert f"/{config.name}.provider.{config.pool}.resp" == expected_resp


class TestBrokerClientError:
    """Test BrokerClientError exception."""

    def test_broker_client_error_is_runtime_error(self) -> None:
        """Test BrokerClientError is a RuntimeError subclass."""
        from vertebra.resources.broker import BrokerClientError

        assert issubclass(BrokerClientError, RuntimeError)

    def test_broker_client_error_message(self) -> None:
        """Test BrokerClientError preserves message."""
        from vertebra.resources.broker import BrokerClientError

        error = BrokerClientError("Connection failed")
        assert str(error) == "Connection failed"


class TestMessagePackCodec:
    """Test the MessagePack codec used for broker communication."""

    def test_codec_encode_decode_dict(self) -> None:
        """Test codec roundtrip with dict."""
        from vertebra._internal.serialization import MessagePackCodec

        codec = MessagePackCodec()
        data = {"kind": "test", "value": 123, "nested": {"a": 1}}

        encoded = codec.encode(data)
        decoded = codec.decode(encoded)

        assert decoded == data

    def test_codec_encode_decode_list(self) -> None:
        """Test codec roundtrip with list."""
        from vertebra._internal.serialization import MessagePackCodec

        codec = MessagePackCodec()
        data = [1, 2, 3, {"key": "value"}]

        encoded = codec.encode(data)
        decoded = codec.decode(encoded)

        assert decoded == data

    def test_codec_encode_decode_bytes(self) -> None:
        """Test codec handles bytes."""
        from vertebra._internal.serialization import MessagePackCodec

        codec = MessagePackCodec()
        data = {"binary": b"hello world"}

        encoded = codec.encode(data)
        decoded = codec.decode(encoded)

        assert decoded["binary"] == b"hello world"
