"""Tests for banner collector module."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from vertebra.cli.banner.collector import ComponentStatus, ServerStatus, StatusCollector


class TestComponentStatus:
    """Test ComponentStatus dataclass."""

    def test_minimal_status(self) -> None:
        status = ComponentStatus(status="ready")
        assert status.status == "ready"
        assert status.version is None
        assert status.uptime_ms is None

    def test_full_status(self) -> None:
        status = ComponentStatus(
            status="ready",
            version="1.1.38",
            uptime_ms=12345,
            details={"key": "value"},
            clients=5,
            pools={"sql": 10, "redis": 5},
            channels=4,
            latency_p99_us=8,
        )
        assert status.status == "ready"
        assert status.version == "1.1.38"
        assert status.uptime_ms == 12345
        assert status.clients == 5
        assert status.pools == {"sql": 10, "redis": 5}
        assert status.channels == 4
        assert status.latency_p99_us == 8


class TestServerStatus:
    """Test ServerStatus dataclass."""

    def test_all_components(self) -> None:
        status = ServerStatus(
            bun=ComponentStatus(status="ready"),
            broker=ComponentStatus(status="starting"),
            ipc=ComponentStatus(status="error"),
        )
        assert status.bun.status == "ready"
        assert status.broker.status == "starting"
        assert status.ipc.status == "error"


class TestStatusCollector:
    """Test StatusCollector class."""

    def test_init_sets_timeout(self) -> None:
        collector = StatusCollector(host="localhost", port=3000, timeout=2.0)
        assert collector.timeout == 2.0

    def test_init_default_timeout(self) -> None:
        collector = StatusCollector()
        assert collector.timeout == 1.0

    def test_fetch_returns_none_on_ipc_error(self) -> None:
        collector = StatusCollector()

        with patch.object(collector, "_get_status_client") as mock_get_client:
            mock_client = MagicMock()
            mock_client.query_status.side_effect = OSError("IPC error")
            mock_get_client.return_value = mock_client
            result = collector.fetch()

        assert result is None

    def test_fetch_returns_none_when_status_client_returns_none(self) -> None:
        collector = StatusCollector()

        with patch.object(collector, "_get_status_client") as mock_get_client:
            mock_client = MagicMock()
            mock_client.query_status.return_value = None
            mock_get_client.return_value = mock_client
            result = collector.fetch()

        assert result is None

    def test_fetch_parses_response(self) -> None:
        collector = StatusCollector()

        mock_status_data = {
            "bun": {"status": "ready", "version": "1.1.38"},
            "broker": {"status": "ready", "clients": 3},
            "ipc": {"status": "ready", "channels": 4},
        }

        with patch.object(collector, "_get_status_client") as mock_get_client:
            mock_client = MagicMock()
            mock_client.query_status.return_value = mock_status_data
            mock_get_client.return_value = mock_client

            result = collector.fetch()

        assert result is not None
        assert result.bun.status == "ready"
        assert result.bun.version == "1.1.38"
        assert result.broker.status == "ready"
        assert result.ipc.status == "ready"
        assert result.ipc.channels == 4

    def test_context_manager(self) -> None:
        with StatusCollector() as collector:
            assert collector.timeout == 1.0

    def test_close_clears_status_client(self) -> None:
        collector = StatusCollector()
        collector._status_client = MagicMock()

        collector.close()

        assert collector._status_client is None


class TestStatusCollectorParseComponent:
    """Test the _parse_component method."""

    def test_parses_minimal_data(self) -> None:
        collector = StatusCollector()
        data = {"status": "ready"}

        result = collector._parse_component(data)

        assert result.status == "ready"
        assert result.version is None

    def test_parses_full_data(self) -> None:
        collector = StatusCollector()
        data = {
            "status": "ready",
            "version": "1.0.0",
            "uptime_ms": 5000,
            "clients": 3,
            "pools": {"sql": 10},
            "channels": 4,
        }

        result = collector._parse_component(data)

        assert result.status == "ready"
        assert result.version == "1.0.0"
        assert result.uptime_ms == 5000
        assert result.clients == 3
        assert result.pools == {"sql": 10}
        assert result.channels == 4

    def test_handles_missing_status(self) -> None:
        collector = StatusCollector()
        data: dict = {}

        result = collector._parse_component(data)

        assert result.status == "unknown"


class TestStatusCollectorAsync:
    """Test async methods of StatusCollector."""

    @pytest.mark.anyio
    async def test_fetch_async_delegates_to_fetch(self) -> None:
        """fetch_async now delegates to synchronous fetch."""
        collector = StatusCollector()

        mock_status_data = {
            "bun": {"status": "ready", "version": "1.1.38"},
            "broker": {"status": "ready", "clients": 3},
            "ipc": {"status": "ready", "channels": 4},
        }

        with patch.object(collector, "_get_status_client") as mock_get_client:
            mock_client = MagicMock()
            mock_client.query_status.return_value = mock_status_data
            mock_get_client.return_value = mock_client

            result = await collector.fetch_async()

        assert result is not None
        assert result.bun.status == "ready"
        assert result.bun.version == "1.1.38"

    @pytest.mark.anyio
    async def test_fetch_async_returns_none_on_error(self) -> None:
        collector = StatusCollector()

        with patch.object(collector, "_get_status_client") as mock_get_client:
            mock_client = MagicMock()
            mock_client.query_status.side_effect = RuntimeError("IPC failure")
            mock_get_client.return_value = mock_client

            result = await collector.fetch_async()

        assert result is None


class TestStatusCollectorGetStatusClient:
    """Test _get_status_client method."""

    def test_get_status_client_caches_client(self) -> None:
        collector = StatusCollector()

        with patch("vertebra.cli.banner.collector.StatusClient") as mock_class:
            mock_instance = MagicMock()
            mock_class.return_value = mock_instance

            # First call should create client
            client1 = collector._get_status_client()
            # Second call should return cached client
            client2 = collector._get_status_client()

            assert client1 is client2
            assert mock_class.call_count == 1

    def test_fetch_handles_runtime_error(self) -> None:
        collector = StatusCollector()

        with patch.object(collector, "_get_status_client") as mock_get_client:
            mock_client = MagicMock()
            mock_client.query_status.side_effect = RuntimeError("Broker unavailable")
            mock_get_client.return_value = mock_client
            result = collector.fetch()

        assert result is None

    def test_fetch_handles_empty_data(self) -> None:
        collector = StatusCollector()

        with patch.object(collector, "_get_status_client") as mock_get_client:
            mock_client = MagicMock()
            # Return data that will cause KeyError during parsing
            mock_client.query_status.return_value = {"bad": "data"}
            mock_get_client.return_value = mock_client
            # Should not raise, returns partial result with defaults
            result = collector.fetch()

        # Should return a ServerStatus with default/unknown values
        assert result is not None
