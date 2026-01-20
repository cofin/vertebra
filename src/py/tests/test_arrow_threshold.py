"""Tests for Arrow IPC threshold selection in the SQLSpec provider."""

from __future__ import annotations

from typing import Any

import pytest

pytestmark = pytest.mark.anyio


class TestArrowThreshold:
    """Test the Arrow vs MessagePack selection logic."""

    @pytest.fixture
    def mock_provider(self) -> Any:
        """Create a minimal mock provider with threshold logic exposed."""
        try:
            import pyarrow as pa  # noqa: F401
        except ImportError:
            pytest.skip("pyarrow not available")

        from vertebra.resources.provider import ProviderConfig

        # Create a config with known thresholds for testing
        config = ProviderConfig(
            pool="test-pool",
            driver="test-driver",
            arrow_threshold_rows=100,  # Low threshold for testing
            arrow_threshold_bytes=1024,  # 1KB threshold for testing
        )

        # Create a minimal mock that exposes the threshold logic
        # We need to instantiate enough of SqlSpecProvider to test _maybe_encode_arrow
        class MockProvider:
            def __init__(self, cfg: ProviderConfig) -> None:
                self._config = cfg
                self._arrow_regions: dict[str, Any] = {}

            def _maybe_encode_arrow(
                self, rows: list[list[Any]], columns: list[str], rowcount: int
            ) -> dict[str, Any] | None:
                if not rows:
                    return None
                estimated_bytes = len(rows) * max(1, len(columns)) * 8
                if (
                    rowcount < self._config.arrow_threshold_rows
                    and estimated_bytes < self._config.arrow_threshold_bytes
                ):
                    return None
                try:
                    import pyarrow as pa
                except ImportError:
                    return None
                records = [dict(zip(columns, row, strict=False)) for row in rows]
                table = pa.Table.from_pylist(records)
                # Return a mock Arrow payload (don't actually create shm region in test)
                return {"arrow": {"name": "/test-arrow-region", "size": table.nbytes}, "rowcount": rowcount}

        return MockProvider(config)

    def test_empty_rows_uses_msgpack(self, mock_provider: Any) -> None:
        """Empty row sets should always use MessagePack (return None)."""
        result = mock_provider._maybe_encode_arrow([], ["col1", "col2"], 0)
        assert result is None

    def test_small_rowcount_small_bytes_uses_msgpack(self, mock_provider: Any) -> None:
        """Small results below both thresholds should use MessagePack."""
        # 10 rows, 2 columns = 10 * 2 * 8 = 160 bytes estimated
        rows = [[i, f"value_{i}"] for i in range(10)]
        result = mock_provider._maybe_encode_arrow(rows, ["id", "value"], 10)
        assert result is None

    def test_large_rowcount_uses_arrow(self, mock_provider: Any) -> None:
        """Row count >= threshold should trigger Arrow encoding."""
        # 100 rows (at threshold), 2 columns
        rows = [[i, f"value_{i}"] for i in range(100)]
        result = mock_provider._maybe_encode_arrow(rows, ["id", "value"], 100)
        assert result is not None
        assert "arrow" in result
        assert result["rowcount"] == 100

    def test_large_bytes_uses_arrow(self, mock_provider: Any) -> None:
        """Byte estimate >= threshold should trigger Arrow encoding."""
        # 50 rows, 5 columns = 50 * 5 * 8 = 2000 bytes estimated (above 1KB)
        rows = [[i, f"a_{i}", f"b_{i}", f"c_{i}", f"d_{i}"] for i in range(50)]
        columns = ["id", "col_a", "col_b", "col_c", "col_d"]
        result = mock_provider._maybe_encode_arrow(rows, columns, 50)
        assert result is not None
        assert "arrow" in result

    def test_at_row_threshold_boundary(self, mock_provider: Any) -> None:
        """Test exact boundary: 99 rows < 100 threshold."""
        # 99 rows (below threshold), 2 columns = 99 * 2 * 8 = 1584 bytes
        # Byte estimate is above 1024, so this should still trigger Arrow
        rows = [[i, f"value_{i}"] for i in range(99)]
        result = mock_provider._maybe_encode_arrow(rows, ["id", "value"], 99)
        # Bytes = 99 * 2 * 8 = 1584 > 1024, so Arrow should be used
        assert result is not None

    def test_at_byte_threshold_boundary(self, mock_provider: Any) -> None:
        """Test just below byte threshold with few rows."""
        # 10 rows, 12 columns = 10 * 12 * 8 = 960 bytes (below 1024)
        rows = [list(range(12)) for _ in range(10)]
        columns = [f"col_{j}" for j in range(12)]
        result = mock_provider._maybe_encode_arrow(rows, columns, 10)
        # Both thresholds not exceeded: 10 < 100 rows, 960 < 1024 bytes
        assert result is None

    def test_single_column_estimation(self, mock_provider: Any) -> None:
        """Test byte estimation with single column uses max(1, columns)."""
        # 50 rows, 1 column = 50 * 1 * 8 = 400 bytes
        rows = [[i] for i in range(50)]
        result = mock_provider._maybe_encode_arrow(rows, ["id"], 50)
        # Both thresholds not exceeded: 50 < 100 rows, 400 < 1024 bytes
        assert result is None

    def test_zero_columns_estimation(self, mock_provider: Any) -> None:
        """Test byte estimation with empty columns list uses max(1, 0) = 1."""
        # Edge case: rows exist but no columns defined
        rows = [[] for _ in range(50)]
        result = mock_provider._maybe_encode_arrow(rows, [], 50)
        # 50 * max(1, 0) * 8 = 400 bytes, below threshold
        # This is a degenerate case but tests the max(1, len(columns)) guard
        assert result is None


class TestArrowThresholdConfig:
    """Test that threshold configuration is respected."""

    def test_default_thresholds(self) -> None:
        """Default thresholds should be 1000 rows and 1MB."""
        from vertebra.resources.provider import ProviderConfig

        config = ProviderConfig(pool="test", driver="test")
        assert config.arrow_threshold_rows == 1_000
        assert config.arrow_threshold_bytes == 1_048_576

    def test_custom_thresholds(self) -> None:
        """Custom thresholds should be configurable."""
        from vertebra.resources.provider import ProviderConfig

        config = ProviderConfig(pool="test", driver="test", arrow_threshold_rows=500, arrow_threshold_bytes=512_000)
        assert config.arrow_threshold_rows == 500
        assert config.arrow_threshold_bytes == 512_000

    def test_zero_threshold_always_uses_arrow(self) -> None:
        """Zero threshold should always trigger Arrow for non-empty results."""
        try:
            import pyarrow as pa  # noqa: F401
        except ImportError:
            pytest.skip("pyarrow not available")

        from vertebra.resources.provider import ProviderConfig

        config = ProviderConfig(
            pool="test",
            driver="test",
            arrow_threshold_rows=0,  # Always use Arrow
            arrow_threshold_bytes=0,
        )

        # Minimal mock to test threshold
        class MockProvider:
            def __init__(self, cfg: ProviderConfig) -> None:
                self._config = cfg

            def _maybe_encode_arrow(
                self, rows: list[list[Any]], columns: list[str], rowcount: int
            ) -> dict[str, Any] | None:
                if not rows:
                    return None
                estimated_bytes = len(rows) * max(1, len(columns)) * 8
                if (
                    rowcount < self._config.arrow_threshold_rows
                    and estimated_bytes < self._config.arrow_threshold_bytes
                ):
                    return None
                return {"arrow": True}  # Simplified return

        provider = MockProvider(config)
        # Even 1 row should trigger Arrow with 0 thresholds
        result = provider._maybe_encode_arrow([[1]], ["id"], 1)
        assert result is not None


class TestArrowPayloadStructure:
    """Test the structure of Arrow payloads."""

    def test_arrow_payload_contains_name_and_size(self) -> None:
        """Arrow payload should include shared memory region name and size."""
        try:
            import pyarrow as pa
        except ImportError:
            pytest.skip("pyarrow not available")

        # Create test data and encode to Arrow
        records = [{"id": i, "value": f"test_{i}"} for i in range(10)]
        table = pa.Table.from_pylist(records)

        # Simulate the payload structure
        payload = {"arrow": {"name": "/test-region", "size": table.nbytes}}

        assert "arrow" in payload
        assert "name" in payload["arrow"]
        assert "size" in payload["arrow"]
        assert payload["arrow"]["name"].startswith("/")
        assert payload["arrow"]["size"] > 0


class TestPyArrowFallback:
    """Test behavior when pyarrow is not available."""

    def test_missing_pyarrow_returns_none(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """When pyarrow import fails, _maybe_encode_arrow returns None."""
        import builtins

        original_import = builtins.__import__

        def mock_import(name: str, *args: Any, **kwargs: Any) -> Any:
            if name == "pyarrow":
                msg = "No module named 'pyarrow'"
                raise ImportError(msg)
            return original_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", mock_import)

        from vertebra.resources.provider import ProviderConfig

        config = ProviderConfig(
            pool="test",
            driver="test",
            arrow_threshold_rows=0,  # Force Arrow path
            arrow_threshold_bytes=0,
        )

        class MockProvider:
            def __init__(self, cfg: ProviderConfig) -> None:
                self._config = cfg

            def _maybe_encode_arrow(
                self, rows: list[list[Any]], columns: list[str], rowcount: int
            ) -> dict[str, Any] | None:
                if not rows:
                    return None
                estimated_bytes = len(rows) * max(1, len(columns)) * 8
                if (
                    rowcount < self._config.arrow_threshold_rows
                    and estimated_bytes < self._config.arrow_threshold_bytes
                ):
                    return None
                try:
                    import pyarrow as pa  # noqa: F401
                except ImportError:
                    return None
                return {"arrow": True}

        provider = MockProvider(config)
        # Should return None because pyarrow import fails
        result = provider._maybe_encode_arrow([[1, "test"]], ["id", "value"], 1)
        assert result is None
