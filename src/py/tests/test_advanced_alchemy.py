"""Tests for Vertebra Advanced Alchemy integration."""

import sys
import unittest.mock

import pytest

# Mock native extension
mock_vertebra_native = unittest.mock.MagicMock()
sys.modules["vertebra._vertebra"] = mock_vertebra_native
mock_vertebra_native.ShmRegion = unittest.mock.MagicMock()
mock_vertebra_native.AsyncRing = unittest.mock.MagicMock()
mock_vertebra_native.SpscRing = unittest.mock.MagicMock()

try:
    import advanced_alchemy

    HAS_AA = True
except ImportError:
    HAS_AA = False


@pytest.mark.skipif(not HAS_AA, reason="advanced-alchemy not installed")
def test_aa_postgres_config() -> None:
    from vertebra.extensions.advanced_alchemy.postgres import VertebraPostgresAlchemyAsyncConfig

    config = VertebraPostgresAlchemyAsyncConfig(pool="test_pool")
    assert str(config.engine.url) == "postgresql+vertebra_async://test_pool"


@pytest.mark.skipif(not HAS_AA, reason="advanced-alchemy not installed")
@pytest.mark.anyio
async def test_aa_storage_backend() -> None:
    from vertebra.extensions.advanced_alchemy._storage import VertebraStorageBackend

    backend = VertebraStorageBackend(pool="test_storage")

    # Mock client
    backend._client.storage_read = unittest.mock.AsyncMock(return_value=b"data")  # type: ignore

    data = await backend.read_bytes("path")
    assert data == b"data"
    backend._client.storage_read.assert_called_with("test_storage", "path")
