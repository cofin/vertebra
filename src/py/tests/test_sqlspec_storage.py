"""Tests for VertebraObjectStore."""

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
    from vertebra.extensions.sqlspec._storage import VertebraObjectStore
    from vertebra.resources.broker import AsyncBrokerClient

    HAS_STORAGE = True
except ImportError:
    HAS_STORAGE = False


@pytest.mark.anyio
@pytest.mark.skipif(not HAS_STORAGE, reason="vertebra storage not available")
async def test_storage_operations() -> None:
    client = AsyncBrokerClient()

    # Mock client methods
    client.storage_read = unittest.mock.AsyncMock(return_value=b"data")  # type: ignore
    client.storage_write = unittest.mock.AsyncMock()  # type: ignore
    client.storage_list = unittest.mock.AsyncMock(return_value=["a", "b"])  # type: ignore
    client.storage_exists = unittest.mock.AsyncMock(return_value=True)  # type: ignore
    client.storage_delete = unittest.mock.AsyncMock()  # type: ignore
    client.storage_copy = unittest.mock.AsyncMock()  # type: ignore
    client.storage_move = unittest.mock.AsyncMock()  # type: ignore
    client.storage_glob = unittest.mock.AsyncMock(return_value=["foo.txt"])  # type: ignore

    store = VertebraObjectStore(pool="test_storage", client=client)

    # Read
    assert await store.read_bytes("path") == b"data"
    client.storage_read.assert_called_with("test_storage", "path")

    # Write
    await store.write_bytes("path", b"data")
    client.storage_write.assert_called_with("test_storage", "path", b"data")

    # List
    items = []
    async for item in store.list_objects("prefix"):
        items.append(item)
    assert items == ["a", "b"]
    client.storage_list.assert_called_with("test_storage", "prefix")

    # Exists
    assert await store.exists("path")
    client.storage_exists.assert_called_with("test_storage", "path")

    # Delete
    await store.delete("path")
    client.storage_delete.assert_called_with("test_storage", "path")

    # Copy
    await store.copy("src", "dst")
    client.storage_copy.assert_called_with("test_storage", "src", "dst")

    # Move
    await store.move("src", "dst")
    client.storage_move.assert_called_with("test_storage", "src", "dst")

    # Glob
    glob_items = []
    async for item in store.glob("*.txt"):
        glob_items.append(item)
    assert glob_items == ["foo.txt"]
    client.storage_glob.assert_called_with("test_storage", "*.txt")
