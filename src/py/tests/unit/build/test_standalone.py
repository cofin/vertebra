import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch
from vertebra.build.standalone import StandaloneManager, STANDALONE_RELEASES

@pytest.fixture
def manager(tmp_path):
    return StandaloneManager(cache_dir=tmp_path)

def test_get_release_filename_valid(manager):
    """Test valid target/profile combinations."""
    filename = manager.get_release_filename("linux-x64", "modern")
    assert "x86_64_v3-unknown-linux-gnu" in filename
    assert filename == STANDALONE_RELEASES["linux-x64-modern"]

    filename_baseline = manager.get_release_filename("linux-x64", "baseline")
    assert "x86_64-unknown-linux-gnu" in filename_baseline
    assert "v3" not in filename_baseline

def test_get_release_filename_arm64(manager):
    """Test arm64 mappings."""
    filename = manager.get_release_filename("linux-arm64", "modern")
    assert "aarch64-unknown-linux-gnu" in filename

def test_get_release_filename_invalid(manager):
    """Test invalid target raises ValueError."""
    with pytest.raises(ValueError):
        manager.get_release_filename("invalid-target", "modern")

@pytest.mark.asyncio
async def test_download_cached(manager, tmp_path):
    """Test that download returns immediately if file exists."""
    target = "linux-x64"
    profile = "modern"
    filename = manager.get_release_filename(target, profile)

    # Create dummy cached file
    cached_file = tmp_path / filename
    cached_file.touch()

    result = await manager.download(target, profile)
    assert result == cached_file

@pytest.mark.asyncio
@patch("vertebra.build.standalone.httpx.AsyncClient")
async def test_download_fetch(mock_client_cls, manager, tmp_path):
    """Test actual download logic (mocked)."""
    target = "linux-x64"
    profile = "modern"
    filename = manager.get_release_filename(target, profile)

    # Mock async iterator for chunks
    async def mock_aiter_bytes():
        yield b"chunk1"
        yield b"chunk2"

    # Mock httpx response
    mock_response = MagicMock()
    mock_response.headers = {"content-length": "10"}
    mock_response.aiter_bytes = mock_aiter_bytes

    # Async context manager mock setup
    mock_client = mock_client_cls.return_value
    mock_client.__aenter__.return_value = mock_client
    mock_client.__aexit__.return_value = None

    mock_client.stream.return_value.__aenter__.return_value = mock_response
    mock_client.stream.return_value.__aexit__.return_value = None

    result = await manager.download(target, profile)

    assert result == tmp_path / filename
    assert result.exists()
    assert result.read_bytes() == b"chunk1chunk2"
