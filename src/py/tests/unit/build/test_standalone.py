import pytest
from pathlib import Path
from unittest.mock import patch
from vertebra.core.build.standalone import StandaloneManager, STANDALONE_RELEASES

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

@pytest.mark.anyio
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

@pytest.mark.anyio
@patch("vertebra.core.build.standalone.urllib.request.urlopen")
async def test_download_fetch(mock_urlopen, manager, tmp_path):
    """Test actual download logic (mocked)."""
    target = "linux-x64"
    profile = "modern"
    filename = manager.get_release_filename(target, profile)

    class DummyResponse:
        def __init__(self, chunks):
            self._chunks = list(chunks)

        def read(self, _size=8192):
            return self._chunks.pop(0) if self._chunks else b""

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    mock_urlopen.return_value = DummyResponse([b"chunk1", b"chunk2"])

    result = await manager.download(target, profile)

    assert result == tmp_path / filename
    assert result.exists()
    assert result.read_bytes() == b"chunk1chunk2"
