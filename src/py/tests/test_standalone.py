"""Tests for standalone Python runtime management."""

from pathlib import Path
from unittest import mock

import pytest

from vertebra.build.standalone import download_standalone, STANDALONE_RELEASES


@pytest.mark.anyio
async def test_download_standalone_cached(tmp_path: Path) -> None:
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    
    target = "linux-x64"
    profile = "modern"
    filename = STANDALONE_RELEASES[f"{target}-{profile}"]
    
    # Pre-create file
    (cache_dir / filename).touch()
    
    with mock.patch("httpx.AsyncClient") as MockClient:
        path = await download_standalone(target, profile, cache_dir)
        
        assert path.exists()
        assert path.name == filename
        # Should not download if exists
        MockClient.assert_not_called()

@pytest.mark.anyio
async def test_download_standalone_fetch(tmp_path: Path) -> None:
    cache_dir = tmp_path / "cache"
    
    target = "linux-x64"
    profile = "modern"
    
    with mock.patch("httpx.AsyncClient") as MockClient:
        mock_client = MockClient.return_value
        mock_client.__aenter__.return_value = mock_client
        mock_client.__aexit__.return_value = None
        
        mock_response = mock.Mock()
        mock_response.status_code = 200
        
        async def async_iter():
            yield b"data"
        mock_response.aiter_bytes.return_value = async_iter()
        
        mock_client.stream.return_value.__aenter__.return_value = mock_response
        
        path = await download_standalone(target, profile, cache_dir)
        
        assert path.exists()
        assert path.read_bytes() == b"data"
