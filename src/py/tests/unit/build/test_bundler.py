import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch, AsyncMock
from vertebra.build.bundler import Bundler

@pytest.fixture
def mock_standalone_manager():
    with patch("vertebra.build.bundler.StandaloneManager") as mock:
        yield mock

@pytest.fixture
def bundler(tmp_path, mock_standalone_manager):
    project_dir = tmp_path / "project"
    project_dir.mkdir()
    output_dir = tmp_path / "dist"
    return Bundler(project_dir, output_dir, target="linux-x64")

@pytest.mark.asyncio
async def test_bundler_flow(bundler, mock_standalone_manager, tmp_path):
    """Test the main build flow."""
    # Mock manager behavior
    manager_instance = mock_standalone_manager.return_value
    manager_instance.download = AsyncMock(return_value=Path("archive.tar.gz"))
    
    # Mock extract to return a path containing site-packages
    fake_python_home = tmp_path / "dist" / "python"
    fake_site_packages = fake_python_home / "lib" / "python3.12" / "site-packages"
    fake_site_packages.mkdir(parents=True)
    manager_instance.extract.return_value = fake_python_home

    # Mock subprocess.run
    with patch("vertebra.build.bundler.subprocess.run") as mock_run:
        mock_run.return_value.returncode = 0
        
        output_path = await bundler.build()
        
        assert output_path == bundler.output_dir
        
        # Verify download called
        manager_instance.download.assert_called_with("linux-x64", "modern")
        
        # Verify install called
        assert mock_run.called
        cmd = mock_run.call_args[0][0]
        assert "uv" in cmd
        assert "pip" in cmd
        assert "install" in cmd
        assert str(fake_site_packages) in cmd

@pytest.mark.asyncio
async def test_bundler_install_fail(bundler, mock_standalone_manager, tmp_path):
    """Test failure during dependency installation."""
    manager_instance = mock_standalone_manager.return_value
    manager_instance.download = AsyncMock(return_value=Path("archive.tar.gz"))
    
    fake_python_home = tmp_path / "dist" / "python"
    fake_site_packages = fake_python_home / "lib" / "python3.12" / "site-packages"
    fake_site_packages.mkdir(parents=True)
    manager_instance.extract.return_value = fake_python_home

    with patch("vertebra.build.bundler.subprocess.run") as mock_run:
        mock_run.return_value.returncode = 1
        mock_run.return_value.stderr = "Installation failed"
        
        with pytest.raises(RuntimeError, match="Dependency installation failed"):
            await bundler.build()
