import pytest
from unittest.mock import patch, AsyncMock
from click.testing import CliRunner
from vertebra.cli import main

@pytest.fixture
def runner():
    return CliRunner()

def test_build_command_one_file(runner, tmp_path):
    """Test standard build command invocation."""
    with patch("vertebra.build.bundler.Bundler") as MockBundler:
        instance = MockBundler.return_value
        instance.build = AsyncMock()
        
        result = runner.invoke(main, [
            "build", 
            ".", 
            "--one-file", 
            "--target", "linux-x64",
            "--profile", "modern",
            "--output", str(tmp_path / "dist")
        ])
        
        assert result.exit_code == 0
        assert "Building single-file executable" in result.output
        
        MockBundler.assert_called_once()
        call_kwargs = MockBundler.call_args[1]
        assert call_kwargs["target"] == "linux-x64"
        assert call_kwargs["profile"] == "modern"
        
        instance.build.assert_called_once()

def test_build_command_missing_target(runner):
    """Test required arguments."""
    result = runner.invoke(main, ["build", ".", "--one-file"])
    assert result.exit_code != 0
    assert "Missing option '--target'" in result.output
