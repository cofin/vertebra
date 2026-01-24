import os
from unittest.mock import patch, MagicMock
import pytest
from vertebra.supervisor import run_supervisor

@pytest.mark.unit
def test_run_supervisor_extra_args() -> None:
    """Test that run_supervisor accepts drain_timeout_ms and shutdown_timeout_ms."""
    bun_cmd = ["bun", "run", "index.ts"]
    env = {"TEST_VAR": "value"}
    
    # We mock Supervisor to avoid starting actual processes
    with patch("vertebra.supervisor.Supervisor") as MockSupervisor:
        mock_instance = MockSupervisor.return_value
        mock_instance.wait.return_value = 0
        
        # This call should currently fail with TypeError
        exit_code = run_supervisor(
            bun_cmd=bun_cmd,
            env=env,
            workers=1,
            drain_timeout_ms=5000,
            shutdown_timeout_ms=30000
        )
        
        assert exit_code == 0
        # Verify environment variables were set
        assert os.environ["VERTEBRA_DRAIN_TIMEOUT_MS"] == "5000"
        assert os.environ["VERTEBRA_SHUTDOWN_TIMEOUT_MS"] == "30000"
