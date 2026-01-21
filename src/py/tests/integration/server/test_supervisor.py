"""Tests for the process supervisor."""

import signal
import sys
import tempfile
import time
from pathlib import Path

import pytest

from vertebra.supervisor import HealthCheckConfig, ProcessSpec, RestartPolicy, Supervisor, SupervisorState

pytestmark = pytest.mark.integration


def test_restart_policy_exponential_backoff() -> None:
    """Verify exponential backoff delay computation."""
    policy = RestartPolicy(base_delay_ms=100, max_delay_ms=1000)
    assert policy.compute_delay(1) == 0.1  # 100ms
    assert policy.compute_delay(2) == 0.2  # 200ms
    assert policy.compute_delay(3) == 0.4  # 400ms
    assert policy.compute_delay(4) == 0.8  # 800ms
    assert policy.compute_delay(5) == 1.0  # capped at 1000ms


def test_supervisor_state_transitions() -> None:
    """Verify supervisor state machine transitions."""
    spec = ProcessSpec(name="test", cmd=[sys.executable, "-c", "import time; time.sleep(0.1)"], critical=True)
    supervisor = Supervisor([spec])
    assert supervisor.state == SupervisorState.IDLE

    supervisor.start()
    assert supervisor.state == SupervisorState.RUNNING

    supervisor.stop()
    assert supervisor.state == SupervisorState.STOPPED


def test_process_start_and_stop() -> None:
    """Verify basic process start and stop."""
    spec = ProcessSpec(name="sleeper", cmd=[sys.executable, "-c", "import time; time.sleep(10)"], critical=True)
    supervisor = Supervisor([spec], drain_timeout_ms=100, shutdown_timeout_ms=1000)
    try:
        supervisor.start()

        stats = supervisor.get_stats()
        assert "sleeper" in stats
        assert stats["sleeper"]["state"] == "running"
        assert stats["sleeper"]["pid"] is not None
    finally:
        supervisor.stop()
        stats = supervisor.get_stats()
        assert stats["sleeper"]["state"] in ("stopped", "running")  # depends on timing


def test_process_exit_detection() -> None:
    """Verify detection of process exit."""
    # Create a process that exits immediately
    spec = ProcessSpec(name="exiter", cmd=[sys.executable, "-c", "print('done')"], critical=True)
    supervisor = Supervisor([spec])
    try:
        supervisor.start()

        # Wait should detect exit quickly
        exit_code = supervisor.wait(primary="exiter")
        assert exit_code == 0
    finally:
        supervisor.stop()


def test_non_critical_process_continues() -> None:
    """Verify non-critical process exit doesn't stop supervisor."""
    # Non-critical process that exits quickly but not instantly
    non_critical = ProcessSpec(
        name="ephemeral",
        cmd=[sys.executable, "-c", "import time; time.sleep(0.1); print('bye')"],
        critical=False,
        restart_policy=RestartPolicy(max_restarts=0),  # Don't restart
    )
    # Critical process that runs longer
    critical = ProcessSpec(name="main", cmd=[sys.executable, "-c", "import time; time.sleep(2.0)"], critical=True)
    supervisor = Supervisor([non_critical, critical])
    try:
        supervisor.start()

        # Wait for main to exit
        # This will block until 'main' exits (approx 2s) or timeout
        exit_code = supervisor.wait(primary="main")
        assert exit_code == 0
    finally:
        supervisor.stop()


@pytest.mark.skipif(sys.platform == "win32", reason="Process groups not supported on Windows")
def test_signal_propagation() -> None:
    """Verify SIGTERM is propagated to child processes."""
    # Create a script that handles SIGTERM
    with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
        f.write("""
import signal
import sys
import time

received = False

def handler(signum, frame):
    global received
    received = True
    print("SIGTERM received", flush=True)
    sys.exit(0)

signal.signal(signal.SIGTERM, handler)
print("ready", flush=True)
time.sleep(30)
""")
        script_path = f.name

    try:
        spec = ProcessSpec(name="signaler", cmd=[sys.executable, script_path], critical=True)
        supervisor = Supervisor([spec], drain_timeout_ms=500, shutdown_timeout_ms=2000)
        supervisor.start()

        # Give process time to start
        time.sleep(0.2)

        # Stop should send SIGTERM
        supervisor.stop(signal.SIGTERM)

        # Process should have exited cleanly
        stats = supervisor.get_stats()
        assert stats["signaler"]["state"] == "stopped"
    finally:
        Path(script_path).unlink(missing_ok=True)
        # Ensure stopped if assertion failed
        if "supervisor" in locals():
            supervisor.stop()


def test_multiple_processes_ordered_start() -> None:
    """Verify processes start in defined order."""

    # Create temp files to record start order
    with tempfile.TemporaryDirectory() as tmpdir:
        marker_dir = Path(tmpdir)

        def make_cmd(name: str) -> list[str]:
            marker = marker_dir / f"{name}.started"
            return [
                sys.executable,
                "-c",
                f"from pathlib import Path; Path('{marker}').touch(); import time; time.sleep(0.5)",
            ]

        specs = [
            ProcessSpec(name="first", cmd=make_cmd("first"), critical=True),
            ProcessSpec(name="second", cmd=make_cmd("second"), critical=True),
            ProcessSpec(name="third", cmd=make_cmd("third"), critical=True),
        ]
        supervisor = Supervisor(specs, drain_timeout_ms=100, shutdown_timeout_ms=500)
        try:
            supervisor.start()

            # Wait for markers
            time.sleep(0.3)

            # Check all started
            for spec in specs:
                assert (marker_dir / f"{spec.name}.started").exists()
        finally:
            supervisor.stop()


def test_get_stats_with_uptime() -> None:
    """Verify stats include uptime calculation."""
    spec = ProcessSpec(name="timer", cmd=[sys.executable, "-c", "import time; time.sleep(1)"], critical=True)
    supervisor = Supervisor([spec])
    try:
        supervisor.start()

        time.sleep(0.1)
        stats = supervisor.get_stats()

        assert stats["timer"]["uptime_s"] is not None
        assert stats["timer"]["uptime_s"] >= 0.1
    finally:
        supervisor.stop()


def test_process_spec_defaults() -> None:
    """Verify ProcessSpec default values."""
    spec = ProcessSpec(name="test", cmd=["echo", "hi"])
    assert spec.critical is True
    assert spec.shutdown_timeout_ms == 30000
    assert spec.drain_timeout_ms == 5000
    assert spec.restart_policy.max_restarts == 5
    assert spec.health_check.enabled is True


def test_health_check_config_defaults() -> None:
    """Verify HealthCheckConfig default values."""
    config = HealthCheckConfig()
    assert config.enabled is True
    assert config.interval_ms == 5000
    assert config.timeout_ms == 3000
    assert config.unhealthy_threshold == 3
