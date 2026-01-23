import os
import signal
import subprocess

import pytest

import vertebra._vertebra as _vertebra

if not hasattr(_vertebra, "NativeSupervisor"):
    pytest.skip("NativeSupervisor not available in _vertebra", allow_module_level=True)

from vertebra._vertebra import NativeSupervisor


@pytest.mark.unit
def test_native_supervisor_lifecycle() -> None:
    ns = NativeSupervisor()

    # Spawn a dummy child process
    proc = subprocess.Popen(["/bin/sleep", "10"])
    ns.add_child(proc.pid)

    # Propagate SIGTERM
    ns.propagate_signal(signal.SIGTERM)

    # Wait for event (should be exit)
    event = ns.wait_next_event(1000)
    assert event["type"] == "exit"
    assert event["pid"] == proc.pid
    # SIGTERM usually results in -15 exit code in event

    proc.wait()


@pytest.mark.unit
def test_native_supervisor_timeout() -> None:
    ns = NativeSupervisor()
    event = ns.wait_next_event(100)
    assert event["type"] == "timeout"


@pytest.mark.unit
def test_native_supervisor_multiple_children() -> None:
    ns = NativeSupervisor()
    proc1 = subprocess.Popen(["/bin/sleep", "10"])
    proc2 = subprocess.Popen(["/bin/sleep", "10"])
    ns.add_child(proc1.pid)
    ns.add_child(proc2.pid)

    os.kill(proc1.pid, signal.SIGTERM)
    event = ns.wait_next_event(1000)
    assert event["type"] == "exit"
    assert event["pid"] == proc1.pid

    os.kill(proc2.pid, signal.SIGTERM)
    event = ns.wait_next_event(1000)
    assert event["type"] == "exit"
    assert event["pid"] == proc2.pid

    proc1.wait()
    proc2.wait()
