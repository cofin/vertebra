import os
import signal
import subprocess
import time
import pytest
from vertebra._vertebra import NativeSupervisor

@pytest.mark.unit
def test_native_supervisor_lifecycle():
    ns = NativeSupervisor()
    
    # Spawn a dummy child process
    proc = subprocess.Popen(["sleep", "10"])
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
def test_native_supervisor_timeout():
    ns = NativeSupervisor()
    event = ns.wait_next_event(100)
    assert event["type"] == "timeout"

@pytest.mark.unit
def test_native_supervisor_multiple_children():
    ns = NativeSupervisor()
    proc1 = subprocess.Popen(["sleep", "10"])
    proc2 = subprocess.Popen(["sleep", "10"])
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
