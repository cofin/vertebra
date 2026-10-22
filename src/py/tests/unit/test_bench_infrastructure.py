"""Unit tests for benchmark infrastructure."""

from __future__ import annotations

from tools.benchmarks.apps.asgi import app as asgi_app
from tools.benchmarks.apps.vertebra_app import native_app
from tools.benchmarks.apps.wsgi import app as wsgi_app
from tools.benchmarks.scenarios import (
    DEFAULT_SCENARIOS,
    FULL_SCENARIOS,
    MULTIWORKER_SCENARIOS,
    SCENARIOS,
)
from tools.benchmarks.targets import (
    ALL_TARGETS,
    BASELINE_TARGETS,
    VERTEBRA_TARGETS,
)


def test_targets_vtb_shm_asgi_4w_target_exists() -> None:
    assert "vtb-shm-asgi-4w" in ALL_TARGETS


def test_targets_vtb_shm_asgi_4w_port() -> None:
    target = ALL_TARGETS["vtb-shm-asgi-4w"]
    assert target.port == 8012


def test_targets_vtb_shm_asgi_4w_workers_env() -> None:
    target = ALL_TARGETS["vtb-shm-asgi-4w"]
    assert target.env["VERTEBRA_WORKERS"] == "4"


def test_targets_vtb_shm_asgi_4w_in_vertebra_targets() -> None:
    assert "vtb-shm-asgi-4w" in VERTEBRA_TARGETS


def test_targets_baseline_targets_defined() -> None:
    assert "granian-asgi" in BASELINE_TARGETS
    assert "uvicorn-httptools" in BASELINE_TARGETS
    assert "spikard" in BASELINE_TARGETS


def test_targets_all_targets_count() -> None:
    assert len(ALL_TARGETS) >= 10


def test_targets_no_port_collisions() -> None:
    ports = [t.port for t in ALL_TARGETS.values()]
    assert len(ports) == len(set(ports))


def test_scenarios_default_includes_plaintext() -> None:
    assert "plaintext" in DEFAULT_SCENARIOS


def test_scenarios_default_includes_echo() -> None:
    assert "echo" in DEFAULT_SCENARIOS


def test_scenarios_default_includes_io() -> None:
    assert "io_10ms" in DEFAULT_SCENARIOS


def test_scenarios_full_has_all_scenarios() -> None:
    assert len(FULL_SCENARIOS) == len(SCENARIOS)


def test_scenarios_multiworker_includes_echo() -> None:
    assert "echo" in MULTIWORKER_SCENARIOS


def test_scenarios_body_100k_not_in_default() -> None:
    assert "body_100k" not in DEFAULT_SCENARIOS


def test_scenarios_body_100k_in_full() -> None:
    assert "body_100k" in FULL_SCENARIOS


def test_endpoint_coverage_asgi_app_has_all_paths() -> None:
    assert callable(asgi_app)


def test_endpoint_coverage_wsgi_app_has_io10() -> None:
    assert callable(wsgi_app)


def test_endpoint_coverage_vertebra_native_app_has_handlers() -> None:
    assert hasattr(native_app, "_handlers")
    assert len(native_app._handlers) > 0
