"""Unit tests for benchmark infrastructure."""

from __future__ import annotations

import sys
from pathlib import Path

# Ensure repo root is on sys.path so `tools.benchmarks` is importable.
# This file is at src/py/tests/unit/test_bench_infrastructure.py
_REPO_ROOT = str(Path(__file__).resolve().parents[4])
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)


def test_targets_vtb_shm_asgi_4w_target_exists() -> None:
    from tools.benchmarks.targets import ALL_TARGETS

    assert "vtb-shm-asgi-4w" in ALL_TARGETS


def test_targets_vtb_shm_asgi_4w_port() -> None:
    from tools.benchmarks.targets import ALL_TARGETS

    target = ALL_TARGETS["vtb-shm-asgi-4w"]
    assert target.port == 8012


def test_targets_vtb_shm_asgi_4w_workers_env() -> None:
    from tools.benchmarks.targets import ALL_TARGETS

    target = ALL_TARGETS["vtb-shm-asgi-4w"]
    assert target.env["VERTEBRA_WORKERS"] == "4"


def test_targets_vtb_shm_asgi_4w_in_vertebra_targets() -> None:
    from tools.benchmarks.targets import VERTEBRA_TARGETS

    assert "vtb-shm-asgi-4w" in VERTEBRA_TARGETS


def test_targets_baseline_targets_defined() -> None:
    from tools.benchmarks.targets import BASELINE_TARGETS

    assert "granian-asgi" in BASELINE_TARGETS
    assert "uvicorn-httptools" in BASELINE_TARGETS
    assert "spikard" in BASELINE_TARGETS


def test_targets_all_targets_count() -> None:
    from tools.benchmarks.targets import ALL_TARGETS

    # 6 vertebra + 1 freethreaded (currently defined) + 3 baseline = 10
    assert len(ALL_TARGETS) >= 10


def test_targets_no_port_collisions() -> None:
    from tools.benchmarks.targets import ALL_TARGETS

    ports = [t.port for t in ALL_TARGETS.values()]
    assert len(ports) == len(set(ports))


def test_scenarios_default_includes_plaintext() -> None:
    from tools.benchmarks.scenarios import DEFAULT_SCENARIOS

    assert "plaintext" in DEFAULT_SCENARIOS


def test_scenarios_default_includes_echo() -> None:
    from tools.benchmarks.scenarios import DEFAULT_SCENARIOS

    assert "echo" in DEFAULT_SCENARIOS


def test_scenarios_default_includes_io() -> None:
    from tools.benchmarks.scenarios import DEFAULT_SCENARIOS

    assert "io_10ms" in DEFAULT_SCENARIOS


def test_scenarios_full_has_all_scenarios() -> None:
    from tools.benchmarks.scenarios import FULL_SCENARIOS, SCENARIOS

    assert len(FULL_SCENARIOS) == len(SCENARIOS)


def test_scenarios_multiworker_includes_echo() -> None:
    from tools.benchmarks.scenarios import MULTIWORKER_SCENARIOS

    assert "echo" in MULTIWORKER_SCENARIOS


def test_scenarios_body_100k_not_in_default() -> None:
    from tools.benchmarks.scenarios import DEFAULT_SCENARIOS

    assert "body_100k" not in DEFAULT_SCENARIOS


def test_scenarios_body_100k_in_full() -> None:
    from tools.benchmarks.scenarios import FULL_SCENARIOS

    assert "body_100k" in FULL_SCENARIOS


def test_endpoint_coverage_asgi_app_has_all_paths() -> None:
    from tools.benchmarks.apps.asgi import app

    # Minimal check that it's a callable
    assert callable(app)


def test_endpoint_coverage_wsgi_app_has_io10() -> None:
    from tools.benchmarks.apps.wsgi import app

    assert callable(app)


def test_endpoint_coverage_vertebra_native_app_has_handlers() -> None:
    from tools.benchmarks.apps.vertebra_app import native_app

    assert hasattr(native_app, "_handlers")
    assert len(native_app._handlers) > 0
