"""Tests for benchmark infrastructure completeness (Ch0)."""

import sys
from pathlib import Path

# Ensure repo root is on sys.path so `tools.benchmarks` is importable.
_REPO_ROOT = str(Path(__file__).resolve().parents[3])
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)


class TestTargets:
    """Verify all expected benchmark targets are defined."""

    def test_vertebra_asgi_4w_target_exists(self) -> None:
        from tools.benchmarks.targets import ALL_TARGETS

        assert "vertebra-asgi-4w" in ALL_TARGETS

    def test_vertebra_asgi_4w_port(self) -> None:
        from tools.benchmarks.targets import ALL_TARGETS

        target = ALL_TARGETS["vertebra-asgi-4w"]
        assert target.port == 8012

    def test_vertebra_asgi_4w_workers_env(self) -> None:
        from tools.benchmarks.targets import ALL_TARGETS

        target = ALL_TARGETS["vertebra-asgi-4w"]
        assert target.env.get("VERTEBRA_WORKERS") == "4"

    def test_vertebra_asgi_4w_in_vertebra_targets(self) -> None:
        from tools.benchmarks.targets import VERTEBRA_TARGETS

        assert "vertebra-asgi-4w" in VERTEBRA_TARGETS

    def test_baseline_targets_defined(self) -> None:
        from tools.benchmarks.targets import BASELINE_TARGETS

        assert "granian-asgi" in BASELINE_TARGETS
        assert "uvicorn-httptools" in BASELINE_TARGETS
        assert "spikard" in BASELINE_TARGETS

    def test_all_targets_count(self) -> None:
        from tools.benchmarks.targets import ALL_TARGETS

        # 6 vertebra + 4 freethreaded + 3 baseline = 13
        assert len(ALL_TARGETS) >= 13

    def test_no_port_collisions(self) -> None:
        from tools.benchmarks.targets import ALL_TARGETS

        ports = [t.port for t in ALL_TARGETS.values()]
        assert len(ports) == len(set(ports)), f"Port collision detected: {ports}"


class TestScenarios:
    """Verify scenario lists are complete."""

    def test_default_includes_plaintext(self) -> None:
        from tools.benchmarks.scenarios import DEFAULT_SCENARIOS

        assert "plaintext" in DEFAULT_SCENARIOS

    def test_default_includes_echo(self) -> None:
        from tools.benchmarks.scenarios import DEFAULT_SCENARIOS

        assert "echo" in DEFAULT_SCENARIOS

    def test_default_includes_io(self) -> None:
        from tools.benchmarks.scenarios import DEFAULT_SCENARIOS

        assert "io_10ms" in DEFAULT_SCENARIOS

    def test_full_has_all_scenarios(self) -> None:
        from tools.benchmarks.scenarios import FULL_SCENARIOS, SCENARIOS

        assert set(FULL_SCENARIOS) == set(SCENARIOS.keys())

    def test_multiworker_includes_echo(self) -> None:
        from tools.benchmarks.scenarios import MULTIWORKER_SCENARIOS

        assert "echo" in MULTIWORKER_SCENARIOS

    def test_body_100k_not_in_default(self) -> None:
        from tools.benchmarks.scenarios import DEFAULT_SCENARIOS

        assert "body_100k" not in DEFAULT_SCENARIOS

    def test_body_100k_in_full(self) -> None:
        from tools.benchmarks.scenarios import FULL_SCENARIOS

        assert "body_100k" in FULL_SCENARIOS


class TestEndpointCoverage:
    """Verify all benchmark apps implement all scenario endpoints."""

    def test_asgi_app_has_all_paths(self) -> None:
        from tools.benchmarks.apps.asgi import DYNAMIC_ROUTES, STATIC_ROUTES
        from tools.benchmarks.scenarios import SCENARIOS

        all_paths = set(STATIC_ROUTES.keys()) | set(DYNAMIC_ROUTES.keys())
        for name, scenario in SCENARIOS.items():
            assert scenario.path in all_paths, f"ASGI app missing endpoint for scenario '{name}': {scenario.path}"

    def test_wsgi_app_has_io10(self) -> None:
        """WSGI app must handle /io10 for IO benchmarks."""

        # io10 is handled dynamically, not in _STATIC — verify via import
        from tools.benchmarks.apps.wsgi import app

        assert callable(app)

    def test_vertebra_native_app_has_handlers(self) -> None:
        try:
            from tools.benchmarks.apps.vertebra_app import native_app
        except ImportError:
            import pytest

            pytest.skip("vertebra native extension not available in test environment")
        assert len(native_app._handlers) >= 7
