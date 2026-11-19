"""Unit tests for benchmark CLI matrix filters, preset resolution, driver dispatch, and broker hooks."""

from __future__ import annotations

import json
import time
from typing import TYPE_CHECKING, Any

import pytest
import rich_click as click
from click.testing import CliRunner
from tools.benchmarks import cli as bench_cli
from tools.benchmarks.runner import BrokerHostHook
from tools.benchmarks.scenarios import (
    DATA_SCENARIOS,
    GRPC_NATIVE_SCENARIOS,
    GRPC_SCENARIOS,
    STORAGE_SCENARIOS,
)
from tools.benchmarks.targets import (
    ALL_TARGETS,
    DATA_BASELINE_TARGETS,
    DATA_TARGETS,
    FRAMEWORK_TARGETS,
    GRPC_TARGETS,
)

if TYPE_CHECKING:
    from collections.abc import Sequence
    from pathlib import Path
    from types import TracebackType

_MATRIX_COLUMNS: tuple[str, ...] = (
    "adapter",
    "protocol",
    "transport",
    "broker_mode",
    "load_mode",
    "target_rps",
    "pool_size",
    "gil_enabled",
)


def _fake_metrics(load_mode: str = "closed_loop", target_rps: float | None = None) -> dict[str, Any]:
    """Build deterministic normalized metrics returned by mocked load runners."""
    return {
        "rps": 42000.0,
        "latency_mean_s": 0.0015,
        "latency_p50_s": 0.001,
        "latency_p95_s": 0.003,
        "latency_p99_s": 0.005,
        "latency_p999_s": 0.008,
        "latency_max_s": 0.012,
        "total_requests": 42000,
        "errors": 0,
        "load_mode": load_mode,
        "target_rps": target_rps,
    }


@pytest.mark.parametrize(
    ("kwargs", "expected_present", "expected_absent"),
    [
        (
            {"protocol": "grpc"},
            ["vtb-grpc-asgi", "vtb-grpc-native-h2c"],
            ["vtb-embedded-native", "vtb-data-shm-native-4w-broker"],
        ),
        (
            {"protocol": "GRPC"},
            ["vtb-grpc-asgi", "vtb-grpc-native-h2c"],
            ["vtb-embedded-native"],
        ),
        (
            {"topology": "free-threaded"},
            ["vtb-ft-native-4w", "vtb-ft-data-native-4t"],
            ["vtb-embedded-native", "granian-asgi"],
        ),
        (
            {"broker_mode": "shm_broker"},
            ["vtb-data-shm-native-4w-broker", "vtb-data-shm-rsgi-4w-broker"],
            ["vtb-data-shm-native-4w", "vtb-embedded-native"],
        ),
        (
            {"topology": "shm", "broker_mode": "shm_broker"},
            ["vtb-data-shm-native-4w-broker"],
            ["vtb-data-shm-native-4w", "vtb-data-embedded-native-4w"],
        ),
        (
            {"protocol": "http1", "topology": "baseline", "adapter": "none"},
            ["granian-asgi", "uvicorn-httptools"],
            ["vtb-embedded-native", "vtb-grpc-asgi"],
        ),
    ],
)
def test_filter_targets_narrows_and_preserves_order(
    kwargs: dict[str, str],
    expected_present: list[str],
    expected_absent: list[str],
) -> None:
    """Verify filter_targets matches Target metadata case-insensitively and preserves order."""
    source = list(ALL_TARGETS)
    filtered = bench_cli.filter_targets(source, **kwargs)

    for name in expected_present:
        assert name in filtered
    for name in expected_absent:
        assert name not in filtered

    positions = [source.index(name) for name in filtered]
    assert positions == sorted(positions)


@pytest.mark.parametrize(
    ("preset", "expected"),
    [
        ("data", DATA_SCENARIOS),
        ("storage", STORAGE_SCENARIOS),
        ("grpc", GRPC_SCENARIOS),
        ("grpc-native", GRPC_NATIVE_SCENARIOS),
        ("db,json", ["db", "json"]),
    ],
)
def test_resolve_scenarios_presets(preset: str, expected: list[str]) -> None:
    """Verify _resolve_scenarios resolves matrix presets and comma-separated names."""
    assert bench_cli._resolve_scenarios(preset) == expected


def test_resolve_scenarios_unknown_raises_bad_parameter() -> None:
    """Verify _resolve_scenarios rejects unknown scenario names with click.BadParameter."""
    with pytest.raises(click.BadParameter, match="Unknown scenario: nonexistent_scenario"):
        bench_cli._resolve_scenarios("nonexistent_scenario")


@pytest.mark.parametrize(
    ("target_spec", "expected"),
    [
        ("data", DATA_TARGETS),
        ("data-baselines", DATA_BASELINE_TARGETS),
        ("frameworks", FRAMEWORK_TARGETS),
        ("grpc", GRPC_TARGETS),
    ],
)
def test_resolve_targets_presets(target_spec: str, expected: list[str]) -> None:
    """Verify _resolve_targets resolves matrix target presets."""
    assert bench_cli._resolve_targets(target_spec) == expected


def test_resolve_targets_unknown_raises_bad_parameter() -> None:
    """Verify _resolve_targets rejects unknown target names with click.BadParameter."""
    with pytest.raises(click.BadParameter, match="Unknown target: nonexistent_target"):
        bench_cli._resolve_targets("nonexistent_target")


def test_list_targets_filters_grpc_and_displays_matrix_columns() -> None:
    """Verify list-targets --protocol grpc prints gRPC targets and matrix columns while omitting HTTP/1.1."""
    runner = CliRunner(env={"COLUMNS": "160"})
    result = runner.invoke(bench_cli.cli, ["list-targets", "--protocol", "grpc"])

    assert result.exit_code == 0, result.output
    assert "Protocol" in result.output
    assert "Transport" in result.output
    assert "Broker" in result.output
    assert "vtb-grpc-asgi" in result.output
    assert "vtb-grpc-native-h2c" in result.output
    assert "vtb-embedded-native" not in result.output
    assert "granian-asgi" not in result.output


@pytest.mark.parametrize(
    "args",
    [
        ["run", "--load-mode", "open_loop"],
        ["run", "--load-mode", "open_loop", "--rate", "0"],
        ["run", "--load-mode", "open_loop", "--rate", "-25"],
        ["run", "--rate", "0"],
    ],
)
def test_run_rejects_invalid_open_loop_rate(args: list[str]) -> None:
    """Verify run rejects open_loop without --rate and any non-positive --rate with BadParameter."""
    runner = CliRunner()
    result = runner.invoke(bench_cli.cli, args)

    assert result.exit_code != 0
    assert "--rate" in result.output


def test_run_broker_target_invokes_hooks_and_persists_matrix_columns(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Verify SHM broker target wires BrokerHostHook pre_start/post_stop and persists all 8 matrix columns."""
    lifecycle_events: list[str] = []
    recorded_hooks: list[Sequence[Any] | None] = []
    oha_calls: list[dict[str, Any]] = []
    stored_rows: list[dict[str, Any]] = []

    class FakeServerRunner:
        def __init__(
            self,
            cmd: list[str],
            host: str = "127.0.0.1",
            port: int = 8000,
            env: dict[str, str] | None = None,
            hooks: Sequence[Any] | None = None,
        ) -> None:
            self.cmd = cmd
            self.host = host
            self.port = port
            self.env = env or {}
            self.hooks = list(hooks) if hooks is not None else []
            recorded_hooks.append(hooks)

        def __enter__(self) -> FakeServerRunner:
            for hook in self.hooks:
                hook.pre_start()
            return self

        def __exit__(
            self,
            exc_type: type[BaseException] | None,
            exc_val: BaseException | None,
            exc_tb: TracebackType | None,
        ) -> None:
            for hook in reversed(self.hooks):
                hook.post_stop()

    class FakeOhaRunner:
        def __init__(
            self,
            url: str,
            duration: int = 10,
            concurrency: int = 64,
            method: str = "GET",
            body: str | None = None,
            rate: float | None = None,
        ) -> None:
            oha_calls.append(
                {
                    "url": url,
                    "duration": duration,
                    "concurrency": concurrency,
                    "method": method,
                    "body": body,
                    "rate": rate,
                }
            )
            self.rate = rate

        def run(self) -> dict[str, Any]:
            mode = "open_loop" if self.rate is not None else "closed_loop"
            return _fake_metrics(load_mode=mode, target_rps=self.rate)

    class FakeStore:
        def create_run(self, note: str = "") -> str:
            return f"run-test-{note or 'default'}"

        def insert_result(self, run_id: str, result: dict[str, Any]) -> None:
            stored_rows.append({"run_id": run_id, **result})

        def close(self) -> None:
            return None

    def fake_pre_start(self: BrokerHostHook) -> dict[str, str]:
        lifecycle_events.append(f"pre_start:{self.config.shm_name}:{self.config.pool_size}")
        return {"VERTEBRA_BROKER_SHM": self.config.shm_name}

    def fake_post_stop(self: BrokerHostHook) -> None:
        lifecycle_events.append(f"post_stop:{self.config.shm_name}")

    monkeypatch.setattr(bench_cli, "ServerRunner", FakeServerRunner)
    monkeypatch.setattr(bench_cli, "OhaRunner", FakeOhaRunner)
    monkeypatch.setattr(bench_cli, "BenchmarkStore", FakeStore)
    monkeypatch.setattr(BrokerHostHook, "pre_start", fake_pre_start)
    monkeypatch.setattr(BrokerHostHook, "post_stop", fake_post_stop)
    monkeypatch.setattr(time, "sleep", lambda _: None)

    out_jsonl = tmp_path / "matrix.jsonl"
    runner = CliRunner()
    result = runner.invoke(
        bench_cli.cli,
        [
            "run",
            "-t",
            "vtb-data-shm-native-4w-broker",
            "-s",
            "db",
            "-d",
            "1",
            "-c",
            "16",
            "-w",
            "0",
            "--broker-mode",
            "shm_broker",
            "--load-mode",
            "open_loop",
            "--rate",
            "1500",
            "-o",
            str(out_jsonl),
        ],
    )

    assert result.exit_code == 0, result.output
    assert len(recorded_hooks) == 1
    assert recorded_hooks[0] is not None
    assert len(recorded_hooks[0]) == 1
    assert isinstance(recorded_hooks[0][0], BrokerHostHook)
    assert lifecycle_events == [
        "pre_start:vertebra-bench-vtb-data-shm-native-4w-broker-8111:40",
        "post_stop:vertebra-bench-vtb-data-shm-native-4w-broker-8111",
    ]
    assert len(oha_calls) == 1
    assert oha_calls[0]["rate"] == pytest.approx(1500.0)

    jsonl_lines = out_jsonl.read_text(encoding="utf-8").strip().splitlines()
    assert len(jsonl_lines) == 1
    row = json.loads(jsonl_lines[0])
    assert len(stored_rows) == 1
    stored_row = stored_rows[0]

    for col in _MATRIX_COLUMNS:
        assert col in row
        assert col in stored_row

    assert row["adapter"] == "none"
    assert row["protocol"] == "http1"
    assert row["transport"] == "native"
    assert row["broker_mode"] == "shm_broker"
    assert row["load_mode"] == "open_loop"
    assert row["target_rps"] == pytest.approx(1500.0)
    assert row["pool_size"] == 40
    assert row["gil_enabled"] is True


def test_run_dispatches_grpc_scenario_to_ghz_driver(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Verify gRPC targets and scenarios dispatch through create_driver('ghz') with grpc:// URL."""
    dispatched: list[tuple[str, Any]] = []

    class FakeServerRunner:
        def __init__(self, **_kwargs: Any) -> None:
            pass

        def __enter__(self) -> FakeServerRunner:
            return self

        def __exit__(
            self,
            exc_type: type[BaseException] | None,
            exc_val: BaseException | None,
            exc_tb: TracebackType | None,
        ) -> None:
            return None

    class FakeDriver:
        def __init__(self, name: str) -> None:
            self.name = name

        def run(self, config: Any) -> dict[str, Any]:
            dispatched.append((self.name, config))
            return _fake_metrics(load_mode="closed_loop", target_rps=None)

    class FakeStore:
        def create_run(self, note: str = "") -> str:
            return "run-grpc"

        def insert_result(self, run_id: str, result: dict[str, Any]) -> None:
            return None

        def close(self) -> None:
            return None

    monkeypatch.setattr(bench_cli, "ServerRunner", FakeServerRunner)
    monkeypatch.setattr(bench_cli, "create_driver", FakeDriver)
    monkeypatch.setattr(bench_cli, "BenchmarkStore", FakeStore)
    monkeypatch.setattr(time, "sleep", lambda _: None)

    out_jsonl = tmp_path / "grpc.jsonl"
    runner = CliRunner()
    result = runner.invoke(
        bench_cli.cli,
        [
            "run",
            "-t",
            "vtb-grpc-asgi",
            "-s",
            "grpc_db",
            "--protocol",
            "grpc",
            "-d",
            "1",
            "-c",
            "8",
            "-w",
            "0",
            "-o",
            str(out_jsonl),
        ],
    )

    assert result.exit_code == 0, result.output
    assert len(dispatched) == 1
    driver_name, config = dispatched[0]
    assert driver_name == "ghz"
    assert config.url == "grpc://127.0.0.1:8120"
    assert config.grpc_call == "vertebra.bench.BenchmarkService.GetDb"

    row = json.loads(out_jsonl.read_text(encoding="utf-8").strip())
    assert row["protocol"] == "grpc"
    assert row["transport"] == "grpc-asgi"
    assert row["load_mode"] == "closed_loop"
    assert row["target_rps"] is None
    assert row["gil_enabled"] is True
