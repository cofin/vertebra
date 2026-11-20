"""Unit tests for full-stack benchmark matrix scenarios and protocol/topology targets."""

from __future__ import annotations

import pytest
from tools.benchmarks import targets as bench_targets
from tools.benchmarks.scenarios import (
    DATA_SCENARIOS,
    DEFAULT_SCENARIOS,
    FULL_SCENARIOS,
    GRPC_NATIVE_SCENARIOS,
    GRPC_SCENARIOS,
    MULTIWORKER_SCENARIOS,
    QUICK_SCENARIOS,
    SCENARIOS,
    STORAGE_SCENARIOS,
)
from tools.benchmarks.targets import (
    ALL_TARGETS,
    BASELINE_TARGETS,
    DATA_BASELINE_TARGETS,
    DATA_FREETHREADED_TARGETS,
    DATA_TARGETS,
    FRAMEWORK_TARGETS,
    FREETHREADED_TARGETS,
    GRPC_TARGETS,
    VERTEBRA_TARGETS,
    Topology,
    apply_shm_broker_dsn,
)

ORIGINAL_TARGET_PORTS: dict[str, int] = {
    "vtb-embedded-native": 8000,
    "vtb-embedded-native-4w": 8016,
    "vtb-embedded-native-8w": 8017,
    "vtb-embedded-asgi": 8001,
    "vtb-embedded-wsgi": 8002,
    "vtb-shm-native-1w": 8010,
    "vtb-shm-native-4w": 8011,
    "vtb-shm-native-8w": 8014,
    "vtb-shm-asgi-1w": 8018,
    "vtb-shm-asgi-4w": 8012,
    "vtb-shm-asgi-8w": 8015,
    "vtb-shm-wsgi-4w": 8013,
    "vtb-ft-native-4w": 8022,
    "granian-asgi": 8003,
    "granian-rsgi": 8006,
    "uvicorn-httptools": 8004,
    "spikard": 8005,
}


def test_all_targets_have_unique_ports_and_preserve_original_seventeen() -> None:
    """Verify zero port collisions across ALL_TARGETS and unchanged original 17 target ports."""
    ports = [target.port for target in ALL_TARGETS.values()]
    assert len(ports) == len(set(ports))
    assert len(ORIGINAL_TARGET_PORTS) == 17
    for name, expected_port in ORIGINAL_TARGET_PORTS.items():
        assert name in ALL_TARGETS
        assert ALL_TARGETS[name].port == expected_port


def test_scenario_presets_exist_and_full_matches_registry() -> None:
    """Verify DATA, STORAGE, GRPC, and GRPC_NATIVE presets exist and FULL_SCENARIOS matches SCENARIOS."""
    assert len(FULL_SCENARIOS) == len(SCENARIOS)
    assert set(FULL_SCENARIOS) == set(SCENARIOS)
    assert QUICK_SCENARIOS == ["hello", "body_1k", "json", "io_10ms"]
    assert DEFAULT_SCENARIOS == ["hello", "plaintext", "body_1k", "body_10k", "json", "echo", "io_10ms"]
    assert MULTIWORKER_SCENARIOS == ["hello", "json", "echo", "io_10ms"]

    for preset in (DATA_SCENARIOS, STORAGE_SCENARIOS, GRPC_SCENARIOS, GRPC_NATIVE_SCENARIOS):
        assert len(preset) > 0
        for key in preset:
            assert key in SCENARIOS


@pytest.mark.parametrize(
    ("scenario_key", "expected_path", "expected_method", "expected_protocol", "expected_driver", "expected_grpc"),
    [
        ("db", "/db", "GET", "http1", "oha", None),
        ("queries_1", "/queries?queries=1", "GET", "http1", "oha", None),
        ("queries_5", "/queries?queries=5", "GET", "http1", "oha", None),
        ("queries_10", "/queries?queries=10", "GET", "http1", "oha", None),
        ("queries_20", "/queries?queries=20", "GET", "http1", "oha", None),
        ("updates_5", "/updates?queries=5", "GET", "http1", "oha", None),
        ("updates_20", "/updates?queries=20", "GET", "http1", "oha", None),
        ("cached_queries_10", "/cached-queries?count=10", "GET", "http1", "oha", None),
        ("cached_queries_100", "/cached-queries?count=100", "GET", "http1", "oha", None),
        ("object_get_4k", "/object/bench-4k", "GET", "http1", "oha", None),
        ("object_put_4k", "/object/bench-4k", "PUT", "http1", "oha", None),
        ("object_get_64k", "/object/bench-64k", "GET", "http1", "oha", None),
        (
            "grpc_db",
            "/vertebra.bench.BenchmarkService/GetDb",
            "POST",
            "grpc",
            "ghz",
            "vertebra.bench.BenchmarkService.GetDb",
        ),
        (
            "grpc_queries_10",
            "/vertebra.bench.BenchmarkService/GetQueries",
            "POST",
            "grpc",
            "ghz",
            "vertebra.bench.BenchmarkService.GetQueries",
        ),
        (
            "grpc_native_db",
            "/vertebra.rpc.ProcedureService/get_db",
            "POST",
            "grpc",
            "ghz",
            "vertebra.rpc.ProcedureService.get_db",
        ),
        (
            "grpc_native_queries_10",
            "/vertebra.rpc.ProcedureService/get_queries",
            "POST",
            "grpc",
            "ghz",
            "vertebra.rpc.ProcedureService.get_queries",
        ),
    ],
)
def test_matrix_scenario_metadata_contracts(
    scenario_key: str,
    expected_path: str,
    expected_method: str,
    expected_protocol: str,
    expected_driver: str,
    expected_grpc: str | None,
) -> None:
    """Verify protocol, driver, HTTP method, path, and gRPC method metadata for each matrix scenario."""
    scenario = SCENARIOS[scenario_key]
    assert scenario.path == expected_path
    assert scenario.method == expected_method
    assert scenario.protocol == expected_protocol
    assert scenario.driver == expected_driver
    assert scenario.grpc_method == expected_grpc
    assert scenario.load_mode == "closed_loop"


def test_matrix_scenario_bodies() -> None:
    """Verify request payloads for object PUT and gRPC multi-query scenarios."""
    assert SCENARIOS["object_put_4k"].body == "x" * 4096
    assert SCENARIOS["grpc_queries_10"].body == '{"count":10}'
    assert SCENARIOS["grpc_native_queries_10"].body == '{"count":10}'


@pytest.mark.parametrize(
    ("per_worker_name", "broker_name"),
    [
        ("vtb-data-shm-native-4w", "vtb-data-shm-native-4w-broker"),
        ("vtb-data-shm-rsgi-4w", "vtb-data-shm-rsgi-4w-broker"),
        ("vtb-data-shm-asgi-4w", "vtb-data-shm-asgi-4w-broker"),
    ],
)
def test_shm_broker_pairs_enforce_total_connection_budget_parity(
    per_worker_name: str,
    broker_name: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify per-worker vs shm_broker target pairs maintain equal 4x10 == 40 pool budgets."""
    per_worker = ALL_TARGETS[per_worker_name]
    broker = ALL_TARGETS[broker_name]

    assert per_worker.topology is Topology.SHM
    assert broker.topology is Topology.SHM
    assert per_worker.broker_mode == "per_worker"
    assert broker.broker_mode == "shm_broker"
    assert per_worker.pool_size is not None
    assert broker.pool_size is not None
    assert per_worker.workers * per_worker.pool_size == broker.pool_size == 40
    assert per_worker.env["VTB_BENCH_BROKER_MODE"] == "per_worker"

    monkeypatch.setenv("VTB_BENCH_DB_DSN", "postgres://user:test-token-placeholder@127.0.0.1:5432/bench")
    broker_env = broker.env
    assert broker_env["VTB_BENCH_BROKER_MODE"] == "shm_broker"
    assert broker_env["VTB_BENCH_DB_DSN"].endswith("?shm_broker=true")


@pytest.mark.parametrize(
    ("raw_dsn", "broker_mode", "expected"),
    [
        ("postgres://localhost/bench", "shm_broker", "postgres://localhost/bench?shm_broker=true"),
        (
            "postgres://localhost/bench?sslmode=disable",
            "shm_broker",
            "postgres://localhost/bench?sslmode=disable&shm_broker=true",
        ),
        ("postgres://localhost/bench?shm_broker=true", "shm_broker", "postgres://localhost/bench?shm_broker=true"),
        ("postgres://localhost/bench", "per_worker", "postgres://localhost/bench"),
        ("", "shm_broker", ""),
    ],
)
def test_apply_shm_broker_dsn_helper(raw_dsn: str, broker_mode: str, expected: str) -> None:
    """Verify apply_shm_broker_dsn appends shm_broker=true idempotently only in shm_broker mode."""
    assert apply_shm_broker_dsn(raw_dsn, broker_mode=broker_mode) == expected


@pytest.mark.parametrize(
    ("target_name", "expected_threads", "expected_transport", "expected_interface"),
    [
        ("vtb-ft-data-native-1t", "1", "native", None),
        ("vtb-ft-data-native-4t", "4", "native", None),
        ("vtb-ft-data-native-8t", "8", "native", None),
        ("vtb-ft-data-rsgi-4t", "4", "rsgi", "rsgi"),
        ("vtb-ft-data-asgi-4t", "4", "asgi", "asgi"),
    ],
)
def test_freethreaded_data_targets_inject_ft_env_and_strict_mode(
    target_name: str,
    expected_threads: str,
    expected_transport: str,
    expected_interface: str | None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify FT data targets call _ft_env(), enable strict free-threading, and configure thread counts."""
    called: list[bool] = []

    def fake_ft_env() -> dict[str, str]:
        called.append(True)
        return {"LD_LIBRARY_PATH": "/opt/ft/lib", "PYTHONPATH": "/opt/ft/site-packages"}

    monkeypatch.setattr(bench_targets, "_ft_env", fake_ft_env)
    target = ALL_TARGETS[target_name]
    assert target_name in DATA_FREETHREADED_TARGETS
    assert target.topology is Topology.FREE_THREADED
    assert target.transport == expected_transport

    env = target.env
    assert called == [True]
    assert env["LD_LIBRARY_PATH"] == "/opt/ft/lib"
    assert env["PYTHONPATH"] == "/opt/ft/site-packages"
    assert env["VERTEBRA_STRICT_FREE_THREADING"] == "1"
    assert env["VERTEBRA_THREADS"] == expected_threads
    assert env["VERTEBRA_WORKERS"] == "0"
    assert env["VERTEBRA_PORT"] == str(target.port)
    if expected_interface is None:
        assert "VERTEBRA_INTERFACE" not in env
    else:
        assert env["VERTEBRA_INTERFACE"] == expected_interface


def test_grpc_targets_distinguish_asgi_and_native_h2c() -> None:
    """Verify vtb-grpc-asgi and vtb-grpc-native-h2c use distinct transports, apps, and H2C flags."""
    asgi_target = ALL_TARGETS["vtb-grpc-asgi"]
    native_target = ALL_TARGETS["vtb-grpc-native-h2c"]

    assert set(GRPC_TARGETS) == {"vtb-grpc-asgi", "vtb-grpc-native-h2c"}
    assert asgi_target.protocol == "grpc"
    assert asgi_target.transport == "grpc-asgi"
    assert asgi_target.env["VERTEBRA_H2C"] == "1"
    assert "--asgi" in asgi_target.cmd
    assert "tools.benchmarks.apps.data_grpc:grpc_asgi_app" in asgi_target.cmd

    assert native_target.protocol == "grpc"
    assert native_target.transport == "grpc-native-h2c"
    assert native_target.env["VERTEBRA_H2C"] == "1"
    assert "--asgi" not in native_target.cmd
    assert "tools.benchmarks.apps.data_grpc:grpc_native_app" in native_target.cmd


@pytest.mark.parametrize(
    ("target_name", "required_tokens"),
    [
        ("granian-rsgi-data", ("granian", "tools.benchmarks.apps.data_rsgi:rsgi_app", "--interface", "rsgi")),
        ("granian-asgi-data", ("granian", "tools.benchmarks.apps.data_asgi:asgi_app", "--interface", "asgi")),
        (
            "uvicorn-httptools-data",
            ("uvicorn", "tools.benchmarks.apps.data_asgi:asgi_app", "--http", "httptools", "--loop", "uvloop"),
        ),
    ],
)
def test_data_baseline_targets_isolated_from_static_baselines(
    target_name: str,
    required_tokens: tuple[str, ...],
) -> None:
    """Verify external data baselines reside in DATA_BASELINE_TARGETS with expected CLI flags."""
    assert target_name in DATA_BASELINE_TARGETS
    assert target_name not in BASELINE_TARGETS
    cmd = ALL_TARGETS[target_name].cmd
    for token in required_tokens:
        assert token in cmd


def test_target_category_groups_do_not_pollute_static_groups() -> None:
    """Verify DATA, GRPC, DATA_BASELINE, DATA_FREETHREADED, and FRAMEWORK targets stay isolated from static groups."""
    assert "vtb-embedded-rsgi" in VERTEBRA_TARGETS
    assert "vtb-data-shm-wsgi-4w" in DATA_TARGETS

    static_groups = set(VERTEBRA_TARGETS) | set(BASELINE_TARGETS) | set(FREETHREADED_TARGETS)
    matrix_groups = (
        set(DATA_TARGETS)
        | set(DATA_BASELINE_TARGETS)
        | set(DATA_FREETHREADED_TARGETS)
        | set(FRAMEWORK_TARGETS)
        | set(GRPC_TARGETS)
    )
    assert static_groups.isdisjoint(matrix_groups)

    expected_frameworks = {
        "django-bolt-data",
        "spikard-data",
        "litestar-vtb-data",
        "litestar-granian-data",
        "litestar-uvicorn-data",
        "fastapi-vtb-data",
        "fastapi-granian-data",
        "fastapi-uvicorn-data",
        "django-vtb-data",
        "django-granian-data",
        "django-uvicorn-data",
        "django-wsgi-vtb-data",
        "django-wsgi-granian-data",
        "flask-vtb-data",
        "flask-granian-data",
        "blacksheep-vtb-data",
        "blacksheep-granian-data",
        "blacksheep-uvicorn-data",
    }
    assert set(FRAMEWORK_TARGETS) == expected_frameworks
    assert ALL_TARGETS["django-bolt-data"].transport == "rust-actix"
    assert "--processes" in ALL_TARGETS["django-bolt-data"].cmd
    assert ALL_TARGETS["spikard-data"].transport == "rust-spikard"
    assert ALL_TARGETS["flask-vtb-data"].transport == "wsgi"
