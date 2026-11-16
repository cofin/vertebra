"""Unit tests for benchmark infrastructure."""

from __future__ import annotations

import subprocess
import sys
import tomllib
from pathlib import Path

import pytest
import yaml
from tools.benchmarks import targets as bench_targets
from tools.benchmarks.apps.asgi import app as asgi_app
from tools.benchmarks.apps.vertebra_app import native_app
from tools.benchmarks.apps.wsgi import app as wsgi_app
from tools.benchmarks.runner import ServerRunner
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


@pytest.mark.parametrize(
    ("name", "expected_topology", "expected_workers"),
    [
        ("vtb-shm-native-1w", "shm", "1"),
        ("vtb-shm-native-4w", "shm", "4"),
        ("vtb-shm-native-8w", "shm", "8"),
        ("vtb-shm-asgi-1w", "shm", "1"),
        ("vtb-shm-asgi-4w", "shm", "4"),
        ("vtb-shm-asgi-8w", "shm", "8"),
        ("vtb-embedded-native-4w", "embedded", "4"),
        ("vtb-embedded-native-8w", "embedded", "8"),
    ],
)
def test_shm_and_embedded_multiworker_targets_pass_explicit_cli_flags(
    name: str,
    expected_topology: str,
    expected_workers: str,
) -> None:
    """Verify multi-worker benchmark targets pass explicit --topology and --workers CLI flags."""
    assert name in ALL_TARGETS
    cmd = ALL_TARGETS[name].cmd
    assert "--topology" in cmd
    assert cmd[cmd.index("--topology") + 1] == expected_topology
    assert "--workers" in cmd
    assert cmd[cmd.index("--workers") + 1] == expected_workers


def test_granian_rsgi_baseline_target_exists() -> None:
    """Verify granian-rsgi baseline benchmark target is registered."""
    assert "granian-rsgi" in BASELINE_TARGETS
    cmd = ALL_TARGETS["granian-rsgi"].cmd
    assert "--interface" in cmd
    assert cmd[cmd.index("--interface") + 1] == "rsgi"


def test_ft_env_prefers_315t_before_314t(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify _ft_env probes FT_PYTHON / 3.15t before falling back to 3.14t."""
    calls: list[str] = []

    def fake_which(name: str) -> str | None:
        return "/usr/bin/uv" if name == "uv" else None

    def fake_check_output(cmd: list[str], **_kwargs: object) -> str:
        version = cmd[-1]
        calls.append(version)
        if version == "3.15t":
            return "/opt/python/3.15t/bin/python3.15t\n"
        raise AssertionError(f"unexpected version probe: {version}")

    monkeypatch.delenv("FT_PYTHON", raising=False)
    monkeypatch.setattr("tools.benchmarks.targets.shutil.which", fake_which)
    monkeypatch.setattr("tools.benchmarks.targets.subprocess.check_output", fake_check_output)
    env = bench_targets._ft_env()
    assert calls == ["3.15t"]
    assert "3.15t" in env["LD_LIBRARY_PATH"]


_REPO_ROOT = Path(__file__).resolve().parents[4]


@pytest.mark.parametrize(
    ("engine", "version", "expected_tokens", "forbidden_tokens"),
    [
        (
            "mysql",
            "8.4",
            (
                "VERTEBRA_CONFORMANCE_DOCKER=1",
                "VERTEBRA_CONFORMANCE_ENGINES=mysql",
                "VERTEBRA_CONFORMANCE_MYSQL_VERSION=8.4",
                "pytest src/py/tests/integration/db src/py/tests/integration/extensions -v -rs",
            ),
            ("VERTEBRA_CONFORMANCE_DUCKDB_HTTPFS",),
        ),
        (
            "mariadb",
            "11.4",
            (
                "VERTEBRA_CONFORMANCE_DOCKER=1",
                "VERTEBRA_CONFORMANCE_ENGINES=mariadb",
                "VERTEBRA_CONFORMANCE_MARIADB_VERSION=11.4",
                "pytest src/py/tests/integration/db src/py/tests/integration/extensions -v -rs",
            ),
            ("VERTEBRA_CONFORMANCE_DUCKDB_HTTPFS",),
        ),
        (
            "mssql",
            "2022",
            (
                "VERTEBRA_CONFORMANCE_DOCKER=1",
                "VERTEBRA_CONFORMANCE_ENGINES=mssql",
                "VERTEBRA_CONFORMANCE_MSSQL_VERSION=2022",
                "pytest src/py/tests/integration/db src/py/tests/integration/extensions -v -rs",
            ),
            ("VERTEBRA_CONFORMANCE_DUCKDB_HTTPFS",),
        ),
        (
            "oracle",
            "23ai",
            (
                "VERTEBRA_CONFORMANCE_DOCKER=1",
                "VERTEBRA_CONFORMANCE_ENGINES=oracle",
                "VERTEBRA_CONFORMANCE_ORACLE_VERSION=23ai",
                "pytest src/py/tests/integration/db src/py/tests/integration/extensions -v -rs",
            ),
            ("VERTEBRA_CONFORMANCE_DUCKDB_HTTPFS",),
        ),
        (
            "duckdb",
            "",
            (
                "VERTEBRA_CONFORMANCE_DOCKER=1",
                "VERTEBRA_CONFORMANCE_ENGINES=duckdb",
                "VERTEBRA_CONFORMANCE_DUCKDB_HTTPFS=1",
                "pytest src/py/tests/integration/db src/py/tests/integration/extensions -v -rs",
            ),
            ("VERTEBRA_CONFORMANCE_DUCKDB_VERSION",),
        ),
    ],
)
def test_db_conformance_dry_run_env_expansion(
    engine: str,
    version: str,
    expected_tokens: tuple[str, ...],
    forbidden_tokens: tuple[str, ...],
) -> None:
    """Verify ``make -n db-conformance`` expands engine, version, and DuckDB httpfs env variables."""
    cmd = ["make", "-n", "db-conformance", f"ENGINE={engine}"]
    if version:
        cmd.append(f"VERSION={version}")
    proc = subprocess.run(cmd, cwd=_REPO_ROOT, capture_output=True, text=True, check=True)
    for token in expected_tokens:
        assert token in proc.stdout
    for token in forbidden_tokens:
        assert token not in proc.stdout


def test_db_conformance_missing_engine_exits_two() -> None:
    """Verify ``make db-conformance`` without ``ENGINE`` exits ``2`` with the required usage error."""
    proc = subprocess.run(
        ["make", "db-conformance"],
        cwd=_REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 2
    assert "ENGINE is required: duckdb, mysql, mariadb, mssql, oracle" in proc.stderr


def test_makefile_isolates_pypi_duckdb_and_oracle_test_has_no_fallback() -> None:
    """Verify PyPI ``duckdb`` is restricted to ``bench`` and ``oracle-test`` has no ``test_freethreaded_gil.py`` fallback."""
    makefile_text = (_REPO_ROOT / "Makefile").read_text(encoding="utf-8")
    assert "--group duckdb" not in makefile_text
    assert "PY_TEST_GROUPS" not in makefile_text

    pyproject = tomllib.loads((_REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    dep_groups = pyproject["dependency-groups"]
    assert "duckdb>=1.0; python_version < '3.14'" in dep_groups["bench"]
    assert all("duckdb" not in str(entry) for entry in dep_groups["test"])

    oracle_dry_run = subprocess.run(
        ["make", "-n", "oracle-test"],
        cwd=_REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    assert "src/py/tests/unit/db/test_oracle_adapter.py" in oracle_dry_run.stdout
    assert "src/py/tests/integration/db/test_oracle_native.py" in oracle_dry_run.stdout
    assert "test_freethreaded_gil.py" not in oracle_dry_run.stdout


def test_ci_workflow_defines_db_conformance_matrix_and_oracle_job() -> None:
    """Verify ``.github/workflows/ci.yml`` defines ``test-db-conformance`` (10 rows) and non-blocking ``conformance-oracle``."""
    ci_data = yaml.safe_load((_REPO_ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8"))
    jobs = ci_data["jobs"]

    db_job = jobs["test-db-conformance"]
    assert db_job["needs"] == ["changes", "lint"]
    assert db_job["timeout-minutes"] == 45
    assert db_job["strategy"]["fail-fast"] is False
    matrix_rows = db_job["strategy"]["matrix"]["include"]
    assert matrix_rows == [
        {"engine": "duckdb", "version": "", "python-version": "3.12"},
        {"engine": "duckdb", "version": "", "python-version": "3.14t"},
        {"engine": "sqlite", "version": "", "python-version": "3.12"},
        {"engine": "sqlite", "version": "", "python-version": "3.14t"},
        {"engine": "mysql", "version": "8.0", "python-version": "3.12"},
        {"engine": "mysql", "version": "8.4", "python-version": "3.12"},
        {"engine": "mysql", "version": "9.7", "python-version": "3.12"},
        {"engine": "mysql", "version": "8.4", "python-version": "3.14t"},
        {"engine": "mariadb", "version": "11.4", "python-version": "3.12"},
        {"engine": "mariadb", "version": "11.8", "python-version": "3.12"},
        {"engine": "mssql", "version": "2022", "python-version": "3.12"},
        {"engine": "mssql", "version": "2025", "python-version": "3.12"},
        {"engine": "redis", "version": "", "python-version": "3.12"},
        {"engine": "redis", "version": "", "python-version": "3.14t"},
        {"engine": "valkey", "version": "", "python-version": "3.12"},
        {"engine": "valkey", "version": "", "python-version": "3.14t"},
    ]
    assert any(
        step.get("run") == "make db-conformance ENGINE=${{ matrix.engine }} VERSION=${{ matrix.version }}"
        for step in db_job["steps"]
    )

    oracle_job = jobs["conformance-oracle"]
    assert oracle_job["needs"] == ["changes", "lint"]
    assert oracle_job["timeout-minutes"] == 45
    assert oracle_job["continue-on-error"] is True
    assert any(step.get("run") == "make db-conformance ENGINE=oracle VERSION=23ai" for step in oracle_job["steps"])


def test_server_runner_exits_with_utf8_and_non_utf8_binary_stderr() -> None:
    """Verify ServerRunner decodes mixed UTF-8 and invalid UTF-8 bytes on early child exit."""
    cmd = [
        sys.executable,
        "-c",
        "import sys; sys.stderr.buffer.write(b'fatal startup failure: \\xff\\xfe\\n'); sys.stderr.buffer.flush(); sys.exit(3)",
    ]
    runner = ServerRunner(cmd, host="127.0.0.1", port=59997)
    try:
        with pytest.raises(RuntimeError) as exc_info:
            runner.start()
        message = str(exc_info.value)
        assert "Server process exited with code 3" in message
        assert "fatal startup failure:" in message
        assert "\ufffd" in message
    finally:
        runner.stop()
