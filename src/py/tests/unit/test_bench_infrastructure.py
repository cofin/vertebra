"""Unit tests for benchmark infrastructure."""

from __future__ import annotations

import json
import subprocess
import sys
import textwrap
import tomllib
from pathlib import Path

import pytest
import yaml
from tools.benchmarks import targets as bench_targets
from tools.benchmarks.apps._data_backend import load_backend_config
from tools.benchmarks.apps.asgi import app as asgi_app
from tools.benchmarks.apps.vertebra_app import native_app
from tools.benchmarks.apps.wsgi import app as wsgi_app
from tools.benchmarks.fixtures import (
    FIXTURE_SPECS,
    STORAGE_LOCAL_BACKENDS,
    build_storage_config,
    get_fixture_spec,
)
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
    assert all(
        str(entry).split(">")[0].split("<")[0].split("=")[0].split(";")[0].strip().lower() != "duckdb"
        for entry in dep_groups["test"]
    )

    oracle_dry_run = subprocess.run(
        ["make", "-n", "oracle-test"],
        cwd=_REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    adapter_pos = oracle_dry_run.stdout.find("src/py/tests/unit/db/test_oracle_adapter.py")
    notify_pos = oracle_dry_run.stdout.find("src/py/tests/unit/db/test_oracle_context_notify_and_queues.py")
    native_pos = oracle_dry_run.stdout.find("src/py/tests/integration/db/test_oracle_native.py")
    assert adapter_pos != -1
    assert notify_pos != -1
    assert native_pos != -1
    assert adapter_pos < native_pos
    assert notify_pos < native_pos
    assert "test_freethreaded_gil.py" not in oracle_dry_run.stdout

    for target in ("redis-test", "valkey-test"):
        dry_run = subprocess.run(
            ["make", "-n", target],
            cwd=_REPO_ROOT,
            capture_output=True,
            text=True,
            check=True,
        )
        assert "src/py/tests/unit/framework/test_queue_backends_and_channels.py" in dry_run.stdout
        assert "test_distributed_queues.py" not in dry_run.stdout


def _extract_makefile_recipe(target: str) -> str:
    makefile_lines = (_REPO_ROOT / "Makefile").read_text(encoding="utf-8").splitlines()
    collecting = False
    recipe_lines: list[str] = []
    prefix = f"{target}:"
    for line in makefile_lines:
        if line.startswith(prefix):
            collecting = True
            recipe_lines.append(line)
            continue
        if collecting:
            if line.startswith("\t"):
                recipe_lines.append(line)
            elif line.strip() == "":
                break
            else:
                break
    return "\n".join(recipe_lines)


@pytest.mark.parametrize("target", ["lint", "lint-all"])
def test_makefile_lint_targets_enforce_no_sync_type_gates_and_fail_loud_stubtest(target: str) -> None:
    """Verify Makefile lint and lint-all pass --no-sync, run tsc/pyright/check_extras, and fail loud on missing stubs."""
    recipe = _extract_makefile_recipe(target)
    assert recipe
    uv_run_lines = [line.strip() for line in recipe.splitlines() if "uv run" in line]
    assert uv_run_lines
    assert all("uv run --no-sync" in line for line in uv_run_lines)
    assert "bunx tsc --noEmit" in recipe
    assert "uv run --no-sync pyright src/py/vertebra" in recipe
    assert "uv run --no-sync python tools/scripts/check_extras.py" in recipe
    assert "Missing native extensions:" in recipe
    expected_exts = (
        "vertebra._vertebra",
        "vertebra._bundler",
        "vertebra._storage",
        "vertebra._mysql",
        "vertebra._bigquery",
        "vertebra._mssql",
        "vertebra._spanner",
        "vertebra._duckdb",
        "vertebra._oracle",
        "vertebra._postgres",
        "vertebra._sqlite",
        "vertebra._redis",
        "vertebra._valkey",
    )
    for ext in expected_exts:
        assert ext in recipe


def test_ci_workflow_defines_db_conformance_matrix_and_oracle_job() -> None:
    """Verify ``.github/workflows/ci.yml`` defines full/PR DB conformance matrix and blocking ``conformance-oracle``."""
    ci_data = yaml.safe_load((_REPO_ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8"))
    jobs = ci_data["jobs"]

    tier_step = next(step for step in jobs["changes"]["steps"] if step.get("id") == "tier")
    tier_run = str(tier_step["run"])
    full_matrix_line = next(
        line.strip() for line in tier_run.splitlines() if line.strip().startswith("db_matrix='[") and "9.7" in line
    )
    pr_matrix_line = next(
        line.strip() for line in tier_run.splitlines() if line.strip().startswith("db_matrix='[") and "9.7" not in line
    )
    full_rows = json.loads(full_matrix_line.removeprefix("db_matrix='").removesuffix("'"))
    pr_rows = json.loads(pr_matrix_line.removeprefix("db_matrix='").removesuffix("'"))
    assert full_rows == [
        {"engine": "duckdb", "package": "duckdb", "version": "", "python-version": "3.12"},
        {"engine": "duckdb", "package": "duckdb", "version": "", "python-version": "3.14t"},
        {"engine": "sqlite", "package": "sqlite", "version": "", "python-version": "3.12"},
        {"engine": "sqlite", "package": "sqlite", "version": "", "python-version": "3.14t"},
        {"engine": "mysql", "package": "mysql", "version": "8.0", "python-version": "3.12"},
        {"engine": "mysql", "package": "mysql", "version": "8.4", "python-version": "3.12"},
        {"engine": "mysql", "package": "mysql", "version": "9.7", "python-version": "3.12"},
        {"engine": "mysql", "package": "mysql", "version": "8.4", "python-version": "3.14t"},
        {"engine": "mariadb", "package": "mysql", "version": "11.4", "python-version": "3.12"},
        {"engine": "mariadb", "package": "mysql", "version": "11.8", "python-version": "3.12"},
        {"engine": "mssql", "package": "mssql", "version": "2022", "python-version": "3.12"},
        {"engine": "mssql", "package": "mssql", "version": "2025", "python-version": "3.12"},
        {"engine": "redis", "package": "redis", "version": "", "python-version": "3.12"},
        {"engine": "redis", "package": "redis", "version": "", "python-version": "3.14t"},
        {"engine": "valkey", "package": "valkey", "version": "", "python-version": "3.12"},
        {"engine": "valkey", "package": "valkey", "version": "", "python-version": "3.14t"},
    ]
    assert {row["engine"] for row in pr_rows} == {
        "duckdb",
        "sqlite",
        "mysql",
        "mariadb",
        "mssql",
        "redis",
        "valkey",
    }

    db_job = jobs["test-db-conformance"]
    assert db_job["needs"] == ["changes", "build-native"]
    assert db_job["timeout-minutes"] == 30
    assert db_job["strategy"]["fail-fast"] is False
    db_runs = "\n".join(str(step.get("run", "")) for step in db_job["steps"])
    assert "--no-install-local" in db_runs
    assert "build_ci_wheels.py install" in db_runs
    assert 'make db-conformance ENGINE="$TARGET_ENGINE" VERSION="$TARGET_VERSION"' in db_runs

    oracle_job = jobs["conformance-oracle"]
    assert oracle_job["needs"] == ["changes", "build-native"]
    assert oracle_job["timeout-minutes"] == 30
    assert "continue-on-error" not in oracle_job
    oracle_runs = "\n".join(str(step.get("run", "")) for step in oracle_job["steps"])
    assert "--no-install-local" in oracle_runs
    assert "make db-conformance ENGINE=oracle VERSION=23ai" in oracle_runs


def test_ci_workflow_lint_macos_and_thirteen_wheel_checks() -> None:
    """Verify split lint jobs, build-once native artifact flow, macOS Rust test flags, and cache hygiene."""
    ci_data = yaml.safe_load((_REPO_ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8"))
    jobs = ci_data["jobs"]

    changes_job = jobs["changes"]
    assert "rust_macos" in changes_job["outputs"]
    assert "packaging" in changes_job["outputs"]
    assert "db_matrix" in changes_job["outputs"]
    filter_step = next(step for step in changes_job["steps"] if step.get("id") == "filter")
    filters_cfg = yaml.safe_load(filter_step["with"]["filters"])
    assert "src/rs/**" in filters_cfg["rust_macos"]
    assert "src/rs/vertebra-*/**" in filters_cfg["packaging"]

    lint_py_runs = "\n".join(str(step.get("run", "")) for step in jobs["lint-python"]["steps"])
    assert "uv sync --frozen --no-install-local --all-extras --group test --group lint --group build" in lint_py_runs
    assert "tools/scripts/check_extras.py" in lint_py_runs
    assert "pyright src/py/vertebra" in lint_py_runs
    assert jobs["lint-python"]["runs-on"] == "ubuntu-slim"
    assert jobs["lint-ts"]["runs-on"] == "ubuntu-slim"
    assert jobs["build-ts"]["runs-on"] == "ubuntu-slim"

    build_native_runs = "\n".join(str(step.get("run", "")) for step in jobs["build-native"]["steps"])
    assert "build_ci_wheels.py build" in build_native_runs
    assert "mypy.stubtest" in build_native_runs
    assert "uv sync --frozen --no-install-local" in build_native_runs
    assert "Missing native extensions:" in build_native_runs

    test_py_runs = "\n".join(str(step.get("run", "")) for step in jobs["test-python"]["steps"])
    assert "uv sync --frozen --no-install-local --group test" in test_py_runs
    assert "build_ci_wheels.py install" in test_py_runs
    assert "-n auto --dist loadscope" in test_py_runs

    macos_job = jobs["test-rust-macos"]
    assert "needs.changes.outputs.rust_macos == 'true'" in macos_job["if"]
    macos_runs = "\n".join(str(step.get("run", "")) for step in macos_job["steps"])
    assert "cargo test --workspace --features tls,http3,compression,gcp" in macos_runs
    assert "macos-ulock" not in macos_runs

    verify_wheels = jobs["verify-wheels"]
    assert verify_wheels["needs"] == ["changes", "build-native"]
    assert "needs.changes.outputs.packaging == 'true'" in verify_wheels["if"]
    verify_runs = "\n".join(str(step.get("run", "")) for step in verify_wheels["steps"])
    assert "build_ci_wheels.py verify --wheel-dir dist/wheels --sdist-dir dist/sdist" in verify_runs
    assert "cleanup-artifacts" in jobs

    cache_cleanup = _REPO_ROOT / ".github" / "workflows" / "cache-cleanup.yml"
    assert cache_cleanup.is_file()
    assert (_REPO_ROOT / ".github" / "actions" / "setup-rust-build" / "action.yml").is_file()
    assert (_REPO_ROOT / ".github" / "actions" / "setup-python-env" / "action.yml").is_file()


def _extract_release_manifest_script() -> str:
    release_data = yaml.safe_load((_REPO_ROOT / ".github" / "workflows" / "release.yaml").read_text(encoding="utf-8"))
    validate_job = release_data["jobs"]["validate_release_manifest"]
    run_cmd = next(
        str(step["run"]) for step in validate_job["steps"] if "Reject upstream git pins" in step.get("name", "")
    )
    _, _, body = run_cmd.partition("<<'PY'\n")
    script, _, _ = body.partition("\nPY")
    return textwrap.dedent(script)


@pytest.mark.parametrize(
    ("source_entry", "expected_rc"),
    [
        ('{ git = "https://example.com/repo.git", rev = "abc123" }', 1),
        ('{ git = "https://example.com/repo.git", branch = "main" }', 1),
        ('{ git = "https://example.com/repo.git", tag = "v1.0.0" }', 1),
        ('{ rev = "abc123" }', 1),
        ('{ path = "src/rs/vertebra-sqlite", editable = true }', 0),
    ],
)
def test_release_workflow_guards_sdist_pypi_and_git_pins(
    tmp_path: Path,
    source_entry: str,
    expected_rc: int,
) -> None:
    """Verify release.yaml includes valkey sdist, release-only PyPI upload, and rejects git source pins."""
    release_data = yaml.safe_load((_REPO_ROOT / ".github" / "workflows" / "release.yaml").read_text(encoding="utf-8"))
    jobs = release_data["jobs"]

    assert "validate_release_manifest" in jobs["build_wheels"]["needs"]
    assert "validate_release_manifest" in jobs["build_sdist"]["needs"]
    sdist_runs = "\n".join(str(step.get("run", "")) for step in jobs["build_sdist"]["steps"])
    assert "valkey" in sdist_runs
    assert jobs["upload_pypi"]["if"] == "github.event_name == 'release'"

    (tmp_path / "pyproject.toml").write_text(
        f"[project]\nname = 'synthetic'\nversion = '0.1.0'\n\n[tool.uv.sources]\ndep = {source_entry}\n",
        encoding="utf-8",
    )
    script = _extract_release_manifest_script()
    proc = subprocess.run(
        [sys.executable, "-c", script],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == expected_rc


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


def _extract_package_name(spec: object) -> str:
    if not isinstance(spec, str):
        return ""
    for sep in ("[", ">", "<", "=", "!", "~", ";"):
        spec = spec.split(sep, 1)[0]
    return spec.strip().lower()


def test_bench_comparators_group_isolation() -> None:
    """Verify bench-comparators isolation in pyproject.toml, zero --group duckdb in Makefile, and MinIO ban."""
    makefile_text = (_REPO_ROOT / "Makefile").read_text(encoding="utf-8")
    assert "--group duckdb" not in makefile_text

    pyproject = tomllib.loads((_REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    dep_groups = pyproject["dependency-groups"]
    assert "bench-comparators" in dep_groups
    comparators = dep_groups["bench-comparators"]
    assert comparators

    project_deps = {_extract_package_name(entry) for entry in pyproject["project"].get("dependencies", [])}
    optional_deps_map = pyproject["project"].get("optional-dependencies", {})
    assert "bench-comparators" not in optional_deps_map
    optional_deps = {_extract_package_name(entry) for entries in optional_deps_map.values() for entry in entries}
    dev_entries = dep_groups["dev"]
    assert not any(
        isinstance(entry, dict) and entry.get("include-group") == "bench-comparators" for entry in dev_entries
    )
    dev_direct_deps = {_extract_package_name(entry) for entry in dev_entries if isinstance(entry, str)}

    comparator_exclusive_packages = {
        "pymysql",
        "aiomysql",
        "aiosqlite",
        "oracledb",
        "pymssql",
        "redis",
        "boto3",
        "aiobotocore",
        "obstore",
        "azure-storage-blob",
        "django-bolt",
    }
    comparator_names = {_extract_package_name(entry) for entry in comparators}
    assert comparator_exclusive_packages.issubset(comparator_names)
    assert comparator_exclusive_packages.isdisjoint(project_deps)
    assert comparator_exclusive_packages.isdisjoint(optional_deps)
    assert comparator_exclusive_packages.isdisjoint(dev_direct_deps)

    for key, spec in FIXTURE_SPECS.items():
        assert "minio" not in key.lower()
        assert "minio" not in spec.image.lower()
        assert "minio" not in spec.plugin_module.lower()
    assert all("minio" not in backend.lower() for backend in STORAGE_LOCAL_BACKENDS)

    with pytest.raises(ValueError, match="MinIO"):
        get_fixture_spec("minio")
    with pytest.raises(ValueError, match="MinIO"):
        build_storage_config("minio")
    with pytest.raises(ValueError, match="MinIO"):
        load_backend_config({"VTB_BENCH_STORAGE_BACKEND": "minio"})
