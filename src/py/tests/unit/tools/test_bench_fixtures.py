"""Unit tests for benchmark comparator dependency isolation and Docker fixture builders."""

from __future__ import annotations

import shutil
import tomllib
from pathlib import Path
from typing import Any

import pytest
from tools.benchmarks.fixtures import (
    FIXTURE_SPECS,
    STORAGE_LOCAL_BACKENDS,
    ContainerFixtureSpec,
    FixtureLauncher,
    build_docker_run_command,
    build_storage_config,
    get_fixture_spec,
)

_REPO_ROOT = Path(__file__).resolve().parents[5]


def _dist_name(requirement: str) -> str:
    """Return the normalized distribution name from a PEP 508 requirement string."""
    return requirement.split(";")[0].split("[")[0].split(">")[0].split("<")[0].split("=")[0].strip().lower()


def test_pyproject_bench_comparators_group_isolated() -> None:
    """Verify bench-comparators stays isolated from dev/test and Makefile has no --group duckdb."""
    pyproject = tomllib.loads((_REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    dep_groups = pyproject["dependency-groups"]
    comparators: list[str] = dep_groups["bench-comparators"]

    expected_entries = {
        "asyncpg>=0.30; python_version < '3.14'",
        "psycopg[binary]>=3.2,<3.4; python_version < '3.14'",
        "pymysql>=1.1",
        "aiomysql>=0.2; python_version < '3.14'",
        "aiosqlite>=0.20",
        "oracledb>=2.5; python_version < '3.14'",
        "pymssql>=2.3; python_version < '3.14'",
        "redis[hiredis]>=5.2; python_version < '3.14'",
        "boto3>=1.35",
        "aiobotocore>=2.15; python_version < '3.14'",
        "obstore>=0.5; python_version < '3.14'",
        "azure-storage-blob>=12.23",
        "duckdb>=1.0; python_version < '3.14'",
        "django-bolt>=0.1; python_version < '3.14'",
        "litestar>=2.14",
        "fastapi>=0.115",
        "django>=5.1",
        "flask>=3.1",
    }
    assert expected_entries.issubset(set(comparators))

    comparator_only_dists = {
        "asyncpg",
        "psycopg",
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
        "duckdb",
        "django-bolt",
    }
    dev_entries = [entry for entry in dep_groups["dev"] if isinstance(entry, str)]
    test_dists = {_dist_name(entry) for entry in dep_groups["test"] if isinstance(entry, str)}
    dev_dists = {_dist_name(entry) for entry in dev_entries}
    assert comparator_only_dists.isdisjoint(test_dists)
    assert comparator_only_dists.isdisjoint(dev_dists)
    assert not any(
        isinstance(entry, dict) and entry.get("include-group") == "bench-comparators" for entry in dep_groups["dev"]
    )

    makefile_text = (_REPO_ROOT / "Makefile").read_text(encoding="utf-8")
    assert "--group duckdb" not in makefile_text


def test_fixture_specs_forbid_minio_and_include_rustfs_floci_azurite() -> None:
    """Verify zero MinIO references across FIXTURE_SPECS and expected storage plugin modules."""
    for key, spec in FIXTURE_SPECS.items():
        assert "minio" not in key.lower()
        assert "minio" not in spec.name.lower()
        assert "minio" not in spec.image.lower()
        assert "minio" not in spec.plugin_module.lower()

    with pytest.raises(ValueError, match="MinIO is forbidden"):
        get_fixture_spec("minio")

    with pytest.raises(ValueError, match="MinIO is forbidden"):
        build_storage_config("minio")

    minio_spec = ContainerFixtureSpec(
        name="minio",
        category="storage",
        plugin_module="pytest_databases.docker.minio",
        image="minio/minio:latest",
        container_port=9000,
    )
    with pytest.raises(ValueError, match="MinIO is forbidden"):
        build_docker_run_command(minio_spec, host_port=19000, container_name="forbidden")

    expected_storage_plugins = {
        "rustfs": "pytest_databases.docker.rustfs",
        "floci_aws": "pytest_databases.docker.floci",
        "floci_gcp": "pytest_databases.docker.floci",
        "floci_azure": "pytest_databases.docker.floci",
        "floci_oci": "pytest_databases.docker.floci",
        "azurite": "pytest_databases.docker.azure_blob",
    }
    for name, expected_plugin in expected_storage_plugins.items():
        spec = get_fixture_spec(name)
        assert spec.plugin_module == expected_plugin
        assert spec.category == "storage"

    assert STORAGE_LOCAL_BACKENDS == ("local", "memory")


@pytest.mark.parametrize(
    ("fixture_name", "expected_fragments"),
    [
        ("postgres", ("--tmpfs", "/var/lib/postgresql/data:rw,noexec,nosuid", "--shm-size", "128m")),
        ("mysql", ("--tmpfs", "/var/lib/mysql:rw,noexec,nosuid")),
        ("mariadb", ("--tmpfs", "/var/lib/mysql:rw,noexec,nosuid")),
        ("oracle", ("--memory", "1g")),
        ("mssql", ("--memory", "1g")),
        ("rustfs", ("--tmpfs", "/data:rw,noexec,nosuid")),
        ("azurite", ("--inMemoryPersistence",)),
    ],
)
def test_docker_run_command_emits_tmpfs_memory_and_shm_size(
    fixture_name: str,
    expected_fragments: tuple[str, ...],
) -> None:
    """Verify build_docker_run_command emits tmpfs, memory, shm-size, and in-memory flags."""
    spec = get_fixture_spec(fixture_name)
    cmd = build_docker_run_command(
        spec,
        host_port=25000,
        container_name=f"bench-{fixture_name}",
        extra_env={"BENCH_EXTRA": "1"},
    )
    assert cmd[:6] == ["docker", "run", "-d", "--rm", "--name", f"bench-{fixture_name}"]
    assert f"127.0.0.1:25000:{spec.container_port}" in cmd
    assert "BENCH_EXTRA=1" in cmd
    for fragment in expected_fragments:
        assert fragment in cmd


@pytest.mark.parametrize(
    ("backend", "expected_subset"),
    [
        ("rustfs", {"bucket": "vertebra-bench", "region": "us-east-1", "allow_http": True}),
        ("floci_aws", {"bucket": "vertebra-bench", "region": "us-east-1", "allow_http": True}),
        ("floci_s3", {"bucket": "vertebra-bench", "region": "us-east-1", "allow_http": True}),
        ("floci_r2", {"bucket": "vertebra-bench", "account_id": "testaccount", "allow_http": True}),
        ("r2", {"bucket": "vertebra-bench", "account_id": "testaccount", "allow_http": True}),
        ("floci_gcp", {"bucket": "vertebra-bench", "service_account_email": "test@vertebra.iam.gserviceaccount.com"}),
        ("floci_azure", {"container": "vertebra-bench", "account": "devstoreaccount1", "use_emulator": True}),
        ("floci_oci", {"bucket": "vertebra-bench", "namespace": "testns", "region": "us-ashburn-1"}),
        ("azurite", {"container": "vertebra-bench", "account": "devstoreaccount1", "use_emulator": True}),
        ("memory", {"namespace": "vertebra-bench"}),
    ],
)
def test_build_storage_config_covers_all_cloud_and_local_backends(
    backend: str,
    expected_subset: dict[str, Any],
    tmp_path: Path,
) -> None:
    """Verify build_storage_config returns normalized kwargs for all cloud and local backends."""
    config = build_storage_config(backend)
    for key, value in expected_subset.items():
        assert config[key] == value

    local_cfg = build_storage_config("local", root_dir=tmp_path)
    assert local_cfg == {"root": str(tmp_path)}

    with pytest.raises(ValueError, match="root_dir is required for local storage backend"):
        build_storage_config("local", root_dir=None)


def test_fixture_launcher_fails_loud_without_docker(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify FixtureLauncher.start() raises RuntimeError when docker is absent from PATH."""
    monkeypatch.setattr(shutil, "which", lambda _cmd: None)
    launcher = FixtureLauncher(get_fixture_spec("rustfs"))
    with pytest.raises(RuntimeError, match="docker executable not found on PATH"):
        launcher.start()
