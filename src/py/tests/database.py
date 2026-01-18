"""Integration-test database containers (Docker/Podman)."""

from __future__ import annotations

import contextlib
import os
import subprocess
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any

import filelock
import pytest

from tools.lib.container import ContainerRuntime
from tools.mysql.database import DatabaseConfig as MySQLDatabaseConfig
from tools.mysql.database import MySQLDatabase
from tools.postgres.database import DatabaseConfig as PostgresDatabaseConfig
from tools.postgres.database import PostgreSQLDatabase
from tools.redis.database import DatabaseConfig as RedisDatabaseConfig
from tools.redis.database import RedisDatabase

if TYPE_CHECKING:
    from collections.abc import Generator


def get_xdist_worker_id() -> str:
    return os.environ.get("PYTEST_XDIST_WORKER", "master")


def is_xdist_master() -> bool:
    return get_xdist_worker_id() == "master"


class SessionContainerManager:
    def __init__(self) -> None:
        self.worker_id = get_xdist_worker_id()
        self.runtime = ContainerRuntime()
        self.lock_dir = Path(tempfile.gettempdir()) / "pytest-vertebra-locks"
        self.lock_dir.mkdir(exist_ok=True, parents=True)

    def get_lock_path(self, name: str) -> Path:
        return self.lock_dir / f"{name}.lock"

    def ensure_service(self, db_cls: type[PostgreSQLDatabase | MySQLDatabase | RedisDatabase], config: Any) -> Any:
        container_name = config.container_name
        lock_path = self.get_lock_path(container_name)
        lock = filelock.FileLock(str(lock_path))

        with lock:
            if self.runtime.container_running(container_name):
                if config.host_port is None:
                    temp_db = db_cls(self.runtime, config)
                    config.host_port = temp_db._get_allocated_port()
            else:
                if self.runtime.container_exists(container_name):
                    with contextlib.suppress(Exception):
                        self.runtime.run_command(["rm", "-f", container_name], check=False)
                db = db_cls(self.runtime, config)
                db.start(pull=False, recreate=False)

        return db_cls(self.runtime, config)


@pytest.fixture(scope="session")
def container_manager() -> SessionContainerManager:
    manager = SessionContainerManager()
    if not manager.runtime.is_available():
        pytest.skip("Docker or Podman is required for integration tests.")
    return manager


@pytest.fixture(scope="session")
def postgres_service(container_manager: SessionContainerManager) -> Generator[PostgreSQLDatabase, None, None]:
    config = PostgresDatabaseConfig(
        image=os.getenv("VERTEBRA_TEST_POSTGRES_IMAGE", "postgres:16"),
        container_name=os.getenv("VERTEBRA_TEST_POSTGRES_CONTAINER", "vertebra-test-postgres"),
        data_volume_name=os.getenv("VERTEBRA_TEST_POSTGRES_VOLUME", "vertebra-test-postgres-data"),
        host_port=None,
        postgres_db=os.getenv("VERTEBRA_TEST_POSTGRES_DB", "vertebra"),
        postgres_user=os.getenv("VERTEBRA_TEST_POSTGRES_USER", "vertebra"),
        postgres_password=os.getenv("VERTEBRA_TEST_POSTGRES_PASSWORD", "vertebra"),
    )
    db = container_manager.ensure_service(PostgreSQLDatabase, config)
    yield db


@pytest.fixture(scope="session")
def mysql_service(container_manager: SessionContainerManager) -> Generator[MySQLDatabase, None, None]:
    config = MySQLDatabaseConfig(
        image=os.getenv("VERTEBRA_TEST_MYSQL_IMAGE", "mysql:8"),
        container_name=os.getenv("VERTEBRA_TEST_MYSQL_CONTAINER", "vertebra-test-mysql"),
        data_volume_name=os.getenv("VERTEBRA_TEST_MYSQL_VOLUME", "vertebra-test-mysql-data"),
        host_port=None,
        mysql_db=os.getenv("VERTEBRA_TEST_MYSQL_DB", "vertebra"),
        mysql_user=os.getenv("VERTEBRA_TEST_MYSQL_USER", "vertebra"),
        mysql_password=os.getenv("VERTEBRA_TEST_MYSQL_PASSWORD", "vertebra"),
        root_password=os.getenv("VERTEBRA_TEST_MYSQL_ROOT_PASSWORD", "vertebra"),
    )
    db = container_manager.ensure_service(MySQLDatabase, config)
    yield db


@pytest.fixture(scope="session")
def redis_service(container_manager: SessionContainerManager) -> Generator[RedisDatabase, None, None]:
    config = RedisDatabaseConfig(
        image=os.getenv("VERTEBRA_TEST_REDIS_IMAGE", "redis:7"),
        container_name=os.getenv("VERTEBRA_TEST_REDIS_CONTAINER", "vertebra-test-redis"),
        host_port=None,
    )
    db = container_manager.ensure_service(RedisDatabase, config)
    yield db


@pytest.fixture(scope="session")
def postgres_url(postgres_service: PostgreSQLDatabase) -> str:
    return postgres_service.connection_url()


@pytest.fixture(scope="session")
def mysql_url(mysql_service: MySQLDatabase) -> str:
    return mysql_service.connection_url()


@pytest.fixture(scope="session")
def redis_url(redis_service: RedisDatabase) -> str:
    return redis_service.connection_url()


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    if not is_xdist_master():
        return

    runtime = ContainerRuntime()
    if not runtime.is_available():
        return

    cmd = runtime.get_runtime_command()
    try:
        containers = runtime.list_containers(all=True)
        test_containers = [c for c in containers if c.startswith("vertebra-test-")]
        if test_containers:
            subprocess.run([cmd, "rm", "-f", *test_containers], capture_output=True, check=False)
    except Exception:  # noqa: BLE001
        return
