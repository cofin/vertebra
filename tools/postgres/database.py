"""PostgreSQL container lifecycle management for integration tests."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tools.lib.container import ContainerRuntime


@dataclass(slots=True)
class DatabaseConfig:
    container_name: str = "vertebra-test-postgres"
    image: str = "postgres:16"
    hostname: str = "db"
    host_port: int | None = None
    container_port: int = 5432
    postgres_password: str = "vertebra"
    postgres_user: str = "vertebra"
    postgres_db: str = "vertebra"
    data_volume_name: str = "vertebra-test-postgres-data"
    health_timeout: int = 60
    health_interval: float = 1.0
    health_retries: int = 60


class PostgreSQLDatabase:
    def __init__(self, runtime: ContainerRuntime, config: DatabaseConfig | None = None) -> None:
        self.runtime = runtime
        self.config = config or DatabaseConfig()

    def start(self, *, pull: bool = False, recreate: bool = False) -> None:
        if self.runtime.container_running(self.config.container_name):
            if not recreate:
                return
            self.remove(force=True)

        if self.runtime.container_exists(self.config.container_name):
            if recreate:
                self.remove(force=True)
            else:
                self.runtime.run_command(["start", self.config.container_name], check=False)
                if self.config.host_port is None:
                    self.config.host_port = self._get_allocated_port()
                self._wait_for_ready()
                return

        if pull:
            self.runtime.run_command(["pull", self.config.image], check=False)

        if not self.runtime.volume_exists(self.config.data_volume_name):
            self.runtime.run_command(["volume", "create", self.config.data_volume_name], check=False)

        run_args = [
            "run",
            "-d",
            "--name",
            self.config.container_name,
            "--hostname",
            self.config.hostname,
            "-e",
            f"POSTGRES_PASSWORD={self.config.postgres_password}",
            "-e",
            f"POSTGRES_USER={self.config.postgres_user}",
            "-e",
            f"POSTGRES_DB={self.config.postgres_db}",
            "-v",
            f"{self.config.data_volume_name}:/var/lib/postgresql/data",
        ]
        if self.config.host_port is None:
            run_args += ["-p", str(self.config.container_port)]
        else:
            run_args += ["-p", f"{self.config.host_port}:{self.config.container_port}"]
        run_args.append(self.config.image)

        self.runtime.run_command(run_args, check=True)
        if self.config.host_port is None:
            self.config.host_port = self._get_allocated_port()
        self._wait_for_ready()

    def stop(self) -> None:
        self.runtime.run_command(["stop", self.config.container_name], check=False)

    def remove(self, *, force: bool = False) -> None:
        args = ["rm"]
        if force:
            args.append("-f")
        args.append(self.config.container_name)
        self.runtime.run_command(args, check=False)

    def connection_url(self) -> str:
        host_port = self.config.host_port or self._get_allocated_port()
        return (
            f"postgresql://{self.config.postgres_user}:{self.config.postgres_password}"
            f"@127.0.0.1:{host_port}/{self.config.postgres_db}"
        )

    def _get_allocated_port(self) -> int:
        _, stdout, _ = self.runtime.run_command(
            ["port", self.config.container_name, f"{self.config.container_port}/tcp"], check=False
        )
        mapping = stdout.strip()
        if not mapping:
            return self.config.container_port
        return int(mapping.split(":")[-1])

    def _wait_for_ready(self) -> None:
        for _ in range(self.config.health_retries):
            returncode, _, _ = self.runtime.run_command(
                [
                    "exec",
                    self.config.container_name,
                    "pg_isready",
                    "-U",
                    self.config.postgres_user,
                    "-d",
                    self.config.postgres_db,
                ],
                check=False,
            )
            if returncode == 0:
                return
            time.sleep(self.config.health_interval)
        msg = f"Postgres container {self.config.container_name} failed readiness checks"
        raise RuntimeError(msg)


__all__ = ("DatabaseConfig", "PostgreSQLDatabase")
