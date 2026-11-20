"""Docker container fixture specifications and storage backend config builders for benchmarks."""

from __future__ import annotations

import contextlib
import http.client
import shutil
import socket
import subprocess
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

import msgspec

from vertebra.utils.module_loader import import_optional

if TYPE_CHECKING:
    from types import TracebackType

_MINIO_FORBIDDEN_MESSAGE = "MinIO is forbidden; use 'rustfs' for S3 or 'floci_aws' for AWS emulation"
_AZURITE_EMULATOR_ACCOUNT = "devstoreaccount1"
_AZURITE_EMULATOR_KEY = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="

STORAGE_LOCAL_BACKENDS: tuple[str, str] = ("local", "memory")
EMULATOR_PROJECT_ID = "emulator-test-project"
SPANNER_EMULATOR_INSTANCE = "emulator-test-instance"
SPANNER_EMULATOR_DATABASE = "vertebra-bench"
BIGQUERY_EMULATOR_DATASET = "test-dataset"


class ContainerFixtureSpec(msgspec.Struct, frozen=True, gc=False):
    """Immutable specification for a tmpfs-backed local Docker benchmark fixture."""

    name: str
    category: Literal["sql", "cache", "storage"]
    plugin_module: str
    image: str
    container_port: int
    tmpfs_mounts: tuple[str, ...] = ()
    mem_limit: str | None = None
    shm_size: str | None = None
    env: tuple[tuple[str, str], ...] = ()
    command: tuple[str, ...] = ()
    health_path: str | None = None
    extra_container_ports: tuple[int, ...] = ()


FIXTURE_SPECS: dict[str, ContainerFixtureSpec] = {
    "postgres": ContainerFixtureSpec(
        name="postgres",
        category="sql",
        plugin_module="pytest_databases.docker.postgres",
        image="postgres:17-alpine",
        container_port=5432,
        tmpfs_mounts=("/var/lib/postgresql/data:rw,noexec,nosuid",),
        shm_size="128m",
        env=(("POSTGRES_PASSWORD", "bench"), ("POSTGRES_DB", "bench")),
    ),
    "mysql": ContainerFixtureSpec(
        name="mysql",
        category="sql",
        plugin_module="pytest_databases.docker.mysql",
        image="mysql:8.4",
        container_port=3306,
        tmpfs_mounts=("/var/lib/mysql:rw,noexec,nosuid",),
        env=(("MYSQL_ROOT_PASSWORD", "bench"), ("MYSQL_DATABASE", "bench")),
    ),
    "mariadb": ContainerFixtureSpec(
        name="mariadb",
        category="sql",
        plugin_module="pytest_databases.docker.mariadb",
        image="mariadb:11.4",
        container_port=3306,
        tmpfs_mounts=("/var/lib/mysql:rw,noexec,nosuid",),
        env=(("MARIADB_ROOT_PASSWORD", "bench"), ("MARIADB_DATABASE", "bench")),
    ),
    "oracle": ContainerFixtureSpec(
        name="oracle",
        category="sql",
        plugin_module="pytest_databases.docker.oracle",
        image="gvenzl/oracle-free:23-slim-faststart",
        container_port=1521,
        mem_limit="4g",
        env=(("ORACLE_PASSWORD", "bench"),),
    ),
    "mssql": ContainerFixtureSpec(
        name="mssql",
        category="sql",
        plugin_module="pytest_databases.docker.mssql",
        image="mcr.microsoft.com/mssql/server:2022-latest",
        container_port=1433,
        mem_limit="3g",
        env=(("ACCEPT_EULA", "Y"), ("MSSQL_SA_PASSWORD", "BenchPass123!")),
    ),
    "spanner": ContainerFixtureSpec(
        name="spanner",
        category="sql",
        plugin_module="pytest_databases.docker.spanner",
        image="gcr.io/cloud-spanner-emulator/emulator:latest",
        container_port=9010,
        extra_container_ports=(9020,),
    ),
    "bigquery": ContainerFixtureSpec(
        name="bigquery",
        category="sql",
        plugin_module="pytest_databases.docker.bigquery",
        image="ghcr.io/goccy/bigquery-emulator:0.8.1",
        container_port=9050,
        env=(("PROJECT_ID", EMULATOR_PROJECT_ID), ("DATASET_NAME", BIGQUERY_EMULATOR_DATASET)),
        command=(f"--project={EMULATOR_PROJECT_ID}", f"--dataset={BIGQUERY_EMULATOR_DATASET}"),
    ),
    "redis": ContainerFixtureSpec(
        name="redis",
        category="cache",
        plugin_module="pytest_databases.docker.redis",
        image="redis:7-alpine",
        container_port=6379,
        command=("redis-server", "--save", "", "--appendonly", "no"),
    ),
    "valkey": ContainerFixtureSpec(
        name="valkey",
        category="cache",
        plugin_module="pytest_databases.docker.valkey",
        image="valkey/valkey:8-alpine",
        container_port=6379,
        command=("valkey-server", "--save", "", "--appendonly", "no"),
    ),
    "rustfs": ContainerFixtureSpec(
        name="rustfs",
        category="storage",
        plugin_module="pytest_databases.docker.rustfs",
        image="rustfs/rustfs:latest",
        container_port=9000,
        tmpfs_mounts=("/data:rw,noexec,nosuid,mode=1777",),
        env=(("RUSTFS_ROOT_USER", "rustfsadmin"), ("RUSTFS_ROOT_PASSWORD", "rustfsadmin")),
        health_path="/health",
    ),
    "floci_aws": ContainerFixtureSpec(
        name="floci_aws",
        category="storage",
        plugin_module="pytest_databases.docker.floci",
        image="floci/floci:2.4.0",
        container_port=4566,
        health_path="/_floci/health",
    ),
    "floci_gcp": ContainerFixtureSpec(
        name="floci_gcp",
        category="storage",
        plugin_module="pytest_databases.docker.floci",
        image="floci/floci-gcp:0.12.0",
        container_port=4588,
        health_path="/_floci/health",
    ),
    "floci_azure": ContainerFixtureSpec(
        name="floci_azure",
        category="storage",
        plugin_module="pytest_databases.docker.floci",
        image="floci/floci-az:0.16.0",
        container_port=4577,
        health_path="/_floci/health",
    ),
    "floci_oci": ContainerFixtureSpec(
        name="floci_oci",
        category="storage",
        plugin_module="pytest_databases.docker.floci",
        image="floci/floci-oci:0.6.0",
        container_port=4599,
        health_path="/_floci/health",
    ),
    "azurite": ContainerFixtureSpec(
        name="azurite",
        category="storage",
        plugin_module="pytest_databases.docker.azure_blob",
        image="mcr.microsoft.com/azure-storage/azurite:latest",
        container_port=10000,
        command=("azurite-blob", "--blobHost", "0.0.0.0", "--inMemoryPersistence"),
    ),
}


def get_fixture_spec(name: str) -> ContainerFixtureSpec:
    """Return the container fixture specification for a service name.

    Args:
        name: Service name (case-insensitive, surrounding whitespace stripped).

    Returns:
        The matching ``ContainerFixtureSpec``.

    Raises:
        ValueError: If ``name`` references MinIO or is not a registered fixture.
    """
    normalized = name.strip().lower()
    if "minio" in normalized:
        raise ValueError(_MINIO_FORBIDDEN_MESSAGE)
    spec = FIXTURE_SPECS.get(normalized)
    if spec is None:
        valid = ", ".join(sorted(FIXTURE_SPECS))
        raise ValueError(f"Unknown fixture {name!r}; expected one of: {valid}")
    return spec


def build_docker_run_command(
    spec: ContainerFixtureSpec,
    *,
    host_port: int,
    container_name: str,
    extra_env: dict[str, str] | None = None,
    extra_host_ports: dict[int, int] | None = None,
) -> list[str]:
    """Build a ``docker run`` argument vector for a container fixture specification.

    Args:
        spec: Container fixture specification.
        host_port: Loopback host port mapped to ``spec.container_port``.
        container_name: Docker container name.
        extra_env: Optional environment variable overrides merged on top of ``spec.env``.
        extra_host_ports: Loopback host ports keyed by each entry of ``spec.extra_container_ports``.

    Returns:
        List of command-line arguments suitable for ``subprocess.run``.

    Raises:
        ValueError: If ``spec`` references MinIO in its name, image, or plugin module.
    """
    if "minio" in spec.name.lower() or "minio" in spec.image.lower() or "minio" in spec.plugin_module.lower():
        raise ValueError(_MINIO_FORBIDDEN_MESSAGE)

    command: list[str] = [
        "docker",
        "run",
        "-d",
        "--rm",
        "--name",
        container_name,
        "-p",
        f"127.0.0.1:{host_port}:{spec.container_port}",
    ]
    for container_port, extra_host_port in (extra_host_ports or {}).items():
        command.extend(["-p", f"127.0.0.1:{extra_host_port}:{container_port}"])
    for mount in spec.tmpfs_mounts:
        command.extend(["--tmpfs", mount])
    if spec.mem_limit is not None:
        command.extend(["--memory", spec.mem_limit])
    if spec.shm_size is not None:
        command.extend(["--shm-size", spec.shm_size])

    merged_env = dict(spec.env)
    if extra_env:
        merged_env.update(extra_env)
    for key, value in merged_env.items():
        command.extend(["-e", f"{key}={value}"])

    command.append(spec.image)
    if spec.command:
        command.extend(spec.command)
    return command


def build_storage_config(
    backend: str,
    *,
    endpoint: str | None = None,
    bucket: str = "vertebra-bench",
    root_dir: str | Path | None = None,
) -> dict[str, Any]:
    """Return normalized storage config kwargs for a cloud emulator or local backend.

    Matches the constructor fields used in ``src/py/tests/integration/storage/conftest.py``
    without importing ``vertebra._storage`` at module import time.

    Args:
        backend: Storage backend identifier (``rustfs``, ``floci_aws``, ``floci_s3``,
            ``floci_r2``, ``r2``, ``floci_gcp``, ``floci_azure``, ``floci_oci``,
            ``azurite``, ``local``, or ``memory``).
        endpoint: Optional override URL for emulator-backed backends.
        bucket: Bucket, container, or memory namespace name.
        root_dir: Filesystem directory required when ``backend`` is ``local``.

    Returns:
        Dictionary of keyword arguments for the corresponding storage config class.

    Raises:
        ValueError: If ``backend`` references MinIO, is unknown, or omits ``root_dir``
            for the ``local`` backend.
    """
    normalized = backend.strip().lower()
    if "minio" in normalized:
        raise ValueError(_MINIO_FORBIDDEN_MESSAGE)

    if normalized == "rustfs":
        return {
            "bucket": bucket,
            "region": "us-east-1",
            "endpoint": endpoint or "http://127.0.0.1:9000",
            "access_key_id": "rustfsadmin",
            "secret_access_key": "rustfsadmin",
            "allow_http": True,
        }
    if normalized in {"floci_aws", "floci_s3"}:
        return {
            "bucket": bucket,
            "region": "us-east-1",
            "endpoint": endpoint or "http://127.0.0.1:4566",
            "access_key_id": "test",
            "secret_access_key": "test",
            "allow_http": True,
        }
    if normalized in {"floci_r2", "r2"}:
        return {
            "bucket": bucket,
            "account_id": "testaccount",
            "access_key_id": "test",
            "secret_access_key": "test",
            "endpoint": endpoint or "http://127.0.0.1:4566",
            "allow_http": True,
        }
    if normalized == "floci_gcp":
        return {
            "bucket": bucket,
            "service_account_email": "test@vertebra.iam.gserviceaccount.com",
            "iam_endpoint": endpoint or "http://127.0.0.1:4588",
            "access_token": "floci-test-token",
        }
    if normalized == "floci_azure":
        return {
            "container": bucket,
            "account": _AZURITE_EMULATOR_ACCOUNT,
            "access_key": _AZURITE_EMULATOR_KEY,
            "endpoint": endpoint or f"http://127.0.0.1:4577/{_AZURITE_EMULATOR_ACCOUNT}",
            "use_emulator": True,
        }
    if normalized == "floci_oci":
        return {
            "bucket": bucket,
            "namespace": "testns",
            "region": "us-ashburn-1",
            "endpoint": endpoint or "http://127.0.0.1:4599",
        }
    if normalized == "azurite":
        return {
            "container": bucket,
            "account": _AZURITE_EMULATOR_ACCOUNT,
            "access_key": _AZURITE_EMULATOR_KEY,
            "endpoint": endpoint or f"http://127.0.0.1:10000/{_AZURITE_EMULATOR_ACCOUNT}",
            "use_emulator": True,
        }
    if normalized == "local":
        if root_dir is None:
            raise ValueError("root_dir is required for local storage backend")
        return {"root": str(Path(root_dir))}
    if normalized == "memory":
        return {"namespace": bucket}

    valid = ", ".join(
        sorted(
            {
                "rustfs",
                "floci_aws",
                "floci_s3",
                "floci_r2",
                "r2",
                "floci_gcp",
                "floci_azure",
                "floci_oci",
                "azurite",
                *STORAGE_LOCAL_BACKENDS,
            }
        )
    )
    raise ValueError(f"Unknown storage backend {backend!r}; expected one of: {valid}")


def _allocate_ephemeral_port() -> int:
    """Bind an ephemeral loopback port and return its port number."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _wait_for_tcp_port(host: str, port: int, timeout: float) -> None:
    """Block until a TCP port accepts connections or raise ``TimeoutError``."""
    deadline = time.monotonic() + timeout
    while True:
        try:
            with socket.create_connection((host, port), timeout=min(0.5, max(0.05, deadline - time.monotonic()))):
                return
        except OSError:
            if time.monotonic() >= deadline:
                raise TimeoutError(f"Timed out waiting {timeout:.1f}s for {host}:{port}") from None
            time.sleep(0.1)


def _wait_for_http_health(url: str, timeout: float) -> None:
    """Block until ``url`` returns HTTP 200 bypassing ambient HTTP proxies."""
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    deadline = time.monotonic() + timeout
    while True:
        remaining = max(0.05, deadline - time.monotonic())
        try:
            with opener.open(url, timeout=min(1.0, remaining)) as resp:
                if resp.status == 200:
                    return
        except (OSError, urllib.error.URLError, ValueError):
            pass
        if time.monotonic() >= deadline:
            raise TimeoutError(f"Timed out waiting {timeout:.1f}s for HTTP 200 from {url}")
        time.sleep(0.1)


def _wait_for_resp_pong(host: str, port: int, timeout: float) -> None:
    """Block until a Redis/Valkey server responds ``+PONG`` to ``PING``."""
    deadline = time.monotonic() + timeout
    while True:
        remaining = max(0.05, deadline - time.monotonic())
        try:
            with socket.create_connection((host, port), timeout=min(0.5, remaining)) as sock:
                sock.settimeout(min(0.5, remaining))
                sock.sendall(b"*1\r\n$4\r\nPING\r\n")
                if sock.recv(64).startswith(b"+PONG"):
                    return
        except OSError:
            pass
        if time.monotonic() >= deadline:
            raise TimeoutError(f"Timed out waiting {timeout:.1f}s for RESP PONG on {host}:{port}")
        time.sleep(0.1)


def _ensure_rustfs_bucket(endpoint_url: str, bucket: str = "vertebra-bench") -> None:
    """Provision the default RustFS S3 bucket when ``pytest_databases`` helpers are available."""
    helpers = import_optional("pytest_databases.helpers")
    create_s3_bucket = getattr(helpers, "create_s3_bucket", None) if helpers is not None else None
    if callable(create_s3_bucket):
        create_s3_bucket(endpoint_url, bucket, access_key="rustfsadmin", secret_key="rustfsadmin")


def _bootstrap_spanner_emulator(rest_port: int) -> None:
    """Create the benchmark instance and database on a Spanner emulator through its REST port."""
    spanner_mod = import_optional("pytest_databases.docker.spanner")
    bootstrap = getattr(spanner_mod, "bootstrap_spanner_emulator", None) if spanner_mod is not None else None
    if not callable(bootstrap):
        raise RuntimeError("pytest-databases is required to bootstrap the Spanner emulator fixture")
    bootstrap(
        "127.0.0.1",
        rest_port,
        project=EMULATOR_PROJECT_ID,
        instance_name=SPANNER_EMULATOR_INSTANCE,
        database_name=SPANNER_EMULATOR_DATABASE,
    )


def _wait_for_bigquery_emulator(port: int, timeout: float) -> None:
    """Block until the BigQuery emulator answers a ``SELECT 1`` query."""
    bigquery_mod = import_optional("pytest_databases.docker.bigquery")
    query = getattr(bigquery_mod, "query_bigquery_emulator", None) if bigquery_mod is not None else None
    if not callable(query):
        raise RuntimeError("pytest-databases is required to probe the BigQuery emulator fixture")
    deadline = time.monotonic() + timeout
    while True:
        try:
            query("127.0.0.1", port, EMULATOR_PROJECT_ID, "SELECT 1", timeout=2.0)
        except (RuntimeError, OSError, ValueError, http.client.HTTPException):
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"Timed out waiting {timeout:.1f}s for the BigQuery emulator on port {port}"
                ) from None
            time.sleep(0.2)
        else:
            return


class FixtureLauncher:
    """Context manager that launches and cleans up a local Docker benchmark fixture."""

    def __init__(
        self,
        spec: ContainerFixtureSpec,
        *,
        host_port: int | None = None,
        container_name: str | None = None,
        timeout: float = 30.0,
        extra_env: dict[str, str] | None = None,
    ) -> None:
        self.spec = spec
        self.host_port: int | None = host_port
        self.container_name = container_name or f"vertebra-bench-{spec.name}-{uuid.uuid4().hex[:8]}"
        self.timeout = timeout
        self.extra_env = extra_env
        self.extra_host_ports: dict[int, int] = {}
        self._started = False

    @property
    def endpoint_url(self) -> str:
        """Return the loopback HTTP endpoint URL for the launched container."""
        if self.host_port is None:
            raise RuntimeError("FixtureLauncher has not been started and has no assigned host_port")
        return f"http://127.0.0.1:{self.host_port}"

    def start(self) -> FixtureLauncher:
        """Start the Docker container and wait until its mapped TCP port is ready."""
        if shutil.which("docker") is None:
            raise RuntimeError("docker executable not found on PATH")

        if self.host_port is None:
            self.host_port = _allocate_ephemeral_port()

        self.extra_host_ports = {port: _allocate_ephemeral_port() for port in self.spec.extra_container_ports}
        command = build_docker_run_command(
            self.spec,
            host_port=self.host_port,
            container_name=self.container_name,
            extra_env=self.extra_env,
            extra_host_ports=self.extra_host_ports,
        )
        subprocess.run(command, check=True, capture_output=True, text=True, timeout=self.timeout)
        self._started = True
        try:
            _wait_for_tcp_port("127.0.0.1", self.host_port, self.timeout)
            if self.spec.health_path is not None:
                _wait_for_http_health(f"{self.endpoint_url}{self.spec.health_path}", self.timeout)
            elif self.spec.category == "cache":
                _wait_for_resp_pong("127.0.0.1", self.host_port, self.timeout)
            if self.spec.name == "rustfs":
                _ensure_rustfs_bucket(self.endpoint_url)
            elif self.spec.name == "spanner":
                rest_port = self.extra_host_ports[9020]
                _wait_for_tcp_port("127.0.0.1", rest_port, self.timeout)
                _bootstrap_spanner_emulator(rest_port)
            elif self.spec.name == "bigquery":
                _wait_for_bigquery_emulator(self.host_port, self.timeout)
        except Exception:
            self.stop()
            raise
        return self

    def stop(self) -> None:
        """Force-remove the container, suppressing errors if it is already absent."""
        with contextlib.suppress(OSError, subprocess.SubprocessError):
            subprocess.run(
                ["docker", "rm", "-f", self.container_name],
                check=False,
                capture_output=True,
                text=True,
                timeout=15.0,
            )
        self._started = False

    def __enter__(self) -> FixtureLauncher:
        return self.start()

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.stop()
