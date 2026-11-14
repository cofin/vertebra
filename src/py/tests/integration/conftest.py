"""Clientless pytest-databases Docker harnesses for database, cache, and storage integration tests."""

from __future__ import annotations

import os
import socket
import struct
from collections.abc import Generator, Iterator, Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

from tests.integration.conformance_gate import ConformanceGate, ServiceVersionMatrix
from vertebra.db.adapters.mssql import MSSQLConfig
from vertebra.db.adapters.mysql import MySQLConfig, SyncMySQLDriver
from vertebra.db.adapters.oracle import OracleConfig
from vertebra.db.adapters.postgres import PoolerMode, PostgreSQLConfig, SyncPostgreSQLDriver
from vertebra.db.adapters.redis import RedisConfig, SyncRedisDriver
from vertebra.db.adapters.valkey import SyncValkeyDriver, ValkeyConfig
from vertebra.extensions.sqlspec import OracleAsyncConfig, PostgreSQLAsyncConfig
from vertebra.typing import ImproperConfigurationError
from vertebra.utils.module_loader import import_optional
from vertebra.utils.text import validate_identifier

if TYPE_CHECKING:
    from pytest_databases._service import DockerService
    from pytest_databases.docker.mariadb import MariaDBService
    from pytest_databases.docker.mssql import MSSQLService
    from pytest_databases.docker.mysql import MySQLService
    from pytest_databases.docker.oracle import OracleService
    from pytest_databases.docker.postgres import PostgresService
    from pytest_databases.docker.redis import RedisService
    from pytest_databases.docker.valkey import ValkeyService
    from pytest_databases.types import ServiceContainer

_ZERO_SKIP_ENV = "VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS"
_INTEGRATION_ROOT = Path(__file__).resolve().parent
_GATE_STASH_KEY = pytest.StashKey[ConformanceGate]()


def pytest_configure(config: pytest.Config) -> None:
    """Register conformance markers and initialize the active ``ConformanceGate``."""
    config.addinivalue_line(
        "markers",
        "driver_engine(engine): mark integration test with its target database driver engine",
    )
    config.addinivalue_line(
        "markers",
        "conformance(engine): mark integration test with its target conformance database engine",
    )
    config.stash[_GATE_STASH_KEY] = ConformanceGate.from_environ(os.environ)


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Filter integration items to the requested ``VERTEBRA_CONFORMANCE_ENGINES`` lane."""
    gate = config.stash.get(_GATE_STASH_KEY, None)
    if gate is None or not gate.active:
        return
    selected: list[pytest.Item] = []
    deselected: list[pytest.Item] = []
    for item in items:
        if gate.selects(item, _INTEGRATION_ROOT):
            selected.append(item)
        else:
            deselected.append(item)
    if deselected:
        config.hook.pytest_deselected(items=deselected)
        items[:] = selected


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item: pytest.Item, call: pytest.CallInfo[None]) -> Generator[None, Any, None]:
    """Record setup and call outcomes for items matching ``VERTEBRA_CONFORMANCE_ENGINES``."""
    _ = call
    outcome = yield
    gate = item.config.stash.get(_GATE_STASH_KEY, None)
    if gate is not None and gate.active:
        report: pytest.TestReport = outcome.get_result()
        gate.record(item.nodeid, ConformanceGate.item_engines(item), report)


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    """Fail the pytest session when ``ConformanceGate`` records skips, xfails, or zero runs."""
    _ = exitstatus
    gate = session.config.stash.get(_GATE_STASH_KEY, None)
    if gate is not None and gate.active and gate.failures():
        session.exitstatus = pytest.ExitCode.TESTS_FAILED


def _postgres_config_from_service(service: PostgresService) -> PostgreSQLConfig:
    """Build a ``PostgreSQLConfig`` from a ``pytest-databases`` PostgreSQL service."""
    dsn = f"postgresql://{service.user}:{service.password}@{service.host}:{service.port}/{service.database}"
    return PostgreSQLConfig(dsn=dsn)


def is_postgres_conformance_nodeid(nodeid: str) -> bool:
    """Return True when ``nodeid`` belongs to PostgreSQL conformance or live acceptance tests."""
    lowered = nodeid.lower()
    if "postgres" not in lowered:
        return False
    if "test_driver_conformance.py" in lowered:
        return "[postgres" in lowered or "-postgres" in lowered
    return (
        "test_postgres_replication_live.py" in lowered
        or "test_postgres_native_features_live.py" in lowered
        or "test_postgres_command_deadlines.py" in lowered
    )


def evaluate_postgres_zero_skip_gate(
    terminalreporter: Any,
    *,
    env: Mapping[str, str] | None = None,
) -> str | None:
    """Return a failure reason when ``VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS=1`` detects gaps."""
    resolved_env = os.environ if env is None else env
    if resolved_env.get(_ZERO_SKIP_ENV, "").strip() != "1":
        return None
    stats = getattr(terminalreporter, "stats", {})
    if not isinstance(stats, Mapping):
        return f"{_ZERO_SKIP_ENV}=1 failed: unavailable terminal reporter stats"

    skipped: list[str] = []
    for report in stats.get("skipped", ()):
        nodeid = str(getattr(report, "nodeid", ""))
        if is_postgres_conformance_nodeid(nodeid):
            skipped.append(nodeid)

    deselected: list[str] = []
    for item in stats.get("deselected", ()):
        nodeid = str(getattr(item, "nodeid", item))
        if is_postgres_conformance_nodeid(nodeid):
            deselected.append(nodeid)

    executed: set[str] = set()
    for outcome in ("passed", "failed", "error", "xfailed", "xpassed"):
        for report in stats.get(outcome, ()):
            nodeid = str(getattr(report, "nodeid", ""))
            if not is_postgres_conformance_nodeid(nodeid):
                continue
            when = getattr(report, "when", "call")
            if outcome != "passed" or when == "call":
                executed.add(nodeid)

    if skipped:
        sample = ", ".join(skipped[:5])
        return f"{_ZERO_SKIP_ENV}=1 rejected {len(skipped)} skipped PostgreSQL test(s): {sample}"
    if deselected:
        sample = ", ".join(deselected[:5])
        return f"{_ZERO_SKIP_ENV}=1 rejected {len(deselected)} deselected PostgreSQL test(s): {sample}"
    if not executed:
        return f"{_ZERO_SKIP_ENV}=1 failed: 0 PostgreSQL conformance tests executed"
    return None


def pytest_terminal_summary(terminalreporter: Any, exitstatus: int, config: pytest.Config) -> None:
    """Fail the pytest session when zero-skip conformance enforcement detects skips or zero runs."""
    _ = exitstatus
    failures: list[str] = []
    pg_failure = evaluate_postgres_zero_skip_gate(terminalreporter)
    if pg_failure is not None:
        failures.append(pg_failure)
    gate = config.stash.get(_GATE_STASH_KEY, None)
    if gate is not None and gate.active:
        failures.extend(gate.failures())
    if not failures:
        return
    for failure in failures:
        terminalreporter.write_sep("=", failure, red=True, bold=True)
    session = getattr(terminalreporter, "_session", None)
    if session is not None:
        session.exitstatus = pytest.ExitCode.TESTS_FAILED


@pytest.fixture
def vtb_postgres_config(postgres_service: PostgresService) -> PostgreSQLConfig:
    """Return a PostgreSQLConfig wired to the pytest-databases PostgresService container."""
    return _postgres_config_from_service(postgres_service)


@pytest.fixture
def vtb_postgres_14_config(postgres_14_service: PostgresService) -> PostgreSQLConfig:
    """Return a PostgreSQLConfig wired to PostgreSQL 14."""
    return _postgres_config_from_service(postgres_14_service)


@pytest.fixture
def vtb_postgres_15_config(postgres_15_service: PostgresService) -> PostgreSQLConfig:
    """Return a PostgreSQLConfig wired to PostgreSQL 15."""
    return _postgres_config_from_service(postgres_15_service)


@pytest.fixture
def vtb_postgres_16_config(postgres_16_service: PostgresService) -> PostgreSQLConfig:
    """Return a PostgreSQLConfig wired to PostgreSQL 16."""
    return _postgres_config_from_service(postgres_16_service)


@pytest.fixture
def vtb_postgres_17_config(postgres_17_service: PostgresService) -> PostgreSQLConfig:
    """Return a PostgreSQLConfig wired to PostgreSQL 17."""
    return _postgres_config_from_service(postgres_17_service)


@pytest.fixture
def vtb_postgres_18_config(postgres_18_service: PostgresService) -> PostgreSQLConfig:
    """Return a PostgreSQLConfig wired to PostgreSQL 18."""
    return _postgres_config_from_service(postgres_18_service)


@pytest.fixture
def vtb_pgvector_config(pgvector_18_service: PostgresService) -> PostgreSQLConfig:
    """Return a PostgreSQLConfig wired to the pgvector PostgreSQL 18 container."""
    return _postgres_config_from_service(pgvector_18_service)


@pytest.fixture(scope="session")
def vtb_postgres_logical_config(
    docker_service: DockerService,
    postgres_host: str,
    postgres_user: str,
    postgres_password: str,
) -> Iterator[PostgreSQLConfig]:
    """Start a PostgreSQL 18 container configured with ``wal_level=logical`` for live CDC tests."""
    pg_helpers = import_optional("pytest_databases.docker.postgres")
    net_helpers = import_optional("pytest_databases.helpers")
    if pg_helpers is None or net_helpers is None:
        msg = "pytest-databases PostgreSQL helpers are required for Docker logical replication tests"
        raise ImproperConfigurationError(msg)
    exec_psql = pg_helpers.exec_psql
    tcp_port_open = net_helpers.tcp_port_open
    validate_identifier(postgres_user, "postgres_user", allow_dots=False)
    image = os.environ.get("VERTEBRA_CONFORMANCE_POSTGRES_LOGICAL_IMAGE", "postgres:18")
    database = "vertebra_logical"

    def _ready(service: ServiceContainer) -> bool:
        exit_code, output = exec_psql(
            service.container,
            user=postgres_user,
            password=postgres_password,
            database="postgres",
            sql="SHOW wal_level",
            tuples_only=True,
        )
        return (
            exit_code == 0
            and output.decode("utf-8", errors="ignore").strip() == "logical"
            and bool(tcp_port_open(service.host, service.port))
        )

    with docker_service.run(
        image=image,
        container_port=5432,
        name="postgres_logical",
        container_host=postgres_host,
        command=[
            "postgres",
            "-c",
            "wal_level=logical",
            "-c",
            "max_replication_slots=8",
            "-c",
            "max_wal_senders=8",
        ],
        env={
            "POSTGRES_USER": postgres_user,
            "POSTGRES_PASSWORD": postgres_password,
            "POSTGRES_DB": database,
        },
        check=_ready,
        timeout=60,
        pause=0.5,
    ) as service:
        dsn = (
            f"postgresql://{postgres_user}:{postgres_password}@{service.host}:{service.port}/{database}?sslmode=disable"
        )
        yield PostgreSQLConfig(dsn=dsn)


def _container_bridge_host_and_port(service: PostgresService) -> tuple[str, int]:
    """Resolve an internal container bridge address when available, else host and port."""
    container = getattr(service, "container", None)
    attrs = getattr(container, "attrs", None)
    if isinstance(attrs, Mapping):
        net = attrs.get("NetworkSettings")
        if isinstance(net, Mapping):
            ip_addr = net.get("IPAddress")
            if isinstance(ip_addr, str) and ip_addr:
                return ip_addr, 5432
            networks = net.get("Networks")
            if isinstance(networks, Mapping):
                for entry in networks.values():
                    if isinstance(entry, Mapping):
                        net_ip = entry.get("IPAddress")
                        if isinstance(net_ip, str) and net_ip:
                            return net_ip, 5432
    return service.host, int(service.port)


def _run_pgbouncer_service(
    pool_mode: PoolerMode,
    docker_service: DockerService,
    postgres_18_service: PostgresService,
) -> Iterator[tuple[PostgreSQLConfig, PostgreSQLConfig]]:
    """Start a PgBouncer container in the requested mode fronting ``postgres_18_service``."""
    net_helpers = import_optional("pytest_databases.helpers")
    if net_helpers is None:
        msg = "pytest-databases helpers are required for Docker PgBouncer tests"
        raise ImproperConfigurationError(msg)
    tcp_port_open = net_helpers.tcp_port_open
    image = os.environ.get("VERTEBRA_CONFORMANCE_PGBOUNCER_IMAGE", "edoburu/pgbouncer:v1.26.0-p0")
    direct_config = _postgres_config_from_service(postgres_18_service)
    pg_host, pg_port = _container_bridge_host_and_port(postgres_18_service)

    def _pgbouncer_ready(service: ServiceContainer) -> bool:
        if not tcp_port_open(service.host, service.port):
            return False
        try:
            with socket.create_connection((service.host, int(service.port)), timeout=1.0) as sock:
                sock.settimeout(1.0)
                sock.sendall(struct.pack("!II", 8, 80877103))
                return sock.recv(1) in (b"N", b"S")
        except OSError:
            return False

    with docker_service.run(
        image=image,
        container_port=5432,
        name=f"pgbouncer_{pool_mode}",
        env={
            "DB_HOST": pg_host,
            "DB_PORT": str(pg_port),
            "DB_USER": postgres_18_service.user,
            "DB_PASSWORD": postgres_18_service.password,
            "DB_NAME": postgres_18_service.database,
            "AUTH_TYPE": "plain",
            "POOL_MODE": pool_mode,
            "LISTEN_PORT": "5432",
            "MAX_CLIENT_CONN": "64",
            "DEFAULT_POOL_SIZE": "16",
        },
        check=_pgbouncer_ready,
        timeout=45,
        pause=0.5,
    ) as service:
        pooler_dsn = (
            f"postgresql://{postgres_18_service.user}:{postgres_18_service.password}"
            f"@{service.host}:{service.port}/{postgres_18_service.database}?sslmode=disable"
        )
        yield (
            PostgreSQLConfig(
                dsn=pooler_dsn,
                direct_dsn=direct_config.dsn,
                pooler_mode=pool_mode,
                pgbouncer=pool_mode == "transaction",
            ),
            direct_config,
        )


@pytest.fixture
def vtb_pgbouncer_transaction_config(
    docker_service: DockerService,
    postgres_18_service: PostgresService,
) -> Iterator[tuple[PostgreSQLConfig, PostgreSQLConfig]]:
    """Start a PgBouncer transaction-pooling container fronting ``postgres_18_service``."""
    yield from _run_pgbouncer_service("transaction", docker_service, postgres_18_service)


@pytest.fixture
def vtb_pgbouncer_statement_config(
    docker_service: DockerService,
    postgres_18_service: PostgresService,
) -> Iterator[tuple[PostgreSQLConfig, PostgreSQLConfig]]:
    """Start a PgBouncer statement-pooling container fronting ``postgres_18_service``."""
    yield from _run_pgbouncer_service("statement", docker_service, postgres_18_service)


@pytest.fixture
def vtb_postgres_driver(vtb_postgres_config: PostgreSQLConfig) -> SyncPostgreSQLDriver:
    """Return a native SyncPostgreSQLDriver wired to the pytest-databases PostgresService container."""
    return SyncPostgreSQLDriver(vtb_postgres_config)


@pytest.fixture
def vtb_mysql_config(request: pytest.FixtureRequest) -> MySQLConfig:
    """Return a MySQLConfig wired to the version-selected pytest-databases MySQLService container."""
    fixture_name = ServiceVersionMatrix.fixture_name("mysql", os.environ)
    mysql_service: MySQLService = request.getfixturevalue(fixture_name)
    return MySQLConfig.from_url(
        f"mysql://{mysql_service.user}:{mysql_service.password}"
        f"@{mysql_service.host}:{mysql_service.port}/{mysql_service.db}"
    )


@pytest.fixture
def vtb_mysql_driver(vtb_mysql_config: MySQLConfig) -> SyncMySQLDriver:
    """Return a native SyncMySQLDriver wired to the pytest-databases MySQLService container."""
    return SyncMySQLDriver(vtb_mysql_config)


@pytest.fixture
def vtb_mariadb_config(request: pytest.FixtureRequest) -> MySQLConfig:
    """Return a MySQLConfig wired to the version-selected pytest-databases MariaDBService container."""
    fixture_name = ServiceVersionMatrix.fixture_name("mariadb", os.environ)
    mariadb_service: MariaDBService = request.getfixturevalue(fixture_name)
    return MySQLConfig.from_url(
        f"mysql://{mariadb_service.user}:{mariadb_service.password}"
        f"@{mariadb_service.host}:{mariadb_service.port}/{mariadb_service.db}"
    )


@pytest.fixture
def vtb_oracle_config(request: pytest.FixtureRequest) -> OracleConfig:
    """Return an OracleConfig wired to the version-selected pytest-databases Oracle container."""
    fixture_name = ServiceVersionMatrix.fixture_name("oracle", os.environ)
    oracle_service: OracleService = request.getfixturevalue(fixture_name)
    return OracleConfig(
        host=oracle_service.host,
        port=oracle_service.port,
        user=oracle_service.user,
        password=oracle_service.password,
        service_name=oracle_service.service_name,
    )


@pytest.fixture
def vtb_mssql_config(request: pytest.FixtureRequest) -> MSSQLConfig:
    """Return a MSSQLConfig wired to the version-selected pytest-databases MSSQLService container."""
    fixture_name = ServiceVersionMatrix.fixture_name("mssql", os.environ)
    mssql_service: MSSQLService = request.getfixturevalue(fixture_name)
    return MSSQLConfig(
        host=mssql_service.host,
        port=mssql_service.port,
        user=mssql_service.user,
        password=mssql_service.password,
        database=mssql_service.database,
    )


@pytest.fixture
def vtb_redis_config(redis_service: RedisService) -> RedisConfig:
    """Return a RedisConfig wired to the pytest-databases RedisService container."""
    return RedisConfig.from_url(f"redis://{redis_service.host}:{redis_service.port}/0")


@pytest.fixture
def vtb_redis_driver(vtb_redis_config: RedisConfig) -> SyncRedisDriver:
    """Return a native RESP3 SyncRedisDriver wired to the pytest-databases RedisService container."""
    return SyncRedisDriver(vtb_redis_config)


@pytest.fixture
def vtb_valkey_config(valkey_service: ValkeyService) -> ValkeyConfig:
    """Return a ValkeyConfig wired to the pytest-databases ValkeyService container."""
    return ValkeyConfig.from_url(f"valkey://{valkey_service.host}:{valkey_service.port}/0")


@pytest.fixture
def vtb_valkey_driver(vtb_valkey_config: ValkeyConfig) -> SyncValkeyDriver:
    """Return a native RESP3 SyncValkeyDriver wired to the pytest-databases ValkeyService container."""
    return SyncValkeyDriver(vtb_valkey_config)


@pytest.fixture
def vtb_sqlspec_postgres_config(vtb_postgres_config: PostgreSQLConfig) -> PostgreSQLAsyncConfig:
    """Return a SQLSpec PostgreSQLAsyncConfig wired to the pytest-databases PostgresService container."""
    return PostgreSQLAsyncConfig(vtb_postgres_config)


@pytest.fixture
def vtb_sqlspec_oracle_config(vtb_oracle_config: OracleConfig) -> OracleAsyncConfig:
    """Return a SQLSpec OracleAsyncConfig wired to the pytest-databases Oracle 23ai container."""
    return OracleAsyncConfig(vtb_oracle_config)
