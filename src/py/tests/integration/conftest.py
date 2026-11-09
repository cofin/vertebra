"""Clientless pytest-databases Docker harnesses for database, cache, and storage integration tests."""

from typing import TYPE_CHECKING

import pytest

from vertebra.db.adapters.mssql import MSSQLConfig
from vertebra.db.adapters.mysql import MySQLConfig, SyncMySQLDriver
from vertebra.db.adapters.oracle import OracleConfig
from vertebra.db.adapters.postgres import PostgreSQLConfig, SyncPostgreSQLDriver
from vertebra.db.adapters.redis import RedisConfig, SyncRedisDriver
from vertebra.db.adapters.valkey import SyncValkeyDriver, ValkeyConfig
from vertebra.extensions.sqlspec import OracleAsyncConfig, PostgreSQLAsyncConfig

if TYPE_CHECKING:
    from pytest_databases.docker.mariadb import MariaDBService
    from pytest_databases.docker.mssql import MSSQLService
    from pytest_databases.docker.mysql import MySQLService
    from pytest_databases.docker.oracle import OracleService
    from pytest_databases.docker.postgres import PostgresService
    from pytest_databases.docker.redis import RedisService
    from pytest_databases.docker.valkey import ValkeyService


@pytest.fixture
def vtb_postgres_config(postgres_service: "PostgresService") -> PostgreSQLConfig:
    """Return a PostgreSQLConfig wired to the pytest-databases PostgresService container."""
    dsn = (
        f"postgresql://{postgres_service.user}:{postgres_service.password}"
        f"@{postgres_service.host}:{postgres_service.port}/{postgres_service.database}"
    )
    return PostgreSQLConfig(dsn=dsn)


@pytest.fixture
def vtb_postgres_driver(vtb_postgres_config: PostgreSQLConfig) -> SyncPostgreSQLDriver:
    """Return a native SyncPostgreSQLDriver wired to the pytest-databases PostgresService container."""
    return SyncPostgreSQLDriver(vtb_postgres_config)


@pytest.fixture
def vtb_mysql_config(mysql_service: "MySQLService") -> MySQLConfig:
    """Return a MySQLConfig wired to the pytest-databases MySQLService container."""
    return MySQLConfig.from_url(
        f"mysql://{mysql_service.user}:{mysql_service.password}"
        f"@{mysql_service.host}:{mysql_service.port}/{mysql_service.db}"
    )


@pytest.fixture
def vtb_mysql_driver(vtb_mysql_config: MySQLConfig) -> SyncMySQLDriver:
    """Return a native SyncMySQLDriver wired to the pytest-databases MySQLService container."""
    return SyncMySQLDriver(vtb_mysql_config)


@pytest.fixture
def vtb_mariadb_config(mariadb_service: "MariaDBService") -> MySQLConfig:
    """Return a MySQLConfig wired to the pytest-databases MariaDBService container."""
    return MySQLConfig.from_url(
        f"mysql://{mariadb_service.user}:{mariadb_service.password}"
        f"@{mariadb_service.host}:{mariadb_service.port}/{mariadb_service.db}"
    )


@pytest.fixture
def vtb_oracle_config(oracle_23ai_service: "OracleService") -> OracleConfig:
    """Return an OracleConfig wired to the pytest-databases Oracle 23ai Free container."""
    return OracleConfig(
        host=oracle_23ai_service.host,
        port=oracle_23ai_service.port,
        user=oracle_23ai_service.user,
        password=oracle_23ai_service.password,
        service_name=oracle_23ai_service.service_name,
    )


@pytest.fixture
def vtb_mssql_config(mssql_service: "MSSQLService") -> MSSQLConfig:
    """Return a MSSQLConfig wired to the pytest-databases MSSQLService container."""
    return MSSQLConfig(
        host=mssql_service.host,
        port=mssql_service.port,
        user=mssql_service.user,
        password=mssql_service.password,
        database=mssql_service.database,
    )


@pytest.fixture
def vtb_redis_config(redis_service: "RedisService") -> RedisConfig:
    """Return a RedisConfig wired to the pytest-databases RedisService container."""
    return RedisConfig.from_url(f"redis://{redis_service.host}:{redis_service.port}/0")


@pytest.fixture
def vtb_redis_driver(vtb_redis_config: RedisConfig) -> SyncRedisDriver:
    """Return a native RESP3 SyncRedisDriver wired to the pytest-databases RedisService container."""
    return SyncRedisDriver(vtb_redis_config)


@pytest.fixture
def vtb_valkey_config(valkey_service: "ValkeyService") -> ValkeyConfig:
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
