"""Clientless pytest-databases Docker harnesses for database, cache, and storage integration tests."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest

from vertebra.db import (
    MSSQLConfig,
    MySQLConfig,
    OracleConfig,
    PostgreSQLConfig,
    RedisConfig,
    SyncMSSQLDriver,
    SyncMySQLDriver,
    SyncOracleDriver,
    SyncPostgreSQLDriver,
    SyncRedisDriver,
    SyncValkeyDriver,
    ValkeyConfig,
)

if TYPE_CHECKING:
    from pytest_databases.docker.mariadb import MariaDBService
    from pytest_databases.docker.mssql import MSSQLService
    from pytest_databases.docker.mysql import MySQLService
    from pytest_databases.docker.oracle import OracleService
    from pytest_databases.docker.postgres import PostgresService
    from pytest_databases.docker.redis import RedisService
    from pytest_databases.docker.valkey import ValkeyService


@pytest.fixture
def vtb_postgres_config(postgres_service: PostgresService) -> PostgreSQLConfig:
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
def vtb_mysql_config(mysql_service: MySQLService) -> MySQLConfig:
    """Return a MySQLConfig wired to the pytest-databases MySQLService container."""
    return MySQLConfig(
        host=mysql_service.host,
        port=mysql_service.port,
        user=mysql_service.user,
        password=mysql_service.password,
        database=mysql_service.db,
    )


@pytest.fixture
def vtb_mysql_driver(vtb_mysql_config: MySQLConfig) -> SyncMySQLDriver:
    """Return a native SyncMySQLDriver wired to the pytest-databases MySQLService container."""
    return SyncMySQLDriver(vtb_mysql_config)


@pytest.fixture
def vtb_mariadb_config(mariadb_service: MariaDBService) -> MySQLConfig:
    """Return a MySQLConfig wired to the pytest-databases MariaDBService container."""
    return MySQLConfig(
        host=mariadb_service.host,
        port=mariadb_service.port,
        user=mariadb_service.user,
        password=mariadb_service.password,
        database=mariadb_service.db,
    )


@pytest.fixture
def vtb_oracle_config(oracle_23ai_service: OracleService) -> OracleConfig:
    """Return an OracleConfig wired to the pytest-databases Oracle 23ai Free container."""
    return OracleConfig(
        host=oracle_23ai_service.host,
        port=oracle_23ai_service.port,
        user=oracle_23ai_service.user,
        password=oracle_23ai_service.password,
        service_name=oracle_23ai_service.service_name,
    )


@pytest.fixture
def vtb_oracle_driver(vtb_oracle_config: OracleConfig) -> SyncOracleDriver:
    """Return a native SyncOracleDriver wired to the pytest-databases Oracle 23ai Free container."""
    return SyncOracleDriver(vtb_oracle_config)


@pytest.fixture
def vtb_mssql_config(mssql_service: MSSQLService) -> MSSQLConfig:
    """Return a MSSQLConfig wired to the pytest-databases MSSQLService container."""
    return MSSQLConfig(
        host=mssql_service.host,
        port=mssql_service.port,
        user=mssql_service.user,
        password=mssql_service.password,
        database=mssql_service.database,
    )


@pytest.fixture
def vtb_mssql_driver(vtb_mssql_config: MSSQLConfig) -> SyncMSSQLDriver:
    """Return a native SyncMSSQLDriver wired to the pytest-databases MSSQLService container."""
    return SyncMSSQLDriver(vtb_mssql_config)


@pytest.fixture
def vtb_redis_config(redis_service: RedisService) -> RedisConfig:
    """Return a RedisConfig wired to the pytest-databases RedisService container."""
    return RedisConfig(host=redis_service.host, port=redis_service.port)


@pytest.fixture
def vtb_redis_driver(vtb_redis_config: RedisConfig) -> SyncRedisDriver:
    """Return a native RESP3 SyncRedisDriver wired to the pytest-databases RedisService container."""
    return SyncRedisDriver(vtb_redis_config)


@pytest.fixture
def vtb_valkey_config(valkey_service: ValkeyService) -> ValkeyConfig:
    """Return a ValkeyConfig wired to the pytest-databases ValkeyService container."""
    return ValkeyConfig(host=valkey_service.host, port=valkey_service.port)


@pytest.fixture
def vtb_valkey_driver(vtb_valkey_config: ValkeyConfig) -> SyncValkeyDriver:
    """Return a native RESP3 SyncValkeyDriver wired to the pytest-databases ValkeyService container."""
    return SyncValkeyDriver(vtb_valkey_config)


@pytest.fixture
def vtb_sqlspec_postgres_config(vtb_postgres_config: PostgreSQLConfig) -> Any:
    """Return a SQLSpec VertebraPostgresConfig wired to the pytest-databases PostgresService container."""
    from vertebra.extensions.sqlspec import VertebraPostgresConfig

    return VertebraPostgresConfig(connection_config={"dsn": vtb_postgres_config.dsn})


@pytest.fixture
def vtb_sqlspec_oracle_config(vtb_oracle_config: OracleConfig) -> Any:
    """Return a SQLSpec VertebraOracleConfig wired to the pytest-databases Oracle 23ai container."""
    from vertebra.extensions.sqlspec import VertebraOracleConfig

    return VertebraOracleConfig(
        connection_config={
            "host": vtb_oracle_config.host,
            "port": vtb_oracle_config.port,
            "user": vtb_oracle_config.user,
            "password": vtb_oracle_config.password,
            "service_name": vtb_oracle_config.service_name,
        }
    )
