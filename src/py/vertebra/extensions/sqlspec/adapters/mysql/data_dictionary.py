"""MySQL and MariaDB synchronous and asynchronous SQLSpec data dictionaries."""

from typing import TYPE_CHECKING, ClassVar

from sqlspec.data_dictionary.dialects.mysql.dictionary import (
    MySQLAsyncDataDictionary as _UpstreamMySQLAsyncDataDictionary,
)
from sqlspec.data_dictionary.dialects.mysql.dictionary import (
    MySQLSyncDataDictionary as _UpstreamMySQLSyncDataDictionary,
)

if TYPE_CHECKING:
    from sqlspec.driver import AsyncDriverAdapterBase, SyncDriverAdapterBase

__all__ = ("MySQLAsyncDataDictionary", "MySQLSyncDataDictionary")


class MySQLSyncDataDictionary(_UpstreamMySQLSyncDataDictionary):
    """Synchronous MySQL and MariaDB data dictionary supporting MySQL 8/9 and MariaDB catalog metadata."""

    dialect: ClassVar[str] = "mysql"

    def get_feature_flag(self, driver: "SyncDriverAdapterBase", feature: str) -> bool:
        """Return whether the connected MySQL or MariaDB version supports `feature`."""
        if feature == "supports_vector":
            engine_version = self._get_engine_version(driver)
            if engine_version is None:
                return False
            if engine_version.engine_family == "mariadb":
                return (engine_version.version.major, engine_version.version.minor) >= (11, 7)
            return engine_version.version.major >= 9
        return super().get_feature_flag(driver, feature)


class MySQLAsyncDataDictionary(_UpstreamMySQLAsyncDataDictionary):
    """Asynchronous MySQL and MariaDB data dictionary supporting MySQL 8/9 and MariaDB catalog metadata."""

    dialect: ClassVar[str] = "mysql"

    async def get_feature_flag(self, driver: "AsyncDriverAdapterBase", feature: str) -> bool:
        """Return whether the connected MySQL or MariaDB version supports `feature`."""
        if feature == "supports_vector":
            engine_version = await self._get_engine_version(driver)
            if engine_version is None:
                return False
            if engine_version.engine_family == "mariadb":
                return (engine_version.version.major, engine_version.version.minor) >= (11, 7)
            return engine_version.version.major >= 9
        return await super().get_feature_flag(driver, feature)
