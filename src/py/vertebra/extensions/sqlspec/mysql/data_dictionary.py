"""MySQL data dictionary helpers for broker-backed adapters."""

from sqlspec.adapters.asyncmy.data_dictionary import AsyncmyDataDictionary
from sqlspec.adapters.pymysql.data_dictionary import PyMysqlDataDictionary

VertebraMysqlSyncDataDictionary = PyMysqlDataDictionary
VertebraMysqlAsyncDataDictionary = AsyncmyDataDictionary

__all__ = ("VertebraMysqlAsyncDataDictionary", "VertebraMysqlSyncDataDictionary")
