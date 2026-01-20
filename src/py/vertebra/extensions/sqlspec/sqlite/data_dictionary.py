"""SQLite data dictionary helpers for broker-backed adapters."""

from sqlspec.adapters.aiosqlite.data_dictionary import AiosqliteDataDictionary
from sqlspec.adapters.sqlite.data_dictionary import SqliteDataDictionary

VertebraSqliteSyncDataDictionary = SqliteDataDictionary
VertebraSqliteAsyncDataDictionary = AiosqliteDataDictionary

__all__ = ("VertebraSqliteAsyncDataDictionary", "VertebraSqliteSyncDataDictionary")
