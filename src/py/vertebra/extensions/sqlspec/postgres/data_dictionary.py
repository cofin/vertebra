"""PostgreSQL data dictionary helpers for broker-backed adapters."""

from sqlspec.adapters.psycopg.data_dictionary import PsycopgAsyncDataDictionary, PsycopgSyncDataDictionary

VertebraPostgresSyncDataDictionary = PsycopgSyncDataDictionary
VertebraPostgresAsyncDataDictionary = PsycopgAsyncDataDictionary

__all__ = ("VertebraPostgresAsyncDataDictionary", "VertebraPostgresSyncDataDictionary")
