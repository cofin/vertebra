"""Type conversion helpers for Bun-backed drivers."""

from __future__ import annotations

import json
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import Any
from uuid import UUID


def _encode_json(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), default=str)


def _decode_json(value: Any) -> Any:
    if isinstance(value, (bytes, bytearray, memoryview)):
        value = bytes(value).decode("utf-8")
    if isinstance(value, str):
        return json.loads(value)
    return value


def _decode_pg_array(value: Any) -> Any:
    if isinstance(value, str) and value.startswith("{") and value.endswith("}"):
        inner = value[1:-1]
        if not inner:
            return []
        return inner.split(",")
    return value


def _normalize_outbound(value: Any) -> Any:
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value)
    if isinstance(value, (datetime, date, time, timedelta, Decimal)):
        return value
    return value


def encode_postgres(value: Any, type_name: str | None = None) -> Any:
    """Encode Python values to Postgres-compatible payloads."""

    if value is None:
        return None
    if type_name in {"json", "jsonb"}:
        return _encode_json(value)
    return _normalize_outbound(value)


def decode_postgres(value: Any, type_name: str | None = None) -> Any:
    """Decode Postgres values into Python-friendly types."""

    if value is None:
        return None
    if type_name in {"json", "jsonb"}:
        return _decode_json(value)
    if type_name == "uuid":
        return UUID(str(value))
    if type_name and type_name.endswith("[]"):
        return _decode_pg_array(value)
    return value


def encode_mysql(value: Any, type_name: str | None = None) -> Any:
    """Encode Python values to MySQL-compatible payloads."""

    if value is None:
        return None
    if type_name == "json":
        return _encode_json(value)
    return _normalize_outbound(value)


def decode_mysql(value: Any, type_name: str | None = None) -> Any:
    """Decode MySQL values into Python-friendly types."""

    if value is None:
        return None
    if type_name == "json":
        return _decode_json(value)
    return value


def encode_sqlite(value: Any, type_name: str | None = None) -> Any:
    """Encode Python values to SQLite-compatible payloads."""

    if value is None:
        return None
    if type_name in {"json", "jsonb"}:
        return _encode_json(value)
    return _normalize_outbound(value)


def decode_sqlite(value: Any, type_name: str | None = None) -> Any:
    """Decode SQLite values into Python-friendly types."""

    if value is None:
        return None
    if type_name in {"json", "jsonb"}:
        return _decode_json(value)
    return value


__all__ = ("decode_mysql", "decode_postgres", "decode_sqlite", "encode_mysql", "encode_postgres", "encode_sqlite")
