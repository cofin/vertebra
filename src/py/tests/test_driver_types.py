from __future__ import annotations

from uuid import UUID

from vertebra.drivers import decode_mysql, decode_postgres, decode_sqlite, encode_mysql, encode_postgres, encode_sqlite


def test_postgres_json_roundtrip() -> None:
    payload = {"alpha": 1, "bravo": ["x", "y"]}
    encoded = encode_postgres(payload, type_name="jsonb")
    decoded = decode_postgres(encoded, type_name="jsonb")
    assert decoded == payload


def test_mysql_json_roundtrip() -> None:
    payload = {"value": 42}
    encoded = encode_mysql(payload, type_name="json")
    decoded = decode_mysql(encoded, type_name="json")
    assert decoded == payload


def test_sqlite_json_roundtrip() -> None:
    payload = {"value": 42}
    encoded = encode_sqlite(payload, type_name="json")
    decoded = decode_sqlite(encoded, type_name="json")
    assert decoded == payload


def test_postgres_uuid_decode() -> None:
    encoded = "7c1fcf7c-9f4b-4c4b-b7a9-1b02e0ea5d5b"
    decoded = decode_postgres(encoded, type_name="uuid")
    assert isinstance(decoded, UUID)
    assert str(decoded) == encoded


def test_postgres_array_decode() -> None:
    decoded = decode_postgres("{alpha,bravo}", type_name="text[]")
    assert decoded == ["alpha", "bravo"]
