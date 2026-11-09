"""Unit tests for owned PostgreSQL value records and query result invariants."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import pytest

from vertebra.db.adapters.postgres.types import (
    PgArray,
    PgBitString,
    PgComposite,
    PgInterval,
    PgJson,
    PgParameter,
    PgTemporal,
    PgVector,
    PostgresColumn,
    PostgresErrorInfo,
    PostgresPipelineOperation,
    PostgresPipelineOutcome,
    PostgresQueryResult,
    _project_public_row,
)


def test_postgres_column_and_query_result_invariants() -> None:
    """Verify PostgresColumn and PostgresQueryResult preserve order, duplicates, empty labels, and row width parity."""
    col1 = PostgresColumn("id", 23, 1, -1, 16384, 1)
    col2 = PostgresColumn("", 25, 0, -1, 0, 0)
    col3 = PostgresColumn("id", 17, 1, -1, 0, 0)

    res = PostgresQueryResult(
        columns=(col1, col2, col3),
        rows=((7, None, b""), (8, "", b"\x00")),
        affected_rows=2,
        command_tag="SELECT 2",
        transaction_status="I",
    )
    assert [c.name for c in res.columns] == ["id", "", "id"]
    assert res.rows[0][1] is None
    assert res.rows[0][2] == b""
    assert res.transaction_status == "I"

    with pytest.raises(ValueError, match="Row width"):
        PostgresQueryResult(
            columns=(col1, col2),
            rows=((1, "a", b"x"),),
            affected_rows=1,
            command_tag="SELECT 1",
            transaction_status="I",
        )


@pytest.mark.parametrize(
    ("kwargs", "exc_type"),
    [
        ({"name": "a", "type_oid": 0, "format": 0, "type_modifier": -1, "table_oid": 0, "attribute_number": 0}, ValueError),
        ({"name": "a", "type_oid": True, "format": 0, "type_modifier": -1, "table_oid": 0, "attribute_number": 0}, TypeError),
        ({"name": "a", "type_oid": 23, "format": True, "type_modifier": -1, "table_oid": 0, "attribute_number": 0}, TypeError),
        ({"name": "a", "type_oid": 23, "format": 2, "type_modifier": -1, "table_oid": 0, "attribute_number": 0}, ValueError),
        ({"name": "a", "type_oid": 23, "format": 0, "type_modifier": -1, "table_oid": -1, "attribute_number": 0}, ValueError),
        ({"name": "a", "type_oid": 23, "format": 0, "type_modifier": -1, "table_oid": 0, "attribute_number": 40000}, ValueError),
    ],
)
def test_postgres_column_rejects_invalid_fields(kwargs: dict[str, Any], exc_type: type[Exception]) -> None:
    """Verify PostgresColumn rejects bools, zero type_oid, invalid format codes, and out-of-range attributes."""
    with pytest.raises(exc_type):
        PostgresColumn(**kwargs)


def test_pg_interval_and_array_and_composite_invariants() -> None:
    """Verify PgInterval, PgArray, and PgComposite preserve lower bounds, empty rank-0, and field OIDs."""
    iv = PgInterval(months=-14, days=3, microseconds=-900_000_000_001)
    assert (iv.months, iv.days, iv.microseconds) == (-14, 3, -900_000_000_001)

    with pytest.raises(TypeError):
        PgInterval(months=True, days=0, microseconds=0)

    empty_arr = PgArray(values=(), dimensions=(), lower_bounds=(), element_oid=23)
    assert empty_arr.dimensions == ()

    arr = PgArray(values=(1, None, 2, 3), dimensions=(2, 2), lower_bounds=(0, -2), element_oid=23)
    assert arr.lower_bounds == (0, -2)

    with pytest.raises(ValueError, match="dimension product"):
        PgArray(values=(1, 2, 3), dimensions=(2, 2), lower_bounds=(1, 1), element_oid=23)

    with pytest.raises(ValueError, match="rank"):
        PgArray(values=(1,), dimensions=(1,) * 7, lower_bounds=(1,) * 7, element_oid=23)

    comp = PgComposite(
        type_oid=2249,
        field_names=("f1", "f2"),
        field_oids=(23, 25),
        values=(42, None),
    )
    assert comp.values == (42, None)

    with pytest.raises(ValueError, match="equal length"):
        PgComposite(type_oid=2249, field_names=("f1",), field_oids=(23, 25), values=(1, "a"))


@pytest.mark.parametrize(
    ("data", "bit_length", "valid"),
    [
        (b"", 0, True),
        (b"\xa0", 3, True),
        (b"\xff\x80", 9, True),
        (b"\xa8", 3, False),
        (b"\xa0", 9, False),
        (b"\x00", -1, False),
    ],
)
def test_pg_bitstring_padding_and_length_invariants(data: bytes, bit_length: int, valid: bool) -> None:
    """Verify PgBitString enforces exact byte count and zero low-bit padding."""
    if valid:
        bs = PgBitString(data=data, bit_length=bit_length)
        assert bs.data == data
        assert bs.bit_length == bit_length
    else:
        with pytest.raises(ValueError):
            PgBitString(data=data, bit_length=bit_length)

    with pytest.raises(TypeError):
        PgBitString(data=bytearray(b"\x80"), bit_length=1)  # type: ignore[arg-type]


def test_pg_vector_bounds_and_finiteness() -> None:
    """Verify PgVector enforces 1..=16000 dimensions, finite floats, and rejects bools."""
    vec = PgVector(values=(1, -0.5, 0.0), kind="halfvec")
    assert vec.values == (1.0, -0.5, 0.0)
    assert all(type(x) is float for x in vec.values)

    with pytest.raises(ValueError, match="dimension"):
        PgVector(values=(), kind="vector")
    with pytest.raises(ValueError, match="dimension"):
        PgVector(values=(0.0,) * 16001, kind="vector")
    with pytest.raises(ValueError, match="finite"):
        PgVector(values=(float("nan"),), kind="vector")
    with pytest.raises(ValueError, match="finite"):
        PgVector(values=(float("inf"),), kind="vector")
    with pytest.raises(ValueError, match="finite"):
        PgVector(values=(70000.0,), kind="halfvec")
    with pytest.raises(TypeError, match="bool"):
        PgVector(values=(True,), kind="vector")


def test_pg_temporal_sentinel_and_wire_text_exclusivity() -> None:
    """Verify PgTemporal enforces sentinel values, finite server bounds, and wire_text exclusivity."""
    eod = PgTemporal(kind="time", value=86_400_000_000, special="end_of_day")
    assert eod.value == 86_400_000_000

    wt = PgTemporal(
        kind="timestamptz",
        value=None,
        special="wire_text",
        wire_text="Wed Jul 01 12:00:00 2026 CST",
        date_style="Postgres, MDY",
        time_zone="Asia/Shanghai",
    )
    assert wt.wire_text == "Wed Jul 01 12:00:00 2026 CST"

    pos_inf_date = PgTemporal(kind="date", value=(1 << 31) - 1, special="positive_infinity")
    assert pos_inf_date.value == (1 << 31) - 1

    bc_date = PgTemporal(kind="date", value=-730_850, special="out_of_range")
    assert bc_date.value == -730_850

    with pytest.raises(ValueError, match="time only supports"):
        PgTemporal(kind="time", value=(1 << 63) - 1, special="positive_infinity")
    with pytest.raises(ValueError, match="wire_text"):
        PgTemporal(
            kind="timestamptz",
            value=0,
            special="wire_text",
            wire_text="2026-01-01 00:00:00 UTC",
            date_style="ISO, MDY",
            time_zone="UTC",
        )
    with pytest.raises(ValueError, match="server calendar range"):
        PgTemporal(kind="date", value=(1 << 31) - 2, special="out_of_range")


def test_pg_json_parameter_pipeline_and_public_row_projection() -> None:
    """Verify PgJson, PgParameter, PostgresPipelineOperation, PostgresPipelineOutcome, and _project_public_row."""
    j_null = PgJson(None)
    j_str_null = PgJson("null")
    j_obj = PgJson({"a": [1, None, True, "ok"]})
    assert j_null.value is None
    assert j_str_null.value == "null"
    assert j_obj.value == {"a": [1, None, True, "ok"]}

    projected = _project_public_row((j_null, j_str_null, {"k": None}, None, PgBitString(b"\x80", 1)))
    assert projected == (None, j_str_null, {"k": None}, None, PgBitString(b"\x80", 1))

    with pytest.raises(TypeError, match="Unsupported PgJson"):
        PgJson(Decimal("1.23"))  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="finite"):
        PgJson(float("nan"))

    cyclic: list[Any] = []
    cyclic.append(cyclic)
    with pytest.raises(ValueError, match="cyclic"):
        PgJson(cyclic)

    p = PgParameter(value=42, type_oid=23)
    assert p.type_oid == 23
    with pytest.raises(ValueError, match="nested"):
        PgParameter(value=p, type_oid=23)  # type: ignore[arg-type]

    op = PostgresPipelineOperation(sql="SELECT $1", parameters=(p,), parameter_oids=(23,), kind="select")
    assert op.kind == "select"
    with pytest.raises(ValueError, match="Conflicting"):
        PostgresPipelineOperation(sql="SELECT $1", parameters=(p,), parameter_oids=(25,))

    err_info = PostgresErrorInfo(
        sqlstate="23505",
        message="duplicate key value",
        severity="ERROR",
        diagnostics=(("C", "23505"), ("M", "duplicate key value")),
    )
    out_err = PostgresPipelineOutcome(index=0, status="error", error=err_info)
    out_skip = PostgresPipelineOutcome(index=1, status="skipped", failed_index=0)
    assert out_err.error == err_info
    assert out_skip.failed_index == 0
    with pytest.raises(ValueError, match="earlier operation"):
        PostgresPipelineOutcome(index=0, status="skipped", failed_index=0)
