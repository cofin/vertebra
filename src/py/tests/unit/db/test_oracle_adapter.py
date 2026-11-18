"""Offline unit tests for the native Oracle adapter, dialer, codecs, pools, and pipelines."""

from __future__ import annotations

import asyncio
import datetime
import importlib
import sys
import time
import uuid
from decimal import Decimal
from typing import TYPE_CHECKING, Any

import pyarrow as pa
import pytest

import vertebra._oracle as _oracle
import vertebra.db.adapters.oracle as oracle_pkg
import vertebra.db.adapters.oracle.core as oracle_core_mod
from tests.unit.db.oracle_test_doubles import (
    _MOCK_OAUTH_TOKEN,
    _build_wallet_zip_bytes,
    _EmployeeRow,
    _reserve_closed_local_port,
    _StubAsyncOracleClient,
    _StubAsyncOraclePool,
    _StubSyncOracleClient,
    _StubSyncOraclePool,
)
from vertebra._vertebra import VectorView
from vertebra.db.adapters.oracle import (
    AsyncOracleDriver,
    AsyncOraclePipeline,
    AsyncOraclePool,
    SyncOracleDriver,
    SyncOraclePipeline,
    SyncOraclePool,
)
from vertebra.db.adapters.oracle.config import (
    DEFAULT_ORACLE_DSN,
    OracleConfig,
    OracleConnectDescriptor,
    OracleDrcpConfig,
    OracleDriverFeatures,
    OracleSessionAttrs,
    OracleTopologyConfig,
    OracleWallet,
)
from vertebra.db.adapters.oracle.core import OracleCore
from vertebra.db.driver import (
    ConnectionFailure,
    DataError,
    IntegrityError,
    InterfaceError,
    OperationalError,
    PoolExhaustedError,
    ProgrammingError,
    QueryCancelledError,
)
from vertebra.db.driver.capabilities import (
    AsyncPipelineCapable,
    SyncPipelineCapable,
)
from vertebra.typing import ImproperConfigurationError

if TYPE_CHECKING:
    from pathlib import Path


@pytest.mark.parametrize(
    ("kwargs", "expected_host", "expected_port", "expected_service", "expected_sid", "expected_protocol"),
    [
        (
            {},
            "localhost",
            1521,
            "FREEPDB1",
            None,
            "TCP",
        ),
        (
            {"host": "db.internal", "port": 1530, "service_name": "ORCLPDB1"},
            "db.internal",
            1530,
            "ORCLPDB1",
            None,
            "TCP",
        ),
        (
            {"host": "legacy.internal", "port": 1521, "sid": "ORCLSID"},
            "legacy.internal",
            1521,
            None,
            "ORCLSID",
            "TCP",
        ),
        (
            {"dsn": "oracle://rac.internal:1525/PROD"},
            "rac.internal",
            1525,
            "PROD",
            None,
            "TCP",
        ),
        (
            {
                "connect_descriptor_str": (
                    "(DESCRIPTION=(ADDRESS=(PROTOCOL=TCPS)(HOST=explicit.internal)(PORT=1522))"
                    "(CONNECT_DATA=(SERVICE_NAME=EXPLICIT_SVC)))"
                ),
                "dsn": "oracle://rac.internal:1525/PROD",
                "host": "db.internal",
            },
            "explicit.internal",
            1522,
            "EXPLICIT_SVC",
            None,
            "TCPS",
        ),
    ],
)
def test_oracle_config_connect_descriptor_precedence(
    kwargs: dict[str, Any],
    expected_host: str,
    expected_port: int,
    expected_service: str | None,
    expected_sid: str | None,
    expected_protocol: str,
) -> None:
    """Verify descriptor precedence in `OracleConfig.connect_descriptor()`."""
    cfg = OracleConfig(**kwargs)
    assert DEFAULT_ORACLE_DSN == "oracle://localhost:1521/FREEPDB1"
    desc = cfg.connect_descriptor()
    assert isinstance(desc, OracleConnectDescriptor)
    assert desc.host == expected_host
    assert desc.port == expected_port
    assert desc.service_name == expected_service
    assert desc.sid == expected_sid
    assert desc.protocol == expected_protocol


@pytest.mark.parametrize(
    "bad_kwargs",
    [
        {"host": "memory"},
        {"host": ":memory:"},
        {"dsn": ":memory:"},
        {"dsn": "memory://"},
        {"dsn": "oracle://:memory:"},
        {"dsn": "oracle://memory/FREEPDB1"},
        {"connect_descriptor_str": "memory"},
    ],
)
def test_oracle_rejects_synthetic_memory_endpoints_across_config_and_native_types(
    bad_kwargs: dict[str, Any],
) -> None:
    """Verify Python `OracleConfig` and Rust `_oracle` types reject synthetic memory endpoints."""
    with pytest.raises(ImproperConfigurationError):
        OracleConfig(**bad_kwargs)

    native_kwargs = dict(bad_kwargs)
    if "connect_descriptor_str" in native_kwargs:
        native_kwargs["connect_descriptor"] = native_kwargs.pop("connect_descriptor_str")
    with pytest.raises(InterfaceError):
        _oracle.SyncOracleClient(**native_kwargs)
    with pytest.raises(InterfaceError):
        _oracle.AsyncOracleClient(**native_kwargs)
    with pytest.raises(InterfaceError):
        _oracle.SyncOraclePool(min_size=0, max_size=1, **native_kwargs)
    with pytest.raises(InterfaceError):
        _oracle.AsyncOraclePool(min_size=0, max_size=1, **native_kwargs)


def test_oracle_config_topology_and_wallet_resolution(tmp_path: Path) -> None:
    """Verify `OracleTopologyConfig.to_tns_descriptor()` and `OracleWallet` alias resolution."""
    topo_cfg = OracleConfig(
        topology=OracleTopologyConfig(
            hosts=("rac1.internal", "rac2.internal"),
            port=1522,
            service_name="RAC_SVC",
            protocol="TCPS",
        ),
        host="ignored.internal",
    )
    topo_desc = topo_cfg.connect_descriptor()
    assert topo_desc.host == "rac1.internal"
    assert topo_desc.port == 1522
    assert topo_desc.service_name == "RAC_SVC"
    assert topo_desc.protocol == "TCPS"

    wallet_bytes = _build_wallet_zip_bytes()
    wallet_cfg = OracleConfig(wallet_zip=wallet_bytes, tns_alias="mydb_high")
    wallet_desc = wallet_cfg.connect_descriptor()
    assert wallet_desc.host == "adb.us-ashburn-1.oraclecloud.com"
    assert wallet_desc.port == 1522
    assert wallet_desc.service_name == "g1_mydb_high.adb.oraclecloud.com"
    assert wallet_desc.protocol == "TCPS"

    zip_file = tmp_path / "Wallet_MYDB.zip"
    zip_file.write_bytes(wallet_bytes)
    path_cfg = OracleConfig(
        wallet_path=str(zip_file),
        tns_alias="mydb_high",
        drcp=OracleDrcpConfig(cclass="APP_POOL", purity="SELF", connection_tag="ROLE=ADMIN"),
        auth_mode="token",
        access_token=_MOCK_OAUTH_TOKEN,
    )
    path_desc = path_cfg.connect_descriptor()
    assert path_desc.host == "adb.us-ashburn-1.oraclecloud.com"
    assert path_cfg.drcp.cclass == "APP_POOL"
    assert path_cfg.drcp.purity == "SELF"
    assert path_cfg.drcp.connection_tag == "ROLE=ADMIN"
    assert path_cfg.auth_mode == "token"
    assert path_cfg.access_token == _MOCK_OAUTH_TOKEN
    wallet = OracleWallet.from_path(zip_file)
    assert "MYDB_HIGH" in wallet.aliases


@pytest.mark.parametrize(
    ("bad_kwargs", "match_substr"),
    [
        ({"port": 0}, "port"),
        ({"purity": "SHARED"}, "purity"),
        ({"auth_mode": "kerberos"}, "auth_mode"),
        ({"connect_timeout": -5.0}, "connect_timeout"),
    ],
)
def test_oracle_native_client_constructor_validation_and_immutability(
    bad_kwargs: dict[str, Any],
    match_substr: str,
) -> None:
    """Verify zero-I/O constructor validation, GIL_USED=False, and immutable PyO3 types."""
    assert _oracle.GIL_USED is False

    sync_client = _oracle.SyncOracleClient(
        host="nonexistent.invalid",
        port=1521,
        service_name="FREEPDB1",
        user="hr",
        password="pw",
    )
    assert sync_client.closed is False
    assert sync_client.in_transaction is False
    sync_client.close()
    assert sync_client.closed is True

    attr_name = "_custom_attr"
    with pytest.raises(TypeError):
        setattr(_oracle.SyncOracleClient, attr_name, 1)
    with pytest.raises(TypeError):
        setattr(_oracle.AsyncOracleClient, attr_name, 1)

    with pytest.raises(InterfaceError, match=match_substr):
        _oracle.SyncOracleClient(**bad_kwargs)
    with pytest.raises(InterfaceError, match=match_substr):
        _oracle.AsyncOracleClient(**bad_kwargs)


def test_sync_oracle_client_fail_loud_connect_local_state_and_post_close() -> None:
    """Verify SyncOracleClient fails loud on unreachable ports and preserves local state invariants."""
    closed_port = _reserve_closed_local_port()
    client = _oracle.SyncOracleClient(
        host="127.0.0.1",
        port=closed_port,
        service_name="FREEPDB1",
        user="hr",
        password="pw",
        connect_timeout=0.5,
    )
    client.cancel()

    with pytest.raises(ConnectionFailure) as conn_exc:
        client.connect()
    assert issubclass(ConnectionFailure, OperationalError)
    assert conn_exc.value.sqlstate == "08006"
    assert conn_exc.value.is_transient is True

    assert client.in_transaction is False
    client.begin()
    assert client.in_transaction is True
    client.commit()
    assert client.in_transaction is False

    client.begin()
    assert client.in_transaction is True
    client.rollback()
    assert client.in_transaction is False

    with pytest.raises(ProgrammingError) as sp_exc:
        client.savepoint("sp;drop")
    assert sp_exc.value.sqlstate == "42000"

    with pytest.raises(OperationalError) as missing_sp_exc:
        client.rollback_to_savepoint("sp_missing")
    assert missing_sp_exc.value.vendor_code == 1086

    assert client.execute_many("INSERT INTO items (id) VALUES (:1)", []) == 0

    client.close()
    assert client.closed is True
    client.close()
    client.cancel()
    with pytest.raises(InterfaceError, match="closed"):
        client.execute("SELECT 1 FROM DUAL")


@pytest.mark.anyio
async def test_async_oracle_client_drainer_fail_loud_connect_and_lifecycle() -> None:
    """Verify AsyncOracleClient completion drainer, fail-loud connect(), and post-close checks."""
    closed_port = _reserve_closed_local_port()
    client = _oracle.AsyncOracleClient(
        host="127.0.0.1",
        port=closed_port,
        service_name="FREEPDB1",
        user="hr",
        password="pw",
        connect_timeout=0.5,
    )
    await client.begin()
    assert client.in_transaction is True

    loop = asyncio.get_running_loop()
    assert hasattr(loop, "_vertebra_oracle_completion_drainer")

    await client.commit()
    assert client.in_transaction is False
    await client.begin()
    await client.rollback()
    assert client.in_transaction is False
    assert await client.execute_many("INSERT INTO items (id) VALUES (:1)", []) == 0

    with pytest.raises(ProgrammingError) as sp_exc:
        await client.savepoint("sp;drop")
    assert sp_exc.value.sqlstate == "42000"

    with pytest.raises(ConnectionFailure) as conn_exc:
        await client.connect()
    assert conn_exc.value.sqlstate == "08006"
    assert conn_exc.value.is_transient is True

    client.cancel()
    await client.close()
    assert client.closed is True
    with pytest.raises(InterfaceError, match="closed"):
        await client.execute("SELECT 1 FROM DUAL")


def test_oracle_column_query_result_and_pipeline_outcome_pyclasses() -> None:
    """Verify OracleColumn, OracleQueryResult, and OraclePipelineOutcome PyO3 classes and immutability."""
    col = _oracle.OracleColumn("id", "NUMBER", 2, 22, 10, 0, False)
    assert col.name == "id"
    assert col.type_code == 2
    assert col.type_name == "NUMBER"
    assert col.oracle_type == "NUMBER"
    assert col.db_size == 22
    assert col.precision == 10
    assert col.scale == 0
    assert col.nullable is False

    res = _oracle.OracleQueryResult([col], [(1,)], rowcount=1, out_binds=(42,), lastrowid="AAAR3sAAEAAAACXAAA")
    assert len(res) == 1
    assert res.is_empty() is False
    assert res.rowcount == 1
    assert res.lastrowid == "AAAR3sAAEAAAACXAAA"
    assert res.out_binds == (42,)
    assert len(res.columns) == 1
    assert res.columns[0].name == "id"
    assert res.rows == [(1,)]
    assert res.to_dicts() == [{"id": 1}]

    outcome = _oracle.OraclePipelineOutcome("completed", res)
    assert outcome.status == "completed"
    assert outcome.result is not None
    assert outcome.error is None

    empty_res = _oracle.OracleQueryResult([col], [], rowcount=0)
    assert len(empty_res) == 0
    assert empty_res.is_empty() is True

    attr_name = "name"
    with pytest.raises(AttributeError):
        setattr(col, attr_name, "x")


@pytest.mark.parametrize(
    ("raw_val", "col_type", "expected_val"),
    [
        (42, "NUMBER", 42),
        (Decimal("12345.67"), "NUMBER", Decimal("12345.67")),
        (3.14159, "BINARY_DOUBLE", 3.14159),
        ("hello UTF-8 \u2603", "VARCHAR2", "hello UTF-8 \u2603"),
        (b"\x00\x01\x02\xff", "RAW", b"\x00\x01\x02\xff"),
        (True, "BOOLEAN", True),
        (False, "BOOLEAN", False),
        (datetime.date(2026, 11, 9), "DATE", datetime.date(2026, 11, 9)),
        (
            datetime.datetime(2026, 11, 9, 14, 30, 45, 123456),
            "TIMESTAMP",
            datetime.datetime(2026, 11, 9, 14, 30, 45, 123456),
        ),
        (
            datetime.datetime(
                2026,
                11,
                9,
                14,
                30,
                45,
                123000,
                tzinfo=datetime.timezone(datetime.timedelta(hours=5, minutes=30)),
            ),
            "TIMESTAMP WITH TIME ZONE",
            datetime.datetime(
                2026,
                11,
                9,
                14,
                30,
                45,
                123000,
                tzinfo=datetime.timezone(datetime.timedelta(hours=5, minutes=30)),
            ),
        ),
        ("inline clob payload", "CLOB", "inline clob payload"),
        (b"inline blob payload", "BLOB", b"inline blob payload"),
    ],
)
def test_oracle_scalar_and_temporal_codecs_roundtrip(
    raw_val: Any,
    col_type: str,
    expected_val: Any,
) -> None:
    """Verify scalar, temporal, boolean, and inline LOB round-trip coercion."""
    features = OracleDriverFeatures(fetch_decimals=(col_type == "NUMBER" and isinstance(raw_val, Decimal)))
    coerced_bind = OracleCore.coerce_bind_parameter(raw_val, features=features)
    decoded_cell = OracleCore.coerce_row_cell(coerced_bind, col_type, features=features)
    assert decoded_cell == expected_val
    assert type(decoded_cell) is type(expected_val)
    if isinstance(expected_val, datetime.datetime) and expected_val.tzinfo is not None:
        assert decoded_cell.utcoffset() == datetime.timedelta(hours=5, minutes=30)


def test_oracle_oson_codec_roundtrip_and_invalid_magic() -> None:
    """Verify OSON 0xFF 'J' 'Z' 0x01 round-trip and DataError on corrupted header."""
    payload = {"tenant": "acme", "scores": [1, 2, 3]}
    encoded = OracleCore.encode_oson(payload)
    assert encoded.startswith(b"\xffJZ\x01")
    decoded = OracleCore.decode_oson(encoded)
    assert decoded == payload

    with pytest.raises((DataError, ValueError)):
        OracleCore.decode_oson(b"\xffJZ\x99corrupted")


@pytest.mark.parametrize(
    ("dtype", "values", "expected_dtype_name", "expected_dims", "expected_list"),
    [
        ("FLOAT32", [1.5, -2.25, 3.0], "float32", 3, [1.5, -2.25, 3.0]),
        ("FLOAT64", [1.125, -0.5], "float64", 2, [1.125, -0.5]),
        ("INT8", [-128, 0, 127], "int8", 3, [-128, 0, 127]),
        ("BINARY", b"\xaa\x55", "binary", 16, [170, 85]),
    ],
)
def test_oracle_vector_codec_all_dtypes_and_return_formats(
    dtype: Any,
    values: Any,
    expected_dtype_name: str,
    expected_dims: int,
    expected_list: list[Any],
) -> None:
    """Verify Oracle 26ai 0xDB 0x01 VECTOR codec across all dtypes and return formats."""
    encoded = OracleCore.encode_vector(values, dtype=dtype)
    assert encoded.startswith(OracleCore.ORACLE_VECTOR_MAGIC)
    assert OracleCore.ORACLE_VECTOR_MAGIC == b"\xdb\x01"

    vv = OracleCore.decode_vector(encoded, return_format="vector_view")
    assert isinstance(vv, VectorView)
    assert vv.dtype == expected_dtype_name
    assert vv.dimensions == expected_dims
    if expected_dtype_name in ("float32", "int8"):
        assert vv.to_list() == expected_list

    as_list = OracleCore.decode_vector(encoded, return_format="list")
    assert as_list == expected_list

    as_bytes = OracleCore.decode_vector(encoded, return_format="bytes")
    assert isinstance(as_bytes, bytes)
    assert len(as_bytes) > 0


def test_oracle_uuid_raw16_feature_toggle() -> None:
    """Verify RAW(16) <-> uuid.UUID conversion governed by enable_uuid_binary."""
    u = uuid.uuid4()
    enabled = OracleDriverFeatures(enable_uuid_binary=True)
    disabled = OracleDriverFeatures(enable_uuid_binary=False)

    bound_bytes = OracleCore.coerce_bind_parameter(u, features=enabled)
    assert isinstance(bound_bytes, bytes)
    assert len(bound_bytes) == 16
    assert bound_bytes == u.bytes

    hydrated = OracleCore.coerce_row_cell(u.bytes, "RAW", features=enabled)
    assert isinstance(hydrated, uuid.UUID)
    assert hydrated == u

    raw_passthrough = OracleCore.coerce_row_cell(u.bytes, "RAW", features=disabled)
    assert isinstance(raw_passthrough, bytes)
    assert raw_passthrough == u.bytes


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("sql", "params", "is_mapping"),
    [
        (
            "SELECT :1 AS id, :2 AS name, :3 AS uid_raw, :4 AS doc, :5 AS emb FROM DUAL",
            (
                42,
                "Ada",
                uuid.UUID("12345678-1234-5678-1234-567812345678"),
                {"role": "admin"},
                VectorView.from_f32([1.0, 2.0], False),
            ),
            False,
        ),
        (
            "SELECT :id AS id, :name AS name, :uid_raw AS uid_raw, :doc AS doc, :emb AS emb FROM DUAL",
            {
                "id": 42,
                ":name": "Ada",
                ":uid_raw": uuid.UUID("12345678-1234-5678-1234-567812345678"),
                "doc": {"role": "admin"},
                "emb": VectorView.from_f32([1.0, 2.0], False),
            },
            True,
        ),
    ],
)
async def test_oracle_positional_and_named_bind_lowering(
    sql: str,
    params: Any,
    is_mapping: bool,
) -> None:
    """Verify positional (:1..:n) and named (:name / id) bind lowering and cell hydration."""
    expected_uuid = uuid.UUID("12345678-1234-5678-1234-567812345678")
    oson_bytes = OracleCore.encode_oson({"role": "admin"})
    vec_bytes = OracleCore.encode_vector([1.0, 2.0], dtype="FLOAT32")
    cols = [
        _oracle.OracleColumn("ID", "NUMBER"),
        _oracle.OracleColumn("NAME", "VARCHAR2"),
        _oracle.OracleColumn("UID_RAW", "RAW", db_size=16),
        _oracle.OracleColumn("DOC", "JSON"),
        _oracle.OracleColumn("EMB", "VECTOR"),
    ]
    canned = _oracle.OracleQueryResult(cols, [(42, "Ada", expected_uuid.bytes, oson_bytes, vec_bytes)], rowcount=1)
    vv_cfg = OracleConfig(driver_features=OracleDriverFeatures(vector_return_format="vector_view"))

    sync_stub = _StubSyncOracleClient()
    sync_stub.queued_results.append(canned)
    sync_driver = SyncOracleDriver(vv_cfg, client=sync_stub)
    rows = sync_driver.select(sql, params)
    assert len(rows) == 1
    row = rows[0]
    assert row["id"] == 42
    assert row["name"] == "Ada"
    assert row["uid_raw"] == expected_uuid
    assert row["doc"] == {"role": "admin"}
    assert isinstance(row["emb"], VectorView)
    assert row["emb"].to_list() == [1.0, 2.0]

    method, payload = sync_stub.calls[0]
    assert method == "query"
    coerced = payload["params"]
    if is_mapping:
        assert set(coerced.keys()) == {"id", "name", "uid_raw", "doc", "emb"}
        assert coerced["uid_raw"] == expected_uuid.bytes
        assert coerced["doc"].startswith(b"\xffJZ\x01")
        assert coerced["emb"].startswith(b"\xdb\x01")
    else:
        assert coerced[2] == expected_uuid.bytes
        assert coerced[3].startswith(b"\xffJZ\x01")
        assert coerced[4].startswith(b"\xdb\x01")
    sync_driver.close()

    async_stub = _StubAsyncOracleClient()
    async_stub.queued_results.append(canned)
    async_driver = AsyncOracleDriver(vv_cfg, client=async_stub)
    async_rows = await async_driver.select(sql, params)
    assert len(async_rows) == 1
    assert async_rows[0]["uid_raw"] == expected_uuid
    assert async_rows[0]["doc"] == {"role": "admin"}
    assert isinstance(async_rows[0]["emb"], VectorView)
    assert async_rows[0]["emb"].to_list() == [1.0, 2.0]
    await async_driver.close()


@pytest.mark.anyio
async def test_oracle_dml_returning_into_single_round_trip() -> None:
    """Verify DML ... RETURNING ... INTO routes via execute() once and projects out_binds."""
    dml_sql = "INSERT INTO items (name) VALUES (:name) RETURNING id, name INTO :id, :name"
    cols = [_oracle.OracleColumn("ID", "NUMBER"), _oracle.OracleColumn("NAME", "VARCHAR2")]

    sync_stub = _StubSyncOracleClient()
    sync_stub.queued_results.extend(
        [
            _oracle.OracleQueryResult(cols, [], rowcount=1, out_binds={"id": 101, "name": "widget"}),
            _oracle.OracleQueryResult(cols, [], rowcount=1, out_binds=(102, "gadget")),
        ]
    )
    sync_driver = SyncOracleDriver(client=sync_stub)
    first = sync_driver.select_one(dml_sql, {"name": "widget"})
    assert first == {"id": 101, "name": "widget"}
    tuples = sync_driver.execute_tuples(dml_sql, {"name": "gadget"})
    assert tuples == [(102, "gadget")]
    assert [c[0] for c in sync_stub.calls] == ["execute", "execute"]
    sync_driver.close()

    async_stub = _StubAsyncOracleClient()
    async_stub.queued_results.extend(
        [
            _oracle.OracleQueryResult(cols, [], rowcount=1, out_binds={"id": 101, "name": "widget"}),
            _oracle.OracleQueryResult(cols, [], rowcount=1, out_binds={"id": 102, "name": "gadget"}),
        ]
    )
    async_driver = AsyncOracleDriver(client=async_stub)
    assert await async_driver.select_one(dml_sql, {"name": "widget"}) == {"id": 101, "name": "widget"}
    assert await async_driver.exec_returning_async(dml_sql, {"name": "gadget"}) == [{"id": 102, "name": "gadget"}]
    assert [c[0] for c in async_stub.calls] == ["execute", "execute"]
    await async_driver.close()


@pytest.mark.anyio
async def test_oracle_execute_many_batches_and_empty_short_circuit() -> None:
    """Verify execute_many dispatches a single batch call and short-circuits empty inputs."""
    batch = [(1, "a"), (2, "b"), (3, "c")]
    sync_stub = _StubSyncOracleClient()
    sync_driver = SyncOracleDriver(client=sync_stub)
    assert sync_driver.execute_many("INSERT INTO items (id, name) VALUES (:1, :2)", []) == 0
    assert sync_stub.calls == []
    assert sync_driver.execute_many("INSERT INTO items (id, name) VALUES (:1, :2)", batch) == 3
    assert len(sync_stub.calls) == 1
    assert sync_stub.calls[0][0] == "execute_many"
    assert sync_stub.calls[0][1]["params_seq"] == [[1, "a"], [2, "b"], [3, "c"]]
    sync_driver.close()

    async_stub = _StubAsyncOracleClient()
    async_driver = AsyncOracleDriver(client=async_stub)
    assert await async_driver.execute_many("INSERT INTO items (id, name) VALUES (:1, :2)", []) == 0
    assert async_stub.calls == []
    assert await async_driver.execute_many("INSERT INTO items (id, name) VALUES (:1, :2)", batch) == 3
    assert len(async_stub.calls) == 1
    assert async_stub.calls[0][0] == "execute_many"
    await async_driver.close()


@pytest.mark.anyio
async def test_oracle_lowercase_column_names_toggle() -> None:
    """Verify enable_lowercase_column_names=True (default) vs False across description, dicts, and structs."""
    query = "SELECT 1 AS EMPLOYEE_ID, 'Ada' AS FIRST_NAME FROM DUAL"
    raw_res = _oracle.OracleQueryResult(
        [_oracle.OracleColumn("EMPLOYEE_ID", "NUMBER"), _oracle.OracleColumn("FIRST_NAME", "VARCHAR2")],
        [(1, "Ada")],
        rowcount=1,
    )

    lower_stub = _StubSyncOracleClient()
    lower_stub.queued_results.extend([raw_res, raw_res, raw_res])
    lower_driver = SyncOracleDriver(
        OracleConfig(driver_features=OracleDriverFeatures(enable_lowercase_column_names=True)),
        client=lower_stub,
    )
    rows, cols = lower_driver.execute_with_description(query)
    assert cols == ("employee_id", "first_name")
    assert rows == [(1, "Ada")]
    assert lower_driver.select(query) == [{"employee_id": 1, "first_name": "Ada"}]
    assert lower_driver.select(query, schema_type=_EmployeeRow) == [_EmployeeRow(employee_id=1, first_name="Ada")]
    lower_driver.close()

    upper_stub = _StubSyncOracleClient()
    upper_stub.queued_results.extend([raw_res, raw_res])
    upper_driver = SyncOracleDriver(
        OracleConfig(driver_features=OracleDriverFeatures(enable_lowercase_column_names=False)),
        client=upper_stub,
    )
    rows_u, cols_u = upper_driver.execute_with_description(query)
    assert cols_u == ("EMPLOYEE_ID", "FIRST_NAME")
    assert rows_u == [(1, "Ada")]
    assert upper_driver.select(query) == [{"EMPLOYEE_ID": 1, "FIRST_NAME": "Ada"}]
    upper_driver.close()

    async_stub = _StubAsyncOracleClient()
    async_stub.queued_results.extend([raw_res, raw_res])
    async_upper = AsyncOracleDriver(
        OracleConfig(driver_features=OracleDriverFeatures(enable_lowercase_column_names=False)),
        client=async_stub,
    )
    _, acols_u = await async_upper.execute_with_description(query)
    assert acols_u == ("EMPLOYEE_ID", "FIRST_NAME")
    assert await async_upper.select(query) == [{"EMPLOYEE_ID": 1, "FIRST_NAME": "Ada"}]
    await async_upper.close()


@pytest.mark.anyio
async def test_oracle_arrow59_inline_and_shm_escalation() -> None:
    """Verify inline vs VTBARROW SHM escalation across SyncOracleDriver and AsyncOracleDriver."""
    query = "SELECT LEVEL AS ID, 'item_' AS NAME FROM DUAL CONNECT BY LEVEL <= 5"
    sample_rows = [(idx, f"item_{idx}") for idx in range(1, 6)]
    sample_cols = ("ID", "NAME")

    sync_stub = _StubSyncOracleClient()
    sync_stub.queued_arrow.extend([(sample_rows, sample_cols), (sample_rows, sample_cols)])
    sync_driver = SyncOracleDriver(client=sync_stub)
    with sync_driver.select_to_arrow(query, force_shm=False) as inline_cur:
        assert inline_cur.used_shm is False
        assert len(inline_cur) == 5
        inline_tbl = pa.ipc.open_stream(pa.py_buffer(inline_cur.buffer_view())).read_all()
        assert inline_tbl.column_names == ["id", "name"]
        assert inline_tbl.num_rows == 5

    with sync_driver.select_to_arrow(query, force_shm=True) as shm_cur:
        assert shm_cur.used_shm is True
        assert len(shm_cur) == 5
        shm_tbl = pa.ipc.open_stream(pa.py_buffer(shm_cur.buffer_view())).read_all()
        assert shm_tbl.column_names == ["id", "name"]
        assert shm_tbl.to_pydict()["id"] == [1, 2, 3, 4, 5]
    sync_driver.close()

    async_stub = _StubAsyncOracleClient()
    async_stub.queued_arrow.extend([(sample_rows, sample_cols), (sample_rows, sample_cols)])
    async_driver = AsyncOracleDriver(client=async_stub)
    with await async_driver.select_to_arrow(query, force_shm=False) as a_inline:
        assert a_inline.used_shm is False
        assert len(a_inline) == 5

    with await async_driver.select_to_arrow(query, force_shm=True) as a_shm:
        assert a_shm.used_shm is True
        assert len(a_shm) == 5
        ashm_tbl = pa.ipc.open_stream(pa.py_buffer(a_shm.buffer_view())).read_all()
        assert ashm_tbl.column_names == ["id", "name"]
        assert ashm_tbl.num_rows == 5
    await async_driver.close()


@pytest.mark.anyio
async def test_oracle_stream_arrow_batches_sync_and_async() -> None:
    """Verify stream_arrow_batches yields chunked ArrowCursor batches with matching IPC streams."""
    query = "SELECT LEVEL AS ID, 'item' AS NAME FROM DUAL CONNECT BY LEVEL <= 5"
    res = _oracle.OracleQueryResult(
        [_oracle.OracleColumn("ID", "NUMBER"), _oracle.OracleColumn("NAME", "VARCHAR2")],
        [(idx, "item") for idx in range(1, 6)],
        rowcount=5,
    )

    sync_stub = _StubSyncOracleClient()
    sync_stub.queued_results.append(res)
    sync_driver = SyncOracleDriver(client=sync_stub)
    sync_batches = list(sync_driver.stream_arrow_batches(query, batch_size=2))
    assert [len(b) for b in sync_batches] == [2, 2, 1]
    assert sum(len(b.buffer_view()) for b in sync_batches) > 0
    sync_driver.close()

    async_stub = _StubAsyncOracleClient()
    async_stub.queued_results.append(res)
    async_driver = AsyncOracleDriver(client=async_stub)
    async_lengths: list[int] = []
    async for batch in async_driver.stream_arrow_batches(query, batch_size=2):
        with batch:
            async_lengths.append(len(batch))
            assert len(batch.buffer_view()) > 0
    assert async_lengths == [2, 2, 1]
    await async_driver.close()


@pytest.mark.anyio
async def test_oracle_load_from_arrow_direct_path_and_identifier_validation() -> None:
    """Verify load_from_arrow APPEND_VALUES hint precedence, empty short-circuit, and identifier validation."""
    table = pa.Table.from_pydict({"id": [1, 2], "name": ["a", "b"]})
    empty_table = pa.Table.from_pydict({"id": [], "name": []})

    sync_stub = _StubSyncOracleClient()
    sync_driver = SyncOracleDriver(client=sync_stub)
    assert sync_driver.load_from_arrow("items", empty_table) == 0
    assert sync_stub.calls == []
    assert sync_driver.load_from_arrow("hr.items", table, direct_path=True) == 2
    assert sync_stub.calls[0][1]["sql"] == "INSERT /*+ APPEND_VALUES */ INTO hr.items (id, name) VALUES (:1, :2)"
    assert sync_driver.load_from_arrow("items", [{"id": 3, "name": "c"}], direct_path=False) == 1
    assert sync_stub.calls[1][1]["sql"] == "INSERT INTO items (id, name) VALUES (:1, :2)"

    with pytest.raises((ValueError, ProgrammingError)):
        sync_driver.load_from_arrow("bad;table", table)
    with pytest.raises((ValueError, ProgrammingError)):
        sync_driver.load_from_arrow("items", table, columns=("id", "bad;col"))
    sync_driver.close()

    async_stub = _StubAsyncOracleClient()
    async_driver = AsyncOracleDriver(client=async_stub)
    assert await async_driver.load_from_arrow("items", empty_table) == 0
    assert await async_driver.load_from_arrow("items", table, direct_path=True) == 2
    assert async_stub.calls[0][1]["sql"] == "INSERT /*+ APPEND_VALUES */ INTO items (id, name) VALUES (:1, :2)"
    with pytest.raises((ValueError, ProgrammingError)):
        await async_driver.load_from_arrow("bad;table", table)
    await async_driver.close()


@pytest.mark.parametrize(
    "bad_kwargs",
    [
        {"max_size": 0},
        {"min_size": -1, "max_size": 4},
        {"min_size": 5, "max_size": 2},
        {"acquire_timeout": 0.0},
        {"reset_timeout": -1.0},
        {"close_timeout": 0.0},
    ],
)
def test_oracle_pool_zero_io_init_and_bounds_validation(bad_kwargs: dict[str, Any]) -> None:
    """Verify native `_oracle` SyncOraclePool and AsyncOraclePool construct with zero I/O and validate bounds."""
    start = time.perf_counter()
    pool = _oracle.SyncOraclePool(host="nonexistent.invalid", min_size=1, max_size=4)
    elapsed_ms = (time.perf_counter() - start) * 1000.0
    assert elapsed_ms < 25.0
    assert pool.min_size == 1
    assert pool.max_size == 4
    assert pool.size == 0
    assert pool.idle_size == 0
    assert pool.borrowed_size == 0
    assert pool.closed is False
    stats = pool.stats()
    assert stats == {
        "min_size": 1,
        "max_size": 4,
        "size": 0,
        "idle": 0,
        "idle_size": 0,
        "borrowed": 0,
        "borrowed_size": 0,
        "opening": 0,
        "closed": 0,
    }
    pool.close()
    assert pool.closed is True
    assert pool.stats()["closed"] == 1

    with pytest.raises((InterfaceError, ValueError)):
        _oracle.SyncOraclePool(host="nonexistent.invalid", **bad_kwargs)
    with pytest.raises((InterfaceError, ValueError)):
        _oracle.AsyncOraclePool(host="nonexistent.invalid", **bad_kwargs)


@pytest.mark.anyio
async def test_native_oracle_pool_fail_loud_connect_rolls_back_opening_slot() -> None:
    """Verify native `_oracle` pools raise ConnectionFailure on dead ports and release reserved opening slots."""
    closed_port = _reserve_closed_local_port()
    pool = _oracle.SyncOraclePool(
        host="127.0.0.1",
        port=closed_port,
        min_size=0,
        max_size=1,
        connect_timeout=0.5,
    )
    with pytest.raises((InterfaceError, ValueError)):
        pool.acquire(purity="BOGUS")
    with pytest.raises(ConnectionFailure) as exc_info:
        pool.acquire()
    assert exc_info.value.sqlstate == "08006"
    assert exc_info.value.is_transient is True
    assert pool.size == 0
    assert pool.borrowed_size == 0
    assert pool.idle_size == 0
    pool.close()
    with pytest.raises(InterfaceError):
        pool.acquire()

    apool = _oracle.AsyncOraclePool(
        host="127.0.0.1",
        port=closed_port,
        min_size=0,
        max_size=1,
        connect_timeout=0.5,
    )
    with pytest.raises((InterfaceError, ValueError)):
        await apool.acquire(purity="BOGUS")
    with pytest.raises(ConnectionFailure) as aexc:
        await apool.acquire()
    assert aexc.value.sqlstate == "08006"
    assert aexc.value.is_transient is True
    assert apool.size == 0
    assert apool.borrowed_size == 0
    assert apool.idle_size == 0
    await apool.close()


@pytest.mark.anyio
async def test_oracle_pool_acquire_release_exhaustion_rollback_and_evict() -> None:
    """Verify Python pool facades propagate PoolExhaustedError (53300), rollback dirty leases, and evict."""
    pool = SyncOraclePool(native_pool=_StubSyncOraclePool(min_size=0, max_size=1))
    c1 = pool.acquire()
    assert pool.size == 1
    assert pool.borrowed_size == 1
    assert pool.idle_size == 0
    c1.begin()
    assert c1.in_transaction is True

    with pytest.raises(PoolExhaustedError) as exc_info:
        pool.acquire(timeout=0.03)
    assert getattr(exc_info.value, "sqlstate", None) == "53300"
    assert getattr(exc_info.value, "is_transient", None) is True

    pool.release(c1)
    assert c1.in_transaction is False
    assert ("rollback", None) in c1.calls
    assert pool.borrowed_size == 0
    assert pool.idle_size == 1

    reused = pool.acquire()
    assert reused is c1
    pool.release(reused, discard=True)
    assert pool.size == 0
    assert reused.closed is True

    c2 = pool.acquire()
    pool.evict(c2)
    assert pool.size == 0
    assert c2.closed is True
    pool.close()

    apool = AsyncOraclePool(native_pool=_StubAsyncOraclePool(min_size=0, max_size=1))
    ac1 = await apool.acquire()
    await ac1.begin()
    with pytest.raises(PoolExhaustedError) as aexc:
        await apool.acquire(timeout=0.03)
    assert getattr(aexc.value, "sqlstate", None) == "53300"
    await apool.release(ac1)
    assert ac1.in_transaction is False
    assert apool.idle_size == 1
    await apool.evict(ac1)
    assert apool.size == 0
    await apool.close()


@pytest.mark.anyio
async def test_oracle_drcp_purity_validation_and_tag_matching() -> None:
    """Verify DRCP cclass, purity (SELF vs NEW), and connection_tag matching on pool checkout."""
    pool = SyncOraclePool(native_pool=_StubSyncOraclePool(min_size=0, max_size=2, cclass="VTB_POOL", purity="SELF"))
    c_a = pool.acquire_from_pool()
    c_b = pool.acquire_from_pool()
    assert c_a.cclass == "VTB_POOL"
    assert c_a.purity == "SELF"
    assert c_a.tag_matched is False
    pool.release_to_pool(c_a, connection_tag="TAG_A")
    pool.release_to_pool(c_b, connection_tag="TAG_B")

    matched = pool.acquire_from_pool(connection_tag="TAG_B", purity="SELF")
    assert matched.connection_tag == "TAG_B"
    assert matched.tag_matched is True
    pool.release_to_pool(matched, connection_tag="TAG_B")

    fresh = pool.acquire_from_pool(connection_tag="TAG_B", purity="NEW")
    assert fresh.purity == "NEW"
    assert fresh.tag_matched is False
    assert fresh.connection_tag is None
    pool.release_to_pool(fresh)

    with pytest.raises((InterfaceError, ValueError)):
        pool.acquire(purity="BOGUS")
    pool.close()

    apool = AsyncOraclePool(
        native_pool=_StubAsyncOraclePool(min_size=0, max_size=2, cclass="VTB_ASYNC_POOL", purity="SELF")
    )
    ac1 = await apool.acquire_from_pool()
    await apool.release_to_pool(ac1, connection_tag="ASYNC_TAG")
    ac_matched = await apool.acquire_from_pool(connection_tag="ASYNC_TAG", purity="SELF")
    assert ac_matched.tag_matched is True
    assert ac_matched.connection_tag == "ASYNC_TAG"
    await apool.release_to_pool(ac_matched)
    await apool.close()


def test_oracle_native_set_session_attrs_coalescing_and_ttc_frame_encoding() -> None:
    """Verify native `_oracle.SyncOracleClient.set_session_attrs` coalesces zero-I/O and encodes `[0x11, 0x6e]`."""
    client = _oracle.SyncOracleClient(host="nonexistent.invalid", service_name="FREEPDB1")
    client.set_session_attrs(client_identifier="t1", module="m1")
    client.set_session_attrs(action="a1")
    assert client.pending_session_attrs == {
        "client_identifier": "t1",
        "module": "m1",
        "action": "a1",
        "ecid": None,
    }
    assert client.last_piggyback_frame is None
    client.close()

    attrs = OracleSessionAttrs(client_identifier="t1", module="m1", action="a1")
    frame = attrs.encode_ttc_piggyback()
    assert frame.startswith(b"\x11\x6e\x03")


@pytest.mark.anyio
async def test_python_oracle_pool_lease_and_driver_piggyback() -> None:
    """Verify SyncOraclePool and AsyncOraclePool lease() checkout and driver last_piggyback_frame."""
    one_row = _oracle.OracleQueryResult([_oracle.OracleColumn("VAL", "NUMBER")], [(1,)], rowcount=1)

    sync_native = _StubSyncOraclePool(min_size=0, max_size=2)
    with SyncOraclePool(native_pool=sync_native) as pool:
        assert pool.min_size == 0
        assert pool.max_size == 2
        assert pool.closed is False
        assert pool.is_closed is False
        with pool.lease(client_identifier="t-0") as drv:
            assert pool.borrowed_size == 1
            drv.client.queued_results.append(one_row)
            drv.set_session_attrs(client_identifier="t-1", module="api")
            assert drv.select_value("SELECT 1 FROM DUAL") == 1
            frame = drv.last_piggyback_frame
            assert frame is not None
            assert frame.startswith(b"\x11\x6e\x02")
        assert pool.borrowed_size == 0
        assert pool.idle_size == 1
    assert pool.closed is True
    assert pool.is_closed is True

    async_native = _StubAsyncOraclePool(min_size=0, max_size=2)
    async with AsyncOraclePool(native_pool=async_native) as apool:
        async with apool.lease() as adrv:
            assert apool.borrowed_size == 1
            adrv.client.queued_results.append(one_row)
            adrv.set_session_attrs(client_identifier="t-async", module="worker")
            assert await adrv.select_value("SELECT 1 FROM DUAL") == 1
            aframe = adrv.last_piggyback_frame
            assert aframe is not None
            assert aframe.startswith(b"\x11\x6e\x02")
        assert apool.borrowed_size == 0
        assert apool.idle_size == 1
    assert apool.closed is True


@pytest.mark.anyio
async def test_oracle_driver_transaction_pins_lease_and_guards_child_tasks() -> None:
    """Verify AsyncOracleDriver transaction() pins a dedicated lease and isolates spawned child tasks."""
    ok_res = _oracle.OracleQueryResult([_oracle.OracleColumn("OK", "NUMBER")], [(1,)], rowcount=1)
    async_native = _StubAsyncOraclePool(min_size=0, max_size=2)
    async with AsyncOraclePool(native_pool=async_native) as apool:
        driver = AsyncOracleDriver(pool=apool)
        assert driver.in_transaction is False

        async with driver.transaction():
            assert driver.in_transaction is True
            assert apool.borrowed_size == 1
            pinned_client = driver.client
            await driver.execute("INSERT INTO tx_guard (id, name) VALUES (1, 'parent')")
            assert driver.client is pinned_client

            async def _child_query() -> tuple[bool, int, Any, list[dict[str, Any]]]:
                in_tx = driver.in_transaction
                rows = await driver.select("SELECT 1 AS ok FROM DUAL")
                return in_tx, apool.size, driver.client, rows

            child_client = _StubAsyncOracleClient()
            child_client.queued_results.append(ok_res)
            async_native._idle.append(child_client)

            child_in_tx, total_size, child_default_client, child_rows = await asyncio.create_task(_child_query())
            assert child_in_tx is False
            assert child_rows == [{"ok": 1}]
            assert total_size == 2
            assert child_default_client is not pinned_client
            assert apool.borrowed_size == 1
            assert apool.idle_size == 1

            pinned_client.queued_results.append(ok_res)
            assert await driver.select_value("SELECT COUNT(*) FROM tx_guard") == 1

        assert driver.in_transaction is False
        assert apool.borrowed_size == 0
        assert apool.idle_size == 2
        await driver.close()


@pytest.mark.anyio
async def test_oracle_driver_savepoint_and_rollback_to_savepoint() -> None:
    """Verify savepoint, rollback_to_savepoint, release_savepoint, and identifier validation."""
    sync_stub = _StubSyncOracleClient()
    with SyncOracleDriver(client=sync_stub) as sync_driver, sync_driver.transaction():
        sync_driver.savepoint("sp_1")
        sync_driver.rollback_to_savepoint("sp_1")
        sync_driver.release_savepoint("sp_1")
        assert list(sync_driver.transaction_events) == [
            "BEGIN",
            "SAVEPOINT sp_1",
            "ROLLBACK TO SAVEPOINT sp_1",
        ]

        with pytest.raises(ValueError, match="savepoint"):
            sync_driver.savepoint("bad;name")
        with pytest.raises(ValueError, match="savepoint"):
            sync_driver.rollback_to_savepoint("bad;name")
        with pytest.raises(ValueError, match="savepoint"):
            sync_driver.release_savepoint("bad;name")

    async_stub = _StubAsyncOracleClient()
    async with AsyncOracleDriver(client=async_stub) as async_driver, async_driver.transaction():
        await async_driver.savepoint("sp_1")
        await async_driver.rollback_to_savepoint("sp_1")
        await async_driver.release_savepoint("sp_1")
        assert list(async_driver.transaction_events) == [
            "BEGIN",
            "SAVEPOINT sp_1",
            "ROLLBACK TO SAVEPOINT sp_1",
        ]

        with pytest.raises(ValueError, match="savepoint"):
            await async_driver.savepoint("bad;name")
        with pytest.raises(ValueError, match="savepoint"):
            await async_driver.rollback_to_savepoint("bad;name")
        with pytest.raises(ValueError, match="savepoint"):
            await async_driver.release_savepoint("bad;name")


@pytest.mark.anyio
async def test_sync_and_async_pipeline_delayed_commit_on_clean_exit() -> None:
    """Verify SyncOraclePipeline and AsyncOraclePipeline hold transaction across results() and commit on exit."""
    row_res = _oracle.OracleQueryResult(
        [_oracle.OracleColumn("ID", "NUMBER"), _oracle.OracleColumn("LABEL", "VARCHAR2")],
        [(1, "a")],
        rowcount=1,
    )
    outcomes = [
        _oracle.OraclePipelineOutcome("completed", _oracle.OracleQueryResult([], [], rowcount=1)),
        _oracle.OraclePipelineOutcome("completed", row_res),
    ]

    sync_stub = _StubSyncOracleClient()
    sync_stub.queued_pipeline.append(outcomes)
    with SyncOracleDriver(client=sync_stub) as driver:
        assert isinstance(driver, SyncPipelineCapable)
        with driver.pipeline() as pipe:
            assert isinstance(pipe, SyncOraclePipeline)
            assert driver.in_transaction is True
            with pytest.raises(InterfaceError, match="already active"):
                driver.pipeline()
            pipe.execute("INSERT INTO items (id, label) VALUES (:1, :2)", (1, "a"))
            pipe.select("SELECT id, label FROM items WHERE id = :1", (1,))
            res = pipe.results()
            assert res == [1, [{"id": 1, "label": "a"}]]
            assert driver.in_transaction is True
            assert driver.transaction_events == ("BEGIN",)
        assert driver.in_transaction is False
        assert driver.transaction_events == ("BEGIN", "COMMIT")

    async_stub = _StubAsyncOracleClient()
    async_stub.queued_pipeline.append(outcomes)
    async with AsyncOracleDriver(client=async_stub) as async_driver:
        assert isinstance(async_driver, AsyncPipelineCapable)
        async with async_driver.pipeline() as apipe:
            assert isinstance(apipe, AsyncOraclePipeline)
            assert async_driver.in_transaction is True
            with pytest.raises(InterfaceError, match="already active"):
                async_driver.pipeline()
            apipe.execute("INSERT INTO items (id, label) VALUES (:1, :2)", (1, "a"))
            apipe.select("SELECT id, label FROM items WHERE id = :1", (1,))
            ares = await apipe.results()
            assert ares == [1, [{"id": 1, "label": "a"}]]
            assert async_driver.in_transaction is True
            assert async_driver.transaction_events == ("BEGIN",)
        assert async_driver.in_transaction is False
        assert async_driver.transaction_events == ("BEGIN", "COMMIT")


@pytest.mark.anyio
async def test_pipeline_rolls_back_when_caller_raises_after_results() -> None:
    """Verify raising after pipe.results() or exiting with unexecuted ops rolls back the batch."""
    row_res = _oracle.OracleQueryResult(
        [_oracle.OracleColumn("ID", "NUMBER"), _oracle.OracleColumn("LABEL", "VARCHAR2")],
        [(1, "transient")],
        rowcount=1,
    )
    outcomes = [
        _oracle.OraclePipelineOutcome("completed", _oracle.OracleQueryResult([], [], rowcount=1)),
        _oracle.OraclePipelineOutcome("completed", row_res),
    ]

    sync_stub = _StubSyncOracleClient()
    sync_stub.queued_pipeline.append(outcomes)
    with SyncOracleDriver(client=sync_stub) as driver:
        with pytest.raises(ValueError, match="hydration failed"), driver.pipeline() as pipe:
            pipe.execute("INSERT INTO items (id, label) VALUES (:1, :2)", (1, "transient"))
            pipe.select("SELECT id, label FROM items WHERE id = :1", (1,))
            assert pipe.results() == [1, [{"id": 1, "label": "transient"}]]
            raise ValueError("hydration failed")
        assert driver.in_transaction is False
        assert driver.transaction_events == ("BEGIN", "ROLLBACK")

        with driver.pipeline() as pipe2:
            pipe2.execute("INSERT INTO items (id, label) VALUES (:1, :2)", (2, "auto_flushed"))
        second_events: tuple[str, ...] = driver.transaction_events
        assert second_events == ("BEGIN", "ROLLBACK", "BEGIN", "COMMIT")
        batch_calls = [c for c in sync_stub.calls if c[0] == "execute_pipeline_batch"]
        assert len(batch_calls) == 2

    async_stub = _StubAsyncOracleClient()
    async_stub.queued_pipeline.append(outcomes)
    async with AsyncOracleDriver(client=async_stub) as async_driver:
        with pytest.raises(ValueError, match="hydration failed"):
            async with async_driver.pipeline() as apipe:
                apipe.execute("INSERT INTO items (id, label) VALUES (:1, :2)", (1, "transient"))
                apipe.select("SELECT id, label FROM items WHERE id = :1", (1,))
                assert await apipe.results() == [1, [{"id": 1, "label": "transient"}]]
                raise ValueError("hydration failed")
        assert async_driver.in_transaction is False
        assert async_driver.transaction_events == ("BEGIN", "ROLLBACK")


@pytest.mark.anyio
async def test_pipeline_nested_inside_transaction_uses_oracle_savepoint_without_release() -> None:
    """Verify nested pipeline inside transaction() uses SAVEPOINT _vtb_pipe_<n> without RELEASE SAVEPOINT."""
    sync_stub = _StubSyncOracleClient()
    with SyncOracleDriver(client=sync_stub) as driver:
        with driver.transaction():
            with driver.pipeline() as pipe1:
                pipe1.execute("INSERT INTO items (id, label) VALUES (2, 'inner_ok')")
                assert pipe1.results() == [1]
            assert driver.in_transaction is True
            assert list(driver.transaction_events) == ["BEGIN", "SAVEPOINT _vtb_pipe_1"]

            with pytest.raises(RuntimeError, match="inner abort"), driver.pipeline() as pipe2:
                pipe2.execute("INSERT INTO items (id, label) VALUES (3, 'inner_bad')")
                assert pipe2.results() == [1]
                raise RuntimeError("inner abort")
            assert driver.in_transaction is True
            assert list(driver.transaction_events) == [
                "BEGIN",
                "SAVEPOINT _vtb_pipe_1",
                "SAVEPOINT _vtb_pipe_2",
                "ROLLBACK TO SAVEPOINT _vtb_pipe_2",
            ]
        assert driver.in_transaction is False
        assert list(driver.transaction_events) == [
            "BEGIN",
            "SAVEPOINT _vtb_pipe_1",
            "SAVEPOINT _vtb_pipe_2",
            "ROLLBACK TO SAVEPOINT _vtb_pipe_2",
            "COMMIT",
        ]

    async_stub = _StubAsyncOracleClient()
    async with AsyncOracleDriver(client=async_stub) as async_driver, async_driver.transaction():
        async with async_driver.pipeline() as apipe1:
            apipe1.execute("INSERT INTO items (id, label) VALUES (2, 'inner_ok')")
            assert await apipe1.results() == [1]
        with pytest.raises(RuntimeError, match="inner abort"):
            async with async_driver.pipeline() as apipe2:
                apipe2.execute("INSERT INTO items (id, label) VALUES (3, 'inner_bad')")
                assert await apipe2.results() == [1]
                raise RuntimeError("inner abort")
        assert async_driver.transaction_events == (
            "BEGIN",
            "SAVEPOINT _vtb_pipe_1",
            "SAVEPOINT _vtb_pipe_2",
            "ROLLBACK TO SAVEPOINT _vtb_pipe_2",
        )


@pytest.mark.anyio
async def test_pipeline_mid_batch_error_marks_subsequent_skipped_and_rolls_back() -> None:
    """Verify mid-batch ORA-00001 failure marks later steps 'skipped', raises IntegrityError, and rolls back."""
    dup_err = IntegrityError("ORA-00001: unique constraint violated")
    dup_err.sqlstate = "23505"
    dup_err.vendor_code = 1
    mid_batch = [
        _oracle.OraclePipelineOutcome("completed", _oracle.OracleQueryResult([], [], rowcount=1)),
        _oracle.OraclePipelineOutcome("error", None, dup_err),
        _oracle.OraclePipelineOutcome("skipped", None, None),
    ]

    sync_stub = _StubSyncOracleClient()
    sync_stub.queued_pipeline.append(mid_batch)
    with SyncOracleDriver(client=sync_stub) as driver:
        with pytest.raises(IntegrityError) as exc_info, driver.pipeline() as pipe:
            pipe.execute("INSERT INTO items (id, label) VALUES (:1, :2)", (1, "first"))
            pipe.execute("INSERT INTO items (id, label) VALUES (:1, :2)", (1, "duplicate"))
            pipe.execute("INSERT INTO items (id, label) VALUES (:1, :2)", (2, "never_run"))
            pipe.results()
        assert exc_info.value.sqlstate == "23505"
        assert exc_info.value.vendor_code == 1
        assert tuple(o.status for o in driver.last_pipeline_outcomes) == ("completed", "error", "skipped")
        assert driver.transaction_events == ("BEGIN", "ROLLBACK")

    async_stub = _StubAsyncOracleClient()
    async_stub.queued_pipeline.append(mid_batch)
    async with AsyncOracleDriver(client=async_stub) as async_driver:
        with pytest.raises(IntegrityError) as aexc_info:
            async with async_driver.pipeline() as apipe:
                apipe.execute("INSERT INTO items (id, label) VALUES (:1, :2)", (1, "first"))
                apipe.execute("INSERT INTO items (id, label) VALUES (:1, :2)", (1, "duplicate"))
                apipe.execute("INSERT INTO items (id, label) VALUES (:1, :2)", (2, "never_run"))
                await apipe.results()
        assert aexc_info.value.sqlstate == "23505"
        assert tuple(o.status for o in async_driver.last_pipeline_outcomes) == ("completed", "error", "skipped")
        assert async_driver.transaction_events == ("BEGIN", "ROLLBACK")


@pytest.mark.parametrize(
    "scenario",
    ["auto_flush", "tail_flush", "caller_tx_clean", "caller_tx_exception"],
)
def test_sync_oracle_pipeline_exit_contract(scenario: str) -> None:
    """SyncOraclePipeline honors unified R1.14 flush, commit, discard, and caller tx ownership."""
    stub = _StubSyncOracleClient()
    with SyncOracleDriver(client=stub) as driver:
        if scenario == "auto_flush":
            with driver.pipeline() as pipe:
                pipe.execute("INSERT INTO items (id, label) VALUES (:1, :2)", (1, "auto"))
            assert driver.in_transaction is False
            assert driver.transaction_events == ("BEGIN", "COMMIT")
            assert [c for c in stub.calls if c[0] == "execute_pipeline_batch"] != []
        elif scenario == "tail_flush":
            with driver.pipeline() as pipe:
                pipe.execute("INSERT INTO items (id, label) VALUES (:1, :2)", (1, "first"))
                assert pipe.results() == [1]
                pipe.execute("INSERT INTO items (id, label) VALUES (:1, :2)", (2, "tail"))
            assert driver.in_transaction is False
            assert driver.transaction_events == ("BEGIN", "COMMIT")
            batch_calls = [c for c in stub.calls if c[0] == "execute_pipeline_batch"]
            assert len(batch_calls) == 2
        elif scenario == "caller_tx_clean":
            with driver.transaction():
                with driver.pipeline() as pipe:
                    pipe.execute("INSERT INTO items (id, label) VALUES (:1, :2)", (3, "nested_auto"))
                assert driver.in_transaction is True
                assert driver.transaction_events == ("BEGIN", "SAVEPOINT _vtb_pipe_1")
                assert len([c for c in stub.calls if c[0] == "execute_pipeline_batch"]) == 1
            assert driver.in_transaction is False
            assert driver.transaction_events == ("BEGIN", "SAVEPOINT _vtb_pipe_1", "COMMIT")
        else:
            with driver.transaction():
                with pytest.raises(RuntimeError, match="abort_inner"), driver.pipeline() as pipe:
                    pipe.execute("INSERT INTO items (id, label) VALUES (:1, :2)", (4, "discarded"))
                    raise RuntimeError("abort_inner")
                assert driver.in_transaction is True
                assert [c for c in stub.calls if c[0] == "execute_pipeline_batch"] == []
                assert driver.transaction_events == (
                    "BEGIN",
                    "SAVEPOINT _vtb_pipe_1",
                    "ROLLBACK TO SAVEPOINT _vtb_pipe_1",
                )
            assert list(driver.transaction_events) == [
                "BEGIN",
                "SAVEPOINT _vtb_pipe_1",
                "ROLLBACK TO SAVEPOINT _vtb_pipe_1",
                "COMMIT",
            ]


@pytest.mark.anyio
@pytest.mark.parametrize(
    "scenario",
    ["auto_flush", "tail_flush", "caller_tx_clean", "caller_tx_exception"],
)
async def test_async_oracle_pipeline_exit_contract(scenario: str) -> None:
    """AsyncOraclePipeline honors unified R1.14 flush, commit, discard, and caller tx ownership."""
    stub = _StubAsyncOracleClient()
    async with AsyncOracleDriver(client=stub) as driver:
        if scenario == "auto_flush":
            async with driver.pipeline() as pipe:
                pipe.execute("INSERT INTO items (id, label) VALUES (:1, :2)", (1, "auto"))
            assert driver.in_transaction is False
            assert driver.transaction_events == ("BEGIN", "COMMIT")
            assert [c for c in stub.calls if c[0] == "execute_pipeline_batch"] != []
        elif scenario == "tail_flush":
            async with driver.pipeline() as pipe:
                pipe.execute("INSERT INTO items (id, label) VALUES (:1, :2)", (1, "first"))
                assert await pipe.results() == [1]
                pipe.execute("INSERT INTO items (id, label) VALUES (:1, :2)", (2, "tail"))
            assert driver.in_transaction is False
            assert driver.transaction_events == ("BEGIN", "COMMIT")
            batch_calls = [c for c in stub.calls if c[0] == "execute_pipeline_batch"]
            assert len(batch_calls) == 2
        elif scenario == "caller_tx_clean":
            async with driver.transaction():
                async with driver.pipeline() as pipe:
                    pipe.execute("INSERT INTO items (id, label) VALUES (:1, :2)", (3, "nested_auto"))
                assert driver.in_transaction is True
                assert driver.transaction_events == ("BEGIN", "SAVEPOINT _vtb_pipe_1")
                assert len([c for c in stub.calls if c[0] == "execute_pipeline_batch"]) == 1
            assert driver.in_transaction is False
            assert driver.transaction_events == ("BEGIN", "SAVEPOINT _vtb_pipe_1", "COMMIT")
        else:
            async with driver.transaction():
                with pytest.raises(RuntimeError, match="abort_inner"):
                    async with driver.pipeline() as pipe:
                        pipe.execute("INSERT INTO items (id, label) VALUES (:1, :2)", (4, "discarded"))
                        raise RuntimeError("abort_inner")
                assert driver.in_transaction is True
                assert [c for c in stub.calls if c[0] == "execute_pipeline_batch"] == []
                assert driver.transaction_events == (
                    "BEGIN",
                    "SAVEPOINT _vtb_pipe_1",
                    "ROLLBACK TO SAVEPOINT _vtb_pipe_1",
                )
            assert list(driver.transaction_events) == [
                "BEGIN",
                "SAVEPOINT _vtb_pipe_1",
                "ROLLBACK TO SAVEPOINT _vtb_pipe_1",
                "COMMIT",
            ]


def test_oracledb_crate_ttc_pipeline_capability_probe() -> None:
    """Verify ORACLESQL_NATIVE_TTC_PIPELINE_SUPPORTED and QueryCancelledError availability."""
    assert _oracle.ORACLESQL_NATIVE_TTC_PIPELINE_SUPPORTED is False
    assert issubclass(QueryCancelledError, OperationalError)


@pytest.mark.parametrize(
    ("symbol_name", "target_module", "expected_module"),
    [
        ("SyncOracleDriver", "vertebra.db.adapters.oracle", "vertebra.db.adapters.oracle._sync"),
        ("SyncOracleDriver", "vertebra.db.adapters.oracle.driver", "vertebra.db.adapters.oracle._sync"),
        ("SyncOracleDriver", "vertebra.db.adapters.oracle._sync", "vertebra.db.adapters.oracle._sync"),
        ("AsyncOracleDriver", "vertebra.db.adapters.oracle", "vertebra.db.adapters.oracle._async"),
        ("AsyncOracleDriver", "vertebra.db.adapters.oracle.driver", "vertebra.db.adapters.oracle._async"),
        ("AsyncOracleDriver", "vertebra.db.adapters.oracle._async", "vertebra.db.adapters.oracle._async"),
        ("SyncOraclePipeline", "vertebra.db.adapters.oracle", "vertebra.db.adapters.oracle.pipeline"),
        ("SyncOraclePipeline", "vertebra.db.adapters.oracle.driver", "vertebra.db.adapters.oracle.pipeline"),
        ("SyncOraclePipeline", "vertebra.db.adapters.oracle.pipeline", "vertebra.db.adapters.oracle.pipeline"),
        ("AsyncOraclePipeline", "vertebra.db.adapters.oracle", "vertebra.db.adapters.oracle.pipeline"),
        ("AsyncOraclePipeline", "vertebra.db.adapters.oracle.driver", "vertebra.db.adapters.oracle.pipeline"),
        ("AsyncOraclePipeline", "vertebra.db.adapters.oracle.pipeline", "vertebra.db.adapters.oracle.pipeline"),
        ("SyncOraclePool", "vertebra.db.adapters.oracle", "vertebra.db.adapters.oracle._sync"),
        ("SyncOraclePool", "vertebra.db.adapters.oracle.driver", "vertebra.db.adapters.oracle._sync"),
        ("SyncOraclePool", "vertebra.db.adapters.oracle._sync", "vertebra.db.adapters.oracle._sync"),
        ("AsyncOraclePool", "vertebra.db.adapters.oracle", "vertebra.db.adapters.oracle._async"),
        ("AsyncOraclePool", "vertebra.db.adapters.oracle.driver", "vertebra.db.adapters.oracle._async"),
        ("AsyncOraclePool", "vertebra.db.adapters.oracle._async", "vertebra.db.adapters.oracle._async"),
    ],
)
def test_oracle_driver_and_pipeline_resolution_identity(
    symbol_name: str,
    target_module: str,
    expected_module: str,
) -> None:
    """Verify Oracle sync/async driver, pool, and pipeline classes resolve identically across import paths."""
    mod = importlib.import_module(target_module)
    exp_mod = importlib.import_module(expected_module)
    assert getattr(mod, symbol_name) is getattr(exp_mod, symbol_name)


@pytest.mark.parametrize(
    "removed_alias",
    [
        "decode_oson",
        "decode_oson_payload",
        "decode_tns_packet",
        "decode_ttc_session_attrs",
        "decode_vector_binary",
        "decode_vector_f32",
        "decode_vector_f64",
        "decode_vector_i8",
        "encode_oson",
        "encode_oson_payload",
        "encode_tns_connect_packet",
        "encode_tns_data_packet",
        "encode_ttc_session_attrs",
        "encode_vector_binary",
        "encode_vector_f32",
        "encode_vector_f64",
        "encode_vector_i8",
    ],
)
def test_oracle_removed_alias_shims_raise_attribute_error(removed_alias: str) -> None:
    """Verify removed module-level Oracle codec alias shims are absent from oracle and oracle.core."""
    with pytest.raises(AttributeError):
        getattr(oracle_core_mod, removed_alias)
    with pytest.raises(AttributeError):
        getattr(oracle_pkg, removed_alias)


def test_oracle_driver_defers_native_extension_import() -> None:
    """Verify importing vertebra.db.adapters.oracle.driver does not eagerly import vertebra._oracle."""
    saved = {
        name: sys.modules.pop(name)
        for name in list(sys.modules)
        if name == "vertebra._oracle"
        or name == "vertebra.db.adapters.oracle.driver"
        or name == "vertebra.db.adapters.oracle._sync"
        or name == "vertebra.db.adapters.oracle._async"
    }
    try:
        drv_mod = importlib.import_module("vertebra.db.adapters.oracle.driver")
        assert "vertebra._oracle" not in sys.modules
        flag = drv_mod.ORACLESQL_NATIVE_TTC_PIPELINE_SUPPORTED
        assert isinstance(flag, bool)
    finally:
        sys.modules.update(saved)
