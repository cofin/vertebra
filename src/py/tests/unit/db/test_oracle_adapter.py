"""Offline unit tests for the native Oracle adapter, dialer, codecs, pools, and pipelines."""

from __future__ import annotations

import asyncio
import datetime
import gc
import importlib
import sys
import time
import uuid
from collections.abc import Awaitable, Generator, Mapping, Sequence
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

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
    _StubAsyncOracleCursor,
    _StubAsyncOraclePool,
    _StubSyncOracleClient,
    _StubSyncOracleCursor,
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
    NotFoundError,
    NotSupportedError,
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

    assert sync_stub.calls[0][0] == "fetch_dicts"
    assert sync_stub.cursor_calls == []
    query_calls = [payload for method, payload in sync_stub.calls if method == "query"]
    assert len(query_calls) == 1
    coerced = query_calls[0]["params"]
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
    assert [c[0] for c in sync_stub.calls] == ["fetch_dicts", "execute", "fetch_rows", "execute"]
    assert sync_stub.cursor_calls == []
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
    assert [c[0] for c in async_stub.calls] == ["fetch_dicts", "execute", "fetch_dicts", "execute"]
    assert async_stub.cursor_calls == []
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


@pytest.mark.parametrize(
    ("cls", "is_async"),
    [
        (_oracle.SyncOracleClient, False),
        (_oracle.AsyncOracleClient, True),
        (_oracle.SyncOraclePool, False),
        (_oracle.AsyncOraclePool, True),
    ],
)
def test_relay_constructor_accepts_flag(cls: type[Any], is_async: bool) -> None:
    """Verify each native Oracle constructor accepts enable_reset_relay=False without connecting."""
    instance = cls(
        host="nonexistent.invalid",
        port=1521,
        service_name="FREEPDB1",
        user="hr",
        password="test-token-placeholder",
        enable_reset_relay=False,
    )
    assert instance.closed is False
    if is_async:

        async def _close() -> None:
            await instance.close()

        asyncio.run(_close())
    else:
        instance.close()
    assert instance.closed is True


@pytest.mark.parametrize(
    ("cls", "is_async"),
    [
        (_oracle.SyncOracleClient, False),
        (_oracle.AsyncOracleClient, True),
        (_oracle.SyncOraclePool, False),
        (_oracle.AsyncOraclePool, True),
    ],
)
def test_relay_constructor_default(cls: type[Any], is_async: bool) -> None:
    """Verify each native Oracle constructor succeeds when enable_reset_relay is omitted."""
    instance = cls(
        host="nonexistent.invalid",
        port=1521,
        service_name="FREEPDB1",
        user="hr",
        password="test-token-placeholder",
    )
    assert instance.closed is False
    if is_async:

        async def _close() -> None:
            await instance.close()

        asyncio.run(_close())
    else:
        instance.close()
    assert instance.closed is True


@pytest.mark.parametrize(
    "cls",
    [
        _oracle.SyncOracleClient,
        _oracle.AsyncOracleClient,
        _oracle.SyncOraclePool,
        _oracle.AsyncOraclePool,
    ],
)
def test_relay_constructor_wrong_type(cls: type[Any]) -> None:
    """Verify passing a non-bool enable_reset_relay raises TypeError."""
    with pytest.raises(TypeError):
        cls(
            host="nonexistent.invalid",
            port=1521,
            service_name="FREEPDB1",
            user="hr",
            password="test-token-placeholder",
            enable_reset_relay="no",
        )


def test_oracle_config_relay_default() -> None:
    """Verify OracleConfig.enable_reset_relay defaults to True and propagates to kwargs."""
    cfg = OracleConfig()
    assert cfg.enable_reset_relay is True
    assert cfg.to_client_kwargs()["enable_reset_relay"] is True
    resolved_cfg, pool_kw = OracleConfig.resolve_pool_kwargs(cfg, {})
    assert resolved_cfg.enable_reset_relay is True
    assert pool_kw["enable_reset_relay"] is True


@pytest.mark.parametrize("enable_reset_relay", [True, False])
def test_oracle_config_relay_client_kwargs(enable_reset_relay: bool) -> None:
    """Verify OracleConfig.to_client_kwargs emits enable_reset_relay and keeps pool_key unchanged."""
    cfg = OracleConfig(enable_reset_relay=enable_reset_relay)
    assert cfg.enable_reset_relay is enable_reset_relay
    assert cfg.to_client_kwargs()["enable_reset_relay"] is enable_reset_relay
    assert cfg.pool_key() == OracleConfig(enable_reset_relay=not enable_reset_relay).pool_key()


@pytest.mark.parametrize("enable_reset_relay", [True, False])
def test_oracle_config_relay_pool_kwargs(enable_reset_relay: bool) -> None:
    """Verify resolve_pool_kwargs preserves enable_reset_relay from config and overrides."""
    resolved_from_cfg, kw_from_cfg = OracleConfig.resolve_pool_kwargs(
        OracleConfig(enable_reset_relay=enable_reset_relay),
        {},
    )
    assert resolved_from_cfg.enable_reset_relay is enable_reset_relay
    assert kw_from_cfg["enable_reset_relay"] is enable_reset_relay

    resolved_from_override, kw_from_override = OracleConfig.resolve_pool_kwargs(
        None,
        {"enable_reset_relay": enable_reset_relay},
    )
    assert resolved_from_override.enable_reset_relay is enable_reset_relay
    assert kw_from_override["enable_reset_relay"] is enable_reset_relay


@pytest.mark.parametrize("enable_reset_relay", [True, False])
def test_oracle_config_relay_native_construct(enable_reset_relay: bool) -> None:
    """Verify native Oracle clients and pools construct from OracleConfig kwargs for both relay states."""
    cfg = OracleConfig(
        host="nonexistent.invalid",
        port=1521,
        service_name="FREEPDB1",
        user="hr",
        password="test-token-placeholder",
        enable_reset_relay=enable_reset_relay,
    )
    client_kw = cfg.to_client_kwargs()
    sync_client = _oracle.SyncOracleClient(**client_kw)
    assert sync_client.closed is False
    sync_client.close()
    assert sync_client.closed is True

    async_client = _oracle.AsyncOracleClient(**client_kw)
    assert async_client.closed is False

    async def _close_async(obj: Any) -> None:
        await obj.close()

    asyncio.run(_close_async(async_client))
    assert async_client.closed is True

    _, pool_kw = OracleConfig.resolve_pool_kwargs(cfg, {})
    sync_pool = _oracle.SyncOraclePool(**pool_kw)
    assert sync_pool.closed is False
    sync_pool.close()
    assert sync_pool.closed is True

    async_pool = _oracle.AsyncOraclePool(**pool_kw)
    assert async_pool.closed is False
    asyncio.run(_close_async(async_pool))
    assert async_pool.closed is True


@pytest.mark.parametrize("enable_reset_relay", [True, False])
def test_oracle_config_relay_from_url(enable_reset_relay: bool) -> None:
    """Verify OracleConfig.from_url accepts enable_reset_relay keyword override and defaults to True."""
    default_cfg = OracleConfig.from_url("oracle://u:test-token-placeholder@h:1521/svc")
    assert default_cfg.enable_reset_relay is True

    overridden_cfg = OracleConfig.from_url(
        "oracle://u:test-token-placeholder@h:1521/svc",
        enable_reset_relay=enable_reset_relay,
    )
    assert overridden_cfg.enable_reset_relay is enable_reset_relay
    assert overridden_cfg.to_client_kwargs()["enable_reset_relay"] is enable_reset_relay


@pytest.mark.parametrize("vector_return_format", ["list", "vector_view", "bytes"])
def test_sync_cursor_creation_options_and_validation(vector_return_format: str) -> None:
    """Verify SyncOracleClient.cursor() zero-I/O creation, options, fetch controls, and context manager."""
    client = _oracle.SyncOracleClient(
        host="127.0.0.1",
        port=_reserve_closed_local_port(),
        service_name="FREEPDB1",
        user="hr",
        password="test-token-placeholder",
        connect_timeout=0.5,
    )
    try:
        with pytest.raises(InterfaceError, match="vector_return_format"):
            client.cursor(vector_return_format="x")

        cur = client.cursor(
            enable_uuid_binary=True,
            fetch_decimals=True,
            vector_return_format=vector_return_format,
            enable_lowercase_column_names=False,
            lazy_begin=False,
        )
        assert isinstance(cur, _oracle.SyncOracleCursor)
        assert cur.description is None
        assert cur.rowcount == -1
        assert cur.lastrowid is None
        assert cur.arraysize == 1
        assert cur.touches_session_context is False

        cur.arraysize = 16
        assert cur.arraysize == 16
        for bad_size in (0, -1, -100):
            with pytest.raises(ProgrammingError, match="arraysize"):
                cur.arraysize = bad_size

        assert cur.fetchone() is None
        assert cur.fetchmany(0) == []
        assert cur.fetchmany() == []
        assert cur.fetchall() == []
        with pytest.raises(ProgrammingError, match="non-negative"):
            cur.fetchmany(-1)

        with cur as entered:
            assert entered is cur
        with pytest.raises(InterfaceError, match="closed"):
            cur.fetchone()
    finally:
        client.close()


def test_sync_cursor_closed_bind_validation_and_session_flag() -> None:
    """Verify SyncOracleCursor closed errors, bind InterfaceError mapping, and session flag tracking."""
    client = _oracle.SyncOracleClient(
        host="127.0.0.1",
        port=_reserve_closed_local_port(),
        service_name="FREEPDB1",
        user="hr",
        password="test-token-placeholder",
        connect_timeout=0.5,
    )
    try:
        cur = client.cursor()
        assert cur.executemany("BEGIN DBMS_SESSION.SET_IDENTIFIER(:1); END;", []) is cur
        assert cur.rowcount == 0
        assert cur.description is None
        assert cur.touches_session_context is True

        assert cur.executemany("INSERT INTO t (id) VALUES (:1)", []) is cur
        assert cur.rowcount == 0
        assert cur.touches_session_context is False

        invalid_params: list[Any] = ["not-a-seq", b"bytes", 42, {123: "v"}]
        for bad_params in invalid_params:
            with pytest.raises(InterfaceError):
                cur.execute("SELECT :1 FROM dual", bad_params)
            assert cur.description is None
            assert cur.rowcount == -1
            assert cur.lastrowid is None
            assert cur.touches_session_context is False

        with pytest.raises((ConnectionFailure, OperationalError)):
            cur.execute("SELECT 1 AS a, 'x' AS b FROM dual")
        assert cur.description is None
        assert cur.rowcount == -1
        assert cur.lastrowid is None
        assert cur.touches_session_context is False

        cur.close()
        cur.close()
        with pytest.raises(InterfaceError, match="closed"):
            cur.execute("SELECT 1 FROM dual")
        with pytest.raises(InterfaceError, match="closed"):
            cur.executemany("INSERT INTO t VALUES (:1)", [(1,)])
        with pytest.raises(InterfaceError, match="closed"):
            cur.fetchone()
        with pytest.raises(InterfaceError, match="closed"):
            cur.fetchmany(1)
        with pytest.raises(InterfaceError, match="closed"):
            cur.fetchall()
    finally:
        client.close()


def test_sync_cursor_lazy_begin_and_lease_lifecycle() -> None:
    """Verify SyncOracleClient lazy_begin transaction opening and LeaseGuard attach/release/drop."""
    client = _oracle.SyncOracleClient(
        host="127.0.0.1",
        port=_reserve_closed_local_port(),
        service_name="FREEPDB1",
        user="hr",
        password="test-token-placeholder",
        connect_timeout=0.5,
    )
    try:
        default_cur = client.cursor()
        with pytest.raises((ConnectionFailure, OperationalError)):
            default_cur.execute("INSERT INTO t VALUES (1)")
        assert client.in_transaction is False

        tx_cur = client.cursor(lazy_begin=True)
        with pytest.raises((ConnectionFailure, OperationalError)):
            tx_cur.execute("INSERT INTO t VALUES (1)")
        assert client.in_transaction is True

        client.commit()
        assert client.in_transaction is False

        with pytest.raises((ConnectionFailure, OperationalError)):
            tx_cur.execute("INSERT INTO t VALUES (2)")
        assert client.in_transaction is True
        client.rollback()
        assert client.in_transaction is False

        calls: list[str] = []
        client.attach_lease(lambda: calls.append("first"))
        client.release_lease()
        client.release_lease()
        assert calls == ["first"]

        client.attach_lease(lambda: calls.append("second"))
        client.attach_lease(lambda: calls.append("third"))
        assert calls == ["first", "second"]

        client.close()
        assert calls == ["first", "second", "third"]

        with pytest.raises(InterfaceError, match="closed"):
            tx_cur.execute("INSERT INTO t VALUES (3)")
        assert tx_cur.description is None
        assert tx_cur.rowcount == -1
    finally:
        client.close()

    drop_calls: list[int] = []
    ephemeral = _oracle.SyncOracleClient(
        host="127.0.0.1",
        port=_reserve_closed_local_port(),
        service_name="FREEPDB1",
        user="hr",
        password="test-token-placeholder",
    )
    ephemeral.attach_lease(lambda: drop_calls.append(1))
    del ephemeral
    gc.collect()
    assert drop_calls == [1]


@pytest.mark.anyio
@pytest.mark.parametrize("vector_return_format", ["list", "vector_view", "bytes"])
async def test_async_cursor_creation_options_and_validation(vector_return_format: str) -> None:
    """Verify AsyncOracleClient.cursor() zero-I/O creation, options, and fetch controls."""
    client = _oracle.AsyncOracleClient(
        host="127.0.0.1",
        port=_reserve_closed_local_port(),
        service_name="FREEPDB1",
        user="hr",
        password="test-token-placeholder",
        connect_timeout=0.5,
    )
    try:
        with pytest.raises(InterfaceError, match="vector_return_format"):
            client.cursor(vector_return_format="x")

        cur = client.cursor(
            enable_uuid_binary=True,
            fetch_decimals=True,
            vector_return_format=vector_return_format,
            enable_lowercase_column_names=False,
            lazy_begin=False,
        )
        assert isinstance(cur, _oracle.AsyncOracleCursor)
        assert cur.description is None
        assert cur.rowcount == -1
        assert cur.lastrowid is None
        assert cur.arraysize == 1
        assert cur.touches_session_context is False

        cur.arraysize = 8
        assert cur.arraysize == 8
        for bad_size in (0, -2):
            with pytest.raises(ProgrammingError, match="arraysize"):
                cur.arraysize = bad_size

        assert await cur.fetchone() is None
        assert await cur.fetchmany(0) == []
        assert await cur.fetchmany() == []
        assert await cur.fetchall() == []
        with pytest.raises(ProgrammingError, match="non-negative"):
            await cur.fetchmany(-1)

        await cur.close()
        await cur.close()
        with pytest.raises(InterfaceError, match="closed"):
            await cur.fetchone()
    finally:
        await client.close()


@pytest.mark.anyio
async def test_async_cursor_closed_bind_validation_and_session_flag() -> None:
    """Verify AsyncOracleCursor closed checks, bind InterfaceError mapping, and session flag."""
    client = _oracle.AsyncOracleClient(
        host="127.0.0.1",
        port=_reserve_closed_local_port(),
        service_name="FREEPDB1",
        user="hr",
        password="test-token-placeholder",
        connect_timeout=0.5,
    )
    try:
        cur = client.cursor()
        assert await cur.executemany("BEGIN DBMS_SESSION.SET_IDENTIFIER(:1); END;", []) is cur
        assert cur.rowcount == 0
        assert cur.description is None
        assert cur.touches_session_context is True

        assert await cur.executemany("INSERT INTO t (id) VALUES (:1)", []) is cur
        assert cur.rowcount == 0
        assert cur.touches_session_context is False

        invalid_params: list[Any] = ["not-a-seq", b"bytes", 42, {123: "v"}]
        for bad_params in invalid_params:
            with pytest.raises(InterfaceError):
                await cur.execute("SELECT :1 FROM dual", bad_params)
            assert cur.description is None
            assert cur.rowcount == -1
            assert cur.lastrowid is None
            assert cur.touches_session_context is False

        with pytest.raises((ConnectionFailure, OperationalError)):
            await cur.execute("SELECT 1 AS a, 'x' AS b FROM dual")
        assert cur.description is None
        assert cur.rowcount == -1
        assert cur.lastrowid is None
        assert cur.touches_session_context is False

        await cur.close()
        with pytest.raises(InterfaceError, match="closed"):
            await cur.execute("SELECT 1 FROM dual")
        with pytest.raises(InterfaceError, match="closed"):
            await cur.executemany("INSERT INTO t VALUES (:1)", [(1,)])
        with pytest.raises(InterfaceError, match="closed"):
            await cur.fetchmany(1)
        with pytest.raises(InterfaceError, match="closed"):
            await cur.fetchall()
    finally:
        await client.close()


@pytest.mark.anyio
async def test_async_cursor_lazy_begin_and_lease_lifecycle() -> None:
    """Verify AsyncOracleClient lazy_begin transaction opening and LeaseGuard attach/release/drop."""
    client = _oracle.AsyncOracleClient(
        host="127.0.0.1",
        port=_reserve_closed_local_port(),
        service_name="FREEPDB1",
        user="hr",
        password="test-token-placeholder",
        connect_timeout=0.5,
    )
    try:
        default_cur = client.cursor()
        with pytest.raises((ConnectionFailure, OperationalError)):
            await default_cur.execute("INSERT INTO t VALUES (1)")
        assert client.in_transaction is False

        tx_cur = client.cursor(lazy_begin=True)
        with pytest.raises((ConnectionFailure, OperationalError)):
            await tx_cur.execute("INSERT INTO t VALUES (1)")
        assert client.in_transaction is True

        await client.commit()
        assert client.in_transaction is False

        with pytest.raises((ConnectionFailure, OperationalError)):
            await tx_cur.execute("INSERT INTO t VALUES (2)")
        assert client.in_transaction is True
        await client.rollback()
        assert client.in_transaction is False

        calls: list[str] = []
        client.attach_lease(lambda: calls.append("first"))
        client.release_lease()
        client.release_lease()
        assert calls == ["first"]

        client.attach_lease(lambda: calls.append("second"))
        client.attach_lease(lambda: calls.append("third"))
        assert calls == ["first", "second"]

        await client.close()
        assert calls == ["first", "second", "third"]

        with pytest.raises(InterfaceError, match="closed"):
            await tx_cur.execute("INSERT INTO t VALUES (3)")
        assert tx_cur.description is None
        assert tx_cur.rowcount == -1
    finally:
        await client.close()

    drop_calls: list[int] = []
    ephemeral = _oracle.AsyncOracleClient(
        host="127.0.0.1",
        port=_reserve_closed_local_port(),
        service_name="FREEPDB1",
        user="hr",
        password="test-token-placeholder",
    )
    ephemeral.attach_lease(lambda: drop_calls.append(1))
    del ephemeral
    gc.collect()
    assert drop_calls == [1]


def test_cursor_out_bind_var_and_surface() -> None:
    """Verify OracleOutBindVar constructor, attributes, getvalue/setvalue, and converter behavior."""
    var = _oracle.OracleOutBindVar("NUMBER", arraysize=0, outconverter=str)
    assert type(var).__module__ == "vertebra._oracle"
    assert var.type_code == "NUMBER"
    assert var.size == 0
    assert var.arraysize == 1
    assert var.outconverter is str
    assert var.values == [None]
    assert var.getvalue(0) is None

    same_list = var.values
    var.setvalue(2, 5)
    assert var.values is same_list
    assert var.values == [None, None, 5]
    assert var.getvalue(2) == "5"
    assert var.getvalue(0) is None
    with pytest.raises(IndexError):
        var.getvalue(9)

    var.type_code = "VARCHAR2"
    var.size = 64
    var.arraysize = 3
    var.outconverter = None
    var.values = ["a", "b", "c"]
    assert var.type_code == "VARCHAR2"
    assert var.size == 64
    assert var.arraysize == 3
    assert var.outconverter is None
    assert var.getvalue(1) == "b"


@pytest.mark.anyio
async def test_cursor_out_bind_cursor_var_and_size_noops() -> None:
    """Verify SyncOracleCursor and AsyncOracleCursor var(), setinputsizes(), setoutputsize(), and out_binds."""
    sync_client = _oracle.SyncOracleClient(
        host="127.0.0.1",
        port=_reserve_closed_local_port(),
        service_name="FREEPDB1",
        user="hr",
        password="test-token-placeholder",
        connect_timeout=0.5,
    )
    async_client = _oracle.AsyncOracleClient(
        host="127.0.0.1",
        port=_reserve_closed_local_port(),
        service_name="FREEPDB1",
        user="hr",
        password="test-token-placeholder",
        connect_timeout=0.5,
    )
    try:
        sync_cur = sync_client.cursor()
        assert sync_cur.out_binds is None
        sync_cur_any: Any = sync_cur
        with pytest.raises(AttributeError):
            sync_cur_any.out_binds = {}
        sync_var = sync_cur.var(int)
        assert isinstance(sync_var, _oracle.OracleOutBindVar)
        assert sync_var.values == [None]
        assert sync_var.arraysize == 1
        assert sync_var.size == 0
        sync_cur.setinputsizes(10, 20)
        sync_cur.setoutputsize(100)
        sync_cur.setoutputsize(100, 1)

        sync_cur.close()
        with pytest.raises(InterfaceError, match="closed"):
            sync_cur.var(int)

        async_cur = async_client.cursor()
        assert async_cur.out_binds is None
        async_var = async_cur.var(int, size=8, arraysize=2, outconverter=str)
        assert isinstance(async_var, _oracle.OracleOutBindVar)
        assert async_var.size == 8
        assert async_var.arraysize == 2
        assert async_var.values == [None, None]
        async_cur.setinputsizes(10)
        async_cur.setoutputsize(200, 2)

        await async_cur.close()
        with pytest.raises(InterfaceError, match="closed"):
            async_cur.var(int)
    finally:
        sync_client.close()
        await async_client.close()


@pytest.mark.parametrize("param_shape", ["named", "positional", "duck_typed"])
@pytest.mark.anyio
async def test_cursor_out_bind_executemany_rejected(param_shape: str) -> None:
    """Verify executemany rejects out-bind variables with NotSupportedError on sync and async cursors."""

    class _DuckOutVar:
        def __init__(self) -> None:
            self.type_code = "NUMBER"

        def setvalue(self, pos: int, value: Any) -> None:
            self._last = (pos, value)

    sync_client = _oracle.SyncOracleClient(
        host="127.0.0.1",
        port=_reserve_closed_local_port(),
        service_name="FREEPDB1",
        user="hr",
        password="test-token-placeholder",
        connect_timeout=0.5,
    )
    async_client = _oracle.AsyncOracleClient(
        host="127.0.0.1",
        port=_reserve_closed_local_port(),
        service_name="FREEPDB1",
        user="hr",
        password="test-token-placeholder",
        connect_timeout=0.5,
    )
    try:
        sync_cur = sync_client.cursor()
        async_cur = async_client.cursor()
        var_obj: Any = _DuckOutVar() if param_shape == "duck_typed" else sync_cur.var("NUMBER")
        batch: list[Any] = [{"out": var_obj}] if param_shape == "named" else [(1, var_obj)]

        with pytest.raises(NotSupportedError):
            sync_cur.executemany("INSERT INTO t (a) VALUES (:1)", batch)
        with pytest.raises(NotSupportedError):
            await async_cur.executemany("INSERT INTO t (a) VALUES (:1)", batch)
    finally:
        sync_client.close()
        await async_client.close()


@pytest.mark.parametrize(
    ("sql", "params_factory", "out_binds"),
    [
        (
            "BEGIN :out := :a + 1; END;",
            lambda cur: {"a": 41, "out": cur.var("NUMBER")},
            None,
        ),
        (
            "BEGIN :2 := :1 + 1; END;",
            lambda cur: [41, cur.var("NUMBER")],
            None,
        ),
        (
            "INSERT INTO t (name) VALUES (:name) RETURNING id INTO :id",
            lambda cur: {"name": "x", "id": cur.var("NUMBER")},
            None,
        ),
        (
            "BEGIN :out := 1; END;",
            lambda cur: {"out": cur.var("STRING")},
            {"out": "NUMBER"},
        ),
        (
            "BEGIN :out := 1; END;",
            lambda cur: {"OUT": cur.var("NUMBER")},
            None,
        ),
        (
            "SELECT 1 FROM dual",
            lambda _cur: (),
            None,
        ),
    ],
)
@pytest.mark.anyio
async def test_cursor_out_bind_execute_accepts_vars_and_keyword_hints(
    sql: str,
    params_factory: Any,
    out_binds: dict[str, str] | list[str] | None,
) -> None:
    """Verify SyncOracleCursor and AsyncOracleCursor execute accept out-bind vars and keyword-only out_binds."""
    sync_client = _oracle.SyncOracleClient(
        host="127.0.0.1",
        port=_reserve_closed_local_port(),
        service_name="FREEPDB1",
        user="hr",
        password="test-token-placeholder",
        connect_timeout=0.5,
    )
    async_client = _oracle.AsyncOracleClient(
        host="127.0.0.1",
        port=_reserve_closed_local_port(),
        service_name="FREEPDB1",
        user="hr",
        password="test-token-placeholder",
        connect_timeout=0.5,
    )
    try:
        sync_cur = sync_client.cursor()
        with pytest.raises((ConnectionFailure, OperationalError)):
            sync_cur.execute(sql, params_factory(sync_cur), out_binds=out_binds)
        assert sync_cur.out_binds is None

        async_cur = async_client.cursor()
        with pytest.raises((ConnectionFailure, OperationalError)):
            await async_cur.execute(sql, params_factory(async_cur), out_binds=out_binds)
        assert async_cur.out_binds is None
    finally:
        sync_client.close()
        await async_client.close()


@pytest.mark.anyio
async def test_cursor_out_bind_execute_keyword_only_and_validation() -> None:
    """Verify execute rejects positional out_binds with TypeError and string out_binds with InterfaceError."""
    sync_client = _oracle.SyncOracleClient(
        host="127.0.0.1",
        port=_reserve_closed_local_port(),
        service_name="FREEPDB1",
        user="hr",
        password="test-token-placeholder",
        connect_timeout=0.5,
    )
    async_client = _oracle.AsyncOracleClient(
        host="127.0.0.1",
        port=_reserve_closed_local_port(),
        service_name="FREEPDB1",
        user="hr",
        password="test-token-placeholder",
        connect_timeout=0.5,
    )
    try:
        sync_cur = sync_client.cursor()
        sync_exec: Any = sync_cur.execute
        with pytest.raises(TypeError):
            sync_exec("SELECT 1 FROM dual", (), ["NUMBER"])
        with pytest.raises(InterfaceError, match="out_binds"):
            sync_cur.execute("SELECT 1 FROM dual", (), out_binds="NUMBER")

        async_cur = async_client.cursor()
        async_exec: Any = async_cur.execute
        with pytest.raises(TypeError):
            await async_exec("SELECT 1 FROM dual", (), ["NUMBER"])
        with pytest.raises(InterfaceError, match="out_binds"):
            await async_cur.execute("SELECT 1 FROM dual", (), out_binds="NUMBER")
    finally:
        sync_client.close()
        await async_client.close()


def test_cursor_out_bind_no_dbapi_import_in_crate() -> None:
    """Verify the Oracle Rust crate holds no reference to vertebra.db.dbapi or dbapi."""
    crate_src = Path(__file__).resolve().parents[4] / "rs" / "vertebra-oracle" / "src"
    assert crate_src.is_dir()
    matches: list[str] = []
    for rs_file in sorted(crate_src.rglob("*.rs")):
        text = rs_file.read_text(encoding="utf-8")
        if "dbapi" in text or "vertebra.db.dbapi" in text:
            matches.append(str(rs_file))
    assert matches == []


class _RecordingLeaseGrant:
    """Lease grant double with slot_index and generation attributes."""

    def __init__(self, slot_index: int, generation: int = 1) -> None:
        self.slot_index = slot_index
        self.generation = generation


class _RecordingLeaseBroker:
    """Recording broker double asserting checkout-level lease acquire and release counts."""

    def __init__(self, *, fail_acquire: BaseException | None = None) -> None:
        self.acquire_calls: list[tuple[int, int, int]] = []
        self.release_calls: list[tuple[int, int]] = []
        self._fail_acquire = fail_acquire
        self._next_slot = 0

    def acquire(
        self,
        owner_pid: int,
        timeout_ms: int = 5000,
        bind_key_hash: int = 0,
    ) -> _RecordingLeaseGrant:
        """Record an acquire attempt and return a grant or raise configured failure."""
        self.acquire_calls.append((owner_pid, timeout_ms, bind_key_hash))
        if self._fail_acquire is not None:
            raise self._fail_acquire
        self._next_slot += 1
        return _RecordingLeaseGrant(self._next_slot)

    def release(self, slot_index: int, generation: int) -> None:
        """Record a release call for the given slot and generation."""
        self.release_calls.append((slot_index, generation))


@pytest.mark.parametrize(
    ("lowercase", "expected_rows"),
    [
        (True, [{"a": 1}]),
        (False, [{"A": 1}]),
    ],
)
def test_sync_oracle_driver_cursor_select_and_casing(
    lowercase: bool,
    expected_rows: list[dict[str, Any]],
) -> None:
    """Verify SyncOracleDriver.select routes through cursor with cached _cursor_options."""
    stub = _StubSyncOracleClient()
    stub.queued_results.append(_oracle.OracleQueryResult([_oracle.OracleColumn("A", "NUMBER")], [(1,)], rowcount=1))
    cfg = OracleConfig(
        driver_features=OracleDriverFeatures(
            enable_lowercase_column_names=lowercase,
            enable_uuid_binary=False,
            fetch_decimals=True,
            vector_return_format="array",
        )
    )
    driver = SyncOracleDriver(cfg, client=stub)
    opts = driver._cursor_options()
    assert driver._cursor_options() is opts
    assert opts == {
        "enable_uuid_binary": False,
        "fetch_decimals": True,
        "vector_return_format": "array",
        "enable_lowercase_column_names": lowercase,
    }
    assert driver.select("SELECT 1 AS a FROM dual") == expected_rows
    assert stub.cursor_calls == []
    fetch_calls = [payload for kind, payload in stub.calls if kind == "fetch_dicts"]
    assert len(fetch_calls) == 1
    assert fetch_calls[0]["enable_lowercase_column_names"] is lowercase
    assert fetch_calls[0]["fetch_decimals"] is True
    driver.close()


def test_sync_oracle_driver_execute_dml_ddl_tuples_out_bind_and_errors() -> None:
    """Verify DML rowcount, DDL empty description, tuples, out-bind population, and ORA-00001 IntegrityError."""
    stub = _StubSyncOracleClient()
    stub.queued_results.extend(
        [
            _oracle.OracleQueryResult(
                [_oracle.OracleColumn("A", "NUMBER"), _oracle.OracleColumn("B", "NUMBER")],
                [(1, 2)],
                rowcount=1,
            ),
            _oracle.OracleQueryResult([], [], rowcount=2),
            _oracle.OracleQueryResult([], [], rowcount=0),
            _oracle.OracleQueryResult([], [], rowcount=1, out_binds={"out": 99}),
        ]
    )
    dup_err = IntegrityError("ORA-00001: unique constraint violated")
    dup_err.vendor_code = 1
    stub.queued_results.append(dup_err)

    driver = SyncOracleDriver(client=stub)
    rows, names = driver.execute_with_description("SELECT 1 AS a, 2 AS b FROM dual")
    assert rows == [(1, 2)]
    assert names == ("a", "b")

    assert driver.execute("UPDATE items SET name = :1 WHERE id IN (1, 2)", ("updated",)) == 2
    assert driver.execute_with_description("CREATE TABLE temp_items (id NUMBER)") == ([], ())

    out_var = _oracle.OracleOutBindVar("NUMBER")
    assert driver.execute("BEGIN :out := 99; END;", {"out": out_var}) == 1
    assert out_var.getvalue() == 99
    assert driver._last_out_binds == {"out": 99}
    assert driver.out_binds == {"out": 99}

    with pytest.raises(IntegrityError, match="ORA-00001"):
        driver.execute("INSERT INTO items (id) VALUES (1)")
    driver.close()


@pytest.mark.parametrize("lazy_begin", [False, True])
def test_sync_oracle_driver_cursor_and_checkout_cursor_contracts(lazy_begin: bool) -> None:
    """Verify cursor() on pooled/unpooled/tx drivers and checkout_cursor() lifecycle."""
    single_stub = _StubSyncOracleClient()
    single_driver = SyncOracleDriver(client=single_stub)
    single_cur = single_driver.cursor(lazy_begin=lazy_begin)
    assert single_stub.cursor_calls[-1]["lazy_begin"] is lazy_begin
    single_cur.close()
    single_driver.close()

    pool = SyncOraclePool(native_pool=_StubSyncOraclePool(min_size=0, max_size=2))
    pooled_driver = SyncOracleDriver(pool=pool)
    with pytest.raises(
        InterfaceError,
        match=r"cursor\(\) needs an open transaction on a pooled Oracle driver; use checkout_cursor\(\)",
    ):
        pooled_driver.cursor(lazy_begin=lazy_begin)

    with pooled_driver.checkout_cursor(lazy_begin=lazy_begin) as cur:
        stub_cur = cast("_StubSyncOracleCursor", cur)
        stub_cur._client.queued_results.append(
            _oracle.OracleQueryResult([_oracle.OracleColumn("VAL", "NUMBER")], [(1,)], rowcount=1)
        )
        cur.execute("SELECT 1 FROM dual")
        assert cur.fetchall() == [(1,)]
        assert stub_cur.lazy_begin is lazy_begin
        assert pool.borrowed_size == 1
    assert stub_cur.closed is True
    assert pool.borrowed_size == 0
    assert pool.idle_size == 1

    pooled_driver.begin()
    try:
        tx_cur = cast("_StubSyncOracleCursor", pooled_driver.cursor(lazy_begin=lazy_begin))
        assert tx_cur._client is pooled_driver.client
        assert tx_cur.lazy_begin is lazy_begin
        tx_cur.close()
    finally:
        pooled_driver.rollback()
    assert pool.borrowed_size == 0
    pooled_driver.close()
    pool.close()


def test_sync_oracle_driver_session_context_only_tracked_when_flagged(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify track_session_context_sql runs only when cursor.touches_session_context is true."""
    stub = _StubSyncOracleClient()
    stub.queued_results.extend(
        [
            _oracle.OracleQueryResult([_oracle.OracleColumn("A", "NUMBER")], [(1,)], rowcount=1),
            _oracle.OracleQueryResult([], [], rowcount=2),
            _oracle.OracleQueryResult([], [], rowcount=0),
        ]
    )
    tracked_calls: list[tuple[str, Any]] = []
    original_track = OracleCore.track_session_context_sql

    def _spy_track(
        session_context: dict[tuple[str, str], str],
        sql: str,
        parameters: Any,
    ) -> None:
        tracked_calls.append((sql, parameters))
        original_track(session_context, sql, parameters)

    monkeypatch.setattr(OracleCore, "track_session_context_sql", staticmethod(_spy_track))

    driver = SyncOracleDriver(client=stub)
    assert driver.select("SELECT 1 AS a FROM dual") == [{"a": 1}]
    assert driver.execute("UPDATE items SET n = 1") == 2
    assert tracked_calls == []

    driver.execute("BEGIN DBMS_SESSION.SET_IDENTIFIER(:value); END;", {"value": "tenant-42"})
    assert len(tracked_calls) == 1
    assert driver._session_context[("USERENV", "CLIENT_IDENTIFIER")] == "tenant-42"
    driver.close()


def test_sync_oracle_driver_lease_no_broker_runs_no_lease_calls() -> None:
    """Verify ten statements with broker=None never invoke lease attachment or release."""
    stub = _StubSyncOracleClient()
    driver = SyncOracleDriver(client=stub, broker=None)
    assert driver._lease_at_checkout is False
    driver.connect()
    for _ in range(10):
        stub.queued_results.append(_oracle.OracleQueryResult([_oracle.OracleColumn("A", "NUMBER")], [(1,)], rowcount=1))
        assert driver.select("SELECT 1 AS a FROM dual") == [{"a": 1}]
    driver.close()
    assert stub.attach_lease_calls == 0
    assert stub.release_lease_calls == 1


def test_sync_oracle_driver_lease_pooled_transaction_and_error_discard() -> None:
    """Verify pooled checkout acquires one lease per checkout and releases on commit or fatal error."""
    broker = _RecordingLeaseBroker()
    raw_pool = _StubSyncOraclePool(min_size=0, max_size=2)
    pool = SyncOraclePool(native_pool=raw_pool)
    driver = SyncOracleDriver(pool=pool, broker=broker)
    assert driver._lease_at_checkout is True

    driver.begin()
    for _ in range(5):
        driver.execute("UPDATE items SET n = n + 1")
    driver.commit()
    assert len(broker.acquire_calls) == 1
    assert len(broker.release_calls) == 1
    assert pool.borrowed_size == 0
    assert pool.idle_size == 1

    fatal_err = OperationalError("ORA-03113: end-of-file on communication channel")
    fatal_err.vendor_code = 3113
    fatal_err.is_transient = True
    raw_pool._idle[0].queued_results.append(fatal_err)
    with pytest.raises(OperationalError, match="ORA-03113"):
        driver.execute("SELECT 1 FROM dual")
    assert len(broker.acquire_calls) == 2
    assert len(broker.release_calls) == 2
    assert pool.borrowed_size == 0
    assert pool.idle_size == 0

    driver.close()
    pool.close()


def test_sync_oracle_driver_lease_pool_attach_failure_releases_client() -> None:
    """Verify a raising lease attach returns the pooled client before propagating."""
    raw_pool = _StubSyncOraclePool(min_size=0, max_size=2)
    pool = SyncOraclePool(native_pool=raw_pool)

    class _AsyncBroker:
        async def acquire(self, owner_pid: int, timeout_ms: int = 5000) -> Any:
            return (owner_pid, timeout_ms)

    driver = SyncOracleDriver(pool=pool, broker=_AsyncBroker())
    with pytest.raises(InterfaceError, match="Async broker transport"):
        driver.execute("SELECT 1 FROM dual")
    assert pool.borrowed_size == 0
    assert pool.idle_size == 1
    driver.close()
    pool.close()


def test_sync_oracle_driver_lease_unpooled_lifecycle_refusal_and_gc() -> None:
    """Verify single-client lease connect/close, refused acquire, and GC drop release."""
    broker = _RecordingLeaseBroker()
    stub = _StubSyncOracleClient()
    driver = SyncOracleDriver(client=stub, broker=broker)
    driver.connect()
    assert len(broker.acquire_calls) == 1
    assert len(broker.release_calls) == 0
    for _ in range(10):
        stub.queued_results.append(_oracle.OracleQueryResult([_oracle.OracleColumn("A", "NUMBER")], [(1,)], rowcount=1))
        assert driver.select("SELECT 1 AS a FROM dual") == [{"a": 1}]
    assert len(broker.acquire_calls) == 1
    assert len(broker.release_calls) == 0
    driver.close()
    assert len(broker.acquire_calls) == 1
    assert len(broker.release_calls) == 1

    refusing_broker = _RecordingLeaseBroker(fail_acquire=RuntimeError("broker full"))
    refused_stub = _StubSyncOracleClient()
    refused_driver = SyncOracleDriver(client=refused_stub, broker=refusing_broker)
    refused_driver.connect()
    refused_stub.queued_results.append(
        _oracle.OracleQueryResult([_oracle.OracleColumn("A", "NUMBER")], [(1,)], rowcount=1)
    )
    assert refused_driver.select("SELECT 1 AS a FROM dual") == [{"a": 1}]
    assert refused_stub.attach_lease_calls == 0
    refused_driver.close()
    assert len(refusing_broker.release_calls) == 0

    gc_broker = _RecordingLeaseBroker()
    gc_driver = SyncOracleDriver(client=_StubSyncOracleClient(), broker=gc_broker)
    gc_driver.connect()
    gc_driver.execute("UPDATE items SET n = 1")
    assert len(gc_broker.acquire_calls) == 1
    assert len(gc_broker.release_calls) == 0
    del gc_driver
    gc.collect()
    assert len(gc_broker.release_calls) == 1


class _AsyncRecordingLeaseBroker:
    """Async broker double whose `acquire` is a coroutine and `release` is synchronous."""

    def __init__(self, *, fail_acquire: BaseException | None = None) -> None:
        self._fail_acquire = fail_acquire
        self._next_slot = 0
        self.acquire_calls: list[tuple[int, int, int]] = []
        self.release_calls: list[tuple[int, int]] = []

    async def acquire(
        self,
        owner_pid: int,
        timeout_ms: int = 5000,
        bind_key_hash: int = 0,
    ) -> _RecordingLeaseGrant:
        """Record an async acquire attempt and return a grant or raise configured failure."""
        await asyncio.sleep(0)
        self.acquire_calls.append((owner_pid, timeout_ms, bind_key_hash))
        if self._fail_acquire is not None:
            raise self._fail_acquire
        self._next_slot += 1
        return _RecordingLeaseGrant(self._next_slot)

    def release(self, slot_index: int, generation: int) -> None:
        """Record a release call for the given slot and generation."""
        self.release_calls.append((slot_index, generation))


@pytest.mark.parametrize(
    ("lowercase", "expected_rows"),
    [
        (True, [{"a": 1}]),
        (False, [{"A": 1}]),
    ],
)
def test_async_oracle_driver_cursor_select_and_casing(
    lowercase: bool,
    expected_rows: list[dict[str, Any]],
) -> None:
    """Verify AsyncOracleDriver.select routes through cursor with cached _cursor_options."""

    async def _run() -> None:
        stub = _StubAsyncOracleClient()
        stub.queued_results.append(_oracle.OracleQueryResult([_oracle.OracleColumn("A", "NUMBER")], [(1,)], rowcount=1))
        cfg = OracleConfig(
            driver_features=OracleDriverFeatures(
                enable_lowercase_column_names=lowercase,
                enable_uuid_binary=False,
                fetch_decimals=True,
                vector_return_format="array",
            )
        )
        driver = AsyncOracleDriver(cfg, client=stub)
        opts = driver._cursor_options()
        assert driver._cursor_options() is opts
        assert opts == {
            "enable_uuid_binary": False,
            "fetch_decimals": True,
            "vector_return_format": "array",
            "enable_lowercase_column_names": lowercase,
        }
        assert await driver.select("SELECT 1 AS a FROM dual") == expected_rows
        assert stub.cursor_calls == []
        fetch_calls = [payload for kind, payload in stub.calls if kind == "fetch_dicts"]
        assert len(fetch_calls) == 1
        assert fetch_calls[0]["enable_lowercase_column_names"] is lowercase
        assert fetch_calls[0]["fetch_decimals"] is True
        await driver.close()

    asyncio.run(_run())


def test_async_oracle_driver_execute_dml_many_out_bind_and_errors() -> None:
    """Verify async DML rowcount, execute_many, out-bind population, and ORA-00001 IntegrityError."""

    async def _run() -> None:
        stub = _StubAsyncOracleClient()
        stub.queued_results.extend(
            [
                _oracle.OracleQueryResult(
                    [_oracle.OracleColumn("A", "NUMBER"), _oracle.OracleColumn("B", "NUMBER")],
                    [(1, 2)],
                    rowcount=1,
                ),
                _oracle.OracleQueryResult([], [], rowcount=2),
                _oracle.OracleQueryResult([], [], rowcount=0),
                _oracle.OracleQueryResult([], [], rowcount=1, out_binds={"out": 99}),
            ]
        )
        dup_err = IntegrityError("ORA-00001: unique constraint violated")
        dup_err.vendor_code = 1
        stub.queued_results.append(dup_err)

        driver = AsyncOracleDriver(client=stub)
        rows, names = await driver.execute_with_description("SELECT 1 AS a, 2 AS b FROM dual")
        assert rows == [(1, 2)]
        assert names == ("a", "b")

        assert await driver.execute("UPDATE items SET name = :1 WHERE id IN (1, 2)", ("updated",)) == 2
        assert await driver.execute_with_description("CREATE TABLE temp_items (id NUMBER)") == ([], ())

        out_var = _oracle.OracleOutBindVar("NUMBER")
        assert await driver.execute("BEGIN :out := 99; END;", {"out": out_var}) == 1
        assert out_var.getvalue() == 99
        assert driver.out_binds == {"out": 99}

        assert await driver.execute_many("INSERT INTO items (id) VALUES (:1)", [(1,), (2,), (3,)]) == 3

        with pytest.raises(IntegrityError, match="ORA-00001"):
            await driver.execute("INSERT INTO items (id) VALUES (1)")
        await driver.close()

    asyncio.run(_run())


@pytest.mark.parametrize("lazy_begin", [False, True])
def test_async_oracle_driver_cursor_and_checkout_cursor_contracts(lazy_begin: bool) -> None:
    """Verify async cursor() on pooled/unpooled/tx drivers and checkout_cursor() lifecycle."""

    async def _run() -> None:
        single_stub = _StubAsyncOracleClient()
        single_driver = AsyncOracleDriver(client=single_stub)
        single_cur = single_driver.cursor(lazy_begin=lazy_begin)
        assert single_stub.cursor_calls[-1]["lazy_begin"] is lazy_begin
        await single_cur.close()
        await single_driver.close()

        pool = AsyncOraclePool(native_pool=_StubAsyncOraclePool(min_size=0, max_size=2))
        pooled_driver = AsyncOracleDriver(pool=pool)
        with pytest.raises(
            InterfaceError,
            match=r"cursor\(\) needs an open transaction on a pooled Oracle driver; use checkout_cursor\(\)",
        ):
            pooled_driver.cursor(lazy_begin=lazy_begin)

        async with pooled_driver.checkout_cursor(lazy_begin=lazy_begin) as cur:
            stub_cur = cast("_StubAsyncOracleCursor", cur)
            stub_cur._sync._client.queued_results.append(
                _oracle.OracleQueryResult([_oracle.OracleColumn("VAL", "NUMBER")], [(1,)], rowcount=1)
            )
            await cur.execute("SELECT 1 FROM dual")
            assert await cur.fetchall() == [(1,)]
            assert stub_cur.lazy_begin is lazy_begin
            assert pool.borrowed_size == 1
        assert stub_cur.closed is True
        assert pool.borrowed_size == 0
        assert pool.idle_size == 1

        await pooled_driver.begin()
        try:
            tx_cur = cast("_StubAsyncOracleCursor", pooled_driver.cursor(lazy_begin=lazy_begin))
            assert tx_cur._sync._client is pooled_driver.client._sync
            assert tx_cur.lazy_begin is lazy_begin
            await tx_cur.close()
        finally:
            await pooled_driver.rollback()
        assert pool.borrowed_size == 0
        await pooled_driver.close()
        await pool.close()

    asyncio.run(_run())


def test_async_oracle_driver_lease_no_broker_runs_no_lease_calls() -> None:
    """Verify ten statements with broker=None never invoke lease attachment or release."""

    async def _run() -> None:
        stub = _StubAsyncOracleClient()
        driver = AsyncOracleDriver(client=stub, broker=None)
        assert driver._lease_at_checkout is False
        await driver.connect()
        for _ in range(10):
            stub.queued_results.append(
                _oracle.OracleQueryResult([_oracle.OracleColumn("A", "NUMBER")], [(1,)], rowcount=1)
            )
            assert await driver.select("SELECT 1 AS a FROM dual") == [{"a": 1}]
        await driver.close()
        assert stub.attach_lease_calls == 0
        assert stub.release_lease_calls == 1

    asyncio.run(_run())


@pytest.mark.parametrize(
    "broker_factory",
    [
        _RecordingLeaseBroker,
        _AsyncRecordingLeaseBroker,
    ],
)
def test_async_oracle_driver_lease_pooled_transaction_and_cancel(
    broker_factory: type[Any],
) -> None:
    """Verify pooled async checkout acquires one lease per checkout and releases on commit or cancel."""

    async def _run() -> None:
        broker = broker_factory()
        raw_pool = _StubAsyncOraclePool(min_size=0, max_size=2)
        pool = AsyncOraclePool(native_pool=raw_pool)
        driver = AsyncOracleDriver(pool=pool, broker=broker)
        assert driver._lease_at_checkout is True

        await driver.begin()
        for _ in range(5):
            await driver.execute("UPDATE items SET n = n + 1")
        await driver.commit()
        assert len(broker.acquire_calls) == 1
        assert len(broker.release_calls) == 1
        assert pool.borrowed_size == 0
        assert pool.idle_size == 1

        entered = asyncio.Event()

        async def _cancellable_checkout() -> None:
            async with driver.checkout_cursor():
                entered.set()
                await asyncio.sleep(60)

        task = asyncio.create_task(_cancellable_checkout())
        await entered.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert len(broker.acquire_calls) == 2
        assert len(broker.release_calls) == 2
        assert pool.borrowed_size == 0
        assert pool.idle_size == 1

        await driver.close()
        await pool.close()

    asyncio.run(_run())


def test_async_oracle_driver_lease_unpooled_lifecycle_refusal_and_gc() -> None:
    """Verify async single-client lease connect/close, refused acquire, and GC drop release."""

    async def _run() -> None:
        broker = _AsyncRecordingLeaseBroker()
        stub = _StubAsyncOracleClient()
        driver = AsyncOracleDriver(client=stub, broker=broker)
        await driver.connect()
        assert len(broker.acquire_calls) == 1
        assert len(broker.release_calls) == 0
        for _ in range(10):
            stub.queued_results.append(
                _oracle.OracleQueryResult([_oracle.OracleColumn("A", "NUMBER")], [(1,)], rowcount=1)
            )
            assert await driver.select("SELECT 1 AS a FROM dual") == [{"a": 1}]
        assert len(broker.acquire_calls) == 1
        assert len(broker.release_calls) == 0
        await driver.close()
        assert len(broker.acquire_calls) == 1
        assert len(broker.release_calls) == 1

        refusing_broker = _AsyncRecordingLeaseBroker(fail_acquire=RuntimeError("broker full"))
        refused_stub = _StubAsyncOracleClient()
        refused_driver = AsyncOracleDriver(client=refused_stub, broker=refusing_broker)
        await refused_driver.connect()
        refused_stub.queued_results.append(
            _oracle.OracleQueryResult([_oracle.OracleColumn("A", "NUMBER")], [(1,)], rowcount=1)
        )
        assert await refused_driver.select("SELECT 1 AS a FROM dual") == [{"a": 1}]
        assert refused_stub.attach_lease_calls == 0
        await refused_driver.close()
        assert len(refusing_broker.release_calls) == 0

        gc_broker = _AsyncRecordingLeaseBroker()
        gc_driver = AsyncOracleDriver(client=_StubAsyncOracleClient(), broker=gc_broker)
        await gc_driver.connect()
        await gc_driver.execute("UPDATE items SET n = 1")
        assert len(gc_broker.acquire_calls) == 1
        assert len(gc_broker.release_calls) == 0
        del gc_driver
        gc.collect()
        assert len(gc_broker.release_calls) == 1

    asyncio.run(_run())


@pytest.mark.parametrize(
    "parts",
    [
        ("is", "dml", "or", "plsql"),
        ("split", "out", "bind", "parameters"),
        ("populate", "out", "bind", "vars"),
        ("", "is", "out", "bind", "var"),
        ("", "out", "bind", "type", "hint"),
        ("normalize", "sql"),
    ],
)
def test_oracle_core_removed_cursor_helpers_raise_attribute_error(parts: tuple[str, ...]) -> None:
    """Verify helpers superseded by the native Oracle cursor are removed from OracleCore."""
    helper_name = "_".join(parts)
    with pytest.raises(AttributeError):
        getattr(OracleCore, helper_name)


_SINGLE_CALL_NATIVE_METHODS: frozenset[str] = frozenset({"fetch_rows", "fetch_dicts", "fetch_value", "execute_dml"})
_SYNC_SINGLE_CALL_CASES: list[tuple[str, str, str, Any]] = [
    ("select", "SELECT 1 AS a FROM dual", "fetch_dicts", [{"a": 1}]),
    ("select_one", "SELECT 1 AS a FROM dual", "fetch_dicts", {"a": 1}),
    ("select_one_or_none", "SELECT 1 AS a FROM dual", "fetch_dicts", {"a": 1}),
    ("select_value", "SELECT 1 AS a FROM dual", "fetch_value", 1),
    ("execute_with_description", "SELECT 1 AS a FROM dual", "fetch_rows", ([(1,)], ("a",))),
    ("execute_tuples", "SELECT 1 AS a FROM dual", "fetch_rows", [(1,)]),
    ("execute", "UPDATE items SET n = 1", "execute_dml", 2),
    ("execute_dml", "UPDATE items SET n = 1", "execute_dml", (2, None)),
]


def _make_single_call_result(method_name: str) -> _oracle.OracleQueryResult:
    if method_name in {"execute", "execute_dml"}:
        return _oracle.OracleQueryResult([], [], rowcount=2)
    return _oracle.OracleQueryResult([_oracle.OracleColumn("A", "NUMBER")], [(1,)], rowcount=1)


@pytest.mark.parametrize(("method_name", "sql", "expected_native", "expected_result"), _SYNC_SINGLE_CALL_CASES)
def test_sync_oracle_driver_single_call_single_client(
    method_name: str,
    sql: str,
    expected_native: str,
    expected_result: Any,
) -> None:
    """Verify each SyncOracleDriver query method makes one native single-call without cursor()."""
    stub = _StubSyncOracleClient()
    stub.queued_results.append(_make_single_call_result(method_name))
    driver = SyncOracleDriver(client=stub)

    result = getattr(driver, method_name)(sql)

    assert result == expected_result
    single_entries = [kind for kind, _ in stub.calls if kind in _SINGLE_CALL_NATIVE_METHODS]
    assert single_entries == [expected_native]
    assert stub.cursor_calls == []
    driver.close()


@pytest.mark.parametrize(("method_name", "sql", "expected_native", "expected_result"), _SYNC_SINGLE_CALL_CASES)
def test_sync_oracle_driver_single_call_pooled_direct_route(
    monkeypatch: pytest.MonkeyPatch,
    method_name: str,
    sql: str,
    expected_native: str,
    expected_result: Any,
) -> None:
    """Verify pooled SyncOracleDriver without a broker dispatches directly to native pool single-call methods."""
    raw_pool = _StubSyncOraclePool(min_size=0, max_size=2)
    raw_pool.single_call_results.append(_make_single_call_result(method_name))
    pool = SyncOraclePool(native_pool=raw_pool)
    driver = SyncOracleDriver(pool=pool)

    python_acquires: list[str] = []

    def _forbid_acquire(*args: Any, **kwargs: Any) -> Any:
        python_acquires.append("acquire")
        raise AssertionError("direct pool route must not acquire from Python")

    monkeypatch.setattr(pool, "acquire", _forbid_acquire)
    monkeypatch.setattr(pool, "acquire_from_pool", _forbid_acquire)
    monkeypatch.setattr(driver, "_acquire_pool_client", _forbid_acquire)

    result = getattr(driver, method_name)(sql)

    assert result == expected_result
    assert [entry[0] for entry in raw_pool.single_calls] == [expected_native]
    assert python_acquires == []
    assert pool.borrowed_size == 0
    driver.close()
    pool.close()


def test_sync_oracle_driver_single_call_pinned_transaction_route() -> None:
    """Verify statements inside begin()/rollback() run on the pinned client without pool single-calls."""
    raw_pool = _StubSyncOraclePool(min_size=0, max_size=2)
    pool = SyncOraclePool(native_pool=raw_pool)
    driver = SyncOracleDriver(pool=pool)

    driver.begin()
    pinned = cast("_StubSyncOracleClient", driver.client)
    pinned.queued_results.extend(
        [
            _oracle.OracleQueryResult([_oracle.OracleColumn("A", "NUMBER")], [(1,)], rowcount=1),
            _oracle.OracleQueryResult([], [], rowcount=2),
            _oracle.OracleQueryResult([], [], rowcount=3, lastrowid="AAARowId"),
        ]
    )

    assert driver.select("SELECT 1 AS a FROM dual") == [{"a": 1}]
    assert driver.execute("UPDATE items SET n = 1") == 2
    assert driver.execute_dml("UPDATE items SET n = 2") == (3, "AAARowId")
    driver.rollback()

    single_entries = [kind for kind, _ in pinned.calls if kind in _SINGLE_CALL_NATIVE_METHODS]
    assert single_entries == ["fetch_dicts", "execute_dml", "execute_dml"]
    assert pinned.cursor_calls == []
    assert raw_pool.single_calls == []
    assert pool.borrowed_size == 0
    driver.close()
    pool.close()


@pytest.mark.parametrize("mode", ["single", "pooled", "pinned"])
def test_sync_oracle_driver_query_methods_skip_checkout_generators(
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
) -> None:
    """Verify none of the eight query methods enter checkout_cursor or _checkout_client."""
    raw_pool: _StubSyncOraclePool | None = None
    pool: SyncOraclePool | None = None
    stub: _StubSyncOracleClient | None = None
    if mode == "single":
        stub = _StubSyncOracleClient()
        driver = SyncOracleDriver(client=stub)
    else:
        raw_pool = _StubSyncOraclePool(min_size=0, max_size=2)
        pool = SyncOraclePool(native_pool=raw_pool)
        driver = SyncOracleDriver(pool=pool)
        if mode == "pinned":
            driver.begin()

    generator_calls: list[str] = []

    def _spy_checkout_cursor(*args: Any, **kwargs: Any) -> Any:
        generator_calls.append("checkout_cursor")
        raise AssertionError("checkout_cursor must not be entered by query methods")

    def _spy_checkout_client(*args: Any, **kwargs: Any) -> Any:
        generator_calls.append("_checkout_client")
        raise AssertionError("_checkout_client must not be entered by query methods")

    monkeypatch.setattr(driver, "checkout_cursor", _spy_checkout_cursor)
    monkeypatch.setattr(driver, "_checkout_client", _spy_checkout_client)

    target_queue = (
        stub.queued_results
        if stub is not None
        else (
            cast("_StubSyncOracleClient", driver.client).queued_results
            if mode == "pinned"
            else cast("_StubSyncOraclePool", raw_pool).single_call_results
        )
    )
    for method_name, sql, _, expected_result in _SYNC_SINGLE_CALL_CASES:
        target_queue.append(_make_single_call_result(method_name))
        assert getattr(driver, method_name)(sql) == expected_result

    assert generator_calls == []
    if mode == "pinned":
        driver.rollback()
    driver.close()
    if pool is not None:
        pool.close()


def test_sync_oracle_driver_typed_select_value_type_dml_and_out_binds_reset() -> None:
    """Verify schema_type hydration via fetch_rows, value_type coercion, NotFoundError, and out_binds reset."""
    stub = _StubSyncOracleClient()
    stub.queued_results.extend(
        [
            _oracle.OracleQueryResult(
                [_oracle.OracleColumn("EMPLOYEE_ID", "NUMBER"), _oracle.OracleColumn("FIRST_NAME", "VARCHAR2")],
                [(1, "Ada")],
                rowcount=1,
            ),
            _oracle.OracleQueryResult([_oracle.OracleColumn("V", "VARCHAR2")], [("42",)], rowcount=1),
            _oracle.OracleQueryResult([_oracle.OracleColumn("V", "NUMBER")], [], rowcount=0),
            _oracle.OracleQueryResult([], [], rowcount=2),
            _oracle.OracleQueryResult([], [], rowcount=1, lastrowid="AAARowId"),
            _oracle.OracleQueryResult([], [], rowcount=1, out_binds={"out": 99}),
            _oracle.OracleQueryResult([_oracle.OracleColumn("A", "NUMBER")], [(1,)], rowcount=1),
            _oracle.OracleQueryResult([], [], rowcount=1, out_binds={"out": 77}),
            _oracle.OracleQueryResult([_oracle.OracleColumn("A", "NUMBER")], [(7,)], rowcount=1),
            _oracle.OracleQueryResult([], [], rowcount=1, out_binds={"out": 55}),
            _oracle.OracleQueryResult([_oracle.OracleColumn("A", "NUMBER")], [(8,)], rowcount=1),
            _oracle.OracleQueryResult([], [], rowcount=1, out_binds={"out": 33}),
            _oracle.OracleQueryResult([_oracle.OracleColumn("A", "NUMBER")], [(9,)], rowcount=1),
        ]
    )
    driver = SyncOracleDriver(client=stub)

    hydrated = driver.select("SELECT employee_id, first_name FROM employees", schema_type=_EmployeeRow)
    assert hydrated == [_EmployeeRow(employee_id=1, first_name="Ada")]
    assert [k for k, _ in stub.calls if k in _SINGLE_CALL_NATIVE_METHODS] == ["fetch_rows"]
    assert stub.cursor_calls == []

    assert driver.select_value("SELECT '42' AS v FROM dual", value_type=int) == 42
    with pytest.raises(NotFoundError):
        driver.select_value("SELECT 1 AS v FROM dual WHERE 1 = 0")

    assert driver.execute_dml("UPDATE items SET n = 1") == (2, None)
    assert driver.execute_dml("INSERT INTO items (n) VALUES (1)") == (1, "AAARowId")

    out_var = _oracle.OracleOutBindVar("NUMBER")
    assert driver.execute("BEGIN :out := 99; END;", {"out": out_var}) == 1
    assert out_var.getvalue() == 99
    assert driver.out_binds == {"out": 99}

    assert driver.select("SELECT 1 AS a FROM dual") == [{"a": 1}]
    assert driver.out_binds is None

    assert driver.execute("BEGIN :out := 77; END;", {"out": out_var}) == 1
    assert driver.out_binds == {"out": 77}
    assert driver.select_value("SELECT 7 AS a FROM dual") == 7
    assert driver.out_binds is None

    assert driver.execute("BEGIN :out := 55; END;", {"out": out_var}) == 1
    assert driver.out_binds == {"out": 55}
    assert driver.execute_tuples("SELECT 8 AS a FROM dual") == [(8,)]
    assert driver.out_binds is None

    assert driver.execute("BEGIN :out := 33; END;", {"out": out_var}) == 1
    assert driver.out_binds == {"out": 33}
    assert driver.execute_with_description("SELECT 9 AS a FROM dual") == ([(9,)], ("a",))
    assert driver.out_binds is None
    driver.close()


@pytest.mark.parametrize("use_pool", [False, True])
def test_sync_oracle_driver_pending_session_attrs_then_direct_route(
    monkeypatch: pytest.MonkeyPatch,
    use_pool: bool,
) -> None:
    """Verify pending session attributes apply once on first statement, then later statements take the direct route."""
    cfg = OracleConfig(client_identifier="user-7")
    if use_pool:
        raw_pool = _StubSyncOraclePool(min_size=0, max_size=2)
        pool = SyncOraclePool(cfg, native_pool=raw_pool)
        driver = SyncOracleDriver(cfg, pool=pool)
        assert driver._pending_session_attrs is not None

        assert driver.execute("UPDATE items SET n = 1") == 1
        assert driver._pending_session_attrs is None
        assert driver.last_piggyback_frame is not None
        assert raw_pool.single_calls == []
        assert len(raw_pool.created_clients) == 1
        assert raw_pool.created_clients[0].active_session_attrs["client_identifier"] == "user-7"

        raw_pool.single_call_results.append(
            _oracle.OracleQueryResult([_oracle.OracleColumn("A", "NUMBER")], [(1,)], rowcount=1)
        )
        assert driver.select("SELECT 1 AS a FROM dual") == [{"a": 1}]
        assert [entry[0] for entry in raw_pool.single_calls] == ["fetch_dicts"]
        driver.close()
        pool.close()
        return

    stub = _StubSyncOracleClient()
    stub.queued_results.extend(
        [
            _oracle.OracleQueryResult([_oracle.OracleColumn("A", "NUMBER")], [(1,)], rowcount=1),
            _oracle.OracleQueryResult([_oracle.OracleColumn("A", "NUMBER")], [(2,)], rowcount=1),
        ]
    )
    driver = SyncOracleDriver(cfg, client=stub)
    assert driver._pending_session_attrs is not None

    assert driver.select("SELECT 1 AS a FROM dual") == [{"a": 1}]
    assert driver._pending_session_attrs is None
    assert stub.active_session_attrs["client_identifier"] == "user-7"
    assert driver.last_piggyback_frame is not None

    apply_calls: list[Any] = []
    monkeypatch.setattr(driver, "_apply_pending_session_attrs", apply_calls.append)
    assert driver.select("SELECT 2 AS a FROM dual") == [{"a": 2}]
    assert apply_calls == []
    assert stub.cursor_calls == []
    driver.close()


def test_sync_oracle_driver_pooled_broker_autocommit_leases_and_fatal_discard(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify pooled driver with broker acquires one lease per autocommit statement and discards on ORA-03113."""
    broker = _RecordingLeaseBroker()
    raw_pool = _StubSyncOraclePool(min_size=0, max_size=2)
    pool = SyncOraclePool(native_pool=raw_pool)
    driver = SyncOracleDriver(pool=pool, broker=broker)

    release_discards: list[bool] = []
    original_release = pool.release_to_pool

    def _spy_release(client: Any, **kwargs: Any) -> None:
        release_discards.append(bool(kwargs.get("discard", False)))
        original_release(client, **kwargs)

    monkeypatch.setattr(pool, "release_to_pool", _spy_release)

    assert driver.execute("UPDATE items SET n = 1") == 1
    raw_pool._idle[0].queued_results.append(
        _oracle.OracleQueryResult([_oracle.OracleColumn("A", "NUMBER")], [(1,)], rowcount=1)
    )
    assert driver.select("SELECT 1 AS a FROM dual") == [{"a": 1}]
    raw_pool._idle[0].queued_results.append(
        _oracle.OracleQueryResult([_oracle.OracleColumn("A", "NUMBER")], [(9,)], rowcount=1)
    )
    assert driver.select_value("SELECT 9 AS a FROM dual") == 9

    assert len(broker.acquire_calls) == 3
    assert len(broker.release_calls) == 3
    assert raw_pool.single_calls == []
    assert release_discards == [False, False, False]

    fatal_err = OperationalError("ORA-03113: end-of-file on communication channel")
    fatal_err.vendor_code = 3113
    fatal_err.is_transient = True
    raw_pool._idle[0].queued_results.append(fatal_err)
    with pytest.raises(OperationalError, match="ORA-03113"):
        driver.execute("SELECT 1 FROM dual")

    assert len(broker.acquire_calls) == 4
    assert len(broker.release_calls) == 4
    assert release_discards == [False, False, False, True]
    assert pool.idle_size == 0
    assert pool.borrowed_size == 0
    driver.close()
    pool.close()


class _CountingAwaitable:
    """Awaitable wrapper that increments a shared counter each time it is awaited."""

    def __init__(self, inner: Awaitable[Any], counter: list[int]) -> None:
        self._inner = inner
        self._counter = counter

    def __await__(self) -> Generator[Any, None, Any]:
        self._counter[0] += 1
        return self._inner.__await__()


class _CountingAsyncOracleClient(_StubAsyncOracleClient):
    """Async client double returning `_CountingAwaitable` from plain `def` single-call methods."""

    def __init__(self, *, counter: list[int] | None = None) -> None:
        super().__init__()
        self.await_counter: list[int] = counter if counter is not None else [0]

    def fetch_rows(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
        *,
        out_binds: Mapping[str, str] | Sequence[str] | None = None,
        enable_uuid_binary: bool = True,
        fetch_decimals: bool = False,
        vector_return_format: str = "list",
        enable_lowercase_column_names: bool = True,
    ) -> Any:
        """Return a counted awaitable delegating to `_StubAsyncOracleClient.fetch_rows`."""
        return _CountingAwaitable(
            super().fetch_rows(
                sql,
                params,
                out_binds=out_binds,
                enable_uuid_binary=enable_uuid_binary,
                fetch_decimals=fetch_decimals,
                vector_return_format=vector_return_format,
                enable_lowercase_column_names=enable_lowercase_column_names,
            ),
            self.await_counter,
        )

    def fetch_dicts(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
        *,
        out_binds: Mapping[str, str] | Sequence[str] | None = None,
        enable_uuid_binary: bool = True,
        fetch_decimals: bool = False,
        vector_return_format: str = "list",
        enable_lowercase_column_names: bool = True,
    ) -> Any:
        """Return a counted awaitable delegating to `_StubAsyncOracleClient.fetch_dicts`."""
        return _CountingAwaitable(
            super().fetch_dicts(
                sql,
                params,
                out_binds=out_binds,
                enable_uuid_binary=enable_uuid_binary,
                fetch_decimals=fetch_decimals,
                vector_return_format=vector_return_format,
                enable_lowercase_column_names=enable_lowercase_column_names,
            ),
            self.await_counter,
        )

    def fetch_value(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
        *,
        out_binds: Mapping[str, str] | Sequence[str] | None = None,
        enable_uuid_binary: bool = True,
        fetch_decimals: bool = False,
        vector_return_format: str = "list",
        enable_lowercase_column_names: bool = True,
    ) -> Any:
        """Return a counted awaitable delegating to `_StubAsyncOracleClient.fetch_value`."""
        return _CountingAwaitable(
            super().fetch_value(
                sql,
                params,
                out_binds=out_binds,
                enable_uuid_binary=enable_uuid_binary,
                fetch_decimals=fetch_decimals,
                vector_return_format=vector_return_format,
                enable_lowercase_column_names=enable_lowercase_column_names,
            ),
            self.await_counter,
        )

    def execute_dml(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
        *,
        out_binds: Mapping[str, str] | Sequence[str] | None = None,
        enable_uuid_binary: bool = True,
        fetch_decimals: bool = False,
        vector_return_format: str = "list",
        enable_lowercase_column_names: bool = True,
    ) -> Any:
        """Return a counted awaitable delegating to `_StubAsyncOracleClient.execute_dml`."""
        return _CountingAwaitable(
            super().execute_dml(
                sql,
                params,
                out_binds=out_binds,
                enable_uuid_binary=enable_uuid_binary,
                fetch_decimals=fetch_decimals,
                vector_return_format=vector_return_format,
                enable_lowercase_column_names=enable_lowercase_column_names,
            ),
            self.await_counter,
        )


class _CountingAsyncOraclePool(_StubAsyncOraclePool):
    """Async pool double returning `_CountingAwaitable` from plain `def` single-call methods."""

    def __init__(self, *, min_size: int = 0, max_size: int = 2) -> None:
        super().__init__(min_size=min_size, max_size=max_size)
        self.await_counter: list[int] = [0]

    def fetch_rows(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
        *,
        out_binds: Mapping[str, str] | Sequence[str] | None = None,
        enable_uuid_binary: bool = True,
        fetch_decimals: bool = False,
        vector_return_format: str = "list",
        enable_lowercase_column_names: bool = True,
    ) -> Any:
        """Return a counted awaitable delegating to `_StubAsyncOraclePool.fetch_rows`."""
        return _CountingAwaitable(
            super().fetch_rows(
                sql,
                params,
                out_binds=out_binds,
                enable_uuid_binary=enable_uuid_binary,
                fetch_decimals=fetch_decimals,
                vector_return_format=vector_return_format,
                enable_lowercase_column_names=enable_lowercase_column_names,
            ),
            self.await_counter,
        )

    def fetch_dicts(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
        *,
        out_binds: Mapping[str, str] | Sequence[str] | None = None,
        enable_uuid_binary: bool = True,
        fetch_decimals: bool = False,
        vector_return_format: str = "list",
        enable_lowercase_column_names: bool = True,
    ) -> Any:
        """Return a counted awaitable delegating to `_StubAsyncOraclePool.fetch_dicts`."""
        return _CountingAwaitable(
            super().fetch_dicts(
                sql,
                params,
                out_binds=out_binds,
                enable_uuid_binary=enable_uuid_binary,
                fetch_decimals=fetch_decimals,
                vector_return_format=vector_return_format,
                enable_lowercase_column_names=enable_lowercase_column_names,
            ),
            self.await_counter,
        )

    def fetch_value(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
        *,
        out_binds: Mapping[str, str] | Sequence[str] | None = None,
        enable_uuid_binary: bool = True,
        fetch_decimals: bool = False,
        vector_return_format: str = "list",
        enable_lowercase_column_names: bool = True,
    ) -> Any:
        """Return a counted awaitable delegating to `_StubAsyncOraclePool.fetch_value`."""
        return _CountingAwaitable(
            super().fetch_value(
                sql,
                params,
                out_binds=out_binds,
                enable_uuid_binary=enable_uuid_binary,
                fetch_decimals=fetch_decimals,
                vector_return_format=vector_return_format,
                enable_lowercase_column_names=enable_lowercase_column_names,
            ),
            self.await_counter,
        )

    def execute_dml(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = (),
        *,
        out_binds: Mapping[str, str] | Sequence[str] | None = None,
        enable_uuid_binary: bool = True,
        fetch_decimals: bool = False,
        vector_return_format: str = "list",
        enable_lowercase_column_names: bool = True,
    ) -> Any:
        """Return a counted awaitable delegating to `_StubAsyncOraclePool.execute_dml`."""
        return _CountingAwaitable(
            super().execute_dml(
                sql,
                params,
                out_binds=out_binds,
                enable_uuid_binary=enable_uuid_binary,
                fetch_decimals=fetch_decimals,
                vector_return_format=vector_return_format,
                enable_lowercase_column_names=enable_lowercase_column_names,
            ),
            self.await_counter,
        )


@pytest.mark.parametrize(("method_name", "sql", "expected_native", "expected_result"), _SYNC_SINGLE_CALL_CASES)
def test_async_oracle_driver_single_call_single_client(
    monkeypatch: pytest.MonkeyPatch,
    method_name: str,
    sql: str,
    expected_native: str,
    expected_result: Any,
) -> None:
    """Verify each AsyncOracleDriver query method awaits one native single-call on a single client."""

    async def _run() -> None:
        stub = _CountingAsyncOracleClient()
        stub.queued_results.append(_make_single_call_result(method_name))
        driver = AsyncOracleDriver(client=stub)

        checked_calls: list[str] = []

        async def _spy_checked(*args: Any, **kwargs: Any) -> Any:
            checked_calls.append("checked")
            raise AssertionError("direct single-client route must not enter _single_call_checked")

        monkeypatch.setattr(driver, "_single_call_checked", _spy_checked)

        result = await getattr(driver, method_name)(sql)

        assert result == expected_result
        single_entries = [kind for kind, _ in stub.calls if kind in _SINGLE_CALL_NATIVE_METHODS]
        assert single_entries == [expected_native]
        assert stub.cursor_calls == []
        assert stub.await_counter == [1]
        assert checked_calls == []
        await driver.close()

    asyncio.run(_run())


@pytest.mark.parametrize(("method_name", "sql", "expected_native", "expected_result"), _SYNC_SINGLE_CALL_CASES)
def test_async_oracle_driver_single_call_pooled_direct_route(
    monkeypatch: pytest.MonkeyPatch,
    method_name: str,
    sql: str,
    expected_native: str,
    expected_result: Any,
) -> None:
    """Verify pooled AsyncOracleDriver without a broker awaits one native pool single-call with no helper coroutine."""

    async def _run() -> None:
        raw_pool = _CountingAsyncOraclePool(min_size=0, max_size=2)
        raw_pool.single_call_results.append(_make_single_call_result(method_name))
        pool = AsyncOraclePool(native_pool=raw_pool)
        driver = AsyncOracleDriver(pool=pool)

        python_acquires: list[str] = []
        checked_calls: list[str] = []

        async def _forbid_acquire(*args: Any, **kwargs: Any) -> Any:
            python_acquires.append("acquire")
            raise AssertionError("direct pool route must not acquire from Python")

        async def _spy_checked(*args: Any, **kwargs: Any) -> Any:
            checked_calls.append("checked")
            raise AssertionError("direct pool route must not enter _single_call_checked")

        monkeypatch.setattr(pool, "acquire", _forbid_acquire)
        monkeypatch.setattr(pool, "acquire_from_pool", _forbid_acquire)
        monkeypatch.setattr(driver, "_acquire_pool_client", _forbid_acquire)
        monkeypatch.setattr(driver, "_single_call_checked", _spy_checked)

        result = await getattr(driver, method_name)(sql)

        assert result == expected_result
        assert [entry[0] for entry in raw_pool.single_calls] == [expected_native]
        assert raw_pool.await_counter == [1]
        assert python_acquires == []
        assert checked_calls == []
        assert pool.borrowed_size == 0
        await driver.close()
        await pool.close()

    asyncio.run(_run())


@pytest.mark.parametrize(("method_name", "sql", "expected_native", "expected_result"), _SYNC_SINGLE_CALL_CASES)
def test_async_oracle_driver_single_call_pinned_each_method(
    monkeypatch: pytest.MonkeyPatch,
    method_name: str,
    sql: str,
    expected_native: str,
    expected_result: Any,
) -> None:
    """Verify each AsyncOracleDriver query method awaits one native single-call on a pinned transaction client."""

    async def _run() -> None:
        raw_pool = _CountingAsyncOraclePool(min_size=0, max_size=2)
        counting_client = _CountingAsyncOracleClient()
        raw_pool._idle.append(counting_client)
        pool = AsyncOraclePool(native_pool=raw_pool)
        driver = AsyncOracleDriver(pool=pool)

        await driver.begin()
        assert driver.client is counting_client
        counting_client.queued_results.append(_make_single_call_result(method_name))

        checked_calls: list[str] = []

        async def _spy_checked(*args: Any, **kwargs: Any) -> Any:
            checked_calls.append("checked")
            raise AssertionError("pinned direct route must not enter _single_call_checked")

        monkeypatch.setattr(driver, "_single_call_checked", _spy_checked)

        result = await getattr(driver, method_name)(sql)
        await driver.rollback()

        assert result == expected_result
        single_entries = [kind for kind, _ in counting_client.calls if kind in _SINGLE_CALL_NATIVE_METHODS]
        assert single_entries == [expected_native]
        assert counting_client.cursor_calls == []
        assert counting_client.await_counter == [1]
        assert raw_pool.single_calls == []
        assert raw_pool.await_counter == [0]
        assert checked_calls == []
        assert pool.borrowed_size == 0
        await driver.close()
        await pool.close()

    asyncio.run(_run())


def test_async_oracle_driver_single_call_pinned_transaction_route() -> None:
    """Verify statements inside begin()/rollback() run on the pinned client without pool single-calls."""

    async def _run() -> None:
        raw_pool = _CountingAsyncOraclePool(min_size=0, max_size=2)
        counting_client = _CountingAsyncOracleClient()
        raw_pool._idle.append(counting_client)
        pool = AsyncOraclePool(native_pool=raw_pool)
        driver = AsyncOracleDriver(pool=pool)

        await driver.begin()
        pinned = cast("_CountingAsyncOracleClient", driver.client)
        pinned.queued_results.extend(
            [
                _oracle.OracleQueryResult([_oracle.OracleColumn("A", "NUMBER")], [(1,)], rowcount=1),
                _oracle.OracleQueryResult([], [], rowcount=2),
                _oracle.OracleQueryResult([], [], rowcount=3, lastrowid="AAARowId"),
            ]
        )

        assert await driver.select("SELECT 1 AS a FROM dual") == [{"a": 1}]
        assert await driver.execute("UPDATE items SET n = 1") == 2
        assert await driver.execute_dml("UPDATE items SET n = 2") == (3, "AAARowId")
        await driver.rollback()

        single_entries = [kind for kind, _ in pinned.calls if kind in _SINGLE_CALL_NATIVE_METHODS]
        assert single_entries == ["fetch_dicts", "execute_dml", "execute_dml"]
        assert pinned.cursor_calls == []
        assert pinned.await_counter == [3]
        assert raw_pool.single_calls == []
        assert pool.borrowed_size == 0
        await driver.close()
        await pool.close()

    asyncio.run(_run())


@pytest.mark.parametrize("mode", ["single", "pooled", "pinned"])
def test_async_oracle_driver_query_methods_skip_checkout_generators(
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
) -> None:
    """Verify none of the eight async query methods enter checkout_cursor or _checkout_client."""

    async def _run() -> None:
        raw_pool: _StubAsyncOraclePool | None = None
        pool: AsyncOraclePool | None = None
        stub: _StubAsyncOracleClient | None = None
        if mode == "single":
            stub = _StubAsyncOracleClient()
            driver = AsyncOracleDriver(client=stub)
        else:
            raw_pool = _StubAsyncOraclePool(min_size=0, max_size=2)
            pool = AsyncOraclePool(native_pool=raw_pool)
            driver = AsyncOracleDriver(pool=pool)
            if mode == "pinned":
                await driver.begin()

        generator_calls: list[str] = []

        def _spy_checkout_cursor(*args: Any, **kwargs: Any) -> Any:
            generator_calls.append("checkout_cursor")
            raise AssertionError("checkout_cursor must not be entered by query methods")

        def _spy_checkout_client(*args: Any, **kwargs: Any) -> Any:
            generator_calls.append("_checkout_client")
            raise AssertionError("_checkout_client must not be entered by query methods")

        monkeypatch.setattr(driver, "checkout_cursor", _spy_checkout_cursor)
        monkeypatch.setattr(driver, "_checkout_client", _spy_checkout_client)

        target_queue = (
            stub.queued_results
            if stub is not None
            else (
                cast("_StubAsyncOracleClient", driver.client).queued_results
                if mode == "pinned"
                else cast("_StubAsyncOraclePool", raw_pool).single_call_results
            )
        )
        for method_name, sql, _, expected_result in _SYNC_SINGLE_CALL_CASES:
            target_queue.append(_make_single_call_result(method_name))
            assert await getattr(driver, method_name)(sql) == expected_result

        assert generator_calls == []
        if mode == "pinned":
            await driver.rollback()
        await driver.close()
        if pool is not None:
            await pool.close()

    asyncio.run(_run())


def test_async_oracle_driver_typed_select_value_type_returning_and_out_binds_reset() -> None:
    """Verify async schema_type hydration, value_type, NotFoundError, exec_returning_async, and out_binds reset."""

    async def _run() -> None:
        stub = _StubAsyncOracleClient()
        stub.queued_results.extend(
            [
                _oracle.OracleQueryResult(
                    [_oracle.OracleColumn("EMPLOYEE_ID", "NUMBER"), _oracle.OracleColumn("FIRST_NAME", "VARCHAR2")],
                    [(1, "Ada")],
                    rowcount=1,
                ),
                _oracle.OracleQueryResult([_oracle.OracleColumn("V", "VARCHAR2")], [("42",)], rowcount=1),
                _oracle.OracleQueryResult([_oracle.OracleColumn("V", "NUMBER")], [], rowcount=0),
                _oracle.OracleQueryResult([], [], rowcount=2),
                _oracle.OracleQueryResult([], [], rowcount=1, lastrowid="AAARowId"),
                _oracle.OracleQueryResult([], [], rowcount=1, out_binds={"out": 99}),
                _oracle.OracleQueryResult([_oracle.OracleColumn("A", "NUMBER")], [(1,)], rowcount=1),
                _oracle.OracleQueryResult([], [], rowcount=1, out_binds={"out": 77}),
                _oracle.OracleQueryResult([_oracle.OracleColumn("A", "NUMBER")], [(7,)], rowcount=1),
                _oracle.OracleQueryResult([], [], rowcount=1, out_binds={"out": 55}),
                _oracle.OracleQueryResult([_oracle.OracleColumn("A", "NUMBER")], [(8,)], rowcount=1),
                _oracle.OracleQueryResult([], [], rowcount=1, out_binds={"out": 33}),
                _oracle.OracleQueryResult([_oracle.OracleColumn("A", "NUMBER")], [(9,)], rowcount=1),
                _oracle.OracleQueryResult(
                    [_oracle.OracleColumn("EMPLOYEE_ID", "NUMBER"), _oracle.OracleColumn("FIRST_NAME", "VARCHAR2")],
                    [],
                    rowcount=1,
                    out_binds={"employee_id": 7, "first_name": "Grace"},
                ),
                _oracle.OracleQueryResult(
                    [_oracle.OracleColumn("EMPLOYEE_ID", "NUMBER"), _oracle.OracleColumn("FIRST_NAME", "VARCHAR2")],
                    [],
                    rowcount=1,
                    out_binds={"employee_id": 8, "first_name": "Linus"},
                ),
            ]
        )
        driver = AsyncOracleDriver(client=stub)

        hydrated = await driver.select("SELECT employee_id, first_name FROM employees", schema_type=_EmployeeRow)
        assert hydrated == [_EmployeeRow(employee_id=1, first_name="Ada")]
        assert [k for k, _ in stub.calls if k in _SINGLE_CALL_NATIVE_METHODS] == ["fetch_rows"]
        assert stub.cursor_calls == []

        assert await driver.select_value("SELECT '42' AS v FROM dual", value_type=int) == 42
        with pytest.raises(NotFoundError):
            await driver.select_value("SELECT 1 AS v FROM dual WHERE 1 = 0")

        assert await driver.execute_dml("UPDATE items SET n = 1") == (2, None)
        assert await driver.execute_dml("INSERT INTO items (n) VALUES (1)") == (1, "AAARowId")

        out_var = _oracle.OracleOutBindVar("NUMBER")
        assert await driver.execute("BEGIN :out := 99; END;", {"out": out_var}) == 1
        assert out_var.getvalue() == 99
        assert driver.out_binds == {"out": 99}

        assert await driver.select("SELECT 1 AS a FROM dual") == [{"a": 1}]
        assert driver.out_binds is None

        assert await driver.execute("BEGIN :out := 77; END;", {"out": out_var}) == 1
        assert driver.out_binds == {"out": 77}
        assert await driver.select_value("SELECT 7 AS a FROM dual") == 7
        assert driver.out_binds is None

        assert await driver.execute("BEGIN :out := 55; END;", {"out": out_var}) == 1
        assert driver.out_binds == {"out": 55}
        assert await driver.execute_tuples("SELECT 8 AS a FROM dual") == [(8,)]
        assert driver.out_binds is None

        assert await driver.execute("BEGIN :out := 33; END;", {"out": out_var}) == 1
        assert driver.out_binds == {"out": 33}
        assert await driver.execute_with_description("SELECT 9 AS a FROM dual") == ([(9,)], ("a",))
        assert driver.out_binds is None

        returning_sql = (
            "INSERT INTO employees (first_name) VALUES (:first_name) "
            "RETURNING employee_id, first_name INTO :employee_id, :first_name"
        )
        assert await driver.exec_returning_async(returning_sql, {"first_name": "Grace"}) == [
            {"employee_id": 7, "first_name": "Grace"}
        ]
        assert await driver.exec_returning_async(
            returning_sql,
            {"first_name": "Linus"},
            schema_type=_EmployeeRow,
        ) == [_EmployeeRow(employee_id=8, first_name="Linus")]
        await driver.close()

    asyncio.run(_run())


def test_async_oracle_driver_session_context_tracking_and_plain_dml(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify async execute tracks DBMS_SESSION.SET_IDENTIFIER and skips tracking on plain DML."""

    async def _run() -> None:
        stub = _StubAsyncOracleClient()
        driver = AsyncOracleDriver(client=stub)

        await driver.execute(
            "BEGIN DBMS_SESSION.SET_IDENTIFIER(:value); END;",
            {"value": "tenant-42"},
        )
        assert driver._session_context[("USERENV", "CLIENT_IDENTIFIER")] == "tenant-42"

        track_calls: list[str] = []
        original_track = OracleCore.track_session_context_sql

        def _spy_track(session_context: dict[tuple[str, str], str], sql: str, params: Any) -> None:
            track_calls.append(sql)
            original_track(session_context, sql, params)

        monkeypatch.setattr(OracleCore, "track_session_context_sql", staticmethod(_spy_track))
        assert await driver.execute("UPDATE items SET n = 1") == 1
        assert track_calls == []
        await driver.close()

    asyncio.run(_run())


@pytest.mark.parametrize("use_pool", [False, True])
def test_async_oracle_driver_pending_session_attrs_then_direct_route(
    monkeypatch: pytest.MonkeyPatch,
    use_pool: bool,
) -> None:
    """Verify async pending session attributes apply once, then later statements take the direct route."""

    async def _run() -> None:
        cfg = OracleConfig(client_identifier="user-7")
        if use_pool:
            raw_pool = _CountingAsyncOraclePool(min_size=0, max_size=2)
            pool = AsyncOraclePool(cfg, native_pool=raw_pool)
            driver = AsyncOracleDriver(cfg, pool=pool)
            assert driver._pending_session_attrs is not None

            assert await driver.execute("UPDATE items SET n = 1") == 1
            assert driver._pending_session_attrs is None
            assert driver.last_piggyback_frame is not None
            assert raw_pool.single_calls == []
            assert len(raw_pool.created_clients) == 1
            assert raw_pool.created_clients[0].active_session_attrs["client_identifier"] == "user-7"

            checked_calls: list[str] = []

            async def _spy_checked(*args: Any, **kwargs: Any) -> Any:
                checked_calls.append("checked")
                raise AssertionError("subsequent pooled statements must take the direct route")

            monkeypatch.setattr(driver, "_single_call_checked", _spy_checked)
            raw_pool.single_call_results.append(
                _oracle.OracleQueryResult([_oracle.OracleColumn("A", "NUMBER")], [(1,)], rowcount=1)
            )
            assert await driver.select("SELECT 1 AS a FROM dual") == [{"a": 1}]
            assert [entry[0] for entry in raw_pool.single_calls] == ["fetch_dicts"]
            assert raw_pool.await_counter == [1]
            assert checked_calls == []
            await driver.close()
            await pool.close()
            return

        stub = _CountingAsyncOracleClient()
        stub.queued_results.extend(
            [
                _oracle.OracleQueryResult([_oracle.OracleColumn("A", "NUMBER")], [(1,)], rowcount=1),
                _oracle.OracleQueryResult([_oracle.OracleColumn("A", "NUMBER")], [(2,)], rowcount=1),
            ]
        )
        driver = AsyncOracleDriver(cfg, client=stub)
        assert driver._pending_session_attrs is not None

        assert await driver.select("SELECT 1 AS a FROM dual") == [{"a": 1}]
        assert driver._pending_session_attrs is None
        assert stub.active_session_attrs["client_identifier"] == "user-7"
        assert driver.last_piggyback_frame is not None

        apply_calls: list[Any] = []
        checked_calls = []

        async def _spy_checked_single(*args: Any, **kwargs: Any) -> Any:
            checked_calls.append("checked")
            raise AssertionError("subsequent single-client statements must take the direct route")

        monkeypatch.setattr(driver, "_apply_pending_session_attrs", apply_calls.append)
        monkeypatch.setattr(driver, "_single_call_checked", _spy_checked_single)
        assert await driver.select("SELECT 2 AS a FROM dual") == [{"a": 2}]
        assert apply_calls == []
        assert checked_calls == []
        assert stub.cursor_calls == []
        assert stub.await_counter == [2]
        await driver.close()

    asyncio.run(_run())


def test_async_oracle_driver_pooled_broker_autocommit_cancel_and_fatal_discard(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify pooled async broker leases per autocommit statement, cancel cleanup, and ORA-03113 discard."""

    async def _run() -> None:
        broker = _AsyncRecordingLeaseBroker()
        raw_pool = _StubAsyncOraclePool(min_size=0, max_size=2)
        pool = AsyncOraclePool(native_pool=raw_pool)
        driver = AsyncOracleDriver(pool=pool, broker=broker)

        release_discards: list[bool] = []
        original_release = pool.release_to_pool

        async def _spy_release(client: Any, **kwargs: Any) -> None:
            release_discards.append(bool(kwargs.get("discard", False)))
            await original_release(client, **kwargs)

        monkeypatch.setattr(pool, "release_to_pool", _spy_release)

        assert await driver.execute("UPDATE items SET n = 1") == 1
        raw_pool._idle[0].queued_results.append(
            _oracle.OracleQueryResult([_oracle.OracleColumn("A", "NUMBER")], [(1,)], rowcount=1)
        )
        assert await driver.select("SELECT 1 AS a FROM dual") == [{"a": 1}]
        raw_pool._idle[0].queued_results.append(
            _oracle.OracleQueryResult([_oracle.OracleColumn("A", "NUMBER")], [(9,)], rowcount=1)
        )
        assert await driver.select_value("SELECT 9 AS a FROM dual") == 9

        assert len(broker.acquire_calls) == 3
        assert len(broker.release_calls) == 3
        assert raw_pool.single_calls == []
        assert release_discards == [False, False, False]

        entered = asyncio.Event()
        idle_client = raw_pool._idle[0]
        original_fetch_value = idle_client.fetch_value

        async def _blocking_fetch_value(*args: Any, **kwargs: Any) -> Any:
            entered.set()
            await asyncio.sleep(60)
            return await original_fetch_value(*args, **kwargs)

        monkeypatch.setattr(idle_client, "fetch_value", _blocking_fetch_value)
        task = asyncio.create_task(driver.select_value("SELECT 1 FROM dual"))
        await entered.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        monkeypatch.setattr(idle_client, "fetch_value", original_fetch_value)

        assert len(broker.acquire_calls) == 4
        assert len(broker.release_calls) == 4
        assert pool.borrowed_size == 0
        assert pool.idle_size == 1

        fatal_err = OperationalError("ORA-03113: end-of-file on communication channel")
        fatal_err.vendor_code = 3113
        fatal_err.is_transient = True
        raw_pool._idle[0].queued_results.append(fatal_err)
        with pytest.raises(OperationalError, match="ORA-03113"):
            await driver.execute("SELECT 1 FROM dual")

        assert len(broker.acquire_calls) == 5
        assert len(broker.release_calls) == 5
        assert release_discards == [False, False, False, False, True]
        assert pool.idle_size == 0
        assert pool.borrowed_size == 0
        await driver.close()
        await pool.close()

    asyncio.run(_run())
