"""Unit tests for the native Oracle database adapter, dialer, worker bridge, codecs, pools, and tenancy."""

from __future__ import annotations

import asyncio
import datetime
import io
import threading
import time
import uuid
import zipfile
from decimal import Decimal
from typing import TYPE_CHECKING, Any

import msgspec
import pyarrow as pa
import pytest
from sqlspec import StatementStack
from sqlspec.core.parameters import ParameterStyle

import vertebra._oracle as _oracle
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
    OracleTopologyConfig,
    OracleWallet,
)
from vertebra.db.adapters.oracle.core import OracleCore
from vertebra.db.driver import (
    DataError,
    IntegrityError,
    InterfaceError,
    OperationalError,
    PoolExhaustedError,
    ProgrammingError,
    QueryCancelledError,
)
from vertebra.db.driver.capabilities import AsyncPipelineCapable, SyncPipelineCapable
from vertebra.db.rls import AsyncTenantConnectionScope, TenantConnectionScope, TenantPredicate, TenantScopeConfig
from vertebra.extensions.sqlspec.adapters import OracleAsyncDriver, OracleSyncDriver

if TYPE_CHECKING:
    from pathlib import Path


def _build_wallet_zip_bytes() -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr(
            "tnsnames.ora",
            (
                "mydb_high = (DESCRIPTION=(ADDRESS=(PROTOCOL=TCPS)(PORT=1522)"
                "(HOST=adb.us-ashburn-1.oraclecloud.com))"
                "(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=g1_mydb_high.adb.oraclecloud.com))"
                '(SECURITY=(SSL_SERVER_CERT_DN="CN=adb.us-ashburn-1.oraclecloud.com")))\n'
            ),
        )
        zf.writestr("sqlnet.ora", "WALLET_LOCATION = (SOURCE = (METHOD = file))")
        zf.writestr(
            "ewallet.pem",
            b"-----BEGIN CERTIFICATE-----\nMIIB...\n-----END CERTIFICATE-----\n",
        )
    return buf.getvalue()


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
    """Verify 4-tier priority order in `OracleConfig.connect_descriptor()`."""
    cfg = OracleConfig(**kwargs)
    assert DEFAULT_ORACLE_DSN == "oracle://localhost:1521/FREEPDB1"
    desc = cfg.connect_descriptor()
    assert isinstance(desc, OracleConnectDescriptor)
    assert desc.host == expected_host
    assert desc.port == expected_port
    assert desc.service_name == expected_service
    assert desc.sid == expected_sid
    assert desc.protocol == expected_protocol


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
        access_token="ya29-sample",
    )
    path_desc = path_cfg.connect_descriptor()
    assert path_desc.host == "adb.us-ashburn-1.oraclecloud.com"
    assert path_cfg.drcp.cclass == "APP_POOL"
    assert path_cfg.drcp.purity == "SELF"
    assert path_cfg.drcp.connection_tag == "ROLE=ADMIN"
    assert path_cfg.auth_mode == "token"
    assert path_cfg.access_token == "ya29-sample"
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


def test_sync_oracle_client_cancel_transactions_and_post_close() -> None:
    """Verify SyncOracleClient out-of-band cancel(), transaction/savepoint state, and post-close errors."""
    client = _oracle.SyncOracleClient(host="memory", service_name="FREEPDB1")
    client.cancel()

    def _trigger_cancel() -> None:
        time.sleep(0.03)
        client.cancel()

    canceller = threading.Thread(target=_trigger_cancel)
    canceller.start()
    with pytest.raises(QueryCancelledError) as exc_info:
        client.execute("SELECT /*+ VTB_SLEEP_MS(2000) */ 1 FROM DUAL")
    canceller.join()
    assert exc_info.value.sqlstate == "57014"
    assert exc_info.value.vendor_code == 1013

    assert client.in_transaction is False
    client.begin()
    assert client.in_transaction is True
    client.savepoint("sp_1")
    client.rollback_to_savepoint("sp_1")
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

    client.close()
    assert client.closed is True
    client.close()
    client.cancel()
    with pytest.raises(InterfaceError, match="closed"):
        client.execute("SELECT 1 FROM DUAL")


@pytest.mark.anyio
async def test_async_oracle_client_drainer_cancel_and_lifecycle() -> None:
    """Verify AsyncOracleClient completion drainer, out-of-band cancel(), and task cancellation."""
    client = _oracle.AsyncOracleClient(host="memory", service_name="FREEPDB1")
    await client.begin()
    assert client.in_transaction is True

    loop = asyncio.get_running_loop()
    assert hasattr(loop, "_vertebra_oracle_completion_drainer")

    await client.savepoint("sp_async_1")
    await client.rollback_to_savepoint("sp_async_1")
    await client.commit()
    assert client.in_transaction is False

    async def _delayed_cancel() -> None:
        await asyncio.sleep(0.03)
        client.cancel()

    cancel_task = asyncio.create_task(_delayed_cancel())
    with pytest.raises(QueryCancelledError) as exc_info:
        await client.execute("SELECT /*+ VTB_SLEEP_MS(2000) */ 1 FROM DUAL")
    await cancel_task
    assert exc_info.value.sqlstate == "57014"
    assert exc_info.value.vendor_code == 1013

    fut = asyncio.ensure_future(client.execute("SELECT /*+ VTB_SLEEP_MS(2000) */ 1 FROM DUAL"))
    await asyncio.sleep(0.02)
    fut.cancel()
    with pytest.raises(asyncio.CancelledError):
        await fut

    exec_res = await client.execute("SELECT 1 FROM DUAL")
    assert isinstance(exec_res, _oracle.OracleQueryResult)
    assert exec_res.rowcount == 1
    await client.close()
    assert client.closed is True
    with pytest.raises(InterfaceError, match="closed"):
        await client.execute("SELECT 1 FROM DUAL")


def test_oracle_column_and_query_result_pyclasses() -> None:
    """Verify OracleColumn and OracleQueryResult PyO3 classes, methods, and immutability."""
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



class _EmployeeRow(msgspec.Struct, frozen=True, gc=False):
    employee_id: int
    first_name: str


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("sql", "params"),
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
        ),
    ],
)
async def test_oracle_positional_and_named_bind_lowering(
    sql: str,
    params: Any,
) -> None:
    """Verify positional (:1..:n) and named (:name / id) bind lowering across SyncOracleDriver and AsyncOracleDriver."""
    expected_uuid = uuid.UUID("12345678-1234-5678-1234-567812345678")
    vv_cfg = OracleConfig(driver_features=OracleDriverFeatures(vector_return_format="vector_view"))
    with SyncOracleDriver(vv_cfg) as sync_driver:
        rows = sync_driver.select(sql, params)
        assert len(rows) == 1
        row = rows[0]
        assert row["id"] == 42
        assert row["name"] == "Ada"
        assert row["uid_raw"] == expected_uuid
        assert row["doc"] == {"role": "admin"}
        assert isinstance(row["emb"], VectorView)
        assert row["emb"].to_list() == [1.0, 2.0]

    async with AsyncOracleDriver(vv_cfg) as async_driver:
        async_rows = await async_driver.select(sql, params)
        assert len(async_rows) == 1
        arow = async_rows[0]
        assert arow["id"] == 42
        assert arow["name"] == "Ada"
        assert arow["uid_raw"] == expected_uuid
        assert arow["doc"] == {"role": "admin"}
        assert isinstance(arow["emb"], VectorView)
        assert arow["emb"].to_list() == [1.0, 2.0]


@pytest.mark.anyio
async def test_oracle_dml_returning_into_single_round_trip() -> None:
    """Verify DML ... RETURNING ... INTO captures out_binds and projects rows in a single round-trip."""
    dml_sql = "INSERT INTO items (name) VALUES (:name) RETURNING id, name INTO :out_id, :out_name"
    with SyncOracleDriver() as sync_driver:
        first = sync_driver.select_one(dml_sql, {"name": "widget"})
        assert first == {"id": 101, "name": "widget"}
        tuples = sync_driver.execute_tuples(dml_sql, {"name": "gadget"})
        assert tuples == [(102, "gadget")]
        raw_res = sync_driver.client.execute(dml_sql, {"name": "sprocket"})
        assert raw_res.out_binds == {"out_id": 103, "out_name": "sprocket"}

    async with AsyncOracleDriver() as async_driver:
        async_first = await async_driver.select_one(dml_sql, {"name": "widget"})
        assert async_first == {"id": 101, "name": "widget"}
        ret_rows = await async_driver.exec_returning_async(dml_sql, {"name": "gadget"})
        assert ret_rows == [{"id": 102, "name": "gadget"}]


@pytest.mark.anyio
async def test_oracle_execute_many_batches_and_empty_short_circuit() -> None:
    """Verify execute_many batches rows on the worker and short-circuits empty sequences."""
    batch = [(1, "a"), (2, "b"), (3, "c")]
    with SyncOracleDriver() as sync_driver:
        sync_driver.execute("CREATE TABLE items (id NUMBER PRIMARY KEY, name VARCHAR2(64))")
        assert sync_driver.execute_many("INSERT INTO items (id, name) VALUES (:1, :2)", []) == 0
        affected = sync_driver.execute_many("INSERT INTO items (id, name) VALUES (:1, :2)", batch)
        assert affected == 3
        assert sync_driver.select_value("SELECT COUNT(*) FROM items") == 3

    async with AsyncOracleDriver() as async_driver:
        await async_driver.execute("CREATE TABLE items (id NUMBER PRIMARY KEY, name VARCHAR2(64))")
        assert await async_driver.execute_many("INSERT INTO items (id, name) VALUES (:1, :2)", []) == 0
        async_affected = await async_driver.execute_many(
            "INSERT INTO items (id, name) VALUES (:1, :2)",
            batch,
        )
        assert async_affected == 3
        assert await async_driver.select_value("SELECT COUNT(*) FROM items") == 3


@pytest.mark.anyio
async def test_oracle_lowercase_column_names_toggle() -> None:
    """Verify enable_lowercase_column_names=True (default) vs False across description, dicts, and structs."""
    query = "SELECT 1 AS EMPLOYEE_ID, 'Ada' AS FIRST_NAME FROM DUAL"
    with SyncOracleDriver(
        OracleConfig(driver_features=OracleDriverFeatures(enable_lowercase_column_names=True))
    ) as lower_driver:
        rows, cols = lower_driver.execute_with_description(query)
        assert cols == ("employee_id", "first_name")
        assert rows == [(1, "Ada")]
        assert lower_driver.select(query) == [{"employee_id": 1, "first_name": "Ada"}]
        assert lower_driver.select(query, schema_type=_EmployeeRow) == [
            _EmployeeRow(employee_id=1, first_name="Ada")
        ]

    with SyncOracleDriver(
        OracleConfig(driver_features=OracleDriverFeatures(enable_lowercase_column_names=False))
    ) as upper_driver:
        rows_u, cols_u = upper_driver.execute_with_description(query)
        assert cols_u == ("EMPLOYEE_ID", "FIRST_NAME")
        assert rows_u == [(1, "Ada")]
        assert upper_driver.select(query) == [{"EMPLOYEE_ID": 1, "FIRST_NAME": "Ada"}]

    async with AsyncOracleDriver(
        OracleConfig(driver_features=OracleDriverFeatures(enable_lowercase_column_names=False))
    ) as async_upper:
        _, acols_u = await async_upper.execute_with_description(query)
        assert acols_u == ("EMPLOYEE_ID", "FIRST_NAME")
        assert await async_upper.select(query) == [{"EMPLOYEE_ID": 1, "FIRST_NAME": "Ada"}]


@pytest.mark.anyio
async def test_oracle_arrow59_inline_and_shm_escalation() -> None:
    """Verify Arrow 59 inline vs 16-byte VTBARROW SHM escalation across SyncOracleDriver and AsyncOracleDriver."""
    query = "SELECT LEVEL AS ID, 'item_' AS NAME FROM DUAL CONNECT BY LEVEL <= 5"
    with SyncOracleDriver() as sync_driver:
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

    async with AsyncOracleDriver() as async_driver:
        with await async_driver.select_to_arrow(query, force_shm=False) as a_inline:
            assert a_inline.used_shm is False
            assert len(a_inline) == 5

        with await async_driver.select_to_arrow(query, force_shm=True) as a_shm:
            assert a_shm.used_shm is True
            assert len(a_shm) == 5
            ashm_tbl = pa.ipc.open_stream(pa.py_buffer(a_shm.buffer_view())).read_all()
            assert ashm_tbl.column_names == ["id", "name"]
            assert ashm_tbl.num_rows == 5


@pytest.mark.anyio
async def test_oracle_stream_arrow_batches_sync_and_async() -> None:
    """Verify stream_arrow_batches yields chunked ArrowCursor batches with matching IPC streams."""
    query = "SELECT LEVEL AS ID, 'item' AS NAME FROM DUAL CONNECT BY LEVEL <= 5"
    with SyncOracleDriver() as sync_driver:
        sync_batches = list(sync_driver.stream_arrow_batches(query, batch_size=2))
        assert [len(b) for b in sync_batches] == [2, 2, 1]
        assert sum(len(b.buffer_view()) for b in sync_batches) > 0

    async with AsyncOracleDriver() as async_driver:
        async_lengths: list[int] = []
        async for batch in async_driver.stream_arrow_batches(query, batch_size=2):
            with batch:
                async_lengths.append(len(batch))
                assert len(batch.buffer_view()) > 0
        assert async_lengths == [2, 2, 1]


@pytest.mark.anyio
async def test_oracle_load_from_arrow_direct_path_and_identifier_validation() -> None:
    """Verify load_from_arrow APPEND_VALUES hint precedence, empty short-circuit, and identifier validation."""
    table = pa.Table.from_pydict({"id": [1, 2], "name": ["a", "b"]})
    empty_table = pa.Table.from_pydict({"id": [], "name": []})

    with SyncOracleDriver() as sync_driver:
        sync_driver.execute("CREATE TABLE items (id NUMBER PRIMARY KEY, name VARCHAR2(64))")
        assert sync_driver.load_from_arrow("items", empty_table) == 0
        loaded = sync_driver.load_from_arrow("items", table, direct_path=True)
        assert loaded == 2
        loaded_std = sync_driver.load_from_arrow(
            "items",
            [{"id": 3, "name": "c"}],
            direct_path=False,
        )
        assert loaded_std == 1
        assert sync_driver.select_value("SELECT COUNT(*) FROM items") == 3

        with pytest.raises((ValueError, ProgrammingError)):
            sync_driver.load_from_arrow("bad;table", table)
        with pytest.raises((ValueError, ProgrammingError)):
            sync_driver.load_from_arrow("items", table, columns=("id", "bad;col"))

    async with AsyncOracleDriver() as async_driver:
        await async_driver.execute("CREATE TABLE items (id NUMBER PRIMARY KEY, name VARCHAR2(64))")
        assert await async_driver.load_from_arrow("items", empty_table) == 0
        async_loaded = await async_driver.load_from_arrow("items", table, direct_path=True)
        assert async_loaded == 2
        with pytest.raises((ValueError, ProgrammingError)):
            await async_driver.load_from_arrow("bad;table", table)


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
    """Verify SyncOraclePool and AsyncOraclePool construct with zero I/O and reject invalid bounds."""
    start = time.perf_counter()
    pool = _oracle.SyncOraclePool(host="memory", min_size=1, max_size=4)
    elapsed_ms = (time.perf_counter() - start) * 1000.0
    assert elapsed_ms < 25.0
    assert pool.min_size == 1
    assert pool.max_size == 4
    assert pool.size == 0
    assert pool.idle_size == 0
    assert pool.borrowed_size == 0
    assert pool.closed is False
    stats = pool.stats()
    assert stats["min_size"] == 1
    assert stats["max_size"] == 4
    assert stats["size"] == 0
    assert stats["idle_size"] == 0
    assert stats["borrowed_size"] == 0
    pool.close()
    assert pool.closed is True

    with pytest.raises((InterfaceError, ValueError)):
        _oracle.SyncOraclePool(host="memory", **bad_kwargs)
    with pytest.raises((InterfaceError, ValueError)):
        _oracle.AsyncOraclePool(host="memory", **bad_kwargs)


@pytest.mark.anyio
async def test_oracle_pool_acquire_release_and_exhaustion_timeout() -> None:
    """Verify bounded slot reservation and PoolExhaustedError (53300) on acquire timeout."""
    pool = _oracle.SyncOraclePool(host="memory", min_size=0, max_size=1, acquire_timeout=0.04)
    c1 = pool.acquire()
    assert pool.size == 1
    assert pool.borrowed_size == 1
    assert pool.idle_size == 0

    with pytest.raises(PoolExhaustedError) as exc_info:
        pool.acquire(timeout=0.03)
    assert getattr(exc_info.value, "sqlstate", None) == "53300"
    assert getattr(exc_info.value, "is_transient", None) is True

    pool.release(c1)
    assert pool.borrowed_size == 0
    assert pool.idle_size == 1
    pool.close()
    with pytest.raises(InterfaceError):
        pool.acquire()

    apool = _oracle.AsyncOraclePool(host="memory", min_size=0, max_size=1, acquire_timeout=0.04)
    ac1 = await apool.acquire()
    assert apool.borrowed_size == 1
    with pytest.raises(PoolExhaustedError) as aexc:
        await apool.acquire(timeout=0.03)
    assert getattr(aexc.value, "sqlstate", None) == "53300"
    assert getattr(aexc.value, "is_transient", None) is True
    await apool.release(ac1)
    assert apool.idle_size == 1
    await apool.close()


@pytest.mark.anyio
async def test_oracle_pool_rollback_on_release_and_evict_on_broken_session() -> None:
    """Verify release() rolls back dirty transactions and evicts closed or discarded sessions."""
    pool = _oracle.SyncOraclePool(host="memory", min_size=0, max_size=2)
    client = pool.acquire()
    client.execute("CREATE TABLE t_pool (id NUMBER PRIMARY KEY, name VARCHAR2(32))")
    client.begin()
    client.execute("INSERT INTO t_pool (id, name) VALUES (1, 'uncommitted')")
    assert client.in_transaction is True

    pool.release(client)
    assert pool.idle_size == 1
    assert pool.borrowed_size == 0

    reused = pool.acquire()
    assert reused.in_transaction is False
    assert reused.query("SELECT COUNT(*) FROM t_pool").rows[0][0] == 0

    pool.release(reused, discard=True)
    assert pool.size == 0
    assert pool.idle_size == 0
    assert reused.closed is True

    c_evict = pool.acquire()
    assert pool.size == 1
    pool.evict(c_evict)
    assert pool.size == 0
    assert c_evict.closed is True
    pool.close()


def test_oracle_pool_max_lifetime_and_idle_timeout_eviction() -> None:
    """Verify expired idle sessions are closed and replaced on subsequent checkout."""
    pool = _oracle.SyncOraclePool(
        host="memory",
        min_size=0,
        max_size=2,
        idle_timeout_seconds=0.02,
    )
    first = pool.acquire()
    pool.release(first)
    assert pool.idle_size == 1

    time.sleep(0.03)
    second = pool.acquire()
    assert first.closed is True
    assert second.closed is False
    assert pool.size == 1
    assert pool.borrowed_size == 1
    pool.release(second)
    pool.close()


@pytest.mark.anyio
async def test_oracle_drcp_purity_validation_and_tag_matching() -> None:
    """Verify DRCP cclass, purity (SELF vs NEW), and connection_tag matching on pool checkout."""
    pool = _oracle.SyncOraclePool(
        host="memory",
        cclass="VTB_POOL",
        purity="SELF",
        min_size=0,
        max_size=2,
    )
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

    apool = _oracle.AsyncOraclePool(
        host="memory",
        cclass="VTB_ASYNC_POOL",
        purity="SELF",
        min_size=0,
        max_size=2,
    )
    ac1 = await apool.acquire_from_pool()
    await apool.release_to_pool(ac1, connection_tag="ASYNC_TAG")
    ac_matched = await apool.acquire_from_pool(connection_tag="ASYNC_TAG", purity="SELF")
    assert ac_matched.tag_matched is True
    assert ac_matched.connection_tag == "ASYNC_TAG"
    await apool.release_to_pool(ac_matched)
    await apool.close()


@pytest.mark.anyio
async def test_oracle_ttc_piggyback_stages_on_checkout_and_flushes_on_first_query() -> None:
    """Verify OracleSessionAttrs stages on checkout and flushes [0x11, 0x6e] on the first statement."""
    pool = _oracle.SyncOraclePool(host="memory", min_size=0, max_size=2)
    client = pool.acquire(
        client_identifier="tenant-42",
        module="billing",
        action="invoice",
        ecid="req-99",
    )
    assert client.pending_session_attrs == {
        "client_identifier": "tenant-42",
        "module": "billing",
        "action": "invoice",
        "ecid": "req-99",
    }
    assert client.last_piggyback_frame is None

    res = client.query("SELECT SYS_CONTEXT('USERENV', 'CLIENT_IDENTIFIER') AS CID FROM DUAL")
    assert res.rows == [("tenant-42",)]
    assert client.pending_session_attrs is None
    assert client.active_session_attrs == {
        "client_identifier": "tenant-42",
        "module": "billing",
        "action": "invoice",
        "ecid": "req-99",
    }
    frame = client.last_piggyback_frame
    assert frame is not None
    assert frame.startswith(b"\x11\x6e\x04")
    pool.release(client)
    pool.close()

    apool = _oracle.AsyncOraclePool(host="memory", min_size=0, max_size=2)
    aclient = await apool.acquire(
        client_identifier="tenant-async",
        module="orders",
        action="checkout",
        ecid="req-100",
    )
    assert aclient.pending_session_attrs == {
        "client_identifier": "tenant-async",
        "module": "orders",
        "action": "checkout",
        "ecid": "req-100",
    }
    await aclient.query("SELECT 1 FROM DUAL")
    assert aclient.pending_session_attrs is None
    assert aclient.active_session_attrs["client_identifier"] == "tenant-async"
    assert aclient.last_piggyback_frame is not None
    assert aclient.last_piggyback_frame.startswith(b"\x11\x6e\x04")
    await apool.release(aclient)
    await apool.close()


def test_oracle_multiple_set_session_attrs_coalesce_before_statement() -> None:
    """Verify multiple set_session_attrs calls coalesce into a single [0x11, 0x6e] frame before execute."""
    client = _oracle.SyncOracleClient(host="memory")
    client.execute("CREATE TABLE t_piggy (x NUMBER)")
    client.set_session_attrs(client_identifier="t1", module="m1")
    client.set_session_attrs(action="a1")
    assert client.pending_session_attrs == {
        "client_identifier": "t1",
        "module": "m1",
        "action": "a1",
        "ecid": None,
    }
    client.execute("UPDATE t_piggy SET x = 1")
    assert client.pending_session_attrs is None
    assert client.active_session_attrs == {
        "client_identifier": "t1",
        "module": "m1",
        "action": "a1",
        "ecid": None,
    }
    assert client.last_piggyback_frame is not None
    assert client.last_piggyback_frame.startswith(b"\x11\x6e\x03")
    client.close()


@pytest.mark.anyio
async def test_python_oracle_pool_lease_and_driver_piggyback() -> None:
    """Verify SyncOraclePool and AsyncOraclePool lease() checkout and driver last_piggyback_frame."""
    with SyncOraclePool(min_size=0, max_size=2) as pool:
        assert pool.min_size == 0
        assert pool.max_size == 2
        assert pool.closed is False
        assert pool.is_closed is False
        with pool.lease() as drv:
            assert pool.borrowed_size == 1
            drv.set_session_attrs(client_identifier="t-1", module="api")
            val = drv.select_value("SELECT 1 FROM DUAL")
            assert val == 1
            frame = drv.last_piggyback_frame
            assert frame is not None
            assert frame.startswith(b"\x11\x6e")
        assert pool.borrowed_size == 0
        assert pool.idle_size == 1
    assert pool.closed is True
    assert pool.is_closed is True

    async with AsyncOraclePool(min_size=0, max_size=2) as apool:
        async with apool.lease() as adrv:
            assert apool.borrowed_size == 1
            adrv.set_session_attrs(client_identifier="t-async", module="worker")
            aval = await adrv.select_value("SELECT 1 FROM DUAL")
            assert aval == 1
            aframe = adrv.last_piggyback_frame
            assert aframe is not None
            assert aframe.startswith(b"\x11\x6e")
        assert apool.borrowed_size == 0
        assert apool.idle_size == 1
    assert apool.closed is True


@pytest.mark.anyio
async def test_oracle_driver_transaction_pins_lease_and_guards_child_tasks() -> None:
    """Verify AsyncOracleDriver transaction() pins a dedicated lease and isolates spawned child tasks."""
    async with AsyncOraclePool(min_size=0, max_size=2) as apool:
        driver = AsyncOracleDriver(pool=apool)
        assert driver.in_transaction is False

        async with driver.transaction():
            assert driver.in_transaction is True
            assert apool.borrowed_size == 1
            pinned_client = driver.client
            await driver.execute("CREATE TABLE tx_guard (id NUMBER PRIMARY KEY, name VARCHAR2(32))")
            await driver.execute("INSERT INTO tx_guard (id, name) VALUES (1, 'parent')")
            assert driver.client is pinned_client

            async def _child_query() -> tuple[bool, int, Any, list[dict[str, Any]]]:
                in_tx = driver.in_transaction
                rows = await driver.select("SELECT 1 AS ok FROM DUAL")
                return in_tx, apool.size, driver.client, rows

            child_in_tx, total_size, child_default_client, child_rows = await asyncio.create_task(_child_query())
            assert child_in_tx is False
            assert child_rows == [{"ok": 1}]
            assert total_size == 2
            assert child_default_client is not pinned_client
            assert apool.borrowed_size == 1
            assert apool.idle_size == 1

            count = await driver.select_value("SELECT COUNT(*) FROM tx_guard")
            assert count == 1

        assert driver.in_transaction is False
        assert apool.borrowed_size == 0
        assert apool.idle_size == 2


@pytest.mark.anyio
async def test_oracle_driver_savepoint_and_rollback_to_savepoint() -> None:
    """Verify savepoint, rollback_to_savepoint, release_savepoint, and identifier validation."""
    with SyncOracleDriver() as sync_driver, sync_driver.transaction():
        sync_driver.execute("CREATE TABLE sp_items (id NUMBER PRIMARY KEY, name VARCHAR2(32))")
        sync_driver.execute("INSERT INTO sp_items (id, name) VALUES (1, 'keep')")
        sync_driver.savepoint("sp_1")
        sync_driver.execute("INSERT INTO sp_items (id, name) VALUES (2, 'discard')")
        assert sync_driver.select_value("SELECT COUNT(*) FROM sp_items") == 2
        sync_driver.rollback_to_savepoint("sp_1")
        assert sync_driver.select_value("SELECT COUNT(*) FROM sp_items") == 1
        sync_driver.release_savepoint("sp_1")

        with pytest.raises(ValueError, match="savepoint"):
            sync_driver.savepoint("bad;name")
        with pytest.raises(ValueError, match="savepoint"):
            sync_driver.rollback_to_savepoint("bad;name")
        with pytest.raises(ValueError, match="savepoint"):
            sync_driver.release_savepoint("bad;name")

    async with AsyncOracleDriver() as async_driver, async_driver.transaction():
        await async_driver.execute("CREATE TABLE sp_items (id NUMBER PRIMARY KEY, name VARCHAR2(32))")
        await async_driver.execute("INSERT INTO sp_items (id, name) VALUES (1, 'keep')")
        await async_driver.savepoint("sp_1")
        await async_driver.execute("INSERT INTO sp_items (id, name) VALUES (2, 'discard')")
        assert await async_driver.select_value("SELECT COUNT(*) FROM sp_items") == 2
        await async_driver.rollback_to_savepoint("sp_1")
        assert await async_driver.select_value("SELECT COUNT(*) FROM sp_items") == 1
        await async_driver.release_savepoint("sp_1")

        with pytest.raises(ValueError, match="savepoint"):
            await async_driver.savepoint("bad;name")
        with pytest.raises(ValueError, match="savepoint"):
            await async_driver.rollback_to_savepoint("bad;name")
        with pytest.raises(ValueError, match="savepoint"):
            await async_driver.release_savepoint("bad;name")


@pytest.mark.anyio
async def test_sync_and_async_pipeline_delayed_commit_on_clean_exit() -> None:
    """Verify SyncOraclePipeline and AsyncOraclePipeline hold transaction across results() and commit on exit."""
    with SyncOracleDriver() as driver:
        assert isinstance(driver, SyncPipelineCapable)
        driver.execute("CREATE TABLE items (id NUMBER PRIMARY KEY, label VARCHAR2(32))")
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
        assert driver.select("SELECT id, label FROM items") == [{"id": 1, "label": "a"}]

    async with AsyncOracleDriver() as async_driver:
        assert isinstance(async_driver, AsyncPipelineCapable)
        await async_driver.execute("CREATE TABLE items (id NUMBER PRIMARY KEY, label VARCHAR2(32))")
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
        assert await async_driver.select("SELECT id, label FROM items") == [{"id": 1, "label": "a"}]


@pytest.mark.anyio
async def test_pipeline_rolls_back_when_caller_raises_after_results() -> None:
    """Verify raising an exception after pipe.results() inside the with block rolls back the batch."""
    with SyncOracleDriver() as driver:
        driver.execute("CREATE TABLE items (id NUMBER PRIMARY KEY, label VARCHAR2(32))")
        with pytest.raises(ValueError, match="hydration failed"), driver.pipeline() as pipe:
            pipe.execute("INSERT INTO items (id, label) VALUES (:1, :2)", (1, "transient"))
            pipe.select("SELECT id, label FROM items WHERE id = :1", (1,))
            res = pipe.results()
            assert res == [1, [{"id": 1, "label": "transient"}]]
            raise ValueError("hydration failed")
        assert driver.in_transaction is False
        assert driver.transaction_events == ("BEGIN", "ROLLBACK")
        assert driver.select("SELECT id, label FROM items") == []

        with pytest.raises(InterfaceError, match="unexecuted operations"), driver.pipeline() as pipe2:
            pipe2.execute("INSERT INTO items (id, label) VALUES (:1, :2)", (2, "unflushed"))
        assert driver.select("SELECT id, label FROM items") == []

    async with AsyncOracleDriver() as async_driver:
        await async_driver.execute("CREATE TABLE items (id NUMBER PRIMARY KEY, label VARCHAR2(32))")
        with pytest.raises(ValueError, match="hydration failed"):
            async with async_driver.pipeline() as apipe:
                apipe.execute("INSERT INTO items (id, label) VALUES (:1, :2)", (1, "transient"))
                apipe.select("SELECT id, label FROM items WHERE id = :1", (1,))
                ares = await apipe.results()
                assert ares == [1, [{"id": 1, "label": "transient"}]]
                raise ValueError("hydration failed")
        assert async_driver.in_transaction is False
        assert async_driver.transaction_events == ("BEGIN", "ROLLBACK")
        assert await async_driver.select("SELECT id, label FROM items") == []


@pytest.mark.anyio
async def test_pipeline_nested_inside_transaction_uses_oracle_savepoint_without_release() -> None:
    """Verify nested pipeline inside transaction() uses SAVEPOINT _vtb_pipe_<n> without RELEASE SAVEPOINT."""
    with SyncOracleDriver() as driver:
        driver.execute("CREATE TABLE items (id NUMBER PRIMARY KEY, label VARCHAR2(32))")
        with driver.transaction():
            driver.execute("INSERT INTO items (id, label) VALUES (1, 'outer')")
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
            assert driver.select("SELECT id, label FROM items ORDER BY id") == [
                {"id": 1, "label": "outer"},
                {"id": 2, "label": "inner_ok"},
            ]
        assert driver.in_transaction is False
        assert list(driver.transaction_events) == [
            "BEGIN",
            "SAVEPOINT _vtb_pipe_1",
            "SAVEPOINT _vtb_pipe_2",
            "ROLLBACK TO SAVEPOINT _vtb_pipe_2",
            "COMMIT",
        ]

    async with AsyncOracleDriver() as async_driver:
        await async_driver.execute("CREATE TABLE items (id NUMBER PRIMARY KEY, label VARCHAR2(32))")
        async with async_driver.transaction():
            await async_driver.execute("INSERT INTO items (id, label) VALUES (1, 'outer')")
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
        assert await async_driver.select("SELECT id, label FROM items ORDER BY id") == [
            {"id": 1, "label": "outer"},
            {"id": 2, "label": "inner_ok"},
        ]


@pytest.mark.anyio
async def test_pipeline_mid_batch_error_marks_subsequent_skipped_and_rolls_back() -> None:
    """Verify mid-batch ORA-00001 failure marks later steps 'skipped', raises IntegrityError, and rolls back."""
    with SyncOracleDriver() as driver:
        driver.execute("CREATE TABLE items (id NUMBER PRIMARY KEY, label VARCHAR2(32))")
        with pytest.raises(IntegrityError) as exc_info, driver.pipeline() as pipe:
            pipe.execute("INSERT INTO items (id, label) VALUES (:1, :2)", (1, "first"))
            pipe.execute("INSERT INTO items (id, label) VALUES (:1, :2)", (1, "duplicate"))
            pipe.execute("INSERT INTO items (id, label) VALUES (:1, :2)", (2, "never_run"))
            pipe.results()
        assert exc_info.value.sqlstate == "23505"
        assert exc_info.value.vendor_code == 1
        statuses = tuple(o.status for o in driver.last_pipeline_outcomes)
        assert statuses == ("completed", "error", "skipped")
        assert driver.transaction_events == ("BEGIN", "ROLLBACK")
        assert driver.select("SELECT id, label FROM items") == []

    async with AsyncOracleDriver() as async_driver:
        await async_driver.execute("CREATE TABLE items (id NUMBER PRIMARY KEY, label VARCHAR2(32))")
        with pytest.raises(IntegrityError) as aexc_info:
            async with async_driver.pipeline() as apipe:
                apipe.execute("INSERT INTO items (id, label) VALUES (:1, :2)", (1, "first"))
                apipe.execute("INSERT INTO items (id, label) VALUES (:1, :2)", (1, "duplicate"))
                apipe.execute("INSERT INTO items (id, label) VALUES (:1, :2)", (2, "never_run"))
                await apipe.results()
        assert aexc_info.value.sqlstate == "23505"
        assert tuple(o.status for o in async_driver.last_pipeline_outcomes) == (
            "completed",
            "error",
            "skipped",
        )
        assert async_driver.transaction_events == ("BEGIN", "ROLLBACK")
        assert await async_driver.select("SELECT id, label FROM items") == []


def test_oracledb_crate_ttc_pipeline_capability_probe() -> None:
    """Verify ORACLESQL_NATIVE_TTC_PIPELINE_SUPPORTED is False on oracledb =26.0.0-beta.5."""
    assert _oracle.ORACLESQL_NATIVE_TTC_PIPELINE_SUPPORTED is False


def test_oracle_core_build_session_and_clear_context_set_identifier_default() -> None:
    """Verify default USERENV namespace builds DBMS_SESSION.SET_IDENTIFIER and CLEAR_IDENTIFIER."""
    cfg = OracleConfig()
    assert cfg.context_namespace == "USERENV"
    assert cfg.context_package_proc is None
    assert cfg.context_clear_proc is None

    set_sql, set_params = OracleCore.build_session_context_sql("acme-42")
    assert set_sql == "BEGIN DBMS_SESSION.SET_IDENTIFIER(:value); END;"
    assert set_params == {"value": "acme-42"}

    clear_sql, clear_params = OracleCore.build_clear_context_sql()
    assert clear_sql == "BEGIN DBMS_SESSION.CLEAR_IDENTIFIER; END;"
    assert clear_params == {}


def test_oracle_core_build_session_and_clear_context_trusted_package() -> None:
    """Verify trusted context package procedures and custom namespaces build parameterized PL/SQL blocks."""
    scope_cfg = TenantScopeConfig(setting_name="app.tenant_id")
    set_sql, set_params = OracleCore.build_session_context_sql(
        "acme-42",
        scope_cfg,
        namespace="VERTEBRA_CTX",
        context_package_proc="APP_SEC.CTX_PKG.SET_ATTR",
    )
    assert set_sql == "BEGIN APP_SEC.CTX_PKG.SET_ATTR(:namespace, :attribute, :value); END;"
    assert set_params == {"namespace": "VERTEBRA_CTX", "attribute": "tenant_id", "value": "acme-42"}

    clear_sql, clear_params = OracleCore.build_clear_context_sql(
        scope_cfg,
        namespace="VERTEBRA_CTX",
        context_clear_proc="APP_SEC.CTX_PKG.CLEAR_ATTR",
    )
    assert clear_sql == "BEGIN APP_SEC.CTX_PKG.CLEAR_ATTR(:namespace, :attribute); END;"
    assert clear_params == {"namespace": "VERTEBRA_CTX", "attribute": "tenant_id"}

    direct_sql, direct_params = OracleCore.build_session_context_sql(
        "acme-42",
        scope_cfg,
        namespace="VERTEBRA_CTX",
    )
    assert direct_sql == "BEGIN DBMS_SESSION.SET_CONTEXT(:namespace, :attribute, :value); END;"
    assert direct_params == {"namespace": "VERTEBRA_CTX", "attribute": "tenant_id", "value": "acme-42"}

    direct_clear_sql, direct_clear_params = OracleCore.build_clear_context_sql(
        scope_cfg,
        namespace="VERTEBRA_CTX",
    )
    assert direct_clear_sql == "BEGIN DBMS_SESSION.CLEAR_CONTEXT(:namespace, NULL, :attribute); END;"
    assert direct_clear_params == {"namespace": "VERTEBRA_CTX", "attribute": "tenant_id"}


@pytest.mark.parametrize(
    "bad_proc",
    [
        "PKG.PROC; DROP USER system",
        "PKG.PROC()",
        "PKG.PROC -- comment",
        "123PKG.PROC",
    ],
)
def test_oracle_core_rejects_malicious_context_package_proc(bad_proc: str) -> None:
    """Verify PL/SQL injection in context_package_proc or context_clear_proc raises ValueError."""
    with pytest.raises(ValueError, match="Invalid"):
        OracleCore.build_session_context_sql("acme-42", context_package_proc=bad_proc)
    with pytest.raises(ValueError, match="Invalid"):
        OracleCore.build_clear_context_sql(context_clear_proc=bad_proc)


@pytest.mark.anyio
async def test_tenant_connection_scope_resets_on_rollback_and_evicts_on_reset_error() -> None:
    """Verify TenantConnectionScope and AsyncTenantConnectionScope reset on rollback and evict on reset failure."""

    class _SyncDriverDouble:
        def __init__(self, *, fail_reset: bool = False) -> None:
            self.fail_reset = fail_reset
            self.events: list[tuple[str, Any]] = []
            self.evicted = False

        def enter_tenant_scope(self, tenant_value: str, config: TenantScopeConfig) -> None:
            sql, params = OracleCore.build_session_context_sql(tenant_value, config)
            self.events.append(("enter", (sql, params)))

        def exit_tenant_scope(self, config: TenantScopeConfig) -> None:
            sql, params = OracleCore.build_clear_context_sql(config)
            self.events.append(("exit", (sql, params)))
            if self.fail_reset:
                err = OperationalError("ORA-03113: end-of-file on communication channel")
                err.sqlstate = "08006"
                err.vendor_code = 3113
                raise err

        def evict_connection(self) -> None:
            self.evicted = True
            self.events.append(("evict", None))

    class _AsyncDriverDouble:
        def __init__(self, *, fail_reset: bool = False) -> None:
            self.fail_reset = fail_reset
            self.events: list[tuple[str, Any]] = []
            self.evicted = False

        async def enter_tenant_scope(self, tenant_value: str, config: TenantScopeConfig) -> None:
            sql, params = OracleCore.build_session_context_sql(tenant_value, config)
            self.events.append(("enter", (sql, params)))

        async def exit_tenant_scope(self, config: TenantScopeConfig) -> None:
            sql, params = OracleCore.build_clear_context_sql(config)
            self.events.append(("exit", (sql, params)))
            if self.fail_reset:
                err = OperationalError("ORA-03113: end-of-file on communication channel")
                err.sqlstate = "08006"
                err.vendor_code = 3113
                raise err

        async def evict_connection(self) -> None:
            self.evicted = True
            self.events.append(("evict", None))

    sync_ok = _SyncDriverDouble(fail_reset=False)
    with pytest.raises(RuntimeError, match="rolled back body"), TenantConnectionScope(sync_ok, "tenant-a"):
        raise RuntimeError("rolled back body")
    assert sync_ok.evicted is False
    assert [e[0] for e in sync_ok.events] == ["enter", "exit"]
    assert sync_ok.events[1][1] == ("BEGIN DBMS_SESSION.CLEAR_IDENTIFIER; END;", {})

    sync_bad = _SyncDriverDouble(fail_reset=True)
    with pytest.raises(OperationalError, match="ORA-03113"), TenantConnectionScope(sync_bad, "tenant-a"):
        pass
    assert sync_bad.evicted is True
    assert [e[0] for e in sync_bad.events] == ["enter", "exit", "evict"]

    async_ok = _AsyncDriverDouble(fail_reset=False)
    with pytest.raises(RuntimeError, match="async body failure"):
        async with AsyncTenantConnectionScope(async_ok, "tenant-a"):
            raise RuntimeError("async body failure")
    assert async_ok.evicted is False
    assert [e[0] for e in async_ok.events] == ["enter", "exit"]

    async_bad = _AsyncDriverDouble(fail_reset=True)
    with pytest.raises(OperationalError, match="ORA-03113"):
        async with AsyncTenantConnectionScope(async_bad, "tenant-a"):
            pass
    assert async_bad.evicted is True
    assert [e[0] for e in async_bad.events] == ["enter", "exit", "evict"]


@pytest.mark.anyio
async def test_sqlspec_oracle_drivers_declare_named_colon_and_compile_stack() -> None:
    """Verify OracleSyncDriver and OracleAsyncDriver declare NAMED_COLON and execute_stack uses pipeline()."""
    assert OracleSyncDriver._execution_parameter_style == ParameterStyle.NAMED_COLON
    assert OracleAsyncDriver._execution_parameter_style == ParameterStyle.NAMED_COLON

    with SyncOracleDriver() as native_sync:
        native_sync.execute("CREATE TABLE items (id NUMBER PRIMARY KEY, label VARCHAR2(32))")
        sync_adapter = OracleSyncDriver(native_sync)
        stmt = sync_adapter.prepare_statement("SELECT id, label FROM items WHERE id = ?", (7,))
        compiled_sql, compiled_params = stmt.compile()
        assert "?" not in compiled_sql
        assert ":" in compiled_sql
        assert compiled_params

        stack = (
            StatementStack()
            .push_execute("INSERT INTO items (id, label) VALUES (?, ?)", 7, "seven")
            .push_execute("SELECT id, label FROM items WHERE id = ?", 7)
        )
        stack_results = sync_adapter.execute_stack(stack)
        assert len(stack_results) == 2
        assert stack_results[0].rows_affected == 1
        assert stack_results[1].result is not None
        assert stack_results[1].result.data == [{"id": 7, "label": "seven"}]

    async with AsyncOracleDriver() as native_async:
        await native_async.execute("CREATE TABLE items (id NUMBER PRIMARY KEY, label VARCHAR2(32))")
        async_adapter = OracleAsyncDriver(native_async)
        astack = (
            StatementStack()
            .push_execute("INSERT INTO items (id, label) VALUES (?, ?)", 9, "nine")
            .push_execute("SELECT id, label FROM items WHERE id = ?", 9)
        )
        astack_results = await async_adapter.execute_stack(astack)
        assert len(astack_results) == 2
        assert astack_results[0].rows_affected == 1
        assert astack_results[1].result is not None
        assert astack_results[1].result.data == [{"id": 9, "label": "nine"}]


@pytest.mark.anyio
async def test_sqlspec_oracle_apply_tenant_predicate_uses_trusted_package_or_set_identifier() -> None:
    """Verify apply_tenant_predicate and driver tenant scope hooks use SET_IDENTIFIER or trusted package."""
    with SyncOracleDriver(OracleConfig(host="memory")) as sync_default:
        sync_adapter_default = OracleSyncDriver(sync_default)
        pred_sql = sync_adapter_default.apply_tenant_predicate(TenantPredicate(tenant_value="t-99"))
        assert pred_sql == "tenant_id = :tenant_id"
        assert sync_default.select_value("SELECT SYS_CONTEXT('USERENV', 'CLIENT_IDENTIFIER') FROM DUAL") == "t-99"

        with TenantConnectionScope(sync_default, "scoped-sync"):
            assert (
                sync_default.select_value("SELECT SYS_CONTEXT('USERENV', 'CLIENT_IDENTIFIER') FROM DUAL")
                == "scoped-sync"
            )
        assert sync_default.select_value("SELECT SYS_CONTEXT('USERENV', 'CLIENT_IDENTIFIER') FROM DUAL") is None

    trusted_cfg = OracleConfig(
        host="memory",
        context_namespace="APP_CTX",
        context_package_proc="SEC_PKG.SET_CTX",
        context_clear_proc="SEC_PKG.CLEAR_CTX",
    )
    with SyncOracleDriver(trusted_cfg) as sync_trusted:
        sync_adapter_trusted = OracleSyncDriver(sync_trusted)
        pred_trusted = sync_adapter_trusted.apply_tenant_predicate(TenantPredicate(tenant_value="t-trusted"))
        assert pred_trusted == "tenant_id = :tenant_id"
        assert sync_trusted.select_value("SELECT SYS_CONTEXT('APP_CTX', 'TENANT_ID') FROM DUAL") == "t-trusted"

        with TenantConnectionScope(sync_trusted, "scoped-trusted"):
            assert (
                sync_trusted.select_value("SELECT SYS_CONTEXT('APP_CTX', 'TENANT_ID') FROM DUAL")
                == "scoped-trusted"
            )
        assert sync_trusted.select_value("SELECT SYS_CONTEXT('APP_CTX', 'TENANT_ID') FROM DUAL") is None

    async with AsyncOracleDriver(trusted_cfg) as async_trusted:
        async_adapter_trusted = OracleAsyncDriver(async_trusted)
        apred = await async_adapter_trusted.apply_tenant_predicate(TenantPredicate(tenant_value="async-trusted"))
        assert apred == "tenant_id = :tenant_id"
        assert (
            await async_trusted.select_value("SELECT SYS_CONTEXT('APP_CTX', 'TENANT_ID') FROM DUAL")
            == "async-trusted"
        )
        async with AsyncTenantConnectionScope(async_trusted, "async-scoped"):
            assert (
                await async_trusted.select_value("SELECT SYS_CONTEXT('APP_CTX', 'TENANT_ID') FROM DUAL")
                == "async-scoped"
            )
        assert await async_trusted.select_value("SELECT SYS_CONTEXT('APP_CTX', 'TENANT_ID') FROM DUAL") is None


