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

import vertebra._oracle as _oracle
from vertebra._vertebra import VectorView
from vertebra.db.adapters.oracle import (
    AsyncOracleDriver,
    AsyncOraclePool,
    SyncOracleDriver,
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
    InterfaceError,
    PoolExhaustedError,
    ProgrammingError,
    QueryCancelledError,
)

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




