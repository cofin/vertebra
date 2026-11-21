"""Live Oracle 26ai native driver integration suite."""

from __future__ import annotations

import asyncio
import inspect
import os
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import anyio
import msgspec
import pytest

from vertebra._vertebra import VectorView, encode_oracle_vector_i8
from vertebra.db.adapters.oracle.config import OracleConfig, OracleDriverFeatures
from vertebra.db.adapters.oracle.driver import (
    AsyncOracleDriver,
    AsyncOraclePool,
    SyncOracleDriver,
    SyncOraclePool,
)
from vertebra.db.adapters.oracle.embeddings import OracleEmbeddingPushdown
from vertebra.db.adapters.oracle.notify import AsyncOracleNotificationSource, OracleNotifyConfig
from vertebra.db.arrow import ARROW_SHM_THRESHOLD_ROWS
from vertebra.db.driver import (
    DatabaseError,
    IntegrityError,
    InterfaceError,
    NotFoundError,
    QueryCancelledError,
)
from vertebra.db.rls import AsyncTenantConnectionScope, TenantConnectionScope
from vertebra.utils.module_loader import ensure_module

pytestmark = [pytest.mark.driver_engine("oracle")]


def _resolve_oracle_config(request: pytest.FixtureRequest) -> OracleConfig:
    """Resolve a live `OracleConfig` from `VERTEBRA_CONFORMANCE_ORACLE_DSN` or the Docker fixture."""
    ensure_module(
        "vertebra._oracle",
        package="vertebra-core[oracle]",
        install_package="vertebra-core[oracle]",
        extra="oracle",
    )
    dsn = os.environ.get("VERTEBRA_CONFORMANCE_ORACLE_DSN", "").strip()
    if dsn:
        return OracleConfig.from_url(dsn)
    selected_engines = {
        token.strip().lower()
        for token in os.environ.get("VERTEBRA_CONFORMANCE_ENGINES", "").split(",")
        if token.strip()
    }
    if os.environ.get("VERTEBRA_CONFORMANCE_DOCKER") == "1" or "oracle" in selected_engines:
        fixture_cfg = request.getfixturevalue("vtb_oracle_config")
        assert isinstance(fixture_cfg, OracleConfig)
        return fixture_cfg
    pytest.skip(
        "Oracle 26ai integration tests require VERTEBRA_CONFORMANCE_ORACLE_DSN or VERTEBRA_CONFORMANCE_DOCKER=1"
    )


@pytest.fixture
def oracle_config(request: pytest.FixtureRequest) -> OracleConfig:
    """Return the resolved live `OracleConfig` for native Oracle 26ai integration tests."""
    return _resolve_oracle_config(request)


@pytest.mark.anyio
async def test_oracle_26ai_oson_and_vector_roundtrip_and_similarity_search(
    oracle_config: OracleConfig,
) -> None:
    """Verify Oracle 26ai `JSON`/`OSON` and `VECTOR` roundtrips plus `OracleEmbeddingPushdown.similarity_search`."""
    view_cfg = msgspec.structs.replace(
        oracle_config,
        driver_features=OracleDriverFeatures(vector_return_format="vector_view"),
    )
    async with AsyncOracleDriver(oracle_config) as list_driver, AsyncOracleDriver(view_cfg) as driver:
        await driver.execute("DROP TABLE IF EXISTS vtb_ora_native_vec PURGE")
        await driver.execute(
            "CREATE TABLE vtb_ora_native_vec ("
            "id NUMBER(19) PRIMARY KEY, "
            "doc_json JSON, "
            "embedding VECTOR(3, FLOAT32), "
            "vec_i8 VECTOR(3, INT8)"
            ")"
        )
        try:
            await driver.execute(
                "INSERT INTO vtb_ora_native_vec (id, doc_json, embedding, vec_i8) VALUES (:1, :2, :3, :4)",
                (
                    1,
                    {"title": "espresso", "tags": ["arabica", "dark"], "score": 98},
                    VectorView.from_f32([1.0, 0.0, 0.0]),
                    encode_oracle_vector_i8([10, -5, 3]),
                ),
            )
            await driver.execute(
                "INSERT INTO vtb_ora_native_vec (id, doc_json, embedding, vec_i8) VALUES (:1, :2, :3, :4)",
                (
                    2,
                    ["decaf", {"origin": "colombia"}],
                    [0.0, 1.0, 0.0],
                    encode_oracle_vector_i8([0, 12, -4]),
                ),
            )

            rows = await driver.select("SELECT id, doc_json, embedding, vec_i8 FROM vtb_ora_native_vec ORDER BY id ASC")
            assert len(rows) == 2
            assert rows[0]["id"] == 1
            assert rows[0]["doc_json"] == {"title": "espresso", "tags": ["arabica", "dark"], "score": 98}
            emb0 = rows[0]["embedding"]
            assert isinstance(emb0, VectorView)
            assert emb0.dtype == "float32"
            assert emb0.dimensions == 3
            assert emb0.to_list() == pytest.approx([1.0, 0.0, 0.0])
            vec_i8_0 = rows[0]["vec_i8"]
            assert isinstance(vec_i8_0, VectorView)
            assert vec_i8_0.dtype in {"int8", "float32"}
            assert vec_i8_0.to_list() == pytest.approx([10.0, -5.0, 3.0])

            assert rows[1]["id"] == 2
            assert rows[1]["doc_json"] == ["decaf", {"origin": "colombia"}]

            list_rows = await list_driver.select("SELECT id, embedding, vec_i8 FROM vtb_ora_native_vec ORDER BY id ASC")
            assert list_rows[0]["embedding"] == pytest.approx([1.0, 0.0, 0.0])
            assert list_rows[0]["vec_i8"] == [10, -5, 3]

            pushdown = OracleEmbeddingPushdown(
                driver,
                dimensions=3,
                provider="custom",
                endpoint_url="http://127.0.0.1:8080/embed",
            )
            nearest = await pushdown.similarity_search(
                VectorView.from_f32([1.0, 0.0, 0.0]),
                table="vtb_ora_native_vec",
                embedding_column="embedding",
                top_k=1,
                metric="cosine",
            )
            assert len(nearest) == 1
            assert nearest[0]["id"] == 1
            assert float(nearest[0]["distance"]) == pytest.approx(0.0, abs=1e-6)
        finally:
            await driver.execute("DROP TABLE IF EXISTS vtb_ora_native_vec PURGE")


@pytest.mark.anyio
async def test_oracle_26ai_arrow_shm_cursor_above_threshold(
    oracle_config: OracleConfig,
) -> None:
    """Verify `select_to_arrow` materializes a zero-copy `VTBARROW` `ShmCapsule` when `row_count >= 512`."""
    target_rows = max(ARROW_SHM_THRESHOLD_ROWS, 512) + 16
    sql = "SELECT LEVEL AS id, 'item-' || TO_CHAR(LEVEL) AS label FROM DUAL CONNECT BY LEVEL <= :1"

    with SyncOracleDriver(oracle_config) as sync_driver:
        sync_cursor = sync_driver.select_to_arrow(sql, (target_rows,))
        assert sync_cursor.row_count == target_rows
        assert sync_cursor.used_shm is True
        sync_table = sync_cursor.to_pyarrow()
        assert sync_table.num_rows == target_rows
        assert tuple(name.lower() for name in sync_table.column_names) == ("id", "label")

    async with AsyncOracleDriver(oracle_config) as async_driver:
        async_cursor = await async_driver.select_to_arrow(sql, (target_rows,))
        assert async_cursor.row_count == target_rows
        assert async_cursor.used_shm is True
        async_table = async_cursor.to_pyarrow()
        assert async_table.num_rows == target_rows
        assert tuple(name.lower() for name in async_table.column_names) == ("id", "label")


@pytest.mark.anyio
async def test_oracle_26ai_dml_returning_into_single_roundtrip(
    oracle_config: OracleConfig,
) -> None:
    """Verify `INSERT ... RETURNING ... INTO` binds out-variables and returns row values in one round trip."""
    async with AsyncOracleDriver(oracle_config) as driver:
        await driver.execute("DROP TABLE IF EXISTS vtb_ora_native_ret PURGE")
        await driver.execute(
            "CREATE TABLE vtb_ora_native_ret (id NUMBER(19) PRIMARY KEY, label VARCHAR2(255) NOT NULL)"
        )
        try:
            row = await driver.select_one(
                "INSERT INTO vtb_ora_native_ret (id, label) VALUES (:id, :label) "
                "RETURNING id, label INTO :out_id, :out_label",
                {"id": 42, "label": "returned-item"},
            )
            assert int(row["id"]) == 42
            assert row["label"] == "returned-item"

            raw_res = await driver._client.execute(
                "INSERT INTO vtb_ora_native_ret (id, label) VALUES (:id, :label) "
                "RETURNING id, label INTO :out_id, :out_label",
                {"id": 43, "label": "second-item"},
            )
            assert raw_res.out_binds is not None
            assert int(raw_res.out_binds["out_id"]) == 43
            assert raw_res.out_binds["out_label"] == "second-item"
        finally:
            await driver.execute("DROP TABLE IF EXISTS vtb_ora_native_ret PURGE")


@pytest.mark.anyio
async def test_oracle_26ai_pipeline_delayed_commit_rolls_back_on_caller_error(
    oracle_config: OracleConfig,
) -> None:
    """Verify delayed-commit `pipeline()` rolls back earlier DML when post-`results()` validation raises."""
    with SyncOracleDriver(oracle_config) as sync_driver:
        sync_driver.execute("DROP TABLE IF EXISTS vtb_ora_native_pipe PURGE")
        sync_driver.execute(
            "CREATE TABLE vtb_ora_native_pipe (id NUMBER(19) PRIMARY KEY, label VARCHAR2(255) NOT NULL)"
        )
        try:
            sync_driver.execute(
                "INSERT INTO vtb_ora_native_pipe (id, label) VALUES (:1, :2)",
                (1, "baseline"),
            )
            with pytest.raises(NotFoundError), sync_driver.pipeline() as pipe:
                pipe.execute(
                    "INSERT INTO vtb_ora_native_pipe (id, label) VALUES (:1, :2)",
                    (2, "must-rollback-sync"),
                )
                pipe.select(
                    "SELECT id, label FROM vtb_ora_native_pipe WHERE id = :1",
                    (999,),
                )
                sync_results = pipe.results()
                assert sync_results[0] == 1
                if not sync_results[1]:
                    raise NotFoundError("Missing required row after pipeline flush")

            count_sync = sync_driver.select_value("SELECT COUNT(*) FROM vtb_ora_native_pipe")
            assert int(count_sync) == 1
        finally:
            sync_driver.execute("DROP TABLE IF EXISTS vtb_ora_native_pipe PURGE")

    async with AsyncOracleDriver(oracle_config) as async_driver:
        await async_driver.execute("DROP TABLE IF EXISTS vtb_ora_native_pipe_async PURGE")
        await async_driver.execute(
            "CREATE TABLE vtb_ora_native_pipe_async (id NUMBER(19) PRIMARY KEY, label VARCHAR2(255) NOT NULL)"
        )
        try:
            await async_driver.execute(
                "INSERT INTO vtb_ora_native_pipe_async (id, label) VALUES (:1, :2)",
                (1, "baseline"),
            )
            with pytest.raises(NotFoundError):
                async with async_driver.pipeline() as pipe:
                    pipe.execute(
                        "INSERT INTO vtb_ora_native_pipe_async (id, label) VALUES (:1, :2)",
                        (2, "must-rollback-async"),
                    )
                    pipe.select(
                        "SELECT id, label FROM vtb_ora_native_pipe_async WHERE id = :1",
                        (999,),
                    )
                    async_results = await pipe.results()
                    assert async_results[0] == 1
                    if not async_results[1]:
                        raise NotFoundError("Missing required row after async pipeline flush")

            count_async = await async_driver.select_value("SELECT COUNT(*) FROM vtb_ora_native_pipe_async")
            assert int(count_async) == 1
        finally:
            await async_driver.execute("DROP TABLE IF EXISTS vtb_ora_native_pipe_async PURGE")


@pytest.mark.anyio
async def test_oracle_26ai_vpd_client_identifier_reset_across_rollback(
    oracle_config: OracleConfig,
) -> None:
    """Verify `CLIENT_IDENTIFIER` is cleared on pool return even after an explicit transaction `ROLLBACK`."""
    pooled_cfg = OracleConfig(
        dsn=oracle_config.dsn,
        user=oracle_config.user,
        password=oracle_config.password,
        host=oracle_config.host,
        port=oracle_config.port,
        service_name=oracle_config.service_name,
        pool_min_size=1,
        pool_max_size=1,
        context_namespace="USERENV",
    )
    with SyncOraclePool(pooled_cfg) as sync_pool:
        with sync_pool.lease() as leased_drv, TenantConnectionScope(leased_drv, "tenant-alpha"):
            leased_drv._client.set_session_attrs(module="vtb-order-svc", action="sync-checkout")
            ctx_row = leased_drv.select_one(
                "SELECT SYS_CONTEXT('USERENV', 'CLIENT_IDENTIFIER') AS cid, "
                "SYS_CONTEXT('USERENV', 'MODULE') AS mod_name, "
                "SYS_CONTEXT('USERENV', 'ACTION') AS act_name FROM DUAL"
            )
            assert ctx_row["cid"] == "tenant-alpha"
            assert ctx_row["mod_name"] == "vtb-order-svc"
            assert ctx_row["act_name"] == "sync-checkout"
            leased_drv.begin()
            leased_drv.rollback()

        with sync_pool.lease() as next_drv:
            cleared_row = next_drv.select_one(
                "SELECT SYS_CONTEXT('USERENV', 'CLIENT_IDENTIFIER') AS cid, "
                "SYS_CONTEXT('USERENV', 'MODULE') AS mod_name, "
                "SYS_CONTEXT('USERENV', 'ACTION') AS act_name FROM DUAL"
            )
            assert cleared_row["cid"] is None
            assert cleared_row["mod_name"] is None
            assert cleared_row["act_name"] is None

    async with AsyncOraclePool(pooled_cfg) as async_pool:
        async with async_pool.lease() as async_drv, AsyncTenantConnectionScope(async_drv, "tenant-beta"):
            async_drv._client.set_session_attrs(module="vtb-async-svc", action="async-checkout")
            active_async_row = await async_drv.select_one(
                "SELECT SYS_CONTEXT('USERENV', 'CLIENT_IDENTIFIER') AS cid, "
                "SYS_CONTEXT('USERENV', 'MODULE') AS mod_name, "
                "SYS_CONTEXT('USERENV', 'ACTION') AS act_name FROM DUAL"
            )
            assert active_async_row["cid"] == "tenant-beta"
            assert active_async_row["mod_name"] == "vtb-async-svc"
            assert active_async_row["act_name"] == "async-checkout"
            await async_drv.begin()
            await async_drv.rollback()

        async with async_pool.lease() as next_async_drv:
            cleared_async_row = await next_async_drv.select_one(
                "SELECT SYS_CONTEXT('USERENV', 'CLIENT_IDENTIFIER') AS cid, "
                "SYS_CONTEXT('USERENV', 'MODULE') AS mod_name, "
                "SYS_CONTEXT('USERENV', 'ACTION') AS act_name FROM DUAL"
            )
            assert cleared_async_row["cid"] is None
            assert cleared_async_row["mod_name"] is None
            assert cleared_async_row["act_name"] is None


@pytest.mark.anyio
async def test_oracle_26ai_out_of_band_cancel_aborts_sleep(
    oracle_config: OracleConfig,
) -> None:
    """Verify out-of-band `cancel()` aborts an in-flight `DBMS_SESSION.SLEEP(10)` with `ORA-01013` under 4s."""
    with SyncOracleDriver(oracle_config) as sync_driver:
        assert int(sync_driver.select_value("SELECT 1 AS ok FROM DUAL")) == 1
        start_sync = time.monotonic()
        timer = threading.Timer(0.25, sync_driver._client.cancel)
        timer.start()
        try:
            with pytest.raises(QueryCancelledError) as exc_info:
                sync_driver.execute("BEGIN DBMS_SESSION.SLEEP(10); END;")
            elapsed_sync = time.monotonic() - start_sync
            assert elapsed_sync < 4.0
            assert exc_info.value.sqlstate == "57014"
            assert exc_info.value.vendor_code == 1013
        finally:
            timer.cancel()

        probe_val = sync_driver.select_value("SELECT 1 AS ok FROM DUAL")
        assert int(probe_val) == 1

    async with AsyncOracleDriver(oracle_config) as async_driver:
        assert int(await async_driver.select_value("SELECT 1 AS ok FROM DUAL")) == 1
        start_async = time.monotonic()
        async_timer = threading.Timer(0.25, async_driver._client.cancel)
        async_timer.start()
        try:
            with pytest.raises(QueryCancelledError) as async_exc_info:
                await async_driver.execute("BEGIN DBMS_SESSION.SLEEP(10); END;")
            elapsed_async = time.monotonic() - start_async
            assert elapsed_async < 4.0
            assert async_exc_info.value.sqlstate == "57014"
            assert async_exc_info.value.vendor_code == 1013
        finally:
            async_timer.cancel()

        async_probe_val: Any = await async_driver.select_value("SELECT 1 AS ok FROM DUAL")
        assert int(async_probe_val) == 1


@pytest.mark.anyio
async def test_oracle_26ai_plsql_out_binds_and_subhour_tz_roundtrip(
    oracle_config: OracleConfig,
) -> None:
    """Verify PL/SQL `OUT` binds and negative sub-hour `TIMESTAMP WITH TIME ZONE` roundtrips."""
    async with AsyncOracleDriver(oracle_config) as driver:
        plsql_res = await driver._client.execute(
            "BEGIN :out_total := :lhs + :rhs; :out_label := UPPER(:tag); END;",
            {"lhs": 19, "rhs": 23, "tag": "vertebra"},
            out_binds={"out_total": "NUMBER", "out_label": "VARCHAR2"},
        )
        assert plsql_res.out_binds == {"out_total": 42, "out_label": "VERTEBRA"}

        await driver.execute("DROP TABLE IF EXISTS vtb_ora_native_tstz PURGE")
        await driver.execute(
            "CREATE TABLE vtb_ora_native_tstz (id NUMBER(19) PRIMARY KEY, ts TIMESTAMP WITH TIME ZONE)"
        )
        try:
            neg_half_hour = timezone(timedelta(minutes=-30))
            sample = datetime(2026, 1, 1, 0, 15, 30, 125000, tzinfo=neg_half_hour)
            await driver.execute("INSERT INTO vtb_ora_native_tstz (id, ts) VALUES (:1, :2)", (1, sample))
            fetched = await driver.select_value("SELECT ts FROM vtb_ora_native_tstz WHERE id = :1", (1,))
            assert isinstance(fetched, datetime)
            assert fetched.tzinfo is not None
            assert fetched == sample
        finally:
            await driver.execute("DROP TABLE IF EXISTS vtb_ora_native_tstz PURGE")


@pytest.mark.anyio
async def test_oracle_26ai_aq_raw_binary_notification_and_clean_close(
    oracle_config: OracleConfig,
) -> None:
    """Verify live `DBMS_AQ` raw binary notification enqueue/dequeue and prompt listener cancellation on `close()`."""
    qtable = "VTB_AQ_NATIVE_QT"
    qname = "VTB_AQ_NATIVE_Q"
    cleanup_plsql = (
        "BEGIN "
        f"  BEGIN DBMS_AQADM.STOP_QUEUE(queue_name => '{qname}'); EXCEPTION WHEN OTHERS THEN NULL; END; "
        f"  BEGIN DBMS_AQADM.DROP_QUEUE(queue_name => '{qname}'); EXCEPTION WHEN OTHERS THEN NULL; END; "
        f"  BEGIN DBMS_AQADM.DROP_QUEUE_TABLE(queue_table => '{qtable}', force => TRUE); EXCEPTION WHEN OTHERS THEN NULL; END; "
        "END;"
    )
    setup_plsql = (
        "BEGIN "
        f"  DBMS_AQADM.CREATE_QUEUE_TABLE(queue_table => '{qtable}', queue_payload_type => 'RAW'); "
        f"  DBMS_AQADM.CREATE_QUEUE(queue_name => '{qname}', queue_table => '{qtable}'); "
        f"  DBMS_AQADM.START_QUEUE(queue_name => '{qname}'); "
        "END;"
    )
    binary_payload = b"\x00\xff\xfevtb-aq-binary\x00\x80"

    async with AsyncOracleDriver(oracle_config) as driver:
        await driver.execute(cleanup_plsql)
        await driver.execute(setup_plsql)
        notify_cfg = OracleNotifyConfig(wait_timeout_seconds=30, poll_timeout=0.5)
        source = AsyncOracleNotificationSource(driver, notify_cfg)
        try:
            msg_id = await source.publish_aq(qname, binary_payload)
            assert len(msg_id) == 32

            stream = driver.listen_notifications([qname], config=notify_cfg)
            try:
                event = await anext(stream)
                assert event.channel == qname
                assert event.payload == binary_payload
                assert event.sender_id == msg_id
            finally:
                close_start = time.monotonic()
                aclose = getattr(stream, "aclose", None)
                if aclose is not None:
                    await aclose()
                assert (time.monotonic() - close_start) < 4.0

            second_payload = b"\xff\x00vtb-aq-second\x7f"
            msg_id_2 = await source.publish_aq(qname, second_payload)
            aq_stream = source.listen_aq(qname)
            try:
                event_2 = await anext(aq_stream)
                assert event_2.channel == qname
                assert event_2.payload == second_payload
                assert event_2.sender_id == msg_id_2
            finally:
                source_close_start = time.monotonic()
                await source.close()
                aq_aclose = getattr(aq_stream, "aclose", None)
                if aq_aclose is not None:
                    await aq_aclose()
                assert (time.monotonic() - source_close_start) < 4.0
        finally:
            await source.close()
            await driver.execute(cleanup_plsql)


@pytest.mark.anyio
async def test_oracle_26ai_aq_wait_timeout_seconds_idle_liveness_and_delivery(
    oracle_config: OracleConfig,
) -> None:
    """Verify ``OracleNotifyConfig(wait_timeout_seconds=2)`` probes liveness across an idle window and delivers messages."""
    qtable = "VTB_AQ_LIVENESS_QT"
    qname = "VTB_AQ_LIVENESS_Q"
    cleanup_plsql = (
        "BEGIN "
        f"  BEGIN DBMS_AQADM.STOP_QUEUE(queue_name => '{qname}'); EXCEPTION WHEN OTHERS THEN NULL; END; "
        f"  BEGIN DBMS_AQADM.DROP_QUEUE(queue_name => '{qname}'); EXCEPTION WHEN OTHERS THEN NULL; END; "
        f"  BEGIN DBMS_AQADM.DROP_QUEUE_TABLE(queue_table => '{qtable}', force => TRUE); EXCEPTION WHEN OTHERS THEN NULL; END; "
        "END;"
    )
    setup_plsql = (
        "BEGIN "
        f"  DBMS_AQADM.CREATE_QUEUE_TABLE(queue_table => '{qtable}', queue_payload_type => 'RAW'); "
        f"  DBMS_AQADM.CREATE_QUEUE(queue_name => '{qname}', queue_table => '{qtable}'); "
        f"  DBMS_AQADM.START_QUEUE(queue_name => '{qname}'); "
        "END;"
    )
    payload = b"vtb-aq-after-idle-liveness-probe"

    async with AsyncOracleDriver(oracle_config) as driver:
        await driver.execute(cleanup_plsql)
        await driver.execute(setup_plsql)
        notify_cfg = OracleNotifyConfig(wait_timeout_seconds=2, poll_timeout=0.5)
        source = AsyncOracleNotificationSource(driver, notify_cfg)
        try:
            stream = source.listen_aq(qname)
            try:
                await anyio.sleep(2.4)
                msg_id = await source.publish_aq(qname, payload)
                event = await anext(stream)
                assert event.channel == qname
                assert event.payload == payload
                assert event.sender_id == msg_id
            finally:
                close_start = time.monotonic()
                aclose = getattr(stream, "aclose", None)
                if aclose is not None:
                    await aclose()
                assert (time.monotonic() - close_start) < 2.0
        finally:
            await source.close()
            await driver.execute(cleanup_plsql)


def test_sync_single_call_client_matches_cursor(oracle_config: OracleConfig) -> None:
    """Verify `SyncOracleClient.fetch_rows`, `fetch_dicts`, and `fetch_value` match `SyncOracleCursor`."""
    mod = ensure_module(
        "vertebra._oracle",
        package="vertebra-core[oracle]",
        install_package="vertebra-core[oracle]",
        extra="oracle",
    )
    client = mod.SyncOracleClient(**oracle_config.to_client_kwargs())
    try:
        client.connect()
        sql = "SELECT 1 AS a, 'hello' AS b FROM dual"
        rows, names = client.fetch_rows(sql)
        cur = client.cursor()
        try:
            cur.execute(sql)
            cur_rows = cur.fetchall()
            assert cur.description is not None
            cur_names = tuple(col[0] for col in cur.description)
        finally:
            cur.close()
        assert rows == cur_rows
        assert names == cur_names
        assert client.fetch_dicts(sql) == [{"a": 1, "b": "hello"}]
        assert client.fetch_value(sql) == 1
    finally:
        client.close()


def test_sync_single_call_pool_sequential_reuse(oracle_config: OracleConfig) -> None:
    """Verify 50 sequential `SyncOraclePool.fetch_value` calls reuse a size-1 pool with zero leaked borrows."""
    mod = ensure_module(
        "vertebra._oracle",
        package="vertebra-core[oracle]",
        install_package="vertebra-core[oracle]",
        extra="oracle",
    )
    _, pool_kwargs = OracleConfig.resolve_pool_kwargs(oracle_config, {"min_size": 1, "max_size": 1})
    pool = mod.SyncOraclePool(**pool_kwargs)
    try:
        results = [pool.fetch_value("SELECT 1 FROM dual") for _ in range(50)]
        assert results == [1] * 50
        assert pool.borrowed_size == 0
    finally:
        pool.close()


def test_sync_single_call_pool_recovers_after_statement_error(oracle_config: OracleConfig) -> None:
    """Verify a non-fatal statement error (`ORA-00942`) releases the pooled connection cleanly."""
    mod = ensure_module(
        "vertebra._oracle",
        package="vertebra-core[oracle]",
        install_package="vertebra-core[oracle]",
        extra="oracle",
    )
    _, pool_kwargs = OracleConfig.resolve_pool_kwargs(oracle_config, {"min_size": 1, "max_size": 1})
    pool = mod.SyncOraclePool(**pool_kwargs)
    try:
        with pytest.raises(DatabaseError) as exc_info:
            pool.fetch_rows("SELECT 1 FROM vtb_nonexistent_sync_single_call_tbl")
        assert getattr(exc_info.value, "vendor_code", None) == 942
        assert pool.fetch_value("SELECT 1 FROM dual") == 1
        assert pool.borrowed_size == 0
    finally:
        pool.close()


def test_sync_single_call_execute_dml_unique_violation_raises_integrity_error(
    oracle_config: OracleConfig,
) -> None:
    """Verify `SyncOracleClient.execute_dml` maps a unique constraint violation to `IntegrityError`."""
    mod = ensure_module(
        "vertebra._oracle",
        package="vertebra-core[oracle]",
        install_package="vertebra-core[oracle]",
        extra="oracle",
    )
    client = mod.SyncOracleClient(**oracle_config.to_client_kwargs())
    table = "vtb_ora_sync_sc_uniq"
    try:
        client.connect()
        client.execute_dml(f"DROP TABLE IF EXISTS {table} PURGE")
        client.execute_dml(f"CREATE TABLE {table} (id NUMBER(10) PRIMARY KEY)")
        try:
            rowcount, _, out_binds, touches_ctx = client.execute_dml(
                f"INSERT INTO {table} (id) VALUES (:1)",
                (1,),
            )
            assert rowcount == 1
            assert out_binds is None
            assert touches_ctx is False
            with pytest.raises(IntegrityError):
                client.execute_dml(f"INSERT INTO {table} (id) VALUES (:1)", (1,))
        finally:
            client.execute_dml(f"DROP TABLE IF EXISTS {table} PURGE")
    finally:
        client.close()


def test_pool_release_reset_autocommit_dml_visible(oracle_config: OracleConfig) -> None:
    """Verify pooled autocommit DML commits cleanly and stays visible from a second connection."""
    mod = ensure_module(
        "vertebra._oracle",
        package="vertebra-core[oracle]",
        install_package="vertebra-core[oracle]",
        extra="oracle",
    )
    _, pool_kwargs = OracleConfig.resolve_pool_kwargs(oracle_config, {"min_size": 1, "max_size": 1})
    pool = mod.SyncOraclePool(**pool_kwargs)
    observer = mod.SyncOracleClient(**oracle_config.to_client_kwargs())
    table = "vtb_ora_pool_reset_dml"
    try:
        observer.connect()
        observer.execute_dml(f"DROP TABLE IF EXISTS {table} PURGE")
        observer.execute_dml(f"CREATE TABLE {table} (id NUMBER(10) PRIMARY KEY, label VARCHAR2(64))")
        try:
            rowcount, _, _, _ = pool.execute_dml(
                f"INSERT INTO {table} (id, label) VALUES (:1, :2)",
                (1, "committed"),
            )
            assert rowcount == 1
            assert pool.borrowed_size == 0
            assert observer.fetch_value(f"SELECT COUNT(*) FROM {table} WHERE id = :1", (1,)) == 1
        finally:
            observer.execute_dml(f"DROP TABLE IF EXISTS {table} PURGE")
    finally:
        observer.close()
        pool.close()


def test_pool_release_reset_abandoned_transaction_rolled_back(oracle_config: OracleConfig) -> None:
    """Verify releasing a pooled connection with an uncommitted transaction rolls back the insert."""
    mod = ensure_module(
        "vertebra._oracle",
        package="vertebra-core[oracle]",
        install_package="vertebra-core[oracle]",
        extra="oracle",
    )
    _, pool_kwargs = OracleConfig.resolve_pool_kwargs(oracle_config, {"min_size": 1, "max_size": 1})
    pool = mod.SyncOraclePool(**pool_kwargs)
    observer = mod.SyncOracleClient(**oracle_config.to_client_kwargs())
    table = "vtb_ora_pool_reset_abandon"
    try:
        observer.connect()
        observer.execute_dml(f"DROP TABLE IF EXISTS {table} PURGE")
        observer.execute_dml(f"CREATE TABLE {table} (id NUMBER(10) PRIMARY KEY, label VARCHAR2(64))")
        try:
            client = pool.acquire()
            try:
                client.begin()
                client.execute_dml(
                    f"INSERT INTO {table} (id, label) VALUES (:1, :2)",
                    (1, "uncommitted"),
                )
            finally:
                pool.release(client)
            assert pool.borrowed_size == 0
            assert observer.fetch_value(f"SELECT COUNT(*) FROM {table} WHERE id = :1", (1,)) == 0
        finally:
            observer.execute_dml(f"DROP TABLE IF EXISTS {table} PURGE")
    finally:
        observer.close()
        pool.close()


def test_pool_release_reset_select_for_update_releases_row_lock(oracle_config: OracleConfig) -> None:
    """Verify pooled `SELECT ... FOR UPDATE` rolls back on release so another session updates without blocking."""
    mod = ensure_module(
        "vertebra._oracle",
        package="vertebra-core[oracle]",
        install_package="vertebra-core[oracle]",
        extra="oracle",
    )
    _, pool_kwargs = OracleConfig.resolve_pool_kwargs(oracle_config, {"min_size": 1, "max_size": 1})
    pool = mod.SyncOraclePool(**pool_kwargs)
    observer_kwargs = {**oracle_config.to_client_kwargs(), "call_timeout_ms": 5000}
    observer = mod.SyncOracleClient(**observer_kwargs)
    table = "vtb_ora_pool_reset_lock"
    try:
        observer.connect()
        observer.execute_dml(f"DROP TABLE IF EXISTS {table} PURGE")
        observer.execute_dml(f"CREATE TABLE {table} (id NUMBER(10) PRIMARY KEY, label VARCHAR2(64))")
        observer.execute_dml(f"INSERT INTO {table} (id, label) VALUES (:1, :2)", (1, "initial"))
        try:
            rows, _ = pool.fetch_rows(f"SELECT id, label FROM {table} WHERE id = :1 FOR UPDATE", (1,))
            assert rows == [(1, "initial")]
            assert pool.borrowed_size == 0

            rowcount, _, _, _ = observer.execute_dml(
                f"UPDATE {table} SET label = :1 WHERE id = :2",
                ("updated", 1),
            )
            assert rowcount == 1
            assert observer.fetch_value(f"SELECT label FROM {table} WHERE id = :1", (1,)) == "updated"
        finally:
            observer.execute_dml(f"DROP TABLE IF EXISTS {table} PURGE")
    finally:
        observer.close()
        pool.close()


@pytest.mark.anyio
async def test_async_cursor_fetch_ready_does_not_iterate_event_loop(
    oracle_config: OracleConfig,
) -> None:
    """Verify `AsyncOracleCursor` fetch and close awaitables resolve synchronously without turning the event loop."""
    mod = ensure_module(
        "vertebra._oracle",
        package="vertebra-core[oracle]",
        install_package="vertebra-core[oracle]",
        extra="oracle",
    )
    client = mod.AsyncOracleClient(**oracle_config.to_client_kwargs())
    try:
        await client.connect()
        cur = client.cursor()
        try:
            await cur.execute("SELECT 1 AS a FROM dual UNION ALL SELECT 2 AS a FROM dual")
            loop = asyncio.get_running_loop()
            ran: list[int] = []
            loop.call_soon(ran.append, 1)
            rows = await cur.fetchall()
            assert rows == [(1,), (2,)]
            assert ran == []
        finally:
            loop = asyncio.get_running_loop()
            close_ran: list[int] = []
            loop.call_soon(close_ran.append, 1)
            await cur.close()
            assert close_ran == []
    finally:
        await client.close()


@pytest.mark.anyio
async def test_async_single_call_client_matches_cursor_and_shapes(
    oracle_config: OracleConfig,
) -> None:
    """Verify `AsyncOracleClient` single-call methods match cursor output, handle out-binds, and validate at call time."""
    mod = ensure_module(
        "vertebra._oracle",
        package="vertebra-core[oracle]",
        install_package="vertebra-core[oracle]",
        extra="oracle",
    )
    client = mod.AsyncOracleClient(**oracle_config.to_client_kwargs())
    try:
        await client.connect()
        sql = "SELECT 1 AS a, 'hello' AS b FROM dual"
        rows, names = await client.fetch_rows(sql)
        cur = client.cursor()
        try:
            await cur.execute(sql)
            cur_rows = await cur.fetchall()
            assert cur.description is not None
            cur_names = tuple(col[0] for col in cur.description)
        finally:
            await cur.close()
        assert rows == cur_rows
        assert names == cur_names
        assert await client.fetch_dicts(sql) == [{"a": 1, "b": "hello"}]
        assert await client.fetch_value(sql) == 1

        with pytest.raises(NotFoundError):
            await client.fetch_value("SELECT 1 FROM dual WHERE 1 = 0")

        with pytest.raises(InterfaceError):
            client.fetch_rows("SELECT 1 FROM dual", "abc")

        var = mod.OracleOutBindVar("NUMBER")
        rowcount, _, out_binds, touches_ctx = await client.execute_dml(
            "BEGIN :out := 99; END;",
            {"out": var},
        )
        assert rowcount == 0
        assert out_binds == {"out": 99}
        assert var.getvalue() == 99
        assert touches_ctx is False
    finally:
        await client.close()


@pytest.mark.anyio
async def test_async_single_call_pool_one_awaitable_shapes_and_error_recovery(
    oracle_config: OracleConfig,
) -> None:
    """Verify `AsyncOraclePool` single-call methods return one non-coroutine awaitable and recover after errors."""
    mod = ensure_module(
        "vertebra._oracle",
        package="vertebra-core[oracle]",
        install_package="vertebra-core[oracle]",
        extra="oracle",
    )
    _, pool_kwargs = OracleConfig.resolve_pool_kwargs(oracle_config, {"min_size": 1, "max_size": 1})
    pool = mod.AsyncOraclePool(**pool_kwargs)
    try:
        aw = pool.fetch_value("SELECT 1 FROM dual")
        assert not inspect.iscoroutine(aw)
        assert await aw == 1
        assert pool.borrowed_size == 0

        sql = "SELECT 1 AS a, 'hello' AS b FROM dual"
        rows, names = await pool.fetch_rows(sql)
        assert rows == [(1, "hello")]
        assert names == ("a", "b")
        assert await pool.fetch_dicts(sql) == [{"a": 1, "b": "hello"}]
        assert pool.borrowed_size == 0

        with pytest.raises(DatabaseError) as exc_info:
            await pool.fetch_rows("SELECT 1 FROM vtb_nonexistent_async_single_call_tbl")
        assert getattr(exc_info.value, "vendor_code", None) == 942
        assert await pool.fetch_value("SELECT 1 FROM dual") == 1
        assert pool.borrowed_size == 0
    finally:
        await pool.close()


@pytest.mark.anyio
async def test_async_single_call_pool_cancel_returns_connection(
    oracle_config: OracleConfig,
) -> None:
    """Verify cancelling a slow `AsyncOraclePool.fetch_value` releases the borrowed slot so a later call succeeds."""
    mod = ensure_module(
        "vertebra._oracle",
        package="vertebra-core[oracle]",
        install_package="vertebra-core[oracle]",
        extra="oracle",
    )
    _, pool_kwargs = OracleConfig.resolve_pool_kwargs(oracle_config, {"min_size": 1, "max_size": 1})
    pool = mod.AsyncOraclePool(**pool_kwargs)
    try:
        assert await pool.fetch_value("SELECT 1 FROM dual") == 1

        async def _slow_call() -> Any:
            return await pool.fetch_value("BEGIN DBMS_SESSION.SLEEP(10); END;")

        task = asyncio.create_task(_slow_call())
        await asyncio.sleep(0.2)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        deadline = time.monotonic() + 4.0
        while pool.borrowed_size != 0 and time.monotonic() < deadline:
            await asyncio.sleep(0.02)
        assert pool.borrowed_size == 0
        assert await pool.fetch_value("SELECT 1 FROM dual") == 1
        assert pool.borrowed_size == 0
    finally:
        await pool.close()


@pytest.mark.anyio
async def test_async_single_call_pool_dml_and_abandoned_transaction(
    oracle_config: OracleConfig,
) -> None:
    """Verify async pooled autocommit DML commits while an abandoned async transaction rolls back on release."""
    mod = ensure_module(
        "vertebra._oracle",
        package="vertebra-core[oracle]",
        install_package="vertebra-core[oracle]",
        extra="oracle",
    )
    _, pool_kwargs = OracleConfig.resolve_pool_kwargs(oracle_config, {"min_size": 1, "max_size": 1})
    pool = mod.AsyncOraclePool(**pool_kwargs)
    observer = mod.AsyncOracleClient(**oracle_config.to_client_kwargs())
    table = "vtb_ora_async_pool_sc_dml"
    try:
        await observer.connect()
        await observer.execute_dml(f"DROP TABLE IF EXISTS {table} PURGE")
        await observer.execute_dml(f"CREATE TABLE {table} (id NUMBER(10) PRIMARY KEY, label VARCHAR2(64))")
        try:
            rowcount, _, _, _ = await pool.execute_dml(
                f"INSERT INTO {table} (id, label) VALUES (:1, :2)",
                (1, "committed"),
            )
            assert rowcount == 1
            assert pool.borrowed_size == 0
            assert await observer.fetch_value(f"SELECT COUNT(*) FROM {table} WHERE id = :1", (1,)) == 1

            client = await pool.acquire()
            try:
                await client.begin()
                await client.execute_dml(
                    f"INSERT INTO {table} (id, label) VALUES (:1, :2)",
                    (2, "uncommitted"),
                )
            finally:
                await pool.release(client)
            assert pool.borrowed_size == 0
            assert await observer.fetch_value(f"SELECT COUNT(*) FROM {table} WHERE id = :1", (2,)) == 0
        finally:
            await observer.execute_dml(f"DROP TABLE IF EXISTS {table} PURGE")
    finally:
        await observer.close()
        await pool.close()


@pytest.mark.anyio
async def test_async_single_call_pool_concurrent_gather(
    oracle_config: OracleConfig,
) -> None:
    """Verify `asyncio.gather` over five concurrent `AsyncOraclePool.fetch_value` calls returns all five values."""
    mod = ensure_module(
        "vertebra._oracle",
        package="vertebra-core[oracle]",
        install_package="vertebra-core[oracle]",
        extra="oracle",
    )
    _, pool_kwargs = OracleConfig.resolve_pool_kwargs(oracle_config, {"min_size": 1, "max_size": 5})
    pool = mod.AsyncOraclePool(**pool_kwargs)
    try:
        values = await asyncio.gather(*(pool.fetch_value("SELECT :1 FROM dual", (i,)) for i in range(1, 6)))
        assert list(values) == [1, 2, 3, 4, 5]
        assert pool.borrowed_size == 0
    finally:
        await pool.close()
