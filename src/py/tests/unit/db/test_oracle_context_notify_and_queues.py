"""Offline unit tests for Oracle context, tenancy, notifications, embeddings, and queues."""

from __future__ import annotations

from collections import deque
from typing import Any, cast

import msgspec
import pytest
from sqlspec import StatementStack
from sqlspec.core.parameters import ParameterStyle

import vertebra._oracle as _oracle
from tests.unit.db.oracle_test_doubles import (
    _AsyncDriverDouble,
    _FakeAsyncOracleDriver,
    _FakeAsyncOracleNotifySource,
    _FakeSyncOracleDriver,
    _FakeSyncOracleNotifySource,
    _reserve_closed_local_port,
    _StubAsyncOracleClient,
    _StubSyncOracleClient,
    _SyncDriverDouble,
)
from vertebra._vertebra import VectorBatchView
from vertebra.db.adapters.oracle import (
    AsyncOracleDriver,
    AsyncOracleQueueStore,
    OracleEmbeddingPushdown,
    SyncOracleDriver,
    SyncOracleQueueStore,
)
from vertebra.db.adapters.oracle.config import (
    OracleConfig,
)
from vertebra.db.adapters.oracle.core import OracleCore
from vertebra.db.adapters.oracle.notify import (
    AsyncOracleNotificationSource,
    OracleNotifyConfig,
    validate_oracle_channel_name,
)
from vertebra.db.cdc import ChangeFeed
from vertebra.db.driver import (
    ConnectionFailure,
    DataError,
    InterfaceError,
    OperationalError,
    ProgrammingError,
)
from vertebra.db.driver.capabilities import (
    AsyncNotificationSource,
    AsyncQueueStore,
    QueueEntry,
    QueueReservation,
    SyncQueueStore,
)
from vertebra.db.rls import AsyncTenantConnectionScope, SyncTenantConnectionScope, TenantPredicate, TenantScopeConfig
from vertebra.extensions.sqlspec.adapters.oracle import OracleAsyncDriver, OracleSyncDriver
from vertebra.framework.queues.oracle import OracleQueueBackend


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
    """Verify SyncTenantConnectionScope and AsyncTenantConnectionScope reset on rollback and evict on reset failure."""
    sync_ok = _SyncDriverDouble(fail_reset=False)
    with pytest.raises(RuntimeError, match="rolled back body"), SyncTenantConnectionScope(sync_ok, "tenant-a"):
        raise RuntimeError("rolled back body")
    assert sync_ok.evicted is False
    assert [e[0] for e in sync_ok.events] == ["enter", "exit"]
    assert sync_ok.events[1][1] == ("BEGIN DBMS_SESSION.CLEAR_IDENTIFIER; END;", {})

    sync_bad = _SyncDriverDouble(fail_reset=True)
    with pytest.raises(OperationalError, match="ORA-03113"), SyncTenantConnectionScope(sync_bad, "tenant-a"):
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

    cols = [_oracle.OracleColumn("ID", "NUMBER"), _oracle.OracleColumn("LABEL", "VARCHAR2")]
    sync_stub = _StubSyncOracleClient()
    sync_stub.queued_pipeline.append(
        [
            _oracle.OraclePipelineOutcome("completed", _oracle.OracleQueryResult([], [], rowcount=1)),
            _oracle.OraclePipelineOutcome("completed", _oracle.OracleQueryResult(cols, [(7, "seven")], rowcount=1)),
        ]
    )
    with SyncOracleDriver(client=sync_stub) as native_sync:
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

    async_stub = _StubAsyncOracleClient()
    async_stub.queued_pipeline.append(
        [
            _oracle.OraclePipelineOutcome("completed", _oracle.OracleQueryResult([], [], rowcount=1)),
            _oracle.OraclePipelineOutcome("completed", _oracle.OracleQueryResult(cols, [(9, "nine")], rowcount=1)),
        ]
    )
    async with AsyncOracleDriver(client=async_stub) as native_async:
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
async def test_sqlspec_oracle_apply_tenant_predicate_and_driver_scope_dispatches() -> None:
    """Verify apply_tenant_predicate and driver tenant scope hooks issue SET_IDENTIFIER or trusted PL/SQL."""
    sync_stub = _StubSyncOracleClient()
    with SyncOracleDriver(client=sync_stub) as sync_default:
        sync_adapter_default = OracleSyncDriver(sync_default)
        pred_sql = sync_adapter_default.apply_tenant_predicate(TenantPredicate(tenant_value="t-99"))
        assert pred_sql == "tenant_id = :tenant_id"
        default_execs = [payload for kind, payload in sync_stub.calls if kind == "execute"]
        assert default_execs[0]["sql"] == "BEGIN DBMS_SESSION.SET_IDENTIFIER(:value); END;"
        assert default_execs[0]["params"] == {"value": "t-99"}

        with SyncTenantConnectionScope(sync_default, "scoped-sync"):
            assert sync_default._session_context[("USERENV", "tenant_id")] == "scoped-sync"
        assert ("USERENV", "tenant_id") not in sync_default._session_context
        default_execs = [payload for kind, payload in sync_stub.calls if kind == "execute"]
        assert default_execs[2]["sql"] == "BEGIN DBMS_SESSION.CLEAR_IDENTIFIER; END;"

    trusted_cfg = OracleConfig(
        context_namespace="APP_CTX",
        context_package_proc="SEC_PKG.SET_CTX",
        context_clear_proc="SEC_PKG.CLEAR_CTX",
    )
    trusted_sync_stub = _StubSyncOracleClient()
    with SyncOracleDriver(trusted_cfg, client=trusted_sync_stub) as sync_trusted:
        sync_adapter_trusted = OracleSyncDriver(sync_trusted)
        assert (
            sync_adapter_trusted.apply_tenant_predicate(TenantPredicate(tenant_value="t-tr"))
            == "tenant_id = :tenant_id"
        )
        trusted_execs = [payload for kind, payload in trusted_sync_stub.calls if kind == "execute"]
        assert trusted_execs[0]["sql"] == "BEGIN SEC_PKG.SET_CTX(:namespace, :attribute, :value); END;"
        with SyncTenantConnectionScope(sync_trusted, "scoped-trusted"):
            assert sync_trusted._session_context[("APP_CTX", "tenant_id")] == "scoped-trusted"
        trusted_execs = [payload for kind, payload in trusted_sync_stub.calls if kind == "execute"]
        assert trusted_execs[2]["sql"] == "BEGIN SEC_PKG.CLEAR_CTX(:namespace, :attribute); END;"

    trusted_async_stub = _StubAsyncOracleClient()
    async with AsyncOracleDriver(trusted_cfg, client=trusted_async_stub) as async_trusted:
        async_adapter_trusted = OracleAsyncDriver(async_trusted)
        apred = await async_adapter_trusted.apply_tenant_predicate(TenantPredicate(tenant_value="async-tr"))
        assert apred == "tenant_id = :tenant_id"
        async with AsyncTenantConnectionScope(async_trusted, "async-scoped"):
            assert async_trusted._session_context[("APP_CTX", "tenant_id")] == "async-scoped"
        assert ("APP_CTX", "tenant_id") not in async_trusted._session_context
        async_execs = [payload for kind, payload in trusted_async_stub.calls if kind == "execute"]
        assert async_execs[2]["sql"] == "BEGIN SEC_PKG.CLEAR_CTX(:namespace, :attribute); END;"


@pytest.mark.anyio
async def test_oracle_notification_and_listener_exports_validation_and_fail_loud_worker() -> None:
    """Verify OracleNotification attributes, AQ validation, and fail-loud AsyncOracleListener connect error."""
    notification = _oracle.OracleNotification(
        "VTB_AQ_ORDERS",
        b"msg-001",
        b'{"event":"created"}',
        "ORDER_WORKER",
        1700000000000,
    )
    assert notification.queue_name == "VTB_AQ_ORDERS"
    assert notification.consumer_name == "ORDER_WORKER"
    assert notification.msg_id == b"msg-001"
    assert notification.payload == b'{"event":"created"}'
    assert notification.enqueued_at_epoch_ms == 1700000000000

    closed_port = _reserve_closed_local_port()
    client = _oracle.AsyncOracleClient(
        host="127.0.0.1",
        port=closed_port,
        user="app",
        password="secret",
        connect_timeout=0.5,
    )
    try:
        with pytest.raises(ProgrammingError) as exc_info:
            await client.open_listener(queue_name="BAD;QUEUE")
        assert exc_info.value.sqlstate == "42602"

        with pytest.raises(ProgrammingError) as c_exc:
            await client.open_listener(queue_name="VTB_AQ_ORDERS", consumer_name="BAD;CONSUMER")
        assert c_exc.value.sqlstate == "42602"

        with pytest.raises(InterfaceError):
            await client.open_listener(queue_name="VTB_AQ_ORDERS", max_buffered=0)

        with pytest.raises(ProgrammingError) as enq_exc:
            await client.enqueue_notification(queue_name="BAD;QUEUE", payload=b"x")
        assert enq_exc.value.sqlstate == "42602"

        listener = await client.open_listener(
            queue_name="VTB_AQ_ORDERS",
            consumer_name="ORDER_WORKER",
            wait_timeout_seconds=1,
            max_buffered=16,
        )
        assert isinstance(listener, _oracle.AsyncOracleListener)
        assert listener.queue_name == "VTB_AQ_ORDERS"
        assert listener.consumer_name == "ORDER_WORKER"
        assert listener.dropped_count == 0

        with pytest.raises(ConnectionFailure) as listen_exc:
            await listener.next_notification(timeout=5.0)
        assert listen_exc.value.sqlstate == "08006"
        assert listen_exc.value.is_transient is True

        await listener.close()
        assert listener.is_closed is True
        await listener.close()
        assert await listener.next_notification(timeout=0.05) is None
    finally:
        await client.close()


@pytest.mark.parametrize(
    ("raw_channel", "expected_queue"),
    [
        ("orders", "VTB_AQ_ORDERS"),
        ("  VTB_AQ_BILLING  ", "VTB_AQ_BILLING"),
        ("_events$1#", "VTB_AQ__EVENTS$1#"),
    ],
)
def test_validate_oracle_channel_name_resolves_valid_identifiers(
    raw_channel: str,
    expected_queue: str,
) -> None:
    """Verify valid channel names resolve to uppercase prefixed AQ identifiers."""
    assert validate_oracle_channel_name(raw_channel) == expected_queue


@pytest.mark.parametrize(
    "invalid_channel",
    [
        "",
        "   ",
        "1leading_digit",
        "orders; DROP TABLE users",
        "orders with spaces",
        "schema.orders",
        "A" * 129,
    ],
)
def test_validate_oracle_channel_name_rejects_invalid_identifiers(
    invalid_channel: str,
) -> None:
    """Verify invalid channel names raise ProgrammingError with sqlstate 42602."""
    with pytest.raises(ProgrammingError) as exc_info:
        validate_oracle_channel_name(invalid_channel)
    assert exc_info.value.sqlstate == "42602"


@pytest.mark.anyio
async def test_oracle_notification_source_protocol_publish_subscribe_listen_and_change_feed() -> None:
    """Verify notification source protocol compliance, publish/subscribe/listen, and ChangeFeed envelopes."""
    async_stub = _StubAsyncOracleClient()
    async with AsyncOracleDriver(client=async_stub) as driver:
        source = driver.notification_source(
            OracleNotifyConfig(queue_prefix="VTB_AQ_", poll_timeout=0.05),
        )
        assert isinstance(source, AsyncNotificationSource)

        with pytest.raises(ProgrammingError) as exc_info:
            async for _ in source.listen():
                break
        assert exc_info.value.sqlstate == "42602"

        await source.subscribe(["orders"])
        await source.publish("orders", b"raw-order-payload")
        received_events = []
        async for evt in source.listen():
            received_events.append(evt)
            break

        assert len(received_events) == 1
        assert received_events[0].channel == "orders"
        assert received_events[0].payload == b"raw-order-payload"
        assert received_events[0].metadata["queue_name"] == "VTB_AQ_ORDERS"

        feed = ChangeFeed()
        cf_source = driver.notification_source(
            OracleNotifyConfig(queue_prefix="VTB_AQ_", poll_timeout=0.05),
            change_feed=feed,
        )
        assert cf_source.change_feed is feed
        await cf_source.subscribe(["invoices"])
        await cf_source.publish("invoices", "invoice-99")
        cf_events = []
        async for evt in cf_source.listen():
            cf_events.append(evt)
            break

        assert len(cf_events) == 1
        assert cf_events[0].channel == "invoices"
        assert cf_events[0].payload == b"invoice-99"
        assert cf_events[0].metadata["change_event_id"] == 1
        assert cf_events[0].metadata["spilled"] is False
        assert len(feed.replay_since(0)) == 1


@pytest.mark.anyio
async def test_oracle_notification_source_reconnect_backoff_and_cancellation_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify exponential reconnect backoff on transient OperationalError and listener cleanup on exit."""
    recorded_delays: list[float] = []

    async def fake_sleep(delay: float) -> None:
        recorded_delays.append(delay)

    monkeypatch.setattr("vertebra.db.adapters.oracle.notify.asyncio.sleep", fake_sleep)

    async_stub = _StubAsyncOracleClient(listener_fail_modes=("transient", "transient", None))
    async_stub.aq_queues["VTB_AQ_RECOVER"] = deque(
        [
            _oracle.OracleNotification(
                "VTB_AQ_RECOVER",
                b"msg-recovered",
                b"recovered-ok",
                None,
                1700000000123,
            )
        ]
    )
    async with AsyncOracleDriver(client=async_stub) as driver:
        source = AsyncOracleNotificationSource(
            driver,
            OracleNotifyConfig(
                reconnect_initial_delay=0.25,
                reconnect_max_delay=15.0,
                max_reconnect_attempts=5,
                poll_timeout=0.05,
            ),
        )
        await source.subscribe(["recover"])
        async for evt in source.listen():
            assert evt.channel == "recover"
            assert evt.payload == b"recovered-ok"
            await source.close()
            break

        assert recorded_delays == [0.25, 0.5]
        assert len(async_stub.created_listeners) == 3
        assert all(listener.is_closed and listener.close_calls >= 1 for listener in async_stub.created_listeners)


def test_oracle_embedding_pushdown_extracts_native_vector_view_bytes_and_sequence() -> None:
    """Verify _extract_vector unwraps VectorView, ORACLE_VECTOR_MAGIC bytes, and tuple rows."""
    vec_view = VectorBatchView.from_f32_rows([[0.25, -0.5, 1.0]], normalize=False)[0]
    raw_bytes = OracleCore.encode_vector_f32([0.25, -0.5, 1.0])
    expected = [0.25, -0.5, 1.0]

    assert OracleEmbeddingPushdown._extract_vector(vec_view, 3) == expected
    assert OracleEmbeddingPushdown._extract_vector({"vec": vec_view}, 3) == expected
    assert OracleEmbeddingPushdown._extract_vector({"embed_vector": vec_view}, 3) == expected
    assert OracleEmbeddingPushdown._extract_vector({"EMBED_VECTOR": raw_bytes}, 3) == expected
    assert OracleEmbeddingPushdown._extract_vector((raw_bytes,), 3) == expected
    assert OracleEmbeddingPushdown._extract_vector(([0.25, -0.5, 1.0],), 3) == expected

    with pytest.raises(DataError, match="Oracle embedding row has no valid vector"):
        OracleEmbeddingPushdown._extract_vector({"vec": [0.25, -0.5]}, 3)

    with pytest.raises(DataError, match="Oracle embedding row has no valid vector"):
        OracleEmbeddingPushdown._extract_vector({"vec": [0.25, -0.5, 1.0, 2.0]}, 3)

    with pytest.raises(DataError, match="Oracle embedding row has no valid vector"):
        OracleEmbeddingPushdown._extract_vector({"vec": [True, False, 1.0]}, 3)

    with pytest.raises(DataError, match="Oracle embedding row has no valid vector"):
        OracleEmbeddingPushdown._extract_vector({"vec": "not-json"}, 3)


@pytest.mark.anyio
async def test_oracle_embedding_pushdown_batch_vectors_and_similarity_search() -> None:
    """Verify embed_documents and similarity_search unwrap and bind native VectorView over protocol doubles."""
    v1 = VectorBatchView.from_f32_rows([[0.25, -0.5, 1.0]], normalize=False)[0]
    v2 = VectorBatchView.from_f32_rows([[0.5, 0.25, -0.75]], normalize=False)[0]

    assert OracleEmbeddingPushdown._extract_batch_vectors(
        [{"utl_to_embeddings": [v1, v2]}],
        2,
        3,
    ) == [[0.25, -0.5, 1.0], [0.5, 0.25, -0.75]]
    assert OracleEmbeddingPushdown._extract_batch_vectors(
        [
            {"embed_data": {"embed_vector": [0.25, -0.5, 1.0]}},
            {"EMBED_DATA": {"EMBED_VECTOR": "[0.5, 0.25, -0.75]"}},
        ],
        2,
        3,
    ) == [[0.25, -0.5, 1.0], [0.5, 0.25, -0.75]]

    async_stub = _StubAsyncOracleClient()
    async_stub.queued_results.extend(
        [
            _oracle.OracleQueryResult(
                [_oracle.OracleColumn("UTL_TO_EMBEDDINGS", "VECTOR")],
                [([v1, v2],)],
                rowcount=1,
            ),
            _oracle.OracleQueryResult(
                [_oracle.OracleColumn("ID", "NUMBER"), _oracle.OracleColumn("DISTANCE", "BINARY_DOUBLE")],
                [(1, 0.05)],
                rowcount=1,
            ),
        ]
    )
    async with AsyncOracleDriver(client=async_stub) as driver:
        pushdown = OracleEmbeddingPushdown(
            driver,
            dimensions=3,
            provider="custom",
            normalize=False,
        )
        batch = await pushdown.embed_documents(["alpha", "beta"], normalize=False)
        assert isinstance(batch, VectorBatchView)
        assert len(batch) == 2
        assert batch[0].to_list() == [0.25, -0.5, 1.0]
        assert batch[1].to_list() == [0.5, 0.25, -0.75]

        rows = await pushdown.similarity_search(
            v1,
            table="docs",
            embedding_column="embedding",
            top_k=5,
            metric="cosine",
        )
        assert rows == [{"id": 1, "distance": 0.05}]
        query_calls = [payload for kind, payload in async_stub.calls if kind == "query"]
        assert "DBMS_VECTOR.UTL_TO_EMBEDDINGS" in query_calls[0]["sql"]
        search_call = query_calls[1]
        assert "VECTOR_DISTANCE(embedding, TO_VECTOR(:query_vector), COSINE)" in search_call["sql"]
        assert search_call["params"]["top_k"] == 5
        assert msgspec.json.decode(search_call["params"]["query_vector"]) == [0.25, -0.5, 1.0]


@pytest.mark.parametrize(
    ("wait_timeout_seconds", "valid"),
    [
        (1, True),
        (5, True),
        (30, True),
        (0, False),
        (-3, False),
    ],
)
def test_oracle_notify_config_validates_wait_timeout_seconds(
    wait_timeout_seconds: int,
    valid: bool,
) -> None:
    """Verify OracleNotifyConfig requires wait_timeout_seconds >= 1."""
    if valid:
        cfg = OracleNotifyConfig(wait_timeout_seconds=wait_timeout_seconds)
        assert cfg.wait_timeout_seconds == wait_timeout_seconds
    else:
        with pytest.raises(ValueError, match="wait_timeout_seconds"):
            OracleNotifyConfig(wait_timeout_seconds=wait_timeout_seconds)


def test_sync_oracle_queue_store_lifecycle() -> None:
    """Verify SyncOracleQueueStore protocol conformance, :param binds, claims, fencing, and DLQ."""
    drv = _FakeSyncOracleDriver()
    notify = _FakeSyncOracleNotifySource()
    store = SyncOracleQueueStore(cast("Any", drv), notification_source=cast("Any", notify))
    assert isinstance(store, SyncQueueStore)

    store.ensure_schema()
    assert any("CREATE TABLE vertebra_queue_jobs" in call[0] for call in drv.executed)

    entry = QueueEntry(
        job_id="ora-job-1",
        queue="critical",
        task_name="tasks.billing",
        payload=b'{"amount": 100}',
        headers={"x-correlation": "corr-1"},
    )
    assigned = store.enqueue(entry)
    assert assigned == "ora-job-1"
    assert notify.published == [("critical", "wakeup:ora-job-1")]

    drv.select_rows = [
        {
            "JOB_ID": "ora-job-1",
            "QUEUE": "critical",
            "TASK_NAME": "tasks.billing",
            "PAYLOAD": b'{"amount": 100}',
            "ATTEMPT": 1,
            "MAX_ATTEMPTS": 3,
            "STATUS": "ready",
            "LEASE_UNTIL_MS": 1000,
            "SCHEDULED_AT_MS": None,
            "HEADERS": '{"x-correlation": "corr-1"}',
        }
    ]
    claimed = store.claim(["critical"], worker_id="ora-w1", lease_seconds=30.0)
    assert len(claimed) == 1
    assert claimed[0].job_id == "ora-job-1"
    assert claimed[0].headers == {"x-correlation": "corr-1"}
    assert any("FOR UPDATE SKIP LOCKED" in call[0] for call in drv.executed)

    assert store.ack("ora-job-1", attempt=1) is True
    assert store.heartbeat(["ora-job-1"], worker_id="ora-w1", lease_seconds=15.0) == 1

    drv.select_rows = [
        {
            "JOB_ID": "ora-job-1",
            "QUEUE": "critical",
            "TASK_NAME": "tasks.billing",
            "PAYLOAD": b'{"amount": 100}',
            "ATTEMPT": 1,
            "MAX_ATTEMPTS": 3,
            "STATUS": "claimed",
            "LEASE_UNTIL_MS": 1000,
            "SCHEDULED_AT_MS": None,
            "HEADERS": "{}",
        }
    ]
    assert store.nack("ora-job-1", attempt=1, error="network failure", retry_delay_s=10.0) is True

    drv.select_rows = [
        {
            "JOB_ID": "ora-job-1",
            "QUEUE": "critical",
            "TASK_NAME": "tasks.billing",
            "PAYLOAD": b'{"amount": 100}',
            "ATTEMPT": 3,
            "MAX_ATTEMPTS": 3,
            "STATUS": "claimed",
            "LEASE_UNTIL_MS": 1000,
            "SCHEDULED_AT_MS": None,
            "HEADERS": "{}",
        }
    ]
    assert store.nack("ora-job-1", attempt=3, error="fatal error") is True

    assert store.reserve_unique_key("ora-key-1", ttl_seconds=60.0) is True
    store.release_unique_key("ora-key-1")
    assert store.acquire_lease("ora-lease-1", owner_id="ora-w1", ttl_seconds=60.0) is True
    store.release_lease("ora-lease-1", owner_id="ora-w1")


@pytest.mark.anyio
async def test_async_oracle_queue_store_lifecycle() -> None:
    """Verify AsyncOracleQueueStore protocol conformance, :param binds, claims, fencing, and DLQ."""
    drv = _FakeAsyncOracleDriver()
    notify = _FakeAsyncOracleNotifySource()
    store = AsyncOracleQueueStore(cast("Any", drv), notification_source=cast("Any", notify))
    assert isinstance(store, AsyncQueueStore)

    await store.ensure_schema()
    assert any("CREATE TABLE vertebra_queue_jobs" in call[0] for call in drv.executed)

    entry = QueueEntry(
        job_id="ora-async-1",
        queue="analytics",
        task_name="tasks.aggregate",
        payload=b'{"count": 50}',
        headers={"tenant": "acme"},
    )
    assigned = await store.enqueue(entry)
    assert assigned == "ora-async-1"
    assert notify.published == [("analytics", "wakeup:ora-async-1")]

    drv.select_rows = [
        {
            "JOB_ID": "ora-async-1",
            "QUEUE": "analytics",
            "TASK_NAME": "tasks.aggregate",
            "PAYLOAD": b'{"count": 50}',
            "ATTEMPT": 1,
            "MAX_ATTEMPTS": 3,
            "STATUS": "ready",
            "LEASE_UNTIL_MS": 2000,
            "SCHEDULED_AT_MS": None,
            "HEADERS": '{"tenant": "acme"}',
        }
    ]
    claimed = await store.claim(["analytics"], worker_id="async-w1", lease_seconds=20.0)
    assert len(claimed) == 1
    assert claimed[0].job_id == "ora-async-1"
    assert any("FOR UPDATE SKIP LOCKED" in call[0] for call in drv.executed)

    assert await store.ack("ora-async-1", attempt=1) is True
    assert await store.heartbeat(["ora-async-1"], worker_id="async-w1", lease_seconds=20.0) == 1
    assert await store.reserve_unique_key("ora-async-key", ttl_seconds=30.0) is True
    await store.release_unique_key("ora-async-key")
    assert await store.acquire_lease("ora-async-lease", owner_id="async-w1", ttl_seconds=30.0) is True
    await store.release_lease("ora-async-lease", owner_id="async-w1")


def test_sync_oracle_queue_store_extended_capabilities() -> None:
    """Verify SyncOracleQueueStore extended reservation, heartbeat, expiration, and cross-queue listing."""
    drv = _FakeSyncOracleDriver()
    store = SyncOracleQueueStore(cast("Any", drv), table_name="custom_ora_jobs")
    assert isinstance(store, SyncQueueStore)

    with pytest.raises(ValueError):
        SyncOracleQueueStore(cast("Any", drv), table_name="bad; DROP TABLE x")

    store.ensure_schema()
    ddl_sql = "\n".join(call[0] for call in drv.executed)
    assert "priority NUMBER(10)" in ddl_sql
    assert "expires_at_ms NUMBER(19)" in ddl_sql
    assert "completed_at_ms NUMBER(19)" in ddl_sql
    assert "heartbeat_at_ms NUMBER(19)" in ddl_sql
    assert "job_id VARCHAR2(64)" in ddl_sql
    assert "task_name VARCHAR2(256)" in ddl_sql
    assert "created_at_ms NUMBER(19)" in ddl_sql

    drv.executed.clear()
    drv.affected_rows = 1
    assert store.reserve_identity("idem-forever", job_id="job-1", task_name="tasks.send", ttl_seconds=None) is None
    merge_sql, merge_params = drv.executed[-1]
    assert "MERGE INTO vertebra_queue_unique_keys" in merge_sql
    assert merge_params["expires_at_ms"] is None
    assert "expires_at_ms IS NOT NULL" in merge_sql

    drv.affected_rows = 0
    drv.select_rows = [
        {
            "UNIQUE_KEY": "idem-forever",
            "JOB_ID": "job-1",
            "TASK_NAME": "tasks.send",
            "CREATED_AT_MS": 1700000000000,
        }
    ]
    existing = store.reserve_identity("idem-forever", job_id="job-2", task_name="tasks.send", ttl_seconds=60.0)
    assert existing == QueueReservation(
        key="idem-forever",
        job_id="job-1",
        task_name="tasks.send",
        created_at_ms=1700000000000,
    )
    fetched_res = store.get_reservation("idem-forever")
    assert fetched_res == existing

    drv.affected_rows = 1
    assert store.reset_identity("idem-forever", expected_job_id="job-1") is True
    reset_sql, reset_params = drv.executed[-1]
    assert "expected_job_id" in reset_params
    assert "job_id = :expected_job_id" in reset_sql

    drv.executed.clear()
    deleted_count = store.cleanup_terminal(before_ms=1700000005000, limit=25)
    assert deleted_count == 1
    cleanup_job_sql, cleanup_job_params = drv.executed[0]
    cleanup_key_sql, cleanup_key_params = drv.executed[1]
    assert "FETCH FIRST :limit_val ROWS ONLY" in cleanup_job_sql
    assert cleanup_job_params == {"before_ms": 1700000005000, "limit_val": 25}
    assert "expires_at_ms IS NOT NULL" in cleanup_key_sql
    assert cleanup_key_params == {"before_ms": 1700000005000}

    assert store.assign_worker("job-1", worker_id="worker-9", attempt=1) is True
    assert store.touch_heartbeats(["job-1", "job-2"], heartbeat_at_ms=1700000001000) == 1
    assert store.null_heartbeats(["job-1"]) == 1
    assert store.interrupt_task("job-1", error="operator abort") is True
    assert store.expire_overdue(now_ms=1700000009000, limit=10) == 1
    exp_sql, exp_params = drv.executed[-1]
    assert "status = 'expired'" in exp_sql
    assert exp_params == {"now_ms": 1700000009000, "limit_val": 10}

    drv.select_rows = [
        {
            "JOB_ID": "job-1",
            "QUEUE": "default",
            "TASK_NAME": "tasks.send",
            "PAYLOAD": b"{}",
            "ATTEMPT": 1,
            "MAX_ATTEMPTS": 3,
            "LEASE_UNTIL_MS": 0,
            "SCHEDULED_AT_MS": None,
            "HEADERS": '{"key": "idem-1"}',
            "PRIORITY": 5,
            "EXPIRES_AT_MS": 1700000099000,
            "COMPLETED_AT_MS": None,
            "WORKER_ID": "worker-9",
            "HEARTBEAT_AT_MS": 1700000001000,
            "RESULT": b"ok",
            "ERROR": None,
        }
    ]
    by_key = store.get_entry_by_key("idem-1", queue="default")
    assert by_key is not None
    assert by_key.priority == 5
    assert by_key.worker_id == "worker-9"
    assert by_key.result == b"ok"

    drv.executed.clear()
    cross_entries = store.list_entries(queue=None, status="ready", limit=50)
    assert len(cross_entries) == 1
    list_sql, list_params = drv.executed[-1]
    assert "queue = :queue" not in list_sql
    assert "WHERE status = :status" in list_sql
    assert list_params == {"status": "ready", "limit_val": 50}


@pytest.mark.anyio
async def test_async_oracle_queue_store_extended_capabilities_and_dlq_replay() -> None:
    """Verify AsyncOracleQueueStore extended capabilities, cross-queue listing, and DLQ pop narrowing."""
    drv = _FakeAsyncOracleDriver()
    store = AsyncOracleQueueStore(cast("Any", drv))
    assert isinstance(store, AsyncQueueStore)

    drv.affected_rows = 1
    assert await store.reserve_identity("async-idem", job_id="ajob-1", task_name="tasks.async") is None
    drv.affected_rows = 0
    drv.select_rows = [
        {
            "UNIQUE_KEY": "async-idem",
            "JOB_ID": "ajob-1",
            "TASK_NAME": "tasks.async",
            "CREATED_AT_MS": 1700000000100,
        }
    ]
    owner = await store.reserve_identity("async-idem", job_id="ajob-2", task_name="tasks.async", ttl_seconds=10.0)
    assert owner is not None
    assert owner.job_id == "ajob-1"
    assert await store.get_reservation("async-idem") == owner

    drv.affected_rows = 1
    assert await store.reset_identity("async-idem") is True
    assert await store.cleanup_terminal(before_ms=1700000010000) == 1
    assert await store.assign_worker("ajob-1", worker_id="w-async", attempt=2) is True
    assert await store.touch_heartbeats(["ajob-1"], heartbeat_at_ms=1700000002000) == 1
    assert await store.null_heartbeats(["ajob-1"]) == 1
    assert await store.interrupt_task("ajob-1") is True
    assert await store.expire_overdue(now_ms=1700000020000) == 1

    drv.select_rows = [
        {
            "JOB_ID": "ajob-1",
            "QUEUE": "batch",
            "TASK_NAME": "tasks.async",
            "PAYLOAD": b"{}",
            "ATTEMPT": 1,
            "MAX_ATTEMPTS": 3,
            "LEASE_UNTIL_MS": 0,
            "SCHEDULED_AT_MS": None,
            "HEADERS": "{}",
            "PRIORITY": 2,
        }
    ]
    assert await store.get_entry_by_key("async-idem") is not None
    drv.executed.clear()
    listed = await store.list_entries(queue=None, status=None, limit=15)
    assert len(listed) == 1
    list_sql, list_params = drv.executed[-1]
    assert "WHERE" not in list_sql
    assert list_params == {"limit_val": 15}

    backend = OracleQueueBackend(store=store)
    drv.select_rows = [
        {
            "JOB_ID": "dlq-1",
            "QUEUE": "io",
            "TASK_NAME": "tasks.fail",
            "PAYLOAD": b"{}",
            "ATTEMPT": 3,
            "MAX_ATTEMPTS": 3,
            "LEASE_UNTIL_MS": 0,
            "SCHEDULED_AT_MS": None,
            "HEADERS": "{}",
        }
    ]
    replayed = await backend.replay_dlq("dlq-1")
    assert replayed is not None
    assert replayed.task_name == "tasks.fail"
    assert replayed.attempt == 1
