# Row-Level Security, CDC & Arrow Analytics

Vertebra provides dialect-agnostic tenant predicates, engine-neutral change feeds,
and shared-memory Apache Arrow / DuckDB / DataFusion analytics.

## Tenant Predicates and Session Settings

`TenantScopeConfig` names the tenant column, the adapter's session setting, and an
optional type cast. `TenantPredicate` renders an ANSI filter with a caller-selected
placeholder. The cast applies to the parameter so the column remains indexable.
Tenant values must be supplied separately as query parameters.

```python
from vertebra.db.rls import TenantPredicate, TenantScopeConfig

scope = TenantScopeConfig(
    setting_name="app.tenant_id",
    tenant_column="tenant_id",
    parameter_cast="UUID",
)
predicate = TenantPredicate(
    table_alias="o",
    tenant_value="0192f0a0-0000-7000-8000-000000000001",
    config=scope,
)
where_clause = predicate.to_sql_fragment(":tenant_id")
```

The fragment is `o.tenant_id = CAST(:tenant_id AS UUID)`. Without `parameter_cast`,
it is `o.tenant_id = :tenant_id`; the default placeholder is `?`.
`guc_name` and `TenantPredicate.build_session_guc_sql()` have been removed.

PostgreSQL session SQL belongs to `PostgreSQLCore`:

```python
from vertebra.db.adapters.postgres.core import PostgreSQLCore

setting_sql, setting_params = PostgreSQLCore.build_session_guc_sql(
    str(predicate.tenant_value), scope
)
postgres_filter = PostgreSQLCore.build_tenant_predicate_sql(predicate)
```

The SQLSpec PostgreSQL adapters' `apply_tenant_predicate()` prepares the session
statement for psycopg `%s` or asyncpg `$N` binds and returns the PostgreSQL filter.
Setter failures propagate unchanged. Other base SQLSpec adapters raise
`NotSupportedError`. BigQuery and Spanner configs and drivers reject a configured
`tenant_predicate` before initialization; ordinary unconfigured sessions retain
their existing query, script, Arrow, batch, and snapshot behavior.

`OracleCore.build_session_context_sql()` validates the namespace and final segment
of `setting_name`, then returns named context binds. Oracle drivers raise
`MissingDependencyError` because the native Oracle driver package is not yet
published, so no Vertebra session executes the block. Live
`DBMS_SESSION.SET_CONTEXT` requires a trusted package associated with
`CREATE CONTEXT`. See the
[Oracle DBMS_SESSION documentation](https://docs.oracle.com/en/database/oracle/oracle-database/26/arpls/DBMS_SESSION.html).
Vertebra does not provision database RLS or VPD policies.

## Change Data Capture (`vertebra.db.cdc`)

`ChangeFeed` (`CDCConfig`, `ChangeEvent`) and the `SyncChangeSource` / `AsyncChangeSource` capability protocols (`vertebra.db.driver`) provide event envelopes and change-stream capabilities with replay cursors (`replay_since()`) and Parquet archival (`flush_to_parquet()`):

```python
from vertebra.db.adapters.postgres import PostgresNotifyConfig
from vertebra.db.cdc import CDCConfig, ChangeFeed

feed = ChangeFeed(
    CDCConfig(
        channel="orders",
        spill_prefix="cdc/orders",
        max_memory_spills=256,
    )
)
notify_config = PostgresNotifyConfig(
    channel="orders",
    replication_slot_name="vertebra_orders_cdc",
    publication_name="orders_pub",
)
```

`PostgresNotificationSource(driver, notify_config, change_feed=feed)` publishes
parameterized PostgreSQL notifications using the feed's spill envelopes. Its
`build_emit_message_sql()` helper builds logical replication message SQL. Live
`LISTEN` streaming requires a driver that implements `listen_notifications()`.
In-memory spills use a bounded LRU cache; hydrating an evicted spill raises
`KeyError`. Attach a `SyncObjectStore` for persistent spill hydration; storage
errors propagate to the caller.

Spanner readers live in `vertebra.db.adapters.spanner.change_stream` as
`SyncSpannerChangeStreamSource` and `AsyncSpannerChangeStreamSource`. Bind stream
defaults such as `start_timestamp` in the constructor; the resulting `stream()`
method accepts no required arguments and satisfies `SyncChangeSource` or
`AsyncChangeSource`. They are not exported from `vertebra.db.cdc`.

## Zero-Copy Arrow SHM (`ArrowCursor`), DuckDB & DataFusion

Result sets exceeding `ARROW_SHM_THRESHOLD_ROWS` (`256` rows or `64 KiB`) bypass per-row Python tuple allocation and stream directly as Apache Arrow IPC buffers inside POSIX shared memory (`ArrowCursor`) via `driver.select_to_arrow(...)`. DuckDB execution requires the `duckdb` package and raises `MissingDependencyError` without it:

```python
from vertebra.db.adapters.datafusion import DataFusionConfig, DataFusionEngine
from vertebra.db.adapters.duckdb import DuckDBConfig

duck_cfg = DuckDBConfig(
    database=":memory:",
    threads=4,
    memory_limit_mb=2048,
)
engine = DataFusionEngine(DataFusionConfig(target_partitions=4))
```

- **`ArrowCursor`**: Exposes a zero-copy `memoryview` over the shared-memory `ShmRegion` (`buffer_view()`), materializes `pyarrow.Table` or `polars.DataFrame` without copying (`to_arrow_table()`, `to_pyarrow()`, `to_polars()`), and unlinks the `ShmRegion` deterministically on context-manager exit.
- **DuckDB `httpfs` Active ADC Rotation**: When querying `gs://` Parquet datasets in DuckDB, each pooled session tracks `applied_token_generation` against `SyncGcpTokenCache` / `AsyncGcpTokenCache` and executes `CREATE OR REPLACE SECRET vtb_gcs_adc (TYPE GCS, BEARER_TOKEN '...')` automatically whenever the Google Cloud ADC token refreshes.
- **Vector Columns to Arrow SHM**: Convert `VectorBatchView` embeddings directly into an `ArrowCursor` (`FixedSizeListArray(Float32, dims)`) via `batch.to_arrow_shm()` or `await embedder.embed_arrow(cursor, text_column="body")`.
