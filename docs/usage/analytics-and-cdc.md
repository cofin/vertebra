# Row-Level Security, CDC & Arrow Analytics

Vertebra combines multi-tenant PostgreSQL Row-Level Security (`vertebra.db.rls`), engine-neutral Change Data Capture (`vertebra.db.cdc`), and zero-copy shared-memory Apache Arrow / DuckDB / DataFusion analytics.

## Multi-Tenant Row-Level Security (`vertebra.db.rls`)

`TenantPredicate` and `TenantScopeConfig` enforce PostgreSQL tenant isolation by combining transaction-local session GUCs (`SELECT set_config('app.tenant_id', $1, true)`) with planner-unfenced predicates (`alias.tenant_id = $1::uuid`, casting the parameter rather than the column so B-tree indexes remain usable):

```python
from vertebra.db.rls import TenantPredicate, TenantScopeConfig

tenant_id = "0192f0a0-0000-7000-8000-000000000001"
scope = TenantScopeConfig(tenant_column="tenant_id", parameter_cast="uuid")
predicate = TenantPredicate(
    table_alias="o",
    tenant_value=tenant_id,
    config=scope,
)
guc_sql, guc_params = TenantPredicate.build_session_guc_sql(
    tenant_id,
    scope,
)
where_clause = predicate.to_sql_fragment(param_index=1)
```

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

## Zero-Copy Arrow SHM (`ArrowCursor`), DuckDB & DataFusion

Result sets exceeding `ARROW_SHM_THRESHOLD_ROWS` (`256` rows or `64 KiB`) bypass per-row Python tuple allocation and stream directly as Apache Arrow IPC buffers inside POSIX shared memory (`ArrowCursor`) via `driver.select_to_arrow(...)`:

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
