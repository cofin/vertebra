# Row-Level Security, CDC & Arrow Analytics

Vertebra includes dialect-agnostic tenant predicates, change-data-capture feeds, and shared-memory Apache Arrow analytics with DuckDB and DataFusion.

## Tenant Predicates and Session Settings

`TenantScopeConfig` names the tenant column, the database session setting, and an optional parameter type cast. `TenantPredicate` builds a SQL filter where the cast wraps the placeholder so the tenant column remains indexable. Always pass the tenant value as a query parameter.

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

With a UUID cast, the fragment is `o.tenant_id = CAST(:tenant_id AS UUID)`. Without a cast, it is `o.tenant_id = :tenant_id`. The default placeholder is `?`.

For PostgreSQL session settings and positional filters, use `PostgreSQLCore`:

```python
from vertebra.db.adapters.postgres.core import PostgreSQLCore

setting_sql, setting_params = PostgreSQLCore.build_session_guc_sql(
    str(predicate.tenant_value), scope
)
postgres_filter = PostgreSQLCore.build_tenant_predicate_sql(predicate)
```

Adapter behavior by engine:

- **PostgreSQL**: SQLSpec PostgreSQL adapters expose `apply_tenant_predicate()`, which prepares the session setting with native positional binds and returns the filter. Errors from the setter propagate unchanged.
- **Oracle**: Session context helpers validate the namespace and setting name, emitting `DBMS_SESSION.SET_IDENTIFIER` for USERENV, `DBMS_SESSION.SET_CONTEXT` for custom namespaces, or a call to your trusted package procedure. Oracle drivers clear the context before returning a connection to the pool and evict the session if reset fails. Custom namespaces require a trusted package bound via CREATE CONTEXT; see the [Oracle DBMS_SESSION documentation](https://docs.oracle.com/en/database/oracle/oracle-database/26/arpls/DBMS_SESSION.html).
- **BigQuery and Spanner**: Reject configured tenant predicates on initialization; ordinary sessions without tenant predicates behave normally.
- **Other SQLSpec adapters**: Raise `NotSupportedError` for tenant predicates.

Vertebra sets session context and builds filters, but does not create database RLS or VPD policies for you.

## Change Data Capture

`ChangeFeed` wraps change events in a consistent envelope with replay cursors and Parquet archival:

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

How streaming and spills work:

- **PostgreSQL notifications**: Pass the feed to a sync or async PostgreSQL notification source to publish parameterized notifications with automatic spill envelopes. Stream live events with `source.listen()`, or build logical message SQL with `build_emit_message_sql()`.
- **PostgreSQL logical replication**: For durable pgoutput streaming, pass a replication driver and slot/publication config to `PostgresChangeSource`. Checkpoints are saved before acknowledgments are sent back to PostgreSQL.
- **Payload spills**: Oversized payloads spill to a bounded in-memory LRU cache by default; hydrating an evicted entry raises `KeyError`. Attach an object store for durable spill storage.
- **Spanner change streams**: Import sync or async Spanner change-stream sources from `vertebra.db.adapters.spanner.change_stream`. Set defaults such as start timestamp in the constructor so `stream()` takes no required arguments.

## Arrow Analytics, DuckDB, and DataFusion

Query results exceeding 256 rows or 64 KiB stream directly into POSIX shared memory as an `ArrowCursor` via `select_to_arrow()`, avoiding per-row Python tuple allocation. DuckDB queries require the `duckdb` extra and raise `MissingDependencyError` without it.

```python
from vertebra.db.adapters.datafusion import DataFusionConfig, DataFusionEngine
from vertebra.db.adapters.duckdb import DuckDBConfig, DuckDBSecretConfig
from vertebra.db.registry import provide_driver
from vertebra.storage import GCSConfig, SyncGcsBearerSecretProvider

duck_cfg = DuckDBConfig(
    database=":memory:",
    threads=4,
    memory_limit_mb=2048,
    secrets=(
        DuckDBSecretConfig(
            name="lakehouse_s3",
            secret_type="s3",
            value={
                "key_id": "access-key",
                "secret": "secret-key",
                "region": "us-east-1",
            },
        ),
        DuckDBSecretConfig(
            name="gcs_lake",
            secret_type="gcs",
            value_provider=SyncGcsBearerSecretProvider.from_config(
                GCSConfig(bucket="lake"),
            ),
            scope="gs://lake/",
        ),
    ),
)
pg_driver = provide_driver("postgresql://localhost:5432/analytics", sync=True)
engine = DataFusionEngine(DataFusionConfig(mode="in_process", target_partitions=4))
arrow_cursor = pg_driver.select_to_arrow("SELECT * FROM orders")
engine.register_arrow_table("orders", arrow_cursor)
```

Key capabilities:

- **Arrow cursors**: Expose a memoryview, column names, Python row lists, and the Arrow PyCapsule interface so Polars, DuckDB, and DataFusion read data directly with zero extra Arrow dependencies. Wrap existing IPC bytes with `ArrowCursor.from_ipc_bytes()`. Converting to Polars requires the polars extra, and PyArrow table conversion requires the arrow extra. Exiting the cursor context manager releases shared memory, and calls after release raise an error.
- **DuckDB secrets**: `DuckDBSecretConfig` accepts static dictionaries or dynamic provider callables (including GCS bearer token providers) for S3, GCS, R2, Azure, HTTP, bearer, and Iceberg secrets. Dynamic providers require `persistent=False` and a positive `refresh_interval_seconds`. When httpfs returns HTTP 401 or 403, the driver invalidates the provider and re-raises without retrying the failed statement so the next query rotates the secret automatically.
- **Vector columns**: Convert vector batches into an Arrow cursor with `batch.to_arrow_shm()` or embed a text column directly with `embedder.embed_arrow()`.
