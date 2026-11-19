---
type: Knowledge
title: SQLSpec Adapter, Proxy & Store Limitations
description: Operational boundaries for SQLSpec 0.72.2+ nested schema mapping, data dictionary bases, native Oracle sessionless hooks, session reset semantics, storage alias registration, ADK extras, and TxEventQ acknowledgment.
status: stable
tags:
  - domains
  - extensions
  - sqlspec
  - adk
  - oracle
  - storage
  - limitations
generated:
  by: flow/0.30.1
  at: "2026-11-17T06:55:00Z"
last_reviewed_commit: 7ae5ae50
verified:
  - by: agent:flow-executor
    at: "2026-11-18T14:25:00Z"
    commit: 7ae5ae50
sources:
  - id: src-pyproject
    resource: pyproject.toml
    title: SQLSpec >=0.72.2 PyPI dependency floor without upstream git pins
  - id: src-sqlspec-mysql-dict
    resource: src/py/vertebra/extensions/sqlspec/adapters/mysql/data_dictionary.py
    title: MySQL data dictionary subclassing upstream sqlspec.data_dictionary.dialects.mysql.dictionary
  - id: src-sqlspec-pg-dict
    resource: src/py/vertebra/extensions/sqlspec/adapters/postgres/data_dictionary.py
    title: Direct SyncDataDictionaryBase/AsyncDataDictionaryBase subclassing when upstream dialect dictionary is absent
  - id: src-sqlspec-oracle-driver
    resource: src/py/vertebra/extensions/sqlspec/adapters/oracle/driver.py
    title: Oracle SQLSpec driver sessionless transaction FFI checks
  - id: src-sqlspec-oracle-events
    resource: src/py/vertebra/extensions/sqlspec/adapters/oracle/events.py
    title: Oracle AQ and TxEventQ dequeue-commit and nack republish behavior
  - id: src-sqlspec-bigquery-typing
    resource: src/py/vertebra/extensions/sqlspec/adapters/bigquery/_typing.py
    title: BigQuery reset_session_on_release and stateless REST no-op behavior
  - id: src-sqlspec-spanner-typing
    resource: src/py/vertebra/extensions/sqlspec/adapters/spanner/_typing.py
    title: Spanner reset_session_on_release local flag cleanup
  - id: src-sqlspec-duckdb-typing
    resource: src/py/vertebra/extensions/sqlspec/adapters/duckdb/_typing.py
    title: DuckDB reset_session_on_release temporary object cleanup and baseline pragma restoration
  - id: src-sqlspec-mssql-typing
    resource: src/py/vertebra/extensions/sqlspec/adapters/mssql/_typing.py
    title: MSSQL reset_session_on_release EXEC sp_reset_connection batch fallback
  - id: src-sqlspec-storage
    resource: src/py/vertebra/extensions/sqlspec/storage.py
    title: SQLSpec StorageRegistry.register_alias delegation and direct ObjectStoreBackend construction
  - id: src-sqlspec-postgres-adk
    resource: src/py/vertebra/extensions/sqlspec/adapters/postgres/adk.py
    title: ADK memory and artifact store google-adk extra enforcement via ensure_module
  - id: src-sqlspec-bigquery-driver
    resource: src/py/vertebra/extensions/sqlspec/adapters/bigquery/driver.py
    title: BigQuery tenant isolation rejection guard
  - id: src-sqlspec-spanner-driver
    resource: src/py/vertebra/extensions/sqlspec/adapters/spanner/driver.py
    title: Spanner tenant isolation rejection guard
  - id: src-sqlspec-postgres-config
    resource: src/py/vertebra/extensions/sqlspec/adapters/postgres/config.py
    title: PostgresSyncConfig and PostgresAsyncConfig derive_pool cloud auth preservation
  - id: src-sqlspec-mysql-config
    resource: src/py/vertebra/extensions/sqlspec/adapters/mysql/config.py
    title: MySQLSyncConfig and MySQLAsyncConfig derive_pool cloud auth and ssl_ca preservation
  - id: src-dbapi-sync
    resource: src/py/vertebra/db/dbapi/_sync.py
    title: PEP 249 connect_args allowlists and +vtb scheme normalization
---

# SQLSpec Adapter, Proxy & Store Limitations

<!-- truth: start -->
- `pyproject.toml` declares `"sqlspec>=0.72.2"` from PyPI with zero `[tool.uv.sources]` upstream git pins; driver-level `to_schema` and tabular row hydrators default to `normalize_nested=True` for SQL projections, while standalone `sqlspec.utils.schema.to_schema` and `vertebra.utils.schema.to_schema` default to `normalize_nested=False` [^src-pyproject].
- In `sqlspec>=0.72.2`, `sqlspec.data_dictionary.dialects.<dialect>.dictionary` exists only for MySQL; `MySQLSyncDataDictionary` and `MySQLAsyncDataDictionary` subclass it, while the PostgreSQL, Oracle, SQLite, MSSQL, DuckDB, BigQuery, and Spanner dictionaries subclass `SyncDataDictionaryBase` and `AsyncDataDictionaryBase` directly [^src-sqlspec-mysql-dict] [^src-sqlspec-pg-dict].
- Native pure-Rust `vertebra._oracle` does not expose FFI `begin_sessionless_transaction`, `resume_sessionless_transaction`, or `suspend_sessionless_transaction` hooks (nor `suspend_on_success` execution), so `OracleSyncDriver`, `OracleAsyncDriver`, and the Oracle sessionless context managers raise `DialectNotSupportedError` against `vertebra._oracle` until native FFI support lands [^src-sqlspec-oracle-driver].
- `reset_session_on_release=True` is a no-op on stateless BigQuery REST connections without an active `session_id`, falling back to `CALL BQ.ABORT_SESSION(@session_id)` only when a session exists. On Spanner without a native `reset_session` hook, it clears only local transaction flags. On DuckDB, it drops temporary objects plus non-baseline attached databases and re-applies baseline pragmas. On MSSQL without a native reset hook, it executes `EXEC sp_reset_connection` as a T-SQL batch rather than setting a TDS `RESETCONNECTION` packet flag [^src-sqlspec-bigquery-typing] [^src-sqlspec-spanner-typing] [^src-sqlspec-duckdb-typing] [^src-sqlspec-mssql-typing].
- Upstream `StorageRegistry.register_alias` accepts only `"local"`, `"obstore"`, and `"fsspec"` backend names and rejects custom `ObjectStoreBackend` instances or pre-configured `SyncObjectStore` / `AsyncObjectStore` signing state (`SigningConfig`, credentials, custom endpoints); `register_storage_alias` forwards only the URI string without touching `_alias_configs`, so callers needing custom native stores or signing must construct `ObjectStoreBackend(store=...)` directly [^src-sqlspec-storage].
- All copied fallback ADK memory and artifact stores are deleted; `<Engine>{Sync,Async}ADKMemoryStore` and `<Engine>{Sync,Async}ADKArtifactStore` load `sqlspec.extensions.adk` via `ensure_module(..., package="google-adk", extra="adk")` and fail with `MissingDependencyError` when `google-adk` is absent [^src-sqlspec-postgres-adk].
- `OracleSyncEventsBackend` and `OracleAsyncEventsBackend` commit `DBMS_AQ.DEQUEUE` (TxEventQ) messages at dequeue time and re-enqueue via `publish` on `nack()`. DuckDB isolates plain `:memory:` instances per config while sharing `:memory:<name>` catalogs across drivers. BigQuery and Spanner configs and drivers reject non-None `tenant_predicate` or `tenant_id` with `NotSupportedError` [^src-sqlspec-oracle-events] [^src-sqlspec-bigquery-driver] [^src-sqlspec-spanner-driver].
- `PostgresSyncConfig.derive_pool` / `PostgresAsyncConfig.derive_pool` and `MySQLSyncConfig.derive_pool` / `MySQLAsyncConfig.derive_pool` preserve `driver_config.token_provider` (including MySQL-scoped `AzureEntraTokenProvider` instances) across replica overrides. They also retain TLS CA settings (`ca_pem` on PostgreSQL, `ssl_ca` on MySQL unless superseded by a replica URL `?ssl_ca=` query parameter). Because SigV4 providers (`AwsRdsIamTokenProvider`, `AwsAuroraDsqlIamTokenProvider`) bind a fixed `endpoint_host`, distinct AWS replica hostnames require an explicit `token_provider` override. In PEP 249 and SQLAlchemy `+vtb` `connect_args`, `PostgreSQLConfig.ca_pem` and `MySQLConfig.ssl_ca` accept `str` while `MSSQLConfig.ca_pem` requires `bytes`. Cloud IAM dialect forwarding was verified against in-memory `DatabaseTokenProvider` fixtures rather than live AWS or Azure endpoints [^src-sqlspec-postgres-config] [^src-sqlspec-mysql-config] [^src-dbapi-sync].
<!-- truth: end -->

## Operational Boundaries

- **Driver vs. Standalone Nested `msgspec.Struct` Normalization (`sqlspec>=0.72.2`)**: [`pyproject.toml`](../../../../../pyproject.toml)[^src-pyproject] declares `"sqlspec>=0.72.2"` from PyPI without upstream git pins. Driver-level `CommonDriverAttributesMixin.to_schema`, `RoutingSession.to_schema`, and Vertebra's tabular row hydrators (`_hydrate_rows_with_schema`, `BigQueryCore.hydrate_query_rows`, `SpannerCore.hydrate_query_rows`) default to `normalize_nested=True` (with `normalize_nested=False` opt-out), whereas standalone `sqlspec.utils.schema.to_schema` and `vertebra.utils.schema.to_schema` default to `normalize_nested=False`.
- **Data Dictionary Upstream Base Coverage**: Upstream `sqlspec.data_dictionary.dialects` ships a shared `dictionary` base class only for MySQL ([`mysql/data_dictionary.py`](../../../../../src/py/vertebra/extensions/sqlspec/adapters/mysql/data_dictionary.py)[^src-sqlspec-mysql-dict]). PostgreSQL ([`postgres/data_dictionary.py`](../../../../../src/py/vertebra/extensions/sqlspec/adapters/postgres/data_dictionary.py)[^src-sqlspec-pg-dict]), Oracle, SQLite, MSSQL, DuckDB, BigQuery, and Spanner implement their dialect queries directly on `SyncDataDictionaryBase` and `AsyncDataDictionaryBase` without cross-importing `sqlspec.adapters.*`.
- **Oracle 26ai Sessionless Transaction FFI Gap**: [`OracleSyncDriver`](../../../../../src/py/vertebra/extensions/sqlspec/adapters/oracle/driver.py)[^src-sqlspec-oracle-driver] and `OracleAsyncDriver` implement `begin_sessionless_transaction`, `resume_sessionless_transaction`, `suspend_sessionless_transaction`, `sessionless_transaction(...)`, and `execute(..., suspend_on_success=True)`. Because pure-Rust `vertebra._oracle` does not expose native sessionless FFI hooks yet, calls against `vertebra._oracle` connections fail loudly with `DialectNotSupportedError`, while external or test connections that expose those hooks run them directly.
- **`reset_session_on_release` Engine & Transport Caveats**:
  - **BigQuery**: [`bigquery/_typing.py`](../../../../../src/py/vertebra/extensions/sqlspec/adapters/bigquery/_typing.py)[^src-sqlspec-bigquery-typing] invokes `reset_session()` / `reset()` if present or runs `CALL BQ.ABORT_SESSION(@session_id)` when `session_id` / `_session_id` is set; stateless REST connections without an active BigQuery session perform no remote call.
  - **Spanner**: [`spanner/_typing.py`](../../../../../src/py/vertebra/extensions/sqlspec/adapters/spanner/_typing.py)[^src-sqlspec-spanner-typing] invokes `reset_session()` / `reset()` if present and clears local `_active_transaction` / `_in_transaction` flags because Spanner exposes no SQL `RESET ALL` command.
  - **DuckDB**: [`duckdb/_typing.py`](../../../../../src/py/vertebra/extensions/sqlspec/adapters/duckdb/_typing.py)[^src-sqlspec-duckdb-typing] drops temporary macros, views, tables, and sequences in `temp.main`, restores the baseline default database while detaching non-baseline databases, and re-applies baseline `PRAGMA` / `SET` statements; non-temporary tables or views created in file-backed catalogs persist across releases. Plain `:memory:` databases stay isolated per config, while `:memory:<name>` shares named in-memory catalogs across drivers.
  - **SQL Server (`sp_reset_connection`)**: [`mssql/_typing.py`](../../../../../src/py/vertebra/extensions/sqlspec/adapters/mssql/_typing.py)[^src-sqlspec-mssql-typing] prefers `reset_session()` / `reset()` when provided by the connection and otherwise executes `EXEC sp_reset_connection` as a SQL batch rather than setting the TDS `RESETCONNECTION` (`0x08`) wire-packet flag, so transport permissions and an open connection are required.
- **`StorageRegistry.register_alias` vs. Native Stores**: [`sqlspec/storage.py`](../../../../../src/py/vertebra/extensions/sqlspec/storage.py)[^src-sqlspec-storage] delegates `register_storage_alias` to upstream `StorageRegistry.register_alias`, which validates `backend` against `"local"`, `"obstore"`, and `"fsspec"` only and discards custom `ObjectStoreBackend` instances or `SyncObjectStore` / `AsyncObjectStore` `SigningConfig` state. Instantiate `ObjectStoreBackend(store=...)` directly whenever custom native store configuration or presigned URL signing is needed.
- **Mandatory `google-adk` Extra**: ADK session, memory, and artifact stores across all eight engines (`adk.py`) carry no fallback implementations and load `sqlspec.extensions.adk` via `ensure_module(..., package="google-adk", extra="adk")` ([`postgres/adk.py`](../../../../../src/py/vertebra/extensions/sqlspec/adapters/postgres/adk.py)[^src-sqlspec-postgres-adk]), raising `MissingDependencyError` when `google-adk` (`vertebra-core[adk]`) is absent.
- **Oracle TxEventQ Acknowledgment & Cloud Tenant Guards**: [`OracleSyncEventsBackend`](../../../../../src/py/vertebra/extensions/sqlspec/adapters/oracle/events.py)[^src-sqlspec-oracle-events] and `OracleAsyncEventsBackend` commit `DBMS_AQ.DEQUEUE` messages upon dequeue and re-publish on `nack()`. [`BigQuery`](../../../../../src/py/vertebra/extensions/sqlspec/adapters/bigquery/driver.py)[^src-sqlspec-bigquery-driver] and [`Spanner`](../../../../../src/py/vertebra/extensions/sqlspec/adapters/spanner/driver.py)[^src-sqlspec-spanner-driver] configs and drivers reject `tenant_predicate` and `tenant_id` with `NotSupportedError` before base initialization or native calls.
- **Cloud IAM `token_provider` & Root CA Replica Inheritance**: [`PostgresSyncConfig`](../../../../../src/py/vertebra/extensions/sqlspec/adapters/postgres/config.py)[^src-sqlspec-postgres-config] / `PostgresAsyncConfig` and [`MySQLSyncConfig`](../../../../../src/py/vertebra/extensions/sqlspec/adapters/mysql/config.py)[^src-sqlspec-mysql-config] / `MySQLAsyncConfig` carry `driver_config.token_provider` and Root CA bundles (`ca_pem` for PostgreSQL, `ssl_ca` for MySQL unless overridden explicitly or via replica URL `?ssl_ca=`) across `derive_pool` and `replicas` definitions. Because `AwsRdsIamTokenProvider` and `AwsAuroraDsqlIamTokenProvider` bind SigV4 presigned URLs to a single `endpoint_host:port`, replicas on separate AWS endpoints must pass an explicit replica `token_provider` override, whereas `AzureEntraTokenProvider` tokens are resource-scoped (`AZURE_POSTGRES_SCOPE` / `AZURE_MYSQL_SCOPE`) and share the cached provider instance across primary and replica pools.

[^src-pyproject]: `pyproject.toml` — SQLSpec `>=0.72.2` PyPI dependency floor without upstream git pins.
[^src-sqlspec-mysql-dict]: `src/py/vertebra/extensions/sqlspec/adapters/mysql/data_dictionary.py` — MySQL data dictionary subclassing `sqlspec.data_dictionary.dialects.mysql.dictionary`.
[^src-sqlspec-pg-dict]: `src/py/vertebra/extensions/sqlspec/adapters/postgres/data_dictionary.py` — Direct `SyncDataDictionaryBase` / `AsyncDataDictionaryBase` subclassing without an upstream dialect dictionary.
[^src-sqlspec-oracle-driver]: `src/py/vertebra/extensions/sqlspec/adapters/oracle/driver.py` — Oracle SQLSpec driver sessionless transaction FFI checks.
[^src-sqlspec-oracle-events]: `src/py/vertebra/extensions/sqlspec/adapters/oracle/events.py` — Oracle AQ and TxEventQ dequeue-commit and `nack()` republish behavior.
[^src-sqlspec-bigquery-typing]: `src/py/vertebra/extensions/sqlspec/adapters/bigquery/_typing.py` — BigQuery `reset_session_on_release` and stateless REST no-op behavior.
[^src-sqlspec-spanner-typing]: `src/py/vertebra/extensions/sqlspec/adapters/spanner/_typing.py` — Spanner `reset_session_on_release` local flag cleanup.
[^src-sqlspec-duckdb-typing]: `src/py/vertebra/extensions/sqlspec/adapters/duckdb/_typing.py` — DuckDB `reset_session_on_release` temporary object cleanup and baseline pragma restoration.
[^src-sqlspec-mssql-typing]: `src/py/vertebra/extensions/sqlspec/adapters/mssql/_typing.py` — MSSQL `reset_session_on_release` `EXEC sp_reset_connection` batch fallback.
[^src-sqlspec-storage]: `src/py/vertebra/extensions/sqlspec/storage.py` — `StorageRegistry.register_alias` delegation and direct `ObjectStoreBackend` construction.
[^src-sqlspec-postgres-adk]: `src/py/vertebra/extensions/sqlspec/adapters/postgres/adk.py` — ADK memory and artifact store `google-adk` extra enforcement via `ensure_module`.
[^src-sqlspec-bigquery-driver]: `src/py/vertebra/extensions/sqlspec/adapters/bigquery/driver.py` — BigQuery tenant isolation rejection guard.
[^src-sqlspec-spanner-driver]: `src/py/vertebra/extensions/sqlspec/adapters/spanner/driver.py` — Spanner tenant isolation rejection guard.
[^src-sqlspec-postgres-config]: `src/py/vertebra/extensions/sqlspec/adapters/postgres/config.py` — `PostgresSyncConfig` and `PostgresAsyncConfig` `derive_pool` cloud auth preservation.
[^src-sqlspec-mysql-config]: `src/py/vertebra/extensions/sqlspec/adapters/mysql/config.py` — `MySQLSyncConfig` and `MySQLAsyncConfig` `derive_pool` cloud auth and `ssl_ca` preservation.
[^src-dbapi-sync]: `src/py/vertebra/db/dbapi/_sync.py` — PEP 249 `connect_args` allowlists and `+vtb` scheme normalization.
