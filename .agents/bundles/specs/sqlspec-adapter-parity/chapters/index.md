# Chapters: Feature-Complete SQLSpec Adapters (`sqlspec-adapter-parity`)

| Chapter | State | Tasks | Summary & Fast Chapter Gate |
| --- | --- | --- | --- |
| [`ch01-sqlspec-core-driver-and-artifact-ref`](ch01-sqlspec-core-driver-and-artifact-ref/spec.md) | `planned` | `3` | Bump `sqlspec>=0.71.1` + `adk` extra, implement `_driver.py` dispatch bases, `VTBARROW` `provider.py` cutover, and `vertebra.storage.ArtifactReference`. |
| [`ch02-sqlspec-postgres`](ch02-sqlspec-postgres/spec.md) | `planned` | `3` | Deliver `adapters/postgres/` (`$1` `NUMERIC`, Arrow `COPY`, portals, `DataDictionary`, ADK/Events/Litestar stores, `adapter_name='vertebra_postgres'`). |
| [`ch03-sqlspec-sqlite`](ch03-sqlspec-sqlite/spec.md) | `planned` | `3` | Deliver `adapters/sqlite/` (`QMARK` + `NAMED_COLON`, WAL pragmas, `DataDictionary`, `FTS5` ADK/Events/Litestar stores, `adapter_name='vertebra_sqlite'`). |
| [`ch04-sqlspec-mysql`](ch04-sqlspec-mysql/spec.md) | `planned` | `3` | Deliver `adapters/mysql/` (fixing `%s` -> `ParameterStyle.QMARK` `?`, MariaDB/MySQL `DataDictionary`, ADK/Events/Litestar stores, `adapter_name='vertebra_mysql'`). |
| [`ch05-sqlspec-oracle`](ch05-sqlspec-oracle/spec.md) | `planned` | `3` | Deliver `adapters/oracle/` (`:name`/`:1` binds, 23ai `VECTOR`/`OSON` `DataDictionary`, TxEventQ Events/ADK/Litestar stores, `adapter_name='vertebra_oracle'`). |
| [`ch06-sqlspec-mssql`](ch06-sqlspec-mssql/spec.md) | `planned` | `3` | Deliver `adapters/mssql/` (`dialect='tsql'`, `? -> @P1`, `MERGE HOLDLOCK` ADK/Events/Litestar stores, `DataDictionary`, `adapter_name='vertebra_mssql'`). |
| [`ch07-sqlspec-duckdb`](ch07-sqlspec-duckdb/spec.md) | `planned` | `3` | Deliver `adapters/duckdb/` (DuckDB 2.0+ `COPY` ordering, Arrow-divergent `fetchall()` fidelity, `DataDictionary`, ADK/Events/Litestar stores, `adapter_name='vertebra_duckdb'`). |
| [`ch08-sqlspec-bigquery`](ch08-sqlspec-bigquery/spec.md) | `planned` | `3` | Deliver `adapters/bigquery/` (`@name` binds, unsplit `BEGIN...END`, `VECTOR_SEARCH` ADK + `ArtifactReference`, `DataDictionary`, `adapter_name='vertebra_bigquery'`). |
| [`ch09-sqlspec-spanner`](ch09-sqlspec-spanner/spec.md) | `planned` | `3` | Deliver `adapters/spanner/` (`sqlglot` `'spanner'`/`'postgres'` dialect fix, ScaNN/RRF ADK + `ArtifactReference`, sharded stores, `adapter_name='vertebra_spanner'`). |
| [`ch10-sqlspec-cutover-storage-events-docs`](ch10-sqlspec-cutover-storage-events-docs/spec.md) | `planned` | `4` | Upgrade `ObjectStoreBackend(ObjectStoreBase)`, harden `ChangeFeed*EventBackend`, delete `{adapters,bigquery,spanner}.py`, and sync docs/OKF. |
