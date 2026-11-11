---
type: Research
flow_id: integration-remediation
title: "Audit findings for the merged d3d5f8b..142cc49 range"
description: "Verified findings from audits of database framework extensions, framework/embeddings/utils/server, the Rust GCP driver layer, MCP/WebMCP, and vertebra.db, mapped to owning child flows."
status: stable
state: promoted
promoted_to: integration-remediation
created_at: "2026-11-03T03:30:00Z"
updated_at: "2026-11-07T04:15:00Z"
generated:
  by: flow-setup
  at: "2026-11-07T04:15:00Z"
verified:
  - by: agent:flow-setup
    at: "2026-11-07T04:15:00Z"
    commit: 9af810f5
sources:
  - id: db-registry
    resource: src/py/vertebra/db/registry.py
    title: Database driver registry and adapter boundary
  - id: sqlspec-adapters
    resource: src/py/vertebra/extensions/sqlspec/adapters.py
    title: SQLSpec adapter drivers and store bindings
  - id: queue-base
    resource: src/py/vertebra/framework/queues/base.py
    title: First-party QueueService and backend selection
  - id: embeddings-pkg
    resource: src/py/vertebra/embeddings/__init__.py
    title: Embeddings package exports and transports
  - id: mcp-pkg
    resource: src/py/vertebra/agent/mcp/__init__.py
    title: MCP 2026-07-28 protocol package
  - id: utils-pkg
    resource: src/py/vertebra/utils/__init__.py
    title: Public Rust-backed utility helpers
  - id: server-fastpath
    resource: src/py/vertebra/server/_fastpath_core.py
    title: Consolidated framework fastpath provider
  - id: rs-db-lib
    resource: src/rs/vertebra-db/src/lib.rs
    title: Shared Rust database crate
  - id: rs-py-lib
    resource: src/rs/vertebra-py/src/lib.rs
    title: Core PyO3 extension bindings
  - id: rs-core-lib
    resource: src/rs/vertebra-core/src/lib.rs
    title: Core Rust runtime, SHM rings, and GCP auth
  - id: ts-webmcp
    resource: src/ts/vertebra-client/src/webmcp.ts
    title: TypeScript WebMCP browser bridge
tags:
  - research
  - audit
  - remediation
  - drivers
  - mcp
---

# Audit Findings: `d3d5f8b..142cc49`

<!-- truth: start -->
- **Historical Baseline Snapshot (`main @ 142cc49`)**: Records the verified defects across `vertebra.db` [^db-registry], `extensions/sqlspec` [^sqlspec-adapters], `framework/queues` [^queue-base], `embeddings` [^embeddings-pkg], `utils` [^utils-pkg], `server` [^server-fastpath], Rust GCP clients [^rs-db-lib] [^rs-py-lib] [^rs-core-lib], and MCP/WebMCP [^mcp-pkg] [^ts-webmcp] that decomposed into `integration-remediation` Chapters 1–9 and `native-data-platform`.
- **Current Resolution Status on `main @ 077a3fe9`**: Chapters 1 (`364c7cbe`), 2 (`984ce732`), 3 (`80ff8a65`), 7 (`077a3fe9`), 8 (`d2fa5072`), and 9 (`abd33ab6`) plus `db-layout-and-contract` (`21c99f95`) are merged on `main`; remaining driver, SQLSpec, queue, and SQLAlchemy/Django items track in their respective child specs.
<!-- truth: end -->

Baseline: `main @ 142cc49`. It contains PRs #64 (embeddings, GCP ADC), #65 (LoopBackend, C-API stepper, FastPath, shared native pools), #66 (BigQuery/Spanner drivers, SQLSpec/framework adapters, modular driver packages), #69 (dev welcome page), and #70 (Rust text/humanize/uuid primitives).

Each finding names its owning child flow in brackets. Confidence is high unless marked otherwise.

## 1. `vertebra.db` package

### 1.1 Drivers that do not talk to their database [db-driver-foundation]
- `db/oracle/driver.py::SyncOracleDriver.__init__` opens `sqlite3.connect(":memory:")`. Every Oracle query runs against in-memory SQLite. `AsyncOracleDriver` wraps it with a thread offload. `vertebra-db/src/oracle.rs` contains only TTC frame encoders (piggyback session attributes, OSON, VECTOR), with no network I/O.
- `db/mssql/driver.py::SyncMSSQLDriver.__init__` opens `sqlite3.connect(":memory:")`. `vertebra-db/src/mssql.rs` contains TDS encoders only.
- `db/duckdb/driver.py::SyncDuckDBDriver.__init__` silently falls back to `sqlite3.connect(db_path)` when `duckdb` is missing.
- `db/postgres/driver.py::SyncPostgreSQLDriver` and `db/mysql/driver.py::SyncMySQLDriver` return `_in_memory_rows` for `memory://` DSNs and fall through to fake rows on several branches. `db/redis/driver.py` has a `memory://` mode.
- `db/postgres/driver.py` executes through `psycopg_pool.ConnectionPool` (sync). `AsyncPostgreSQLDriver._ensure_async_pool` returns the `asyncpg` *module* rather than a pool, then duck-types `acquire`/`fetch`/`cursor`.
- The Rust Postgres crate (`vertebra-db/src/postgres.rs`) has `encode_parse_frame`, `encode_bind_frame`, `encode_execute_frame`, and `encode_sync_frame`, but no connection or I/O loop.
- This conflicts with the standing rule: no in-memory SQLite or emulator fallbacks in production driver classes.

### 1.2 Pipelining [db-driver-foundation]
- PostgreSQL clients support pipeline mode: psycopg 3 `Connection.pipeline()` / `AsyncConnection.pipeline()`, and sqlspec's psycopg adapter uses it for `execute_stack`.
- python-oracledb 2.4+ supports pipelining: `oracledb.create_pipeline()` with `add_execute`/`add_executemany`/`add_fetchall`/`add_commit`, then `AsyncConnection.run_pipeline(pipeline)`. It needs Thin mode, and the round-trip reduction requires Oracle Database 23ai+. Older servers execute the operations sequentially.
- Vertebra exposes neither. `db/pipeline.py::QueryPipeline` instead compiles steps into one CTE (`jsonb_agg` / `TO_JSON_STRING` envelope), with a hand-written `_rewrite_googlesql_params` tokenizer and `hasattr`-based driver probing in `execute`/`execute_async`.
- `extensions/sqlspec/{bigquery,spanner}.py::*Driver.execute_stack` reuse `QueryPipeline`.
- Pre-remediation pages across `docs/usage/databases.md`, `docs/usage/extensions.md`, `docs/index.md`, and `docs/reference/index.md` (alongside the retired standalone query-pipelines guide removed in `db-engine-primitives`) documented `QueryPipeline` as the pipelining feature.

### 1.3 Engine code inside generic modules [db-driver-foundation]
- `db/cdc.py` combines generic `ChangeEvent`/`ChangeFeed` with Postgres-only `ChangeFeed.build_emit_message_sql` (`pg_logical_emit_message`) and `CDCConfig.replication_slot_name`/`publication_name`/`max_slot_wal_keep_size_mb`, plus the Spanner `SpannerChildPartition`, `SpannerChangeStreamRecord`, and `SpannerChangeStreamSource`.
- `db/channels.py::DatabaseChannelBackend` hardwires Valkey, Redis, Postgres LISTEN/NOTIFY, Oracle TxEventQ, ChangeFeed, Spanner change streams, and sqlspec. It does this through constructor slots and the `from_valkey`/`from_redis`/`from_postgres`/`from_oracle`/`from_changefeed`/`from_spanner_change_stream`/`from_sqlspec` methods. `framework/channels.py::ChannelBus` subclasses it.
- `db/_registry.py::DatabaseRegistry.pool_fingerprint`, `parse_url`, `_build_async_driver`, and `_build_sync_driver` are long `isinstance`/scheme chains over every config type.
- `db/_registry.py::provide_bigquery_driver` and `provide_spanner_driver` are identical apart from a cast. Both read the private `reg._configs`, and `extensions/litestar/__init__.py` re-exports them.
- `db/mcp_tools.py` is GCP-only (`register_gcp_database_tools`, `GCPDatabaseToolPolicy`) and imports `vertebra.agent.core.schema`, `vertebra.agent.runtime`, and `vertebra.framework.security`, so the db layer imports upward.
- `db/pipeline.py::PipelineDialect` and `db/rls.py` hardcode Postgres/GoogleSQL.

### 1.4 Bugs [critical-correctness-fixes]
- `db/_registry.py::DatabaseRegistry.parse_url` sets `dialect="postgresql"` for `spanner+postgresql://`. `SpannerConfig.dialect` is `Literal["googlesql", "spangres"]`, and the canonical scheme is `spanner+spangres://`.
- `db/cdc.py::SpannerChangeStreamSource.build_read_query` emits positional `$1..$4` for Spangres while `poll_partition_sync`/`poll_partition_async` pass named params (`start_ts`, `end_ts`, `token`, `heartbeat_ms`). Medium confidence; confirm with a failing test.
- `db/cdc.py::ChangeFeed._spill_store` is unbounded. `hydrate_notification` swallows every exception. `_next_id` is process-local and not thread-safe. `poll_partition_async` duck-types sync versus async with `asyncio.iscoroutine`.

## 2. SQLSpec, SQLAlchemy / Advanced Alchemy, Django integrations

### 2.1 SQLSpec adapter parity [sqlspec-adapter-parity]
- Upstream resolves extension stores and migrations from module paths. A vertebra config never resolves upstream stores. Filed as https://github.com/litestar-org/sqlspec/issues/876. Sites:
  - `extensions/events/_hints.py::resolve_adapter_name`
  - `extensions/events/_store.py`
  - `extensions/events/_channel.py`
  - `extensions/events/migrations/0001_create_event_queue.py`
  - `extensions/litestar/store.py`
  - `extensions/litestar/migrations/0001_create_session_table.py`
  - `extensions/adk/_config_utils.py` (three sites)
  - Only `adk/migrations/0002_create_mssql_python_memory.py` walks `__mro__`.
- Store classes subclass nothing upstream and have mismatched APIs:
  - `BigQuery{Sync,Async}ADKStore` keys by `session_id` only. It has no app/user state, no `append_event_and_update_state`, and no partitioning, and its DDL differs from upstream `sqlspec.adapters.bigquery.adk.store::BigQueryADKStore`.
  - `BigQuery*ADKMemoryStore.search_memory` selects every row for the user and ranks in Python (`_rank_memory_rows`) despite its docstring claiming VECTOR_SEARCH.
  - `BigQuery{Sync,Async}Store` is not a `litestar.stores.base.Store`.
  - `BigQuery*EventQueueStore.consume` has no lease or claim column, so concurrent consumers get duplicate deliveries.
  - The `Spanner*` ADK/memory/artifact/store/event stores mirror upstream `sqlspec.adapters.spanner.{adk,litestar,events}` with drift. `_farm_fingerprint_shard` uses blake2b client-side while the docstrings claim `FARM_FINGERPRINT`.
  - `BigQueryDataDictionary`/`SpannerDataDictionary` signatures differ from upstream `SyncDataDictionaryBase`/`AsyncDataDictionaryBase`.
- `extensions/sqlspec/adapters.py::BaseSyncDriver`/`BaseAsyncDriver` override `execute`/`select`/`select_one`/..., which bypasses the SQL pipeline, parameter-style conversion, filters, observability, and `SQLResult`. `execute` returns `int`, `**kwargs` is dropped (`_ = (statement_config, kwargs)`), and `data_dictionary` returns `None` for Postgres, MySQL, SQLite, Oracle, MSSQL, and DuckDB.
- About 3,250 of roughly 6,000 lines in `bigquery.py` + `spanner.py` duplicate upstream features, and about 800 more are sync/async copies (for example `resolve_bigquery_config`). The genuinely vertebra-specific parts are the Rust core driver binding, `LeaseBroker`/`StatementCache` injection, ArrowCursor/object-table/ObjectStore export, `update_ddl`/`apply_mutations`/partitioned DML, and the connection shims.
- `adapters.py` holds all six non-GCP engine configs and drivers in the "base" module.
- `extensions/sqlspec/storage.py::build_duckdb_gcs_secret_sql` is engine code in generic storage.

### 2.2 SQLSpec bugs [critical-correctness-fixes unless noted]
- `adapters.py::Base{Sync,Async}Driver.apply_tenant_predicate` wraps the RLS GUC `SET` in `suppress(Exception)`, so tenant isolation silently disappears.
- `bigquery.py::BigQueryConnectionShim.query` discards `job_config`, so upstream stores lose their query parameters. [sqlspec-adapter-parity]
- `BigQueryAsyncConfig.create_connection` builds a new `_CoreSyncBigQueryDriver` on every call and never closes it. [sqlspec-adapter-parity]
- `Base*Driver.execute_stack` merges independent SELECTs into one CTE and returns `len(rows)` for every operation inside `suppress(Exception)`. [db-driver-foundation, removed with QueryPipeline]
- `SyncAdapterConfig.close` closes a registry-shared driver without a refcount. `provide_session` hands the same adapter to every caller. [sqlspec-adapter-parity]
- Spanner KV store:
  - It stores values as `STRING` via `value.decode("utf-8")`.
  - The GoogleSQL DDL `ROW DELETION POLICY (OLDER_THAN(updated_at, INTERVAL 30 DAY))` deletes non-expiring keys, and the Spangres DDL `TTL INTERVAL '0 days' ON updated_at` deletes every row.
  - The DML always uses `@shard_id`, even for Spangres. `SpannerSyncEventQueueStore` has the same pattern. [sqlspec-adapter-parity]
- Spanner async stores call `self._sync.*` inside `async def` (36 sites) and open a second sync driver. [sqlspec-adapter-parity]
- `plugin.py::_inject_sqlspec_pool_defaults` injects SQLAlchemy-only `pool_pre_ping`/`pool_recycle`. [sqlspec-adapter-parity]
- `spanner.py` imports `vertebra.db.bigquery::ObjectRef`. `_cosine_similarity` is duplicated. `bigquery.py` has about 20 private module-level helpers. `BigQuerySyncConfig.__init__` assigns `connection_instance` twice. [sqlspec-adapter-parity]
- `provider.py::SharedArrowBuffer` reimplements `/dev/shm` Arrow IPC in Python (two copies). Its `SharedArrowTable` fallback holds dicts, and segments leak without an unlink. [sqlspec-adapter-parity]

### 2.3 SQLAlchemy / Advanced Alchemy [sqlalchemy-and-django-integration]
- `sqlalchemy/dialects.py` is 1,377 lines. It mixes nine engines with the PEP 249 shim (`DBAPIModule`/`Connection`/`Cursor` plus async variants and the error hierarchy), `SyncEngineAdapter`, and `attach_serverless_disconnect_guard`.
- `BigQueryDialect`, `SpannerDialect`, and `DuckDBDialect` subclass bare `DefaultDialect`. They have no compiler, type compiler, preparer, or reflection: identifiers are double-quoted (GoogleSQL reads those as strings), `String` renders as `VARCHAR`, and Alembic autogenerate gets no reflection.
- `compile_merge_upsert`, `compile_insert_or_update`, `compile_interleave_clause`, and `extract_interleave_options` are called only from tests.
- `alchemy/cache.py::SQLAlchemyCacheBackend` is a roughly 640-line custom region that doubles as a `compiled_cache` MutableMapping. `shm_name` is unused and L1 is a per-process `OrderedDict`. `create_cache_config` passes a URL as the dogpile backend name. It also has a dead `if TYPE_CHECKING: pass` and mixes `TypeVar` with PEP 695.
- `alchemy/broker.py::StatementCache` never reads or writes `_cache`, compiles without a dialect, and its name collides with Rust `StatementCache`.
- `sqlalchemy/cache.py` is a pure re-export shim.
- `alchemy/storage.py` monkeypatches `storages.get = storages.get_backend`.
- `alchemy/plugin.py::normalize_alchemy_url` hardcodes bigquery/bq/spanner/spangres.
- `sqlalchemy/alembic.py` holds eight Impl classes in one module.

### 2.4 Django database backends [sqlalchemy-and-django-integration]
- `extensions/django/db/backends/*/base.py::DatabaseWrapper` subclass only `DatabaseWrapperMixin`, not `BaseDatabaseWrapper`. There are no features/ops/introspection classes and `schema_editor` yields a dict, so none are usable as `ENGINE`.
- `backends/__init__.py::DatabaseWrapperMixin._build_native_config` is a vendor if/elif chain.
- The official Spanner backend is `django-google-spanner` (`ENGINE: "django_spanner"`, googleapis). It supports Django 5.2/6.0 and executes through `google.cloud.spanner_dbapi`. BigQuery has no official Django ORM backend.

## 3. Framework, embeddings, utils, server

### 3.1 Queues [queues-tasks-and-channels]
- `framework/queues/postgres.py::PostgreSQLQueueBackend._build_executor` and `oracle.py::OracleQueueBackend._build_executor` always return in-memory executors.
- `spanner.py::_DriverSpannerQueueExecutor` sends every execute/fetch to `self._fallback`. The real driver gets only DDL (with exceptions swallowed) and a discarded `run_in_transaction`.
- `redis.py` silently falls back to `_InMemoryRedisStreamClient` when there is no hostname.
- `base.py::QueueService._build_backend` switches on URL scheme with hardcoded import strings.
- There are two cron parsers: `base.py::_cron_matches`/`_cron_matches_field` and `scheduler.py::CronExpression`.
- `spanner.py::SpannerQueueBackend._receipt_index` is unbounded.
- The knowledge chapter `domains/framework/distributed-queues-and-email.md` claims `FOR UPDATE SKIP LOCKED` and DBMS_AQ implementations that do not exist.
- `extensions/litestar/queues/backend.py::SHMQueueBackend` extends litestar-queues `InMemoryQueueBackend`.
- litestar-queues' SQLSpec backend resolves adapters from module paths. Filed as https://github.com/cofin/litestar-queues/issues/186.
- `extensions/django/queues.py` exists. Django 6.0 ships `django.tasks` (`BaseTaskBackend`) without a worker.

### 3.2 Stores and channels [critical-correctness-fixes for the bugs, queues-tasks-and-channels for structure]
- `framework/stores.py::KeyValueStore.delete_all` and `extensions/litestar/store.py::MemoryStore.delete_all` call `driver.flushdb()`.
- Expiries under 1 second send `ex=0`, which Redis rejects.
- The local `_entries` dict is unbounded and only expires on read.
- With Redis, local copies go stale across processes; only Valkey gets invalidation.
- `expires_in` returns -1 where Litestar expects `None`.
- About 100 lines are copy-pasted between `KeyValueStore` and `MemoryStore`.
- `extensions/litestar/channels.py::swap_channels_backend` monkeypatches `plugin._backend` and `stores._stores`.
- `VertebraConfig` defaults `swap_memory_channels=True` and `swap_memory_response_cache=True`.

### 3.3 Embeddings [critical-correctness-fixes for bugs, db-driver-foundation for relocation, runtime-structure-and-throughput for transport]
- `embeddings/_spanner.py::SpannerEmbedder._embed_batch_impl` uses `except Exception: rows = []`, then substitutes `_deterministic_vector` (SHA-256 noise). Rows are matched to texts by index with no length check.
- `embeddings/_local.py::FastEmbedder.__init__`/`GemmaEmbedder.__init__` leave the backend unresolved on free-threaded builds. `EmbeddingBroker` then uses `_deterministic_hash_backend`, and `_broker.py::_run_sidecar_worker_main` runs only the hash backend. `FastEmbedder` also discards the `fe_instance` it builds.
- `_broker.py::_drain_pending_once`/`submit_batch` run sync ONNX inference on the event loop. They also sleep `asyncio.sleep(0.002)` per chunk, use `list.pop(0)`, and `assert proc.stdin`. The per-request `/vtb-qvec-*` segments need an unlink check.
- `_google.py::VertexEmbedder` POSTs `application/grpc+proto` over urllib HTTP/1.1 and reads `grpc-status` from headers. It also targets `GenerativeService` on `*-aiplatform.googleapis.com`. `_pending`/`_flush_task` are shared across loops, and `Embedder._run_sync` creates a new thread pool per call. Medium-high confidence; verify.
- `GeminiEmbedder.embed_multimodal` embeds a text descriptor instead of the media.
- `OracleEmbedder._embed_batch_impl` makes one round-trip per text, and `rows[0]` raises `IndexError` on empty results.
- `Embedder.__new__` runs `VertexEmbedder.__init__` twice.
- `_bigquery.py`/`_spanner.py` `build_*_sql` interpolate table, column, and model identifiers with f-strings. `vector_search_sync` with an async driver leaves an un-awaited coroutine.
- `embeddings/__init__.py` eagerly re-exports the BigQuery, Spanner, AlloyDB, and Oracle embedders. `_google.py` holds `OracleEmbedder`/`AlloyDbEmbedder`.

### 3.4 Utils [runtime-structure-and-throughput]
- `vertebra.utils.*` functions are public helpers for users, so a missing in-repo importer is not a reason to delete them.
- `utils/text.py` (`_py_*` fallbacks and `_LATIN_FOLD_MAP`) and `utils/humanize.py` (`_py_*`) resolve through `_get_native_fn(name, fallback)`, but `vertebra._vertebra` is mandatory. The fallbacks drift: Python `humanize_bytes` accepts floats and negatives.
- `humanize.py` imports the private `text._get_native_fn` and `text._split_identifier_words`.
- `utils/uuids.py::uuid4`/`uuid7` carry a test monkeypatch hook (`_load_uuid_utils is not _DEFAULT_LOAD_UUID_UTILS`) and a four-tier chain. The FFI `UUID(int=...)` path needs a benchmark against stdlib.
- `module_loader.py::import_optional` keeps a `_module_cache` duplicating `sys.modules` and never caches misses.
- The per-framework fastpaths duplicate `_is_pydantic_model_type`/`_is_msgspec_struct_type` that `utils.type_guards` already provides.

### 3.5 Server [critical-correctness-fixes for bugs, runtime-structure-and-throughput for structure]
- `server/_loops.py::EventLoopPool.stop`:
  - It cancels tasks and stops the loop in one callback, so cancellations never run.
  - It skips `shutdown_asyncgens`.
  - It closes a loop whose thread may still be alive.
  - It ignores the result of `ready.wait(5.0)`.
- `server/worker.py::_AsgiMountRunner.__call__` hardcodes a 10 s batch timeout, which breaks SSE and long handlers. `fut.cancel()` does not cancel the asyncio task.
- `server/_fastpath.py::extract_scope_body_fast` has no body-size cap. `wrap_asgi_fastpath` calls `body_bytes_fn()` twice. `serialize_fastpath_special_result` matches on `type(result).__name__` and copies Arrow buffers.
- `storage/_streaming.py::to_fastpath_tuple` copies with `bytes(view)` through a blocking `store.get_view` and returns no 416 for invalid ranges.
- `_AsgiSend.send_raw_response` converts bytes headers to str on the hot path.
- `_loops.py::resolve_loop_backend` disables rloop when any unrelated module is quarantined. It has five near-identical branches and repeats the "bucket and call_soon_threadsafe" block four times.
- Per-framework duplication totals about 3,000 lines across `extensions/{litestar,fastapi,starlette,flask,django}/fastpath.py`:
  - five `CompiledRoute` structs;
  - two copies each of `_step_coro_sync`, `_coerce_param`, the scalar converters, and the HTTP-exception serializers;
  - three copies of `_has_custom_exception_handlers`.
- `attach_framework_fastpath` sniffs frameworks by class/module name.
- The fastpath attribute aliases (`_vertebra_fastpath`, `__vertebra_fastpath__`, `fastpath`, `state["vertebra_fastpath_bypass"]`) are compatibility aliasing.
- `litestar/fastpath.py::FastPath.__init__` overloads `router_table`.
- `framework/_app.py::add_route` evicts the welcome route for any method on `/`.
- `_welcome.py` embeds about 450 lines of HTML/CSS in Python.

## 4. Rust GCP drivers [gcp-wire-protocol-correctness unless noted]

### 4.1 Wire-format bugs
Field numbers were checked against the official protos. The tests mock the same wrong numbers in `vertebra-db/tests/bigquery_wire_tests.rs` and `spanner_wire_tests.rs`.
- **BigQuery Storage Read.**
  - `encode_create_read_session_request` writes `data_format` on field 1, `table` on 2, and read options on 5. Correct: ReadSession `data_format=3`, `table=6`, `read_options=8`.
  - `decode_read_session_response` reads the schema from 4 and the streams from 6. Correct: `arrow_schema=5`, `streams=10`.
  - `decode_read_rows_response` reads the batch from 3 and the schema from 7. Correct: `arrow_record_batch=4`, `arrow_schema=8`.
- **BigQuery Storage Write.**
  - `encode_append_rows_request` puts ArrowData on 4. Correct: `arrow_rows=5`, since `proto_rows=4`.
  - `decode_append_rows_response` parses field 5, which is the echoed `write_stream` string, as a RowError.
- **Spanner.**
  - `get_or_create_multiplexed_session` writes `creator_role` on 4. Correct: `Session.creator_role=5`.
  - `encode_execute_sql_request` writes RequestOptions on 10. Correct: `request_options=11`, since `query_options=10`.
  - `decode_transaction_message` reads `precommit_token` from 8. Correct: `Transaction.precommit_token=3`.
  - `commit_read_write_tx` writes `precommit_token` on 12. Correct: `CommitRequest.precommit_token=9`.

### 4.2 False success and safety
- `commit_read_write_tx` returns `Ok` with `commit_timestamp=None` after five MultiplexedSessionRetry responses.
- `update_database_ddl` polls 120 × 10 ms and then returns `Ok(op_name)` while `done=false`.
- `append_arrow_stream` never checks the response count or the returned offsets.
- `inspect_arrow_message_flatbuffer`/`rb_vtable` turn a negative offset into a huge `usize` index and panic on input from `append_arrow`.
- `vertebra-py/src/completion.rs::spawn_rust_db_task` does not catch panics, so the Python future never resolves. It also has no cancellation, so DML still commits after the awaitable is cancelled.
- `rewrite_spangres_placeholders` copies bytes as `char` (non-ASCII corruption) and ignores backslash escapes, backticks, and triple-quoted strings.
- `execute_partitioned_arrow` emits JSON inside a hand-made 24-byte header, not Arrow.
- `split_arrow_ipc_stream` slices arbitrary bytes when the continuation marker is missing, concatenates batches, and treats DictionaryBatch messages as record batches.

### 4.3 Medium-confidence correctness
- `grpc.rs::server_streaming_from_lpm_frame` resends after any `send_request` error, including Commit, DML, and BatchWrite.
- `execute_streaming_sql` resumes with the original selector, so an inline `Begin` can start twice.
- `execute_in_read_write_tx` lacks the documented BeginTransaction fallback.
- `bigquery.rs::check_rest_error` treats `errors[]` warnings as fatal.
- `build_get_query_results_url` does not URL-encode `pageToken`/`location`.
- `grpc.rs::bidi_streaming` breaks silently on an encode error.
- `gcp_auth.rs::fetch_token_from_gcloud_cli` runs a blocking `Command::output()` in async code.
- `GcpTokenCache::new` uses `.no_proxy()`.
- Exception mapping:
  - Sync bindings raise `RuntimeError` via `e.to_string()`, while async bindings may raise `ValueError`.
  - No gRPC code reaches Python.
  - Python matches substrings (`db/spanner/driver.py`, `db/spanner/core.py`, `db/bigquery/core.py`), and "permission_denied" never matches.
- Python `SpannerTransaction.execute` runs each statement as its own read-write transaction.

### 4.4 Layering and duplication [runtime-structure-and-throughput]
- **Engine specifics in generic modules.**
  - `grpc.rs::GrpcRequestMetadata::spanner_request_id` is Spanner-specific.
  - `GrpcPoolConfig.initial_*_window_size` is overridden by `.adaptive_window(true)`.
  - `GcpTokenCache::from_options` defaults to a GCS scope, and `from_gcs_options` is an alias.
  - `completion.rs` naming (`block_on_db`, `DbCompletionRing`) is DB-specific although `gcp_auth.rs` also uses it.
- **Arrow IPC helpers live in engine files:** `spanner.rs::encode_encapsulated_arrow_ipc_frame` and `bigquery.rs::{split_arrow_ipc_stream, try_parse_encapsulated_arrow_stream, inspect_arrow_message_flatbuffer}`.
- **Domain logic in bindings.** `vertebra-py/src/spanner.rs::json_cell_to_protobuf_value` duplicates `vertebra-db spanner.rs::json_to_protobuf_value`.
- **Exports and features.** `lib.rs` glob re-exports, `duckdb.rs` re-exports `arrow_shm` again, and the `duckdb = []` feature is empty.
- **Duplicated helpers.**
  - `google.rpc.Status` decoding: three copies.
  - Varint readers: three (`ProtoReader`, `dialer.rs`, `vertebra-core/src/vector.rs`).
  - `percent_decode_grpc_message`: two (`vertebra-db`, `vertebra-http`).
  - LPM framing: three (`encode_lpm_frame`, `encode_grpc_frame`, `pack_grpc_lpm_frame`).
  - Token/metadata assembly: four.
  - Retry loops: three, none with backoff.
  - Duration/Timestamp encoding: duplicated.
  - Error style is mixed: hand-written `Display` vs `thiserror`.
- **Large files.** `spanner.rs` (3,324 lines) and `bigquery.rs` (2,871 lines) should split into submodules.

### 4.5 Performance [runtime-structure-and-throughput]
- `append_arrow_stream` copies the payload about five times.
- `read_table_to_shm_arrow_with_compression` buffers every stream before copying into SHM.
- `GrpcFrameStream::push_chunk` copies every DATA frame.
- `GrpcChannelPool::acquire_sender` holds a Tokio mutex while dialling.
- Spanner per-RPC `format!`/clones.
- `PartialResultSetAssembler::push_frame` uses `values.remove(0)`.
- Params cross the boundary as JSON strings.

### 4.6 Stubs and basics
- `_vertebra.pyi::RustDbTaskAwaitable.__new__` has no Rust `#[new]`.
- There are two `GcpTokenCache` classes with separate caches (`_vertebra`, `_storage`).
- Dead code: `vertebra-py/src/spanner.rs` `let _ = SpannerTypeCode::Unspecified;`, `generate_request_uuid_v4`, the test-only public `decode_append_rows_request`, and the ignored `max_commit_delay_ms`.
- `id.rs::fill_os_random` uses libc `getrandom` with a predictable time fallback; the `getrandom` crate is already locked. The thread-local pool is not fork-safe.
- Hand-written percent decoding duplicates `percent-encoding`, and URLs are built by string concatenation although `url` is available.
- ProtoReader bounds, completion queue arming, and free-threaded `Arc` sharing are correct. Two `unsafe` blocks in `arrow_shm.rs` lack SAFETY comments, and `allocate_shm_region` silently falls back to anonymous memory.

## 5. MCP and WebMCP [mcp-webmcp-core unless noted]

### 5.1 Architecture
- One `agent/core/schema.py::Tool` class exists, but discovery is repeated four times: `agent/mcp/registry.py::MCPRegistry.discover_app_routes`, `agent/plugin.py::AgentPlugin._inspect_item` (which ignores `@mcp_tool` and sets `input_schema={}`), `SkillController`, and `db/mcp_tools.py`.
- `build_webmcp_manifest` reads `fn._mcp_tool_meta` instead of `Tool`.
- `/.well-known/mcp.json` has three different shapes.
- The Flask and Django dispatchers are identical (`_dispatch_jsonrpc`), report `2024-11-05`, and shadow `MCPConfig`. Starlette/FastAPI mount a manifest only, with no `/mcp`.
- `AgentPlugin` calls `handle_payload` directly and bypasses `MCPHttpHandler`.

### 5.2 Security bugs [critical-correctness-fixes]
- **`Mcp-Name` bypass.** `routes.py::MCPHttpHandler.handle_request` skips the check when outer `params.name` is absent, and `jsonrpc.py::_invoke_handler` passes nested params through.
- **Confirmation never enforced.** `requires_confirmation` is not checked in `service.py::_handle_tools_call` or in `webmcp.ts`.
- **Browser exposure on by default.** `WebMCPConfig.enabled=True`, and `meta.get("webmcp", True)` exposes untagged tools, including the GCP DB tools.
- **Broken `SkillController` guards.** The rebuilt `Tool(_guarded_fn)` drops `self`, `context` is never injected, and `@mcp_tool` methods skip guards.
- **SQL injection.** `db/mcp_tools.py` interpolates `FROM {table_name}`. `is_mutating_sql` checks only the leading verb, so `SELECT 1; DELETE`, `EXECUTE IMMEDIATE`, `CALL`, `GRANT`, `EXPORT DATA`, and `LOAD DATA` pass.
- **Open Origin.** `allowed_origins=("*",)` by default, and the AgentPlugin endpoints skip validation.
- **`asyncio.run` in request handlers.** Flask and Django call it.

### 5.3 Conformance (MCP 2026-07-28)
- `service.py` still serves `initialize`, `ping`, `logging/setLevel`, `resources/subscribe|unsubscribe`, `tasks/result`, and `tasks/list`, and advertises `tasks` as a core capability.
- `resultType`, `ttlMs`, and `cacheScope` are missing.
- **HTTP status codes.** Header and version errors return 200 instead of 400, unknown methods return 200 instead of 404 (-32601), GET returns discover JSON instead of 405, `Mcp-Session-Id` is echoed, and notifications return 204 instead of 202.
- JSON-RPC batches are accepted.
- The `server/discover` shape is incomplete.
- Tool descriptors lack title, outputSchema, annotations, and icons. `BlobContent` uses `type="blob"`.
- The TS client sends no `Accept: application/json, text/event-stream` and no `_meta`, and cannot parse SSE.

### 5.4 WebMCP (CG draft)
- The API is `document.modelContext.registerTool(tool, {signal, exposedTo})`, with `getTools` and `executeTool`. `provideContext`/`unregisterTool` do not exist. Medium confidence: `navigator.modelContext` was deprecated during the Chrome origin trial.
- `webmcp.ts` probes only `navigator.modelContext` and falls back to `provideContext`. It ignores the abort `signal`, forwards MCP annotations unmapped (WebMCP uses `readOnlyHint`/`untrustedContentHint`/`consequentialHint`), drops `requiresConfirmation`/`title`/`outputSchema`, and registers sequentially without a catch, so one bad tool aborts the rest. Re-registration throws, and no dispose handle is returned.

### 5.5 Other
- **Schema derivation.** `SchemaBuilder` reads raw string annotations under PEP 563, which produces `{}` schemas. `X | None` maps to an object. `_convert_argument` uses `eval` and skips validation silently.
- **Litestar overlap.** litestar-mcp already implements stateless 2026-07-28 and has a conformance runner. Installing both plugins double-registers `opt["mcp_tool"]` and collides on `/mcp`.
- **Medium-confidence items.**
  - `asyncio.create_task` is called without keeping a reference.
  - Litestar `/mcp` returns 201.
  - `combined_etag` gets double-quoted.
  - The `*_enabled` flags are unused.
  - The Starlette manifest advertises an unmounted `/mcp`.
  - `encodeMcpHeaderValue` has edge cases.
  - Exception text leaks into responses.
- **Performance.** `inspect.signature` runs per call, the body is decoded up to three times, Struct outputs are encoded twice, and `tools/list` is rebuilt every request.
- **Stale docs.** The knowledge chapter `domains/agent/mcp-and-a2a-protocol-harness.md` describes `McpService`/`McpController`/SSE components that do not exist. `build/_import_map.py::_CLIENT_SUBPATHS` is a third copy of the package exports.

[^db-registry]: `src/py/vertebra/db/registry.py` — Database driver registry and adapter boundary.
[^sqlspec-adapters]: `src/py/vertebra/extensions/sqlspec/adapters.py` — SQLSpec adapter drivers and store bindings.
[^queue-base]: `src/py/vertebra/framework/queues/base.py` — First-party QueueService and backend selection.
[^embeddings-pkg]: `src/py/vertebra/embeddings/__init__.py` — Embeddings package exports and transports.
[^mcp-pkg]: `src/py/vertebra/agent/mcp/__init__.py` — MCP 2026-07-28 protocol package.
[^utils-pkg]: `src/py/vertebra/utils/__init__.py` — Public Rust-backed utility helpers.
[^server-fastpath]: `src/py/vertebra/server/_fastpath_core.py` — Consolidated framework fastpath provider.
[^rs-db-lib]: `src/rs/vertebra-db/src/lib.rs` — Shared Rust database crate.
[^rs-py-lib]: `src/rs/vertebra-py/src/lib.rs` — Core PyO3 extension bindings.
[^rs-core-lib]: `src/rs/vertebra-core/src/lib.rs` — Core Rust runtime, SHM rings, and GCP auth.
[^ts-webmcp]: `src/ts/vertebra-client/src/webmcp.ts` — TypeScript WebMCP browser bridge.
