# Bundle Activity Log

## [2026-11-16T06:50:00Z] archive | strict-typing-hygiene & integration-remediation

- Archived completed specification `strict-typing-hygiene` (`t01`–`t04`, `4/4` tasks at `f34c45c0`) and completed Master PRD `integration-remediation` (`t01`–`t15`, `15/15` child flows complete across framework, ORM, SQLSpec, native caching, distributed queues, Spanner interactive ReadWrite transactions, MCP/WebMCP, HTTP/3, and strict typing) into permanent OKF chapters (`tech-stack.md`, `polyglot-test-matrix.md`, `workflow.md`), promoted `audit-findings_20261103.md` and `google-cloud-rust_20261103.md` into `.agents/bundles/research/`, contracted `.agents/bundles/specs/` to its lazy empty state, and synchronized `index.md` and `research/index.md`.

## [2026-11-16T06:49:00Z] finish | strict-typing-hygiene

- Completed `strict-typing-hygiene` (`t01`–`t04`, `4/4` tasks) at `f34c45c0`: pruned `40` redundant PEP 561 and `django-stubs` `ignore_missing_imports` overrides from `pyproject.toml`, replaced private `django.db.backends.sqlite3._functions` and mid-file Django `DatabaseWrapper` imports with `import_optional_attr` and `ensure_module`, added `ServerRunner` UTF-8/non-UTF-8 binary stderr and 16-row `test-db-conformance` regression coverage in `test_bench_infrastructure.py`, defined structural wire-client, SQL-pushdown, and token-cache `Protocol`s across Spanner, BigQuery, Postgres, Oracle, GCP auth, and MCP bridges, enrolled `src/py/tests/unit` and `src/py/tests/integration` (`756` total Python files) under `mypy` in `pyproject.toml` and `Makefile`, synchronized `docs/reference/` and OKF chapters (`tech-stack.md`, `polyglot-test-matrix.md`, `workflow.md`), and verified `mypy`, `mypy.stubtest` (13 modules), `ruff`, `make docs`, `review-check`, and `4,882` unit tests cleanly.

## [2026-11-16T06:00:00Z] archive | spanner-queue-and-rw-transactions

- Archived completed specification `spanner-queue-and-rw-transactions` (`t01`–`t04`, `4/4` tasks at `d8fb3c96`) into permanent OKF knowledge chapters (`gcp-bigquery-and-spanner-drivers.md`, `distributed-queues-and-email.md`), updated Master PRD `integration-remediation` (`t14`), final child flow `strict-typing-hygiene`, and bundle indexes (`index.md`, `specs/index.md`), and removed `.agents/bundles/specs/spanner-queue-and-rw-transactions/`.

## [2026-11-16T05:59:30Z] finish | spanner-queue-and-rw-transactions

- Completed `spanner-queue-and-rw-transactions` (`t01`–`t04`, `4/4` tasks) at `d8fb3c96`: built stateful `SpannerReadWriteTx` (`src/rs/vertebra-spanner/src/rw_tx.rs`) and `@final SyncSpannerReadWriteTransaction`/`AsyncSpannerReadWriteTransaction` (`vertebra._spanner`) managing `TransactionSelector::Id`, monotonic `seqno`, leader routing, highest-`seq_num` `MultiplexedSessionPrecommitToken`, `Commit` (`MultiplexedSessionRetry` replay), `Rollback`, and `SpannerAbortedError.retry_delay_ms`; bound `SyncSpannerTransaction`, `AsyncSpannerTransaction`, `run_in_transaction` (`SpannerAbortedError` retry with `previous_transaction_id`), and single-attempt `transaction()` to interactive handles; added dialect-aware Spangres parameter encoding in `SpannerCore` and implemented `SyncSpannerQueueStore`/`AsyncSpannerQueueStore` (`vertebra.db.adapters.spanner.queue`) plus auto-wired `SpannerQueueBackend` with stateless `spn:{shard_id}:{task_id}:{attempt}` receipt tokens and conditional non-stealing `acquire_lease`; verified `29` Rust tests, `mypy.stubtest`, `mypy`, `ruff`, docstring scan, `review-check`, and `334` Python unit tests cleanly.

## [2026-11-16T05:33:00Z] archive | queues-tasks-and-channels

- Archived completed specification `queues-tasks-and-channels` (`ch01`–`ch03`, `12/12` tasks at `1094ca5b`) into permanent OKF knowledge chapters (`distributed-queues-and-email.md`, `litestar.md`, `server-runtime-limitations.md`), updated Master PRD `integration-remediation` (`t13`), downstream specs (`spanner-queue-and-rw-transactions`, `strict-typing-hygiene`), and bundle indexes (`index.md`, `specs/index.md`), and removed `.agents/bundles/specs/queues-tasks-and-channels/`.

## [2026-11-16T05:32:00Z] finish | queues-tasks-and-channels

- Completed `queues-tasks-and-channels` (`ch01`–`ch03`, `12/12` tasks) at `1094ca5b`: added `@runtime_checkable SyncQueueStore` and `AsyncQueueStore` (`vertebra.db.driver.capabilities`), `QueueBackendSettings`, `SyncPostgresQueueStore`/`AsyncPostgresQueueStore` (`FOR UPDATE SKIP LOCKED`), `SyncOracleQueueStore`/`AsyncOracleQueueStore`, and standalone `ValkeyQueueBackend` (`vertebra._valkey`) and `RedisQueueBackend` (`vertebra._redis`); wired scoped Dishka DI and `CronScheduler.register` into `WorkerRunner`, `StoreQueueBackend` and `SHMQueueBackend` for `litestar-queues>=0.14.0`, and Django 6.0+ `VertebraTaskBackend` plus `vertebra_worker` management command; routed Litestar `ChannelsPlugin`, Starlette, FastAPI, Flask, and Django channel brokers through `SyncNotificationSource`/`AsyncNotificationSource` and cut `ValkeyStore`/`RedisStore` over to native clients; verified `551 passed, 0 failed` unit tests, `mypy`, `ruff`, docstring scan, and `review-check`.

## [2026-11-16T04:57:00Z] archive | sqlspec-adapter-parity

- Archived completed specification `sqlspec-adapter-parity` (`ch01`–`ch04`, `15/15` tasks at `4fad78c5`) into permanent OKF knowledge and product chapters (`sqlspec-alchemy-and-dialects.md`, `database-driver-limitations.md`, `postgres-limitations.md`, `query-pipeline-cdc-and-tenancy.md`, `pyo3-and-python-runtime.md`, `product.md`, `product-guidelines.md`), updated Master PRD `integration-remediation` (`t12`), downstream specs (`queues-tasks-and-channels`, `strict-typing-hygiene`), and bundle indexes (`index.md`, `specs/index.md`), and removed `.agents/bundles/specs/sqlspec-adapter-parity/`.

## [2026-11-16T04:56:30Z] finish | sqlspec-adapter-parity

- Completed `sqlspec-adapter-parity` (`ch01`–`ch04`, `15/15` tasks) at `4fad78c5`: bumped `sqlspec>=0.71.1` and added `adk = ["google-adk>=1.0.0"]`, implemented `query_with_columns`, `ArtifactReference` (`vertebra.storage`), and template-method `BaseSyncDriver`/`BaseAsyncDriver` (`extensions/sqlspec/_driver.py`), replaced monolithic `{adapters,bigquery,spanner}.py` with 8 self-contained `extensions/sqlspec/adapters/{postgres,sqlite,mysql,oracle,mssql,duckdb,bigquery,spanner}` packages (`adapter_name = "vertebra_<engine>"`, explicit `adk_session_store_class`/`adk_memory_store_class`/`events_store_class`/`litestar_store_class`, `<Engine>{Sync,Async}DataDictionary`, `<Engine>{Sync,Async}ADKStore`/`ADKMemoryStore`/`ADKArtifactStore`, `<Engine>EventQueueStore`, and `<Engine>LitestarStore` with zero `_compat.py` monkeypatching), upgraded `ObjectStoreBackend(ObjectStoreBase)` and `ChangeFeedSyncEventBackend`/`ChangeFeedAsyncEventBackend` checkpointing/spilling, and verified `1,326` SQLSpec/storage/driver unit tests, `mypy`, `ruff`, docstring scan, and `review-check`.

## [2026-11-16T04:22:00Z] archive | native-valkey-driver & native-data-platform

- Archived completed specification `native-valkey-driver` (`ch01`–`ch04`, `16/16` tasks at `60ccf36f`) and completed Master PRD `native-data-platform` (`t01`–`t15`, `15/15` child flows complete across standalone PyO3 wheels for PostgreSQL, MySQL, Oracle 26ai, SQL Server, SQLite, DuckDB, BigQuery, Spanner, Redis, Valkey, Object Storage, and shared-memory `BrokerEngineCapsule` pools) into permanent OKF knowledge chapters (`valkey-limitations.md`, `database-drivers-and-dialers.md`, `distributed-queues-and-email.md`, `workflow.md`, `product.md`), updated Master PRD `integration-remediation`, `queues-tasks-and-channels`, and bundle indexes (`index.md`, `specs/index.md`), and removed `.agents/bundles/specs/native-valkey-driver/` and `.agents/bundles/specs/native-data-platform/`.

## [2026-11-16T04:21:30Z] finish | native-valkey-driver

- Completed `native-valkey-driver` (`ch01`–`ch04`, `16/16` tasks) at `60ccf36f`: built standalone pure-Rust `src/rs/vertebra-valkey` (`vertebra._valkey`, `vertebra-core[valkey]`, `gil_used = false`) for Valkey `8.0+`/`9.0` with Valkey 9.0 hash-field expiration (`hexpire`, `hpexpire`, `httl`, `hpttl`, `hpersist`), `CLIENT CAPA redirect`, `CLIENT TRACKING` L1 write-through and push invalidations, `CLUSTER SHARDS` (`CLUSTER SLOTS` fallback) replica reads, Sentinel primary discovery, Google Cloud Memorystore for Valkey IAM token refresh, Streams (`XREADGROUP`/`XAUTOCLAIM`), and dedicated `SyncValkeyListener`/`AsyncValkeyListener` pub/sub sockets (`SUBSCRIBE`/`PSUBSCRIBE`/`SSUBSCRIBE`), cut `adapters/valkey/` (`SyncValkeyDriver`, `AsyncValkeyDriver`, `SyncValkeyNotificationSource`, `AsyncValkeyNotificationSource`) and `ValkeyQueueBackend` over to `vertebra._valkey` with zero `adapters/redis/` imports, removed legacy `vertebra_db::resp3` and `_vertebra` RESP3 helpers, added `valkey` conformance and wheel-isolation gates, and verified `93` Rust tests, `mypy.stubtest`, `mypy`, `ruff`, `review-check`, and `534` Python tests cleanly.

## [2026-11-16T02:53:00Z] archive | sqlalchemy-and-django-integration

- Archived completed specification `sqlalchemy-and-django-integration` (`ch01`–`ch03`, `12/12` tasks at `1f0716d1`) into permanent OKF knowledge chapters (`sqlspec-alchemy-and-dialects.md`, `litestar.md`, `database-driver-limitations.md`, `adr-002-two-tier-shm-database-and-arrow-data-plane.md`), updated Master PRD `integration-remediation` (`t11`), downstream specs (`sqlspec-adapter-parity`, `queues-tasks-and-channels`, `strict-typing-hygiene`), and bundle indexes (`index.md`, `specs/index.md`), and removed `.agents/bundles/specs/sqlalchemy-and-django-integration/`.

## [2026-11-16T02:52:00Z] finish | sqlalchemy-and-django-integration

- Completed `sqlalchemy-and-django-integration` (`ch01`–`ch03`, `12/12` tasks) at `1f0716d1`: hardened PEP 249 `SyncDBAPIModule`/`AsyncDBAPIModule` (`vertebra.db.dbapi`) with zero `sqlite3` imports and PEP 249 exception translation, split monolithic SQLAlchemy dialects and Alembic modules into `extensions/sqlalchemy/dialects/{postgresql,sqlite,mysql,oracle,mssql,duckdb,bigquery,spanner}.py` (36 `+vertebra`/`+vtb` entry points) and `extensions/sqlalchemy/alembic/{impl,ops,hooks}.py`, implemented 64-byte cache-line-aligned seqlock `ShmCache` + `ShmCacheStats` in `vertebra-core`/`vertebra-py` (`vertebra._vertebra`), delivered Advanced Alchemy `SyncCacheBackend`/`AsyncCacheBackend` (`shm://` with SHM ring-buffer L1 invalidation, `memory://`, `valkey://`, `redis://`, `s3://`/`gs://`/`az://`/`r2://`/`oci://`/`file://` `VTBSTORE\x01`, `CacheConfig(region_factory=...)`, `AsyncCacheManager`, and optional `dogpile.cache` plugins), rebased Django 6.0+ `DatabaseWrapper` backends (`postgresql`, `sqlite3`, `mysql`, `oracle`, `spanner` via `django-google-spanner`) and `CacheBackend(BaseCache)` while removing unsupported `bigquery`/`duckdb`/`mssql` stubs, fixed dynamic `VTBARROW` computed-column type inference in `vertebra-sqlite`, and verified `527 passed, 0 failed` unit tests, `stubtest`, `mypy`, `ruff`, docstring scan, and `review-check`.

## [2026-11-16T02:25:00Z] archive | native-redis-driver

- Archived completed specification `native-redis-driver` (`ch01`–`ch04`, `16/16` tasks at `58e8db93`) into permanent OKF knowledge chapters (`redis-limitations.md`, `database-drivers-and-dialers.md`, `distributed-queues-and-email.md`, `workflow.md`, `product.md`), updated Master PRD `native-data-platform` (`t14`), downstream specs, and bundle indexes (`index.md`, `specs/index.md`), and removed `.agents/bundles/specs/native-redis-driver/`.

## [2026-11-16T02:23:30Z] finish | native-redis-driver

- Completed `native-redis-driver` (`ch01`–`ch04`, `16/16` tasks) at `58e8db93`: built standalone `src/rs/vertebra-redis` (`vertebra._redis`, `vertebra-core[redis]`, `gil_used = false`) with RESP2/RESP3 handshakes, atomic/non-atomic pipelines, `EVALSHA`/`EVAL` script execution, `CLIENT TRACKING` local cache invalidations, Sentinel primary discovery, Cluster `CRC16-XMODEM` slot routing, dedicated `SyncRedisListener`/`AsyncRedisListener` pub/sub sockets, and `XREADGROUP`/`XAUTOCLAIM` Streams, decoupled `adapters/valkey/` and `framework/queues/valkey.py`, cut `SyncRedisDriver`/`AsyncRedisDriver` and `RedisQueueBackend` over to `vertebra._redis`, split `NotificationSource` into `@runtime_checkable SyncNotificationSource` and `AsyncNotificationSource` across `capabilities.py`, `vertebra._postgres` (`SyncPostgresListener`), `vertebra._oracle` (`SyncOracleListener`), and `adapters/redis/notify.py`, added `redis` conformance gating, and verified `stubtest`, `mypy`, `ruff`, `review-check`, and `312` Python tests cleanly.

## [2026-11-16T01:00:00Z] archive | native-sqlite-driver

- Archived completed specification `native-sqlite-driver` (`ch01`–`ch03`, `12/12` tasks at `49fb8150`) into permanent OKF knowledge chapters (`sqlite-limitations.md`, `database-drivers-and-dialers.md`, `workflow.md`), updated Master PRD `native-data-platform`, downstream specs, and bundle indexes (`index.md`, `specs/index.md`), and removed `.agents/bundles/specs/native-sqlite-driver/`.

## [2026-11-16T00:59:30Z] finish | native-sqlite-driver

- Completed `native-sqlite-driver` (`ch01`–`ch03`, `12/12` tasks) at `49fb8150`: built standalone `src/rs/vertebra-sqlite` (`vertebra._sqlite`, `vertebra-core[sqlite]`, `gil_used = false`) over bundled `rusqlite 0.41.1` (`SQLite 3.45+`), implemented 1-writer + `max_readers` WAL worker threads, SQLite `JSONB`, `InterruptHandle` `< 50ms` cancellation, online `backup_to`/`restore_from`, direct `VTBARROW` `ShmCapsule` `ArrowCursor` + `load_from_arrow`, and `broker_engine_capsule()` (`DIALECT_TAG_SQLITE = 3`), removed legacy `_vertebra.sqlite_initialization_pragmas`, added delayed-commit `SyncSQLitePipeline`/`AsyncSQLitePipeline` with nested savepoints, cut `SyncSQLiteDriver` and `AsyncSQLiteDriver` over to `vertebra._sqlite` with zero `import sqlite3`, and verified `16/16` Rust tests, `mypy.stubtest`, `mypy`, `ruff`, `review-check`, and `1783` Python tests cleanly.

## [2026-11-15T23:30:00Z] sync | active specs

- Verified both Master PRDs and all eight executable specs (`native-sqlite-driver`, `native-redis-driver`, `native-valkey-driver`, `sqlalchemy-and-django-integration`, `sqlspec-adapter-parity`, `queues-tasks-and-channels`, `spanner-queue-and-rw-transactions`, `strict-typing-hygiene`) against `main @ 473e1f2e`, corrected baseline facts, source paths, and squash-commit anchors (`native-adapter-organization` `c526639f`, `storage-integrity-and-streaming` `14b70b07`), re-anchored `plan_commit` to `473e1f2e`, and ordered the remaining flows as one serial chain.

## [2026-11-15T17:48:00Z] archive | broker-pool-sharing

- Archived completed specification `broker-pool-sharing` (`ch01`–`ch04`, `16/16` tasks across `936110fc..28415b3f`) into permanent OKF knowledge chapters (`broker-and-shm-leases.md`, `shm-broker-limitations.md`, `sqlspec-alchemy-and-dialects.md`, `cli.md`), aligned Master PRDs (`native-data-platform`, `integration-remediation`), downstream specifications (`native-sqlite-driver`, `native-redis-driver`, `sqlalchemy-and-django-integration`, `sqlspec-adapter-parity`, `queues-tasks-and-channels`, `spanner-queue-and-rw-transactions`, `strict-typing-hygiene`), and bundle indexes (`index.md`, `specs/index.md`), and removed `.agents/bundles/specs/broker-pool-sharing/`.

## [2026-11-15T17:47:30Z] finish | broker-pool-sharing

- Completed `broker-pool-sharing` (`ch01`–`ch04`, `16/16` tasks covering `#95`, `#96`, `#97`, `#98`) across `936110fc..28415b3f`: implemented atomic `ShmOwnerHeader` create-or-attach with `SHM_INIT_REAPING` and magic-publication waiting, async-signal-safe `cleanup_all` and `reap_dead_owner_segments`, unseeded XXH3-64 `stable_hash64` keys and `64` `pool_key()` partitions, `SLOT_STATE_CLAIMING`/`SLOT_STATE_RECLAIMING`/`LeaseState::Quarantined` state transitions, authenticated `0600` UDS `SCM_RIGHTS` attach (`BrokerAttachServer`, `BrokerAttachClient`), versioned C-ABI `BrokerEngineCapsule` + `BrokerEngineRegistry` across PostgreSQL, MySQL, Oracle, MSSQL, and DuckDB, `BrokerHost` (`vtb broker serve|status`) crash `ROLLBACK` cleanup, typed SQLSpec `ProxySyncDriver`/`ProxyAsyncDriver` (`SyncDriverAdapterBase`/`AsyncDriverAdapterBase`), standalone `vertebra.db.dbapi`, isolated `SQLAlchemyAsyncDBAPI`, SQLAlchemy `BrokerPool`/`AsyncBrokerPool`, and verified `make lint`, `make dev`, `make rs-test`, `make postgres-test`, `make docs`, and `766` unit tests cleanly.

## [2026-11-14T22:49:30Z] archive | http3-and-http-hygiene

- Archived completed specification `http3-and-http-hygiene` (`t01`–`t03` across `9b44d409..f277a29b`) into permanent OKF knowledge and product chapters (`ingress-and-routing.md`, `protocol-and-offload-modules.md`, `server-and-dispatch.md`, `lock-free-shm-and-sharding.md`, `polyglot-test-matrix.md`, `workflow.md`, `product.md`, `tech-stack.md`), updated Master PRD `integration-remediation` and bundle indexes (`index.md`, `specs/index.md`), and removed `.agents/bundles/specs/http3-and-http-hygiene/`.

## [2026-11-14T22:49:00Z] finish | http3-and-http-hygiene

- Completed `http3-and-http-hygiene` (`t01`–`t03`) across `9b44d409..f277a29b`: parsed `VERTEBRA_HTTP3*` config settings with upfront `validate_http3()` checks in `main.rs` and `server::run_with_listener`, installed the `ring` crypto provider in `TlsAcceptor::from_config`, bound `quinn::Endpoint` over `bind_udp_listener` (`socket2` `SO_REUSEADDR` and Unix `SO_REUSEPORT`), enforced pre-buffering `Content-Length` and chunked `max_body_size` bounds (`stop_sending` + `413 Payload Too Large`) and RFC 9114 hop-by-hop header stripping (`strip_hop_by_hop_headers`) in `http3::handle_h3_request`, added `SseHub::close_all()` and bounded `JoinSet` + `h3_conn.shutdown(0)` + `endpoint.wait_idle()` draining, shared `payload::decode_buffered_body` between streaming and buffered ingress paths, gated `Alt-Svc` strictly on bound `ServerState::http3_port`, removed dead `McpBridgeConfig`/`McpStdioBridge` in `mcp_bridge.rs`, aligned `Makefile`, CI, release binary, and PGO builds/docs with `--features http3`, and verified `make rs-test`, `make docs`, `make review-check`, Ruff, Mypy, and Clippy.

## [2026-11-14T21:38:30Z] archive | native-adapter-organization

- Archived completed specification `native-adapter-organization` (`t01`–`t04` across `838c54d1..8a836024`) into permanent OKF knowledge chapters (`database-drivers-and-dialers.md`, `gcp-bigquery-and-spanner-drivers.md`, `query-pipeline-cdc-and-tenancy.md`, `module-and-class-layout.md`), updated Master PRD `native-data-platform` and bundle indexes (`index.md`, `specs/index.md`), and removed `.agents/bundles/specs/native-adapter-organization/`.

## [2026-11-14T21:38:00Z] finish | native-adapter-organization

- Completed `native-adapter-organization` (`t01`–`t04`) across `838c54d1..8a836024`: extracted `bigquery/object_tables.py` (`ObjectRef`, `ObjectTableConfig`, single-pass `_split_uri_bucket_and_key`, `(DatabaseError, OSError)` fallback) and `spanner/{mutations,transactions}.py` (`gc=True` on `Mutation`/`MutationGroup`, `_encode_parameters_payload`, async abort backoff), decomposed Oracle (`oracle/pipeline.py`), MySQL (`SyncMySQLPipeline`/`AsyncMySQLPipeline`, non-blocking `_parse_dsn_fields`, literal-safe `MySQLCore` bind compilation, `Exception` catching preserving `CancelledError`), MSSQL, and DuckDB (`_scan_sql_keywords_outside_literals` boundary fix, shared lease/invalidation helpers on `DuckDBCore`) into `_sync.py`/`_async.py`/`driver.py` barrels, narrowed `DataFusionConfig` (`mode="in_process"` with scheme/path/blank-query validation) and removed vendor registration wrappers from `DataFusionEngine`, and passed zero-context reviews, `ruff`, `mypy`, `stubtest`, `make docs`, `make review-check`, and `pytest` (`1144 passed`).

## [2026-11-14T20:02:00Z] align | flow-setup

- Executed `/flow-setup` (`Align & True-Up`) against `fix/audit-remediation @ 104fd6fa` (`main @ 4773e2fc`): fixed OKF v0.2 tag/heading validation errors in `database-drivers-and-dialers.md` and `strict-typing-hygiene:t02`, updated `workflow.md` (`[dependency-groups].bench` `duckdb` isolation), reconciled unstarted spec blueprint drift across `native-adapter-organization` (`depends_on_flows`), `native-sqlite-driver` (`[all]` extra removal, `ConformanceGate`/`registry.py`, `py_async.rs`), `native-redis-driver` (`[all]` extra removal, workspace `Cargo.toml`), `broker-pool-sharing` (`MssqlBrokerBackend` in `src/rs/vertebra-mssql/src/broker.rs`), `sqlspec-adapter-parity` (`execute_with_description` reuse), and `queues-tasks-and-channels` (`MultiLaneWorkerRunner` `read_set`, preserved `SCAN`+`UNLINK`), and removed legacy untracked `.agents/studio-live-state.json`.
- Consolidated chapter structure across all planned multi-chapter specifications (`47` chapters / `150` multi-chapter tasks -> `21` chapters / `85` multi-chapter tasks + flattened `spanner-queue-and-rw-transactions` to `4` flat tasks, keeping `100%` of requirements, contracts, files, and tests intact): `broker-pool-sharing` (`8 -> 4` chapters), `native-sqlite-driver` (`5 -> 3` chapters), `native-redis-driver` (`8 -> 4` chapters), `sqlalchemy-and-django-integration` (`7 -> 3` chapters), `sqlspec-adapter-parity` (`10 -> 4` chapters), `queues-tasks-and-channels` (`7 -> 3` chapters), and `spanner-queue-and-rw-transactions` (`2 -> 0` chapters, flat `t01`–`t04`).

## [2026-11-14T18:30:30Z] archive | driver-conformance-ci

- Archived completed specification `driver-conformance-ci` (`ch01`–`ch04`, `13/13` tasks across `a8ca7b24..cc75373f`) into permanent OKF knowledge chapters (`object-storage.md`, `database-drivers-and-dialers.md`, `database-driver-limitations.md`, `polyglot-test-matrix.md`, `benchmarks-and-free-threaded.md`, `workflow.md`), updated Master PRD `native-data-platform` and bundle indexes (`index.md`, `specs/index.md`), and removed `.agents/bundles/specs/driver-conformance-ci/`.

## [2026-11-14T18:28:30Z] finish | driver-conformance-ci

- Completed `driver-conformance-ci` (`ch01`–`ch04`, `13/13` tasks) through `cc75373f` / `596c32d6`: shipped `SyncGcsBearerSecretProvider` and `AsyncGcsBearerSecretProvider` in `vertebra.storage`, `DuckDBSecretConfig` validation + `(HTTP 401)`/`(HTTP 403)` provider invalidation + startup `_client` cleanup, Oracle AQ `wait_timeout_seconds` `SELECT 1 FROM DUAL` idle liveness probing, zero-skip `ConformanceGate` and `ServiceVersionMatrix`, `mariadb` `EngineConformanceSpec`, live `test_{duckdb,mysql,mssql,oracle}_native.py` suites, `make db-conformance`, `bench`-only PyPI `duckdb` isolation, `.github/workflows/ci.yml` `test-db-conformance` (10-row matrix) + `conformance-oracle` (`continue-on-error: true`), and public/OKF documentation synchronization (`43 passed, 0 skipped` in `make db-conformance ENGINE=duckdb`, `7 passed` in `cargo test -p vertebra-oracle notify::`, `137 passed` in unit suites, `make docs`, and `make lint-all` clean).

## [2026-11-14T18:20:00Z] chapter-complete | driver-conformance-ci ch04-docs-and-okf-sync

- Closed `ch04-docs-and-okf-sync` (`t01`–`t04`) across `57fed1c4` and `cc75373f`: documented `SyncGcsBearerSecretProvider` and `AsyncGcsBearerSecretProvider` across `docs/usage/{storage,analytics-and-cdc,databases}.md` and rendered API directives in `docs/reference/storage.md`, updated `docs/changelog/index.md` and `AGENTS.md` (`70` lines), and synchronized `.agents/bundles/knowledge/testing/{polyglot-test-matrix,benchmarks-and-free-threaded}.md`, `.agents/bundles/knowledge/workflow.md`, and `.agents/bundles/knowledge/domains/data/{database-drivers-and-dialers,database-driver-limitations}.md` (`<= 200` lines each; `make docs` and `make review-check` clean).
- All `4/4` chapters (`13/13` tasks) of `driver-conformance-ci` are complete and ready for `/flow:finish`.

## [2026-11-14T17:59:00Z] chapter-complete | driver-conformance-ci ch03-make-lanes-bench-isolation-and-ci

- Closed `ch03-make-lanes-bench-isolation-and-ci` (`t01`–`t02`) at `b45ebf84` (sync `5cf424fb`): added `make db-conformance` (`ENGINE=<duckdb|mysql|mariadb|mssql|oracle> [VERSION=<v>]`), removed the `test_freethreaded_gil.py` fallback from `oracle-test`, removed `--group duckdb` from `PY_TEST_GROUPS`, `make install`, and `make upgrade` while keeping `"duckdb>=1.0; python_version < '3.14'"` in `[dependency-groups].bench`, and added `test-db-conformance` (10 matrix rows) and `conformance-oracle` (`continue-on-error: true`) in `.github/workflows/ci.yml` (`35 passed` in `test_bench_infrastructure.py`, `ruff`, `make review-check`, and dual zero-context `code-reviewer` + `quality-reviewer` reviews clean).
- Advanced active frontier to `ch04-docs-and-okf-sync` (`driver-conformance-ci:ch04:t01`; `4` tasks remain in `ch04`).

## [2026-11-14T17:48:00Z] chapter-complete | driver-conformance-ci ch02-conformance-harness-and-live-suites

- Closed `ch02-conformance-harness-and-live-suites` (`t01`–`t04`) at `38e02b16` (sync `7d11fb2c`): added `ConformanceGate` and `ServiceVersionMatrix` in `src/py/tests/integration/conformance_gate.py`, wired zero-skip session hooks and versioned `vtb_{mysql,mariadb,oracle,mssql}_config` fixtures in `src/py/tests/integration/conftest.py`, added `mariadb` `EngineConformanceSpec` and removed dead `_DUCKDB_SKIP_REASON` / `VERTEBRA_CONFORMANCE_DUCKDB_DSN` in `test_driver_conformance.py`, and created `test_conformance_gate.py`, `test_duckdb_native.py`, `test_mysql_native.py`, `test_mssql_native.py`, and updated `test_oracle_native.py` (`192 passed, 369 skipped` offline; `42 passed, 0 skipped` under `VERTEBRA_CONFORMANCE_ENGINES=duckdb`; `ruff`, `mypy`, `make review-check`, and dual zero-context `code-reviewer` + `quality-reviewer` reviews clean).
- Advanced active frontier to `ch03-make-lanes-bench-isolation-and-ci` (`driver-conformance-ci:ch03:t01`; `6` tasks remain across `ch03`–`ch04`).

## [2026-11-14T17:26:00Z] revise | driver-conformance-ci

- Revised `driver-conformance-ci` (`plan_revision: 4`, `plan_commit: 37972f2a`) across root `spec.md` and `ch02`–`ch04`: reconciled `ConformanceGate.from_environ` so `VERTEBRA_CONFORMANCE_ENGINES=postgres` delegates to `evaluate_postgres_zero_skip_gate` when `VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS=1` is set (coexisting cleanly with `.github/workflows/ci.yml` `postgres-conformance`) while raising `pytest.UsageError` when `"postgres"` is passed without `VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS=1`, added `pyproject.toml` to `ch02:t01` `files:` for `driver_engine` marker registration, and aligned `ch02:t02` to remove unused `_DUCKDB_SKIP_REASON` and `VERTEBRA_CONFORMANCE_DUCKDB_DSN` from `test_driver_conformance.py`.

## [2026-11-14T17:16:00Z] synthesize | flow-learn-sync

- Audited all `13` Flow specifications line by line against `fix/audit-remediation @ 334dc98d` (`main @ 14b70b07`): confirmed `driver-conformance-ci ch01` (`3/13` tasks, `a8ca7b24`/`b3b113d1`/`cae00a18`) complete while `ch02`–`ch04`, `strict-typing-hygiene`, `http3-and-http-hygiene`, `native-adapter-organization`, `broker-pool-sharing`, `native-sqlite-driver`, `native-redis-driver`, `queues-tasks-and-channels`, `spanner-queue-and-rw-transactions`, `sqlalchemy-and-django-integration`, and `sqlspec-adapter-parity` remain `PARTIAL` or `NOT DONE`.
- Reconciled root indexes (`.agents/bundles/{index,specs/index}.md`), master PRDs (`native-data-platform`, `integration-remediation`), `driver-conformance-ci` (`spec.md`, `ch02`/`ch03`, `log.md` rebased SHAs), `strict-typing-hygiene` (`tools/benchmarks` paths & strict typing fixes landed at `334dc98d`, `52` override patterns, `polyglot-test-matrix.md` at `196L`), `http3-and-http-hygiene` (`quinn 0.11.12`), `native-adapter-organization` (updated LOC baselines & `analytics-and-cdc.md`), `product/tech-stack.md`, `knowledge/` (`benchmarks-and-free-threaded.md`, `polyglot-test-matrix.md`, `packaging-and-pgo.md`, `adr-004`, `object-storage.md`, `database-driver-limitations.md`, `database-drivers-and-dialers.md`, `postgres-native-driver.md`, `ingress-and-routing.md`), and `.agents/skills/{runtime-ipc,review-runtime-ipc,review-python-framework}/SKILL.md`.
- Flagged unstarted spec conflicts requiring `/flow:revise`: `driver-conformance-ci:ch02:t01` `postgres` `UsageError` vs `.github/workflows/ci.yml` (`postgres-conformance`); `native-{sqlite,redis}-driver` `[all]` optional-dependency & `_rust_async.py`/`Cargo.toml` drift; `queues-tasks-and-channels` `QueueStore`/`WorkerRunner`/`CronScheduler`/`flushdb()` drift; `broker-pool-sharing` existing `mssql/broker.rs` & `dialect_tag`; and `sqlspec-adapter-parity` `execute_with_description` reuse.

## [2026-11-14T04:01:30Z] archive | native-postgres-driver

- Archived completed specification `native-postgres-driver` (`ch01`–`ch09`, `32/32` tasks across `18baadcd..d4b87471`) into permanent OKF knowledge chapters (`database-drivers-and-dialers.md`, `postgres-native-driver.md`, `postgres-limitations.md`, `database-driver-limitations.md`, `query-pipeline-cdc-and-tenancy.md`, `sqlspec-alchemy-and-dialects.md`, `distributed-queues-and-email.md`, `packaging-and-pgo.md`, `polyglot-test-matrix.md`, `benchmarks-and-free-threaded.md`, `workflow.md`, `adr-004`, product docs, and `areas.toml`) and removed `.agents/bundles/specs/native-postgres-driver/`.

## [2026-11-14T03:59:30Z] finish | native-postgres-driver

- Completed `native-postgres-driver` (`ch01`–`ch09`, `32/32` tasks) through `d63f9ade`: standalone `vertebra-postgres` (`vertebra._postgres`), verified replication supervision (`ReplicationSupervisor`), cancel-safe `AsyncPostgresReplication`, durable CAS `PostgresChangeSource`, permanent `_sync.py`/`_async.py` drivers, atomic removal of `psycopg`/`asyncpg`, GCP dialer cleanup, PG14–18 conformance, companion wheel isolation, Criterion + Python benchmark lanes, Zensical docs, and cross-bundle OKF reconciliation.

## [2026-11-14T03:59:30Z] chapter-complete | native-postgres-driver ch09-data-packaging-okf-and-routing-sync

- Closed `ch09-data-packaging-okf-and-routing-sync` through `b3ab1375` / `d63f9ade`: trimmed `database-drivers-and-dialers.md` (`<= 200L`), finalized `postgres-native-driver.md`, `postgres-limitations.md`, `database-driver-limitations.md`, `query-pipeline-cdc-and-tenancy.md`, `tech-stack.md`, `product.md`, `packaging-and-pgo.md`, `sqlspec-alchemy-and-dialects.md`, `adr-004`, knowledge/domain indexes, and `areas.toml` routing, resolved follow-up CDC lock/reconcile/cursor invariants (`test_postgres_change_stream.py`: `34 passed`), and obtained clean `PASS` verdicts across zero-context `code-reviewer`, `docs-okf-reviewer`, and `quality-reviewer`.
- Advanced active frontier to completed state (`0` tasks remain).

## [2026-11-14T01:12:30Z] chapter-complete | native-postgres-driver ch08-user-and-reference-docs

- Closed `ch08-user-and-reference-docs` through `0eabc0a2`: synchronized all public Zensical usage/reference guides (`databases.md`, `analytics-and-cdc.md`, `database-capabilities.md`, `native-stubs.md`, `reference/index.md`, `installation.md`, `cloud-dialers.md`, `packaging-and-deployment.md`, `queues-and-realtime.md`, `extensions.md`, `usage/index.md`), `README.md`, and `AGENTS.md` (`<= 100L`), added optional `[loop]` extras (`uvloop`, `winloop`, `rloop`, `zuvloop`) and shared `LOOP_BACKEND_CHOICES` CLI flags across `vtb run`/`vtb dev`/`litestar run`, ensured concrete `connection_type` across all 16 SQLSpec adapter configs without Litestar deprecation warnings, and updated runtime/Litestar/product OKF chapters (`ruff`, `mypy`, `237 passed` offline docs/extension/server tests, `make docs`, `make review-check`, and dual zero-context reviews clean).
- Advanced active frontier to `ch09-data-packaging-okf-and-routing-sync` (`native-postgres-driver:ch09:t01`; `4` tasks remain).

## [2026-11-13T22:53:00Z] chapter-complete | native-postgres-driver ch07-wheel-ci-and-benchmarks

- Closed `ch07-wheel-ci-and-benchmarks` at `480840a3` (sync `af023efd`): shared `tools/scripts/_wheel_verify.py` archive/linkage/isolated-venv primitives across `verify_postgres_wheel.py`, `verify_oracle_wheel.py`, and `verify_storage_wheel.py` with lazy fail-loud `.connect()` probes (`SyncPostgreSQLDriver`, `AsyncPostgreSQLDriver`, `PostgreSQLQueueBackend`) and 14-class smoke checks, fixed `cloudsql.py` `extra="gcp"`, aligned `src/rs/vertebra-postgres/pyproject.toml`, wired CI `postgres-wheel-check` + zero-deselection `VERTEBRA_CONFORMANCE_ENGINES` / `VERTEBRA_CONFORMANCE_POSTGRES_VERSION` PG14–18 lanes + 14-class release checks, added `[dependency-groups].postgres-bench` + `tools/benchmarks/postgres.py` + `benches/postgres_native.rs` (`cargo test --bench postgres_native --locked`) + `Makefile` `postgres-bench{,-check}` targets, rebuilt `vertebra._postgres` via `maturin develop`, verified parameterized free-threaded GIL/immutability/thread-pool suites, and synced testing/workflow OKF chapters (`170L`, `196L`, `122L`; dual zero-context reviews passed cleanly).
- Advanced active frontier to `ch08-user-and-reference-docs` (`native-postgres-driver:ch08:t01`; `8` tasks remain across `ch08`–`ch09`).

## [2026-11-13T21:59:10Z] chapter-complete | native-postgres-driver ch06-live-conformance-and-service-acceptance

- Closed `ch06-live-conformance-and-service-acceptance` at `264b4684` (sync `7a7b5180`): enabled PostgreSQL `14`–`18` (`supports_pipeline=True`, `pending_flow_id=None`, `_POSTGRES_TYPES_DDL`, `PgJson` scalar binding) in `test_driver_conformance.py`, added `VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS=1` terminal enforcement (`conftest.py` + `test_postgres_zero_skip_gate.py`), added `test_postgres_replication_live.py` (`wal_level=logical` `pgoutput`, `PG_UNCHANGED_TOAST`, raw `bytea`, fresh `PostgresChangeSource` resume, slot contention), and added `test_postgres_native_features_live.py` (`pgvector/pgvector:pg18`, binary `COPY`, `VTBARROW`, `LISTEN/NOTIFY`, `PgBouncer` `transaction`/`statement` modes; `18 passed, 5 skipped`, `63 passed, 242 skipped, 144 deselected`, `ruff`, `mypy`, and dual zero-context reviews clean).
- Advanced active frontier to `ch07-wheel-ci-and-benchmarks` (`native-postgres-driver:ch07:t01`; `12` tasks remain across `ch07`–`ch09`).

## [2026-11-13T15:28:00Z] chapter-complete | driver-conformance-ci ch01-duckdb-gcs-secrets-and-oracle-notify

- Closed `ch01-duckdb-gcs-secrets-and-oracle-notify` across rebased branch commits `a8ca7b24` and `b3b113d1` (bundle sync `cae00a18`): shipped `SyncGcsBearerSecretProvider` and `AsyncGcsBearerSecretProvider` in `vertebra.storage`, added `DuckDBSecretConfig.__post_init__` validation, `(HTTP 401)`/`(HTTP 403)` provider invalidation with generation eviction, and fail-closed startup/transaction `_client` cleanup across `SyncDuckDBDriver` and `AsyncDuckDBDriver`, and enforced `OracleNotifyConfig.wait_timeout_seconds` `SELECT 1 FROM DUAL` idle liveness probes in `vertebra-oracle/src/notify.rs` (`7` Rust `notify::` tests, `clippy`, `ruff`, `mypy`, and `151` Python tests clean).
- Advanced active frontier to `ch02-conformance-harness-and-live-suites` (`driver-conformance-ci:ch02:t01`; `10` tasks remain across `ch02`–`ch04`).

## [2026-11-13T16:15:00Z] chapter-complete | native-postgres-driver ch05-atomic-cutover-and-legacy-removal

- Closed `ch05-atomic-cutover-and-legacy-removal` at `d635f818` (sync `11c7c541`): cut SQLSpec (`ParameterStyle.NUMERIC`) and SQLAlchemy (`numeric_dollar`) over to `vertebra._postgres`, replaced `postgres/driver.py` with the native barrel, removed `psycopg`/`asyncpg`/`asyncmy` across drivers/queues/dialers, hardened `cloudsql.py` and `alloydb.py` TLS/RSA/async refresh paths, and synchronized OKF chapters (`715` Python tests, `11` `py_async` Rust tests, `stubtest`, `ruff`, `mypy`, and `review_setup.py validate` clean).
- Advanced active frontier to `ch06-live-conformance-and-service-acceptance` (`native-postgres-driver:ch06:t01`; `15` tasks remain across `ch06`–`ch09`).

## [2026-11-13T05:51:30Z] chapter-complete | native-postgres-driver ch04-permanent-native-postgres-drivers

- Closed `ch04-permanent-native-postgres-drivers` at `50d7a1ef` (sync `c97e037e`): implemented permanent `SyncPostgreSQLDriver` (`_sync.py`) and native `AsyncPostgreSQLDriver` (`_async.py`) over `SyncPostgresPool`/`AsyncPostgresPool`, shielded async transaction rollback/eviction via `anyio.CancelScope(shield=True)`, wired `pipeline.py`, `arrow.py`, and `EventSessionRegistry`-tracked notification/replication hooks (`ruff`, `mypy` across `20` source files, and `131` PG unit tests clean).
- Advanced active frontier to `ch05-atomic-cutover-and-legacy-removal` (`native-postgres-driver:ch05:t01`; `19` tasks remain across `ch05`–`ch09`).

## [2026-11-13T05:15:30Z] chapter-complete | native-postgres-driver ch03-cdc-options-and-bind-compiler

- Closed `ch03-cdc-options-and-bind-compiler` at `bff04dcd` (sync `31024a23`): added `PostgresReplicationDriver` and `EventSessionRegistry`, CAS-before-wire-ack `PostgresChangeSource`, libpq/Unix-socket/multi-host/Cloud-SQL `options.py` normalization, and single-pass lexer `binds.py` (`ruff`, `mypy` across `15` source files, and `111` unit tests clean).
- Advanced active frontier to `ch04-permanent-native-postgres-drivers` (`native-postgres-driver:ch04:t01`; `22` tasks remain across `ch04`–`ch09`).

## [2026-11-13T04:54:36Z] chapter-complete | native-postgres-driver ch02-replication-python-bridge

- Closed `ch02-replication-python-bridge` at `f4d3d1eb` (sync `1eeeffb9`): added cancel-safe `spawn_rust_db_task_with_attr_and_future`, exposed frozen `AsyncPostgresReplication` and `AsyncPostgresClient.{replication_identity, open_replication}` with 14-class `_postgres.pyi`, and synchronized the 3 PyO3 FFI OKF chapters (`<= 200` lines each; `11` `py_async` + `2` `replication` Rust tests, `110` Python unit tests, `stubtest`, `clippy`, `ruff`, `mypy`, and `review_setup.py validate` clean).
- Advanced active frontier to `ch03-cdc-options-and-bind-compiler` (`native-postgres-driver:ch03:t01`; `26` tasks remain across `ch03`–`ch09`).

## [2026-11-13T02:55:00Z] chapter-complete | native-postgres-driver ch01-replication-supervision

- Closed `ch01-replication-supervision` at `bfa2dad7` (`57` wire tests passed, `cargo clippy --all-targets -- -D warnings` clean, `postgres-native-driver.md` and `postgres-limitations.md` synchronized).
- Advanced active frontier to `ch02-replication-python-bridge` (`native-postgres-driver:ch02:t01`; `30` tasks remain across `ch02`–`ch09`).

## [2026-11-13T01:52:00Z] chapter-checkpoint | native-postgres-driver ch01-replication-supervision

- Completed `ch01-replication-supervision` (`t01`–`t02`) at `7d50fd42` on `feat/postgres-driver-complete`: added `ReplicationSupervisor`, `ReplicationEventDelivery`, `ReplicationStreamEvent`, `PostgresReplicationResume`, `raw_reconnect_delay` (`MAX_RAW_RECONNECT_ATTEMPTS = 8`), and `PostgresReplicationConnection::connect_logical_verified`, verified across `56` wire tests (`postgres_replication_wire_tests`) and `cargo clippy --all-targets -- -D warnings`.

## [2026-11-13T20:42:00Z] archive | mcp-protocol-completeness

- Archived `mcp-protocol-completeness` (`7` chapters, `25` tasks, `c4380215..e95e1f19`), delivering single-pass `Tool.authorize`/`Tool.call` (`-32004`/`-32005`), `Mcp-Param-*` validation (`-32020`), expiring single-use `ConfirmationGate`, dual-mode `SUPPORTED_PROTOCOL_VERSIONS` (`MCPSessionStore`, `GET /mcp` SSE, `DELETE /mcp`, `2025-03-26` batches), `resources/subscribe` & `subscriptions/listen`, `SKILLS_EXTENSION` + SEP-1865 `UI_EXTENSION` across mounts (`litestar-mcp>=0.20.0`), incremental `run_stdio_server` (`vtb ai mcp serve --app`), `OidcTokenSource` (`vtb ai mcp bridge --audience`), ES256 `IapIdentitySource`, updated docs/changelog, and present-tense OKF chapters (`mcp-and-a2a-protocol-harness.md` `147L`, `mcp-limitations.md` `58L`, `cli.md`, `litestar.md`, `polyglot-test-matrix.md`).

## [2026-11-13T20:35:00Z] chapter-complete | mcp-protocol-completeness ch06-stdio-serve-bridge-and-gcp-auth

- Closed `ch06-stdio-serve-bridge-and-gcp-auth` at `7607f4d8`: implemented concurrent incremental `run_stdio_server` with `notifications/cancelled` cancellation, legacy session stdout relay, and EOF cleanup; added `resolve_app_mcp_core` / `is_litestar_mcp_app` app discovery (`vtb ai mcp serve --app`, removing `--name`); added headerless legacy `mcp_request_headers(..., protocol_version=None)`, `BridgeTransport` / `GET` SSE relay (`Last-Event-ID`, `5` reconnects, `404`/`405` stop, EOF `DELETE`), and `OidcTokenSource` (`AsyncGcpIdTokenCache`, single `401` retry) with `vtb ai mcp bridge --audience`; and shipped ES256 `IapIdentitySource` + `iap` extra (`uv lock --check --offline`, `ruff`, `mypy`, and `276` Python tests clean).
- Advanced active frontier to `ch07-docs-and-okf-sync` (`mcp-protocol-completeness:ch07:t01`; `4` tasks remain).

## [2026-11-13T05:29:35Z] chapter-complete | mcp-protocol-completeness ch05-framework-mounts-and-litestar-parity

- Closed `ch05-framework-mounts-and-litestar-parity` at `63904aab` (sync `0166aa62`): wired `SkillCatalog` coercion, well-known `skills` lists, and startup `MCPCore.validate()` across `AgentPlugin`, Starlette/FastAPI, Flask, and Django MCP mounts, bumped `litestar-mcp>=0.20.0` in `pyproject.toml` + `uv.lock`, added pre-`0.20` capability guard in `create_mcp_plugin`, and merged Litestar `skill_catalog` names in `/.well-known/mcp.json` (`uv lock --check`, `ruff`, `mypy` across `48` source files, and `279` tests clean).
- Advanced active frontier to `ch06-stdio-serve-bridge-and-gcp-auth` (`mcp-protocol-completeness:ch06:t01`; `7` tasks remain across `ch06`–`ch07`).

## [2026-11-13T05:17:45Z] chapter-complete | mcp-protocol-completeness ch04-skills-and-mcp-apps-core

- Closed `ch04-skills-and-mcp-apps-core` at `b0c0ae12` (sync `7ea0f382`): extended `SkillCatalog` filtering/URI resolution/listeners/refresh, advertised `io.modelcontextprotocol/skills` and `io.modelcontextprotocol/ui` extensions, added `McpIcon`/`ToolSpec.meta`/`@mcp_tool` UI metadata, implemented `ToolRegistry.validate_ui_links` and `MCPCore.validate()`, filtered app-only tools for non-UI clients and WebMCP manifests (`270` unit tests, `ruff`, and `mypy` clean).
- Advanced active frontier to `ch05-framework-mounts-and-litestar-parity` (`mcp-protocol-completeness:ch05:t01`; `10` tasks remain across `ch05`–`ch07`).

## [2026-11-13T05:03:00Z] chapter-complete | mcp-protocol-completeness ch03-sse-streams-and-resource-subscriptions

- Closed `ch03-sse-streams-and-resource-subscriptions` at `2454dbef` (sync `353c0d0c`): `McpHttpResponse.stream` (`SseStreamBroker`), legacy `GET /mcp` SSE adapter writers across ASGI/Starlette/WSGI/native `App`/Litestar/Flask/Django, `list_changed` + severity-gated `MCPCore.log` + progress notifications, `resources/subscribe`, and `2026-07-28` `subscriptions/listen` (`251` unit tests, `ruff`, and `mypy` clean).
- Advanced active frontier to `ch04-skills-and-mcp-apps-core` (`mcp-protocol-completeness:ch04:t01`; `14` tasks remain across `ch04`–`ch07`).

## [2026-11-13T04:43:42Z] chapter-complete | mcp-protocol-completeness ch02-version-negotiation-and-sessions

- Closed `ch02-version-negotiation-and-sessions` at `5f8fd173` (sync `d7839d5f`): dual-mode `SUPPORTED_PROTOCOL_VERSIONS` negotiation, owner-scoped `MCPSessionStore`, legacy `initialize`/`ping`/`logging/setLevel`, `2025-03-26` JSON-RPC batching, `DELETE /mcp` termination, and unified `_server_capabilities` (`206` unit tests, `ruff`, and `mypy` clean).
- Advanced active frontier to `ch03-sse-streams-and-resource-subscriptions` (`mcp-protocol-completeness:ch03:t01`; `18` tasks remain across `ch03`–`ch07`).

## [2026-11-13T02:55:00Z] chapter-complete | mcp-protocol-completeness ch01-guards-params-and-alias-cleanup

- Closed `ch01-guards-params-and-alias-cleanup` at `1f269a58` (`179 passed`, `mypy` and `ruff` clean, non-destructive multi-call confirmation gating, strict nested JSON `Mcp-Param-*` type comparison, and `ConfirmationCapacityError` package export).
- Advanced active frontier to `ch02-version-negotiation-and-sessions` (`mcp-protocol-completeness:ch02:t01`; `21` tasks remain across `ch02`–`ch07`).

## [2026-11-13T01:59:00Z] chapter-checkpoint | mcp-protocol-completeness ch01-guards-params-and-alias-cleanup

- Completed `ch01-guards-params-and-alias-cleanup` (`t01`–`t04`) at `e22452c5` on `feat/mcp-protocol-completeness`: split `Tool.authorize`/`Tool.call` with `-32004`/`-32005` JSON-RPC error mapping, validated `Mcp-Param-*` headers (`-32020`), hardened `ConfirmationGate` (`consume_call_approval`, `ttl_seconds`, `max_tickets`, exact owner checks), and removed the `MCPService` alias subclass (`177 passed`, `mypy` clean).

## [2026-11-13T20:14:00Z] archive | gcp-embeddings-and-identity

- Archived `gcp-embeddings-and-identity` (`4` chapters, `15` tasks, `aecb5a12..6406bbef`), delivering audience-bound OIDC `SyncGcpIdTokenCache`/`AsyncGcpIdTokenCache` ([#113](https://github.com/cofin/vertebra/issues/113)) including `authorized_user` ADC, canonical ambient ADC fallback, Gemini protobuf field-2 and inline/file multimodal wire codecs, Vertex AI v1 REST `:predict`/`:embedContent`, `RemoteEmbedder` ID-token auth, SQL embedding pushdown parity, per-loop `EmbeddingBroker` SHM cleanup, `GemmaEmbedder` `precision` forwarding, and present-tense OKF chapters (`embeddings-and-vectors.md`, `embeddings-limitations.md`, `object-storage.md`, `pyo3-ffi-and-batch-dispatch.md`, `gcp-bigquery-and-spanner-drivers.md`).

## [2026-11-13T15:20:00Z] finish | gcp-embeddings-and-identity

- Completed `gcp-embeddings-and-identity` (`state: completed`) at `1cd60f53` across all `4` chapters (`15/15` tasks) on `feat/gcp-embeddings-and-identity`, delivering audience-bound OIDC `SyncGcpIdTokenCache`/`AsyncGcpIdTokenCache` across GCE metadata, service-account JWTs, IAM impersonation, `gcloud`, and `authorized_user` ADC ([#113](https://github.com/cofin/vertebra/issues/113)), canonical ambient ADC fallback, Gemini protobuf field-2 and inline/file multimodal wire codecs, Vertex AI v1 REST `:predict` / `:embedContent`, `RemoteEmbedder` ID-token auth, SQL pushdown parity, per-loop `EmbeddingBroker` SHM lifecycle, and `GemmaEmbedder` `precision` forwarding.

## [2026-11-13T05:52:30Z] chapter-complete | gcp-embeddings-and-identity ch04-docs-and-okf-sync

- Closed `ch04-docs-and-okf-sync` at `01a74272` (sync `b518e289`): updated `docs/usage/embeddings-and-vectors.md`, `docs/usage/storage-auth-and-signing.md`, and `docs/reference/storage.md` with Vertex AI REST, Gemini multimodal parts, `RemoteEmbedder(id_token_cache=...)`, canonical ADC order, and `authorized_user` / IAP / Cloud Run ID token rules, synchronized `embeddings-and-vectors.md` (`181L`), `object-storage.md` (`179L`), `pyo3-ffi-and-batch-dispatch.md` (`175L`), and `gcp-bigquery-and-spanner-drivers.md` (`153L`), and updated `embeddings-limitations.md` (`53L`) in place (`make docs` and `make review-check` clean).
- Completed all `4` chapters (`15/15` tasks) of `gcp-embeddings-and-identity`.

## [2026-11-13T05:22:00Z] chapter-complete | gcp-embeddings-and-identity ch03-pushdown-and-local-embedding-broker

- Closed `ch03-pushdown-and-local-embedding-broker` at `5771c8e3` (sync `ef00252d`): removed implicit metadata/storage fallbacks from `OracleEmbeddingPushdown`, aligned `embed_batch`/`top_k`/boolean/dimension validation across BigQuery/Spanner/Postgres pushdowns and Spanner `AI.GENERATE_EMBEDDING` parameter binds, isolated `EmbeddingBroker` state per running event loop with `/dev/shm/vtb-qvec-*` cleanup, and forwarded `GemmaEmbedder` `precision` into `FastEmbedder.build_backend` (`ruff`, `mypy` across `12` source files, and `404` tests clean).
- Advanced active frontier to `ch04-docs-and-okf-sync` (`gcp-embeddings-and-identity:ch04:t01`; `3` tasks remain).

## [2026-11-13T05:05:30Z] chapter-complete | gcp-embeddings-and-identity ch02-vertex-and-gemini-embedder-wire-parity

- Closed `ch02-vertex-and-gemini-embedder-wire-parity` at `484568a7` (sync `d031307b`): fixed Gemini `Part.text` protobuf field `2` + `inline_data`/`file_data` PyO3 wire codecs, extracted `_GoogleEmbedder` per-loop auth/retry base, migrated `VertexEmbedder` onto Vertex AI v1 REST `:predict`/`:embedContent` with multimodal, and added `id_token_cache` + `audience` `401` retry to `RemoteEmbedder` (`vector::` `3`, `grpc` `8`, `stubtest`, `ruff`, `mypy`, and `135` unit tests clean).
- Advanced active frontier to `ch03-pushdown-and-local-embedding-broker` (`gcp-embeddings-and-identity:ch03:t01`; `7` tasks remain across `ch03`–`ch04`).

## [2026-11-13T04:50:11Z] chapter-complete | gcp-embeddings-and-identity ch01-gcp-id-token-cache

- Closed `ch01-gcp-id-token-cache` at `a601dee4` (sync `b3253fee`): implemented per-audience `GcpIdTokenCache` across `ServiceAccount`, `MetadataServer`, `ImpersonatedServiceAccount`, `GcloudCli`, and `AuthorizedUser` ADC, finalized canonical ambient ADC fallback, and exposed `GcpIdTokenSnapshot`, `SyncGcpIdTokenCache`, and `AsyncGcpIdTokenCache` in `vertebra._vertebra` and `vertebra.storage` (`13` Rust `gcp_auth::` tests, `19` Python tests + `2` duckdb-skipped, `clippy`, `stubtest`, `ruff`, `mypy` clean).
- Advanced active frontier to `ch02-vertex-and-gemini-embedder-wire-parity` (`gcp-embeddings-and-identity:ch02:t01`; `11` tasks remain across `ch02`–`ch04`).

## [2026-11-13T16:11:30Z] archive | storage-integrity-and-streaming

- Archived `storage-integrity-and-streaming` (`3` chapters, `9` tasks, `7f247d0e..61f0cd0e`), delivering CRC32C/MD5/SHA-256 receipts, verified streaming reads with `AsyncStreamReader.close()`, streaming Arrow/Parquet multipart writes across SQLSpec/Advanced Alchemy/Django, parameterized `{storage,bigquery,spanner,bundler}-wheel-check` isolation gates, promoted `.agents/bundles/research/upstream-sqlspec-direct-arrow-write.md`, and present-tense OKF storage chapters (`object-storage.md`, `object-storage-limitations.md`).

## [2026-11-13T04:59:52Z] chapter-complete | storage-integrity-and-streaming ch03-wheel-isolation-and-okf-sync

- Closed `ch03-wheel-isolation-and-okf-sync` at `66e6913f` (sync `541d89b5`): added `tools/scripts/verify_storage_wheel.py`, `Makefile` (`{storage,bigquery,spanner,bundler}-wheel-check`), and `release.yaml` isolation checks, updated storage user/reference docs and changelog, and synchronized `object-storage.md` (`175L`), `object-storage-limitations.md` (`86L`), and `research/upstream-sqlspec-direct-arrow-write.md` (`41L`).
- Completed all `3` chapters (`9/9` tasks) of `storage-integrity-and-streaming`.

## [2026-11-13T04:46:07Z] chapter-complete | storage-integrity-and-streaming ch02-streaming-arrow-writes-and-integrations

- Closed `ch02-streaming-arrow-writes-and-integrations` at `c9f54436` (sync `099753b4`): threaded `checksum` and `expected_*` digests through `vertebra.storage._arrow`, enabled SQLSpec `supports_direct_arrow_write = True`, added Advanced Alchemy `save_arrow*` + digest forwarding and Django `content.{sha256,crc32c,md5}` forwarding, and added `benches/put_checksum.rs` (`640` + `82` Django Python tests, `cargo test -p vertebra-storage --benches`, `clippy`, `ruff`, `mypy` clean).
- Advanced active frontier to `ch03-wheel-isolation-and-okf-sync` (`storage-integrity-and-streaming:ch03:t01`; `3` tasks remain in `ch03`).

## [2026-11-13T02:56:00Z] chapter-complete | storage-integrity-and-streaming ch01-integrity-receipts-and-verified-reads

- Closed `ch01-integrity-receipts-and-verified-reads` at `b8b25207` (`47` Rust tests, `485` Python tests, unified verified `open_reader` cursor across `read()`/`__iter__()`/`__aiter__()`, and `io.UnsupportedOperation` on random-access `seek`/`read_at` when `expected_sha256` is set).
- Advanced active frontier to `ch02-streaming-arrow-writes-and-integrations` (`storage-integrity-and-streaming:ch02:t01`; `6` tasks remain across `ch02`–`ch03`).

## [2026-11-13T02:21:00Z] chapter-checkpoint | storage-integrity-and-streaming ch01-integrity-receipts-and-verified-reads

- Completed `ch01-integrity-receipts-and-verified-reads` (`t01`–`t03`) at `85849b32` on `feat/storage-integrity-and-streaming`: added CRC32C, MD5, and SHA-256 receipts, abort-before-publish expected-digest checks, `get_verified`/`get_file_verified`/`get_stream_verified`, and `vertebra.typing.IntegrityError` + `vertebra.storage` re-exports (`47` Rust tests, `485` Python tests, `stubtest` and `mypy` clean).

## [2026-11-13T16:02:30Z] archive | server-runtime-correctness

- Archived `server-runtime-correctness` (`7` chapters, `26` tasks, `47143761..06ef86e9`) after verifying full mypy (`436` files), pure-Python doc postbuild/verifier lazy imports (`serializers/__init__.py`), and present-tense OKF knowledge chapters (`pyo3-ffi-and-batch-dispatch.md`, `asgi-and-wsgi-runners.md`, `tiered-rings-and-batch-bridge.md`, `worker-and-protocols.md`, `server-runtime-limitations.md`, `server-and-dispatch.md`, `product.md`).

## [2026-11-13T15:15:00Z] finish | server-runtime-correctness

- Completed `server-runtime-correctness` (`state: completed`) at `92b84a09` across all `7` chapters (`26/26` tasks) on `feat/server-runtime-correctness`, delivering progressive Python/ASGI/RSGI/native response streaming ([#81](https://github.com/cofin/vertebra/issues/81)), fail-loud `WorkerStartupError` lifespan and `__rsgi_init__` startup with SHM exit `78` ([#84](https://github.com/cofin/vertebra/issues/84)), public `vertebra.supervisor` exports ([#83](https://github.com/cofin/vertebra/issues/83)), co-located and `--worker-only` `QueueService` lanes ([#85](https://github.com/cofin/vertebra/issues/85)), relocatable wheel-bundled `vertebra-server` binaries and `core-wheel-check` ([#82](https://github.com/cofin/vertebra/issues/82)), opt-in `StandaloneBuilder` `.agents/skills` staging ([#115](https://github.com/cofin/vertebra/issues/115)), shared `CompiledRouteBase` / `__vertebra_fastpath__` consolidation, and fork-safe `getrandom` UUID entropy.
- Synchronized runtime, worker, IPC, and product OKF chapters in present tense.

## [2026-11-13T06:14:45Z] chapter-complete | server-runtime-correctness ch07-docs-and-okf-sync

- Closed `ch07-docs-and-okf-sync` at `d9475b72` (sync `8727830e`): regenerated `docs/reference/cli.md`, synchronized server/CLI/packaging usage and reference guides, compacted `pyo3-ffi-and-batch-dispatch.md` (`174L`), synced `tiered-rings-and-batch-bridge.md` (`132L`), `asgi-and-wsgi-runners.md` (`142L`), `worker-and-protocols.md` (`137L`), `server-runtime-limitations.md` (`69L`), and `product.md` (`191L`), and switched `cli/__init__.py` and `server/lanes.py` to `ensure_module` (`make docs`, `make review-check`, and `36` unit tests clean).
- Completed all `7` chapters (`26/26` tasks) of `server-runtime-correctness`.

## [2026-11-13T06:02:45Z] chapter-complete | server-runtime-correctness ch06-runtime-native-cleanup

- Closed `ch06-runtime-native-cleanup` at `db02a0b6` (sync `7e78e3f6`): enforced fail-loud `getrandom::fill` panic and Unix `pthread_atfork` `FORK_GENERATION` reseed in `vertebra-core/src/id.rs`, removed duplicate `_module_cache` from `module_loader.py`/`_typing.py`/`typing.py`, bound `_zero_copy.py` helpers directly to `vertebra._vertebra`, and dropped inert `max_commit_delay_ms` from sync/async Spanner `batch_write` (`4` `id::` Rust tests, `clippy`, `ruff`, `mypy`, and `92` unit tests clean).
- Advanced active frontier to `ch07-docs-and-okf-sync` (`server-runtime-correctness:ch07:t01`; `4` tasks remain).

## [2026-11-13T05:51:45Z] chapter-complete | server-runtime-correctness ch05-fastpath-consolidation

- Closed `ch05-fastpath-consolidation` at `780338a4` (sync `d95d65d3`): extracted `FASTPATH_ATTR = "__vertebra_fastpath__"`, `CompiledRouteBase`, `get_attached_fastpath`/`set_attached_fastpath`, and `has_custom_exception_handlers` in `_fastpath_core.py`, removed alias probes across `_fastpath.py`/`_runner.py` and extension plugins, unified all five framework `CompiledRoute` structs, and enforced exact `BUFFERED_RESPONSE_TYPES` copy-free response serialization (`ruff`, `mypy` across `105` files, and `228` tests clean).
- Advanced active frontier to `ch06-runtime-native-cleanup` (`server-runtime-correctness:ch06:t01`; `7` tasks remain across `ch06`–`ch07`).

## [2026-11-13T05:21:00Z] chapter-complete | server-runtime-correctness ch04-wheel-smoke-and-ci

- Closed `ch04-wheel-smoke-and-ci` at `11731ca5` (sync `ca20d6ae`): added `VtbRunSmoke` multi-wheel smoke harness in `tools/scripts/smoke_vtb_run.py`, wired `make core-wheel-check` and the `core-wheel-smoke` PR CI job, scoped core wheel `cibuildwheel` build/repair hooks and 4-OS `test_wheels` matrix in `release.yaml`, and added `linux-arm64` to `release-binaries.yml` (`ruff`, `mypy`, `make -n core-wheel-check`, and `66` packaging tests clean).
- Advanced active frontier to `ch05-fastpath-consolidation` (`server-runtime-correctness:ch05:t01`; `11` tasks remain across `ch05`–`ch07`).

## [2026-11-13T05:08:50Z] chapter-complete | server-runtime-correctness ch03-relocatable-server-binary-and-packaging

- Closed `ch03-relocatable-server-binary-and-packaging` at `7a5cbc61` (sync `0e03d7a1`): dropped build-host rpaths in `vertebra-http/build.rs`, implemented `ServerBinaryTarget` + `ServerBinaryBuilder` and wheel repair exclusions in `build_server_binary.py`, wired `EmbeddedPythonRuntime` shared-libpython detection into `_supervisor.py` and CLI runners, included `src/py/vertebra/bin/*` in core wheels, and added opt-in `.agents/skills` staging (`VERTEBRA_APP_ROOT`) to `StandaloneBuilder` and `vtb build` (`cargo clippy`/`build`, `readelf` 0 rpath, `ruff`, `mypy`, and `121` tests clean).
- Advanced active frontier to `ch04-wheel-smoke-and-ci` (`server-runtime-correctness:ch04:t01`; `14` tasks remain across `ch04`–`ch07`).

## [2026-11-13T04:57:32Z] chapter-complete | server-runtime-correctness ch02-lifespan-supervisor-and-worker-lanes

- Closed `ch02-lifespan-supervisor-and-worker-lanes` at `d2da8fa0` (sync `f32c3234`): added `WorkerStartupError` + SHM exit code `78` supervisor halt, lazy `vertebra.supervisor` exports, `WorkerRunner.resolve_queue_service`, `vtb workers run` signal lifetime, and `vtb run --worker-only` `HealthProbeServer` (`14` Rust `workers` tests, `183` Python unit/integration tests, `clippy`, `ruff`, and `mypy` clean).
- Advanced active frontier to `ch03-relocatable-server-binary-and-packaging` (`server-runtime-correctness:ch03:t01`; `18` tasks remain across `ch03`–`ch07`).

## [2026-11-13T03:42:00Z] chapter-complete | Server runtime progressive streaming (`server-runtime-correctness/ch01`)

- Closed `ch01-python-stream-submission-and-flush` at `c9e7b370` (`cargo test -p vertebra-py --lib` 14 passed, `cargo clippy -p vertebra-core -p vertebra-py` clean, `mypy.stubtest vertebra._vertebra` clean, `ruff` + `mypy` clean, and `132 passed` across unit and live embedded/SHM `vertebra-server` streaming suites including `StreamingStorageResponse`, `ArrowCursor`, and inline `drive_until_complete` async streams).
- Advanced `server-runtime-correctness` to `ch02-lifespan-supervisor-and-worker-lanes` (`server-runtime-correctness:ch02:t01`; `4/26` tasks complete, `22` tasks remaining across `ch02`–`ch07`).

## [2026-11-13T02:38:00Z] chapter-checkpoint | Server runtime progressive streaming (`server-runtime-correctness/ch01`)

- Completed `t01`–`t04` in `ch01-python-stream-submission-and-flush` at `6a23bd18` (`cargo test -p vertebra-py --lib` 13 passed, `mypy.stubtest vertebra._vertebra` clean, `ruff` + `mypy` clean, `120 passed` across unit and live embedded/SHM `vertebra-server` streaming suites).
- Synchronized `server-runtime-limitations.md` and `server-runtime-correctness` specs in present tense (`4/26` tasks completed; `22` tasks remain across `ch02`–`ch07`) ahead of zero-context chapter review.

## [2026-11-13T00:42:00Z] flow-setup + flow-plan | Cross-harness alignment, merged-chapter contraction, and full 16-flow compact plan

- Aligned multi-harness Flow configuration at `main @ 18baadcd`: updated `.agents/setup-state.json` (`stop_gate: "enforce"`, `ignore_policy: "shared"`, `sha_tracking: "strict"`), `AGENTS.md` (`<= 100L`), `.codex/agents/*.toml` (`16` paired reviewer/specialist wrappers), `.agents/skills/**` + `.agents/agents/*.md` (`13` Rust crates / `10` PyO3 extensions, removing `view_file` tool-alias collision), and trimmed `product/tech-stack.md` + `query-pipeline-cdc-and-tenancy.md` (`<= 200L`) while re-anchoring all post-squash `18baadcd` commit references.
- Contracted the `27` squash-merged chapters (`native-postgres-driver` `ch01`–`ch23` & `ch34`, `server-runtime-correctness` `ch01`, `mcp-protocol-completeness` `ch01`, `storage-integrity-and-streaming` `ch01`) into curated OKF chapters (including new [`postgres-native-driver.md`](knowledge/domains/data/postgres-native-driver.md)) and root `spec.md` baselines, reducing `.agents/bundles/specs/` from `55,459` lines to `35,880` lines (`-35.3%` overall; `-66.0%` across the `7` existing multi-chapter flows) while expanding executable coverage across all `16` child flows (`81` chapters + `3` chapterless flows, `281` `worksheet_format: compact` tasks, `0` unrefined `draft` charters remaining).
- Promoted and fully specified `native-sqlite-driver`, `native-redis-driver` (hoisting `SyncNotificationSource`/`AsyncNotificationSource` split into `ch05` to eliminate the notification dependency cycle), `sqlalchemy-and-django-integration`, `sqlspec-adapter-parity` (`sqlspec>=0.71.1`, zero MyPyC `_compat.py` monkeypatching), and `queues-tasks-and-channels` (`SyncQueueStore`/`AsyncQueueStore`, `litestar-queues>=0.14.0`, Django `6.0+` `TaskBackend`), plus four core gap flows (`native-adapter-organization`, `spanner-queue-and-rw-transactions`, `http3-and-http-hygiene`, `strict-typing-hygiene` `#59`) and folded `#115`, GCP dialer TLS/async bugs, native `App` streaming, `vertebra.typing.IntegrityError`, per-loop `EmbeddingBroker`/`GemmaEmbedder`, and `{bigquery,spanner,bundler}-wheel-check` into their owning flows.

## [2026-11-12T20:10:51.550371+00:00] chapter-checkpoint | PostgreSQL single-generation replication

- Chapter 34 completes four tasks at `f520aa6d`, `d99caddf`, `9d4c4258` and `a1340034`. All 48 canonical wire tests, Clippy and package formatting pass.
- The session owns bounded decoding, independent heartbeats, cancellation-safe delivery leases and one-deadline shutdown. Both knowledge files and the living roadmap are synchronized before correctness and fresh quality review. Task progress is 88/129; 41 tasks remain across 12 subsequent chapters.

## [2026-11-12T15:05:07Z] chapter-complete | PostgreSQL feedback

- Chapter 23 is reviewed at `4700ecbc` after correctness and fresh quality approval. All 23 canonical wire tests, Clippy, formatting and documentation checks pass. Source commits are `64d317cd` and `bbb77ecf`; documentation sync is `3142d692`.
- The pointer advances to ch34. Full flow progress is 23/36 chapters and 84/129 tasks; 45 tasks remain. Commits remain local.

## [2026-11-12] chapter-review | PostgreSQL feedback quality

- Correctness review finds no production defects; the stale remaining-task count is corrected. Quality findings are adopted: malformed payload cases have independent test identities and the living review summary states current outcomes directly.

## [2026-11-12T14:58:28Z] chapter-checkpoint | PostgreSQL commit-fenced feedback

- Chapter 23 completes two tasks at `64d317cd` and `bbb77ecf`. The native codecs and bounded received/published/acknowledged Commit state pass 18 canonical wire tests, Clippy, and package formatting.
- PostgreSQL limitations and the full roadmap are synchronized before independent correctness and fresh quality review. The socket pump, reconnect supervisor, Python handle, and downstream release gates remain open.

## [2026-11-12T04:25:15Z] revise-approved | Native PostgreSQL revision 8

- User approval anchors revision 8 to reviewed plan commit `d90d4625`. Revised specifications and worksheets are stable; chapter 23 is ready for `/flow:implement native-postgres-driver ch23`. No production edits or hosted writes occur.

## [2026-11-12T04:16:55Z] revise-candidate | Native PostgreSQL replication seams

- Revision 8 decomposes replication into ch23 feedback, ch34 single-generation sessions, ch35 verified supervision, and ch36 Python publication. The roadmap spans 36 chapters and 129 worksheets; 82 tasks are complete and 47 remain.
- The draft defines delivery leases, safe transport transfer, and feedback initialization/update contracts. Zero-context plan review and approval are required before implementation resumes. Production and tests are untouched.

## [2026-11-12T04:05:24Z] chapter-complete | Parallel implementation batch

- MCP ch01 and server runtime ch01 are completed after correctness and fresh quality review of `b0578ebc`; chapter/task pointers advance to ch02. PostgreSQL ch22 and storage ch01 retain reviewed checkpoint `9c1865b7`.
- The batch completes 12 tasks across four chapters with signed local commits. MCP/storage Python validation passes 520 tests with one existing skip; 51 additional agent consumer tests pass. PostgreSQL, storage, core, and HTTP native gates pass.
- Remaining flow chapters, live service and wheel lanes, benchmarks, and release acceptance stay open. No push or hosted repository mutation occurs.

## [2026-11-12T04:03:34Z] flow-implement | Context and native streaming review checkpoint

- MCP ch01 completes four tasks through `c007f853`; correctness review reproduces and resolves anonymous task-owner access, and fresh quality review approves the implementation and living docs.
- Runtime ch01 completes four tasks through `f45a5fe8`; SHM reader routing and cleanup pass full core/HTTP tests, four doctests, Clippy, and formatting. Review covers abandoned clients, worker death, blocked sends, and shutdown.
- Rebuilt the core extension; combined MCP/storage Python validation passes 520 tests with one existing skip, and 51 additional agent consumer tests pass. Final chapter stamp synchronization remains; all commits stay local.

## [2026-11-12T03:49:28Z] chapter-complete | PostgreSQL ch22 and storage ch01

- Correctness and fresh independent quality reviews approve both chapters at `9c1865b7`. Task SHAs and living documentation agree; root pointers advance to each flow’s ch23 and ch02 respectively.
- PostgreSQL progress is `22/33` chapters and `82/119` tasks; storage is `1/4` chapters and `2/12` tasks. Remaining runtime, integration, wheel, benchmark, and release gates stay open. No hosted write or push occurs.

## [2026-11-12T03:50:00Z] flow-implement | parallel chapter checkpoints

- Storage ch01 tasks commit as `0658fd1f` and `da0d764e`; fresh native and Python recovery gates pass, source correctness and fresh quality reviews have no material findings. Living documentation reflects conditional copy and incremental uploads.
- MCP ch01 tasks t01–t03 commit as `ff668194`, `7e4129c4`, and `5fccef87`; trusted contexts, cancellation, and adapter forwarding pass focused tests, Ruff, and Mypy.
- Runtime ch01 t01 and t03 commit as `2e4ef248` and `3a3fab9a`; bridge and registry tests, full HTTP tests, Clippy, and HTTP/3 checks pass. Ordered SHM frames and reader routing remain active work. All commits stay local.

## [2026-11-12T03:00:27Z] flow-implement | native-postgres-driver ch22 pre-review checkpoint

- Completed `ch22:t03` (`2947d015`) with pgoutput v1 transaction frames, ordered bounded replication records, deduplication coordinates, and a backpressured queue; completed `ch22:t04` (`48b50600`) with the limitations entry and its two knowledge index registrations.
- Focused replication tests, all-target Clippy, full `vertebra-postgres --tests`, documentation structure/source checks, review manifests, privacy, and whitespace checks pass locally. Live database, wheel/free-threaded, and benchmark acceptance remain downstream gates.
- Synchronized present-tense chapter/root/catalog/knowledge state at `82/119` tasks and `37` remaining; Chapter 22 remains active pending correctness and fresh quality review and closeout stamping. Commits remain local.

## [2026-11-12T02:02:40Z] flow-sync | Review-history continuity and server chapter pointer

- Restored original PostgreSQL review-history objects through `b44e28ca`, retained locally at `refs/flow/native-postgres-reviewed-history`, without changing review stamps or task completion counts.
- Reconciled ten task commit references in chapters 19–21 to patch-equivalent commits that are ancestors of their existing chapter review stamps. Stable patch IDs match across source, tools, build, and CI paths; corresponding task checklist and snapshot references agree.
- Set the planned `server-runtime-correctness` chapter/task pointers to `ch01-bridge-stream-frames` / `server-runtime-correctness:ch01:t01`; approval and implementation remain pending.

## [2026-11-12T01:48:00Z] flow-review + flow-spec + flow-revise | PR range 80ff8a65..c8722c76 and 23 GitHub issues

Audited `80ff8a65..c8722c76` across PRs `#80`, `#92`, `#94`, `#101`, `#102`, `#103`, `#104`, `#105`, `#106`, `#107`, and `#108` alongside all `23` open issues in `cofin/vertebra`:

- Verified split wheel/legacy state from PR `#104` (`c8722c76`): `vertebra._postgres` implements `ch01`–`ch21` and `ch22:t01`–`t02` (`80/119` tasks), while `ch22:t03`–`ch33` remain pending and the Python `postgres` adapter cutover stays open.
- Decomposed six executable `status: draft`, `state: planned` flows (`47` chapters, `144` vertical-slice tasks total): `broker-pool-sharing` (`9` chapters, `31` tasks), `mcp-protocol-completeness` (`12` chapters, `35` tasks), `server-runtime-correctness` (`11` chapters, `33` tasks), `gcp-embeddings-and-identity` (`6` chapters, `17` tasks), `driver-conformance-ci` (`5` chapters, `16` tasks), and `storage-integrity-and-streaming` (`4` chapters, `12` tasks).
- Revised active `native-postgres-driver` (`plan_revision: 7`, `plan_commit: c8722c76`, `33` chapters, `119` tasks) adding `ch22:t04` split-state caveats, replacing deleted `build/packaging/pgo.py` references, and reconciling `ch27` with queues and DB-API extraction; resynced both master PRDs (`integration-remediation` at `plan_revision: 10`, `native-data-platform`) and all five remaining draft charters (`native-sqlite-driver`, `native-redis-driver`, `sqlalchemy-and-django-integration`, `sqlspec-adapter-parity`, `queues-tasks-and-channels`).
- Triaged `23` open GitHub issues: `#89` verified fixed in `#101`; `#100` half-complete (upstream `sqlspec` `StoragePipeline.write_arrow` still buffers); `#115` split to separate `.agents/skills` standalone packaging bug into `mcp-protocol-completeness`; `#59`, `#67`, `#68`, and `#116` deferred in `integration-remediation`; `#81`–`#85`, `#95`–`#99`, and `#109`–`#114` mapped to executable tasks.
- Synchronized OKF knowledge chapters (plus four new limitation records across `embeddings`, `mcp`, `database-driver`, and `server-runtime`) and re-anchored `57` pre-squash `verified[].commit` SHAs in `.agents/bundles/{knowledge,product}` to their reachable `main` squash commits (`067fa901`, `af1b3d30`, `47da6820`, `14aa83b8`).

## [2026-11-11T01:53:00Z] flow-implement | native-postgres-driver (ch22:t01-t02 checkpoint)

Completed and verified `native-postgres-driver:ch22:t01` (`81048251`) and `native-postgres-driver:ch22:t02` (`64288c63`) in active Chapter 22 (`ch22-copyboth-and-pgoutput-decoding`, `2/3` tasks complete, raising flow progress to `80/118` tasks on `feat/native-postgres-driver`): shared bounded `protocol::read_frame` + `'W'` `CopyBothResponse` decoding, `PostgresReplicationConnection::{probe_identity, connect_logical, shutdown}` enforcing pre-dial `PoolerMode::Direct | PoolerMode::Session`, parameterized single-`LEFT JOIN` SHA-256 `publication_fingerprint` (PG14 `9` cols / PG15–18 `11` cols with `attnames` and `rowfilter`) and two-stage `pgoutput.c` publication quoting, recursive standalone metadata custom-type preloading (`vector`, `halfvec`, `enum`, `domain`, `range`, PG14+ `multirange`, `array`, `composite`) with zero SQL on `CopyBoth`, incremental `PgOutputDecoder` (`4096`-cap `RelationCache`, mismatch-only `'Y'` invalidation, lossless four-state `'n'`/`'u'`/`'t'`/`'b'` `TupleData`), and pure-Python `PgUnchangedToast` (`PG_UNCHANGED_TOAST`), `PostgresReplicationIdentity`, and GC-tracked `PostgresReplicationRecord` in `src/py/vertebra/db/adapters/postgres/types.py`. Paused cleanly ahead of `native-postgres-driver:ch22:t03` (`Order logical transactions and change envelopes`).

Functional task commits:
- `native-postgres-driver:ch22:t01`: `81048251` — `read_frame`, `read_replication_message`, `VerifiedReplicationIdentity`, and `PostgresReplicationConnection` `CopyBoth` startup/shutdown.
- `native-postgres-driver:ch22:t02`: `64288c63` — `PgOutputDecoder` relation/type/tuple decoding, recursive custom-type preloading, and `PgUnchangedToast` / `PostgresReplicationIdentity` / `PostgresReplicationRecord`.

## [2026-11-11T00:41:00Z] flow-implement | native-postgres-driver (ch21-live-notifications)

Completed and reviewed Chapter 21 (`ch21-live-notifications`, `3/3` tasks, reviewed checkpoint `b44e28ca`, raising flow progress to `21/33` chapters and `78/118` tasks on `feat/native-postgres-driver`): `session::establish_standalone_connection(config, replication_database)` shared standalone dialer, `PostgresListener` with bounded `1024`-frame + `8 MiB` overflow fail-closed termination, `push_front_unconsumed` retention, `PoolerMode` direct/session enforcement, `1..=63` UTF-8 byte channel validation, `Subscribe`/`Unsubscribe` control signaling alongside `close_notify` shutdown wakeups, and optional `reconnect = true` credential refresh / `reconnected_gap` tracking, `SyncPostgresClient`/`AsyncPostgresClient.open_listener` and `AsyncPostgresListener` with `PendingDeliveryGuard` and read-only `closed`, `events.py::PostgresNotificationDriver`, legacy `driver.py::_publish_notification`, and live `PostgresNotificationSource` with single-pass lock-serialized startup subscription convergence and `ChangeFeed` hydration.

Functional task and review commits:
- `native-postgres-driver:ch21:t01`: `e0ff8d78` — `establish_standalone_connection`, `PostgresListener`, bounded pump, and reconnect gap state.
- `native-postgres-driver:ch21:t02`: `8e8972b3` — PyO3 `AsyncPostgresListener`, `open_listener`, `PendingDeliveryGuard`, and `closed` state.
- `native-postgres-driver:ch21:t03`: `ce3a6769` — `PostgresNotificationDriver` protocol, legacy `_publish_notification`, and live `PostgresNotificationSource`.
- Chapter 21 review checkpoints: `0e726a1c`, `2d143805`, `b44e28ca` (correctness and independent quality reviews passed cleanly; advancing to `ch22-copyboth-and-pgoutput-decoding`).

## [2026-11-10T21:55:00Z] flow-implement | native-postgres-driver (ch20-arrow-copy-import-and-portals)

Completed and reviewed Chapter 20 (`ch20-arrow-copy-import-and-portals`, `4/4` tasks, reviewed checkpoint `40ce977c`, raising flow progress to `20/33` chapters and `75/118` tasks on `feat/native-postgres-driver`): `parse_postgres_qualified_table`, `PostgresArrowImportPlan` with pre-`COPY` domain/OID/codec validation and single-`PGCOPY`-header preservation across batches, bounded `<= 8 MiB` Python Arrow IPC chunking (`iter_arrow_ipc_batches`, `load_arrow_sync`, `load_arrow_async`) without row materialization, PyO3 `SyncPostgresCopyOut`/`AsyncPostgresCopyOut`/`SyncPostgresCopyIn`/`AsyncPostgresCopyIn` and `copy_from_arrow` bridges with critical-section + `AtomicU8` writable-buffer extraction and non-aborting `CopyTaskCleanupGuard` / stall-timeout retirement, cold-OID-discovering `execute_select_to_arrow`, and single-scope generation-fenced `SyncPostgresArrowStream` and `AsyncPostgresArrowStream` (`PendingArrowBatchGuard`) over `VTBARROW` SHM cursors.

Functional task and review commits:
- `native-postgres-driver:ch20:t01`: `1a3007d2` — `PostgresArrowImportPlan`, `copy_from_arrow_with_generation_as`, single-header `CopyFromStream`, and Python IPC chunking.
- `native-postgres-driver:ch20:t02`: `c5b56452` — PyO3 sync/async `COPY` handles and `copy_from_arrow` streaming input bridge.
- `native-postgres-driver:ch20:t03`: `97fc682e` — cold-safe `execute_select_to_arrow`, `ArrowPortalStream`, and `SyncPostgresClient` Arrow streams.
- `native-postgres-driver:ch20:t04`: `83ebe4e1` — `AsyncPostgresClient.select_to_arrow`, `AsyncPostgresArrowStream`, and SHM/portal cancellation cleanup.
- Chapter 20 review checkpoints: `76bf5608`, `40ce977c` (correctness and independent quality reviews passed with zero unresolved findings; advancing to `ch21-live-notifications`).

## [2026-11-10T15:35:00Z] flow-revise | native-postgres-driver

Revised remaining `native-postgres-driver` roadmap at `plan_revision: 6` (`plan_commit: 8e9b324e`, `state: active`, `19/33` chapters and `71/118` tasks completed through `ch19-native-arrow-batches-and-shm` at `59a6dc09`): expanded final documentation, OKF knowledge synchronization, review routing, and release sign-off from `ch30`–`ch31` into `ch30`–`ch33` across `33` chapters / `118` tasks with `[0/33 files documented]`, synchronized root and specification bundle indexes, reconciled `ch20`–`ch29` contracts covering raw client `copy_from_arrow` vs Python driver `load_from_arrow`, `select_to_arrow(..., threshold_rows=256)` and `stream_arrow_batches` `ArrowCursor` wrapping, dedicated `CopyBoth` / `LISTEN` `PoolerMode::Direct | PoolerMode::Session` validation and `PendingDeliveryGuard` cancelled-read retention, unified `sqlspec` `ParameterStyle.NUMERIC` and `sqlalchemy` `numeric_dollar` compilation, preserved `postgres/__init__.py` exports and caller-injected `PostgreSQLQueueExecutor` (`vertebra._postgres`) guards, strict zero-skip Docker conformance gating, `PgBouncer` SCRAM `pg_authid.rolpassword` extraction, and Python `3.12`-scoped `[dependency-groups].postgres-bench` wheel/GIL checks, and curated `native-stubs.md`, `postgres-native-driver.md`, `postgres-limitations.md`, and `areas.toml` review routing targets.

## [2026-11-10T05:10:00Z] flow-implement | native-postgres-driver (ch19-native-arrow-batches-and-shm)

Completed and reviewed Chapter 19 (`ch19-native-arrow-batches-and-shm`, `4/4` tasks, reviewed checkpoint `59a6dc09`, raising flow progress to `19/31` chapters and `71/111` tasks on `feat/native-postgres-driver`): lossless `PostgresArrowSchema` (`postgres.arrow.version = "1"`), bounded `PostgresArrowBatchBuilder` scalar and depth-`32` nested array/composite/`pgvector`/bitstring column builders, unified catalog OID codecs (`xid`/`cid`/`xml`/`unknown` binary + `regproc` cold/warm text parity with explicit binary support), single-stream `PostgresArrowIpcWriter` with adaptive inline and `VTBARROW` `ShmArrowStreamWriter::finish_unregistered()` promotion, cross-extension `ShmCapsule` `PostgresArrowResult` conversion, and engine-agnostic `ArrowCursor` `row_count` validation.

Functional task and review commits:
- `native-postgres-driver:ch19:t01`: `3c246fc0` — lossless PostgreSQL Arrow schemas (`PostgresArrowSchema::from_columns`).
- `native-postgres-driver:ch19:t02`: `1726c31b` — bounded native Arrow scalar `RecordBatch` builders (`PostgresArrowBatchBuilder`).
- `native-postgres-driver:ch19:t03`: `fe925db7` — recursive Arrow builders for multidimensional arrays, composites, `vector`/`halfvec`, and `bit`/`varbit`.
- `native-postgres-driver:ch19:t04`: `df033deb` — adaptive inline and `VTBARROW` SHM IPC writer, `PostgresArrowResult`, and `ArrowCursor` `row_count` metadata.
- Chapter 19 review checkpoints: `ea2309c5`, `59a6dc09` (correctness and independent quality reviews passed with zero unresolved findings; advancing to `ch20-arrow-copy-import-and-portals`).

## [2026-11-10T03:00:00Z] flow-archive | native-oracle-driver

Finished and archived 7 chapters and 22 tasks on `feat/native-oracle-driver` stacked on `feat/native-mssql-driver`. Base alignment checkpoint: `c21a2169`.

Durable synthesis is committed in `database-drivers-and-dialers.md`, `query-pipeline-cdc-and-tenancy.md`, `sqlspec-alchemy-and-dialects.md`, `embeddings-and-vectors.md`, `domains/index.md`, `product.md`, `tech-stack.md`, `README.md`, `AGENTS.md`, the database, extensions, analytics, embeddings, and reference docs, and `specs/native-data-platform/spec.md` before contracting `.agents/bundles/specs/native-oracle-driver/`.

Recorded implementation notes on `feat/native-oracle-driver`:
- `SyncOracleDriver` and `AsyncOracleDriver` raise `MissingDependencyError` with `package` and `install_package` set to `vertebra-core[oracle]` only when the wheel is absent; `make oracle-wheel-check` verifies the contract in an isolated venv.
- `OracleCore.build_session_context_sql` defaults to the `USERENV` namespace (`DBMS_SESSION.SET_IDENTIFIER`); custom namespaces use `DBMS_SESSION.SET_CONTEXT`, and `context_package_proc` routes through a trusted package.
- PGO extension discovery stays outside this flow; the hardcoded `EXTENSION_MANIFESTS` list is unchanged.

## [2026-11-10T02:35:00Z] flow-archive | native-mssql-driver

Completed 6 tasks (`t01`–`t06`) on `feat/native-mssql-driver` stacked on `feat/native-mysql-driver`.

Durable synthesis is committed in `database-drivers-and-dialers.md`, `domains/index.md`, `product.md`, `docs/usage/databases.md`, `docs/usage/extensions.md`, `docs/index.md`, `docs/reference/db.md`, `docs/reference/index.md`, `review-core/references/areas.toml`, and `specs/native-data-platform/spec.md` before contracting `.agents/bundles/specs/native-mssql-driver/`.

Recorded implementation notes on `feat/native-mssql-driver`:
- `SyncMSSQLDriver` and `AsyncMSSQLDriver` raise `MissingDependencyError` only when the `vertebra._mssql` wheel is absent.
- `AsyncMSSQLDriver` owns its own `AsyncMSSQLClient` and `AsyncCloudSQLDialer`; `MSSQLConfig.build_async_driver` resolves the async dialer from the registry.

Functional task commits:
- `native-mssql-driver:t01`: `31b58fc0`.
- `native-mssql-driver:t02`: `6108f4d4`.
- `native-mssql-driver:t03`: `d29591a4`.
- `native-mssql-driver:t04`: `6ccd57a9`.
- `native-mssql-driver:t05`: `d9905a5e`.
- `native-mssql-driver:t06`: `9abef6d2`.

## [2026-11-10T01:45:00Z] flow-archive | native-mysql-driver

Completed 6 tasks (`t01`–`t06`) on `feat/native-mysql-driver` stacked on `feat/native-duckdb-driver`.

Durable synthesis is committed in `database-drivers-and-dialers.md`, `docs/usage/databases.md`, `review-core/references/areas.toml`, and `specs/native-data-platform/spec.md` before contracting `.agents/bundles/specs/native-mysql-driver/`.

Recorded implementation notes on `feat/native-mysql-driver`:
- `vertebra-db` carries no MySQL wire module; `vertebra-mysql` owns packet framing, authentication, and the `BrokerEngineBackend` implementation over `mysql_async`, while `MySQLConfig` keeps rejecting `memory://` DSNs.
- `MySQLConfig.build_sync_driver` / `build_async_driver` resolve driver classes through `resolve_driver_classes()` and fall back to the registry broker, matching PostgreSQL and MSSQL.

## [2026-11-09T21:46:00Z] flow-archive | native-duckdb-driver

Completed 6 tasks (`t01`–`t06`) and documentation sync (`84959389`) on `feat/native-duckdb-driver` stacked on `refactor/packaging-user-only`.

Durable synthesis and `/flow:sync` updates are committed in `database-drivers-and-dialers.md`, `query-pipeline-cdc-and-tenancy.md`, `workflow.md`, `polyglot-test-matrix.md`, `benchmarks-and-free-threaded.md`, `product.md`, `tech-stack.md`, `specs/native-data-platform/spec.md`, `AGENTS.md`, `pyo3-bridge`, `review-pyo3-bridge`, and `pyo3-bridge-{reviewer,specialist}.md` before contracting `.agents/bundles/specs/native-duckdb-driver/`.

Recorded implementation notes on `feat/native-duckdb-driver`:
- DuckDB `CREATE SECRET` DDL rejects `?` prepared parameters, so cloud-agnostic `DuckDBSecretConfig` (`value` + sync/async `value_provider`) renders secret options and scopes as single-quoted SQL literals (`'` doubled, NUL/CR/LF rejected) for `CREATE [OR REPLACE] [PERSISTENT] SECRET`.

Functional task commits:
- `native-duckdb-driver:t01`: `c11f5feb`.
- `native-duckdb-driver:t02`: `67b19241`.
- `native-duckdb-driver:t03`: `698edb63`.
- `native-duckdb-driver:t04`: `e9edef5b`.
- `native-duckdb-driver:t05`: `7c17371b`.
- `native-duckdb-driver:t06`: `aeed1dc7` (docs/OKF checkpoint `84959389`).

## [2026-11-09T20:56:00Z] flow-archive | driver-fail-loud

Completed 13 chapters and 38 tasks on `feat/driver-fail-loud`; final correctness and fresh quality review remediations (`df6e0b8e`, `62a29db7`) have no unresolved findings. Source range `3c2b066a..62a29db7` is preserved in Git. Full lint, Clippy, Mypy, stubtest, Rust `vertebra-db` tests, Python fail-loud/queue/packaging/docs tests, and Zensical documentation gates pass.

Durable synthesis is committed in `database-drivers-and-dialers.md`, `query-pipeline-cdc-and-tenancy.md`, `distributed-queues-and-email.md`, `sqlspec-alchemy-and-dialects.md`, `embeddings-and-vectors.md`, `polyglot-test-matrix.md`, `benchmarks-and-free-threaded.md`, `workflow.md`, `product.md`, `tech-stack.md`, and `adr-002-two-tier-shm-database-and-arrow-data-plane.md` before spec deletion.

Reviewed chapter checkpoints:
- `ch01-oracle-mssql-fail-loud`: `da2913ed`.
- `ch02-duckdb-sqlalchemy-queue-fail-loud`: `67d6bca5`.
- `ch03-remove-memory-fakes`: `ca1736b8`.
- `ch04-redis-valkey-memory-removal`: `7f9c2a6c`.
- `ch05-mssql-codec-cleanup`: `b29fdf87`.
- `ch06-oracle-codec-cleanup`: `92e52f31`.
- `ch07-vertebra-db-pools-and-mysql`: `5ae19501`.
- `ch08-install-hints-and-extras-gate`: `3766bda4`.
- `ch09-conformance-outcomes`: `a5bd15e5`.
- `ch10-queue-backend-fail-loud`: `c0b5b022`.
- `ch11-docs-and-okf`: `6035aff2`.
- `ch12-queue-docs-and-knowledge`: `4d07461c`.
- `ch13-agent-knowledge-and-embeddings-docs`: `b02636d9`.
- Review remediation checkpoints: `df6e0b8e`, `62a29db7`.

## [2026-11-09T02:30:00Z] flow-plan | driver-fail-loud

Approved `driver-fail-loud` at 13 chapters and 38 tasks ahead of the standalone native database driver flows, removing SQLite and `memory://` stand-ins across database drivers and SQLAlchemy DB-API, requiring live or injected collaborators across queue backends, deleting dead `vertebra-db` pools and superseded codecs, aligning `MissingDependencyError` install hints behind an AST extras gate, making conformance skip/fail outcomes explicit, and updating database, queue, and agent docs and OKF knowledge.
## [2026-11-09] plan | Approved native-oracle-driver at 7 chapters and 22 tasks (`7b39ca3b`)

- `native-oracle-driver` (`plan_revision: 3`, `has_chapters: true`, `status: stable`, `state: planned`, `plan_commit: 7b39ca3b`) and `contracts.md` are locked with frontmatter-free `chapters/index.md`, `7` chapter specifications (`ch01`–`ch07`), and `22` vertical-slice 8-section task worksheets (`len(files) <= 3`, `len(read_set) <= 6`, `estimated_loc <= 250`) plus a `[0/6 files documented]` documentation and OKF synthesis plan (`ch07:t03`–`t04`).
- Two rounds of isolated zero-context spec review across `ch01`–`ch07` resolved all `23` Round 1 findings in place (`0 Critical`, `0 Important`, `0 Minor` in Round 2).

## [2026-11-09T02:25:00Z] flow-archive | storage-remediation

Completed 6 chapters and 20 tasks on the feature branch; final correctness and fresh quality review `c8c8998a` have no unresolved findings. Source range `077a3fe9..c8c8998a` is preserved in Git. Full lint, locked Rust workspace, Python (2795 passed, 7 skipped), all-five native rebuild/stubtest, and documentation gates pass.

Durable synthesis is committed in `object-storage.md`, `object-storage-limitations.md`, and `product.md` before spec deletion.

Reviewed chapter checkpoints:
- `ch01-native-gcs-plane-and-signing`: `3842b7d0`.
- `ch02-zero-copy-ffi-keys-and-backends`: `942336d7`.
- `ch03-bulk-file-io-compose-and-streams`: `c854444e`.
- `ch04-streaming-arrow-parquet-and-shm`: `13400785`.
- `ch05-framework-adapters-and-test-hygiene`: `3b1a917b`.
- `ch06-docs-okf-and-plan-sync`: `c8c8998a`.

Functional task commits:
- `storage-remediation:ch01:t01`: `8bdb0e97`.
- `storage-remediation:ch01:t02`: `1ece46f3`.
- `storage-remediation:ch01:t03`: `b87c7660`.
- `storage-remediation:ch01:t04`: `0026b671`.
- `storage-remediation:ch02:t01`: `59dc50ff`.
- `storage-remediation:ch02:t02`: `45a7b1f1`.
- `storage-remediation:ch02:t03`: `b010bdf4`.
- `storage-remediation:ch02:t04`: `769e0af6`.
- `storage-remediation:ch03:t01`: `db6562f4`.
- `storage-remediation:ch03:t02`: `4d1783e3`.
- `storage-remediation:ch03:t03`: `ce4a50c7`.
- `storage-remediation:ch03:t04`: `3e49bb38`.
- `storage-remediation:ch04:t01`: `54766b65`.
- `storage-remediation:ch04:t02`: `d965d501`.
- `storage-remediation:ch04:t03`: `e37b8b32`.
- `storage-remediation:ch05:t01`: `4fcbd19a`.
- `storage-remediation:ch05:t02`: `6a4599de`.
- `storage-remediation:ch05:t03`: `71f11476`.
- `storage-remediation:ch06:t01`: `aefe786a`.
- `storage-remediation:ch06:t02`: `6a27e8ff`.

## [2026-11-08T18:55:28.220570Z] flow-archive | storage-completeness

Completed 5 chapters and 15 tasks on the feature branch; final correctness and fresh quality review `77c6acfa` have no unresolved findings. Source range `9af810f5..77c6acfa` is preserved in Git. Full lint, locked Rust workspace, Python (2608 passed, 74 skipped, 170 xfailed), all-five native rebuild/stubtest, and documentation gates pass. Offline native/framework/provider examples and 29 documentation checks pass; live cloud credential calls are not executed.

Durable synthesis is committed in `3ecd1288` across storage/framework domains, FFI architecture, codec patterns, test recipes, ADR-004, and ADR-005 before spec deletion.

Reviewed chapter checkpoints:
- `ch01-native-engine-and-auth`: `0b3d32ea`.
- `ch02-orm-and-sql-storage-contracts`: `b2911b06`.
- `ch03-litestar-and-range-streaming`: `043d9837`.
- `ch04-arrow-shm-and-docs`: `e966025e`.
- `ch05-sqlspec-arrow-and-docs`: `77c6acfa`.

Functional task commits:
- `storage-completeness:ch01:t01`: `7f414f10`.
- `storage-completeness:ch01:t02`: `6267a775`.
- `storage-completeness:ch01:t03`: `2794c4ea`.
- `storage-completeness:ch01:t04`: `0b3d32ea`.
- `storage-completeness:ch02:t01`: `aa8e53ffd58739520c11ca7a2c3e012867f660d9`.
- `storage-completeness:ch02:t02`: `2208b021dc31bdf120b3a3c1fae84c1a2b4e6c6a`.
- `storage-completeness:ch02:t03`: `593632437901e76ea3a487dfa5ba519061a14a8c`.
- `storage-completeness:ch03:t01`: `0220958d`.
- `storage-completeness:ch03:t02`: `11f403fe609e62a73d8b0036d444c1268ac2eb1d`.
- `storage-completeness:ch04:t01`: `c8fc3521`.
- `storage-completeness:ch04:t02`: `e828a361`.
- `storage-completeness:ch04:t03`: `5b1bbc95`.
- `storage-completeness:ch04:t04`: `a8eaf2fa`.
- `storage-completeness:ch05:t01`: `26f8cfcd`.
- `storage-completeness:ch05:t02`: `3ecd1288`.

Remaining storage work: none. Delivery/push is owned by the root coordinator.

## [2026-11-08T18:53:48.034303Z] flow-finish | storage-completeness

All 5 chapters and 15 tasks are complete; correctness and fresh quality reviews pass at `77c6acfa` with no unresolved findings. Full lint, locked Rust workspace, Python (`2608` passed, `74` skipped, `170` xfailed), all-five rebuild/stubtest, and docs gates pass. Native and extensions synthesis plus ADR-005 are committed before archive; no implementation remains. Live cloud credential calls are outside local validation. Changes remain on the signed feature branch for root delivery.

## [2026-11-08] flow-implement | Verified PyArrow sentinel parity (`26f8cfcd`)

- Source `26f8cfcd` preserves omitted, `None`, and `-1` row-group defaults using installed PyArrow as the test oracle across sync/async Table and cursor writers. Invalid `0`/`-2` values remain rejected. The canonical suite passes `106` cases; full lint exits `0` and Python exits `0` (`2608` passed, `74` skipped, `170` xfailed, `17` warnings in `72.79s`). Correctness recheck passes; fresh quality review precedes finish/archive.

## [2026-11-08] flow-implement | Verified SQLSpec optional row-group compatibility (`058da28c`)

- Source `058da28c` preserves installed PyArrow row-group defaults for omitted and `None` options across sync/async Table and cursor writes, while invalid sizes still fail. Eight Red failures lead to `102` canonical passes with actual metadata assertions. Full `lint-all` exits `0`; refreshed Python suite exits `0` (`2604` passed, `74` skipped, `170` xfailed, `17` warnings in `71.51s`). Catalog counts are synchronized; final correctness and fresh quality reviews precede finish/archive.

## [2026-11-08] flow-implement | Completed storage documentation and durable synthesis (`3ecd1288`)

- Source `3ecd1288` completes both user guides and source-backed native/storage/framework knowledge, including ADR-005 for persistent TTL envelopes. Documentation review/build gates and `29` tooling/viewport cases pass; offline native/provider/framework examples pass. Full Python exits `0` with `2592` passed, `74` skipped, `170` xfailed; full lint and locked native workspace gates pass. All `15` implementation tasks and `5` documentation targets are complete; chapter 5 independent reviews precede finish/archive. No live GCS ADC/IAM calls are executed.

## [2026-11-08] flow-implement | Verified SQLSpec bounded Arrow consumers (`a1ee9042`)

- Source `a1ee9042` preserves public `Table`/`None`/iterator contracts while using bounded native async codecs, async pattern matching, raw IPC buffers, format options, and owned inner iterator cleanup. Four Red failures lead to `90` canonical integration passes, including `25` new public cases. Storage/adapter Mypy, Ruff, format, and diff checks pass. Only the final storage/auth documentation task remains.

## [2026-11-08] chapter-reviewed | Completed strict Arrow and bounded storage chapter (`e966025e`)

- Correctness and fresh quality reviews pass at `e966025e` with no unresolved findings. All four chapter tasks are complete, including real S3 minimum-part completion and repeated-cancellation ownership. Full lint, locked Rust workspace, Python (`2567` passed, `74` skipped, `170` xfailed), and documentation gates pass. Chapter `ch05` starts SQLSpec delegation; its final two tasks complete the original documentation scope.

## [2026-11-08] flow-implement | Verified repeated-cancellation codec ownership (`e828a361`)

- Source `e828a361` retains worker and loop-task ownership through repeated task cancellation. Four Red failures lead to `123` canonical store passes, including six real producer-barrier cases across both formats. Focused quality gates pass. Full `lint-all` exits `0`; the refreshed Python suite exits `0` (`2567` passed, `74` skipped, `170` xfailed, `17` warnings in `74.13s`). Native and documentation gates remain valid. Python correctness recheck precedes fresh quality review.

## [2026-11-08] flow-implement | Verified native S3 minimum-part completion (`a8eaf2fa`)

- Source `a8eaf2fa` repairs `8` real S3 Complete failures by applying the existing `8 MiB` native accumulator to normalized S3/B2. Locked native `3` unit + `22` integration tests, `119` canonical Python store cases, all-five rebuild/stubtest, and focused quality gates pass; lockfiles are unchanged. Full locked Rust workspace exits `0`; full Python passes `2563` cases with `74` skips and `170` xfails; docs exits `0`. Chapter `ch04` has `4` completed tasks awaiting correctness and fresh quality review. Two SQLSpec/documentation tasks remain.

## [2026-11-08] flow-implement | Synchronize bounded storage codecs

- Source `5b1bbc95` implements bounded sync/async IPC and Parquet writers. Two public-format Red failures lead to `111` passing canonical storage cases, including `44` writer cases, real native S3 PUT cancellation/abort barriers for both formats, and worker-exit ownership checks. Storage Mypy (`4` files), focused Ruff, format, and whitespace gates pass.
- Storage knowledge documents strict named allocation, real IPC, bounded demand, caller-loop native I/O, per-step `SyncPool` codecs, cancellation ownership, and explicit Parquet prefetch opt-in. Chapter `ch04` has `3/4` tasks complete and is active on a source-grounded S3 minimum-part worksheet. Root progress is `12/15` tasks; native S3 compliance and two SQLSpec/documentation tasks remain. Consumer-guide progress remains `3/5`.

## [2026-11-08] flow-implement | Verified bounded Arrow and Parquet readers (`fe3b44bd`)

- Source `fe3b44bd` adds native sync/async batch readers and schema-preserving full-`Table` helpers, including IPC-file fallback. Four Red cases lead to `67` passing canonical store cases with `18` new codec cases and `4` real native S3 HTTP cancellation/responsiveness barriers. Focused Mypy, Ruff, format, and diff gates pass. Chapter `ch04` executes its writer worksheet; three tasks remain across `ch04`/`ch05`.

## [2026-11-08] flow-implement | Verified strict named Arrow SHM allocation (`c8fc3521`)

- Source `c8fc3521` removes native/Python anonymous allocation fallback and JSON pseudo-IPC from threshold-selected `ArrowCursor` construction. Red reproduces native invalid-name success and three Python failures. Locked Rust `37` unit + `2` stream tests and Clippy pass; all five extensions rebuild, `114` Python cases pass with `5` existing `psycopg` skips, and five-extension stubtest passes. `Cargo.lock` remains unchanged. Chapter `ch04` now executes bounded batch readers; four tasks remain.

## [2026-11-08] chapter-reviewed | Completed storage HTTP and Litestar chapter (`043d9837`)

- Correctness and fresh quality reviews pass at `043d9837` with no unresolved findings. Native FastPath `200`/`206`/`416`, valid ETags, persistent TTL envelopes, namespace deletion, and actual Litestar static consumers are covered. The broad Python gate passes `654` tests, full Mypy covers `419` source files, and docs build passes. Chapter `ch04` starts the strict Arrow SHM worksheet; five storage tasks remain across `ch04` and `ch05`.

## [2026-11-08] review | Repair storage HTTP range metadata and entity tags

- Source `0220958d` preserves direct async range status/Content-Range and formats HTTP entity tags; `ba4c9526` aligns the existing consumer assertion and collapses a boolean-equivalent provider branch for the lint gate.
- Fresh verification passes: 71 Range/storage canonical tests, 654 extension/storage/server cases, full MyPy 419 files, and touched-path Ruff. Chapter correctness recheck and fresh quality review follow this synchronized evidence.

## [2026-11-08] flow-implement | Record storage ch03 Litestar consumers

- Source `11f403fe` implements public object-store Store/static adapters with persistent TTL envelopes, hierarchical namespace deletion, and native escaped-path/cursor reuse. Seven Red cases lead to 21 canonical and 634 extension/storage Green cases; focused MyPy, Ruff, and format checks pass.
- Chapter `ch03` has two completed tasks and remains active pending independent correctness and fresh quality reviews. Root progress is `9/14` tasks and `3/5` documentation targets; five Arrow/storage/SQLSpec/documentation tasks remain across `ch04` and `ch05`. Source commits remain local.

## [2026-11-08] revise | Ground remaining storage streaming contracts

- Range source `7262460c` and native consumer proof `d594ac85` pass 51 canonical cases, Ruff, and storage MyPy. Active task advances to Litestar integration with persistent TTL envelopes and installed filesystem contracts.
- Revision `9` splits original Arrow/storage scope into bounded strict-allocation, read-bridge, write-bridge, SQLSpec integration, and documentation tasks across `ch04` and `ch05`; no original deliverable is omitted. PyArrow codec work uses existing SyncPool with native async I/O on the caller loop; default Parquet batch prefetch is disabled.

## [2026-11-08] chapter-reviewed | Close storage-completeness ch02 at b2911b06

- All three storage integration tasks are complete. Correctness and fresh quality reviews pass at `b2911b06` after native-key file reopening, provider-config migration serialization, and worksheet-note cleanup.
- Verification passes: `136` canonical tests, `592` extension/storage tests, full MyPy (`419` files), Ruff/format, lock validation, documentation build/artifacts, and all five native extension imports.
- The active pointer advances to `ch03-litestar-and-range-streaming` (`storage-completeness:ch03:t01`); `ch03` and `ch04` retain five pending tasks. Commits remain local.

## [2026-11-08] flow-implement | Bind Django file reopening and migration serialization to native storage

- Correctness review identified two Django consumer failures. Source fix `59363243` binds closed-file reopening to the configured native store/key and registers public migration serializers only for the eight owned `StorageConfig` types, preserving public constructors and every field. Real `FieldFile` consumers and async-opened handles retain native content despite colliding local filenames; `MigrationWriter` round trips local/memory storage and provider configuration options.
- Task receipt `91d7942d` records the completed repair under revision `7` at `ecccd498`. Fresh verification passes `136` canonical tests, `592` extension/storage tests, full configured MyPy (`419` files), Ruff checks/format, and the documentation build. Independent correctness recheck passes the original reproductions and canonical tests; the coordinator's focused reproduction gate passes `11` tests.
- Storage knowledge and root/ch02 snapshots reflect the repaired contracts. Documentation coverage remains root `[2/4 files documented]` and chapter `[2/2 files documented in ch02]`; the chapter stays active for final correctness and fresh quality review.

## [2026-11-08] review | Repair Django file reuse and migration serialization

- Correctness review of `34368ff5..ecccd498` reproduces two Important Django consumer gaps: plain File reopening can resolve a process-local filename, and captured provider config objects fail MigrationWriter serialization. Revision `7` reopens `ch02:t03` at `ecccd498` for native-backed reopen and supported serialization of owned config types, with real consumer regressions.

## [2026-11-08] flow-implement | Completed storage-completeness Chapter 02 task implementation

- SQLSpec `ObjectStoreBackend` (`aa8e53ff`) implements exact protocol methods, native buffer streaming, conditional writes, opaque pagination, seekable readers, shared handles, and traversal refusal. Advanced Alchemy `ObjectStoreBackend` (`2208b021`) uses real registry registration and bounded native `FileObject` uploads without monkeypatching or implicit memory targets.
- Django `DjangoStorage` (`5820090c`, runtime annotation fix `fb3dbccc`, task receipt `43a61a56`) subclasses deconstructible Django `Storage`, returns seekable `File` handles, implements sync/async persistence, Django naming/collisions, directory/path/URL contracts, and aware UTC modification time; unsupported creation/access timestamps raise `NotImplementedError`. `django-stubs>=6.1.2` is a lint development dependency.
- Fresh coordinator verification against final source `fb3dbccc`: `UV_NO_SYNC=1 uv run --no-sync pytest src/py/tests/unit/extensions src/py/tests/unit/storage -q --disable-warnings` passes `581` tests (`17` warnings); touched-file Ruff check/format, full configured MyPy (`419` files), `uv lock --check`, whitespace checks, all five native extension imports, and Django runtime annotation introspection pass. `UV_NO_SYNC=1 make docs` passes docstring hygiene, CLI reference verification, Zensical build, postbuild, and artifact checks.
- Storage knowledge and the technology matrix are synchronized (`ch02` `[2/2 files documented]`, root `[2/4 files documented]`). Root/chapter/catalog snapshots show `7/12` tasks completed and `5` planned downstream tasks. Chapter `ch02` remains active pending correctness and fresh quality review; its pointer and review stamp remain unchanged.

## [2026-11-08] revise | Type-check Django storage against upstream stubs

- Active storage `ch02` and `t03` share revision `6` at `749fa2bc`. Task `t03` owns its adapter plus `pyproject.toml` and `uv.lock`, adding `django-stubs` to lint dependencies so MyPy checks the real Django base. Its bounded source manifest remains six files.

## [2026-11-08] revise | Report only available Django storage timestamps

- Storage `ch02` and pending task `t03` share revision `5` at `749fa2bc`. Native `ObjectMeta` exposes modification time only; Django creation/access hooks raise `NotImplementedError` rather than substituting modification time. Completed task evidence remains unchanged.

## [2026-11-08] revise | Preserve native storage reader metadata timing

- Storage `ch02` and its three worksheets share revision `4` at plan baseline `34368ff5`. SQLSpec reader factories retain native eager sync metadata lookup when size is omitted and lazy async reads; this correction requires no native facade changes.

## [2026-11-08] revise | Align storage integration worksheets with installed framework contracts

- [`storage-completeness`](knowledge/domains/storage/object-storage.md), `ch02-orm-and-sql-storage-contracts`, and its three pending worksheets share revision `3` anchored at `aba30976`. The completed `ch01` review stamp `0b3d32ea` and all four task commits remain unchanged and ancestry-verified.
- Installed source evidence establishes SQLSpec `0.71.1` exact `_sync` / `_async` protocol names, Advanced Alchemy `1.12.0` concrete `ObjectStoreBackend` / upstream `get_content` methods, and Django `6.1.3` `Storage.save` / `File` contracts. The blueprints declare explicit native adapter extensions, callable omission errors, public registry registration, and non-blocking async adapter entry points.
- Baseline collection covers `15` canonical storage/Django tests; the coordinator's fresh extension-suite baseline passes `421` tests. This revision changes planning Markdown only and records no implementation acceptance.

## [2026-11-07] flow-implement | Implemented storage-completeness Chapter 01 (ch01-native-engine-and-auth)

- Completed all 4 vertical-slice tasks in `storage-completeness` `ch01-native-engine-and-auth` on `feat/storage-updates` above `23041dfb` and synchronized [`object-storage.md`](knowledge/domains/storage/object-storage.md):
  - **Rust Streaming, Incremental Multipart, Conditional Puts, Pagination & Rename (`ch01:t01`, `7f414f10`)**: Extended `StoredObjectMeta` with `content_type` and `attributes`, added `PutConditionalOptions` (`if_not_exists`, `if_match`, `if_none_match`), `StreamReader` (`get_stream`), stateful `MultipartUploadSession` (uniform `8 MiB` R2/OCI part buffering without whole-payload `Vec<Bytes>` / `Vec<u8>` concatenation), `list_page`, `list_with_delimiter`, and `rename` in `vertebra-storage`.
  - **Python Streaming Facade & `_storage.pyi` Stubs (`ch01:t02`, `6267a775`)**: Wired `SyncStreamReader`, `AsyncStreamReader`, `SyncMultipartUpload`, `AsyncMultipartUpload`, `get_stream`, `put_stream` (with automatic `abort()` on producer failure), conditional `put`, `list_page`, `list_with_delimiter`, `rename`, and streaming `SyncReadableFile.__iter__` / `AsyncReadableFile.__aiter__` across `vertebra._storage` and `vertebra.storage`.
  - **Deduplicated Rust GCS Auth Wrapper & Multi-Cloud Presigning (`ch01:t03`, `2794c4ea`)**: Trimmed `vertebra-storage/src/gcp_auth.rs` to `GCS_OAUTH_SCOPES` and `RefreshingGcpCredentialProvider` over `vertebra_core::gcp_auth::GcpTokenCache`, enforced `1..=604_800s` expiry bounds and RFC 3986 path encoding across GCS V4 (`GOOG4-RSA-SHA256` + IAM `signBlob`) and S3 SigV4, and added native OCI PAR presigning (`sign_oci_par_url`).
  - **Unified `vertebra._vertebra` Token Caches & Fail-Closed Facade (`ch01:t04`, `613783bd`, `699fb9ce`, `0b3d32ea`)**: Removed duplicate `SyncGcpTokenCache`, `AsyncGcpTokenCache`, and `GcpTokenSnapshot` PyO3 classes from `vertebra._storage`, re-exported `vertebra._vertebra` token caches (with ambient ADC precedence/isolation, `GoogleCloudAuthError` handling, and `.to_async()` / `.to_sync()` shared-`Arc` conversion parity), deleted pure-Python `urllib` OAuth fetchers and `self._memory` fallbacks, shared native stores via `.to_async()` across Django/Alchemy/SQLSpec adapters, enforced `ImproperConfigurationError` / `MissingDependencyError` (`vertebra-core[storage]`), and passed zero-context `@code-reviewer` and `@quality-reviewer` closeout gates across `23041dfb..0b3d32ea`.

## [2026-11-07] flow-setup | Aligned OKF v0.2 knowledge, skills, subagents, and chaptered storage-completeness spec

- Executed `/flow:setup` (`Align & True-Up`, `shared` Git tracking policy) on `feat/storage-updates` (`HEAD @ 9af810f5`, `main @ 077a3fe9`):
  - **OKF v0.2 Knowledge & Product Resynthesis**: Re-anchored `verified[].commit` (`9af810f5`) across all `product/*` and `knowledge/**` files (replacing pre-squash SHAs orphaned by squash merges `#80` `2b737240`, `#92` `d2fa5072`, and `#94` `077a3fe9`), split `database-drivers-and-dialers.md` into [`database-drivers-and-dialers.md`](knowledge/domains/data/database-drivers-and-dialers.md) and [`gcp-bigquery-and-spanner-drivers.md`](knowledge/domains/data/gcp-bigquery-and-spanner-drivers.md) so every chapter stays `<= 200` lines, tightened `pyo3-and-python-runtime.md`, moved all `<!-- truth: start -->` bullet blocks immediately after `# H1`, standardized `type: Decision` across `decisions/adr-001..004`, and updated `workflow.md` (`make docs-check`, `make docs-clean`, `make docs-rust`, `make bench-report`) and root `AGENTS.md` (`<= 100` lines).
  - **Skills, Paired Subagents & Local Harness Cleanup**: Synchronized `.agents/skills/review-core/references/{areas.toml,checklist.md,false-positive-suppressions.md}`, all 5 engineering domain skills, and all 8 review skills with live `src/` paths and `--manifest-path src/rs/Cargo.toml` commands; aligned all 16 `.agents/agents/*-{reviewer,specialist}.md` definitions to satisfy both `tools.scripts.review_setup` and Flow `validate.md` (`skills:` + `preloadSkills:`, 4 Flow H1 sections + 4 `review_setup` H2 subsections, `commandExecutionPolicy: sandbox`); removed obsolete `.claude/{skills,agents,commands}` directories that shadowed `.agents/`; and non-destructively merged current `Makefile` allow-rules into `.claude/settings.local.json` (backup at `.claude/settings.local.json.bak`).
  - **Active Specs Normalization & `storage-completeness` Chapterization**: Reconciled `#92` (`d2fa5072`) and `#94` (`077a3fe9`) completion state in Master PRDs `integration-remediation` and `native-data-platform`; migrated `storage-completeness` from flat `t01`–`t07` tasks into 4 chapters (`ch01-native-engine-and-auth`, `ch02-orm-and-sql-storage-contracts`, `ch03-litestar-and-range-streaming`, `ch04-arrow-shm-and-docs`) and 12 8-section task blueprints (`files <= 3`, `read_set <= 6`, `estimated_loc <= 250`); and normalized OKF v0.2 frontmatter, truth blocks, `sources` footnotes, and `9af810f5` commit stamps across `native-postgres-driver` (`t01`–`t102`), all 10 `draft` child flow charters, and all 6 spec research briefs.

## [2026-11-06] flow-archive | Completed and Archived MCP 2026-07-28 Core and WebMCP Bridge (`mcp-webmcp-core`)

- Completed, reviewed (`6b819b6d..d4a5d1c1`), and archived Flow `mcp-webmcp-core` (`integration-remediation` Chapter 7, 8 tasks across `vertebra.agent`, `vertebra.extensions`, `vertebra.build`, `@vertebra/client`, `docs/`, and `.agents/bundles/`):
  - **`ToolSpec`, `ToolAnnotations` & `msgspec.defstruct` Schema Builder (`t01`, `14ee1e3e`)**: Added frozen `ToolSpec` and `ToolAnnotations` structs (`src/py/vertebra/agent/core/schema.py`), replaced custom recursive JSON Schema builders and `eval()` string resolution with `typing.get_type_hints(fn, include_extras=True)` -> `msgspec.defstruct` -> `msgspec.json.schema_components`, coerced tool arguments via `msgspec.convert`, and defaulted `ToolSpec.exposure` to `frozenset({"mcp", "a2a"})`.
  - **Unified `ToolRegistry` Over `ToolProvider`, `@tool`, `SkillController` & Opted-In Routes (`t02`, `376d3faf`)**: Consolidated tool registration into `ToolRegistry` (`MCPRegistry`, `src/py/vertebra/agent/mcp/registry.py`) with `register_provider(provider, *, prefix="", exposure=None, guards=())`, `register_tool`, `register_controller`, `register_route_handler`, `discover_app_routes`, and `list_specs(exposure=...)`.
  - **Transport-Agnostic `MCPCore` Enforcing MCP `2026-07-28` HTTP Semantics (`t03`, `5a4d46e4`)**: Implemented `MCPCore.handle(McpHttpRequest) -> McpHttpResponse` (`src/py/vertebra/agent/mcp/service.py`) enforcing `Origin` validation (`403`), `GET /mcp` -> `405` (`Allow: POST, OPTIONS`), `OPTIONS /mcp` -> `204`, JSON-RPC notifications -> `202`, JSON-RPC batch arrays -> `400` (`-32600`), `MCP-Protocol-Version` (`-32022`) and `Mcp-Method`/`Mcp-Name`/`Mcp-Param-*` (`-32020`) header checks, `-32601` on removed `2024-11-05` methods (`initialize`, `ping`, `logging/setLevel`, `resources/subscribe|unsubscribe`, `tasks/result`), `server/discover`, `resultType`, `ttlMs`/`cacheScope`, `ETag` `304`, `authorize(ctx, spec)`, and `ConfirmationGate` (`-32003`).
  - **Shared `AsgiMcpApp` & `WsgiMcpApp` Across Starlette, FastAPI, Flask & Django (`t04`, `23e7e926`)**: Added `AsgiMcpApp` and `WsgiMcpApp` (`PortalProvider.default()` persistent background portal, `src/py/vertebra/agent/mcp/routes.py`) and replaced legacy `2024-11-05` JSON-RPC dispatchers in `vertebra.extensions.{starlette,fastapi,flask,django}`.
  - **`AgentPlugin` Wiring, `litestar-mcp` Delegation & Canonical `/.well-known/mcp.json` (`t05`, `10143439`)**: Wired `AgentPlugin` (`src/py/vertebra/agent/plugin.py`) to `MCPCore` on native `App`, delegated `/mcp` to `litestar-mcp` when `LitestarMCP` is installed without duplicating `opt["mcp_tool"]` handlers, unified `/.well-known/mcp.json` on `build_well_known_mcp_document`, and projected `ToolSpec` entries (`"a2a" in exposure`) into `AgentCard.skills`.
  - **Server-Side `build_webmcp_manifest` Over `ToolSpec` (`t06`, `f4ed07da`)**: Updated `build_webmcp_manifest` (`src/py/vertebra/agent/mcp/webmcp.py`) to project `WebMCPToolDescriptor` strictly from `registry.list_specs(exposure="webmcp")`, populating `title`, `output_schema` (`outputSchema`), and `annotations["consequentialHint"] = True` when `requires_confirmation=True`.
  - **TypeScript `@vertebra/client/webmcp` `document.modelContext` Bridge (`t07`, `f8b6f844`)**: Upgraded `src/ts/vertebra-client/src/webmcp.ts` to resolve `document.modelContext ?? navigator.modelContext`, call `modelContext.registerTool(tool, { signal })` (removing legacy `provideContext`), return `WebMCPRegistrationHandle` (`tools`, `errors`, `dispose()`), and isolate per-tool registration errors and HMR re-registration.
  - **`McpClient` SSE/`_meta`, Dynamic Client Subpaths & `2026-07-28` Docs/OKF (`t08`, `d4a5d1c1`)**: Updated `McpClient` (`src/ts/vertebra-client/src/agent/index.ts`) to send `Accept: application/json, text/event-stream`, parse `text/event-stream` JSON-RPC frames, attach `2026-07-28` `_meta`, and base64url-encode non-ASCII/control header values (`encodeMcpHeaderValue`); replaced hardcoded `_CLIENT_SUBPATHS` in `src/py/vertebra/build/_import_map.py` with `load_client_subpaths()`; fixed `objects.inv` URLs and staged cache in `zensical.toml`/`Makefile`/`scan_public_docstrings.py`; and synchronized `docs/` and OKF chapters (`mcp-and-a2a-protocol-harness.md`, `client-and-adapter-subpaths.md`).

## [2026-11-06] flow-archive | Completed and Archived Runtime Structure and Throughput (`runtime-structure-and-throughput`)

- Completed, reviewed (`ebbb7fb0..c7b62a8c`), and archived Flow `runtime-structure-and-throughput` (`integration-remediation` Chapter 8, 8 tasks across `vertebra.server`, `vertebra.extensions`, `vertebra.framework`, `vertebra.embeddings`, `vertebra.db.adapters.oracle`, `vertebra.utils`, `vertebra-core`, `vertebra-py`, `vertebra-bigquery`, `vertebra-spanner`, `docs/`, and `.agents/bundles/`):
  - **Shared FastPath Core & `FastPathProvider` Registry (`t01`, `7cafc05a`)**: Consolidated duplicated ASGI/WSGI fastpath engines across Starlette, FastAPI, Litestar, Flask, and Django into `src/py/vertebra/server/_fastpath_core.py` behind the runtime-checkable `FastPathProvider` protocol (`register_fastpath_provider`, `resolve_fastpath`), leaving thin framework adapters.
  - **Bounded Request Body Buffering & Zero-Copy Arrow/Header Preservation (`t02`, `393756df`)**: Enforced `max_body_bytes` (`_FastPathBodyTooLargeError` -> HTTP `413 Payload Too Large`) in `_read_body`, preserved raw `ArrowStreamConvertible` (`__arrow_c_stream__`) returns without dict materialization, and passed raw `bytes` header pairs through `_FastPathResponse` without intermediate UTF-8 round-trips.
  - **Table-Driven Event Loop Selection & Packaged Welcome Assets (`t03`, `5d1b91fe`)**: Replaced `_loops.py` branching with an ordered `_LOOP_BACKENDS` table of `_LoopBackendSpec` structs (`install_best_event_loop`, `resolve_event_loop_policy`) and extracted inline welcome HTML/CSS/SVG literals into `src/py/vertebra/framework/_welcome_assets/` loaded via `importlib.resources.files` (`functools.lru_cache(maxsize=1)`).
  - **Shared Rust Protobuf Wire Helpers & `async_bridge.rs` Rename (`t04`, `81a0e2f1`)**: Extracted `vertebra_core::proto` (`encode_varint`, `decode_varint`, `encode_tag`, `encode_bytes_field`, `encode_string_field`, `encode_message_field`, `encode_uint32_field`, `encode_int64_field`, `encode_bool_field`, `decode_packed_f32_into`), re-exported across `vertebra-http` and `vertebra-py`, and renamed `src/rs/vertebra-py/src/future.rs` to `src/rs/vertebra-py/src/async_bridge.rs`.
  - **GCP Crate Modularization & Shared `py_async` / Staging Helpers (`t05`, `7bf797a6`)**: Split `vertebra-spanner` (`client.rs`, `params.rs`, `arrow_decode.rs`, `spangres.rs`, `admin.rs`) and `vertebra-bigquery` (`client.rs`, `read_session.rs`, `write_stream.rs`, `rest.rs`, `arrow_plan.rs`) into focused submodules, routed both PyO3 crates through `vertebra_db::py_async::{SyncCompletionQueue, AsyncCompletionQueue, spawn_rust_db_task}`, and unified BigQuery/Spanner staging stores on `vertebra.db._staging.build_staging_store`.
  - **Zero-Copy BigQuery Arrow Append Slicing & Reduced RPC Clones (`t06`, `77b54514`)**: Changed `ArrowAppendPlan::build(payload: Bytes)` (`src/rs/vertebra-bigquery/src/arrow_plan.rs`) to slice `serialized_schema` and `record_batches` via `Bytes::slice(range)` with zero heap copies (`serialized_schema.as_ptr()` and `record_batches[i].as_ptr()` lie within `payload.as_ptr()..payload.as_ptr() + payload.len()`) and reduced redundant clones in BigQuery and Spanner client hot paths.
  - **Rust HTTP/2 Vertex Transport, Bounded `EmbeddingBroker` & Batch Oracle Pushdown (`t07`, `2b66400d`)**: Routed `VertexEmbedder` (`src/py/vertebra/embeddings/_google.py`) through `gcp_http2_post` / `gcp_http2_post_async` (`src/rs/vertebra-py/src/gcp_auth.rs`) with `SyncGcpTokenCache` / `AsyncGcpTokenCache` and a persistent background portal in `Embedder._run_sync`, added `max_pending_requests = 1024` queue-cap backpressure and `asyncio.to_thread` offload to `EmbeddingBroker` (`max_batch_tokens = 16_384`), and batched `OracleEmbeddingPushdown` through `DBMS_VECTOR.UTL_TO_EMBEDDINGS` (`VECTOR_ARRAY_T`).
  - **Mandatory Rust `vertebra.utils` Primitives & Docs/OKF Sync (`t08`, `c7b62a8c`)**: Removed pure-Python `_py_*` fallback implementations from `src/py/vertebra/utils/{text,humanize,uuids}.py` in favor of mandatory `vertebra._vertebra` exports, cached negative `importlib.util.find_spec` misses in `module_loader.py`, and synchronized `docs/` and 5 OKF knowledge chapters (`embeddings-and-vectors.md`, `database-drivers-and-dialers.md`, `hot-path-and-async-dispatch.md`, `serialization-and-utils.md`, `ffi-and-gil-discipline.md`).

## [2026-11-06] flow-setup | Aligned OKF v0.2 bundle indexes, subagent sandbox policy, workflow, and active spec continuity

- Executed `/flow:setup` (`Align & True-Up`, `shared` Git tracking policy) on `feat/runtime-structure-and-throughput`:
  - Stripped YAML frontmatter from all 9 subdirectory `index.md` files (`.agents/bundles/product/index.md`, `.agents/bundles/knowledge/index.md`, `.agents/bundles/knowledge/{architecture,decisions,domains,gotchas,patterns,testing}/index.md`, `.agents/bundles/specs/index.md`) while keeping `okf_version: "0.2"`, `bundle: "vertebra"`, and `type: Index` strictly on root `.agents/bundles/index.md`.
  - Removed standalone `---` body horizontal rules across `.agents/bundles/index.md`, `.agents/bundles/knowledge/index.md`, `.agents/bundles/knowledge/domains/agent/embeddings-and-vectors.md`, and `.agents/bundles/log.md`, and extended `ReviewSetupValidator._check_bundles` (`tools/scripts/review_setup.py`) to enforce frontmatter-free subdirectory indexes and rule-free Markdown bodies.
  - Set `commandExecutionPolicy: sandbox` across all 16 reviewer and specialist subagents in `.agents/agents/*-{reviewer,specialist}.md`.
  - Synchronized `.agents/bundles/knowledge/workflow.md`, `AGENTS.md`, `.agents/bundles/index.md`, `.agents/bundles/specs/index.md`, and `runtime-structure-and-throughput` (`spec.md` `state: active` and landed commit SHAs on completed tasks `t01`–`t06`).

## [2026-11-06] flow-review & flow-revise | Post-merge remediation, 0.3.0 docs/OKF sync, per-engine charter split, and Wave 1 plan refinement

- Audited squash-merged `main` (`80ff8a65`, folding [#71](https://github.com/cofin/vertebra/pull/71) `364c7cbe`, [#72](https://github.com/cofin/vertebra/pull/72) `984ce732`, [#73](https://github.com/cofin/vertebra/pull/73) `abd33ab6`, [#75](https://github.com/cofin/vertebra/pull/75) `f7417015`, [#76](https://github.com/cofin/vertebra/pull/76) `21c99f95`, and [#77](https://github.com/cofin/vertebra/pull/77) `80ff8a65`) and remediated findings on `fix/post-merge-review-and-plan-sync`:
  - **Rust SHM, Async Completion & GCP Error Hardening (`e2b282b7`)**: Checked overflow arithmetic in `shm.rs` and `arrow_shm.rs`, added `// SAFETY:` justifications on `arrow_shm.rs` `unsafe` blocks, aborted spawned Tokio workers on Python setup failure in `vertebra-db/src/py_async.rs` and `vertebra-storage/src/lib.rs`, parsed multi-certificate PEM bundles via `Certificate::from_pem_bundle` in `gcp_auth.rs`, mapped `StorageError` to `StorageBuffer` views, and inherited GCP errors from canonical `DatabaseError` subclasses.
  - **Optional Wheel Isolation, Bundler Workspace & PGO Sync (`f7740207`)**: Registered `vertebra-bundler` in `pyproject.toml`/`uv.lock`, excluded Python source/stub cross-contamination in optional `src/rs/vertebra-*/pyproject.toml` manifests, synchronized PGO probe targets, and aligned `.github/workflows/{ci.yml,release.yaml}` and `Makefile`.
  - **Confirmation Binding, Identifier Validation & Async Driver Safety (`7657e8b7`)**: Enforced argument-digest confirmation tokens in `ConfirmationGate`, SQL identifier validation across database tools and pushdown helpers, `AgentContext | None` parameter stripping in `SchemaBuilder`, non-blocking async driver paths, and clean module-level imports across 52 Python modules (`2222 passed`).
- Synchronized `0.3.0` `CHANGELOG.md`, `docs/`, `docs/examples/`, `product/`, and 22 OKF `knowledge/` chapters (including `ReviewSetupValidator` zero-indented YAML sequence parsing and database driver limitation documentation).
- Split multi-engine draft charters (`native-mysql-mssql-drivers`, `native-embedded-and-cache-drivers`) into five standalone single-engine PR charters (`native-mysql-driver`, `native-mssql-driver`, `native-sqlite-driver`, `native-duckdb-driver`, `native-redis-driver`), updated Master PRDs (`native-data-platform`, `integration-remediation`) and `native-postgres-driver` (`t01`–`t102`, `plan_revision: 2`), and refined Wave 1 specs (`runtime-structure-and-throughput` `t01`–`t08`, `mcp-webmcp-core` `t01`–`t08`, `storage-completeness` `t01`–`t07`) to `state: planned` anchored at `7657e8b7c2c96bdda6821aec70a2b603f438a127`.

## [2026-11-05] flow-revise | Align follow-up driver ownership

- Broker pool sharing extracts PEP 249 code directly into neutral `db/dbapi`; SQLAlchemy keeps its async/greenlet and pool integration, and Django consumes the neutral facade. Downstream integration hardens the established surfaces rather than moving them again.
- SQLSpec wrappers remain extension-owned per-engine packages. Shared artifact metadata belongs to storage; BigQuery Object Table values retain engine ownership. Native placeholder/profile and prepared-result parity require actual native acceptance, with explicit configuration preserved.
- MCP consumes the neutral ToolProvider already established by adapter-owned classes. Oracle native tenancy requires trusted-context invocation, pool reset/failed-cleanup eviction, and live VPD acceptance. Remaining native adapter organization is recorded as bounded follow-up planning.
- Independent plan review approves these ownership amendments. Child charters remain drafts; these revisions establish no runtime acceptance and perform no future implementation or hosted writes.

## [2026-11-05] flow-archive | Completed `db-engine-primitives`

- Completed all seven worksheets on `feat/db-engine-primitives`; reviewed source range `954998de..5ad85a1b`. Adapter-owned notification, CDC, embedding and tool implementations consume neutral contracts; the generic agent preserves typed validation, isolated invocation arguments, and caller-bound approval. Native stack dispatch replaces the CTE compiler, and engine-specific session SQL leaves generic RLS.
- Local verification exits `0`: Python (`2060 passed, 69 skipped, 170 xfailed`), lint/type/stub/Clippy checks, documentation build/artifact checks, and repository review validation. Fresh correctness and quality reviews are clean after remediating typed approval matching and removing obsolete SQL hooks/reader state. Evidence is local CPython 3.12 Linux with actual converter/client-port/REST-fixture checks; live PostgreSQL/GCP/Oracle and hosted platform acceptance remain separate. Oracle context behavior has helper/stand-in acceptance only.
- Task source commits: `t01=6c07c8fe`, `t02=7cdad03f`, `t03=254149af`, `t04=b41aae46`, `t05=16b7322d`, `t06=cb9abcb7`, `t07=2e3004af`; subsequent reviewed corrections are included through `5ad85a1b`.
- Curated current domain chapters, ADR-002, Python dispatch/serialization patterns, polyglot testing, and runtime gotchas before deleting the completed spec. Parent continuity advances to `native-postgres-driver`; provider and integration charters retain their remaining acceptance boundaries. No branch delivery or hosted write accompanies this archive.

## [2026-11-05] flow-archive | Completed `db-layout-and-contract`

- Completed all seven worksheets on `feat/db-updates`; source checkpoint `4ce057e`, documentation checkpoint `1fc52474`, and reviewed range `f741701..1fc52474`. Correctness re-verification resolves PY-01 through PY-08, release/artifact ownership findings, and the adjacent pre-existing GCP HTTP-classification defect. Fresh quality/debloat review has no findings; its minor documentation observation is resolved.
- Verification exits `0`: Python (`1738 passed, 69 skipped, 170 xfailed`), Rust (`473 passed, 1 ignored`), TypeScript, lint/type/stub checks, docs, and review validation. Direct and source-built companion wheels pass isolated imports, combined extras installation, dependency checks, and optional-wheel removal/reinstallation. Local artifacts use CPython 3.12 Linux debug builds; live PostgreSQL/GCP and the hosted release platform matrix remain unverified. Hosted CI run `41935822926` is green at antecedent `989ebbd`; remediation commits remain local.
- Curated domain, architecture, FFI pattern, testing, gotcha, packaging, and ADR-004 chapters before deleting the completed spec. The [database foundation](knowledge/domains/data/database-drivers-and-dialers.md) and [optional-wheel ownership decision](knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md) are durable entry points.
- Native Data Platform Chapter 1 is complete. `db-engine-primitives` remains planned with all seven worksheets intact; its foundation prerequisite is satisfied.

## [2026-11-04] flow-implement | Completed `db-layout-and-contract` (`t01`–`t07`, checkpoint `a5dbea6`)

- Completed all 7 tasks and Phase 1–3 Recovery Checkpoints on `feat/db-updates`: `t01` (`8675187`, `vertebra.db.adapters.<engine>` move and fail-closed `DataFusionEngine`), `t02` (`2fa781f`, `vertebra-db` `connection.rs`/`py_async.rs`, `ShmCapsule`, and `DatabaseError` hierarchy), `t03` (`95afe7a`, standalone `vertebra-bigquery` and `vertebra-spanner` PyO3 wheels and optional extras), `t04` (`c031fc9`, unified `SyncDatabaseDriver`/`AsyncDatabaseDriver` contract), `t05` (`2c0d365`, runtime-checkable `Sync*`/`Async*` capability protocols), `t06` (`144ef8b`, polymorphic `DatabaseRegistry`, `provide_driver`, and upward-import AST gate), and `t07` (`a5dbea6`, parameterized cross-engine conformance suite, `docs/`, and OKF data chapters).

## [2026-11-04] flow-revise | Revised `db-layout-and-contract` (`plan_revision: 5`, `plan_commit: 2c0d365`)

- Added all eleven `src/py/vertebra/db/adapters/<engine>/__init__.py` barrels (`lazy_exports`) to `db-layout-and-contract:t06` so `DatabaseRegistry.parse_url` imports `<Engine>Config` without eagerly importing `driver.py`, specified `+vtb`/`+vertebra` URL scheme fallback and `pool_fingerprint -> tuple[Any, ...] | None`, and aligned `t07` `conftest.py` and `docs/` targets (`0` review findings).

## [2026-11-04] flow-setup | Cleaned Pre-OKF Legacy Artifacts and Aligned OKF v0.2 Bundle on `feat/db-updates`

- Executed `/flow:setup` brownfield alignment (`Align & True-Up`, `shared` Git tracking policy) on `feat/db-updates` (`main @ f741701`):
  - **Legacy Artifact & Beads Cleanup**: Audited and removed obsolete `.beads/` (`262` historical issues superseded on `main`), `.agents/skills/flow-memory-keeper/`, and legacy top-level `.agents/{archive,knowledge,research,specs,wisps,*.md,beads.json}` artifacts; cleaned `.git/info/exclude`, removed stale `merge.beads` / `beads` / `core.hooksPath` entries from `.git/config`, installed repository `pre-commit` hooks, and merged `.claude/settings.local.json` (`plansDirectory: ".agents/bundles/specs"`, backup at `.claude/settings.local.json.bak`).
  - **OKF v0.2 Bundle, Truth Blocks & Provenance**: Added `bundle: "vertebra"` to `.agents/bundles/index.md`, created `.agents/bundles/specs/index.md`, stripped concept-only keys from reserved `index.md` and `log.md` files, added bounded `<= 40`-line `<!-- truth: start -->` blocks across all 35 remaining `knowledge/**` chapters, added `generated`, `verified`, and `sources` (`{id, resource, title}`) with matching `[^id]:` body footnotes across all 53 `product/**` and `knowledge/**` chapters, and synchronized `product/{product,product-guidelines,tech-stack}.md` (`0.3.0`, `JWTGuard`, `WASM*`, `MCP*`, `drive_native_batch`) and `okf-frontmatter.md`.
  - **Active Spec & Research Continuity**: Re-anchored `plan_commit` (`f741701`) across `native-data-platform`, `integration-remediation`, `db-layout-and-contract` (`t01`–`t07`), and `db-engine-primitives` (`t01`–`t07`), updated squash-merged checkpoint SHAs (`364c7cb`, `984ce73`, `abd33ab`) in `integration-remediation/spec.md`, and marked all 4 spec-scoped research briefs `state: promoted`.

## [2026-11-03] flow-archive | Completed and Archived GCP Driver Resilience and Typed Errors (`gcp-driver-resilience`)

- Completed, reviewed (`f6a57aa..cf0b0e5`), and archived Flow `gcp-driver-resilience` (`integration-remediation` Chapter 9, 6 tasks across `vertebra-core`, `vertebra-db`, `vertebra-storage`, `vertebra-py`, `vertebra.db.{spanner,bigquery}`, `docs/`, and `.agents/bundles/`):
  - **Panic-Safe & Cancellable Async Completion Bridge (`t01`)**: Updated `spawn_rust_db_task` and `DbCompletionDrainer::__call__` in `src/rs/vertebra-py/src/completion.rs` to spawn the worker future on a separate Tokio task, catch worker (`JoinError::is_panic()`) and `map_ok` (`catch_unwind`) panics, resolve the Python `asyncio.Future` with `TaskPanicError(RuntimeError)`, attach `_DbTaskCanceller` via `add_done_callback` to call `AbortHandle::abort()` when the Python future is cancelled, and remove `RustDbTaskAwaitable.__new__` from `src/py/vertebra/_vertebra.pyi`.
  - **Async `gcloud` CLI, `ProxyPolicy` & Explicit Default OAuth Scopes (`t02`)**: Replaced blocking `std::process::Command` in `src/rs/vertebra-core/src/gcp_auth.rs::fetch_token_from_gcloud_cli` with `tokio::process::Command` (`kill_on_drop(true)` and 30-second `GCLOUD_CLI_TIMEOUT`, configurable via `GCLOUD_BIN`); added `ProxyPolicy` (`Environment`, `Direct`, `Explicit(reqwest::Proxy)`) on `token_client` while keeping `metadata_client` direct (`.no_proxy()`); and replaced `GcpTokenCache::from_gcs_options` with `GcpTokenCache::from_options(raw_opts, default_scopes)` across `vertebra-db`, `vertebra-storage`, and `vertebra-py`.
  - **BigQuery REST Warning Tolerance & `jobs.getQueryResults` URL Encoding (`t03`)**: Updated `check_rest_error` and `rest_error_details` in `src/rs/vertebra-db/src/bigquery.rs` to fail only on top-level `error` or `status.errorResult` (allowing informational `errors[]` warnings) while preserving `reason` and `canonical_status` on `BigQueryError::Api`, and added `BigQueryClient::build_get_query_results_url` using `url::Url::path_segments_mut()` and `query_pairs_mut()`.
  - **Typed `GoogleCloudError` Exception Hierarchy & Status-Code Fallback (`t04`)**: Added `src/rs/vertebra-py/src/errors.rs` (`GoogleCloudError`, `GoogleCloudAuthError`, `SpannerError`, `SpannerAbortedError`, `BigQueryError`, `BigQueryRowError` inheriting from `vertebra._typing.VertebraError` with structured `code`, `code_name`, `message`, `http_status`, `retry_delay_ms`, `reason`, and `row_errors` attributes), wired `spanner_error_to_py`, `bigquery_error_to_py`, and `auth_error_to_py` across sync and async `vertebra-py` bindings, and switched `SpannerCore.is_partitioned_query_fallback_error`, `BigQueryCore.is_storage_api_fallback_error`, and `run_in_transaction` `SpannerAbortedError` retries to inspect gRPC status codes and typed exceptions instead of substring matching.
  - **Char-Aware GoogleSQL Positional Placeholder Rewriter (`t05`)**: Rewrote `rewrite_spangres_placeholders` in `src/rs/vertebra-db/src/spanner.rs` as a `char`-aware lexer handling single-, double-, triple-quoted, raw (`r`/`R`), and bytes (`b`/`B`) string literals, backslash escapes, backtick identifiers, and `--`, `#`, and `/* */` comments while preserving non-ASCII UTF-8 characters.
  - **Documentation & OKF Curation (`t06`)**: Updated `docs/usage/databases.md`, `docs/usage/storage-auth-and-signing.md`, `docs/changelog/index.md`, `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md`, and `.agents/bundles/knowledge/patterns/rust/ffi-and-gil-discipline.md`.

## [2026-11-03] flow-archive | Completed and Archived Official Google Cloud Rust Clients for Spanner and BigQuery Storage (`gcp-wire-protocol-correctness`)

- Completed, reviewed (`5494583..260ae8d`), and archived Flow `gcp-wire-protocol-correctness` (`integration-remediation` Chapter 2, 6 tasks across `vertebra-db`, `vertebra-py`, `vertebra.db.{spanner,bigquery}`, `docs/`, and `.agents/bundles/`):
  - **Official Google Cloud Rust Crates & Credential Bridge (`t01`)**: Pinned `google-cloud-spanner =0.34.8-preview`, `google-cloud-bigquery =0.19.0`, `google-cloud-auth 1.18`, `google-cloud-gax 1.15`, `google-cloud-longrunning 1.14` (all `default-features = false`), `tonic 0.14` (`tls-ring`, `tls-webpki-roots`), and `futures 0.3`; implemented `vertebra_db::gcp::{install_rustls_ring_provider, GcpTokenCredentials, official_credentials}` bridging `GcpTokenCache` into `google_cloud_auth::credentials::Credentials` (`authorization: Bearer <token>` and optional `x-goog-user-project`).
  - **Spanner Single-Use & Partitioned Reads on the Official Client (`t02`)**: Replaced custom Spanner query wire encoders in `src/rs/vertebra-db/src/spanner.rs` with `google_cloud_spanner::client::{Spanner, DatabaseClient}` (`single_use`, `batch_read_only_transaction`, `partition_query`, concurrent partition execution via `try_join_all`), added `SpannerClient::with_clients`, replaced `execute_partitioned_arrow[_async]` on `vertebra._vertebra.SpannerClient` with `partitioned_query[_async]` returning rows, and moved partitioned-query fallback (`is_partitioned_arrow_fallback_error`) and `ArrowCursor.from_rows` materialization into `SyncSpannerDriver.fetch_arrow` and `AsyncSpannerDriver.fetch_arrow`.
  - **Spanner Writes, DDL `admin_endpoint` & Legacy Wire Removal (`t03`)**: Moved `execute_dml`, `execute_batch_dml`, `execute_partitioned_dml`, `apply_mutations`, and `batch_write` onto the official `DatabaseClient` read-write, partitioned-DML, and write-only transaction APIs; enforced `commit_timestamp.is_some()` on commit responses (`SpannerError::Protocol`); implemented REST `update_database_ddl` via `google_cloud_spanner::client::DatabaseAdmin` with `effective_admin_endpoint` (`*.googleapis.com` → `https://spanner.googleapis.com`, emulator `:9010` → `:9020`, explicit `admin_endpoint` required otherwise) and deadline-bounded LRO polling (`DatabaseAdmin::get_operation`, `SpannerError::Ddl`, `SpannerError::DdlTimeout`); enforced `SpannerConfig.ca_pem` Omni TLS validation (`.with_omni_tls` for non-`*.googleapis.com` HTTPS endpoints only); and deleted all hand-written Spanner protobuf/session/precommit wire code.
  - **BigQuery Storage Read on the Official Client (`t04`)**: Moved `BigQueryClient::read_table_to_shm_arrow_with_compression` onto `google_cloud_bigquery::client::Read` (`create_read_session` + concurrent `read_rows` streams collected in stream index order into `ShmArrowStreamWriter`), added `BigQueryClient::with_read_client`, and removed legacy Storage Read protobuf encoders/decoders.
  - **BigQuery Storage Write, Hardened `ArrowAppendPlan` & `grpc.rs` Removal (`t05`)**: Implemented `ArrowAppendPlan::{build, total_rows, verify_responses}` and bounds-checked `ArrowMessageHeader::parse` (one record batch per `ArrowAppendRequest`, rejecting dictionary batches, missing continuation markers, non-initial/duplicate schemas, oversized batches, and trailing bytes); moved `BigQueryClient::append_arrow_stream` onto `google_cloud_bigquery::client::Write` across `Default`, `Committed`, `Pending`, and `Buffered` modes with `WriteRetryableErrors.with_attempt_limit(3)` on `open_default_stream` and response count/offset verification (`BigQueryError::AppendVerification`); and deleted `src/rs/vertebra-db/src/grpc.rs` and `src/rs/vertebra-db/tests/grpc_wire_and_pool_tests.rs`.
  - **Documentation & OKF Curation (`t06`)**: Updated `.agents/bundles/product/tech-stack.md`, `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md`, `docs/usage/databases.md`, `docs/usage/packaging-and-deployment.md`, and `docs/changelog/index.md`, and promoted `google-cloud-rust_20261103.md` into `.agents/bundles/specs/integration-remediation/research/`.

## [2026-11-03] flow-archive | Completed and Archived Silent Failure and Security Fixes (`critical-correctness-fixes`)

- Completed, reviewed (`0d178f2..b61726a`), and archived Flow `critical-correctness-fixes` (`integration-remediation` Chapter 1, 8 regression-TDD tasks across `vertebra.db`, `vertebra.agent`, `vertebra.framework`, `vertebra.extensions`, `vertebra.embeddings`, and `vertebra.server`):
  - **Tenant RLS Propagation, Spangres URL Routing & Spangres CDC Positional Params (`t01`)**: `BaseSyncDriver.apply_tenant_predicate` and `BaseAsyncDriver.apply_tenant_predicate` (`src/py/vertebra/extensions/sqlspec/adapters.py`) propagate GUC `SET LOCAL` exceptions unchanged; `DatabaseRegistry.parse_url` (`src/py/vertebra/db/_registry.py`) routes `spangres://` to `SpannerConfig.from_dsn` and removes the dead `spanner+postgresql` branch; `SpannerChangeStreamSource.poll_partition_sync` and `poll_partition_async` (`src/py/vertebra/db/cdc.py`) pass positional parameters (`[start_ts, end_ts, partition_token, heartbeat_ms]`) when `dialect == "spangres"`.
  - **WebMCP Opt-In & MCP Tool Confirmation Enforcement (`t02`)**: Removed `WebMCPConfig.include_tagged_only`, defaulted `mcp_tool(webmcp=False)` and `Tool(webmcp=False)`, and updated `build_webmcp_manifest`, `SkillController.get_tools`, `MCPRegistry.register_route_handler`, and `AgentPlugin._inspect_item` so WebMCP manifests list only tools opted in with `webmcp=True` while preserving `webmcp`, `annotations`, `requires_confirmation`, `timeout_seconds`, and `guards` across controller and route `opt["mcp_tool"]` tools; enforced `Tool.requires_confirmation` in `MCPService._handle_tools_call` (`_meta["vertebra/confirmationToken"]`, error `CONFIRMATION_REQUIRED = -32003`) bound to ticket status, owner, tool name, and arguments; fixed `ConfirmationGate.approve` and `reject` to mutate `_approved_tokens` only after ticket/owner validation; and wired `AgentPlugin` so `MCPService` and `AgentRuntime` share the same `ConfirmationGate`.
  - **`Mcp-Name` Header Binding & Nested-Params Rejection (`t03`)**: `MCPHttpHandler.handle_request` (`src/py/vertebra/agent/mcp/routes.py`) rejects `Mcp-Name` when the body omits `params.name`/`params.uri` or mismatches, and rejects `Mcp-Method`/`Mcp-Name` headers on JSON-RPC batch arrays; `JSONRPCRouter._invoke_handler` (`src/py/vertebra/agent/core/jsonrpc.py`) passes mapping params directly as `params=` without unwrapping nested `"params"` keys or retrying handler `TypeError`s.
  - **DB Tool Identifier Validation & Multi-Statement Classification (`t04`)**: `bigquery_object_table_inspect` validates `table_name` via `validate_identifier(..., allow_hyphens=True)`, and `is_mutating_sql` (`src/py/vertebra/db/mcp_tools.py`) rejects any non-trailing semicolon with `PermissionDeniedError` and classifies procedural, privilege, export, and load statements (`CALL`, `GRANT`, `REVOKE`, `EXECUTE`, `EXPORT`, `LOAD`, `COPY`) as mutating.
  - **Prefix-Scoped Store Deletes, Millisecond TTLs, `expires_in` Contract & Litestar Swap Defaults (`t05`)**: Added `scan`, `unlink`, and `px` to `SyncRedisDriver` and `AsyncRedisDriver` (`src/py/vertebra/db/redis/driver.py`), added validated `key_prefix` (`"vertebra"` on `KeyValueStore`, Litestar `MemoryStore`, and Starlette `CacheStore`; `"vtb:aa:"` on `SQLAlchemyCacheBackend`) with cursor-iterated `SCAN` + `UNLINK` in `delete_all()` / `clear()`, switched sub-second expiries to millisecond `px`, returned `None` from `expires_in()` when a key has no expiry, and defaulted `LitestarConfig.swap_memory_channels` and `swap_memory_response_cache` to `False`.
  - **Remote Embedder Error Propagation & Identifier Validation (`t06`)**: Added `EmbeddingError(VertebraError, RuntimeError)` (`src/py/vertebra/embeddings/_base.py`), removed silent SHA-256 fallback vectors from `SpannerEmbedder`, raised `EmbeddingError` on empty or short result sets across `SpannerEmbedder`, `OracleEmbedder`, and `BigQueryEmbedder`, ordered generated embeddings by input offset (`ORDER BY idx`), and validated SQL identifiers via `validate_identifier`.
  - **Free-Threaded Local Embedder `BackendFactory` Sidecar (`t07`)**: Added `BackendFactory` (`src/py/vertebra/embeddings/_broker.py`) and `FastEmbedder.build_backend` (`src/py/vertebra/embeddings/_local.py`), removed `_deterministic_hash_backend` from `EmbeddingBroker`, and executed the configured `BackendFactory` inside the free-threaded subprocess quarantine worker.
  - **`EventLoopPool.stop` Drain & Configurable ASGI Batch Timeout (`t08`)**: Updated `EventLoopPool.__init__` and `stop` (`src/py/vertebra/server/_loops.py`) to verify shard startup readiness, drain all shard tasks (`asyncio.all_tasks()`) and async generators (`loop.shutdown_asyncgens()`), and leave loops open if a thread is still alive after join timeout; propagated future cancellation from `schedule_batch` to shard asyncio tasks; and made `_AsgiMountRunner.__call__` (`src/py/vertebra/server/worker.py`) and buffered `async_native` `dispatch_batch(async_mode=False)` (`src/py/vertebra/server/_dispatch.py`) honor `VERTEBRA_ASGI_BATCH_TIMEOUT` (default `30.0` s, `0`/`none` disables) and return `504 Gateway Timeout` on expiry.
  - Curated `.agents/bundles/knowledge/architecture/runtime/platform-and-lifecycle.md`, `.agents/bundles/knowledge/domains/{agent/embeddings-and-vectors,agent/mcp-and-a2a-protocol-harness,data/database-drivers-and-dialers,data/query-pipeline-cdc-and-tenancy,extensions/litestar,extensions/sqlspec-alchemy-and-dialects,framework/subdomains,server/worker-and-protocols}.md`, and `.agents/bundles/knowledge/gotchas/pyo3-and-python-runtime.md`.

## [2026-11-03] flow-plan | Planned `critical-correctness-fixes`, `gcp-wire-protocol-correctness`, and `gcp-driver-resilience`

- `critical-correctness-fixes`: 8 regression-TDD tasks (plan_revision 2) covering tenant isolation, MCP confirmation/exposure/header binding, DB tool SQL safety, store data safety, embedder errors, the free-threaded embedding backend, and loop shutdown.
- `gcp-wire-protocol-correctness`: 6 tasks (plan_revision 2) moving Spanner and BigQuery Storage onto the official `googleapis/google-cloud-rust` crates and deleting the hand-written gRPC wire code. Rationale is in `research/google-cloud-rust_20261103.md`.
- `gcp-driver-resilience` (new Chapter 9): 6 tasks (plan_revision 2) covering the panic-safe/cancellable completion bridge, async gcloud and proxy policy, BigQuery REST fixes, typed `GoogleCloudError` exceptions on `VertebraError`, and the GoogleSQL placeholder lexer.
- Each plan passed two rounds of zero-context review.

## [2026-11-03] flow-prd | Authored Master PRD `native-data-platform` and revised `integration-remediation`

- User decisions:
  - Engine code moves to `db/adapters/<engine>` and engine-agnostic code to `db/common`.
  - Native Vertebra queue backends ship for Postgres and Oracle first and plug into first-party queues, litestar-queues, and Django tasks.
  - Required and feature complete: any AA/SQLSpec config shares a broker-owned pool over the SHM ring, and Vertebra's own Rust drivers and storage back every engine.
- Added `native-data-platform` with child flows `db-layout-and-contract`, `native-postgres-driver`, `broker-pool-sharing`, `native-oracle-driver`, `native-mysql-mssql-drivers`, `native-embedded-and-cache-drivers`, and `storage-completeness`.
- Renamed `db-driver-foundation` to `db-engine-primitives` and rescoped `queues-tasks-and-channels`.

## [2026-11-03] flow-prd | Authored Master PRD `integration-remediation`

- Audited `d3d5f8b..142cc49` (PRs #64, #65, #66, #69, #70) across the database extensions, framework/embeddings/utils/server, the Rust GCP drivers, MCP/WebMCP, and `vertebra.db`. Findings are in `specs/integration-remediation/research/audit-findings_20261103.md`.
- Seeded draft child flows `critical-correctness-fixes`, `gcp-wire-protocol-correctness`, `db-driver-foundation`, `sqlspec-adapter-parity`, `queues-tasks-and-channels`, `sqlalchemy-and-django-integration`, `mcp-webmcp-core`, and `runtime-structure-and-throughput`.
- Filed upstream [sqlspec#876](https://github.com/litestar-org/sqlspec/issues/876) and [litestar-queues#186](https://github.com/cofin/litestar-queues/issues/186) for adapter-name resolution outside `sqlspec.adapters`.

## [2026-11-03] flow-archive | Completed and Archived Google Cloud BigQuery and Cloud Spanner Platform (`google-bigquery-and-spanner-platform` & Chapters 1–4)

- Completed and archived Master PRD `google-bigquery-and-spanner-platform` and all 4 child flows (`gcp-foundation-and-object-pipelines`, `bigquery-driver-objects-and-sqlspec`, `spanner-driver-objects-and-sqlspec`, `gcp-framework-adapters-and-sqlalchemy`):
  - **Chapter 1 (`gcp-foundation-and-object-pipelines`)**: Shared `vertebra_core::gcp_auth::GcpTokenCache` (Service Account PKCS#8 RS256 JWT assertion, custom OAuth scopes, `quota_project_id`), zero-RTT local RSA-SHA256 GCS V4 presigning (`sign_gcs_v4_url`) + cached IAM `signBlob`, `vertebra_db::grpc` (`128 MiB` `hyper` HTTP/2 + cleartext `h2c` channel pool, `google.rpc.Status` trailer decoding, streaming Protobuf v3 codec), `vertebra_core::CompletionQueue<T>` + per-extension `loop.add_reader` drainers, unified PEP 3118 `ArrowCursor` + 16-byte `ShmArrowStreamWriter` (`vertebra_db::arrow_shm`), `SHMBrokerTransport._DIALECT_TAGS` (`1..9`), GoogleSQL `QueryPipeline`, and `StreamingStorageResponse` + `ObjectStoreBackend` `VTBARROW` streaming.
  - **Chapter 2 (`bigquery-driver-objects-and-sqlspec`)**: Pure-Rust `BigQueryConfig`, `BigQueryCore`, `SyncBigQueryDriver`, and `AsyncBigQueryDriver` (`src/rs/vertebra-db/src/bigquery.rs`, `src/py/vertebra/db/bigquery/`) over REST v2 (`jobs.query`, `jobs.insert`, `dry_run`, multi-statement `session_id` ACID transactions) and Storage Read/Write v1 (`BigQueryRead` -> `VTBARROW` `ShmRegion`, `BigQueryWrite.AppendRows` with automatic REST fallback when Storage API is disabled), `gs://` `EXPORT`/`LOAD` + multicloud `ObjectStoreBackend` staging + BigQuery Object Tables (`ObjectTableConfig` / `ObjectRef`), native `BigQuerySyncConfig` / `BigQueryAsyncConfig` + ADK/Litestar/Events/Artifact stores + `create_connection()` shim, `BigQueryEmbedder`, `DataFusionEngine.register_bigquery_table`, and FinOps telemetry (`record_bigquery_job`).
  - **Chapter 3 (`spanner-driver-objects-and-sqlspec`)**: Pure-Rust `SpannerConfig`, `SpannerCore`, `SyncSpannerDriver`, and `AsyncSpannerDriver` (`src/rs/vertebra-db/src/spanner.rs`, `src/py/vertebra/db/spanner/`) for GoogleSQL (`@p1`) and Spangres (`$1`) with cached `multiplexed = true` sessions, leader-aware routing, `PartialResultSet` chunk merger + `resume_token` replay, lock-free `ReadOnly` snapshots (`StalenessBound`), full `ReadWrite` state machine + wound-wait `Aborted` retry with mutation buffer replay, `BatchWrite`, parallel `PartitionQuery`/Data Boost -> `VTBARROW` (with automatic single-partition fallback), `ObjectStoreBackend` Parquet/Arrow bridge, native `SpannerSyncConfig` / `SpannerAsyncConfig` + ADK/Memory/Artifact/Litestar/Events stores, `SpannerQueueBackend`, `SpannerChangeStreamSource`, and `SpannerEmbedder`.
  - **Chapter 4 (`gcp-framework-adapters-and-sqlalchemy` & Modular Driver Architecture)**: Restructured all 10 built-in database/cache drivers (`postgres`, `mysql`, `sqlite`, `oracle`, `mssql`, `duckdb`, `bigquery`, `spanner`, `redis`, `valkey`) into per-driver packages (`config.py`, `core.py`, `driver.py`, `__init__.py`) backed by `src/py/vertebra/db/driver/` (`BaseDatabaseConfig`, `CommonDriverMixin`, `SyncDatabaseDriver`, `AsyncDatabaseDriver`, `ThreadOffloadAsyncDatabaseDriver`); removed hardcoded emulator hooks from production configs; added zero-copy `ArrowCursor` and `StreamingStorageResponse` `FastPath` serialization across native, Litestar, FastAPI, Starlette, Flask, and Django; Django `bigquery` and `spanner` `DatabaseWrapper(DatabaseWrapperMixin)` backends; `provide_bigquery_driver` / `provide_spanner_driver` DI helpers; governed `register_gcp_database_tools` MCP `2026-07-28` / A2A `1.0` tool factory; secondary SQLAlchemy 2.1 `+vtb` dialects (`BigQueryDialect`, `SpannerDialect`, `SpangresDialect` + async variants), `Vector`/`BinaryJSON` type extensions, and DDL-batching `SpannerAlembicImpl`; direct TCP/TLS (`:5432`, `:3306`) IAM OAuth2 token password resolution (`resolve_iam_password_sync`/`_async`) on `PostgreSQLConfig` and `MySQLConfig`; and standardized `src/rs/vertebra-db/src/duckdb.rs` naming.

## [2026-11-03] flow-archive | Completed and Archived Rust-Accelerated Text, Humanize, Identifier, UUID, and Runtime Utilities (`core-text-and-runtime-utils`)

- Completed and archived Flow `core-text-and-runtime-utils` (5 tasks across `vertebra-core`, `vertebra-py`, `vertebra.utils`, `vertebra.{build,cli,db,extensions,framework,telemetry}`, and `docs/reference/`):
  - **Rust `vertebra_core::text` & `vertebra_core::id` + `vertebra._vertebra` Bindings (`t01`)**: Implemented single-pass zero-regex ASCII/UTF-8 state machines in `src/rs/vertebra-core/src/text.rs` (`snake_case`, `camelize`, `pascalize`, `kebabize`, `slugify` with Latin-1/Latin-Extended-A diacritic folding, `quote_identifier`, `quote_backtick_identifier`, `quote_bracket_identifier`, `quote_dialect_identifier`, `normalize_identifier`, `split_qualified_identifier`, `validate_identifier`, `truncate_utf8_bytes`, `truncate`, `humanize` stripping `_id`/`_ids`, `titleize`, `pluralize`, `singularize`, `ordinalize`, `humanize_bytes`, `parse_bytes`, `humanize_duration`, `parse_duration`, `humanize_number`, `humanize_rate`) and RFC 9562 generators in `src/rs/vertebra-core/src/id.rs` (`uuid4_u128`, lock-free `AtomicU64` monotonic `uuid7_u128`, `nanoid_generate`), exposed on `vertebra._vertebra` with `.pyi` stubs and `--only-group doc` `fallback_none_attrs` registration in `tools/docs/generate_cli_docs.py`.
  - **Python `vertebra.utils.text`, `humanize`, and `uuids` + Call-Site Consolidation (`t02`)**: Added `src/py/vertebra/utils/text.py` and `src/py/vertebra/utils/humanize.py` with native `_vertebra` dispatch (`callable(fn) and not isinstance(fn, type)`) and pure-Python fallbacks, expanded `src/py/vertebra/utils/uuids.py` (`uuid6`, `uuid7`, `nanoid`, `uuid_from_string`, `uuid_from_bytes`, `uuid_from_int`), and consolidated ad-hoc case converters, slugifiers, SQL identifier validators, UTF-8 byte truncators, and pluralization across `protobuf.py`, `_metadata.py`, `openapi.py`, `procedures.py`, `_welcome.py`, `grpc/__init__.py`, `db_propagation.py`, `pipeline.py`, `rls.py`, `sqlspec/storage.py`, and `banner/renderer.py`.
  - **Type Dispatch, Schema Hydration, Type Converters & Expanded Type Guards (`t03`)**: Added `TypeDispatcher[T]` (`src/py/vertebra/utils/dispatch.py`), `transform_dict_keys` / `to_schema` / `to_value_type` (`src/py/vertebra/utils/schema.py`), ISO/Decimal/UUID converter builders (`src/py/vertebra/utils/type_converters.py`), and expanded `src/py/vertebra/utils/type_guards.py` (`is_readable`, `is_async_readable`, `is_typed_dict`, `is_dict_row`, `is_mapping_like`, `is_iterable_parameters`, `is_attrs_schema`, `is_schema`, `get_msgspec_rename_config`, `extract_dataclass_fields`, `extract_dataclass_items`, `supports_close`).
  - **Config Tools, Module Loader, Deprecation, Correlation & Shim Removal (`t04`)**: Added variadic `read_pyproject_section(*section_path, start_path=...)` (`[tool.vertebra]` with `[tool.vtb]` fallback), `find_pyproject`, `SingletonMeta`, `get_env`, `get_env_bool`, `get_env_int`, `get_env_float`, `get_env_list`, `get_env_dict`, and `get_env_path` in `src/py/vertebra/utils/config_tools.py`, `import_optional_attr`, `resolve_optional_attr`, and `module_to_os_path` in `src/py/vertebra/utils/module_loader.py`, `@deprecated` / `warn_deprecation` / `assert_deprecated` in `src/py/vertebra/utils/deprecation.py`, `CorrelationContext` / `CorrelationLoggerAdapter` in `src/py/vertebra/utils/correlation.py`, and deleted `src/py/vertebra/utils/module_loading.py` (`PY-07`).
  - **Knowledge Contraction & Docs (`t05`)**: Curated `knowledge/patterns/python/serialization-and-utils.md`, `knowledge/gotchas/pyo3-and-python-runtime.md`, and `docs/reference/index.md`.

## [2026-11-02] flow-archive | Completed and Archived Development Welcome Page & HTML Docs Content-Type (`dev-welcome-page`)

- Completed and archived Flow `dev-welcome-page` (2 tasks across `vertebra.framework`, `vertebra.supervisor`, `vertebra.server`, and `vertebra-http`):
  - **Offline-Ready Welcome Page Renderer & `App.ensure_dev_welcome_route()` (`t01`)**: Implemented `WelcomeRouteItem`, `WelcomeSnapshot`, and `WelcomePageRenderer` in `src/py/vertebra/framework/_welcome.py` (lazy-exported on `vertebra.framework`), added `App(debug: bool = False)`, `App.debug`, and `App.ensure_dev_welcome_route()` in `src/py/vertebra/framework/_app.py` with automatic preemption/eviction when user routes or root mounts claim `"/"` and without polluting `app.routes`, and updated `App.enable_docs()` to return `Response(200, html_bytes, [("content-type", "text/html; charset=utf-8")])`.
  - **Supervisor, Worker & `vertebra-http` Route Table Wiring (`t02`)**: Wired `ensure_dev_welcome_route()` into `src/py/vertebra/supervisor/_worker.py::{resolve_worker_targets,_build_app_asgi_adapter}`, `src/py/vertebra/server/worker.py::Worker.__init__`, and `src/rs/vertebra-http/src/main.rs::{build_route_table_from_app,spawn_python_workers}`, and curated `knowledge/domains/framework/routing-and-app.md`.

## [2026-11-02] flow-archive | Completed and Archived Unified Event Loop Backends, C-API Coroutine Stepper, Rust Completion Ring, 6-Framework Stacked FastPath & Shared Native Database Pool (`async-loop-and-framework-fastpath`)

- Completed and archived Flow `async-loop-and-framework-fastpath` (6 tasks across `vertebra-py`, `vertebra-storage`, `vertebra.server`, `vertebra.db`, `vertebra.utils`, and `vertebra.extensions.*`):
  - **Unified `LoopBackend` & GIL Audit (`t01`)**: Added `LoopBackend(StrEnum)` (`auto`, `uvloop`, `winloop`, `rloop`, `asyncio`) and `resolve_loop_backend` in `src/py/vertebra/server/_loops.py`, added `--loop` (`VERTEBRA_LOOP`) to `vtb run` and `vtb dev`, bumped `uvloop>=0.23.0` (`eager_start` + `3.15`/`3.15t` wheels) and added `winloop>=0.8.1` on Windows in `pyproject.toml`, and removed deprecated `asyncio.set_event_loop_policy(asyncio.DefaultEventLoopPolicy())` calls from `src/py/vertebra/server/_gil_audit.py`.
  - **C-API `drive_native_batch` & Single-Call `send_raw_response` (`t02`)**: Added `drive_native_batch` (`ProtocolFinalizer::Native` on `SteppedCoroutine` via `PyIter_Send`), `AsgiScope.body_bytes()`, `AsgiProtocol.body_bytes()`, `AsgiProtocol.__call__`, and `AsgiProtocol.send_raw_response` in `src/rs/vertebra-py/src/asgi.rs`, `AsgiSend.send_raw_response` in `src/py/vertebra/server/_asgi/_queues.py`, `_WsgiStartResponse.send_raw_response` in `src/rs/vertebra-py/src/wsgi/runner.rs`, and flattened `_invoke_compiled_sync_to_coro` in `src/py/vertebra/server/_dispatch.py`.
  - **Per-Loop `RustCompletionRing` & Coalesced `SyncPool` Wakeups (`t03`)**: Replaced per-operation Tokio `Python::attach` + `call_soon_threadsafe` in `src/rs/vertebra-storage/src/lib.rs::spawn_rust_task` with a per-loop `RustCompletionRing` + `vertebra_core::notify::Notifier` (`eventfd`/`kqueue`) registered once via `loop.add_reader`, and added `_LoopCompletionQueue` `0 -> 1` wakeup coalescing and right-sized default worker bounds in `src/py/vertebra/utils/sync_tools.py::SyncPool`.
  - **Zero-Closure ASGI `FastPath` Across Litestar, FastAPI & Starlette (`t04`)**: Created `src/py/vertebra/server/_fastpath.py` (`resolve_relative_path`, `extract_scope_body_fast`, `send_asgi_direct`, `wrap_asgi_fastpath`, `attach_framework_fastpath`), wired zero-closure `wrap_asgi` across `Litestar`, `FastAPI`, and `Starlette`, unwrapped `FromPath[T]`, `FromQuery[T]`, and `JSONBody[T]` in Litestar `_compile_direct_signature_plan`, closed unawaited `ASGIFileResponse.file_info` coroutines on `pathsend`, skipped `starlette.requests.Request` allocation in `FastAPI` when `CompiledRoute.needs_request` is `False`, and defaulted sync `FastAPI`/`Starlette` endpoints and dependencies to inline execution (`sync_to_thread=False`).
  - **Flask & Django `async def` Views, Cached `MsgspecView` & WSGI `send_raw_response` (`t05`)**: Added `async def` view execution (`_step_coro_sync`, `dispatch_async`, `wrap_asgi`), cached stateless `MsgspecView.as_view()` instances, and `_WsgiStartResponse.send_raw_response` dispatch in `src/py/vertebra/extensions/flask/fastpath.py` and `src/py/vertebra/extensions/django/fastpath.py`.
  - **Stacked `App.mount` Auto-FastPath & Cross-Framework Shared Native Database Pools (`t06`)**: Wired `attach_framework_fastpath` into `Worker._instantiate_mount_runners` and `WsgiAdapter`, and added `DatabaseRegistry.default()`, `DatabaseRegistry.default_broker()`, `DatabaseRegistry.pool_fingerprint()`, `get_shared_driver()`, and `get_shared_sync_driver()` across `Django` (`DatabaseWrapperMixin`), `SQLSpec` (`SyncAdapterConfig` / `AsyncAdapterConfig`), and `SQLAlchemy` (`+vtb` `DBAPIConnection` / `AsyncDBAPIConnection`).
- Contracted `.agents/bundles/specs/` back to lazy (`.gitkeep` only).

## [2026-11-01] flow-archive | Completed and Archived Shared-Memory Vector Embeddings, Active GCP ADC Credential Refresh, `vtb ai` CLI & Documentation Split (`shm-vector-embeddings`)

- Completed and archived Flow `shm-vector-embeddings` (7 tasks across `vertebra-core`, `vertebra-storage`, `vertebra-db`, `vertebra-http`, `vertebra-py`, `vertebra`, and `docs/`):
  - **Active GCP ADC Credential Refresh & Non-Blocking Storage (`t01`)**: Implemented `GcpTokenSnapshot`, `GcpAuthSource`, `GcpTokenCache`, and `RefreshingGcpCredentialProvider` in `src/rs/vertebra-storage/src/gcp_auth.rs` with `300s` proactive refresh skew, `tokio::sync::Mutex` single-flight deduplication, and generation-checked invalidation across `AuthorizedUser`, `MetadataServer`, `ImpersonatedServiceAccount`, and `Static` sources; replaced `block_on` with non-blocking `spawn_rust_task` + `TaskCompletionCallback` in `src/rs/vertebra-storage/src/lib.rs`; added per-session `applied_token_generation` secret rotation in `SyncDuckDBDriver` and `AsyncDuckDBDriver` (`src/py/vertebra/db/duckdb.py`).
  - **Shared-Memory Vector Primitives & PEP 3118 Views (`t02`)**: Implemented `VectorSlab`, `VectorCache`, SIMD MRL truncation and quantization (`int8`, `binary`), `StaticEmbedderCore`, and `format_gemma_prompt` in `src/rs/vertebra-core/src/vector.rs`, plus frozen PEP 3118 `VectorView` (1D) and `VectorBatchView` (2D) in `src/rs/vertebra-py/src/vector.rs`.
  - **Binary Vector Database Wire Codecs (`t03`)**: Added binary `pgvector` (`vector`, `halfvec`, `bit`) wire codecs in `src/rs/vertebra-db/src/postgres.rs`, zero-copy `PyBuffer` Oracle 26ai `0xDB` codecs in `src/rs/vertebra-py/src/db.rs`, `VectorBatchView.to_arrow_shm()`, and SQLAlchemy `Vector` bind/result pushdown in `src/py/vertebra/extensions/sqlalchemy/types.py`.
  - **Google-First Zero-Weight Cloud & SQL Pushdown Embedders (`t04`)**: Added Protobuf v3 packed-`f32` gRPC framing in `src/rs/vertebra-core/src/vector.rs` and `src/rs/vertebra-http/src/grpc.rs`, and implemented `Embedder`, `VertexEmbedder`, `GeminiEmbedder`, `AlloyDbEmbedder`, `OracleEmbedder`, `RemoteEmbedder`, and `create_embedding_grpc_service` in `src/py/vertebra/embeddings/`.
  - **`1x` RAM `EmbeddingBroker` & Local Models (`t05`)**: Implemented `ModelSpec` catalog (`src/py/vertebra/embeddings/_models.py`), `EmbeddingBroker` with token-budgeted length-sorted dynamic batching and `3.14t`/`3.15t` subprocess GIL quarantine (`src/py/vertebra/embeddings/_broker.py`), and `GemmaEmbedder` (with `fp16` overflow guard), `FastEmbedder`, and `StaticEmbedder` (`src/py/vertebra/embeddings/_local.py`).
  - **`vtb ai` CLI, Co-Located Workers & Dynamic A2A URLs (`t06`)**: Consolidated `vtb ai models list|pull|stage|warm` and `vtb ai mcp serve|bridge` (`src/py/vertebra/cli/ai.py`), added `--with-workers`, `--queues`, `--lane-concurrency`, and `[tool.vertebra.workers]` to `vtb run` and `vtb dev`, added `--oneshot` to `vtb workers run`, and enabled automatic `AgentInterface` and dynamic request-origin URL resolution on `AgentCard` (`src/py/vertebra/agent/a2a/`).
  - **Documentation Split, Branding & OKF Sync (`t07`)**: Added `docs/usage/embeddings-and-vectors.md` and `docs/examples/embeddings_app.py`, split `docs/usage/` into 14 focused topic guides in `zensical.toml`, applied the Option B Isometric Axial Vertebrae mark across `docs/assets/`, pinned `VERTEBRA / <Current Page>` in `docs/stylesheets/extra.css`, and curated `knowledge/domains/agent/embeddings-and-vectors.md`.
- Contracted `.agents/bundles/specs/` back to lazy.

## [2026-11-01] flow-setup | Aligned OKF v0.2 Bundle, Engineering Domain Skills, and Multi-Harness Subagents

- Executed `/flow:setup` brownfield alignment (`Align & True-Up`, `shared` Git tracking policy) on branch `chore/flow-setup-alignment` off `main` (`v0.2.0`):
  - **Legacy Artifact Removal & Continuity Alignment**: Removed legacy `.agents/transactions/`, `.agents/skills/flow-memory-keeper/`, and `.agents/config.json`; elevated project nuances into `knowledge/workflow.md`; updated `.agents/setup-state.json` and `.gitignore` to track `.agents/setup-state.json`, `.agents/bundles/**`, `.agents/skills/**`, and `.agents/agents/**` while ignoring `.agents/scratch/`.
  - **OKF v0.2 Bundle & ADRs**: Added bounded `<= 40`-line `<!-- truth: start -->` blocks to `product/product-guidelines.md` and `knowledge/patterns/typescript/client-and-adapter-subpaths.md`, created `product/index.md` and `knowledge/decisions/{index,adr-001-dual-topology-embedded-and-shm-runtime,adr-002-two-tier-shm-database-and-arrow-data-plane,adr-003-pypi-vertebra-core-distribution-and-wheel-bundled-client}.md`, and synchronized `product/product.md` and `product/tech-stack.md` with `v0.2.0` (`@vertebra/client` `0.2.0`, Starlette/FastAPI/Flask/Django extensions, `/router`, `/webmcp`, and C-API `msgspec_bridge`).
  - **Engineering Domain Skills & Multi-Harness Subagents**: Created stack-specific engineering domain skills in `.agents/skills/{runtime-ipc,http-ingress,pyo3-bridge,python-framework,typescript-client}/{SKILL.md,references/*.md}`, aligned all 16 subagents in `.agents/agents/*-{reviewer,specialist}.md` (`model: inherit`, top-level H1 `# ` headings, `readonly: true` and `disallowedTools` YAML list on reviewers, paired domain skill preloads), and updated `.agents/skills/index.md` and `tools/scripts/review_setup.py`.

## [2026-11-01] flow-archive | Completed and Archived Unified Runtime (`v0.2.0`), Zero-Copy C-API Fastpaths & Multi-Framework Extensions (`unified-runtime-and-framework-extensions`)

- Completed and archived Master PRD `unified-runtime-and-framework-extensions` and all 6 child Flow specs (`runtime-zero-copy-and-driver-parity`, `vite-parity-and-configurable-docs-mcp`, `starlette-extension-and-fastpath`, `fastapi-extension-and-fastpath`, `wsgi-optimizations-and-flask-extension`, `django-extension-and-db-backends`):
  - **Runtime & Zero-Copy C-API (`vertebra-py`, `vertebra.utils.serializers`)**: Added `Request.decode_msgspec`, `Request.decode_pydantic_json`, `WsgiInput.decode_msgspec`, `WsgiInput.decode_pydantic_json`, `hydrate_rows_vectorcall`, `orm_to_struct_vectorcall`, and `decode_grpc_lpm_frames_zero_copy` in `src/rs/vertebra-py/src/msgspec_bridge.rs`; added `ZeroCopyCodecRegistry` in `src/py/vertebra/utils/serializers/_zero_copy.py`; bumped package versions to `0.2.0` and promoted `msgspec>=0.20.0` to core `[project.dependencies]`.
  - **Data, Storage, Caching & gRPC Parity**: Added `vertebra.db.channels.ChannelBus` (`Memory`, `SHM`, `Redis`, `Valkey`, `PostgreSQL`, `Oracle`), `AsyncCacheManager`, `ZeroCopyServiceMixin`, and `ObjectStoreBackend` for Advanced Alchemy (`vertebra.extensions.alchemy.{cache,storage}`) and SQLSpec (`vertebra.extensions.sqlspec.storage` with `SyncStoragePipeline` / `AsyncStoragePipeline` + Arrow/Parquet streaming), Cloud Dialers (`CloudSQLDialer`, `AlloyDBDialer`) & PgBouncer across SQLSpec/Alchemy adapters, and bidirectional Protobuf v3 <-> Route codegen (`ProtobufParser`, `routes_to_proto`, `proto_to_python_routes`) with zero-copy gRPC dispatch.
  - **Vite, Docs Portals & MCP/WebMCP**: Added bundled `@vertebra/client` path helpers (`get_vertebra_client_path`, `get_vertebra_client_vite_aliases`), configurable multi-backend API docs portals (`ScalarDocsConfig`, `SwaggerDocsConfig`, `AsyncAPIDocsConfig`), and route-annotation/controller-driven MCP (`@mcp_tool`, `@mcp_resource`, `@mcp_prompt`, `SkillController`, `PromptController`) and W3C WebMCP (`/.well-known/mcp.json`, `@vertebra/client/webmcp`).
  - **First-Class Starlette, FastAPI, Flask & Django Extensions**: Implemented `vertebra.extensions.starlette`, `vertebra.extensions.fastapi` (with topological `DependencyPlan` compiler), `vertebra.extensions.flask`, and `vertebra.extensions.django` (with `DjangoPlugin`, `DjangoAppConfig`, `@vertebra_view`, `MsgspecView`, `ArrowQuerySet`, and 6 native `vertebra.extensions.django.db.backends.{postgresql,mysql,sqlite3,oracle,mssql,duckdb}` database backends).
- Contracted `.agents/bundles/specs/` back to lazy (`.gitkeep` only).

## [2026-10-31] flow-archive | Completed and Archived Ecosystem Drivers, SQLSpec/Alchemy Realignment, Litestar Extensions & Single-Runtime Documentation (`ecosystem_drivers_and_naming_20261031`)

- Completed and archived Flow `ecosystem_drivers_and_naming_20261031` (7 tasks across Phases 1–4):
  - Standardized packaging and dependencies: pinned `sqlspec>=0.69.0`, `advanced-alchemy>=1.12.0`, and wheel-native `@vertebra/client` TypeScript SDK, resolving PyPI build, sdist, and typing exports.
  - Aligned naming: `MsgPackSerializer` matches `JSONSerializer`, canonical acronyms (`SQLiteConfig`, `DuckDBConfig`, `MSSQLConfig`, `MySQLConfig`, `CloudSQLConfig`, `AlloyDBConfig`, `JSONRPCRouter`).
  - Storage consolidation: deleted shadow abstractions (`UploadTracker`, `StagedUpload`, `StorageRegistry`), delegated storage registry to upstream `advanced-alchemy` / `sqlspec`, and provided drop-in `ObjectStoreBackend` implementations.
  - Database primitives & SQLSpec adapters: implemented Tier-1 `SyncDatabaseConfig` / `AsyncDatabaseConfig` across 6 database engines (`PostgreSQL`, `MySQL`, `SQLite`, `Oracle`, `MSSQL`, `DuckDB`), Tier-2 universal broker proxy `ProxySyncConfig` / `ProxyAsyncConfig`, and CDC `ChangeFeedSyncEventBackend` / `ChangeFeedAsyncEventBackend`.
  - SQLAlchemy & Advanced Alchemy: implemented real SQLAlchemy 2.1 sync/async `+vtb` dialects (`PostgreSQLDialect_vtb`, etc.) with `NullPool` + `LeaseBroker` connection leasing, Cloud SQL / AlloyDB dialers, `Vector` / `BinaryJSON` column types, and thin plugin factories (`create_sqlspec_plugin`, `create_alchemy_plugin`) returning real upstream classes.
  - Litestar ecosystem: nested all `litestar-*` plugins under `vertebra.extensions.litestar.*` (`queues`, `security`, `autowire`, `email`, `mcp`, `vite`, `asyncapi`, `htmx`, `saq`) with zero-friction helper functions (`create_shm_queue_config()`, etc.), keeping built-in framework modules (`vertebra.framework.queues`, `vertebra.framework.security`, `vertebra.framework.autowire`, `vertebra.framework.mcp`) completely decoupled.
  - Unified runtime branding & docs: standardized tagline *"One runtime for Python and TypeScript — backend, frontend, web, gRPC, workers, and standalone apps."* across `README.md`, `docs/index.md`, `zensical.toml`, `pyproject.toml`, `.agents/bundles/index.md`, and `.agents/bundles/product/product.md`, removing "powered by Rust" marketing clichés and eliminating all references to deleted symbols across `docs/` and OKF knowledge chapters.
- Contracted `.agents/bundles/specs/` back to lazy (`.gitkeep` only).

## [2026-10-31] flow-archive | Completed and Archived Documentation Branding, Left Sidebar Formatting, Information Architecture & Educational Content (`fix-docs-organization-and-branding`)

- Completed and archived Flow `fix-docs-organization-and-branding` (6 tasks across Phases 1–2):
  - `zensical.toml`, `src/rs/Cargo.toml`, & `tools/docs/verify_docs_artifacts.py`: Replaced `litestar-org/vertebra`, `vertebra-io/vertebra`, and `https://vertebra.litestar.dev/` with `cofin/vertebra` (`https://github.com/cofin/vertebra` and `https://cofin.github.io/vertebra/`) and added legacy branding strings and empty attribute row patterns to `verify_docs_artifacts.py`.
  - `docs/stylesheets/extra.css` & `tools/docs/templates/python/material/{summary,docstring}/attributes.html.jinja`: Restored consistent 2px left vertical guide bars across top-level and nested `.doc-contents` blocks (overriding Zensical's `(0, 2, 1)` `div.doc-contents:not(.first)` rule with `--vertebra-doc-guide-border`), separated top-level vs nested `h4.doc-heading` typography, and filtered un-annotated `__init__` attributes lacking both `annotation` and `description` while rendering `Default: <code>...</code>` fallbacks.
  - `src/py/vertebra/{agent,db,storage,server,_vertebra.pyi}`: Added PEP 257 field docstrings across all rendered `msgspec.Struct` classes and `.pyi` stub classes and exposed curated `members:` lists in `docs/reference/agent.md` and `docs/reference/db.md`.
  - `zensical.toml`, `docs/usage/{databases,storage,server-and-workers,agent-protocols,index}.md`, & `docs/reference/*.md`: Split `docs/usage/databases-and-storage.md` into dedicated `docs/usage/databases.md` and `docs/usage/storage.md` guides (with a redirect map entry), removed `navigation.indexes` so `.md-sidebar--primary` never renders empty on single-page tabs or hides section `Overview` links, separated `Server & Runtime`, `Databases`, and `Object Storage` into distinct navigation groups, and added architectural overview tables across reference and usage guides.
- Contracted `.agents/bundles/specs/` back to lazy (`.gitkeep` only).

## [2026-10-31] flow-archive | Completed and Archived Native `Route`, `Controller`, `Procedure`, Hierarchical DI & Comprehensive Documentation Synthesis (`native_routing_and_di_20261031`)

- Completed and archived Flow `native_routing_and_di_20261031` (4 tasks across Phases 1–2) and comprehensive documentation synthesis across `README.md`, `docs/`, and `.agents/bundles/`:
  - `knowledge/domains/framework/{routing-and-app,subdomains}.md` & `knowledge/domains/server/worker-and-protocols.md`: Documented canonical `Route`, `Controller`, `Procedure`, `Provide`, `Inject`, `Container`, `DependencyGraph`, `RequestScope` (with zero backward-compatibility aliases for `RouteDef`/`ProcedureDef`/`procedure_*`), hierarchical `App -> Router -> Controller -> Route` dependency/guard/tag composition, and pre-compiled parameter/body/guard/DI dispatch in `vertebra.server._dispatch.compile_handler_spec`.
  - `docs/`: Synthesized plain-English documentation and runnable examples across all subsystems including CTE statement stacking (`QueryPipeline`, `CteStep`, `RelOverlay`), all 9 database drivers, Cloud SQL & AlloyDB dialers, `SQLSpecPlugin` Tier-1/Tier-2 SHM proxy & `SQLSpecStore`, `AlchemyPlugin` & `+vtb` dialects, `GrpcServiceRegistry`, Oracle APEX & 26ai Duality Views, Inertia v3 & HTMX, MCP/A2A/SpecTree/WorkflowEngine, and `@vertebra/client` adapters.
- Contracted `.agents/bundles/specs/` back to lazy (`.gitkeep` only).

## [2026-10-31] flow-archive | Completed and Archived Zensical Polyglot Documentation Pipeline & Auto-SHM Topology (`zensical_docs_20261031`)

- Completed and archived Flow `zensical_docs_20261031` (7 tasks across Phases 1–4) plus automatic `shm` topology selection for multi-worker (`--workers > 1`) non-free-threaded Python runtimes.
- Synthesized OKF v0.2 knowledge across `product/tech-stack.md`, `knowledge/workflow.md`, and `knowledge/architecture/runtime/topologies-and-embedding.md`:
  - `product/tech-stack.md` & `knowledge/workflow.md`: Documented the Zensical documentation pipeline (`zensical.toml`, `docs/stylesheets/extra.css`, Litestar Gold `#EDB641` + Navy `#202235` branding assets in `docs/assets/`, `tools/docs/{scan_public_docstrings,generate_cli_docs,postbuild,verify_docs_artifacts}.py`, custom Griffe `msgspec.Struct` templates, runnable `docs/examples/` tested by `src/py/tests/unit/docs/test_examples.py`, `make docs` / `make docs-serve` / `make docs-check` / `make docs-rust`, and `.github/workflows/docs.yml`).
  - `knowledge/architecture/runtime/topologies-and-embedding.md`: Documented `ServerLaunchEnv.to_environ()` (`src/py/vertebra/cli/_launch.py`) and `ServerConfig::from_env()` (`src/rs/vertebra-http/src/config.rs`) automatic `shm` topology selection when `workers > 1` on POSIX standard GIL Python (`not is_free_threaded_runtime()`), preserving `embedded` for single-worker (`<= 1`) or free-threaded (`3.14t`/`3.15t`) runtimes and honoring explicit `--topology embedded|shm` overrides.
- Contracted `.agents/bundles/specs/` back to lazy (`.gitkeep` only).

## [2026-10-29] flow-resynthesize | Added Release Workflow, `bump-my-version` Automation, and Initial 0.1.0 Changelog

- Configured `bump-my-version` in `pyproject.toml` (`[tool.bumpversion]`) to synchronize release versions across `pyproject.toml`, `src/py/vertebra/__init__.py`, `src/rs/Cargo.toml`, `src/rs/vertebra-bundler/pyproject.toml`, `src/rs/vertebra-storage/pyproject.toml`, and `src/ts/vertebra-client/package.json`.
- Added canonical `make release bump=<patch|minor|major>` and `make pre-release version=<version>` targets in `Makefile` and documented them in `knowledge/workflow.md`.
- Added initial 0.1.0 release changelog in `docs/changelog.rst` and `CHANGELOG.md`.

## [2026-10-29] flow-resynthesize | Synchronized OKF v0.2 Knowledge Bundle for `vertebra-core` PyPI Distribution, Maturin `sdist` Workspace Globs, and Release Workflow

- Resynthesized and validated `index.md`, `product/tech-stack.md`, `knowledge/domains/framework/cli.md`, `knowledge/domains/frontend/packaging-and-pgo.md`, and `.agents/skills/flow-memory-keeper/SKILL.md`:
  - `product/tech-stack.md` and `index.md`: Documented `[project] name = "vertebra-core"` as the PyPI distribution name paired with `[tool.maturin] python-source = "src/py"`, `python-packages = ["vertebra"]`, `module-name = "vertebra._vertebra"`, and `sdist` `include` globs (`src/rs/Cargo.toml`, `src/rs/Cargo.lock`, `src/rs/vertebra-*/**/*`) so `vertebra-core` wheels and source distributions install the top-level `vertebra` import package, and updated `@biomejs/biome` to `^2.6.4`.
  - `knowledge/domains/framework/cli.md`: Documented `package_name="vertebra-core"` and `prog_name="vertebra"` in `vertebra.cli:main`, `importlib.metadata.version("vertebra-core")` in `vertebra.cli.banner.renderer:_get_version`, and `dependencies = ["vertebra-core"]` in `vtb init` project scaffolding.
  - `knowledge/domains/frontend/packaging-and-pgo.md`: Documented `.github/workflows/release.yaml` pre-build setup (`dtolnay/rust-toolchain@stable`, `oven-sh/setup-bun@v2`, `src/ts/vertebra-client` bundle build before `cibuildwheel` and `uv build --sdist`, `CIBW_BEFORE_ALL_LINUX` / `CIBW_ENVIRONMENT_LINUX` container Rust provisioning, and PyPI publication to `https://pypi.org/p/vertebra-core`).

## [2026-10-29] flow-resynthesize | Synchronized OKF v0.2 Knowledge Bundle Across WSGI Batch Runner, Container-Aware SHM Profiles, and Ingress Fast-Paths

- Resynthesized and validated the OKF v0.2 knowledge bundle across `product/`, `knowledge/architecture/`, `knowledge/domains/`, `knowledge/patterns/`, `knowledge/gotchas/`, and `.agents/skills/flow-memory-keeper/SKILL.md` for all runtime, IPC, HTTP ingress, and protocol changes landed in PRs #48, #49, and #50:
  - `architecture/worker/{asgi-and-wsgi-runners,pyo3-ffi-and-batch-dispatch}.md`, `domains/server/worker-and-protocols.md`, `patterns/{rust/ffi-and-gil-discipline,python/hot-path-and-async-dispatch}.md`, and `gotchas/pyo3-and-python-runtime.md`: Documented Rust `drive_wsgi_batch` (`src/rs/vertebra-py/src/wsgi/runner.rs`), zero-copy `WsgiInput::from_snapshot` (`src/rs/vertebra-py/src/wsgi/input.rs`), lazy `WsgiEnviron::with_root_path`, PEP 3333 `_WsgiStartResponse` (`called` vs `headers_sent` state machine and compact binary header encoding), `PreparedRawBody::PyBytes` single-chunk zero-copy submission, `WsgiAdapter.spawn_batch`, `single_root_wsgi` fast-path in `Worker._process_batch` / `dispatch_batch`, `AsgiLifespan` (`scope["state"]`), and `PYO3_PYTHON` / `prepare_embedded_python_env` embedded interpreter resolution.
  - `architecture/ipc/tiered-rings-and-batch-bridge.md`, `architecture/runtime/{topologies-and-embedding,shm-supervision-and-recovery,platform-and-lifecycle}.md`, `domains/ipc/transport-and-rings.md`, `patterns/rust/lock-free-shm-and-sharding.md`, and `gotchas/shm-and-cross-platform-ipc.md`: Documented container-aware `RingPairConfig::for_environment` (`46 MiB/worker` default vs `12 MiB/worker` `compact` profile via `VERTEBRA_SHM_PROFILE` and cgroup/shm detection), dynamic `RingLayout::slots_from_total_size` in `RingPair::from_env`, `VQOV`/`VROV` oversized payload overflow regions with automatic `/tmp/vtb-*` file-backed fallback (`ShmRegion::create_overflow_region`, `VERTEBRA_OVERFLOW_BACKEND`), 1:1 batcher-to-worker shard affinity (`home = shard_idx % worker_count`, per-worker `healthy` / `in_flight` state, and least-loaded spillover in `ShmDispatcher`), and Dekker `cancel_request_sleep` ordering before `request_notifier.clear()` in `Transport::poll_batch`.
  - `architecture/http/{ingress-and-routing,batching-dispatch-and-backpressure}.md` and `domains/http/server-and-dispatch.md`: Documented `RequestBatcher` `in_flight`-gated `tokio::task::yield_now().await` (`dispatcher.in_flight() > 0 || !rx.is_empty()` for zero-yield `c=1` dispatch) and the bodiless request fast-path (`body.size_hint().exact() == Some(0)`) in `response::handle`.

## [2026-10-25] flow-archive | Completed and Archived Two-Phase Rust PGO Pipeline, Python 3.15 Packaging, and Documentation Sync (`pgo-packaging-and-docs-sync`)

- Completed and archived Flow `pgo-packaging-and-docs-sync` (3 tasks across Phases 1–2, final completion commit `78c6c9527fad067e5bc4b8287456d837adc49a4c` on `feat/pgo-packaging-and-docs-sync`).
- Synthesized OKF v0.2 knowledge across `product/` and `knowledge/`:
  - `domains/frontend/packaging-and-pgo.md`, `domains/framework/cli.md`, and `testing/benchmarks-and-free-threaded.md`: Documented the unified two-phase Rust PGO pipeline (`run_pgo_pipeline`, `PgoPipelineResult`, `python -m tools.pgo.rust_pgo`, `make build-pgo`) across `vertebra-http` and `EXTENSION_MANIFESTS` (`_vertebra`, `_bundler`, `_storage`), `resolve_cargo_release_dir` (`CARGO_TARGET_DIR`), single-pass `validate_profdata` (`llvm-profdata show`), `run_default_pgo_workload`, `BuildTarget` `SUPPORTED_PYTHON_VERSIONS` (`3.12`–`3.15`, `3.14t`, `3.15t`) and Linux `musl` targets, and `StandaloneBuilder` / `vtb build` `--entrypoint`, `--profdata`, and `--server-binary` staging (`VERTEBRA_SERVER_BINARY` + `VERTEBRA_APP` launch bootstrap).
  - `product/tech-stack.md`, `product/product-guidelines.md`, `knowledge/workflow.md`, `architecture/runtime/platform-and-lifecycle.md`, `patterns/python/hot-path-and-async-dispatch.md`, `gotchas/pyo3-and-python-runtime.md`, and `testing/polyglot-test-matrix.md`: Synchronized `tools/pgo` lint/type-check coverage, all 12 `pyproject.toml` SQLAlchemy dialect entry points (`{Postgres,Oracle,Mysql,Sqlite,Mssql,Duckdb}Dialect`), the `sqlspec` extra and mypy overrides, and live runtime symbols (`BatchDispatchPlan.sync_native`, `state.health_endpoint.try_intercept(req_path)`, `drive_asgi_batch(single_root_asgi.dispatch_app, batch, ...)`).
- Contracted `.agents/bundles/specs/` back to lazy (`.gitkeep` only).

## [2026-10-24] flow-archive | Completed and Archived Runtime Throughput, Multi-Core Scaling, and Python 3.15 Optimizations (`runtime-throughput-and-315`)

- Completed and archived Flow `runtime-throughput-and-315` (12 tasks across Phases 1–5, final completion commit `bc4cfe57817008626ac98602488242a76bf4e580` on `perf/runtime-throughput-and-315`).
- Synthesized OKF v0.2 knowledge across `product/`, `architecture/`, `domains/`, `patterns/`, `testing/`, and `gotchas/`:
  - `architecture/ipc/*`, `domains/ipc/transport-and-rings.md`, `patterns/rust/lock-free-shm-and-sharding.md`, and `gotchas/shm-and-cross-platform-ipc.md`: Unbounded deadlock-free `BatchBridge` with zero-copy `ResponseSink` (`DirectResponseSink`), right-sized `TieredRingConfig` (`4 KiB×512`, `32 KiB×128`, `256 KiB×32`, `2 MiB×8`; `30 MiB`), `ShmHeader` `ABI_VERSION = 3` Dekker `SeqCst` `consumer_sleeping` and `active_flags` handshakes, generation-synchronized cached SPSC heads, `SpscRing::drain_with`, and `CacheLine<AtomicU64>`-padded `MpmcRing` sequence slots.
  - `architecture/runtime/*`, `architecture/http/*`, and `domains/http/server-and-dispatch.md`: Explicit `tokio::runtime::Builder` honoring `VERTEBRA_THREADS`, `SO_REUSEPORT` multi-process `EmbeddedSupervisor` (`VERTEBRA_WORKERS > 1`) with shared `ShmRegion` `LeaseBroker`, lock-free `BackpressureController` 16-bucket atomic sliding-window circuit breaker, lock-free `ShmDispatcher` per-batch routing, `try_recv()` burst-draining `RequestBatcher`, and wired Rust ingress offloads (static assets, CORS, compression, OpenAPI schema cache, and cached `HealthEndpoint`).
  - `architecture/worker/*`, `domains/server/worker-and-protocols.md`, `patterns/python/*`, `patterns/rust/ffi-and-gil-discipline.md`, and `gotchas/pyo3-and-python-runtime.md`: Python `3.15` / `3.15t` specializations (PEP 810 `__lazy_modules__`, PEP 782 `PyBytes::new_with`, PEP 814 `PyFrozenDict::empty`, `PyUnstable_SetImmortal`, conditional `freelist` / `immutable_type`), anchored `Request.__getbuffer__` (`PyBuffer_FillInfo` borrowed `slf.as_ptr()`), Rust `AsgiProtocol` + `drive_asgi_batch` and lazy `RsgiScope` + `drive_rsgi_batch` (`PyIter_Send` + `_SteppedCoroutine`), inline vectorized `"sync_inline"` batch dispatch (`submit_batch_raw`), and single-thread `uvloop` `loop.add_reader` Dekker polling.
  - `domains/extensions/litestar.md`, `domains/framework/*`, `domains/frontend/packaging-and-pgo.md`, and `testing/*`: `LitestarFastPathBypass.dispatch` and `offload_litestar_middlewares` / `LitestarOffloadPlan` v2/v3 parity (`resolve_opts`, `ownership_layers`, `zstd_compress_level`, guarded `compression_config` clearing), `vtb run` env-var preservation, `CARGO_TARGET_DIR` / `st_mtime_ns` `find_server_binary`, `Python 3.15`/`3.15t` CI/wheel/benchmark matrix, and static subagent tool validation in `areas.toml`.
- Contracted `.agents/bundles/specs/` back to lazy (`.gitkeep` only).

## [2026-10-23] flow-archive | Completed and Archived SHM Data Plane PRD (`shm-native-drivers-and-broker-proxy`), 5 Child Specs, and 16 Tasks

- Completed and archived Master PRD `shm-native-drivers-and-broker-proxy` and all 5 child specs (`rust-native-shm-drivers`, `rust-native-enterprise-olap-drivers`, `shm-broker-driver-proxy`, `shm-database-broker`, `redis-valkey-resp3` — 16 tasks total) following merge of PR #42 and PR #43 into `main` (`034703d`).
- Synthesized 5-dimension OKF v0.2 knowledge across `product/` and nested `knowledge/` chapters:
  - `domains/data/database-drivers-and-dialers.md`: Documented Tier-1 Rust-native SHM drivers in `src/rs/vertebra-db` and `vertebra.db` across PostgreSQL, MySQL, SQLite, Oracle (pure-Rust Thin TNS/TTC, Autonomous Database Wallet extraction, 26ai `VECTOR`/`OSON`), SQL Server (`tiberius` TDS, `@P1` rewriting), DuckDB (single-writer SHM broker host, DuckDB 2.0-safe startup pragmas), Redis/Valkey RESP3 (`CLIENT TRACKING`, Sentinel, Cluster CRC16 slot routing, Memorystore IAM auth), `ArrowCursor` over `ShmRegion` Arrow IPC, and `CloudSqlDialer` (`:3307`) / `AlloyDbDialer` (`:5433`).
  - `domains/data/broker-and-shm-leases.md` & `architecture/runtime/shm-supervision-and-recovery.md`: Documented single-FD co-location of `ShmStatementCache` and `ShmChannelSlot` inside `ShmLeaseTable` (`VERTEBRA_BROKER_SHM_FD` / `VERTEBRA_BROKER_SHM_SIZE`, `LeaseBroker.statement_cache()`), supervisor worker-exit lease reclamation (`reclaim_worker`), and cross-process channel pub/sub.
  - `domains/extensions/sqlspec-alchemy-and-dialects.md`: Documented Tier-1 `SyncAdapterConfig` / `AsyncAdapterConfig` (`{Postgres,Mysql,Sqlite,Oracle,Mssql,Duckdb}{Sync,Async}Config`), Tier-2 `SqlSpecProvider` / `ProxySyncDriver` / `ProxyAsyncDriver` with threshold-based `ShmRegion` Arrow IPC transfer (`SharedArrowBuffer`, `SharedArrowTable`), and all six `+vtb` `DialectDescriptor` instances (`{Postgres,Oracle,Mysql,Sqlite,Mssql,Duckdb}Dialect`).
  - `testing/agentic-review.md` & `product/*`: Documented `PY-07` (clean domain naming without brand/mechanism prefixes or compatibility aliases) and `PY-08` (`vertebra.typing` boundary check) in `tools/scripts/review_setup.py`, and reconciled all moved source paths across the bundle.
- Contracted `.agents/bundles/specs/` back to lazy (`.gitkeep` only).

## [2026-10-23] flow-archive | Completed and Archived 5 Master PRDs, 21 Child Chapters, and 126 Tasks

- Completed and archived 5 Master PRDs (`vertebra-design-quality`, `vertebra-shm-connection-broker`, `vertebra-native-db-drivers`, `vertebra-queues-and-messaging`, `vertebra-agentic-harness`) and all 21 child specs (126 tasks total) following merge into `main` across PRs #36, #37, #38, #39, and #40.
- Synthesized 5-dimension OKF v0.2 knowledge across nested domain chapters:
  - `domains/agent/mcp-and-a2a-protocol-harness.md`: MCP `2026-07-28` SSE/Stdio transports, A2A `1.0` JCS/RFC 8785 Ed25519/ECDSA signed agent cards, durable DAG workflows, SpecTree execution, Rust PyO3 cryptographic fastpaths, and GenAI telemetry.
  - `domains/data/broker-and-shm-leases.md`: Cross-process SHM connection lease broker (`ShmLeaseTable`, `ShmStatementCache` in `vertebra-core`), PyO3 `NativeConnectionBroker`, and Python `ConnectionBroker` / `SyncPool` / `LoopShardedResource`.
  - `domains/framework/distributed-queues-and-email.md`: Distributed queue backends (`MemoryQueueBackend`, `RedisQueueBackend`, `ValkeyQueueBackend`, `PostgresQueueBackend`, `OracleQueueBackend`), `CronScheduler` leader election, `WorkerRunner` with priority lanes, and transactional email providers (`SmtpProvider`, `ResendProvider`, `SendGridProvider`, `MailgunProvider`, `SesProvider` with native Rust SigV4 signing).
  - `domains/extensions/oracle-apex.md`: Creative Vertebra + Oracle APEX integration (`ApexIslandRegistry`, `LiveState` two-way binding, `ApexOfflineOutbox` with monotonic HLC sync, `ApexFederatedQuery`, `ApexSessionAuth` HMAC handoff, and `ApexGateway` MCP/A2A synthesis with `ConfirmationGate`).
  - `domains/data/database-drivers-and-dialers.md`: Native network database drivers for Postgres, MySQL, Oracle, Redis, Valkey, and DataFusion Arrow streaming.
  - `gotchas/pyo3-and-python-runtime.md`: `AsyncLogCore` batched newline-delimited JSONL buffer handling, TNS descriptor validation, and async socket teardown timeouts.
- Contracted `.agents/bundles/specs/` so the specifications tree remains lazy until new active specs are planned.

## [2026-10-22] flow-plan | Materialized 5 Master PRDs, 21 Child Chapters, and 126 Refined Task Worksheets

- Authored and validated 5 Master PRDs (`vertebra-design-quality`, `vertebra-agentic-harness`, `vertebra-native-db-drivers`, `vertebra-shm-connection-broker`, `vertebra-queues-and-messaging`), 21 child Flow specs, and 126 refined task worksheets (154 markdown files) under `.agents/bundles/specs/`.
- Resolved architectural decisions `D1`–`D4` in `vertebra-design-quality/spec.md`: retained `vertebra-core/src/guards.rs` (`JwtValidator`, `RateLimiter`), `AsyncLogCore`, `W3cTraceContext`, and `sanitize_sql_statement` for Rust ingress pre-dispatch and PyO3 Litestar/native boosting alongside drop-in standard library `logging.Handler` and Litestar `LoggingConfig` integration; extracted full feature roadmaps into dedicated PRDs (`vertebra-agentic-harness`, `vertebra-native-db-drivers`, `vertebra-shm-connection-broker`, `vertebra-queues-and-messaging`).

## [2026-10-22] flow-resynthesize | OKF v0.2 Knowledge Bundle Audit & Full Resynthesis

- Audited and resynthesized the entire OKF v0.2 bundle against the consolidated repository baseline (`vertebra-core`, `vertebra-http`, `vertebra-py`, `vertebra-bundler`, `vertebra-storage`, `vertebra-db`, `src/py/vertebra/`, and `src/ts/vertebra-client/`).
- Reconciled all path moves (`builder/` $\to$ `build/`, `builder/_codegen/` $\to$ `build/codegen/`, `server/_asgi/_litestar_*.py` $\to$ `extensions/litestar/*.py`, `build/{standalone,onefile}.py` $\to$ `build/packaging/`) and canonical domain renames (`ObjectStore`/`SyncStore`/`AsyncStore`, `SQLSpecPlugin`, `AlchemyPlugin`, `BrokerPlugin`, `LitestarPlugin`, `ChannelsBackend`, `MemoryStore`, `WorkerRunner`/`LaneConfig`, `SseClient`/`WebSocketClient`, `LiveState`/`LiveStateClient`, `TraceContext`, `LogSink`/`LogHandler`, `CgroupQuota`, `LameDuckController`, `ZPagesRegistry`).
- Rewrote `product/*`, `knowledge/workflow.md`, `architecture/**` (adding `runtime/platform-and-lifecycle.md` and `http/protocol-and-offload-modules.md`), `domains/**` (adding `storage/object-storage.md`, `data/database-drivers-and-dialers.md`, `data/query-pipeline-cdc-and-tenancy.md`, `telemetry/logging-tracing-and-genai.md`, `extensions/{litestar,sqlspec-alchemy-and-dialects,oracle-apex,grpc}.md`, `framework/{subdomains,live-state,cli}.md`, `server/worker-lanes.md`, and `frontend/{packaging-and-pgo,wasm}.md`), `patterns/**` (adding `python/serialization-and-utils.md`), `testing/**`, `gotchas/**`, and all category/root `index.md` files with zero broken links or missing source resources.

- **2026-10-21 (`flow-archive`: `litestar-ecosystem-and-agentic-runtime`)**: Archived completed spec `litestar-ecosystem-and-agentic-runtime` (6 tasks closed) and synthesized multi-dimension OKF knowledge across `architecture/worker/asgi-and-wsgi-runners.md` (`extensions/litestar/{router,offload,fastpath,plugin,channels,store}.py`, `extensions/worker/runner.py`) and `architecture/http/ingress-and-routing.md` (`cors.rs`, `discovery.rs`, `mcp_bridge.rs`).
- **2026-10-21 (`flow-archive`: `realtime-sse-websocket-pubsub`)**: Archived completed spec `realtime-sse-websocket-pubsub` (5 tasks closed) and synthesized multi-dimension OKF knowledge across `architecture/http/batching-dispatch-and-backpressure.md` (`sse.rs`, `ws_mount.rs`, `pubsub.rs`, `_asgi/_sse.py`, `_asgi/_websocket.py`, `try_acquire_for_path` priority route bypass) and `patterns/typescript/client-and-adapter-subpaths.md` (`sse.ts`, `websocket.ts`).
- **2026-10-21 (`flow-archive`: `storage-packaging-and-pgo`)**: Archived completed spec `storage-packaging-and-pgo` (4 tasks closed) and synthesized multi-dimension OKF knowledge (`vertebra-storage` `_storage` PyO3 crate, `ObjectStore`, `SQLSpecPlugin`, `AlchemyPlugin`, multi-target codegen with Litestar v2/v3 parameter markers and `external_name` wire aliases, `StandaloneBuilder`/`OnefileBuilder` `VTB1FILE` stub, and Rust/Python PGO tooling).
- **2026-10-21 (`flow-archive`: `nextgen-wire-protocols`)**: Archived completed spec `nextgen-wire-protocols` (4 tasks closed) and synthesized multi-dimension OKF knowledge into `architecture/http/ingress-and-routing.md` (`h2c.rs` prior-knowledge + `Upgrade: h2c`, `tls.rs` mTLS `WebPkiClientVerifier` + leaf Subject CN extraction + XFCC/IAP headers, `http3.rs` QUIC listener, `grpc.rs` 5-byte LPM framing, and `vertebra.extensions.grpc`).
- **2026-10-21 (`flow-archive`: `gateway-protocols-and-ingress-fastpaths`)**: Archived completed spec `gateway-protocols-and-ingress-fastpaths` (5 tasks closed) and synthesized multi-dimension OKF knowledge across `architecture/http/ingress-and-routing.md` (`health.rs`, `payload.rs`, `multipart.rs`, `access_log.rs`, `compression.rs`, `profile.rs`, `assets.rs` precompressed sidecar `ETag`/`Vary`) and `architecture/worker/asgi-and-wsgi-runners.md` (RSGI 1.6 `rsgi_scope.rs`/`rsgi_protocol.rs`/`_rsgi/_adapter.py`, 20-slot `AsgiScope`, `asgi_bridge.rs`, and `wsgi_worker.rs`).
- **2026-10-21 (`flow-archive`: `transport-streaming-and-314t-core`)**: Archived completed spec `transport-streaming-and-314t-core` (5 tasks closed) and synthesized multi-dimension OKF knowledge across `architecture/runtime/topologies-and-embedding.md`, `architecture/ipc/tiered-rings-and-batch-bridge.md`, `architecture/http/batching-dispatch-and-backpressure.md`, `architecture/worker/pyo3-ffi-and-batch-dispatch.md`, `domains/{ipc/transport-and-rings,http/server-and-dispatch,server/worker-and-protocols}.md`, `patterns/rust/{ffi-and-gil-discipline,lock-free-shm-and-sharding}.md`, and `gotchas/pyo3-and-python-runtime.md`.
- **2026-10-20 (`flow-setup` / `align`)**: Revalidated OKF v0.2 bundle integrity across all 36 bundle files (`0` broken links, `0` missing sources, `0` legacy tracker artifacts), backfilled strict OKF v0.2 frontmatter (`type:`, `title:`, `description:`, `status: stable`, `tags:`, and `sources: - resource: ...`) across 29 nested `knowledge/` chapters and sub-indexes, synchronized `knowledge/workflow.md` with `flow-template-v2` sections (`Low-Signal Test & Gate Policy`, `Commits & Checkpoints`, `Knowledge Lifecycle`, `Quality Gates`), and updated `.agents/setup-state.json`.
- **2026-10-20 (`flow-sync`)**: Synchronized repository baseline to `main` (`0e84dc3` / PR #6 merge) and reconciled OKF v0.2 knowledge base: resolved YAML frontmatter syntax errors in Rust and Python pattern chapters, updated `JSONSerializer` documentation to reflect the clean removal of `orjson` in favor of direct `msgspec.json` + stdlib `json`, clarified TypeScript adapter source paths, and aligned test directory references across operational skills.
- **2026-10-19 (`flow-setup` / `resynthesize`)**: Initialized and resynthesized OKF v0.2 bundle (`product/`, nested `knowledge/{architecture/{runtime,ipc,http,worker,frontend},domains/{framework,server,ipc,http,frontend},patterns/{python,rust,typescript},testing,gotchas}/`, and customized polyglot `knowledge/workflow.md`) synchronized with commit `6dc6be4`. Configured `shared` Git tracking policy and cleaned up legacy `.agent/`, `.gemini/`, and `.beads/` artifacts.

## Chapter 34 drain-frame review repair

- Adopted the correctness finding that truncated shutdown-drain frames require Protocol rather than Transport classification. Commit `caa4a0fb` passes 49 canonical tests, Clippy, package formatting and diff checks; Red reproduces the original wrong category.

## [2026-11-12T21:09:21.836151+00:00] chapter-closeout | PostgreSQL session

- Correctness and fresh quality reviews approve `f58aa695..6e4e6bfc`; chapter 34 is completed at reviewed checkpoint `6e4e6bfc`. All 49 canonical wire tests and focused native checks pass. The next chapter is ch35; 41 tasks across 12 chapters remain.
