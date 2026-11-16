# Framework Extensions

`vertebra.extensions` connects Vertebra's native HTTP server, shared-memory worker pools, and multicloud object storage to Litestar, SQLSpec, SQLAlchemy / Advanced Alchemy, gRPC, and Oracle APEX.

=== "uv"

    ```bash
    uv add "vertebra-core[sqlspec,alchemy]"
    ```

=== "pip"

    ```bash
    pip install "vertebra-core[sqlspec,alchemy]"
    ```

=== "pdm"

    ```bash
    pdm add "vertebra-core[sqlspec,alchemy]"
    ```

=== "poetry"

    ```bash
    poetry add "vertebra-core[sqlspec,alchemy]"
    ```

## Litestar Integration & Middleware Offload (`vertebra.extensions.litestar`)

Vertebra provides first-class support for Litestar applications. Use `VertebraPlugin` and `VertebraConfig` to run a Litestar application on Vertebra's native HTTP ingress while offloading CORS, response compression, and static asset serving directly to the native server:

```python
from litestar import Litestar, get
from vertebra.extensions.litestar import VertebraConfig, VertebraPlugin


@get("/hello")
async def hello() -> dict[str, str]:
    return {"hello": "litestar"}


app = Litestar(
    route_handlers=[hello],
    plugins=[VertebraPlugin(VertebraConfig())],
)
```

`VertebraConfig(swap_memory_response_cache=True)` opts into replacing Litestar's default response cache store with Vertebra's `MemoryStore` (off by default); attach `create_channels_plugin(...)` for realtime pub/sub channels.

### CLI Integration (`litestar run` Override & `vtb` CLI Extensions)

- **`litestar run` CLI Override**: `VertebraPlugin` implements Litestar's `CLIPluginProtocol` (`on_cli_init(cli)` and `server_lifespan(app)`). When `VertebraPlugin` is registered on a `Litestar` app, running `litestar --app app:app run` launches the native Vertebra HTTP/IPC server supervisor while supporting all standard `litestar run` flags (`--host`, `--port`, `--wc` / `--web-concurrency`, `--reload`, `--debug`, `--pdb`) plus Vertebra runtime options (`--workers`, `--topology`, `--threads`, `--loop`, `--drain-timeout`, `--jit` / `--no-jit`, `--no-banner`).
- **Auto-Integration on `vtb run` / `vtb dev`**: Running an unmodified `Litestar`, `FastAPI`, `Starlette`, or `Flask` application with `vtb run app:app` (or `vtb --app app:app run`) automatically attaches and finalizes the corresponding `VertebraPlugin` (`ensure_vertebra_integration`) when native acceleration is enabled.
- **Pluggable `vtb` CLI (`CLIPlugin`, `VertebraExtensionGroup`, `VertebraEnv`)**: Both `vertebra.App(plugins=[...])` and host-framework plugins implementing `on_cli_init(cli)` can register custom subcommands on `vtb`. Command callbacks declaring `app` or `env: VertebraEnv` parameters automatically receive the resolved application instance and `VertebraEnv` context when invoked via `vtb --app app:app <subcommand>`.

### Fast-Path Dispatch & Native Middleware Offload

- **Route Compiler**: `RouteCompiler` extracts Litestar routes into an optimized route table matched by the native HTTP router.
- **Fast-Path Dispatch**: `FastPath` dispatches simple handlers directly, bypassing ASGI middleware layers when routes have no custom guards or intermediate transforms.
- **Realtime Channels**: `create_channels_plugin` (`ChannelsBackend`) integrates Litestar `ChannelsPlugin` with the engine-neutral `ChannelBus` across Memory, Valkey (`vertebra._valkey`), Redis (`vertebra._redis`), PostgreSQL (`vertebra._postgres`), and Oracle (`vertebra._oracle`) over `SyncNotificationSource` and `AsyncNotificationSource` with origin-node deduplication and cancellation-shielded stream cleanup.
- **Response & Key-Value Stores**: `ValkeyStore` (`vertebra._valkey` with RESP3 `CLIENT TRACKING` L1 invalidation), `RedisStore` (`vertebra._redis`), `MemoryStore`, and `ResponseCacheStore` implement `litestar.stores.base.Store` with prefix-scoped `SCAN` + `UNLINK` deletion.

### Litestar Ecosystem Plugins (`vertebra.extensions.litestar.*`)

Vertebra provides modular, independently installable extensions for the Litestar ecosystem under `vertebra.extensions.litestar`:

| Plugin | Module | Optional Extra | Description |
| --- | --- | --- | --- |
| **Queues** | `vertebra.extensions.litestar.queues` | `queues` | `litestar-queues>=0.14.0` integration: `StoreQueueBackend` (`StoreQueueBackendConfig`, `create_store_queue_config`) over `AsyncQueueStore`, `SHMQueueBackend` (`SHMQueueBackendConfig`, `create_shm_queue_config`), `SHMExecutionBackend`, `create_sqlspec_queue_config`, and `ContainerTaskDependencyProvider`. |
| **Security** | `vertebra.extensions.litestar.security` | `security` | `JWTAuthenticationMechanism` accelerated by native `JWTGuard`, `IngressIdentityMechanism` for trusted mTLS/XFCC headers, and `RateLimitGuard`. |
| **Autowire** | `vertebra.extensions.litestar.autowire` | `autowire` | `AutowireBridgeIntegration` running controller discovery deterministically before the route compiler. |
| **Email** | `vertebra.extensions.litestar.email` | `email` | `SESEmailBackend` with native AWS SigV4 signing (`py_sign_aws_sigv4_headers`) requiring zero `botocore` dependency. |
| **MCP** | `vertebra.extensions.litestar.mcp` | `mcp` | `ObjectStoreSkillSource`, `WellKnownAgentCardOffload` with ETag/304 caching, and `JSONRPCFrameAccelerator`. |
| **Vite** | `vertebra.extensions.litestar.vite` | `vite` | `VitePlugin` integration with automatic resolution of wheel-native `@vertebra/client` assets. |
| **AsyncAPI** | `vertebra.extensions.litestar.asyncapi` | `asyncapi` | Event and schema documentation for background tasks and WebSocket channels. |
| **HTMX** | `vertebra.extensions.litestar.htmx` | `htmx` | HTMX response headers, partials, and push-URL directives. |

Configure Litestar integration with `VertebraPlugin` and `VertebraConfig` (and attach any optional `vertebra.extensions.litestar.*` helper plugins listed above):

```python
from litestar import Litestar
from vertebra.extensions.litestar import (
    VertebraConfig,
    VertebraPlugin,
)

app = Litestar(
    plugins=[
        VertebraPlugin(VertebraConfig()),
    ],
)
```

## SQLSpec 0.71.1+ Per-Engine Adapters, Stores & SHM Proxy (`vertebra.extensions.sqlspec`)

Vertebra provides first-class support for `sqlspec>=0.71.1` across 8 per-engine adapter packages (`vertebra.extensions.sqlspec.adapters.{postgres,sqlite,mysql,oracle,mssql,duckdb,bigquery,spanner}`), native statement pipelining, shared-memory connection pooling, and Google ADK / Litestar / Events stores:

=== "uv"

    ```bash
    uv add "vertebra-core[sqlspec,adk]"
    ```

=== "pip"

    ```bash
    pip install "vertebra-core[sqlspec,adk]"
    ```

=== "pdm"

    ```bash
    pdm add "vertebra-core[sqlspec,adk]"
    ```

=== "poetry"

    ```bash
    poetry add "vertebra-core[sqlspec,adk]"
    ```

```mermaid
flowchart LR
    App["Application / Litestar / ADK"]
    Cfg["<Engine>SyncConfig / <Engine>AsyncConfig"]
    Drv["BaseSyncDriver / BaseAsyncDriver"]
    Stores["DataDictionary + ADK / Events / Litestar Stores"]
    Obj["ObjectStoreBackend + ArtifactReference"]
    App --> Cfg
    Cfg --> Drv
    Cfg --> Stores
    Stores --> Obj
```

- **Per-Engine Adapters & `DataDictionary`**: Dedicated `vertebra.extensions.sqlspec.adapters.<engine>` packages (`adapter_name = "vertebra_<engine>"`) export `PostgresSyncConfig` / `PostgresAsyncConfig`, `MySQLSyncConfig` / `MySQLAsyncConfig`, `SQLiteSyncConfig` / `SQLiteAsyncConfig`, `OracleSyncConfig` / `OracleAsyncConfig`, `MSSQLSyncConfig` / `MSSQLAsyncConfig`, `DuckDBSyncConfig` / `DuckDBAsyncConfig`, `BigQuerySyncConfig` / `BigQueryAsyncConfig`, and `SpannerSyncConfig` / `SpannerAsyncConfig` (`SpangresSyncConfig` / `SpangresAsyncConfig`), each paired with `<Engine>SyncDataDictionary` and `<Engine>AsyncDataDictionary` (`get_version`, `get_tables`, `get_columns`, `get_indexes`, `get_foreign_keys`, `get_optimal_type`, `get_feature_flag`).
- **Google ADK, Events & Litestar Stores (`*_store_class` Wiring)**: Every engine config binds explicit `adk_session_store_class`, `adk_memory_store_class`, `events_store_class`, and `litestar_store_class` attributes directly onto upstream `sqlspec>=0.71.1` store bases (`<Engine>{Sync,Async}ADKStore`, `<Engine>{Sync,Async}ADKMemoryStore`, `<Engine>{Sync,Async}ADKArtifactStore`, `<Engine>EventQueueStore`, `<Engine>LitestarStore`), offloading ADK binary artifacts through `vertebra.storage.ArtifactReference` (`canonical_uri`, `version`, `mime_type`, `size_bytes`, `etag`, `sha256_hex`) without storing raw blobs in database rows.
- **Shared-Memory Proxy**: `ProxySyncConfig` and `ProxyAsyncConfig` (`ProxySyncDriver`, `ProxyAsyncDriver`) lease pooled connections over shared memory from the parent process so multi-worker deployments never multiply connection counts.
- **Native Statement Pipelining**: SQLSpec `execute_stack` uses `SyncPipelineCapable` / `AsyncPipelineCapable` through `driver.pipeline()` for supported stacks and returns one ordered result per operation. Other stacks use sequential execution with transaction boundaries.
- **Change Feed & Durable Checkpointing**: `ChangeFeedSyncEventBackend` and `ChangeFeedAsyncEventBackend` implement `sqlspec.extensions.events` protocols with durable checkpoint persistence and automatic `EventQueueStore` / object-store spillover for payloads exceeding `8,000` bytes (`PostgresSyncEventsBackend` / `PostgresAsyncEventsBackend` over `LISTEN/NOTIFY` and `OracleSyncEventsBackend` / `OracleAsyncEventsBackend` over TxEventQ / `DBMS_AQ`).
- **Object Storage & Arrow Streaming**: `ObjectStoreBackend(ObjectStoreBase)` integrates with `sqlspec.storage.storage_registry`, providing native async/sync `stream_arrow_*`, `copy_*`, `move_*`, and `sign_*` presigned URLs over `vertebra._storage`.

```python
from vertebra.extensions.sqlspec import (
    PostgresAsyncConfig,
    SQLiteSyncConfig,
    create_sqlspec_plugin,
)

sqlite_cfg = SQLiteSyncConfig(path=":memory:")
pg_cfg = PostgresAsyncConfig(dsn="postgresql://app_user:secret@localhost:5432/app")
sqlspec_plugin = create_sqlspec_plugin(sqlite_cfg, pg_cfg)
```

## SQLAlchemy 2.0+, Advanced Alchemy 1.12+, Native Caching & `+vtb` Dialects (`vertebra.extensions.alchemy`, `vertebra.extensions.sqlalchemy`)

`vertebra.extensions.alchemy` and `vertebra.extensions.sqlalchemy` provide drop-in SQLAlchemy 2.0.36+ and Advanced Alchemy 1.12+ support backed by Vertebra's native drivers, connection leasing, and multi-tier caching:

- **Dialects & Broker Pooling**: Registers all 36 `+vertebra`, `+vtb`, `+vertebra_async`, and `+vtb_async` URL schemes (`Sync<Engine>Dialect` / `Async<Engine>Dialect` across `postgresql`, `mysql`, `sqlite`, `oracle`, `mssql`, `duckdb`, `bigquery`, `spanner`, and `spangres`) over standalone [`vertebra.db.dbapi`](../reference/db.md) PEP 249 interfaces (`SyncDBAPIModule` and `AsyncDBAPIModule`, adapted for async SQLAlchemy by `SQLAlchemyAsyncDBAPI`). `bigquery+vtb://`, `spanner+vtb://`, and `spangres+vtb://` use `NullPool`, `?shm_broker=true` selects `NullPool` on relational dialects, `:memory:` SQLite uses `StaticPool`, and relational file/TCP URLs default to `QueuePool` / `AsyncAdaptedQueuePool`, while `create_sync_alchemy_config` / `create_async_alchemy_config` wire `BrokerPool` / `AsyncBrokerPool` when a shared-memory broker is attached.
- **Native Caching (`SyncCacheBackend`, `AsyncCacheBackend`, `ShmCache`)**: `SyncCacheBackend` (implementing Advanced Alchemy's `SyncCacheRegionProtocol`) and `AsyncCacheBackend` support `shm://<segment>` (`vertebra._vertebra.ShmCache` seqlock POSIX shared memory + `ShmLeaseTable` ring-buffer L1 invalidation on `b"vtb:cache:inv"`), `valkey://` (`vertebra._valkey`), `redis://` (`vertebra._redis`), `s3://`/`gs://`/`az://`/`r2://`/`oci://`/`file://` (`vertebra._storage` `VTBSTORE\x01`), and `memory://`. `create_cache_config(use_dogpile=False)` wires Advanced Alchemy `CacheConfig(region_factory=...)` and `AsyncCacheManager` directly without third-party dependencies, while `use_dogpile=True` (`vertebra-core[dogpile]`) registers optional `dogpile.cache` backend plugins (`vertebra.shm`, `vertebra.memory`, `vertebra.valkey`, `vertebra.redis`, `vertebra.storage`). SQLAlchemy `CompiledCache` and `ResultCache` reuse the same backends.
- **Type System**: Native `Vector(dimensions)` for vector embeddings and `BinaryJSON` for PostgreSQL `JSONB`, SQLite `JSONB`, and Oracle `OSON`.
- **Object Storage Attachments**: `ObjectStoreBackend` implements Advanced Alchemy's `advanced_alchemy.types.file_object.StorageBackend`, enabling transactional file attachments on models via `FileObject` and `StoredObject`.
- **Alembic Custom Operations & Autogenerate**: Registers `+vertebra`/`+vtb` `DefaultImpl` subclasses (`PostgreSQLAlembicImpl`, `MySQLAlembicImpl`, `SQLiteAlembicImpl`, `OracleAlembicImpl`, `MSSQLAlembicImpl`, `DuckDBAlembicImpl`, `BigQueryAlembicImpl`, `SpannerAlembicImpl` batching `UpdateDatabaseDdl` LROs) and autogenerate hooks for `Vector`, `BinaryJSON`, `CreateVectorIndexOp`, `DropVectorIndexOp`, `CreateViewOp`, and `DropViewOp`.

BigQuery, Spanner, and Spangres DBAPI connections use statement autocommit. Their
`commit()` and `rollback()` calls do not group or undo completed statements;
setting `autocommit=False` raises `NotSupportedError`.

```python
from vertebra.extensions.alchemy import (
    ObjectStoreBackend,
    create_alchemy_plugin,
    create_async_alchemy_config,
    create_cache_config,
)
from vertebra.storage import SyncObjectStore

cache_cfg, cache_mgr = create_cache_config("shm:///vtb-app-cache")
alchemy_config = create_async_alchemy_config(
    connection_string="sqlite+vtb_async:///:memory:",
    cache_config=cache_cfg,
)
alchemy_plugin = create_alchemy_plugin(alchemy_config)

store = SyncObjectStore("memory://")
backend = ObjectStoreBackend(store=store, prefix="uploads")
```

## Django 6.0+ Databases, Caching, Tasks & Channels (`vertebra.extensions.django`)

Vertebra supports Django `6.0+` (`vertebra-core[django]`) with 5 native `DATABASES` engines over `vertebra.db.dbapi` (`vertebra.extensions.django.db.backends.{postgresql,mysql,oracle,sqlite3,spanner}`, with `spanner` guarded by `django-google-spanner`), native `CACHES` via `vertebra.extensions.django.cache.CacheBackend` (`SyncCacheBackend` / `AsyncCacheBackend`), `django.tasks` background execution via `vertebra.extensions.django.tasks.TaskBackend` (`enqueue` via `SyncQueueStore`, `aenqueue` via `AsyncQueueStore`, and `python manage.py vertebra_worker` with `Container.scope()` DI), and distributed `CHANNEL_LAYERS` via `vertebra.extensions.django.channels.ChannelLayer` across Memory, Valkey, Redis, PostgreSQL, and Oracle. BigQuery, DuckDB, and SQL Server are not supported as Django `DATABASES` backends and raise `ImproperConfigurationError` during startup validation (use SQLAlchemy or SQLSpec instead).

```python
DATABASES = {
    "default": {
        "ENGINE": "vertebra.extensions.django.db.backends.postgresql",
        "OPTIONS": {"dsn": "postgresql://app_user:secret@localhost:5432/app"},
    }
}

CACHES = {
    "default": {
        "BACKEND": "vertebra.extensions.django.cache.CacheBackend",
        "LOCATION": "shm:///vtb-django-cache",
    }
}

TASKS = {
    "default": {
        "BACKEND": "vertebra.extensions.django.tasks.TaskBackend",
        "OPTIONS": {"BACKEND_URL": "postgresql://app_user:secret@localhost:5432/app"},
    }
}

CHANNEL_LAYERS = {
    "default": {
        "BACKEND": "vertebra.extensions.django.channels.ChannelLayer",
        "CONFIG": {"url": "valkey://localhost:6379/0"},
    }
}
```

## gRPC Services & Protobuf Schemas (`vertebra.extensions.grpc`)

`GrpcServiceRegistry` and `@grpc_method` route 5-byte length-prefixed HTTP/2 gRPC frames from `vertebra-http` directly to typed Python methods across all 4 RPC modes (`unary`, `server_streaming`, `client_streaming`, `bidi_streaming`), with automatic `.proto` generation from `msgspec.Struct` models (`ProtobufSchemaGenerator`):

```python
import msgspec
from vertebra.extensions.grpc import (
    GrpcContext,
    GrpcServiceRegistry,
    ProtobufSchemaGenerator,
    grpc_method,
)


class LookupRequest(msgspec.Struct):
    sku: str


class LookupReply(msgspec.Struct):
    sku: str
    available: int


registry = GrpcServiceRegistry()


@grpc_method(service="Inventory", package="inventory", name="Lookup")
async def lookup(request: LookupRequest, ctx: GrpcContext) -> LookupReply:
    return LookupReply(sku=request.sku, available=42)


registry.register(lookup)
schema_gen = ProtobufSchemaGenerator(package="inventory")
proto_source = schema_gen.generate_from_registry(registry)
```

## Oracle APEX Gateway, Islands & 26ai Duality Views (`vertebra.extensions.apex`)

Modernize Oracle APEX applications incrementally without a full rewrite using `vertebra.extensions.apex` and `@vertebra/client/apex`:

- **Session & Security**: `ApexSessionContext`, `OracleSessionManager.acquire_scoped` (`DBMS_SESSION.SET_IDENTIFIER` + automatic teardown), `ApexSessionAuth` (`verify_handoff_token`), `ApexGateway`, and `ConfirmationGate` for human-in-the-loop PL/SQL mutations.
- **Islands & Offline Outbox**: `ApexIslandRegistry` mounts Vite-bundled React/Vue/Svelte islands inside APEX regions (`ApexRegion`), and `ApexOfflineSyncEngine` (`ApexOfflineOutbox`) replays offline mutations with HLC conflict detection.
- **Federated Queries & 26ai Duality Views**: `ApexFederatedQuery` joins Oracle tables with Parquet/S3 datasets in DataFusion, while `ApexOpenAPIGenerator`, `DualityViewSpec`, and `DualityViewMcpHandler` expose Oracle 26ai JSON Relational Duality Views as OpenAPI endpoints and MCP agent tools with ETag optimistic concurrency.

## Shared-Memory Session Broker (`vertebra.extensions.broker`)

`BrokerPlugin` (`BrokerConfig`, `BrokerHost`, `BrokerEngineDiscovery`, `SyncSHMBrokerTransport` / `AsyncSHMBrokerTransport`, `SyncConnectionLease` / `AsyncConnectionLease`, `StatementPlan`, `LeaseQuarantinedError`) manages cross-worker database connection leases and prepared statement plans over shared memory so N Python worker processes share a bounded pool without multiplying database connections.

```mermaid
flowchart LR
    W["Worker Processes (SQLSpec Proxy / BrokerPool)"]
    T["SyncSHMBrokerTransport / AsyncSHMBrokerTransport"]
    SHM["UDS SCM_RIGHTS Attach & ShmLeaseTable"]
    H["BrokerHost (vtb broker serve)"]
    C["BrokerEngineCapsule / Server Pools"]
    W --> T
    T --> SHM
    SHM --> H
    H --> C
```

- **Named SHM & Authenticated UDS Attach**: Owner segments write `ShmOwnerHeader` generation and PID ownership metadata; cross-process workers attach either by name or through `BrokerAttachClient` / `BrokerAttachServer` over a `0600` Unix domain socket inside a `0700` directory authenticated with `VERTEBRA_BROKER_SECRET` (`SCM_RIGHTS` FD passing).
- **Supervision, Crash Recovery & Quarantine**: `vtb broker serve` runs `BrokerHost` and drains or rolls back active slots on shutdown (`ShutdownReport`), while `reclaim_worker_leases` transitions crashed worker slots into `Quarantined`, runs server `ROLLBACK` cleanup, and disposes dirty slots (`vtb broker status` and `DbPoolMetricSample` report `quarantined`, `total_quarantined`, and `total_disposed`). Session release or pool check-in failures quarantine the lease and raise `LeaseQuarantinedError`.
- **SQLAlchemy & Advanced Alchemy Pool Sharing**: Pass `BrokerPool` / `AsyncBrokerPool` (and `broker_creator`) with `create_engine` / `create_async_engine`, or pass a transport to `create_sync_alchemy_config` / `create_async_alchemy_config` to partition broker leases by config `pool_key()`.

```python
from sqlalchemy import create_engine, text
from vertebra.extensions.broker import (
    BrokerConfig,
    BrokerHost,
    SyncSHMBrokerTransport,
)
from vertebra.extensions.sqlalchemy import BrokerPool, broker_creator

config = BrokerConfig(pool_size=4, shm_name="/vtb-app-broker", role="owner")
with BrokerHost(config) as host:
    worker_cfg = BrokerConfig(
        pool_size=4,
        shm_name="/vtb-app-broker",
        role="worker",
    )
    transport = SyncSHMBrokerTransport(worker_cfg)
    pool_cls = BrokerPool.bind_class(
        broker=transport,
        url="sqlite+vtb:///app.db",
    )
    engine = create_engine(
        "sqlite+vtb:///app.db",
        poolclass=pool_cls,
        creator=broker_creator("sqlite+vtb:///app.db", broker=transport),
    )
    with engine.connect() as conn:
        value = conn.execute(text("SELECT 1")).scalar_one()
    engine.dispose()
    transport.close()
```
