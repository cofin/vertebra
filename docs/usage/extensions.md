# Framework Extensions

`vertebra.extensions` connects Vertebra's Rust HTTP server, shared-memory worker pools, and object storage to SQLSpec, SQLAlchemy / Advanced Alchemy, Litestar, gRPC, and Oracle APEX.

## SQLSpec Integration, Statement Stacks & SHM Proxy (`vertebra.extensions.sqlspec`)

`SQLSpecPlugin` integrates `sqlspec` query building, named `.sql` file loading, `StatementStack` pipelined execution, and request-scoped database sessions in two modes:

- **Tier-1 Native Drivers**: Typed sync and async adapters (`PostgresSyncConfig`, `PostgresAsyncConfig`, `MysqlSyncConfig`, `MysqlAsyncConfig`, `SqliteSyncConfig`, `SqliteAsyncConfig`, `OracleSyncConfig`, `OracleAsyncConfig`, `MssqlSyncConfig`, `MssqlAsyncConfig`, `DuckdbSyncConfig`, `DuckdbAsyncConfig`).
- **Tier-2 Shared-Memory Proxy**: `ProxySyncConfig` and `ProxyAsyncConfig` (`SqlSpecProvider`, `ProxySyncDriver`, `ProxyAsyncDriver`) lease pooled connections from the Rust parent process over shared memory so multi-worker deployments share a single database pool, support batched `execute_many` calls, and return zero-copy Arrow result tables (`SharedArrowBuffer`, `SharedArrowTable`).
- **Streaming Object Storage Export (`SQLSpecStore`)**: Export query results directly to `ObjectStore` as JSON Lines (`export_jsonl`) or Apache Arrow IPC streams (`export_arrow_ipc`, `StreamingStorageResponse`).

```python
from vertebra.db import SqliteConfig
from vertebra.extensions.sqlspec import (
    SQLSpecConfig,
    SQLSpecPlugin,
    SqliteSyncConfig,
)

sqlspec_plugin = SQLSpecPlugin(
    SQLSpecConfig(
        configs=[
            SqliteSyncConfig(
                connection_config=SqliteConfig(path=":memory:"),
                commit_mode="autocommit",
            )
        ]
    )
)
```

## SQLAlchemy, Advanced Alchemy & `+vtb` Dialects (`vertebra.extensions.alchemy`, `vertebra.extensions.sqlalchemy`)

`AlchemyPlugin` (`SQLAlchemySyncConfig`, `SQLAlchemyAsyncConfig`) provides request-scoped ORM sessions, generic `DatabaseService[ModelT]` repositories (`get`, `list`, `list_and_count`, `create`, `update`, `delete`), and `ObjectStoreBackend` (`StoredObject`, `FileAttachment`) for transactional model file attachments.

`vertebra.extensions.sqlalchemy` registers `+vertebra` and `+vtb` SQLAlchemy URL schemes (`postgresql+vtb://`, `mysql+vtb://`, `sqlite+vtb://`, `oracle+vtb://`, `mssql+vtb://`, `duckdb+vtb://`) backed by Vertebra's Rust drivers, plus:

- **Custom Column Types & Batching**: `Vector(dim)` embeddings, `BinaryJson` (`JSONB` / Oracle `OSON`), `SelectInLoadSpec`, `InsertSentinelSpec` (`RETURNING` / `OUTPUT INSERTED`), and `DeleteUsingClause`.
- **Free-Threaded & Alembic Support**: `SyncEngineAdapter` for free-threaded `3.14t`/`3.15t` worker pools, and `AlembicRunner` (`AlembicImpl`, `CreateVectorIndexOp`, `CreateViewOp`, `DropViewOp`).

```python
from vertebra.extensions.alchemy import AlchemyPlugin, SQLAlchemySyncConfig
from vertebra.extensions.sqlalchemy import AlembicRunner, Vector

alchemy = AlchemyPlugin(
    SQLAlchemySyncConfig(connection_string="sqlite+vtb:///:memory:")
)
embedding_col = Vector(dimensions=768)
migrations = AlembicRunner(dsn="sqlite+vtb:///:memory:")
```

## Litestar Integration & Middleware Offload (`vertebra.extensions.litestar`)

Use `LitestarPlugin` and `LitestarConfig` to run a Litestar application on Vertebra's Rust HTTP server while offloading CORS, rate limiting, JWT validation, response compression, and static asset serving to Rust (`LitestarOffloadPlan`, `LitestarFastPathBypass`). Also includes `ChannelsBackend` for Litestar Channels and `MemoryStore` for `litestar.stores`:

```python
from litestar import Litestar, get
from vertebra.extensions.litestar import LitestarConfig, LitestarPlugin


@get("/hello")
async def hello() -> dict[str, str]:
    return {"hello": "litestar"}


app = Litestar(
    route_handlers=[hello],
    plugins=[LitestarPlugin(LitestarConfig())],
)
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


class LookupRequest(msgspec.Struct, frozen=True, gc=False):
    sku: str


class LookupReply(msgspec.Struct, frozen=True, gc=False):
    sku: str
    available: int


class InventoryService:
    @grpc_method("/inventory.Inventory/Lookup")
    async def lookup(self, request: LookupRequest, ctx: GrpcContext) -> LookupReply:
        return LookupReply(sku=request.sku, available=42)


registry = GrpcServiceRegistry()
registry.register(InventoryService())
schema_gen = ProtobufSchemaGenerator(package="inventory")
proto_source = schema_gen.generate([LookupRequest, LookupReply])
```

## Oracle APEX Gateway, Islands & 26ai Duality Views (`vertebra.extensions.apex`)

Modernize Oracle APEX applications incrementally without a full rewrite using `vertebra.extensions.apex` and `@vertebra/client/apex`:

- **Session & Security**: `ApexSessionContext`, `OracleSessionManager.acquire_scoped` (`DBMS_SESSION.SET_IDENTIFIER` + automatic teardown), `ApexSessionAuth`, `ApexGateway` (`verify_checksum`), and `ConfirmationGate` for human-in-the-loop PL/SQL mutations.
- **Islands & Offline Outbox**: `ApexIslandRegistry` mounts Vite-bundled React/Vue/Svelte islands inside APEX regions (`ApexRegion`), and `ApexOfflineSyncEngine` (`ApexOfflineOutbox`) replays offline mutations with HLC conflict detection.
- **Federated Queries & 26ai Duality Views**: `ApexFederatedQuery` joins Oracle tables with Parquet/S3 datasets in DataFusion, while `ApexOpenAPIGenerator`, `DualityViewSpec`, and `DualityViewMcpHandler` expose Oracle 26ai JSON Relational Duality Views as OpenAPI endpoints and MCP agent tools with ETag optimistic concurrency.

## Shared-Memory Session Broker (`vertebra.extensions.broker`)

`BrokerPlugin` (`BrokerConfig`, `ShmSessionBroker`, `LeaseBroker`, `StatementCache`) manages cross-worker database connection leases and prepared statement plans over shared memory so N Python worker processes never multiply database connection counts.
