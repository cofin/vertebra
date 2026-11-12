---
type: Spec
flow_id: queues-tasks-and-channels
title: "Native Queues, Tasks, Channels, and Stores"
description: "Build native Vertebra QueueStore implementations for PostgreSQL and Oracle on the standalone Rust driver wheels, plug them into first-party QueueService, litestar-queues, and Django tasks with settings-driven selection and DI, correct the Oracle queue install hint, unify cron parsing, provide distributed Litestar/Starlette channel backends, and consolidate ValkeyStore."
status: draft
state: draft
has_chapters: false
parent_prd: integration-remediation
depends_on_flows:
  - native-postgres-driver
  - native-redis-driver
  - sqlspec-adapter-parity
  - native-oracle-driver
  - db-engine-primitives
  - driver-fail-loud
research:
  - ../integration-remediation/research/audit-findings_20261103.md
plan_revision: 2
plan_commit: null
state_revision: 0
current_task: null
last_verified_checkpoint: null
created_at: "2026-11-03T03:30:00Z"
updated_at: "2026-11-12T01:30:00Z"
generated:
  by: flow-revise
  at: "2026-11-12T01:30:00Z"
verified: []
sources:
  - id: queue-service
    resource: src/py/vertebra/framework/queues/base.py::QueueService
    title: First-party QueueService backend construction and cron scheduling
  - id: queue-store
    resource: src/py/vertebra/db/driver/capabilities.py::QueueStore
    title: Asynchronous QueueStore capability protocol
  - id: notification-source
    resource: src/py/vertebra/db/driver/capabilities.py::NotificationSource
    title: Unprefixed asynchronous NotificationSource protocol renamed and split by this flow
  - id: pg-notify
    resource: src/py/vertebra/db/adapters/postgres/notify.py::PostgresNotificationSource
    title: PostgreSQL LISTEN/NOTIFY source renamed to AsyncPostgresNotificationSource with a new SyncPostgresNotificationSource
  - id: oracle-notify
    resource: src/py/vertebra/db/adapters/oracle/notify.py::OracleNotificationSource
    title: Oracle TxEventQ source renamed to AsyncOracleNotificationSource with a new SyncOracleNotificationSource
  - id: queue-oracle
    resource: src/py/vertebra/framework/queues/oracle.py::OracleQueueBackend
    title: Oracle queue backend probing the oracledb Python package when executor is omitted
  - id: queue-postgres
    resource: src/py/vertebra/framework/queues/postgres.py::PostgreSQLQueueBackend
    title: PostgreSQL queue backend requiring an injected PostgreSQLQueueExecutor
  - id: litestar-queues
    resource: src/py/vertebra/extensions/litestar/queues/backend.py::SHMQueueBackend
    title: litestar-queues extension backends
  - id: litestar-channels
    resource: src/py/vertebra/extensions/litestar/channels.py::swap_channels_backend
    title: Litestar channels backend monkeypatching
  - id: channel-bus
    resource: src/py/vertebra/server/_channels.py::ChannelBus
    title: Engine-neutral in-process channel bus
  - id: fail-loud-baseline
    resource: .agents/bundles/knowledge/domains/framework/distributed-queues-and-email.md
    title: Completed fail-loud queue baseline requiring explicit executor= or client= collaborators
tags:
  - queues
  - tasks
  - django
  - litestar
  - channels
  - stores
  - di
---

# Flow Charter: Native Queues, Tasks, Channels, and Stores

<!-- truth: start -->
- **Current State**: `PostgreSQLQueueBackend`, `OracleQueueBackend`, and `SpannerQueueBackend` require an injected executor and raise `MissingDependencyError` or `ImproperConfigurationError` without one; `OracleQueueBackend` probes the `oracledb` Python package, so its install hint names `oracledb` rather than `vertebra-core[oracle]`; the `QueueStore` protocol in `db/driver/capabilities.py` has no implementation; `NotificationSource` is a single unprefixed protocol whose methods are all `async`, implemented by `PostgresNotificationSource` and `OracleNotificationSource`, so synchronous consumers have no notification surface [^notification-source] [^pg-notify] [^oracle-notify]; `QueueService._build_backend` constructs backends from `QueueConfig` [^queue-oracle] [^queue-postgres] [^queue-store] [^queue-service] [^fail-loud-baseline].
- **Sync and Async Notification Sources (target state)**: `vertebra/db/driver/capabilities.py` defines `SyncNotificationSource` and `AsyncNotificationSource`; PostgreSQL, Oracle, and Redis ship `Sync<Engine>NotificationSource` / `Async<Engine>NotificationSource` pairs, and no unprefixed `NotificationSource`, `PostgresNotificationSource`, or `OracleNotificationSource` name remains.
- **Durable Native `QueueStore` Implementations (target state)**: `PostgresQueueStore` (`vertebra-postgres`) and `OracleQueueStore` (`vertebra-oracle`) implement `FOR UPDATE SKIP LOCKED` leases, fenced completions, retries, DLQ transitions, and `AsyncNotificationSource` wakeups, and serve as the live executors that `PostgreSQLQueueBackend` and `OracleQueueBackend` require.
- **Framework Task & DI Integration (target state)**: `QueueService`, `litestar-queues`, and Django `django.tasks` select stores by settings and resolve handler dependencies through `framework/di.py::Container` [^litestar-queues].
- **Distributed Channels & `ValkeyStore` (target state)**: Litestar and Starlette `ChannelsBackend`s run on `AsyncNotificationSource` without `swap_channels_backend` monkeypatching, and `ValkeyStore` consolidates onto `vertebra._redis` [^litestar-channels] [^channel-bus].
<!-- truth: end -->

## Charter

Vertebra executes durable queuing and pub/sub on its own Rust-backed adapter primitives and plugs cleanly into framework contracts. This flow builds native `QueueStore` implementations for **PostgreSQL and Oracle**, wires them into first-party `QueueService`, `litestar-queues`, and Django `django.tasks` with DI-resolved task dependencies, provides distributed Litestar/Starlette channel backends over `AsyncNotificationSource`, consolidates `ValkeyStore`, and splits notification sources into `SyncNotificationSource` / `AsyncNotificationSource` so Django, Flask, synchronous workers, and CLI listeners consume notifications without an event loop.

## Baseline

The merged `driver-fail-loud` queue baseline (`af1b3d30`, [knowledge](../../knowledge/domains/framework/distributed-queues-and-email.md)) removes production in-memory queue executors and clients: `OracleQueueBackend`, `PostgreSQLQueueBackend`, and `SpannerQueueBackend` require an injected `executor=`, and `RedisQueueBackend` / `ValkeyQueueBackend` require a live `client=` or a `redis://` / `rediss://` URL. The executor and stream-client fakes live in the queue test modules [^fail-loud-baseline]. Sibling flows own adjacent probes: `native-postgres-driver` Chapter 27 replaces the `PostgreSQLQueueBackend` `psycopg` probe with `vertebra._postgres`, and `native-redis-driver` moves `RedisQueueBackend` / `ValkeyQueueBackend` off `redis.asyncio` onto `vertebra._redis`. This flow owns the live `PostgresQueueStore` and `OracleQueueStore` executors, the Oracle install hint, and the notification-source rename.

## Scope

1. **Sync and async notification sources** [^notification-source] [^pg-notify] [^oracle-notify]. This flow owns the rename and runs it first, after `native-postgres-driver` completes so its pending worksheets keep their current names:
   - Replace `db/driver/capabilities.py::NotificationSource` with `AsyncNotificationSource` (the current `async def publish`, `subscribe`, `unsubscribe`, and `listen() -> AsyncIterator[NotificationEvent]`) and a new `SyncNotificationSource` (blocking `publish(channel: str, payload: bytes | str) -> None`, `subscribe(channels: Sequence[str]) -> None`, `unsubscribe(channels: Sequence[str]) -> None`, `listen() -> Iterator[NotificationEvent]`, `close() -> None`), both `@runtime_checkable` protocols, and update the `db/driver/__init__.py` and `db/__init__.py` exports.
   - Rename `db/adapters/postgres/notify.py::PostgresNotificationSource` to `AsyncPostgresNotificationSource` and add `SyncPostgresNotificationSource` over `SyncPostgreSQLDriver` and the native synchronous listener (`publish` emits `SELECT pg_notify($1, $2)` on the pinned transaction socket when inside `with driver.transaction():`, `listen()` blocks on a dedicated `LISTEN` session); update `db/adapters/postgres/__init__.py` exports.
   - Rename `db/adapters/oracle/notify.py::OracleNotificationSource` to `AsyncOracleNotificationSource` and add `SyncOracleNotificationSource` over `SyncOracleDriver` (TxEventQ enqueue and blocking dequeue); update `db/adapters/oracle/driver.py` factory methods so the async driver returns `AsyncOracleNotificationSource` and the sync driver returns `SyncOracleNotificationSource`.
   - Update `server/_channels.py::ChannelBus` annotations to `AsyncNotificationSource`, and migrate every test that imports the old names (`test_native_db_drivers.py`, `test_oracle_adapter.py`, `test_pg_dataplane_and_datafusion.py`, `test_postgres_native_notifications.py`, `integration/db/test_oracle_native.py`) in place, adding sync-source cases to those canonical modules.
   - No compatibility aliases: `git grep -nE "\b(Postgres|Oracle)?NotificationSource\b" -- src docs` returns nothing after the rename.
   - `native-redis-driver` ships `SyncRedisNotificationSource` / `AsyncRedisNotificationSource` against these protocols; its pub/sub task starts after this rename lands.
2. **Native queue stores.** `vertebra/db/adapters/postgres/queue.py::PostgresQueueStore` (`vertebra-postgres`) and `vertebra/db/adapters/oracle/queue.py::OracleQueueStore` (`vertebra-oracle`) implement the `QueueStore` protocol in `db/driver/capabilities.py` (location and naming per Open Decisions):
   - Single and bulk `enqueue`, `FOR UPDATE SKIP LOCKED` `claim` with visibility leases, fenced `complete`/`fail` on `(job_id, attempt)`, `heartbeat`, exponential retry backoff, dead-letter queue transition, `promote_scheduled`, idempotent DDL (`ensure_schema`), and `AsyncNotificationSource` wakeups (`AsyncPostgresNotificationSource` `LISTEN`/`NOTIFY` and `AsyncOracleNotificationSource` TxEventQ, with `SyncPostgresNotificationSource` / `SyncOracleNotificationSource` for synchronous workers, with polling when notifications are not configured).
3. **Oracle install hint.** `framework/queues/oracle.py::OracleQueueBackend` probes `ensure_module("vertebra._oracle", package="vertebra-core[oracle]", extra="oracle")` instead of `oracledb`, so its `MissingDependencyError` names `vertebra-core[oracle]` and its `ImproperConfigurationError` names `OracleQueueStore` as the executor to bind. The queue tests in their canonical module assert both messages.
4. **First-party `QueueService`.**
   - Convert `framework/queues/base.py::QueueService._build_backend` to a config-keyed backend registry driven by a new `QueueBackendSettings(kind="postgres" | "oracle" | "memory", ...)`.
   - Bind `PostgresQueueStore` and `OracleQueueStore` as the `executor=` that `PostgreSQLQueueBackend` and `OracleQueueBackend` require when settings select `kind="postgres"` or `kind="oracle"`, keeping `MemoryQueueBackend` strictly for explicit `kind="memory"` single-process/test use.
   - Keep one cron parser (`framework/queues/scheduler.py::CronExpression`) and delete `base.py::_cron_matches` / `_cron_matches_field`.
5. **`litestar-queues` integration.**
   - Add one generic `litestar_queues` `BaseQueueBackend` adapter in `extensions/litestar/queues/backend.py` over any `QueueStore` (claim fencing, heartbeats, maintenance leases, execution refs, and statistics), selected by settings to wrap `PostgresQueueStore` or `OracleQueueStore`, alongside `SHMQueueBackend` for single-host deployments. The extension module holds no engine code (naming per Open Decisions).
6. **Django `django.tasks` integration.**
   - Implement a new `extensions/django/tasks.py::TaskBackend` (`django.tasks.backends.base.BaseTaskBackend`) backed by `QueueService`, add a Django management command to run workers, and fold `extensions/django/queues.py` into `tasks.py`.
7. **Dependency injection.**
   - Resolve task handler parameters through `framework/di.py::Container` within a per-task lifecycle scope across `QueueService`, `litestar-queues`, and Django tasks.
8. **Distributed channels.**
   - Building on `server/_channels.py::ChannelBus` (decoupled from database channel backends in `db-engine-primitives`), provide Litestar and Starlette `ChannelsBackend` implementations backed by `AsyncPostgresNotificationSource`, `AsyncOracleNotificationSource`, and `AsyncRedisNotificationSource` (from `native-redis-driver`), and remove `extensions/litestar/channels.py::swap_channels_backend` monkeypatching. All three implement `AsyncNotificationSource`, per the [native-data-platform notification source decision](../native-data-platform/spec.md).
9. **Key-value stores.**
   - Implement `ValkeyStore(litestar.stores.base.Store)` in `extensions/litestar/store.py` (consuming `vertebra.db.adapters.valkey` / `vertebra._redis` with RESP3 client-side tracking invalidation), replacing `framework/stores.py::KeyValueStore` duplication and aligning `extensions/starlette/store.py`.
10. **Docs & OKF.** Update `docs/reference/database-capabilities.md`, `docs/reference/db.md`, `docs/usage/analytics-and-cdc.md`, `docs/usage/databases.md`, `docs/usage/extensions.md`, `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md`, `.agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md`, and `.agents/bundles/knowledge/domains/extensions/litestar.md` to the `Sync*` / `Async*` notification source names with a synchronous listener example, and update `docs/usage/queues-and-realtime.md`, `docs/reference/queues.md`, and `.agents/bundles/knowledge/domains/framework/distributed-queues-and-email.md` from the executor-or-raise and client-or-raise baseline to the live PostgreSQL and Oracle stores, and record caveats (including the Oracle polling path and deferred Spanner/Redis stores) in a queue limitations chapter under `.agents/bundles/knowledge/domains/framework/`.

## Out of Scope

Spanner and Redis/Valkey native `QueueStore` implementations (deferred to the PRD backlog); the live Spanner queue executor is a follow-up opened with `/flow:plan spanner-queue-executor`. The Redis/Valkey queue client cutover belongs to `native-redis-driver`, and the PostgreSQL queue probe to `native-postgres-driver`.

## Public Seams

`SyncNotificationSource`, `AsyncNotificationSource`, `SyncPostgresNotificationSource`, `AsyncPostgresNotificationSource`, `SyncOracleNotificationSource`, `AsyncOracleNotificationSource`, `PostgresQueueStore`, `OracleQueueStore`, `QueueService`, `QueueBackend`, `QueueBackendSettings`, the generic `litestar-queues` store backend, `TaskBackend`, distributed `ChannelsBackend`s, and `ValkeyStore`.

## Dependencies

- `native-postgres-driver` (active): `PostgresQueueStore` executes on the cut-over native PostgreSQL driver; the notification-source rename starts after its pending chapters complete.
- `native-redis-driver` (draft): `AsyncRedisNotificationSource` / `SyncRedisNotificationSource` for distributed channels and synchronous listeners (its pub/sub task follows this flow's rename task) and `vertebra._redis` for `ValkeyStore`.
- `sqlspec-adapter-parity` (draft): events queue stores and adapter resolution consumed by `litestar-queues`.
- Merged: `native-oracle-driver` (`47da6820`), integration-remediation `db-engine-primitives` (`80ff8a65`), and `driver-fail-loud` (`af1b3d30`).

## Acceptance Boundary

- Multi-process worker delivery, lease expiration recovery, and fenced completion integration tests pass against PostgreSQL and Oracle 23ai/26ai Free via `pytest-databases`.
- A `litestar-queues` application executes tasks end-to-end on the generic store backend wrapping `PostgresQueueStore`, and Django `task.enqueue()` executes via the worker command with DI-injected dependencies.
- `OracleQueueBackend()` without `executor=` raises `MissingDependencyError` naming `vertebra-core[oracle]` when `vertebra._oracle` is absent, and `git grep -n "oracledb" -- src/py/vertebra/framework` returns nothing.
- `SyncPostgresNotificationSource` and `SyncOracleNotificationSource` deliver a published payload to a blocking `listen()` iterator without an event loop, `AsyncPostgresNotificationSource` and `AsyncOracleNotificationSource` satisfy `isinstance(source, AsyncNotificationSource)`, and no unprefixed notification source name remains in `src/` or `docs/`.
- `make lint-all`, `make py-test`, and `make docs` pass.

## Open Decisions

- **Queue class naming and `QueueStore` module location.** Options: (a) keep the `QueueStore` protocol in `db/driver/capabilities.py`, place implementations as engine-local primitives `db/adapters/postgres/queue.py::PostgresQueueStore` and `db/adapters/oracle/queue.py::OracleQueueStore` (matching `AsyncPostgresNotificationSource` / `AsyncOracleNotificationSource`), keep the existing first-party `PostgreSQLQueueBackend` / `OracleQueueBackend` names in `framework/queues/`, and give `litestar-queues` one generic store-backed adapter class in `extensions/litestar/queues/backend.py`; (b) add per-engine `PostgresQueueBackend` / `OracleQueueBackend` classes in the Litestar extension; (c) place queue stores under `framework/queues/`. Recommended: (a). The locked layout puts engine-local primitives in `db/adapters/<engine>/` and framework integrations in the consuming extension; option (b) would define a second `OracleQueueBackend` that collides with `framework/queues/oracle.py::OracleQueueBackend` and a `PostgresQueueBackend` one word away from `PostgreSQLQueueBackend`, and would put engine code in a generic extension module; option (c) inverts the `vertebra.db` / framework layering.

## Draft Task Outline

- t01 Notification source split: `SyncNotificationSource` / `AsyncNotificationSource` protocols, `Sync`/`Async` PostgreSQL and Oracle sources, `ChannelBus` annotations, in-place test migration, and docs/OKF renames.
- t02 `PostgresQueueStore` (`FOR UPDATE SKIP LOCKED`, leases, wakeups).
- t03 `OracleQueueStore` (`FOR UPDATE SKIP LOCKED`, leases, TxEventQ wakeups) and the `OracleQueueBackend` `vertebra-core[oracle]` install hint.
- t04 `QueueService` registry, `QueueBackendSettings`, live store executor binding, and single `CronExpression` parser.
- t05 Generic `litestar-queues` store backend over `QueueStore` with settings selection for PostgreSQL and Oracle.
- t06 Django `django.tasks` backend, worker management command, and per-task DI scope.
- t07 Distributed Litestar/Starlette `ChannelsBackend`s over `AsyncNotificationSource` + `swap_channels_backend` removal.
- t08 `ValkeyStore` consolidation.
- t09 Docs, OKF, and queue limitations chapter.

[^notification-source]: `src/py/vertebra/db/driver/capabilities.py::NotificationSource` — Unprefixed asynchronous notification protocol that this flow splits into `SyncNotificationSource` and `AsyncNotificationSource`.
[^pg-notify]: `src/py/vertebra/db/adapters/postgres/notify.py::PostgresNotificationSource` — PostgreSQL source renamed to `AsyncPostgresNotificationSource`.
[^oracle-notify]: `src/py/vertebra/db/adapters/oracle/notify.py::OracleNotificationSource` — Oracle source renamed to `AsyncOracleNotificationSource`.
[^queue-service]: `src/py/vertebra/framework/queues/base.py::QueueService` — First-party QueueService backend construction and cron scheduling.
[^queue-store]: `src/py/vertebra/db/driver/capabilities.py::QueueStore` — Asynchronous `QueueStore` capability protocol.
[^queue-oracle]: `src/py/vertebra/framework/queues/oracle.py::OracleQueueBackend` — Oracle queue backend probing the `oracledb` Python package when `executor` is omitted.
[^queue-postgres]: `src/py/vertebra/framework/queues/postgres.py::PostgreSQLQueueBackend` — PostgreSQL queue backend requiring an injected `PostgreSQLQueueExecutor`.
[^litestar-queues]: `src/py/vertebra/extensions/litestar/queues/backend.py::SHMQueueBackend` — litestar-queues extension backends.
[^litestar-channels]: `src/py/vertebra/extensions/litestar/channels.py::swap_channels_backend` — Litestar channels backend monkeypatching.
[^channel-bus]: `src/py/vertebra/server/_channels.py::ChannelBus` — Engine-neutral in-process channel bus.
[^fail-loud-baseline]: `.agents/bundles/knowledge/domains/framework/distributed-queues-and-email.md` — Completed fail-loud queue baseline requiring explicit `executor=` or `client=` collaborators.
