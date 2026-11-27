---
type: Concept
title: "Distributed Queues, Cron Leader Election & Transactional Email"
description: "The vertebra.framework.queues backends (Memory, Redis, Valkey, PostgreSQL, Oracle, Spanner), CronScheduler leader election, WorkerRunner lanes, and vertebra.framework.email transactional transports."
status: stable
tags:
  - domains
  - framework
  - queues
  - email
  - cron
  - workers
generated:
  by: flow/0.26.0
  at: "2026-11-04T04:33:00Z"
verified:
  - by: agent:flow-executor
    at: "2026-11-17T08:15:00Z"
    commit: 5a833832
  - by: agent:flow-archive
    at: "2026-11-09T20:56:00Z"
    commit: af1b3d30
  - by: agent:flow-setup
    at: "2026-11-07T03:55:00Z"
    commit: 067fa901
sources:
  - id: src-framework-queues-init-py
    resource: src/py/vertebra/framework/queues/__init__.py
    title: "Python implementation module (src/py/vertebra/framework/queues/__init__.py)"
  - id: src-framework-email-init-py
    resource: src/py/vertebra/framework/email/__init__.py
    title: "Python implementation module (src/py/vertebra/framework/email/__init__.py)"
  - id: src-server-lanes-py
    resource: src/py/vertebra/server/lanes.py
    title: "Python implementation module (src/py/vertebra/server/lanes.py)"
  - id: src-vertebra-storage-src-lib-rs
    resource: src/rs/vertebra-storage/src/lib.rs
    title: "Rust implementation module (src/rs/vertebra-storage/src/lib.rs)"
  - id: src-tests-unit-framework-test-queue-worker-and-scheduler-py
    resource: src/py/tests/unit/framework/test_queue_worker_and_scheduler.py
    title: "Distributed queue workers, retry/DLQ lifecycle, cron scheduler, and Django tasks tests"
  - id: src-tests-unit-framework-test-queue-backends-and-channels-py
    resource: src/py/tests/unit/framework/test_queue_backends_and_channels.py
    title: "Queue backend configuration, stores, Lua promotion, and channel wiring tests"
  - id: src-tests-unit-framework-test-email-providers-py
    resource: src/py/tests/unit/framework/test_email_providers.py
    title: "Python implementation module (src/py/tests/unit/framework/test_email_providers.py)"
  - id: src-db-capabilities-py
    resource: src/py/vertebra/db/driver/capabilities.py
    title: "SyncQueueStore, AsyncQueueStore, and QueueEntry capability contracts"
  - id: src-db-pg-queue-py
    resource: src/py/vertebra/db/adapters/postgres/queue.py
    title: "SyncPostgresQueueStore and AsyncPostgresQueueStore"
  - id: src-db-ora-queue-py
    resource: src/py/vertebra/db/adapters/oracle/queue.py
    title: "SyncOracleQueueStore and AsyncOracleQueueStore"
  - id: src-db-spn-queue-py
    resource: src/py/vertebra/db/adapters/spanner/queue.py
    title: "SyncSpannerQueueStore and AsyncSpannerQueueStore"
  - id: src-framework-queues-base-py
    resource: src/py/vertebra/framework/queues/base.py::QueueService._build_backend
    title: "Queue URL scheme routing and QueueBackendSettings construction"
  - id: src-framework-queues-postgres-py
    resource: src/py/vertebra/framework/queues/postgres.py::PostgreSQLQueueBackend
    title: "PostgreSQL queue backend over Sync/AsyncPostgresQueueStore"
  - id: src-framework-queues-backends-py
    resource: src/py/vertebra/framework/queues/backends.py::SHMQueueBackend
    title: "Cross-process SHMQueueBackend over Rust ShmQueueRing"
  - id: src-rs-queue-ring-rs
    resource: src/rs/vertebra-py/src/queue_ring.rs::ShmQueueRing
    title: "POSIX shared-memory queue ring and CAS record store"
  - id: src-framework-queues-resp-py
    resource: src/py/vertebra/framework/queues/_resp.py
    title: "Shared RESPQueueDialect, RESPStreamQueueBackend, and RESPQueueStoreBase"
  - id: src-framework-queues-redis-py
    resource: src/py/vertebra/framework/queues/redis.py::RedisQueueBackend
    title: "Redis Streams queue backend over vertebra._redis"
  - id: src-framework-queues-valkey-py
    resource: src/py/vertebra/framework/queues/valkey.py::ValkeyQueueBackend
    title: "Valkey Streams queue backend over vertebra._valkey"
  - id: src-db-redis-queue-py
    resource: src/py/vertebra/db/adapters/redis/queue.py
    title: "SyncRedisQueueStore and AsyncRedisQueueStore"
  - id: src-db-valkey-queue-py
    resource: src/py/vertebra/db/adapters/valkey/queue.py
    title: "SyncValkeyQueueStore and AsyncValkeyQueueStore"
  - id: src-framework-queues-oracle-py
    resource: src/py/vertebra/framework/queues/oracle.py::OracleQueueBackend
    title: "Oracle queue backend over Sync/AsyncOracleQueueStore"
  - id: src-framework-queues-spanner-py
    resource: src/py/vertebra/framework/queues/spanner.py::SpannerQueueBackend
    title: "Spanner queue backend"
  - id: src-framework-stores-py
    resource: src/py/vertebra/framework/stores.py
    title: "ValkeyStore, RedisStore, MemoryStore, and KeyValueStore"
  - id: src-framework-channels-py
    resource: src/py/vertebra/framework/channels.py
    title: "ChannelsBackend and create_channels_backend across 5 backends"
---

# Distributed Queues, Cron Leader Election & Transactional Email

<!-- truth: start -->
- `capabilities.py` defines `@runtime_checkable SyncQueueStore` and `@runtime_checkable AsyncQueueStore` (`QueueEntry` with priority/expiry/heartbeat/result/error fields, `QueueReservation`, `cleanup_terminal`, `get_entry_by_key`, `reserve_identity`/`get_reservation`/`reset_identity`, `assign_worker`, `touch_heartbeats`/`null_heartbeats`, `interrupt_task`, `expire_overdue`, `next_due_delay`) implemented by PostgreSQL (`FOR UPDATE OF q SKIP LOCKED`, `pg_notify`), Oracle (`FOR UPDATE SKIP LOCKED`, TxEventQ), Spanner (interactive `ReadWrite` `SELECT ... FOR UPDATE`), Redis, and Valkey (`SyncRESPQueueStore` / `AsyncRESPQueueStore` in `framework/queues/_resp.py`) [^src-db-capabilities-py] [^src-db-pg-queue-py] [^src-db-ora-queue-py] [^src-db-spn-queue-py] [^src-framework-queues-resp-py].
- `SHMQueueBackend` (`framework/queues/backends.py`) persists tasks and CAS records across Unix processes in Rust [`ShmQueueRing`](../../../../../src/rs/vertebra-py/src/queue_ring.rs)[^src-rs-queue-ring-rs] with macOS-safe 22-byte `/vtbq_<16-hex>` hashed segment paths, producer-exit persistence, and futex/ulock wakeups [^src-framework-queues-backends-py].
- `QueueService` (`QueueBackendSettings`, `TaskResultRecord`) and `@task` execute priority, delayed, sync (`enqueue_sync`), and dead-letter jobs across `MemoryQueueBackend`, `SHMQueueBackend`, `PostgreSQLQueueBackend`, `OracleQueueBackend`, `RedisQueueBackend` (`vertebra._redis`), `ValkeyQueueBackend` (`vertebra._valkey`), and `SpannerQueueBackend` (`vertebra._spanner`) [^src-framework-queues-base-py] [^src-framework-queues-redis-py] [^src-framework-queues-valkey-py] [^src-framework-queues-spanner-py].
- `WorkerRunner` (`server/lanes.py`) enters `Container.scope()` per task attempt and maintains lease heartbeats alongside `CronScheduler` slot-keyed leader election [^src-server-lanes-py].
- `ChannelsBackend` (`create_channels_backend`) bridges `SyncNotificationSource`/`AsyncNotificationSource` across Memory, Redis, Valkey, PostgreSQL, and Oracle with origin-node deduplication, while `ValkeyStore` (`vertebra._valkey`) and `RedisStore` (`vertebra._redis`) provide RESP3 `CLIENT TRACKING` L1 invalidation and prefix-scoped `SCAN` + `UNLINK` [^src-framework-channels-py] [^src-framework-stores-py].
- `EmailService` sends transactional email across `MemoryEmailTransport`, `SMTPEmailTransport`, `ResendEmailTransport`, `SendGridEmailTransport`, `MailgunEmailTransport`, and `SESEmailTransport` behind the `EmailTransport` protocol [^src-framework-email-init-py].
<!-- truth: end -->

Vertebra provides pluggable distributed background task queues, multi-process cron scheduling, worker execution lanes, distributed channels, RESP3 key-value stores, and transactional email dispatch through [`vertebra.framework.queues`](../../../../../src/py/vertebra/framework/queues/__init__.py)[^src-framework-queues-init-py], [`vertebra.server.lanes`](../../../../../src/py/vertebra/server/lanes.py)[^src-server-lanes-py], [`vertebra.framework.channels`](../../../../../src/py/vertebra/framework/channels.py)[^src-framework-channels-py], [`vertebra.framework.stores`](../../../../../src/py/vertebra/framework/stores.py)[^src-framework-stores-py], and [`vertebra.framework.email`](../../../../../src/py/vertebra/framework/email/__init__.py)[^src-framework-email-init-py]. See [Framework Extension Limitations](framework-extension-limitations.md) for `ShmQueueRing` capacity and host extra boundaries.

## 1. Distributed Queue Backends (`framework/queues/` & `db/adapters/*/queue.py`)

[`QueueService`](../../../../../src/py/vertebra/framework/queues/__init__.py) dispatches `@task` enqueueing (`enqueue` and synchronous `enqueue_sync`), delayed scheduling, `TaskResultRecord` storage, dead-letter routing, and retry backoff across storage backends routed by `QueueService._build_backend`[^src-framework-queues-base-py]:

| Backend | Implementation & Storage Mechanics |
| :--- | :--- |
| `MemoryQueueBackend` | In-process priority queue with monotonic sequence tracking for hermetic unit testing and single-process servers. |
| `SHMQueueBackend` | Cross-process Unix POSIX shared-memory queue over Rust `ShmQueueRing` (`capacity=4096`, `slot_bytes=8192` default) with macOS-safe `/vtbq_<16-hex>` hashed paths, producer-exit survival, futex/ulock `wait`/`wake`, and atomic CAS record updates[^src-framework-queues-backends-py] [^src-rs-queue-ring-rs]. |
| `PostgreSQLQueueBackend` | Auto-constructs `SyncPostgresQueueStore` / `AsyncPostgresQueueStore` (`vertebra-core[postgres]`) executing `FOR UPDATE OF q SKIP LOCKED ... RETURNING`, heartbeats, DLQ, cron leases, and `pg_notify` wakeups (or accepts an explicit `store=` / `executor=`)[^src-framework-queues-postgres-py] [^src-db-pg-queue-py]. |
| `OracleQueueBackend` | Auto-constructs `SyncOracleQueueStore` / `AsyncOracleQueueStore` (`vertebra-core[oracle]`) executing `FOR UPDATE SKIP LOCKED`, `MERGE INTO vertebra_queue_leases`, heartbeats, DLQ, and TxEventQ wakeups (or accepts `store=` / `executor=`)[^src-framework-queues-oracle-py] [^src-db-ora-queue-py]. |
| `RedisQueueBackend` | Thin driver binding over shared `RESPStreamQueueBackend` (`_resp.py`) and `vertebra._redis` (`vertebra-core[redis]`): per-lane Redis Streams (`XADD`, `XREADGROUP`, `XAUTOCLAIM`, `XACK`), sorted-set delayed scheduling (`ZADD`), `TaskResultRecord` persistence, and `SyncRedisQueueStore` / `AsyncRedisQueueStore` (`SyncRESPQueueStore` / `AsyncRESPQueueStore`)[^src-framework-queues-resp-py] [^src-framework-queues-redis-py] [^src-db-redis-queue-py]. |
| `ValkeyQueueBackend` | Thin driver binding over shared `RESPStreamQueueBackend` (`_resp.py`) and `vertebra._valkey` (`vertebra-core[valkey]`): Valkey Streams (`vtb:valkey:queue`), sorted-set promotion, `TaskResultRecord` persistence, and `SyncValkeyQueueStore` / `AsyncValkeyQueueStore`[^src-framework-queues-resp-py] [^src-framework-queues-valkey-py] [^src-db-valkey-queue-py]. |
| `SpannerQueueBackend` | Auto-constructs `SyncSpannerQueueStore` / `AsyncSpannerQueueStore` (`vertebra-core[spanner]`) with hash-sharded primary keys (`shard_count` default `16`), atomic `SELECT ... FOR UPDATE` -> `UPDATE ... THEN RETURN *` (`RETURNING *` on Spangres) inside `driver.run_in_transaction`, stateless `spn:{shard_id}:{task_id}:{attempt}` receipt tokens, conditional non-stealing `acquire_lease`, adaptive polling wakeups (`next_due_delay`), and asynchronous `ROW DELETION POLICY` / `TTL INTERVAL` background compaction[^src-framework-queues-spanner-py] [^src-db-spn-queue-py]. |

## 2. Multi-Lane Worker Runner, Scoped DI, Channels & Stores

[`vertebra.server.lanes`](../../../../../src/py/vertebra/server/lanes.py), [`CronScheduler`](../../../../../src/py/vertebra/framework/queues/__init__.py), [`ChannelsBackend`](../../../../../src/py/vertebra/framework/channels.py), and [`ValkeyStore` / `RedisStore`](../../../../../src/py/vertebra/framework/stores.py) coordinate worker execution and cross-node fan-out:

- **Priority Worker Lanes & Scoped DI (`WorkerRunner`)**: Partitions workers into `TaskLane.CPU`, `TaskLane.IO`, and `TaskLane.AGENT` lanes, entering `framework/di.py::Container.scope()` per task attempt with automatic lease heartbeats and lame-duck shutdown draining.
- **Distributed Cron Leader Election (`CronScheduler`)**: Uses slot-keyed heartbeat leases (`acquire_lease` / `renew_lease`) so exactly one node enqueues each scheduled cron window.
- **Distributed Channels & Native Stores**: `create_channels_backend` connects `ChannelBus` to `SyncNotificationSource` and `AsyncNotificationSource` across `memory`, `redis`, `valkey`, `postgres`, and `oracle`, while `ValkeyStore` and `RedisStore` pair native drivers with RESP3 `CLIENT TRACKING` local L1 eviction and prefix-scoped `SCAN` + `UNLINK`.

## 3. Transactional Email Transports (`framework/email/`)

[`EmailService`](../../../../../src/py/vertebra/framework/email/__init__.py) manages outbound email templates, DKIM/SPF headers, webhook signature verification (`WebhookVerifier`, `WebhookEvent`), and multi-provider failover across `EmailTransport` implementations:

| Transport | Transport & Authentication Mechanics |
| :--- | :--- |
| `MemoryEmailTransport` | Records dispatched messages in an in-memory audit log for test inspection (`clear()`, `sent`). |
| `SMTPEmailTransport` | Standard SMTP over STARTTLS or SSL/TLS with configurable auth credentials. |
| `ResendEmailTransport` | HTTP REST API transport using bearer API keys and JSON payload formatting. |
| `SendGridEmailTransport` | HTTP REST v3 transport with mail-send payload serialization. |
| `MailgunEmailTransport` | HTTP API transport with basic auth API keys and domain-routed sending. |
| `SESEmailTransport` | AWS Simple Email Service transport authenticated via native Rust `sign_aws_sigv4_headers` in `vertebra-storage`, eliminating Boto3 dependencies. |

Provenance sources: [^src-vertebra-storage-src-lib-rs], [^src-tests-unit-framework-test-queue-worker-and-scheduler-py], [^src-tests-unit-framework-test-queue-backends-and-channels-py], [^src-tests-unit-framework-test-email-providers-py].

[^src-framework-queues-init-py]: `src/py/vertebra/framework/queues/__init__.py` — Python implementation module (src/py/vertebra/framework/queues/__init__.py).
[^src-framework-email-init-py]: `src/py/vertebra/framework/email/__init__.py` — Python implementation module (src/py/vertebra/framework/email/__init__.py).
[^src-server-lanes-py]: `src/py/vertebra/server/lanes.py` — Python implementation module (src/py/vertebra/server/lanes.py).
[^src-vertebra-storage-src-lib-rs]: `src/rs/vertebra-storage/src/lib.rs` — Rust implementation module (src/rs/vertebra-storage/src/lib.rs).
[^src-tests-unit-framework-test-queue-worker-and-scheduler-py]: `src/py/tests/unit/framework/test_queue_worker_and_scheduler.py` — Distributed queue workers, retry/DLQ lifecycle, cron scheduler, and Django tasks tests.
[^src-tests-unit-framework-test-queue-backends-and-channels-py]: `src/py/tests/unit/framework/test_queue_backends_and_channels.py` — Queue backend configuration, stores, Lua promotion, and channel wiring tests.
[^src-tests-unit-framework-test-email-providers-py]: `src/py/tests/unit/framework/test_email_providers.py` — Python implementation module (src/py/tests/unit/framework/test_email_providers.py).
[^src-db-capabilities-py]: `src/py/vertebra/db/driver/capabilities.py` — `SyncQueueStore`, `AsyncQueueStore`, and `QueueEntry` capability contracts.
[^src-db-pg-queue-py]: `src/py/vertebra/db/adapters/postgres/queue.py` — `SyncPostgresQueueStore` and `AsyncPostgresQueueStore`.
[^src-db-ora-queue-py]: `src/py/vertebra/db/adapters/oracle/queue.py` — `SyncOracleQueueStore` and `AsyncOracleQueueStore`.
[^src-db-spn-queue-py]: `src/py/vertebra/db/adapters/spanner/queue.py` — `SyncSpannerQueueStore` and `AsyncSpannerQueueStore`.
[^src-framework-queues-base-py]: `src/py/vertebra/framework/queues/base.py::QueueService._build_backend` — Queue URL scheme routing and `QueueBackendSettings` construction.
[^src-framework-queues-backends-py]: `src/py/vertebra/framework/queues/backends.py::SHMQueueBackend` — Cross-process `SHMQueueBackend` over Rust `ShmQueueRing`.
[^src-rs-queue-ring-rs]: `src/rs/vertebra-py/src/queue_ring.rs::ShmQueueRing` — POSIX shared-memory queue ring and CAS record store.
[^src-framework-queues-resp-py]: `src/py/vertebra/framework/queues/_resp.py` — Shared `RESPQueueDialect`, `RESPStreamQueueBackend`, and `RESPQueueStoreBase`.
[^src-framework-queues-postgres-py]: `src/py/vertebra/framework/queues/postgres.py::PostgreSQLQueueBackend` — PostgreSQL queue backend over `SyncPostgresQueueStore` and `AsyncPostgresQueueStore`.
[^src-framework-queues-redis-py]: `src/py/vertebra/framework/queues/redis.py::RedisQueueBackend` — Redis Streams queue backend over `vertebra._redis`.
[^src-framework-queues-valkey-py]: `src/py/vertebra/framework/queues/valkey.py::ValkeyQueueBackend` — Valkey Streams queue backend over `vertebra._valkey`.
[^src-db-redis-queue-py]: `src/py/vertebra/db/adapters/redis/queue.py` — `SyncRedisQueueStore` and `AsyncRedisQueueStore`.
[^src-db-valkey-queue-py]: `src/py/vertebra/db/adapters/valkey/queue.py` — `SyncValkeyQueueStore` and `AsyncValkeyQueueStore`.
[^src-framework-queues-oracle-py]: `src/py/vertebra/framework/queues/oracle.py::OracleQueueBackend` — Oracle queue backend over `SyncOracleQueueStore` and `AsyncOracleQueueStore`.
[^src-framework-queues-spanner-py]: `src/py/vertebra/framework/queues/spanner.py::SpannerQueueBackend` — Spanner queue backend.
[^src-framework-stores-py]: `src/py/vertebra/framework/stores.py` — `ValkeyStore`, `RedisStore`, `MemoryStore`, and `KeyValueStore`.
[^src-framework-channels-py]: `src/py/vertebra/framework/channels.py` — `ChannelsBackend` and `create_channels_backend` across 5 backends.
