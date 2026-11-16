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
  - id: src-tests-unit-framework-test-distributed-queues-py
    resource: src/py/tests/unit/framework/test_distributed_queues.py
    title: "Python implementation module (src/py/tests/unit/framework/test_distributed_queues.py)"
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
  - id: src-framework-queues-base-py
    resource: src/py/vertebra/framework/queues/base.py::QueueService._build_backend
    title: "Queue URL scheme routing and QueueBackendSettings construction"
  - id: src-framework-queues-postgres-py
    resource: src/py/vertebra/framework/queues/postgres.py::PostgreSQLQueueBackend
    title: "PostgreSQL queue backend over Sync/AsyncPostgresQueueStore"
  - id: src-framework-queues-redis-py
    resource: src/py/vertebra/framework/queues/redis.py::RedisQueueBackend
    title: "Redis Streams queue backend over vertebra._redis"
  - id: src-framework-queues-valkey-py
    resource: src/py/vertebra/framework/queues/valkey.py::ValkeyQueueBackend
    title: "Valkey Streams queue backend over vertebra._valkey"
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
- `capabilities.py` defines `@runtime_checkable SyncQueueStore` and `@runtime_checkable AsyncQueueStore` (`QueueEntry`, `ack`/`nack`) implemented by `SyncPostgresQueueStore`/`AsyncPostgresQueueStore` (`FOR UPDATE OF q SKIP LOCKED`, `pg_notify`) and `SyncOracleQueueStore`/`AsyncOracleQueueStore` (`FOR UPDATE SKIP LOCKED`, TxEventQ) [^src-db-capabilities-py] [^src-db-pg-queue-py] [^src-db-ora-queue-py].
- `QueueService` (`QueueBackendSettings`, `TaskResultRecord`) and `@task` execute priority, delayed, sync (`enqueue_sync`), and dead-letter jobs across `MemoryQueueBackend`, `PostgreSQLQueueBackend`, `OracleQueueBackend`, `RedisQueueBackend` (`vertebra._redis`), `ValkeyQueueBackend` (`vertebra._valkey`), and `SpannerQueueBackend` (`SyncSpannerQueueStore`/`AsyncSpannerQueueStore` in `spanner-queue-and-rw-transactions`) [^src-framework-queues-base-py] [^src-framework-queues-redis-py] [^src-framework-queues-valkey-py].
- `WorkerRunner` (`server/lanes.py`) enters `Container.scope()` per task attempt and maintains lease heartbeats alongside `CronScheduler` slot-keyed leader election [^src-server-lanes-py].
- `ChannelsBackend` (`create_channels_backend`) bridges `SyncNotificationSource`/`AsyncNotificationSource` across Memory, Redis, Valkey, PostgreSQL, and Oracle with origin-node deduplication, while `ValkeyStore` (`vertebra._valkey`) and `RedisStore` (`vertebra._redis`) provide RESP3 `CLIENT TRACKING` L1 invalidation and prefix-scoped `SCAN` + `UNLINK` [^src-framework-channels-py] [^src-framework-stores-py].
- `EmailService` sends transactional email across `MemoryEmailTransport`, `SmtpEmailTransport`, `ResendEmailTransport`, `SendGridEmailTransport`, `MailgunEmailTransport`, and `SesEmailTransport` behind the `EmailTransport` protocol [^src-framework-email-init-py].
<!-- truth: end -->

Vertebra provides pluggable distributed background task queues, multi-process cron scheduling, worker execution lanes, distributed channels, RESP3 key-value stores, and transactional email dispatch through [`vertebra.framework.queues`](../../../../../src/py/vertebra/framework/queues/__init__.py)[^src-framework-queues-init-py], [`vertebra.server.lanes`](../../../../../src/py/vertebra/server/lanes.py)[^src-server-lanes-py], [`vertebra.framework.channels`](../../../../../src/py/vertebra/framework/channels.py)[^src-framework-channels-py], [`vertebra.framework.stores`](../../../../../src/py/vertebra/framework/stores.py)[^src-framework-stores-py], and [`vertebra.framework.email`](../../../../../src/py/vertebra/framework/email/__init__.py)[^src-framework-email-init-py].

## 1. Distributed Queue Backends (`framework/queues/` & `db/adapters/*/queue.py`)

[`QueueService`](../../../../../src/py/vertebra/framework/queues/__init__.py) dispatches `@task` enqueueing (`enqueue` and synchronous `enqueue_sync`), delayed scheduling, `TaskResultRecord` storage, dead-letter routing, and retry backoff across six storage backends routed by `QueueService._build_backend`[^src-framework-queues-base-py]:

| Backend | Implementation & Storage Mechanics |
| :--- | :--- |
| `MemoryQueueBackend` | In-process priority queue with monotonic sequence tracking for hermetic unit testing and single-process servers. |
| `PostgreSQLQueueBackend` | Auto-constructs `SyncPostgresQueueStore` / `AsyncPostgresQueueStore` (`vertebra-core[postgres]`) executing `FOR UPDATE OF q SKIP LOCKED ... RETURNING`, heartbeats, DLQ, cron leases, and `pg_notify` wakeups (or accepts an explicit `store=` / `executor=`)[^src-framework-queues-postgres-py] [^src-db-pg-queue-py]. |
| `OracleQueueBackend` | Auto-constructs `SyncOracleQueueStore` / `AsyncOracleQueueStore` (`vertebra-core[oracle]`) executing `FOR UPDATE SKIP LOCKED`, `MERGE INTO vertebra_queue_leases`, heartbeats, DLQ, and TxEventQ wakeups (or accepts `store=` / `executor=`)[^src-framework-queues-oracle-py] [^src-db-ora-queue-py]. |
| `RedisQueueBackend` | Per-lane Redis Streams (`XADD`, `XREADGROUP`, `XAUTOCLAIM`, `XACK`), sorted-set delayed scheduling (`ZADD`), `TaskResultRecord` persistence, and `SyncRedisNotificationSource` / `AsyncRedisNotificationSource` wakeups over `vertebra._redis` (`vertebra-core[redis]`)[^src-framework-queues-redis-py]. |
| `ValkeyQueueBackend` | Standalone Valkey Streams backend (`vtb:valkey:queue`), sorted-set promotion, `TaskResultRecord` persistence, and `SyncValkeyNotificationSource` / `AsyncValkeyNotificationSource` wakeups over `vertebra._valkey` (`vertebra-core[valkey]`)[^src-framework-queues-valkey-py]. |
| `SpannerQueueBackend` | Hash-sharded primary keys (`shard_count` default `16`), GoogleSQL or Spangres DDL, and `FOR UPDATE` claims (`SyncSpannerQueueStore` / `AsyncSpannerQueueStore` delivered in `spanner-queue-and-rw-transactions`)[^src-framework-queues-spanner-py]. |

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
| `SmtpEmailTransport` | Standard SMTP over STARTTLS or SSL/TLS with configurable auth credentials. |
| `ResendEmailTransport` | HTTP REST API transport using bearer API keys and JSON payload formatting. |
| `SendGridEmailTransport` | HTTP REST v3 transport with mail-send payload serialization. |
| `MailgunEmailTransport` | HTTP API transport with basic auth API keys and domain-routed sending. |
| `SesEmailTransport` | AWS Simple Email Service transport authenticated via native Rust `sign_aws_sigv4_headers` in `vertebra-storage`, eliminating Boto3 dependencies. |

Provenance sources: [^src-vertebra-storage-src-lib-rs], [^src-tests-unit-framework-test-distributed-queues-py], [^src-tests-unit-framework-test-email-providers-py].

[^src-framework-queues-init-py]: `src/py/vertebra/framework/queues/__init__.py` — Python implementation module (src/py/vertebra/framework/queues/__init__.py).
[^src-framework-email-init-py]: `src/py/vertebra/framework/email/__init__.py` — Python implementation module (src/py/vertebra/framework/email/__init__.py).
[^src-server-lanes-py]: `src/py/vertebra/server/lanes.py` — Python implementation module (src/py/vertebra/server/lanes.py).
[^src-vertebra-storage-src-lib-rs]: `src/rs/vertebra-storage/src/lib.rs` — Rust implementation module (src/rs/vertebra-storage/src/lib.rs).
[^src-tests-unit-framework-test-distributed-queues-py]: `src/py/tests/unit/framework/test_distributed_queues.py` — Python implementation module (src/py/tests/unit/framework/test_distributed_queues.py).
[^src-tests-unit-framework-test-email-providers-py]: `src/py/tests/unit/framework/test_email_providers.py` — Python implementation module (src/py/tests/unit/framework/test_email_providers.py).
[^src-db-capabilities-py]: `src/py/vertebra/db/driver/capabilities.py` — `SyncQueueStore`, `AsyncQueueStore`, and `QueueEntry` capability contracts.
[^src-db-pg-queue-py]: `src/py/vertebra/db/adapters/postgres/queue.py` — `SyncPostgresQueueStore` and `AsyncPostgresQueueStore`.
[^src-db-ora-queue-py]: `src/py/vertebra/db/adapters/oracle/queue.py` — `SyncOracleQueueStore` and `AsyncOracleQueueStore`.
[^src-framework-queues-base-py]: `src/py/vertebra/framework/queues/base.py::QueueService._build_backend` — Queue URL scheme routing and `QueueBackendSettings` construction.
[^src-framework-queues-postgres-py]: `src/py/vertebra/framework/queues/postgres.py::PostgreSQLQueueBackend` — PostgreSQL queue backend over `SyncPostgresQueueStore` and `AsyncPostgresQueueStore`.
[^src-framework-queues-redis-py]: `src/py/vertebra/framework/queues/redis.py::RedisQueueBackend` — Redis Streams queue backend over `vertebra._redis`.
[^src-framework-queues-valkey-py]: `src/py/vertebra/framework/queues/valkey.py::ValkeyQueueBackend` — Valkey Streams queue backend over `vertebra._valkey`.
[^src-framework-queues-oracle-py]: `src/py/vertebra/framework/queues/oracle.py::OracleQueueBackend` — Oracle queue backend over `SyncOracleQueueStore` and `AsyncOracleQueueStore`.
[^src-framework-queues-spanner-py]: `src/py/vertebra/framework/queues/spanner.py::SpannerQueueBackend` — Spanner queue backend.
[^src-framework-stores-py]: `src/py/vertebra/framework/stores.py` — `ValkeyStore`, `RedisStore`, `MemoryStore`, and `KeyValueStore`.
[^src-framework-channels-py]: `src/py/vertebra/framework/channels.py` — `ChannelsBackend` and `create_channels_backend` across 5 backends.
