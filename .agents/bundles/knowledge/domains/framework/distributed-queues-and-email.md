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
  - id: src-framework-queues-base-py
    resource: src/py/vertebra/framework/queues/base.py::QueueService._build_backend
    title: "Queue URL scheme routing to backend constructors"
  - id: src-framework-queues-postgres-py
    resource: src/py/vertebra/framework/queues/postgres.py::PostgreSQLQueueBackend
    title: "PostgreSQL queue backend requiring an injected executor"
  - id: src-framework-queues-redis-py
    resource: src/py/vertebra/framework/queues/redis.py::RedisQueueBackend
    title: "Redis and Valkey Streams queue backends requiring a redis client"
  - id: src-framework-queues-oracle-py
    resource: src/py/vertebra/framework/queues/oracle.py::OracleQueueBackend
    title: "Oracle queue backend requiring an injected executor"
  - id: src-framework-queues-spanner-py
    resource: src/py/vertebra/framework/queues/spanner.py::SpannerQueueBackend
    title: "Spanner queue backend requiring an injected executor"
---

# Distributed Queues, Cron Leader Election & Transactional Email

<!-- truth: start -->
- `QueueService` and `@task` execute priority, delayed, and dead-letter jobs across `MemoryQueueBackend`, `RedisQueueBackend`, `ValkeyQueueBackend`, `PostgreSQLQueueBackend`, `OracleQueueBackend`, and `SpannerQueueBackend` alongside lease-elected `CronScheduler` and `WorkerRunner` lanes.
- `OracleQueueBackend`, `PostgreSQLQueueBackend`, and `SpannerQueueBackend` bind an injected `executor=`; without one they raise `MissingDependencyError` when `oracledb`, `vertebra._postgres` (`vertebra-core[postgres]`), or `vertebra._spanner` is missing and `ImproperConfigurationError` otherwise. No live executor ships for these backends.
- `RedisQueueBackend` and `ValkeyQueueBackend` bind an injected `client=`; without one they raise `MissingDependencyError` when `redis.asyncio` is missing, `ImproperConfigurationError` when `backend_url` has no hostname (including hostless `unix://` URLs), and otherwise pass `backend_url` directly to `redis.asyncio.from_url` (`redis://` / `rediss://` schemes only; `valkey://`, `valkeys://`, and `unix://` targets require a `redis://` / `rediss://` host URL or an injected `client=`).
- `QueueService` routes `""` and `memory` schemes to `MemoryQueueBackend`.
- `EmailService` sends transactional email across `MemoryEmailTransport`, `SmtpEmailTransport`, `ResendEmailTransport`, `SendGridEmailTransport`, `MailgunEmailTransport`, and `SesEmailTransport` behind the `EmailTransport` protocol.
<!-- truth: end -->

Vertebra provides pluggable distributed background task queues, multi-process cron scheduling, worker execution lanes, and transactional email dispatch through [`vertebra.framework.queues`](../../../../../src/py/vertebra/framework/queues/__init__.py)[^src-framework-queues-init-py], [`vertebra.server.lanes`](../../../../../src/py/vertebra/server/lanes.py)[^src-server-lanes-py], and [`vertebra.framework.email`](../../../../../src/py/vertebra/framework/email/__init__.py)[^src-framework-email-init-py].

## 1. Distributed Queue Backends (`framework/queues/`)

[`QueueService`](../../../../../src/py/vertebra/framework/queues/__init__.py) dispatches `@task` enqueueing, delayed scheduling, dead-letter routing, and retry backoff across six storage backends routed by `QueueService._build_backend`[^src-framework-queues-base-py]; Oracle, PostgreSQL, and Spanner run SQL through an injected executor protocol (`executor=`), while Redis and Valkey run through an injected or URL-built `RedisStreamClient` (`client=`):

| Backend | Implementation & Storage Mechanics |
| :--- | :--- |
| `MemoryQueueBackend` | In-process asyncio `PriorityQueue` with monotonic sequence tracking for hermetic unit testing and single-process servers. |
| `RedisQueueBackend` | Per-lane Redis Streams with consumer groups (`XADD`, `XREADGROUP`, `XAUTOCLAIM` visibility recovery, `XACK`) plus a scheduled sorted set (`ZADD`) promoted into streams by a `ZRANGEBYSCORE` + `XADD` Lua script; binds `client=` or a hosted `redis://` / `rediss://` URL via `redis.asyncio.from_url` (hostless `unix://` URLs raise `ImproperConfigurationError` unless `client=` is passed)[^src-framework-queues-redis-py]. |
| `ValkeyQueueBackend` | `RedisQueueBackend` subclass with default `stream_prefix` `vtb:valkey:queue`; `redis.asyncio.from_url` accepts `redis://` / `rediss://` schemes only, so `valkey://` / `valkeys://` and `unix://` endpoints require a `redis://` / `rediss://` host URL or an injected `client=`[^src-framework-queues-redis-py]. |
| `PostgreSQLQueueBackend` | `FOR UPDATE SKIP LOCKED` claims over `vertebra_queue_jobs` with `vertebra_queue_dlq` and `vertebra_queue_leases`, `NOTIFY` on `vertebra_queue_wakeup`, through an injected `PostgreSQLQueueExecutor` (`executor=`)[^src-framework-queues-postgres-py]. |
| `OracleQueueBackend` | `FOR UPDATE SKIP LOCKED` claims over `vertebra_queue_jobs`, leases via `MERGE INTO vertebra_queue_leases`, through an injected `OracleQueueExecutor` (`executor=`)[^src-framework-queues-oracle-py]. |
| `SpannerQueueBackend` | Hash-sharded primary keys (`shard_count` default `16`), GoogleSQL or Spangres DDL, `FOR UPDATE` claims over `vertebra_queue_jobs` through an injected `SpannerQueueExecutor` (`executor=`)[^src-framework-queues-spanner-py]. |

## 2. Multi-Lane Worker Runner & Cron Leader Election

[`vertebra.server.lanes`](../../../../../src/py/vertebra/server/lanes.py) and [`CronScheduler`](../../../../../src/py/vertebra/framework/queues/__init__.py) isolate background processing:

- **Priority Worker Lanes (`WorkerRunner`)**: Partitions workers into dedicated lanes:
  - `TaskLane.CPU`: Dedicated worker pool for heavy compute, compression, or cryptographic work.
  - `TaskLane.IO`: High-concurrency event-loop lanes for network calls, database queries, and webhooks.
  - `TaskLane.AGENT`: Specialized lanes with bounded concurrency for LLM agent loops, tool dispatch, and streaming workflows.
- **Distributed Cron Leader Election (`CronScheduler`)**: Uses heartbeat leases to elect exactly one active cron leader across multiple server instances, preventing duplicate task dispatch while allowing failover when a leader process terminates.

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
[^src-framework-queues-base-py]: `src/py/vertebra/framework/queues/base.py::QueueService._build_backend` — Queue URL scheme routing to backend constructors.
[^src-framework-queues-postgres-py]: `src/py/vertebra/framework/queues/postgres.py::PostgreSQLQueueBackend` — PostgreSQL queue backend requiring an injected executor.
[^src-framework-queues-redis-py]: `src/py/vertebra/framework/queues/redis.py::RedisQueueBackend` — Redis and Valkey Streams queue backends requiring a redis client.
[^src-framework-queues-oracle-py]: `src/py/vertebra/framework/queues/oracle.py::OracleQueueBackend` — Oracle queue backend requiring an injected executor.
[^src-framework-queues-spanner-py]: `src/py/vertebra/framework/queues/spanner.py::SpannerQueueBackend` — Spanner queue backend requiring an injected executor.
