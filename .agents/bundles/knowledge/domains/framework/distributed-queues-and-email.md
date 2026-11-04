---
type: Concept
title: "Distributed Queues, Cron Leader Election & Transactional Email"
description: "The `vertebra.framework.queues` backends (Memory, Redis, Valkey, Postgres, Oracle), `CronScheduler` leader election, `WorkerRunner` with priority lanes, and `vertebra.framework.email` transactional providers (SMTP, Resend, SendGrid, Mailgun, AWS SES with Rust SigV4 signing)."
status: stable
tags: [domains, framework, queues, email, cron, workers]
generated:
  by: flow/0.26.0
  at: "2026-11-04T04:33:00Z"
verified:
  - by: agent:flow-setup
    at: "2026-11-04T04:33:00Z"
    commit: f741701
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
---

# Distributed Queues, Cron Leader Election & Transactional Email

<!-- truth: start -->
- `QueueService` and `@task` execute priority, delayed, and dead-letter jobs across `Memory`, `Redis`, `Valkey`, `Postgres`, `Oracle`, and `Spanner` backends alongside lease-elected `CronScheduler` and `WorkerRunner` lanes.
- `EmailService` sends transactional email across SMTP, Resend, SendGrid, Mailgun, and AWS SES behind explicit `Sync` and `Async` provider contracts.
<!-- truth: end -->

Vertebra provides pluggable distributed background task queues, multi-process cron scheduling, worker execution lanes, and transactional email dispatch through [`vertebra.framework.queues`](../../../../../src/py/vertebra/framework/queues/__init__.py)[^src-framework-queues-init-py], [`vertebra.server.lanes`](../../../../../src/py/vertebra/server/lanes.py)[^src-server-lanes-py], and [`vertebra.framework.email`](../../../../../src/py/vertebra/framework/email/__init__.py)[^src-framework-email-init-py].

## 1. Distributed Queue Backends (`framework/queues/`)

[`QueueService`](../../../../../src/py/vertebra/framework/queues/__init__.py) provides unified `@task` enqueueing, delayed scheduling, dead-letter routing, and retry backoff across five storage backends:

| Backend | Implementation & Storage Mechanics |
| :--- | :--- |
| `MemoryQueueBackend` | In-process asyncio `PriorityQueue` with monotonic sequence tracking for hermetic unit testing and single-process servers. |
| `RedisQueueBackend` | Redis list and sorted-set (`ZADD`/`ZPOPMIN`) primitives for FIFO queues and delayed task scheduling. |
| `ValkeyQueueBackend` | Valkey client-side tracking with atomic queue leasing and tenant-isolated namespaces. |
| `PostgresQueueBackend` | `FOR UPDATE SKIP LOCKED` row-level leasing over a dedicated `vertebra_tasks` table, with index-accelerated lease expiration checks. |
| `OracleQueueBackend` | Native Oracle Advanced Queuing (`DBMS_AQ`) and table-backed queue leasing using `SKIP LOCKED` queries and sequence-backed enqueues. |

## 2. Multi-Lane Worker Runner & Cron Leader Election

[`vertebra.server.lanes`](../../../../../src/py/vertebra/server/lanes.py) and [`CronScheduler`](../../../../../src/py/vertebra/framework/queues/__init__.py) isolate background processing:

- **Priority Worker Lanes (`WorkerRunner`)**: Partitions workers into dedicated lanes:
  - `TaskLane.CPU`: Dedicated worker pool for heavy compute, compression, or cryptographic work.
  - `TaskLane.IO`: High-concurrency event-loop lanes for network calls, database queries, and webhooks.
  - `TaskLane.AGENT`: Specialized lanes with bounded concurrency for LLM agent loops, tool dispatch, and streaming workflows.
- **Distributed Cron Leader Election (`CronScheduler`)**: Uses heartbeat leases to elect exactly one active cron leader across multiple server instances, preventing duplicate task dispatch while allowing seamless failover when a leader process terminates.

## 3. Transactional Email Providers (`framework/email/`)

[`EmailService`](../../../../../src/py/vertebra/framework/email/__init__.py) manages outbound email templates, DKIM/SPF headers, and multi-provider failover:

| Provider | Transport & Authentication Mechanics |
| :--- | :--- |
| `MemoryEmailProvider` | Records dispatched messages in an in-memory audit log for test inspection (`clear()`, `sent_messages`). |
| `SmtpProvider` | Standard SMTP over STARTTLS or SSL/TLS with connection pooling and configurable auth credentials. |
| `ResendProvider` | HTTP REST API transport using bearer API keys and JSON payload formatting. |
| `SendGridProvider` | HTTP REST v3 transport with mail-send payload serialization. |
| `MailgunProvider` | HTTP API transport with basic auth API keys and domain-routed sending. |
| `SesProvider` | AWS Simple Email Service transport authenticated via native Rust `sign_aws_sigv4_headers` in `vertebra-storage`, eliminating Python-level Boto3 overhead and executing SigV4 calculations with zero memory allocations. |

Provenance sources: [^src-vertebra-storage-src-lib-rs], [^src-tests-unit-framework-test-distributed-queues-py], [^src-tests-unit-framework-test-email-providers-py].

[^src-framework-queues-init-py]: `src/py/vertebra/framework/queues/__init__.py` — Python implementation module (src/py/vertebra/framework/queues/__init__.py).
[^src-framework-email-init-py]: `src/py/vertebra/framework/email/__init__.py` — Python implementation module (src/py/vertebra/framework/email/__init__.py).
[^src-server-lanes-py]: `src/py/vertebra/server/lanes.py` — Python implementation module (src/py/vertebra/server/lanes.py).
[^src-vertebra-storage-src-lib-rs]: `src/rs/vertebra-storage/src/lib.rs` — Rust implementation module (src/rs/vertebra-storage/src/lib.rs).
[^src-tests-unit-framework-test-distributed-queues-py]: `src/py/tests/unit/framework/test_distributed_queues.py` — Python implementation module (src/py/tests/unit/framework/test_distributed_queues.py).
[^src-tests-unit-framework-test-email-providers-py]: `src/py/tests/unit/framework/test_email_providers.py` — Python implementation module (src/py/tests/unit/framework/test_email_providers.py).
