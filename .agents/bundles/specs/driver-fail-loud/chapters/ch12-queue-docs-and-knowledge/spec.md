---
type: Spec
parent_flow: driver-fail-loud
flow_id: driver-fail-loud
chapter_id: ch12-queue-docs-and-knowledge
title: "Chapter 12: Queue Backend Docs and Knowledge"
description: Align the queues usage guide, the queues API reference, and the OKF distributed-queues chapter with the executor-or-raise and client-or-raise queue backend contracts delivered by chapters 02 and 10.
status: stable
state: completed
planning_status: complete
depends_on_chapters:
  - ch10-queue-backend-fail-loud
  - ch11-docs-and-okf
replan_trigger: null
current_task: null
plan_revision: 2
plan_commit: fd571e3a
state_revision: 1
last_verified_checkpoint: 3cee2a23
last_reviewed_commit: null
created_at: "2026-11-07T00:00:00Z"
updated_at: "2026-11-09T15:16:39Z"
generated:
  by: flow/0.28.0
  at: "2026-11-07T00:00:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-08T00:00:00Z"
    commit: 0acdf576
sources:
  - id: docs-queues-usage
    resource: docs/usage/queues-and-realtime.md
    title: Queues, LiveState, email, and telemetry usage guide with the queue backend table
  - id: docs-queues-reference
    resource: docs/reference/queues.md
    title: vertebra.framework.queues API reference page
  - id: okf-queues
    resource: .agents/bundles/knowledge/domains/framework/distributed-queues-and-email.md
    title: OKF distributed queues, cron leader election, and transactional email chapter
  - id: queue-factory
    resource: src/py/vertebra/framework/queues/base.py::QueueService._build_backend
    title: Queue URL scheme routing to backend constructors
  - id: pg-queue
    resource: src/py/vertebra/framework/queues/postgres.py::PostgreSQLQueueBackend
    title: PostgreSQL queue backend over vertebra_queue_jobs with LISTEN/NOTIFY wakeups
  - id: redis-queue
    resource: src/py/vertebra/framework/queues/redis.py::RedisQueueBackend
    title: Redis Streams and scheduled sorted-set queue backend and its Valkey subclass
  - id: oracle-queue
    resource: src/py/vertebra/framework/queues/oracle.py::OracleQueueBackend
    title: Oracle queue backend using FOR UPDATE SKIP LOCKED over vertebra_queue_jobs
  - id: spanner-queue
    resource: src/py/vertebra/framework/queues/spanner.py::SpannerQueueBackend
    title: Spanner queue backend with hash-sharded keys and FOR UPDATE claims
  - id: ch10-contract
    resource: .agents/bundles/specs/driver-fail-loud/chapters/ch10-queue-backend-fail-loud/spec.md
    title: PostgreSQL, Redis, and Spanner queue fail-loud contracts
  - id: ch02-oracle-contract
    resource: .agents/bundles/specs/driver-fail-loud/chapters/ch02-duckdb-sqlalchemy-queue-fail-loud/tasks/t03-oracle-queue-requires-oracledb.md
    title: Oracle queue executor-or-raise contract
tags:
  - docs
  - okf
  - queues
  - fail-loud
---

# Chapter 12: Queue Backend Docs and Knowledge

<!-- truth: start -->
- **Executor-or-raise backends**: user docs and OKF state that `OracleQueueBackend`, `PostgreSQLQueueBackend`, and `SpannerQueueBackend` bind an injected `executor=` and otherwise raise `MissingDependencyError` (package missing) or `ImproperConfigurationError` (package installed, no executor).[^ch02-oracle-contract][^ch10-contract]
- **Client-or-raise backends**: user docs and OKF state that `RedisQueueBackend` and `ValkeyQueueBackend` bind an injected `client=`, otherwise require the `redis` package and a `backend_url` with a host, and raise `MissingDependencyError` or `ImproperConfigurationError` otherwise.[^ch10-contract][^redis-queue]
- **Install packages**: Oracle `oracledb`, PostgreSQL `psycopg[binary,pool]`, Redis and Valkey `redis`, Cloud Spanner `vertebra-core[spanner]`.[^ch02-oracle-contract][^ch10-contract]
- **No unshipped mechanics**: no target file claims wound-wait claim transactions, multiplexed sessions, Oracle Advanced Queuing (`DBMS_AQ`), Valkey client-side tracking, a `vertebra_tasks` table, or Redis lists with `ZPOPMIN`.[^docs-queues-usage][^okf-queues]
- **Storage facts**: PostgreSQL, Oracle, and Spanner queues use `vertebra_queue_jobs`; Redis and Valkey use per-lane Streams with consumer groups plus a scheduled sorted set.[^pg-queue][^oracle-queue][^spanner-queue][^redis-queue]
- **memory:// unchanged**: `QueueService` routes `""` and `memory` schemes to `MemoryQueueBackend`; docs keep the `memory://` examples.[^queue-factory]
- **OKF provenance**: the edited knowledge chapter keeps 1:1 `sources` to `[^id]:` footnotes, a `<= 40`-line truth block, zero body `---` rules, and a `verified:` list whose first entry is the prepended `{by: agent:flow-executor, at: <UTC timestamp>, commit: <HEAD short sha>}`; earlier entries stay.[^okf-queues]
- **No internal names in user docs**: `docs/**` targets contain no flow, chapter, or spec identifiers.
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

Chapter 02 task t03 makes `OracleQueueBackend` raise when no executor is injected, and chapter 10 applies the same contract to `PostgreSQLQueueBackend` and `SpannerQueueBackend` and a client-or-raise contract to `RedisQueueBackend` / `ValkeyQueueBackend`.[^ch02-oracle-contract][^ch10-contract] The three documents that describe queue backends still present every network backend as usable from a URL alone and carry mechanics no shipped code implements:

- `docs/usage/queues-and-realtime.md` lists PostgreSQL, Redis, Valkey, Oracle, and Cloud Spanner rows with no executor, client, or install requirement, and the Cloud Spanner row claims "Wound-wait claim transactions".[^docs-queues-usage]
- `docs/reference/queues.md` omits `SpannerQueueBackend` from its intro sentence, Storage Backends row, and Queue Backends autodoc blocks; the row claims "RESP3 streams" and "Oracle AQ/table queues"; every backend block lists a `claim` member that no backend defines (the method is `dequeue`).[^docs-queues-reference]
- `.agents/bundles/knowledge/domains/framework/distributed-queues-and-email.md` describes Redis as list plus `ZADD`/`ZPOPMIN`, Valkey as client-side tracking, PostgreSQL as a `vertebra_tasks` table, Oracle as `DBMS_AQ`, and Spanner as multiplexed-session leasing, and states no executor or client rule.[^okf-queues]

This chapter rewrites those passages in present tense so user docs and agent knowledge describe the queue backends at the chapter 10 closing commit.

**Non-goals**: `valkey://` / `valkeys://` URL support in `RedisQueueBackend` (follow-up spec); live `oracledb`, `psycopg`, or Spanner executors (owned by the `native-<engine>-driver` flows); database driver docs (owned by `ch11-docs-and-okf`); Python docstrings in `src/py/vertebra/framework/queues/*.py` (owned by ch02 t03 and ch10); `docs/examples/background_queues.py`, `docs/usage/testing.md`, and the changelog.

### 1.2 Upstream Execution Baseline

- Branch `feat/storage-updates`, plan anchored at `0acdf576`; parent roadmap [`spec.md`](../../spec.md).
- Depends on `ch10-queue-backend-fail-loud` (PostgreSQL, Redis/Valkey, Spanner contracts) and transitively on `ch02-duckdb-sqlalchemy-queue-fail-loud` t03 (Oracle contract) and `ch08-install-hints-and-extras-gate` (`MissingDependencyError` hint defaults to `vertebra-core[<extra>]`).[^ch10-contract][^ch02-oracle-contract]
- Depends on `ch11-docs-and-okf` so the two documentation chapters never edit concurrently and the user-doc phrasing for `MissingDependencyError` stays consistent.
- Executors read the post-ch10 source named in the worksheet and document the code as found.

### 1.3 Code Analysis & Seam Summary

| Area | Files Examined (`path/to/file::Symbol`) | Doc Claim to Correct |
| --- | --- | --- |
| Usage guide | `docs/usage/queues-and-realtime.md::Background Task Queues, Backends & Cron (vertebra.framework.queues)`[^docs-queues-usage] | Backend table without requirements; Cloud Spanner "Wound-wait claim transactions"; schemes list only one URL per backend |
| Reference | `docs/reference/queues.md` intro paragraph, component table "Storage Backends" row, "Queue Backends" section[^docs-queues-reference] | No Spanner; "RESP3 streams", "Oracle AQ/table queues"; `members: [enqueue, claim, ack, nack]` names a nonexistent `claim` |
| OKF chapter | `distributed-queues-and-email.md` truth block and "1. Distributed Queue Backends (`framework/queues/`)"[^okf-queues] | Redis `ZPOPMIN`, Valkey client-side tracking, `vertebra_tasks`, `DBMS_AQ`, multiplexed sessions; no executor/client rule |
| URL routing | `src/py/vertebra/framework/queues/base.py::QueueService._build_backend`[^queue-factory] | Scheme list: `redis`/`rediss`, `valkey`/`valkeys`, `postgresql`/`postgres`, `oracle`, `spanner`/`spanner+googlesql`/`spangres`/`spanner+spangres` |
| PostgreSQL | `postgres.py::PostgreSQLQueueBackend`[^pg-queue] | `vertebra_queue_jobs`, `vertebra_queue_dlq`, `vertebra_queue_leases`; `NOTIFY` on `vertebra_queue_wakeup` |
| Redis / Valkey | `redis.py::RedisQueueBackend`, `redis.py::ValkeyQueueBackend`[^redis-queue] | `XADD` / `XREADGROUP` / `XAUTOCLAIM` / `XACK` per lane, scheduled set promoted by a `ZRANGEBYSCORE` + `XADD` Lua script; Valkey subclass defaults `stream_prefix="vtb:valkey:queue"` |
| Oracle | `oracle.py::OracleQueueBackend`[^oracle-queue] | `FOR UPDATE SKIP LOCKED` over `vertebra_queue_jobs`; leases via `MERGE INTO vertebra_queue_leases`; no `DBMS_AQ` |
| Spanner | `spanner.py::SpannerQueueBackend`[^spanner-queue] | Hash-sharded keys (`shard_count` default `16`), GoogleSQL or Spangres DDL, `FOR UPDATE` claims; no session management |

### 1.4 Relevant OKF Knowledge Chapters & Skills

- [Distributed queues and email](../../../../knowledge/domains/framework/distributed-queues-and-email.md)
- [Chapter 10 queue contracts](../ch10-queue-backend-fail-loud/spec.md)
- [Chapter 02 Oracle queue worksheet](../ch02-duckdb-sqlalchemy-queue-fail-loud/tasks/t03-oracle-queue-requires-oracledb.md)
- [Project workflow](../../../../knowledge/workflow.md)
- [Skills index](../../../../../skills/index.md)

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Chapter Requirements

- **R17 (t01)**: `docs/usage/queues-and-realtime.md`, `docs/reference/queues.md`, and `distributed-queues-and-email.md` state the executor-or-raise rule for Oracle, PostgreSQL, and Cloud Spanner, the client-or-raise rule for Redis and Valkey, and each backend's install package; they drop wound-wait, multiplexed-session, `DBMS_AQ`, client-side-tracking, `vertebra_tasks`, and `ZPOPMIN` claims; the reference lists `SpannerQueueBackend` and only methods the backends define.

### 2.2 Documentation Contracts

Backend facts used across all three files (executors adapt sentence structure, not facts):

| Backend | Required Claim | Install Package | Forbidden Claim |
| --- | --- | --- | --- |
| `MemoryQueueBackend` | `memory://` or empty scheme; in-process | none | Any statement that `memory://` is removed |
| `PostgreSQLQueueBackend` | `postgresql://` / `postgres://`; `FOR UPDATE SKIP LOCKED` over `vertebra_queue_jobs` and `LISTEN/NOTIFY` wakeups; requires `executor=` (`PostgreSQLQueueExecutor`) | `psycopg[binary,pool]` | `vertebra_tasks`; usable from a URL alone |
| `RedisQueueBackend` | `redis://` / `rediss://`; per-lane Streams with consumer groups (`XREADGROUP` / `XACK`, `XAUTOCLAIM` recovery) plus a scheduled sorted set; binds `client=` or `redis.asyncio.from_url(backend_url)` when the URL has a host | `redis` | Redis lists, `ZPOPMIN`, "RESP3 streams" |
| `ValkeyQueueBackend` | Same mechanics and client rule through the `redis` package; default `stream_prefix` `vtb:valkey:queue`; the default `QueueConfig` (`memory://`) has no host and raises | `redis` | Client-side tracking; tenant-isolated namespaces; `valkey://` as a working client URL |
| `OracleQueueBackend` | `oracle://`; `FOR UPDATE SKIP LOCKED` over `vertebra_queue_jobs`; requires `executor=` (`OracleQueueExecutor`) | `oracledb` | Oracle Advanced Queuing, `DBMS_AQ`, sequence-backed enqueues |
| `SpannerQueueBackend` | `spanner://`, `spanner+googlesql://`, `spangres://`, `spanner+spangres://`; hash-sharded keys and `FOR UPDATE` claims with a dead-letter table; requires `executor=` (`SpannerQueueExecutor`) | `vertebra-core[spanner]` | Wound-wait claim transactions; multiplexed sessions; `driver=` |

Error contract quoted in docs (from ch02 t03 and ch10):[^ch02-oracle-contract][^ch10-contract]

- `MissingDependencyError` when no `executor=` / `client=` is passed and the backend's package is absent. Hints: `Install it with: uv add oracledb`, `Install it with: uv add psycopg[binary,pool]`, `Install it with: uv add redis`, `Install it with: uv add vertebra-core[spanner]`.
- `ImproperConfigurationError` when the package is installed and no executor is passed (messages begin `OracleQueueBackend requires an OracleQueueExecutor`, `PostgreSQLQueueBackend requires a PostgreSQLQueueExecutor`, `SpannerQueueBackend requires a SpannerQueueExecutor`), or when a Redis/Valkey `backend_url` has no host (message contains `has no host`).
- `SpannerQueueBackend` raises `ImproperConfigurationError` (`requires a spanner:// backend_url`) for a non-Spanner scheme before any package check.
- Vertebra ships no live executor for Oracle, PostgreSQL, or Spanner queues, so URL-only `QueueService(QueueConfig(backend_url=...))` raises for those schemes; callers build the backend and pass it with `QueueService(config, backend=...)`.[^queue-factory]

### 2.3 Caller -> Seam -> Collaborator Map

- Reader of `docs/usage/queues-and-realtime.md` → backend table row → `QueueService._build_backend` → backend `__init__` → `ensure_module(...)` → `MissingDependencyError` / `ImproperConfigurationError`.
- Reader of `docs/reference/queues.md` → `::: vertebra.framework.queues.<module>.<Backend>` blocks → mkdocstrings Griffe over `src/py` → `__init__` `Raises:` docstrings authored by ch02 t03 and ch10.
- Agent loading `distributed-queues-and-email.md` → truth block and backend table → `src/py/vertebra/framework/queues/{base,postgres,redis,oracle,spanner}.py` footnotes.
- `make docs` → Zensical build, `tools.docs.postbuild`, `tools.docs.verify_docs_artifacts` → unresolved autodoc members and broken anchors fail the build.
- `make review-check` → `tools.scripts.review_setup validate` → bundle and privacy rules over `.agents/bundles/**`.

### 2.4 Edge-Case & Failure-Mode Matrix

| Scenario | Trigger | Expected Behavior | Invariant |
| --- | --- | --- | --- |
| Upstream message or hint differs | Post-ch10 code raises a different message, or the Spanner hint set by ch10 t03 (`install_package="vertebra-core[spanner]"`) differs | Document the code as found; add a Notes line; report to the coordinator | Docs describe code at `HEAD` |
| `valkey://` URL | Reader passes `QueueConfig(backend_url="valkey://host:6379/0")` | Docs state `valkey://` routes to `ValkeyQueueBackend` but the `redis` client accepts only `redis://` and `rediss://` here; recommend a `redis://` URL pointing at Valkey or `client=` | No advertised URL that fails at construction |
| Hostless URL | `unix:///tmp/redis.sock` or the default `memory://` | `ImproperConfigurationError` (`has no host`); docs never advertise `unix://` | Only host URLs or `client=` |
| Spanner autodoc block added | `::: vertebra.framework.queues.spanner.SpannerQueueBackend` with `members: [enqueue, dequeue, ack, nack]` | `make docs` resolves every member | Reference lists only defined methods |
| Source id dropped from OKF body | Stale footnote deleted with prose | Pairing check reports the orphan; delete or re-cite the `sources` entry | 1:1 sources/footnotes |
| Truth block growth | New executor/client bullets push the block past 40 lines | Merge bullets; keep `<= 40` lines | Bounded truth block |
| Internal name leak | Worksheet wording copied into `docs/**` | Leak grep `native-[a-z-]+-drivers?\|pending_flow_id\|driver work\|driver-fail-loud\|ch[0-9]{2}` returns matches; rewrite | No flow/spec ids in user docs |
| `memory://` example | Global search for `memory://` in the usage guide | Example `QueueService(QueueConfig(backend_url="memory://"))` stays | Valid in-process backend kept |

### 2.5 Acceptance Scenarios

- Given the rebuilt docs, when a reader opens the queues usage guide, then the PostgreSQL, Oracle, and Cloud Spanner rows require `executor=` and name `psycopg[binary,pool]`, `oracledb`, and `vertebra-core[spanner]`, and the Redis and Valkey rows require the `redis` package and a host URL or `client=`.
- Given the queues reference, when a reader scans the Storage Backends row and Queue Backends section, then `SpannerQueueBackend` appears in both and no block lists `claim`.
- Given `distributed-queues-and-email.md`, when an agent searches for `multiplexed`, `vertebra_tasks`, `ZPOPMIN`, or `DBMS_AQ`, then it finds no match, and the truth block states both raise rules.

### 2.6 Alternatives Considered

| Alternative | Outcome |
| --- | --- |
| Fold queue docs into `ch11-docs-and-okf` | Rejected: ch11 already owns 11 files across 4 tasks and excludes `docs/usage/queues-and-realtime.md`; a separate chapter keeps the task at 3 files |
| Remove the Oracle, PostgreSQL, and Spanner rows until live executors ship | Rejected: the classes, protocols, and SQL ship and accept injected executors; readers need the requirement |
| Keep `valkey://` as the Valkey scheme without a caveat | Rejected: after ch10 the `redis` client raises on that scheme at construction |
| Split into one task per file | Rejected: three short edits share one fact table and one doc gate; one task stays within `read_set <= 6` and `estimated_loc <= 250` |

## 3. Documentation & OKF Knowledge Plan

- **Progress**: `[3/3 files documented]`
- `[x]` `docs/usage/queues-and-realtime.md` — backend table requirement column, install packages, injection sentence; wound-wait claim removed (t01)
- `[x]` `docs/reference/queues.md` — Spanner in intro, Storage Backends row, and autodoc blocks; requirement clause; `claim` member replaced by `dequeue` (t01)
- `[x]` `.agents/bundles/knowledge/domains/framework/distributed-queues-and-email.md` — corrected backend table, executor-or-raise and client-or-raise rules, refreshed provenance (t01)

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Doc Gate |
| --- | --- | --- | --- |
| R17 | [t01](tasks/t01-queue-docs-and-knowledge.md) | `documentation_validation` | stale-claim grep, required-claim grep, OKF pairing check, relative-link check, `make docs`, `make review-check` |

## 5. Implementation Plan

- [x] [3cee2a23] driver-fail-loud:ch12:t01 — Queue usage guide, queue reference, and OKF queues chapter state executor/client requirements and drop unshipped mechanics (tasks/t01-queue-docs-and-knowledge.md)
- **Chapter / Phase Recovery Checkpoint**: `make review-check && make docs && uv run --no-sync pytest src/py/tests/unit/tools/test_docs_tooling.py -q`

## 6. Current State & Remaining Plan Snapshot

- **Verified Baseline (`HEAD`)**: `3cee2a23` — chapter completed and verified (`1/1` tasks completed).
- **Remaining Tasks**: None (`1/1` completed).
- **Documentation & OKF Sync**: `[3/3 files documented]`.
- **Active Invariants**: `memory://` queue docs unchanged; changelog untouched; no internal identifiers in user docs.

[^docs-queues-usage]: `docs/usage/queues-and-realtime.md` — Queues, LiveState, email, and telemetry usage guide with the queue backend table.
[^docs-queues-reference]: `docs/reference/queues.md` — vertebra.framework.queues API reference page.
[^okf-queues]: `.agents/bundles/knowledge/domains/framework/distributed-queues-and-email.md` — OKF distributed queues, cron leader election, and transactional email chapter.
[^queue-factory]: `src/py/vertebra/framework/queues/base.py::QueueService._build_backend` — Queue URL scheme routing to backend constructors.
[^pg-queue]: `src/py/vertebra/framework/queues/postgres.py::PostgreSQLQueueBackend` — PostgreSQL queue backend over vertebra_queue_jobs with LISTEN/NOTIFY wakeups.
[^redis-queue]: `src/py/vertebra/framework/queues/redis.py::RedisQueueBackend` — Redis Streams and scheduled sorted-set queue backend and its Valkey subclass.
[^oracle-queue]: `src/py/vertebra/framework/queues/oracle.py::OracleQueueBackend` — Oracle queue backend using FOR UPDATE SKIP LOCKED over vertebra_queue_jobs.
[^spanner-queue]: `src/py/vertebra/framework/queues/spanner.py::SpannerQueueBackend` — Spanner queue backend with hash-sharded keys and FOR UPDATE claims.
[^ch10-contract]: `.agents/bundles/specs/driver-fail-loud/chapters/ch10-queue-backend-fail-loud/spec.md` — PostgreSQL, Redis, and Spanner queue fail-loud contracts.
[^ch02-oracle-contract]: `.agents/bundles/specs/driver-fail-loud/chapters/ch02-duckdb-sqlalchemy-queue-fail-loud/tasks/t03-oracle-queue-requires-oracledb.md` — Oracle queue executor-or-raise contract.
