---
type: Task
id: "driver-fail-loud:ch12:t01"
chapter_id: ch12-queue-docs-and-knowledge
title: State queue backend executor and client requirements in the queues guide, reference, and OKF chapter
description: Rewrite the queue backend table in the usage guide, the Storage Backends row and autodoc blocks in the queues reference, and the OKF distributed-queues backend section so each network backend states its executor-or-raise or client-or-raise rule and install package, and no file claims mechanics the code does not implement.
status: stable
state: completed
priority: P2
verification_strategy: documentation_validation
depends_on: []
read_set:
  - docs/usage/queues-and-realtime.md
  - docs/reference/queues.md
  - .agents/bundles/knowledge/domains/framework/distributed-queues-and-email.md
  - src/py/vertebra/framework/queues/postgres.py
  - src/py/vertebra/framework/queues/redis.py
  - src/py/vertebra/framework/queues/spanner.py
files:
  - docs/usage/queues-and-realtime.md
  - docs/reference/queues.md
  - .agents/bundles/knowledge/domains/framework/distributed-queues-and-email.md
tests: []
estimated_loc: 90
plan_revision: 2
plan_commit: c2b09b9b
state_revision: 1
claimed_by: agent:flow-executor
claimed_at: "2026-11-09T15:14:30Z"
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: "2026-11-09T15:16:39Z"
last_verified_commit: adaf1243
verification_evidence: "stale-claim grep, required-claim grep, OKF pairing check (11 sources), relative-link check, make docs, make review-check, and pytest src/py/tests/unit/tools/test_docs_tooling.py -q (28 passed)"
created_at: "2026-11-07T00:00:00Z"
updated_at: "2026-11-09T15:16:39Z"
commit: adaf1243
generated:
  by: flow/0.28.0
  at: "2026-11-07T00:00:00Z"
verified:
  - by: agent:flow-executor
    at: "2026-11-09T15:16:39Z"
    commit: adaf1243
sources:
  - id: docs-queues-usage
    resource: docs/usage/queues-and-realtime.md
    title: Queues usage guide backend table
  - id: docs-queues-reference
    resource: docs/reference/queues.md
    title: vertebra.framework.queues API reference page
  - id: okf-queues
    resource: .agents/bundles/knowledge/domains/framework/distributed-queues-and-email.md
    title: OKF distributed queues and email chapter
  - id: pg-queue
    resource: src/py/vertebra/framework/queues/postgres.py::PostgreSQLQueueBackend
    title: PostgreSQL queue backend executor guard
  - id: redis-queue
    resource: src/py/vertebra/framework/queues/redis.py::RedisQueueBackend._build_client
    title: Redis client factory and ValkeyQueueBackend subclass
  - id: spanner-queue
    resource: src/py/vertebra/framework/queues/spanner.py::SpannerQueueBackend
    title: Spanner queue backend scheme check and executor guard
  - id: oracle-queue
    resource: src/py/vertebra/framework/queues/oracle.py::OracleQueueBackend
    title: Oracle queue backend executor guard
  - id: queue-factory
    resource: src/py/vertebra/framework/queues/base.py::QueueService._build_backend
    title: Queue URL scheme routing
tags:
  - docs
  - okf
  - queues
  - fail-loud
---

# Task t01: Queue Docs and Knowledge

## Objective & Scope Boundary

Rewrite three files so they describe the post-ch10 queue backends.[^docs-queues-usage][^docs-queues-reference][^okf-queues]

In scope:

- `docs/usage/queues-and-realtime.md::Background Task Queues, Backends & Cron (vertebra.framework.queues)` — the backend table and the prose directly after it.
- `docs/reference/queues.md` — intro paragraph, component table "Storage Backends" row, and the "Queue Backends" autodoc section.
- `.agents/bundles/knowledge/domains/framework/distributed-queues-and-email.md` — frontmatter `verified` and `sources`, the truth block, "1. Distributed Queue Backends (`framework/queues/`)", the word "seamless" in "2. Multi-Lane Worker Runner & Cron Leader Election", and the footnote list.

Out of scope: LiveState, email, store, and telemetry sections of the usage guide; the `memory://` code example; Python docstrings and source; `valkey://` scheme support; database driver docs (ch11); `docs/examples/background_queues.py`; the changelog.

## Bounded Read Manifest

- `docs/usage/queues-and-realtime.md` — section "Background Task Queues, Backends & Cron (`vertebra.framework.queues`)".[^docs-queues-usage]
- `docs/reference/queues.md` — intro paragraph, component table, "Queue Backends".[^docs-queues-reference]
- `.agents/bundles/knowledge/domains/framework/distributed-queues-and-email.md` — whole file (frontmatter, truth block, sections 1–2, footnotes).[^okf-queues]
- `src/py/vertebra/framework/queues/postgres.py::PostgreSQLQueueBackend.__init__` — `ensure_module` literals and `ImproperConfigurationError` message.[^pg-queue]
- `src/py/vertebra/framework/queues/redis.py::RedisQueueBackend._build_client` and `ValkeyQueueBackend.__init__` — `ensure_module` literals, hostless-URL message, default `stream_prefix`.[^redis-queue]
- `src/py/vertebra/framework/queues/spanner.py::SpannerQueueBackend.__init__` — accepted schemes, `ensure_module` literals, both `ImproperConfigurationError` messages.[^spanner-queue]

Contracts outside the read set are quoted below and checked by grep in Verification: `oracle.py::OracleQueueBackend.__init__` (`ensure_module("oracledb", package="oracledb", install_package="oracledb")`, message `OracleQueueBackend requires an OracleQueueExecutor; pass executor= to bind an oracledb cursor executor`)[^oracle-queue] and `base.py::QueueService._build_backend` (scheme routing).[^queue-factory]

## Symbol & Signature Contracts

Target text, per section (adapt wording; keep every fact). Quote messages and hints from the code as found.

**Usage guide backend table** — replace the three-column table with four columns `Backend | Scheme / Class | Backing Mechanism | Requires`:

| Backend | Scheme / Class | Backing Mechanism | Requires |
| --- | --- | --- | --- |
| Memory | `memory://` (`MemoryQueueBackend`) | In-process queue for local dev and unit tests | Nothing |
| PostgreSQL | `postgresql://`, `postgres://` (`PostgreSQLQueueBackend`) | `FOR UPDATE SKIP LOCKED` + `LISTEN/NOTIFY` wakeups | `executor=` (`PostgreSQLQueueExecutor`); `psycopg[binary,pool]` |
| Redis | `redis://`, `rediss://` (`RedisQueueBackend`) | Redis Streams consumer groups (`XREADGROUP` / `XACK`) + sorted-set delayed scheduling | `redis` package and a URL with a host, or `client=` (`RedisStreamClient`) |
| Valkey | `ValkeyQueueBackend` | Same Streams + sorted-set mechanics under the `vtb:valkey:queue` prefix | `redis` package and a `redis://` / `rediss://` URL pointing at Valkey, or `client=` |
| Oracle | `oracle://` (`OracleQueueBackend`) | `FOR UPDATE SKIP LOCKED` transactional queue tables | `executor=` (`OracleQueueExecutor`); `oracledb` |
| Cloud Spanner | `spanner://`, `spangres://` (`SpannerQueueBackend`) | Hash-sharded keys + `FOR UPDATE` claims + dead-letter table | `executor=` (`SpannerQueueExecutor`); `vertebra-core[spanner]` |

Directly after the table (before the code block), add prose, no new fenced block:

- Backends that need an executor raise `MissingDependencyError` when their package is missing and `ImproperConfigurationError` when it is installed but no `executor=` is passed. Vertebra does not ship these executors, so build the backend and pass it in: `QueueService(config, backend=PostgreSQLQueueBackend(config, executor=executor))`.
- `RedisQueueBackend` and `ValkeyQueueBackend` raise `MissingDependencyError` without the `redis` package and `ImproperConfigurationError` when `backend_url` has no host (the default `memory://` included), unless `client=` is passed.
- `valkey://` and `valkeys://` URLs route to `ValkeyQueueBackend`, but the backend accepts only `redis://` and `rediss://` URLs with a host; point a `redis://` URL at the Valkey server or pass `client=`. Do not mention `unix://`: a hostless URL raises `ImproperConfigurationError`.
- Install commands: `uv add "psycopg[binary,pool]"`, `uv add redis`, `uv add oracledb`, `uv add "vertebra-core[spanner]"`.

**Reference intro paragraph** — the backend list reads `MemoryQueueBackend`, `PostgreSQLQueueBackend`, `RedisQueueBackend`, `ValkeyQueueBackend`, `OracleQueueBackend`, `SpannerQueueBackend`.

**Reference "Storage Backends" row** — Classes cell adds `SpannerQueueBackend`. Purpose cell: `Persist and claim task envelopes with SKIP LOCKED tables (PostgreSQL, Oracle), Redis Streams consumer groups (Redis, Valkey), or hash-sharded FOR UPDATE claims (Spanner). PostgreSQLQueueBackend, OracleQueueBackend, and SpannerQueueBackend require executor=; RedisQueueBackend and ValkeyQueueBackend require the redis package and a host URL or client=; otherwise construction raises MissingDependencyError or ImproperConfigurationError.` (format identifiers as inline code). Remove `RESP3 streams` and `Oracle AQ/table queues`.

**Reference "Queue Backends" section** — in the four existing `members:` lists replace `- claim` with `- dequeue`; append after the Oracle block:

```text
::: vertebra.framework.queues.spanner.SpannerQueueBackend
    options:
      heading_level: 3
      members:
        - enqueue
        - dequeue
        - ack
        - nack
```

**OKF chapter frontmatter**:

- Keep `type`, `title`, `description`, `status`, `tags`, `generated`.
- Prepend one entry to the `verified` list, keeping the existing entries below it: `by: agent:flow-executor`, `at:` the execution UTC timestamp, `commit:` the `HEAD` short SHA before the task commit.
- Add sources (keep the six existing ones):

```yaml
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
```

  Each new id gets a matching footnote line in the same `` `resource` — title. `` format as the existing six, and at least one inline citation in the body.

**OKF truth block** (stays `<= 40` lines) — keep the two existing bullets and add:

- `OracleQueueBackend`, `PostgreSQLQueueBackend`, and `SpannerQueueBackend` bind an injected `executor=`; without one they raise `MissingDependencyError` when `oracledb`, `psycopg`, or `vertebra._spanner` is missing and `ImproperConfigurationError` otherwise. No live executor ships for these backends.
- `RedisQueueBackend` and `ValkeyQueueBackend` bind an injected `client=`; without one they raise `MissingDependencyError` when `redis.asyncio` is missing, `ImproperConfigurationError` when `backend_url` has no host, and otherwise bind `redis.asyncio.from_url(backend_url)`.
- `QueueService` routes `""` and `memory` schemes to `MemoryQueueBackend`.

**OKF section 1** — intro sentence: `QueueService` dispatches to six backends; Oracle, PostgreSQL, and Spanner run SQL through an injected executor protocol, Redis and Valkey through an injected or URL-built `RedisStreamClient`. Backend table rows:

| Backend | Row Text |
| --- | --- |
| `MemoryQueueBackend` | Unchanged |
| `RedisQueueBackend` | Per-lane Redis Streams with consumer groups (`XADD`, `XREADGROUP`, `XAUTOCLAIM` visibility recovery, `XACK`) plus a scheduled sorted set (`ZADD`) promoted into streams by a `ZRANGEBYSCORE` + `XADD` Lua script; client from `client=` or `redis.asyncio.from_url`; cite `src-framework-queues-redis-py`. |
| `ValkeyQueueBackend` | `RedisQueueBackend` subclass with the same mechanics and client rule, default `stream_prefix` `vtb:valkey:queue`. |
| `PostgreSQLQueueBackend` | `FOR UPDATE SKIP LOCKED` claims over `vertebra_queue_jobs` with `vertebra_queue_dlq` and `vertebra_queue_leases`, `NOTIFY` on `vertebra_queue_wakeup`, through an injected `PostgreSQLQueueExecutor`; cite `src-framework-queues-postgres-py`. |
| `OracleQueueBackend` | `FOR UPDATE SKIP LOCKED` claims over `vertebra_queue_jobs`, leases via `MERGE INTO vertebra_queue_leases`, through an injected `OracleQueueExecutor`; cite `src-framework-queues-oracle-py`. |
| `SpannerQueueBackend` | Hash-sharded primary keys (`shard_count` default `16`), GoogleSQL or Spangres DDL, `FOR UPDATE` claims through an injected `SpannerQueueExecutor`; cite `src-framework-queues-spanner-py`. |

Cite `src-framework-queues-base-py` on the routing sentence. In section 2 replace "seamless failover" with "failover".

## Edge Cases & Error Semantics

- If post-ch10 code differs from a quoted message, hint, or scheme list (for example, the Spanner hint still reads `vtb[spanner]` because ch10 t03 is incomplete), stop and report a contradiction; never document planned behavior.
- If `valkey://` scheme mapping exists in `redis.py` at execution time, drop the `valkey://` caveat and list `valkey://` / `valkeys://` in the Valkey row; add a Notes line.
- Keep the usage-guide `QueueService(QueueConfig(backend_url="memory://"))` example and the `KeyValueStore` / `EmailService` example unchanged.
- No new fenced Python blocks in `docs/**`. The reference `:::` block is a directive, not executable code.
- OKF file: no standalone `---` in the body, no `Revision History`, and every `sources[].id` has exactly one `[^id]:` definition and at least one inline reference.
- No flow, chapter, or spec identifiers, `Previously` / `now` sediment, or exclamation marks in `docs/**`.

## Public-Seam Test Blueprint

Canonical gates: stale-claim grep, required-claim grep, internal-name grep, OKF pairing check, relative-link check, `make docs`, `make review-check`, `src/py/tests/unit/tools/test_docs_tooling.py`.

| Case | File / Section | Expected Text / Claim |
| --- | --- | --- |
| Wound-wait removed | usage guide table | `grep -ci 'wound-wait'` = `0` |
| Executor rows | usage guide table | PostgreSQL row contains `executor=` and `psycopg[binary,pool]`; Oracle row `executor=` and `oracledb`; Cloud Spanner row `executor=` and `vertebra-core[spanner]` |
| Client rows | usage guide table | Redis and Valkey rows contain `redis` package and `client=` |
| Error names | usage guide prose | Contains `MissingDependencyError` and `ImproperConfigurationError` |
| Valkey caveat | usage guide prose | Contains `valkey://` and `rediss://` in one sentence; `grep -c 'unix://'` = `0` |
| Kept example | usage guide code block | `QueueService(QueueConfig(backend_url="memory://"))` present |
| Spanner in reference | `docs/reference/queues.md` | `grep -c SpannerQueueBackend` >= `3` (intro, row, autodoc) |
| No phantom member | `docs/reference/queues.md` | `grep -cE '^\s+- claim$'` = `0`; `grep -cE '^\s+- dequeue$'` = `5` |
| Stale reference claims | `docs/reference/queues.md` | `grep -cE 'RESP3|Oracle AQ'` = `0` |
| OKF stale claims | knowledge chapter | `grep -ciE 'multiplexed|vertebra_tasks|ZPOPMIN|DBMS_AQ|client-side tracking|seamless'` = `0` |
| OKF facts | knowledge chapter | Contains `vertebra_queue_jobs`, `XREADGROUP`, `executor=`, `client=` |
| OKF provenance | knowledge chapter | Pairing script prints `okf ok` (11 sources, 11 footnotes, truth block `<= 40` lines, no body `---`) |
| No leaks | both `docs/**` files | Internal-name grep returns no match |

## Verification

Baseline (expect matches before editing):

```bash
grep -niE 'wound-wait|multiplexed|vertebra_tasks|ZPOPMIN|DBMS_AQ|Oracle AQ|RESP3|client-side tracking' docs/usage/queues-and-realtime.md docs/reference/queues.md .agents/bundles/knowledge/domains/framework/distributed-queues-and-email.md
grep -nE '^\s+- claim$' docs/reference/queues.md
grep -n 'ensure_module("oracledb"' src/py/vertebra/framework/queues/oracle.py
```

The third command must match before editing (ch02 t03 complete); if it does not, stop and set `blocked_reason`.

Final (all must hold):

```bash
! grep -niE 'wound-wait|multiplexed|vertebra_tasks|ZPOPMIN|DBMS_AQ|Oracle AQ|RESP3|client-side tracking|seamless' docs/usage/queues-and-realtime.md docs/reference/queues.md .agents/bundles/knowledge/domains/framework/distributed-queues-and-email.md
! grep -nE '^\s+- claim$' docs/reference/queues.md
test "$(grep -cE '^\s+- dequeue$' docs/reference/queues.md)" -eq 5
test "$(grep -c 'SpannerQueueBackend' docs/reference/queues.md)" -ge 3
grep -n 'psycopg\[binary,pool\]' docs/usage/queues-and-realtime.md
grep -n 'oracledb' docs/usage/queues-and-realtime.md
grep -n 'vertebra-core\[spanner\]' docs/usage/queues-and-realtime.md
grep -n 'uv add redis' docs/usage/queues-and-realtime.md
grep -n 'ImproperConfigurationError' docs/usage/queues-and-realtime.md docs/reference/queues.md
grep -n 'MissingDependencyError' docs/usage/queues-and-realtime.md docs/reference/queues.md
grep -n 'QueueService(QueueConfig(backend_url="memory://"))' docs/usage/queues-and-realtime.md
grep -n 'vertebra_queue_jobs' .agents/bundles/knowledge/domains/framework/distributed-queues-and-email.md
grep -n 'XREADGROUP' .agents/bundles/knowledge/domains/framework/distributed-queues-and-email.md
! grep -rnE 'native-[a-z-]+-drivers?|pending_flow_id|driver work|driver-fail-loud|ch[0-9]{2}' docs/usage/queues-and-realtime.md docs/reference/queues.md
! grep -n 'unix://' docs/usage/queues-and-realtime.md docs/reference/queues.md
sed -n '/^verified:/{n;p;q}' .agents/bundles/knowledge/domains/framework/distributed-queues-and-email.md | grep 'agent:flow-executor'
uv run --no-sync python -I - .agents/bundles/knowledge/domains/framework/distributed-queues-and-email.md <<'PY'
import pathlib, re, sys
text = pathlib.Path(sys.argv[1]).read_text()
_, front, body = text.split("---\n", 2)
ids = re.findall(r"^\s+- id: (\S+)$", front, re.M)
defs = re.findall(r"^\[\^([\w-]+)\]:", body, re.M)
prose = "\n".join(line for line in body.splitlines() if not line.startswith("[^"))
refs = set(re.findall(r"\[\^([\w-]+)\]", prose))
truth = body.split("<!-- truth: start -->")[1].split("<!-- truth: end -->")[0].strip().splitlines()
errors = []
if sorted(ids) != sorted(defs):
    errors.append(f"sources/footnotes mismatch: {set(ids) ^ set(defs)}")
if len(defs) != len(set(defs)):
    errors.append("duplicate footnote definitions")
if set(ids) - refs:
    errors.append(f"uncited sources: {set(ids) - refs}")
if len(truth) > 40:
    errors.append(f"truth block {len(truth)} lines")
if re.search(r"^---\s*$", body, re.M):
    errors.append("standalone --- in body")
if "verified:" not in front:
    errors.append("missing verified")
print("\n".join(errors) or f"okf ok ({len(ids)} sources)")
sys.exit(1 if errors else 0)
PY
uv run --no-sync python -I - docs/usage/queues-and-realtime.md docs/reference/queues.md .agents/bundles/knowledge/domains/framework/distributed-queues-and-email.md <<'PY'
import pathlib, re, sys
bad = []
for p in map(pathlib.Path, sys.argv[1:]):
    for target in re.findall(r"\]\(([^)#\s]+)(?:#[^)]*)?\)", p.read_text()):
        if "://" in target or target.startswith("mailto:"):
            continue
        if not (p.parent / target).resolve().exists():
            bad.append(f"{p}: {target}")
print("\n".join(bad) or "all relative links resolve")
sys.exit(1 if bad else 0)
PY
make docs
make review-check
uv run --no-sync pytest src/py/tests/unit/tools/test_docs_tooling.py -q
```

## Acceptance Criteria

- [x] `docs/usage/queues-and-realtime.md` rows for Oracle, PostgreSQL, Redis, Valkey, and Cloud Spanner state the executor or client requirement and install package, and the wound-wait claim is gone (R17).
- [x] `docs/reference/queues.md` lists `SpannerQueueBackend` in the intro, Storage Backends row, and autodoc blocks, states the requirement, and lists only defined methods (R17).
- [x] `distributed-queues-and-email.md` describes Redis Streams plus sorted set, `vertebra_queue_jobs`, no multiplexed sessions or `DBMS_AQ`, and states the executor-or-raise and client-or-raise rules with 1:1 provenance (R17).
- [x] Every Verification command exits `0`.

## Notes & Discoveries

- [2026-11-07 00:00] At `d2b168dc` no queue backend defined `claim`; `docs/reference/queues.md` listed it in four `members:` blocks while the claim method is `dequeue`.
- [2026-11-07 00:00] At `d2b168dc` `oracle.py` had no `DBMS_AQ` usage and `ValkeyQueueBackend` only changed the default `stream_prefix`; the OKF chapter's Advanced Queuing and client-side tracking rows described unshipped mechanics.
- [2026-11-07 00:00] Update `[k/3 files documented]` in the chapter spec and root spec when closing this task.

[^docs-queues-usage]: `docs/usage/queues-and-realtime.md` — Queues usage guide backend table.
[^docs-queues-reference]: `docs/reference/queues.md` — vertebra.framework.queues API reference page.
[^okf-queues]: `.agents/bundles/knowledge/domains/framework/distributed-queues-and-email.md` — OKF distributed queues and email chapter.
[^pg-queue]: `src/py/vertebra/framework/queues/postgres.py::PostgreSQLQueueBackend` — PostgreSQL queue backend executor guard.
[^redis-queue]: `src/py/vertebra/framework/queues/redis.py::RedisQueueBackend._build_client` — Redis client factory and ValkeyQueueBackend subclass.
[^spanner-queue]: `src/py/vertebra/framework/queues/spanner.py::SpannerQueueBackend` — Spanner queue backend scheme check and executor guard.
[^oracle-queue]: `src/py/vertebra/framework/queues/oracle.py::OracleQueueBackend` — Oracle queue backend executor guard.
[^queue-factory]: `src/py/vertebra/framework/queues/base.py::QueueService._build_backend` — Queue URL scheme routing.
