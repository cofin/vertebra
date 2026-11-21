---
type: Task
id: "native-cursor-data-path:wire:t01"
chapter_id: wire-parity
title: "PostgreSQL async hop budget"
description: "Split the latency of one pooled async PostgreSQL query into call-to-write, server round trip, and readable-to-resume for the native driver and the comparator, name the crossing that holds the remaining difference, and answer the charter questions from the code."
worksheet_format: full
status: draft
state: completed
priority: P1
verification_strategy: characterization
depends_on: []
read_set:
  - ".agents/bundles/specs/native-cursor-data-path/chapters/wire-parity/spec.md"
  - ".agents/bundles/specs/native-cursor-data-path/chapters/single-call-paths/spec.md"
  - "tools/benchmarks/postgres.py::_measure_async_vertebra_point_select"
  - "tools/benchmarks/postgres.py::_measure_asyncpg_point_select"
  - "tools/benchmarks/fixtures.py::FIXTURE_SPECS"
  - "src/py/vertebra/db/adapters/postgres/_async.py::AsyncPostgreSQLDriver.select_value"
  - "src/rs/vertebra-postgres/src/python_pool.rs::PyAsyncPostgresPool"
  - "src/rs/vertebra-postgres/src/session.rs::PostgresConnection"
  - "src/rs/vertebra-postgres/src/session.rs::ConnectionStream"
  - "src/rs/vertebra-postgres/src/session.rs::SessionBacking"
  - "src/rs/vertebra-postgres/src/session.rs::CommandBudget"
  - "src/rs/vertebra-postgres/src/pool.rs::PostgresPool"
  - "src/rs/vertebra-postgres/src/pool.rs::PooledConnection"
  - "src/rs/vertebra-postgres/src/cancel.rs"
  - "src/rs/vertebra-postgres/src/tls.rs"
  - "src/rs/vertebra-db/src/connection.rs::TransportStream"
  - "src/rs/vertebra-core/src/py_async.rs"
files:
  - .agents/bundles/specs/native-cursor-data-path/chapters/wire-parity/spec.md
tests: []
estimated_loc: 40
plan_revision: 4
plan_commit: 06e99fb
state_revision: 1
claimed_by: null
claimed_at: null
commit: null
created_at: 2026-11-21T16:23:03Z
sources:
  - id: wire-spec
    resource: .agents/bundles/specs/native-cursor-data-path/chapters/wire-parity/spec.md
    title: "Chapter charter, questions Q1 to Q8, and the table this task fills"
  - id: bench-pg
    resource: tools/benchmarks/postgres.py::_measure_async_vertebra_point_select
    title: "The benchmark call for the PostgreSQL async pooled row"
  - id: async-bridge
    resource: src/rs/vertebra-core/src/py_async.rs
    title: "Tokio task spawn, completion queue, and loop drainer"
  - id: pg-connection
    resource: src/rs/vertebra-postgres/src/session.rs::PostgresConnection
    title: "Owner of the PostgreSQL socket"
tags: [performance, postgres, asyncio, measurement]
---

# t01 — PostgreSQL async hop budget

## Objective & Scope Boundary

The chapter spec's section 6 says where the time of one pooled async PostgreSQL query goes, for the native driver and for `asyncpg`, and which of the two thread crossings holds the difference that is left. It also holds an answer to each charter question Q1 to Q8.[^wire-spec]

Precondition: the closing capture of `single-call-paths` reports the PostgreSQL async pooled row above `1.0`. When it does not, mark this task `deferred` with the reported ratio as the reason and stop.

This is a measurement task. It edits one file: the chapter spec, section 6 only. It makes no edit under `src/` or `tools/`. Scripts, traces, and reports live under a directory from `mktemp -d` and are not committed.

## Bounded Read Manifest

- The chapter spec, for the charter, the questions, and the table to fill.
- The `single-call-paths` chapter spec, for the names of the single-call methods as built.
- `_measure_async_vertebra_point_select` and `_measure_asyncpg_point_select` for the exact calls the acceptance row times.[^bench-pg]
- `FIXTURE_SPECS` for the PostgreSQL container image and port.
- `AsyncPostgreSQLDriver.select_value` for the path from the driver to the pool call.
- The Rust symbols in `read_set`, for the charter answers. Read `src/rs/vertebra-core/src/py_async.rs` from the task spawn function through the drainer.[^async-bridge][^pg-connection]

## Symbol & Signature Contracts

No production symbol is added or changed. The task writes two throwaway scripts under `"$out"`.

`"$out/pg_hops.py"`, run as `python pg_hops.py --driver {vertebra,asyncpg} --dsn <url> --iterations <n> --warmup <n>`:

- `vertebra`: build the driver as the benchmark does, `AsyncPostgreSQLDriver(config)` with the configuration the benchmark derives from the DSN, with the pool limited to one connection. The measured call is `await driver.select_value("SELECT $1::int4", (42,), value_type=int)`.
- `asyncpg`: one connection from `asyncpg.connect(dsn)`. The measured call is `await conn.fetchval("SELECT $1::int4", 42)`.
- Before the loop, the script duplicates a descriptor for `/dev/null` onto descriptor `900`.
- Each measured iteration runs: `os.write(900, b"B")`, read `time.perf_counter_ns()`, the measured call, read `time.perf_counter_ns()`, `os.write(900, b"E")`.
- The script prints the median of the measured deltas in microseconds, and the process id.
- Warmup iterations run the same call with no marker writes.

`"$out/pg_hops_parse.py"`, run as `python pg_hops_parse.py <trace file> --port <server port>`:

- It finds the socket descriptor from the `connect` line whose address has the server port. With one pooled connection there is one.
- For each pair of `B` and `E` marker writes it computes the parts defined below, and it prints the median of each part, the thread id that ran each socket call, and the per-query counts.

Part definitions, the same for both drivers:

| Part | Start | End |
| --- | --- | --- |
| Call to request bytes written | The `B` marker write | Return of the first send-type call on the socket after `B` |
| Server round trip | Return of that send-type call | Return of the readiness wait that precedes the first receive-type call on the socket with a positive result. When the receive-type call is not preceded by a readiness wait on its thread, use the start of the receive-type call |
| Reply bytes readable to coroutine resumed | End of the server round trip | The `E` marker write |

- Send-type calls: `sendto`, `sendmsg`, `write`, `writev` on the socket descriptor. Receive-type calls: `recvfrom`, `recvmsg`, `read`, `readv` on the socket descriptor. Readiness waits: `epoll_wait`, `epoll_pwait`, `epoll_pwait2`.
- Wake syscalls per query: the count of `futex` calls plus 8-byte `write` calls to a descriptor that is not the socket and not `900`, between `B` and `E`, over all threads.
- The three parts sum to the traced total, `E` minus `B`.

## Edge Cases & Error Semantics

- Tracing slows the process. Record the untraced total and the traced total, and report each part both in traced microseconds and as a share of the traced total.
- `strace` is not installed or not permitted: use `perf trace` with the same syscall list. Neither is permitted: stop and report. Do not estimate the parts.
- The native driver opens more than one connection to the server port: the pool limit did not apply. Fix the script's configuration, not the driver.
- The receive on the native driver runs on a different thread from the marker writes. That is expected and is crossing 1 and crossing 2 made visible: record both thread ids.
- A part's median is negative or the parts do not sum to the traced total within 5 percent: the parser matched the wrong calls. Fix the parser before recording.
- Run-to-run spread: take three untraced runs per driver and record the median of the three medians. Take one traced run per driver.
- The PostgreSQL container is not available: stop and report. The task is not marked complete with an empty table.
- A charter question the code does not settle is answered with: what the code shows, with its symbol; what is missing; and the smallest experiment that would settle it. No answer is written from assumption.

## Public-Seam Test Blueprint

There is no test file. The checks are on the recorded table:

| Check | Expected |
| --- | --- |
| Untraced total, native driver | Within 15 percent of the PostgreSQL async pooled median in the closing capture of `single-call-paths` |
| Untraced total, comparator | Within 15 percent of the `asyncpg` median in that capture |
| Parts, both drivers | Three positive medians that sum to the traced total within 5 percent |
| Thread ids, comparator | The send and the receive run on the thread that writes the markers |
| Thread ids, native driver | Recorded for the send and for the receive |
| Charter answers | Eight entries, Q1 to Q8, each with a symbol under `src/` that `git grep` finds |
| `git status --short -- src tools` | No change from this task |

Record in the chapter spec, section 6, under "PostgreSQL async hop budget":

1. The six rows of the table, with the unit in each row label kept.
2. The line "Crossing that holds the remaining difference". Write one of: `caller to Tokio task`, `Tokio task to event loop`, `both, in the measured shares`, or `neither; the difference is in <part>`. Give the microseconds that support it.
3. A list "Charter answers" with one item per question, in order.
4. One line with the run date, the Python version, the server image, and whether `strace` or `perf trace` was used.

Then update the first bullet of section 6 to say that t01 ran. Change nothing else in the spec.

## Verification

```bash
make build
out="$(mktemp -d)"
uv run --no-sync python "$out/pg_hops.py" --driver vertebra --dsn "$PG_DSN" --iterations 2000 --warmup 200
uv run --no-sync python "$out/pg_hops.py" --driver asyncpg --dsn "$PG_DSN" --iterations 2000 --warmup 200
uv run --no-sync strace -f -ttt -T -e trace=connect,sendto,sendmsg,recvfrom,recvmsg,read,write,readv,writev,futex,epoll_wait,epoll_pwait,epoll_pwait2 -o "$out/vertebra.strace" python "$out/pg_hops.py" --driver vertebra --dsn "$PG_DSN" --iterations 300 --warmup 100
uv run --no-sync strace -f -ttt -T -e trace=connect,sendto,sendmsg,recvfrom,recvmsg,read,write,readv,writev,futex,epoll_wait,epoll_pwait,epoll_pwait2 -o "$out/asyncpg.strace" python "$out/pg_hops.py" --driver asyncpg --dsn "$PG_DSN" --iterations 300 --warmup 100
uv run --no-sync python "$out/pg_hops_parse.py" "$out/vertebra.strace" --port "$PG_PORT"
uv run --no-sync python "$out/pg_hops_parse.py" "$out/asyncpg.strace" --port "$PG_PORT"
git status --short -- src tools
grep -c "not measured" .agents/bundles/specs/native-cursor-data-path/chapters/wire-parity/spec.md
```

`PG_DSN` and `PG_PORT` are the URL and port of the PostgreSQL fixture container. Run each untraced line three times. `git status` prints no change from this task. The last command counts only the Oracle table's cells once this task is done.

## Acceptance Criteria

- [x] The hop budget table is filled for both drivers, with untraced and traced totals.
- [x] The three parts sum to the traced total within 5 percent for both drivers.
- [x] The crossing that holds the remaining difference is named with its microseconds.
- [x] Q1 to Q8 each have an answer with a code reference, or a statement of what the code does not settle and what would.
- [x] No file under `src/` or `tools/` changed, and no script, trace, or report is committed.

## Notes & Discoveries

None yet.

[^wire-spec]: `.agents/bundles/specs/native-cursor-data-path/chapters/wire-parity/spec.md`
[^bench-pg]: `tools/benchmarks/postgres.py::_measure_async_vertebra_point_select`
[^async-bridge]: `src/rs/vertebra-core/src/py_async.rs`
[^pg-connection]: `src/rs/vertebra-postgres/src/session.rs::PostgresConnection`
