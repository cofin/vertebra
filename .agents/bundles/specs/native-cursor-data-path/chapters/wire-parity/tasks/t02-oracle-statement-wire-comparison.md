---
type: Task
id: "native-cursor-data-path:wire:t02"
chapter_id: wire-parity
title: "Oracle statement wire comparison"
description: "Capture one cached SELECT 1 FROM DUAL on a warm connection with the relay off for the native driver and the comparator, compare round trips, bytes, and time inside and outside the wire, and name the differences with code references."
worksheet_format: full
status: draft
state: completed
priority: P1
verification_strategy: characterization
depends_on:
  - "native-cursor-data-path:wire:t01"
read_set:
  - ".agents/bundles/specs/native-cursor-data-path/chapters/wire-parity/spec.md"
  - ".agents/bundles/specs/native-cursor-data-path/chapters/single-call-paths/spec.md"
  - "tools/benchmarks/adapters/sql.py::capture_oracle_report"
  - "tools/benchmarks/adapters/sql.py::_oracle_connect_kwargs"
  - "tools/benchmarks/fixtures.py::FIXTURE_SPECS"
  - "src/py/vertebra/db/adapters/oracle/_sync.py::SyncOracleDriver"
  - "src/rs/vertebra-oracle/src/client.rs"
  - "src/rs/vertebra-oracle/src/cursor.rs"
  - "src/rs/vertebra-oracle/src/pool.rs"
  - "src/rs/vertebra-oracle/src/dialer.rs"
files:
  - .agents/bundles/specs/native-cursor-data-path/chapters/wire-parity/spec.md
tests: []
estimated_loc: 30
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
    title: "Chapter charter and the table this task fills"
  - id: bench-oracle
    resource: tools/benchmarks/adapters/sql.py::capture_oracle_report
    title: "The benchmark calls for the Oracle rows"
  - id: oracle-client
    resource: src/rs/vertebra-oracle/src/client.rs
    title: "Oracle worker session and statement execution"
  - id: oracle-pool
    resource: src/rs/vertebra-oracle/src/pool.rs
    title: "Oracle pool checkout and release"
tags: [performance, oracle, measurement]
---

# t02 — Oracle statement wire comparison

## Objective & Scope Boundary

The chapter spec's section 6 says what one cached statement costs on the wire, with the relay off, for the native Oracle driver and for `oracledb`. It gives the send and receive calls, the bytes each way, and the time before, on, and after the wire. It names each difference with a reference into `src/rs/vertebra-oracle/src/`.[^wire-spec]

Precondition: the closing capture of `single-call-paths` reports the Oracle relay-off row above `1.0`. When it does not, mark this task `deferred` with the reported ratio as the reason and stop.

This task depends on t01 only because both write to section 6 of the same file. A `deferred` t01 does not hold it back.

This is a measurement task. It edits one file: the chapter spec, section 6 only. It makes no edit under `src/` or `tools/`. Scripts, traces, and reports live under a directory from `mktemp -d` and are not committed.

## Bounded Read Manifest

- The chapter spec, for the three possible differences and the table to fill.
- The `single-call-paths` chapter spec, for the single-call methods as built and for its change to pool release. A release sends no rollback after a successful commit or rollback. It still sends one after a query, because the driver cannot prove that no transaction is open. This task measures after that change.
- `capture_oracle_report` and `_oracle_connect_kwargs` for the exact calls and connection arguments the acceptance row uses.[^bench-oracle]
- `FIXTURE_SPECS` for the Oracle container image and port.
- `SyncOracleDriver` for the path from `select_one` to the native call.
- `client.rs`, `cursor.rs`, `pool.rs`, and `dialer.rs` in the Oracle crate, for the references behind each named difference. In `client.rs`, start at the function that runs a query or statement on the worker session and at the prefetch and array size arguments of the client constructors.[^oracle-client][^oracle-pool]

## Symbol & Signature Contracts

No production symbol is added or changed. The task writes two throwaway scripts under `"$out"`.

`"$out/ora_wire.py"`, run as `python ora_wire.py --driver {vertebra,oracledb} --dsn <url> --iterations <n> --warmup <n> [--native-only]`:

- `vertebra`: build the configuration as the benchmark does, from `OracleConfig.from_url(dsn)` with `enable_reset_relay=False`, and open `SyncOracleDriver` on it. The measured call is `driver.select_one("SELECT 1 FROM DUAL")`.
- `vertebra` with `--native-only`: the measured call is the one native method that `SyncOracleDriver.select_one` calls, invoked directly on the native client the driver holds, with the same SQL text. Read the method name from the driver source.
- `oracledb`: connect with the benchmark's connection arguments. The measured call opens a cursor, runs `cur.execute("SELECT 1 FROM DUAL")`, calls `cur.fetchone()`, and closes the cursor, as the benchmark's comparator function does.
- Before the loop, the script duplicates a descriptor for `/dev/null` onto descriptor `900`.
- Each measured iteration runs: `os.write(900, b"B")`, read `time.perf_counter_ns()`, the measured call, read `time.perf_counter_ns()`, `os.write(900, b"E")`.
- The script prints the median of the measured deltas in microseconds.
- Warmup iterations run the same call with no marker writes, so the statement is cached and the connection is warm before the first marker.

`"$out/ora_wire_parse.py"`, run as `python ora_wire_parse.py <trace file> --port <server port>`:

- It finds the socket descriptor from the `connect` line whose address has the server port.
- For each pair of `B` and `E` marker writes it computes the figures defined below, and it prints the median of each.

Figure definitions, the same for both drivers:

| Figure | Definition |
| --- | --- |
| Send calls | Count of `sendto`, `sendmsg`, `write`, `writev` calls on the socket between `B` and `E` |
| Receive calls | Count of `recvfrom`, `recvmsg`, `read`, `readv` calls on the socket with a positive result between `B` and `E` |
| Bytes sent, bytes received | Sum of the results of those calls |
| Time before the first send | The `B` marker write to the start of the first send call |
| Wire time | Start of the first send call to the return of the last receive call |
| Time after the last receive | Return of the last receive call to the `E` marker write |
| Total, traced | `E` minus `B` |

## Edge Cases & Error Semantics

- With the relay off, the trace holds no connection to a loopback relay port. A second `connect` from the native driver during the measured window means the relay is on: fix the script's configuration.
- The native driver's socket calls may run on a thread other than the one that writes the markers. Record the thread id. The figures are computed over all threads.
- Tracing slows the process. Record the untraced total beside the traced figures.
- `strace` is not installed or not permitted: use `perf trace` with the same syscall list. Neither is permitted: stop and report.
- The counts differ between iterations: record the median and the range, and say which iterations differ.
- The Oracle container is not available: stop and report. The task is not marked complete with an empty table.
- A difference is named only when a figure shows it. Equal figures are recorded as equal.
- The comparator row has no "extension call alone" figure. Leave that cell empty.

## Public-Seam Test Blueprint

There is no test file. The checks are on the recorded table:

| Check | Expected |
| --- | --- |
| Untraced total, native driver | Within 15 percent of the Oracle relay-off median in the closing capture of `single-call-paths` |
| Untraced total, comparator | Within 15 percent of the `oracledb` median in that capture |
| Send and receive calls | One integer median per driver, each at least `1` |
| The three time figures | Sum to the traced total within 5 percent, per driver |
| Extension call alone | Recorded for the native driver, and at or below its untraced total |
| Differences named | One entry per row of the charter's table of possible differences: present with its figures and a code reference, or absent with the equal figures |
| `git status --short -- src tools` | No change from this task |

Record in the chapter spec, section 6, under "Oracle statement wire comparison":

1. The ten rows of the table.
2. A list "Differences named" with three items, in the order of the charter's table: an extra round trip, larger frames, row decode or result building time. Each item states whether the capture shows it, the figures, and the symbol in `src/rs/vertebra-oracle/src/` where it arises.
3. One line with the run date, the Python version, the server image, and whether `strace` or `perf trace` was used.

Then update the first bullet of section 6 to say that t02 ran. Change nothing else in the spec.

## Verification

```bash
make build
out="$(mktemp -d)"
uv run --no-sync python "$out/ora_wire.py" --driver vertebra --dsn "$ORA_DSN" --iterations 1000 --warmup 100
uv run --no-sync python "$out/ora_wire.py" --driver vertebra --native-only --dsn "$ORA_DSN" --iterations 1000 --warmup 100
uv run --no-sync python "$out/ora_wire.py" --driver oracledb --dsn "$ORA_DSN" --iterations 1000 --warmup 100
uv run --no-sync strace -f -ttt -T -e trace=connect,sendto,sendmsg,recvfrom,recvmsg,read,write,readv,writev -o "$out/vertebra.strace" python "$out/ora_wire.py" --driver vertebra --dsn "$ORA_DSN" --iterations 200 --warmup 100
uv run --no-sync strace -f -ttt -T -e trace=connect,sendto,sendmsg,recvfrom,recvmsg,read,write,readv,writev -o "$out/oracledb.strace" python "$out/ora_wire.py" --driver oracledb --dsn "$ORA_DSN" --iterations 200 --warmup 100
uv run --no-sync python "$out/ora_wire_parse.py" "$out/vertebra.strace" --port "$ORA_PORT"
uv run --no-sync python "$out/ora_wire_parse.py" "$out/oracledb.strace" --port "$ORA_PORT"
git status --short -- src tools
```

`ORA_DSN` and `ORA_PORT` are the URL and port of the Oracle fixture container. Run each untraced line three times and record the median of the three medians. `git status` prints no change from this task.

## Acceptance Criteria

- [x] The wire table is filled for both drivers.
- [x] Call counts and byte totals come from the trace, and the three time figures sum to the traced total within 5 percent.
- [x] Each of the three possible differences is recorded as present or absent, with figures, and each present one has a code reference.
- [x] No file under `src/` or `tools/` changed, and no script, trace, or report is committed.

## Notes & Discoveries

None yet.

[^wire-spec]: `.agents/bundles/specs/native-cursor-data-path/chapters/wire-parity/spec.md`
[^bench-oracle]: `tools/benchmarks/adapters/sql.py::capture_oracle_report`
[^oracle-client]: `src/rs/vertebra-oracle/src/client.rs`
[^oracle-pool]: `src/rs/vertebra-oracle/src/pool.rs`
