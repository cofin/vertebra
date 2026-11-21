---
type: Task
id: "native-cursor-data-path:single:t15"
chapter_id: single-call-paths
title: "Oracle bind fast path and sync single-call methods on client and pool"
description: "Oracle parameter extraction checks exact container types first, and SyncOracleClient and SyncOraclePool gain fetch_rows, fetch_dicts, fetch_value, and execute_dml."
worksheet_format: compact
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-cursor-data-path:cleanup:t17"
read_set:
  - ".agents/bundles/specs/native-cursor-data-path/spec.md"
  - ".agents/bundles/research/native-cursor-hot-paths_20261121.md"
  - "src/rs/vertebra-oracle/src/cursor.rs:28-45"
  - "src/rs/vertebra-oracle/src/cursor.rs:339-381"
  - "src/rs/vertebra-oracle/src/cursor.rs:474-666"
  - "src/rs/vertebra-oracle/src/cursor.rs:729-934"
  - "src/rs/vertebra-oracle/src/cursor.rs:1028-1090"
  - "src/rs/vertebra-oracle/src/client.rs:845-934"
  - "src/rs/vertebra-oracle/src/client.rs:2052-2096"
  - "src/rs/vertebra-oracle/src/client.rs:2643-2672"
  - "src/rs/vertebra-oracle/src/pool.rs:357-424"
  - "src/rs/vertebra-oracle/src/pool.rs:504-570"
  - "src/rs/vertebra-oracle/src/pool.rs:708-720"
  - "src/rs/vertebra-oracle/src/pool.rs:889-1000"
  - "src/rs/vertebra-oracle/src/errors.rs:186-280"
  - "src/rs/vertebra-oracle/src/errors.rs:376-410"
  - "src/py/vertebra/_oracle.pyi:137-330"
  - "src/py/vertebra/_oracle.pyi:508-606"
  - "src/py/vertebra/db/adapters/oracle/core.py:717-723"
  - "src/py/tests/unit/db/oracle_test_doubles.py:239-470"
  - "src/py/tests/unit/db/oracle_test_doubles.py:1177-1330"
files:
  - "src/rs/vertebra-oracle/src/cursor.rs"
  - "src/rs/vertebra-oracle/src/client.rs"
  - "src/rs/vertebra-oracle/src/pool.rs"
  - "src/rs/vertebra-oracle/src/errors.rs"
  - "src/py/vertebra/_oracle.pyi"
tests:
  - "src/py/tests/integration/db/test_oracle_native.py"
  - "src/py/tests/unit/db/oracle_test_doubles.py"
estimated_loc: 380
plan_revision: 4
plan_commit: 06e99fb
state_revision: 1
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
created_at: 2026-11-21T16:23:02Z
commit: null
sources:
  - id: root-spec
    resource: .agents/bundles/specs/native-cursor-data-path/spec.md
    title: "Root spec, requirements R16 and R19"
  - id: oracle-cursor
    resource: "src/rs/vertebra-oracle/src/cursor.rs::SyncOracleCursor"
    title: "Sync cursor whose execute holds the statement run and the bind extraction"
  - id: oracle-pool
    resource: "src/rs/vertebra-oracle/src/pool.rs::SyncOraclePool"
    title: "Sync pool class and its checkout core"
tags: [oracle, rust, pyo3, performance]
---

# Oracle bind fast path and sync single-call methods on client and pool

## Objective & Scope Boundary

One call on `SyncOracleClient` or `SyncOraclePool` runs a statement and returns finished Python objects. A `tuple`, `list`, or `dict` of parameters skips the protocol checks, and an empty container is treated as no parameters.

`estimated_loc` is above 300. The extensions are built without PyO3's `multiple-pymethods` feature, so the client methods and the pool methods have to sit in `client.rs` and `pool.rs`, and the bind extraction and the runner both live in `cursor.rs`. The count is about 240 Rust lines, 70 stub lines, and 70 lines in the class doubles. Tests add about 170 lines.

Out of scope:

- The async classes (t16) and the Python drivers (t17, t18).
- The existing `query` and `execute` methods of the client classes. They stay until the removal audit decides.
- When a pool release sends a rollback (t31).
- The wire. The research brief leaves the remaining gap inside the extension call to the `wire-parity` chapter.

## Bounded Read Manifest

- Root spec, section "Single-call methods (R16)", for the return shapes.
- `prepare_bind_args_and_out_binds` in `cursor.rs` (`:483-666`) is the parameter extraction. A tuple reaches the positional loop only after a failed `dict` cast and two attribute probes.
- `store_reply` in `cursor.rs` (`:833-898`) turns a worker reply into cursor state. `build_execute_command` (`:900-934`) picks the worker command. `execute` on `SyncOracleCursor` (`:1032-1090`) is the sync statement run.
- `statement_facts` in `client.rs` (`:929-934`) gives the statement kind and the session-context flag from a cache keyed by SQL text.
- `client.rs:2643-2672` is `cursor()` on `SyncOracleClient`, with the four cursor options.
- `acquire_sync` on `OraclePoolCore` in `pool.rs` (`:357-424`) and `release_sync` (`:504-570`) are what `SyncOraclePool.acquire` and `release` call.
- `core.py:717-723` is the Python rule for a fatal transport error. The pool method applies the same rule inside the extension.

## Contracts & Edge Cases

### Bind fast path (R19)

`prepare_bind_args_and_out_binds`, new order after the existing `None` returns:

1. Exact `tuple` (`is_exact_instance_of` for `PyTuple`) or exact `list`: empty returns `(BindArgs::Positional(Vec::new()), explicit_spec, OutBindVars::None)` at once. Otherwise the items are read from the container directly by the positional loop.
2. Exact `dict`: empty returns the same no-parameter value as step 1. Otherwise the existing `dict` branch.
3. Any other type, in the order the function uses now: a `dict` subclass, an object with `keys` and `items`, the `str`, `bytes`, and `bytearray` rejection, then `try_iter`.

Move the positional loop (`:626-665`) into one private function that takes an iterator of items, so the exact path and the `try_iter` path share it. Error texts and out-bind handling do not change. `coerce_bind_seq` calls the same function per parameter set and needs no edit.

### Shared runner in `cursor.rs`

Extract from `execute` on `SyncOracleCursor` and `store_reply`. All items are `pub(crate)`.

| Item | Contract |
| --- | --- |
| `struct PreparedExecute { cmd: WorkerCommand, facts: StatementFacts, out_spec: OutBindSpec, out_vars: OutBindVars, options: OracleValueOptions, lowercase: bool }` | A statement ready to dispatch. `OutBindVars` becomes `pub(crate)`. |
| `fn prepare_execute(py, sql: &str, params: Option<&Bound<'_, PyAny>>, out_binds: Option<&Bound<'_, PyAny>>, options: OracleValueOptions, lowercase: bool) -> PyResult<PreparedExecute>` | `prepare_bind_args_and_out_binds` with `reject_out_bind_vars` false, `statement_facts(sql)`, then `build_execute_command`. |
| `struct BuiltReply { description: Option<Py<PyTuple>>, rows: Vec<Py<PyTuple>>, rowcount: i64, lastrowid: Option<Py<PyAny>>, out_binds: Option<Py<PyAny>>, touches_session_context: bool }` | The Python values of one reply. |
| `fn build_reply(py, reply: WorkerReply, facts: StatementFacts, options: OracleValueOptions, lowercase: bool, out_spec: &OutBindSpec, out_vars: &OutBindVars) -> PyResult<BuiltReply>` | The body of `store_reply` without the state writes. It fills out-bind variables in place, as today. `RowCount(n)` gives no description and `rowcount` `n`. Any other reply gives no description and `rowcount` `0`. |

`store_reply` keeps its signature and becomes `build_reply` followed by `state.store(...)` and the two state fields. `execute` on `SyncOracleCursor` calls `prepare_execute` and keeps its lock, its lazy begin, and its state handling.

### Result shapers in `cursor.rs`

Each takes `(py: Python<'_>, built: BuiltReply)` and returns `PyResult<Py<PyAny>>`. Column names are the first item of each description entry, so they already follow the casing option.

| Item | Returns |
| --- | --- |
| `reply_rows_and_names` | `(list_of_tuples, tuple_of_names)`. `([], ())` when the description is `None`. |
| `reply_dicts` | A list with one `dict` per row, keyed by column name. With a repeated name the last column wins. |
| `reply_first_value` | First cell of the first row. No row: `NotFoundError` with the message `Query returned no rows`. A row with no cell: `None`. |
| `reply_dml` | `(rowcount, lastrowid, out_binds, touches_session_context)`. `rowcount` is never negative. `lastrowid` and `out_binds` are `None` when absent. |

Add `cursor_not_found_err(py, message: &str) -> PyErr`, built like `cursor_interface_err`. In `errors.rs` make the `not_found` field of the exception cache `pub(crate)`. That is the only edit to `errors.rs`.

### Return shape decision

- `fetch_rows`, `fetch_dicts`, and `fetch_value` return exactly the shapes in the root spec.
- `execute_dml` on the Oracle classes returns four items: `(rowcount, lastrowid, out_binds, touches_session_context)`. The driver needs the last two for `out_binds` and for session-context tracking, and a single call has no cursor to read them from. The first two items are the root spec's pair.
- A statement run through a fetch method still fills out-bind variables passed in the parameters. When the statement returns no rows but has out-bind values, the fetch methods return those values as one row, as the cursor does.

### Python methods

On `SyncOracleClient` (`client.rs`, next to `cursor()`) and on `SyncOraclePool` (`pool.rs`), four methods each. The keyword-only extras are `out_binds`, which is the extra of the cursor's `execute`, and the four options of `cursor()`, because a single call has no cursor to carry them.

```rust
#[pyo3(
    signature = (
        sql,
        params = None,
        *,
        out_binds = None,
        enable_uuid_binary = true,
        fetch_decimals = false,
        vector_return_format = "list",
        enable_lowercase_column_names = true,
    ),
    text_signature = "($self, sql, params=(), *, out_binds=None, enable_uuid_binary=True, fetch_decimals=False, vector_return_format='list', enable_lowercase_column_names=True)"
)]
#[allow(clippy::too_many_arguments)]
pub fn fetch_rows(
    &self,
    py: Python<'_>,
    sql: &str,
    params: Option<&Bound<'_, PyAny>>,
    out_binds: Option<&Bound<'_, PyAny>>,
    enable_uuid_binary: bool,
    fetch_decimals: bool,
    vector_return_format: &str,
    enable_lowercase_column_names: bool,
) -> PyResult<Py<PyAny>>
```

`fetch_dicts`, `fetch_value`, and `execute_dml` have the same parameters.

Client body, in order: `parse` on `VectorReturnFormat` (error through `to_py_err`), build `OracleValueOptions`, `prepare_execute`, `py.detach` around `worker.dispatch_sync(cmd)`, `to_py_err` on failure, then `build_reply` and the shaper. Put the steps after the option parsing in one `pub(crate) fn run_single_call_sync(py, worker: &Arc<OracleWorkerHandle>, prepared: PreparedExecute, shape: fn(Python<'_>, BuiltReply) -> PyResult<Py<PyAny>>) -> PyResult<Py<PyAny>>` in `cursor.rs`.

- No cursor is created and no cursor state is stored.
- A client method never begins a transaction. Inside an open transaction it runs in it.

Pool body: parse the options and call `prepare_execute` first, so a parameter error costs no checkout. Then, inside one `py.detach`:

1. `inner.acquire_sync(None, None, None, None, None, None, None)`.
2. `worker.dispatch_sync(cmd)`.
3. `discard` is true when `worker.is_closed()`, or when the statement failed and `is_fatal_transport(&err)`.
4. `inner.release_sync(&worker, None, discard)`.

Then `build_reply` and the shaper under the interpreter.

- Add `fn is_fatal_transport(err: &OracleError) -> bool` in `pool.rs`: `err.classify()` has `sqlstate == "08006"` or a vendor code of `3113`, `3114`, `3135`, or `12537`. This is the rule in `core.py:717-723`.
- A failed statement raises its own error even when the release also fails. A failed release after a successful statement raises the release error.
- The acquire uses the pool's default timeout, purity, and tag, and passes no session attributes. A caller that needs session attributes or a broker lease checks a client out and calls the client method (t17).
- The release is the one `SyncOraclePool.release` performs with no tag, including its rollback. Task t31 changes when that rollback is sent.
- A closed pool raises the error `acquire` raises today.

### Stub

Add to `SyncOracleClient` and to `SyncOraclePool` in `src/py/vertebra/_oracle.pyi`:

```python
def fetch_rows(
    self,
    sql: str,
    params: Sequence[Any] | Mapping[str, Any] | None = (),
    *,
    out_binds: Mapping[str, str] | Sequence[str] | None = None,
    enable_uuid_binary: bool = True,
    fetch_decimals: bool = False,
    vector_return_format: str = "list",
    enable_lowercase_column_names: bool = True,
) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]: ...
```

The `out_binds` annotation is the one on the cursor's `execute` stub. This stub file writes each body as `...` on the signature line and carries no docstrings; the new entries follow it. `fetch_dicts`, `fetch_value`, and `execute_dml` take the same parameters. Their return types:

| Method | Return type |
| --- | --- |
| `fetch_dicts` | `list[dict[str, Any]]` |
| `fetch_value` | `Any` |
| `execute_dml` | `tuple[int, str \| None, dict[str, Any] \| tuple[Any, ...] \| None, bool]` |

### Class doubles

`src/py/tests/unit/db/oracle_test_doubles.py` mirrors the native classes. Add the four methods, with the stub signatures, to `_StubSyncOracleClient`, `_StubAsyncOracleClient`, `_StubSyncOraclePool`, and `_StubAsyncOraclePool`. The async forms are `async def`.

- A client double runs the statement through its own `cursor(...)` with the four options and shapes the result, so it cannot drift from its cursor. It appends `(method_name, sql, params)` to its `calls`.
- A pool double gains two attributes in `__init__`: `single_call_results: deque[_oracle.OracleQueryResult | BaseException]` and `single_calls: list[tuple[str, str, Any]]`.
- Each single-call method on a pool double does this, in order: append `(method_name, sql, params)` to `single_calls`; `client = self.acquire()` (awaited on the async double); when `single_call_results` is not empty, move its first item to the end of `client.queued_results`; call the same method on `client` with the same arguments; release the client with `self.release(client, discard=discard)`, where `discard` is true when the call raised an error that `OracleCore.is_fatal_transport_error` accepts.
- A test queues a reply for a pooled single call with `stub_pool.single_call_results.append(result)`, where `stub_pool` is the double it passed as `native_pool`. A queued exception is raised by the client double, as it is for a cursor. With nothing queued the client double returns its existing default result.
- A test reads the calls back from `stub_pool.single_calls`, and the client that ran them from `stub_pool.created_clients`.

The async doubles are added here so that t16, t17, and t18 do not edit this file.

## Test Matrix

Rust unit tests go in the existing `tests` module of `cursor.rs`, next to `cursor_bind_coercion_named_colon_strip_typed_param_and_errors` and `cursor_store_reply_matrix_select_decimal_uuid_ddl_dml_returning_plsql_and_session`. Live tests named `test_sync_single_call_*` go in `src/py/tests/integration/db/test_oracle_native.py` and are skipped without a database.

| Case | Input | Expected |
| --- | --- | --- |
| Exact containers | `(1, "a")` and `[1, "a"]` | Equal positional `BindArgs` values |
| Exact dict | `{"a": 1, ":b": 2}` | Named `BindArgs` with keys `a` and `b` |
| Empty containers | `()`, `[]`, `{}`, `None` | Positional `BindArgs` with no values |
| Subclasses | A named tuple; an `OrderedDict`; a `MappingProxyType` | Positional; named; named |
| Text container | `"abc"` | `InterfaceError` with the existing text |
| Out-bind variable in a tuple | `(OracleOutBindVar("NUMBER"),)` | Positional out-bind spec, as today |
| Shapers, select reply | Reply with two rows | `([(...), (...)], ("a", "b"))`; two dicts; first cell |
| Shapers, upper-case names | `lowercase` false | Names `("A", "B")` |
| Shapers, no result set | DDL reply | `([], ())`, `[]`, `NotFoundError`, `(0, None, None, False)` |
| Shapers, DML | `RowCount(2)` | `(2, None, None, False)` |
| Shapers, out-binds | PL/SQL reply with a named out-bind | `execute_dml` item three is `{"out": 99}`; the variable is filled |
| Shapers, session context | `BEGIN DBMS_SESSION.SET_IDENTIFIER(:v); END;` | `execute_dml` item four is `True` |
| Cursor unchanged | The existing `store_reply` matrix test | Passes unchanged |
| Live equality | `client.fetch_rows(sql)` against `client.cursor().execute(sql).fetchall()` | Same rows and names |
| Live pool | 50 sequential `pool.fetch_value("SELECT 1 FROM dual")` on a pool of size 1 | All return `1`; `borrowed_size` is 0 after |
| Live pool error | A statement that raises `ORA-00942`, then a select | The select succeeds |
| Live error class | Unique violation through `execute_dml` | `IntegrityError` |

## Verification

- `export PATH="$HOME/.cargo/bin:$PATH"`
- `make oracle-test`
- `cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-oracle --all-targets -- -D warnings`
- `make dev`
- `uv run --no-sync --frozen python -m mypy.stubtest vertebra._oracle`
- `uv run --no-sync --frozen pytest src/py/tests/unit/db/test_oracle_adapter.py src/py/tests/unit/db/test_oracle_context_notify_and_queues.py -p no:xdist -q`
- `uv run --no-sync --frozen ruff check src/py/tests/unit/db/oracle_test_doubles.py src/py/vertebra/_oracle.pyi`

`make oracle-test` runs the crate's `cargo test` with the interpreter exports a PyO3 crate needs, then the stub check and the Oracle Python suites. The live tests in it run only when an Oracle database is reachable.

## Acceptance Criteria

- [ ] `prepare_bind_args_and_out_binds` tests exact `tuple`, `list`, and `dict` before any other check, and the empty cases return without building a list or a map.
- [ ] `execute` on `SyncOracleCursor` and the eight new methods share `prepare_execute` and `build_reply`.
- [ ] The pool methods check out, run, and release inside one detached section, with the fatal-transport discard rule.
- [ ] The stub diff adds eight methods and no class, and `mypy.stubtest` passes with the allow-list unchanged.
- [ ] Existing cursor and adapter tests pass unchanged.
