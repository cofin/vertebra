---
type: Task
id: critical-correctness-fixes:t04
title: DB tool identifier validation and statement classification
state: completed
priority: P1
verification_strategy: regression_tdd
depends_on: []
files:
  - src/py/vertebra/db/mcp_tools.py
  - docs/changelog/index.md
tests:
  - src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py
plan_revision: 2
plan_commit: 24e2931
state_revision: 2
claimed_by: executor
claimed_at: 2026-11-03T16:45:30Z
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: 2026-11-03T16:47:08Z
last_verified_commit: 97dc3df
verification_evidence: "RED: 15 failed, 7 passed in 1.56s. GREEN: 27 passed in 1.19s across test_gcp_adapters_and_sqlalchemy.py; ruff check/format passed; mypy passed (1 source file)."
created_at: 2026-11-03T15:30:00Z
updated_at: 2026-11-03T16:47:08Z
commit: null
tags: [bugfix, security, sql, mcp-tools]
---

# t04 — DB tool identifier validation and statement classification

## Objective

The governed BigQuery/Spanner MCP tools never interpolate an unvalidated table name, never run a second statement hidden behind a semicolon, and send procedural, privilege, export, and load statements through the confirmation gate.

## Context

- **Strategy**: `regression_tdd`. All three defects reproduce at `142cc49`.
- **Defects confirmed at HEAD** (`src/py/vertebra/db/mcp_tools.py`):
  - `register_gcp_database_tools` inner `bigquery_object_table_inspect` builds `f"SELECT uri, content_type, size, md5_hash, updated FROM {table_name} LIMIT {bounded_limit}"` from the raw argument.
  - `is_mutating_sql` returns `_extract_leading_verb(sql) in _MUTATING_VERBS`; `_MUTATING_VERBS` is `{INSERT, UPDATE, DELETE, MERGE, CREATE, ALTER, DROP, TRUNCATE}`. `SELECT 1; DELETE ...`, `EXECUTE IMMEDIATE ...`, `CALL ...`, `GRANT ...`, `REVOKE ...`, `EXPORT DATA ...`, and `LOAD DATA ...` all return `False`, so `bigquery_query` runs them without confirmation.
  - Inner `spanner_query` repeats the leading-verb check and sends non-matching statements to a read-only snapshot.
- **Single-statement rule (decided)**: after removing trailing whitespace and trailing semicolons, any remaining `;` raises `PermissionDeniedError`, including a semicolon inside a literal or comment. GoogleSQL uses backslash escapes while Spangres uses standard strings, `E''` strings, and dollar quoting, so one literal-masking pass can hide a separator in one of the dialects. Rejecting every non-trailing semicolon cannot be bypassed in any dialect; values that contain `;` go in query parameters.
- **Identifier rule (decided)**: `validate_identifier(table_name, "table_name", allow_hyphens=True)` (dots allowed by default) accepts `warehouse.invoices_obj` and `analytics-proj.warehouse.invoices_obj` and raises `ValueError("Invalid SQL identifier for table_name: ...")` for anything else.
- **Exception types**: reuse `vertebra.framework.security.PermissionDeniedError` (already imported by this module and asserted by the existing tests) and `ValueError` from `vertebra.utils.text.validate_identifier`. Moving this module out of `vertebra.db` belongs to `db-engine-primitives`.
- **Canonical test module**: `src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py`. It holds `test_governed_mcp_and_a2a_gcp_database_tools`, `_StubBigQueryDriver`, and imports `FakeSpannerClient`, `ConfirmationGate`, `register_gcp_database_tools`, `is_mutating_sql`, `BigQueryConfig`, `SpannerConfig`, `SyncSpannerDriver`, and `PermissionDeniedError`.
- **Code rules**: `msgspec.Struct(frozen=True, gc=False)` instead of dataclasses; PEP 585/604/695 types; Google-style docstrings; zero inline `#` comments; top-level imports only (except `TYPE_CHECKING` and optional-dependency helpers); class layout attributes → `__init__` → properties → public methods → private methods; JSON through `vertebra.utils.serializers`; `@pytest.mark.parametrize` instead of loops over cases; no compatibility aliases or `try/except` fallbacks for test seeds; no finding IDs in code, tests, or commits.

## Steps

### 1. Write failing tests (RED)

In `src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py`, append after `test_governed_mcp_and_a2a_gcp_database_tools`:

```python
@pytest.fixture
def gcp_database_tools() -> dict[str, Any]:
    """Governed GCP database tools backed by the stub BigQuery driver and the fake Spanner client."""
    bq_driver = _StubBigQueryDriver(
        BigQueryConfig(
            project_id="analytics-proj",
            dataset_id="warehouse",
            rest_endpoint="http://localhost:9050",
            maximum_bytes_billed=1_000,
        )
    )
    spanner_driver = SyncSpannerDriver(
        SpannerConfig(
            project_id="core-proj",
            instance_id="prod-inst",
            database_id="orders-db",
            endpoint="http://localhost:9010",
        ),
        client=FakeSpannerClient(),
    )
    return dict(
        register_gcp_database_tools(
            bigquery_driver=bq_driver,
            spanner_driver=spanner_driver,
            maximum_bytes_billed=1_000,
            confirmation_gate=ConfirmationGate(),
        )
    )


@pytest.mark.parametrize(
    ("sql", "expected"),
    [
        ("SELECT * FROM users", False),
        ("WITH cte AS (SELECT 1) SELECT * FROM cte", False),
        ("SELECT 1;", False),
        ("/* audit */ SELECT 1", False),
        ("INSERT INTO users (id) VALUES (1)", True),
        ("DROP TABLE users", True),
        ("EXECUTE IMMEDIATE 'DELETE FROM users WHERE TRUE'", True),
        ("CALL warehouse.cleanup()", True),
        ("GRANT `roles/bigquery.dataViewer` ON TABLE warehouse.t TO 'user:a@example.com'", True),
        ("REVOKE SELECT ON TABLE orders FROM ROLE analyst", True),
        ("EXPORT DATA OPTIONS (uri = 'gs://bucket/*.csv', format = 'CSV') AS SELECT * FROM t", True),
        ("LOAD DATA INTO warehouse.t FROM FILES (format = 'CSV', uris = ['gs://bucket/a.csv'])", True),
    ],
)
def test_is_mutating_sql_classifies_statements(sql: str, expected: bool) -> None:
    """is_mutating_sql flags DML, DDL, procedural, privilege, export, and load statements."""
    assert is_mutating_sql(sql) is expected


@pytest.mark.parametrize(
    "sql",
    ["SELECT 1; DELETE FROM orders", "SELECT 1; -- done", "SELECT 'a;b' AS v"],
    ids=["second-statement", "trailing-comment", "semicolon-in-literal"],
)
def test_is_mutating_sql_rejects_non_trailing_semicolons(sql: str) -> None:
    """Any semicolon before the trailing terminator is rejected, so no second statement can hide."""
    with pytest.raises(PermissionDeniedError, match="Multi-statement SQL is not allowed"):
        is_mutating_sql(sql)


@pytest.mark.anyio
@pytest.mark.parametrize(
    "table_name",
    ["warehouse.invoices_obj LIMIT 1; DROP TABLE x", "warehouse.`obj`", "1warehouse.obj"],
)
async def test_bigquery_object_table_inspect_rejects_invalid_table_name(
    gcp_database_tools: dict[str, Any],
    table_name: str,
) -> None:
    """bigquery_object_table_inspect rejects table names that are not plain qualified identifiers."""
    with pytest.raises(ValueError, match="table_name"):
        await gcp_database_tools["bigquery_object_table_inspect"].fn(table_name=table_name)


@pytest.mark.anyio
async def test_bigquery_object_table_inspect_accepts_project_qualified_name(
    gcp_database_tools: dict[str, Any],
) -> None:
    """Hyphenated project IDs remain valid table name prefixes."""
    result = await gcp_database_tools["bigquery_object_table_inspect"].fn(
        table_name="analytics-proj.warehouse.invoices_obj"
    )
    assert result["row_count"] == 1


@pytest.mark.anyio
async def test_spanner_query_rejects_multiple_statements(gcp_database_tools: dict[str, Any]) -> None:
    """spanner_query rejects a SELECT followed by a second statement."""
    with pytest.raises(PermissionDeniedError, match="Multi-statement SQL is not allowed"):
        await gcp_database_tools["spanner_query"].fn(sql="SELECT 1; DELETE FROM orders WHERE id = 1")


@pytest.mark.anyio
@pytest.mark.parametrize(
    "sql",
    ["GRANT SELECT ON TABLE orders TO ROLE analyst", "CALL cancel_query('12345')"],
)
async def test_spanner_query_requires_confirmation_for_privilege_and_procedure_calls(
    gcp_database_tools: dict[str, Any],
    sql: str,
) -> None:
    """GRANT and CALL statements are gated by the ConfirmationGate."""
    with pytest.raises(PermissionDeniedError, match="ConfirmationGate"):
        await gcp_database_tools["spanner_query"].fn(sql=sql)
```

Run RED:

```bash
uv run pytest src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py -q -k "is_mutating_sql or object_table_inspect or spanner_query"
```

Expected: the six procedural/privilege/export/load cases of `test_is_mutating_sql_classifies_statements` fail (`assert False is True`); all three `test_is_mutating_sql_rejects_non_trailing_semicolons` cases fail with `DID NOT RAISE`; the three invalid table names fail with `DID NOT RAISE`; `test_spanner_query_rejects_multiple_statements` and the two confirmation cases fail with a non-`PermissionDeniedError` error or `DID NOT RAISE`; `test_bigquery_object_table_inspect_accepts_project_qualified_name` passes.

### 2. Implement (GREEN)

In `src/py/vertebra/db/mcp_tools.py`:

1. Add `from vertebra.utils.text import validate_identifier` to the first-party imports.
2. Replace the verb sets and add the message constant:

   ```python
   _MUTATING_VERBS: frozenset[str] = frozenset(
       {
           "INSERT",
           "UPDATE",
           "DELETE",
           "MERGE",
           "CREATE",
           "ALTER",
           "DROP",
           "TRUNCATE",
           "EXECUTE",
           "CALL",
           "GRANT",
           "REVOKE",
           "EXPORT",
           "LOAD",
       }
   )
   _DDL_VERBS: frozenset[str] = frozenset({"CREATE", "ALTER", "DROP", "TRUNCATE", "GRANT", "REVOKE"})
   _MULTI_STATEMENT_MESSAGE: str = (
       "Multi-statement SQL is not allowed; submit one statement per call and pass values containing ';' "
       "as query parameters"
   )
   ```

3. Replace `is_mutating_sql` with:

   ```python
   def is_mutating_sql(sql: str) -> bool:
       """Return True when ``sql`` changes data, schema, or privileges, or runs procedural code.

       ``sql`` must hold exactly one statement. Any semicolon before the trailing terminator is
       rejected, including one inside a literal or comment, because GoogleSQL and Spangres quote
       literals differently and a dialect-specific parse could hide a second statement.

       Raises:
           PermissionDeniedError: When ``sql`` contains a semicolon before its trailing terminator.
       """
       body = (sql or "").rstrip().rstrip(";").rstrip()
       if ";" in body:
           raise PermissionDeniedError(_MULTI_STATEMENT_MESSAGE)
       return _extract_leading_verb(body) in _MUTATING_VERBS
   ```

   Keep `_extract_leading_verb` unchanged. `EXECUTE IMMEDIATE`, `EXPORT DATA`, and `LOAD DATA` classify by their first keyword.
4. Inner `bigquery_object_table_inspect`: make `validate_identifier(table_name, "table_name", allow_hyphens=True)` the first statement of the function body (before `_resolve_bq()`). Add an `Raises:` section to its docstring: `ValueError: When table_name is not a dotted identifier.`
5. Inner `spanner_query`: replace

   ```python
   verb = _extract_leading_verb(sql)
   if verb in _MUTATING_VERBS:
   ```

   with

   ```python
   if is_mutating_sql(sql):
       verb = _extract_leading_verb(sql)
   ```

   keeping the existing body (`_require_confirmation`, then `update_ddl` when `verb in _DDL_VERBS`, otherwise DML). The read-only snapshot path below is unchanged.
6. Changelog `docs/changelog/index.md`, `## 0.3.0` → `### Fixed`, append: `` - The governed GCP database MCP tools (`register_gcp_database_tools`) validate `bigquery_object_table_inspect` table names, reject SQL with more than one statement, and require confirmation for `EXECUTE`, `CALL`, `GRANT`, `REVOKE`, `EXPORT DATA`, and `LOAD DATA` (`is_mutating_sql`). ``

### 3. Refactor & verify

1. Run the GREEN commands; run `make lint`.
2. Commit:

   ```bash
   git add src/py/vertebra/db/mcp_tools.py src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py docs/changelog/index.md
   git commit -S -m "fix(db): validate MCP tool identifiers and classify multi-statement SQL"
   ```

## Verification

- **RED**: the Step 1 command exits `1` with the failures listed.
- **GREEN**:

  ```bash
  uv run pytest src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py -q
  uv run ruff check src/py/vertebra/db/mcp_tools.py src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py
  uv run ruff format --check src/py/vertebra/db/mcp_tools.py src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py
  uv run mypy src/py/vertebra/db/mcp_tools.py
  ```

  All exit `0`, including the existing `test_governed_mcp_and_a2a_gcp_database_tools`.
- **Phase gate**: Phase 1 checkpoint in `spec.md`.

## Acceptance Criteria

- [x] R7: Invalid `table_name` values raise `ValueError` before any SQL runs; hyphenated project-qualified names work.
- [x] R7: Non-trailing semicolons raise `PermissionDeniedError` in `is_mutating_sql` and `spanner_query`.
- [x] R7: `EXECUTE`, `CALL`, `GRANT`, `REVOKE`, `EXPORT`, `LOAD` are mutating; Spanner `GRANT`/`REVOKE` go through `update_ddl` after confirmation.
- [x] Changelog updated; targeted lint/type checks pass; one signed commit.

## Notes & Discoveries

- [2026-11-03 15:30] Planning: literal-aware masking was rejected because GoogleSQL backslash escapes and Spangres `E''`/dollar quoting disagree; rejecting all non-trailing semicolons fails closed.
