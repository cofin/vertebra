"""Pipelined and CTE-batched single-RTT PostgreSQL and GoogleSQL query compiler with RelOverlay."""

from __future__ import annotations

import inspect
from typing import TYPE_CHECKING, Any, Literal

import msgspec

from vertebra.utils.serializers import decode_json
from vertebra.utils.text import validate_identifier

if TYPE_CHECKING:
    from collections.abc import Sequence

__all__ = (
    "CTEStep",
    "PipelineDialect",
    "QueryPipeline",
    "RelOverlay",
)

type PipelineDialect = Literal["postgres", "googlesql"]

_VALID_PIPELINE_DIALECTS: frozenset[str] = frozenset({"postgres", "googlesql"})

class RelOverlay(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Intra-statement read-your-own-writes CTE overlay (`UNION ALL` / `EXCEPT` over base table)."""

    base_table: str
    """Physical base table name that the CTE overlay shadows."""
    inserted_cte: str | None = None
    """Optional CTE name producing newly inserted or updated rows (`UNION ALL`)."""
    deleted_ids_cte: str | None = None
    """Optional CTE name producing primary keys excluded from the base table (`NOT IN`)."""
    id_column: str = "id"
    """Primary key column name used to filter deleted rows."""

    def __post_init__(self) -> None:
        """Validate all constituent SQL identifiers."""
        validate_identifier(self.base_table, "base_table")
        if self.inserted_cte is not None:
            validate_identifier(self.inserted_cte, "inserted_cte")
        if self.deleted_ids_cte is not None:
            validate_identifier(self.deleted_ids_cte, "deleted_ids_cte")
        validate_identifier(self.id_column, "id_column")

    def to_cte_sql(self, overlay_name: str) -> str:
        """Render the overlay CTE combining uncommitted CTE mutations with the base table."""
        validate_identifier(overlay_name, "overlay_name")
        base_select = f"SELECT * FROM {self.base_table}"
        if self.deleted_ids_cte:
            base_select = (
                f"{base_select} WHERE {self.id_column} NOT IN (SELECT {self.id_column} FROM {self.deleted_ids_cte})"
            )
        if self.inserted_cte:
            return f"{overlay_name} AS ({base_select} UNION ALL SELECT * FROM {self.inserted_cte})"
        return f"{overlay_name} AS ({base_select})"


class CTEStep(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Single named CTE step within a single-RTT ``QueryPipeline``."""

    name: str
    """SQL identifier for the common table expression (`WITH <name> AS (...)`)."""
    sql: str
    """SQL query or `RETURNING` mutation body wrapped inside the CTE."""
    params: dict[str, Any] = msgspec.field(default_factory=dict)
    """Step-local named parameters merged into the compiled pipeline bind list."""
    return_json_agg: bool = True
    """When True, include JSON array aggregation for this step in the outer `SELECT`."""

    def __post_init__(self) -> None:
        """Validate step name identifier."""
        validate_identifier(self.name, "name")


class QueryPipeline:
    """Composes multiple read and write steps into a single-RTT PostgreSQL or GoogleSQL CTE statement."""

    __slots__ = ("_dialect", "_overlays", "_steps")

    def __init__(self, *, dialect: PipelineDialect = "postgres") -> None:
        self._dialect: PipelineDialect = self._validate_pipeline_dialect(dialect)
        self._steps: list[CTEStep] = []
        self._overlays: list[tuple[str, RelOverlay]] = []

    @property
    def dialect(self) -> PipelineDialect:
        """Return the configured SQL dialect (`"postgres"` or `"googlesql"`)."""
        return self._dialect

    @property
    def steps(self) -> tuple[CTEStep, ...]:
        """Return registered CTE steps."""
        return tuple(self._steps)

    @staticmethod
    def build_jsonb_object(
        columns: Sequence[tuple[str, str] | str],
        *,
        max_pairs_per_call: int = 50,
        dialect: PipelineDialect = "postgres",
    ) -> str:
        """Build a PostgreSQL ``jsonb_build_object(...)`` or GoogleSQL ``TO_JSON(STRUCT(...))`` expression."""
        resolved_dialect = QueryPipeline._validate_pipeline_dialect(dialect)
        normalized: list[tuple[str, str]] = [item if isinstance(item, tuple) else (item, item) for item in columns]
        if resolved_dialect == "googlesql":
            if not normalized:
                return "JSON '{}'"
            fields = ", ".join(f"{expr} AS {validate_identifier(key, 'key')}" for key, expr in normalized)
            return f"TO_JSON(STRUCT({fields}))"

        if not normalized:
            return "'{}'::jsonb"
        if max_pairs_per_call < 1 or max_pairs_per_call > 50:
            msg = f"max_pairs_per_call must be between 1 and 50 (got {max_pairs_per_call})"
            raise ValueError(msg)
        chunks: list[str] = []
        for offset in range(0, len(normalized), max_pairs_per_call):
            batch = normalized[offset : offset + max_pairs_per_call]
            args = ", ".join(f"'{key}', {expr}" for key, expr in batch)
            chunks.append(f"jsonb_build_object({args})")
        return " || ".join(chunks)

    def add_step(
        self,
        name: str,
        sql: str,
        *,
        params: dict[str, Any] | None = None,
        return_json_agg: bool = True,
    ) -> QueryPipeline:
        """Append a named CTE step to the pipeline."""
        self._steps.append(CTEStep(name=name, sql=sql, params=dict(params or {}), return_json_agg=return_json_agg))
        return self

    def add_overlay(self, overlay_name: str, overlay: RelOverlay) -> QueryPipeline:
        """Register an intra-statement ``RelOverlay`` CTE."""
        validate_identifier(overlay_name, "overlay_name")
        self._overlays.append((overlay_name, overlay))
        return self

    def compile(self, *, dialect: PipelineDialect | None = None) -> tuple[str, dict[str, Any]]:
        """Compile all steps and overlays into a single CTE SQL statement and merged parameter dict."""
        if not self._steps:
            msg = "QueryPipeline requires at least one step"
            raise ValueError(msg)
        resolved_dialect = self._validate_pipeline_dialect(dialect) if dialect is not None else self._dialect
        cte_parts: list[str] = []
        merged_params: dict[str, Any] = {}
        for step in self._steps:
            step_sql = self._rewrite_googlesql_params(step.sql) if resolved_dialect == "googlesql" else step.sql
            cte_parts.append(f"{step.name} AS ({step_sql})")
            for k, v in step.params.items():
                clean_k = k.lstrip("@:")
                if clean_k in merged_params and merged_params[clean_k] != v:
                    msg = f"Conflicting query pipeline parameter: {clean_k!r}"
                    raise ValueError(msg)
                merged_params[clean_k] = v
        for ov_name, overlay in self._overlays:
            cte_parts.append(overlay.to_cte_sql(ov_name))

        if resolved_dialect == "googlesql":
            struct_fields = [
                f"COALESCE((SELECT ARRAY_AGG(TO_JSON(_s)) FROM {step.name} AS _s), ARRAY<JSON>[]) AS {step.name}"
                for step in self._steps
                if step.return_json_agg
            ]
            envelope_expr = (
                f"TO_JSON_STRING(TO_JSON(STRUCT({', '.join(struct_fields)})))" if struct_fields else "'{}'"
            )
            full_sql = f"WITH {', '.join(cte_parts)} SELECT {envelope_expr} AS payload"
            return full_sql, merged_params

        result_pairs: list[tuple[str, str]] = [
            (
                step.name,
                f"COALESCE((SELECT jsonb_agg(row_to_json({step.name})) FROM {step.name}), '[]'::jsonb)",
            )
            for step in self._steps
            if step.return_json_agg
        ]
        envelope_expr = self.build_jsonb_object(result_pairs, max_pairs_per_call=25, dialect="postgres")
        full_sql = f"WITH {', '.join(cte_parts)} SELECT {envelope_expr} AS payload"
        return full_sql, merged_params

    @staticmethod
    def _validate_pipeline_dialect(dialect: str) -> PipelineDialect:
        """Validate that the requested pipeline dialect is supported."""
        if dialect not in _VALID_PIPELINE_DIALECTS:
            msg = f"Unsupported QueryPipeline dialect: {dialect!r}"
            raise ValueError(msg)
        return "googlesql" if dialect == "googlesql" else "postgres"

    @staticmethod
    def _rewrite_googlesql_params(sql: str) -> str:
        """Rewrite `:param` bind placeholders to `@param` outside string literals and `::` casts."""
        out: list[str] = []
        i = 0
        n = len(sql)
        in_single = False
        in_double = False
        in_backtick = False

        while i < n:
            ch = sql[i]
            if in_single:
                out.append(ch)
                if ch == "\\" and i + 1 < n:
                    out.append(sql[i + 1])
                    i += 2
                    continue
                if ch == "'":
                    if i + 1 < n and sql[i + 1] == "'":
                        out.append("'")
                        i += 2
                        continue
                    in_single = False
                i += 1
                continue
            if in_double:
                out.append(ch)
                if ch == "\\" and i + 1 < n:
                    out.append(sql[i + 1])
                    i += 2
                    continue
                if ch == '"':
                    in_double = False
                i += 1
                continue
            if in_backtick:
                out.append(ch)
                if ch == "`":
                    in_backtick = False
                i += 1
                continue

            if ch == "'":
                in_single = True
                out.append(ch)
                i += 1
                continue
            if ch == '"':
                in_double = True
                out.append(ch)
                i += 1
                continue
            if ch == "`":
                in_backtick = True
                out.append(ch)
                i += 1
                continue
            if ch == ":" and i + 1 < n and sql[i + 1] == ":":
                out.append("::")
                i += 2
                continue
            prev_ch = sql[i - 1] if i > 0 else ""
            if (
                ch == ":"
                and i + 1 < n
                and (sql[i + 1].isalpha() or sql[i + 1] == "_")
                and not (prev_ch.isalnum() or prev_ch in {"_", ":", "@"})
            ):
                j = i + 1
                while j < n and (sql[j].isalnum() or sql[j] == "_"):
                    j += 1
                out.append(f"@{sql[i + 1 : j]}")
                i = j
                continue
            out.append(ch)
            i += 1

        return "".join(out)

    @staticmethod
    def _unpack_payload_value(payload: Any) -> dict[str, Any]:
        """Unpack a raw driver scalar or row value into a decoded dictionary."""
        if payload is None:
            return {}
        if isinstance(payload, (bytes, bytearray, memoryview)):
            raw = bytes(payload)
            if raw.startswith(b"\x01"):
                raw = raw[1:]
            return dict(decode_json(raw))
        if isinstance(payload, str):
            return dict(decode_json(payload))
        if isinstance(payload, dict):
            return dict(payload)
        return dict(payload)

    def execute(self, driver: Any) -> dict[str, Any]:
        """Execute compiled CTE query synchronously and unpack the result JSON payload."""
        sql, params = self.compile()
        if hasattr(driver, "execute_with_description") and not inspect.iscoroutinefunction(
            driver.execute_with_description
        ):
            res = driver.execute_with_description(sql, params)
            rows = res[0] if isinstance(res, tuple) else res
            if not rows:
                return {}
            first = rows[0]
            payload = first.get("payload") if isinstance(first, dict) else (first[0] if first else None)
            return self._unpack_payload_value(payload)
        if hasattr(driver, "fetchval") and not inspect.iscoroutinefunction(driver.fetchval):
            return self._unpack_payload_value(driver.fetchval(sql, params))
        if hasattr(driver, "select") and not inspect.iscoroutinefunction(driver.select):
            rows = driver.select(sql, params)
            if not rows:
                return {}
            first = rows[0]
            payload = first.get("payload") if isinstance(first, dict) else (first[0] if first else None)
            return self._unpack_payload_value(payload)
        msg = f"Unsupported synchronous driver for QueryPipeline.execute: {type(driver)!r}"
        raise TypeError(msg)

    async def execute_async(self, driver: Any) -> dict[str, Any]:
        """Execute compiled CTE query asynchronously and unpack the result JSON payload."""
        sql, params = self.compile()
        if hasattr(driver, "execute_with_description"):
            maybe_coro = driver.execute_with_description(sql, params)
            res = await maybe_coro if inspect.isawaitable(maybe_coro) else maybe_coro
            rows = res[0] if isinstance(res, tuple) else res
            if not rows:
                return {}
            first = rows[0]
            payload = first.get("payload") if isinstance(first, dict) else (first[0] if first else None)
            return self._unpack_payload_value(payload)
        if hasattr(driver, "fetchval"):
            maybe_val = driver.fetchval(sql, params)
            val = await maybe_val if inspect.isawaitable(maybe_val) else maybe_val
            return self._unpack_payload_value(val)
        if hasattr(driver, "select"):
            maybe_rows = driver.select(sql, params)
            rows = await maybe_rows if inspect.isawaitable(maybe_rows) else maybe_rows
            if not rows:
                return {}
            first = rows[0]
            payload = first.get("payload") if isinstance(first, dict) else (first[0] if first else None)
            return self._unpack_payload_value(payload)
        msg = f"Unsupported asynchronous driver for QueryPipeline.execute_async: {type(driver)!r}"
        raise TypeError(msg)

