"""Pipelined and CTE-batched single-RTT PostgreSQL query compiler with RelOverlay read-your-own-writes."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any

import msgspec

from vertebra.utils.serializers import decode_json

if TYPE_CHECKING:
    from collections.abc import Sequence

    from vertebra.db.postgres import AsyncPostgresDriver, SyncPostgresDriver

__all__ = (
    "CteStep",
    "QueryPipeline",
    "RelOverlay",
)

_IDENTIFIER_PATTERN: re.Pattern[str] = re.compile(r"^[A-Za-z_][A-Za-z0-9_.]*$")


def _validate_sql_identifier(identifier: str, field_name: str = "identifier") -> str:
    """Validate that identifier matches safe SQL naming syntax."""
    if not _IDENTIFIER_PATTERN.match(identifier):
        msg = f"Invalid SQL identifier for {field_name}: {identifier!r}"
        raise ValueError(msg)
    return identifier


class RelOverlay(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Intra-statement read-your-own-writes CTE overlay (`UNION ALL` / `EXCEPT` over base table)."""

    base_table: str
    inserted_cte: str | None = None
    deleted_ids_cte: str | None = None
    id_column: str = "id"

    def __post_init__(self) -> None:
        """Validate all constituent SQL identifiers."""
        _validate_sql_identifier(self.base_table, "base_table")
        if self.inserted_cte is not None:
            _validate_sql_identifier(self.inserted_cte, "inserted_cte")
        if self.deleted_ids_cte is not None:
            _validate_sql_identifier(self.deleted_ids_cte, "deleted_ids_cte")
        _validate_sql_identifier(self.id_column, "id_column")

    def to_cte_sql(self, overlay_name: str) -> str:
        """Render the overlay CTE combining uncommitted CTE mutations with the base table."""
        _validate_sql_identifier(overlay_name, "overlay_name")
        base_select = f"SELECT * FROM {self.base_table}"
        if self.deleted_ids_cte:
            base_select = (
                f"{base_select} WHERE {self.id_column} NOT IN (SELECT {self.id_column} FROM {self.deleted_ids_cte})"
            )
        if self.inserted_cte:
            return f"{overlay_name} AS ({base_select} UNION ALL SELECT * FROM {self.inserted_cte})"
        return f"{overlay_name} AS ({base_select})"


class CteStep(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Single named CTE step within a single-RTT ``QueryPipeline``."""

    name: str
    sql: str
    params: dict[str, Any] = msgspec.field(default_factory=dict)
    return_json_agg: bool = True

    def __post_init__(self) -> None:
        """Validate step name identifier."""
        _validate_sql_identifier(self.name, "name")


class QueryPipeline:
    """Composes multiple read and write steps into a single-RTT PostgreSQL CTE statement."""

    __slots__ = ("_overlays", "_steps")

    def __init__(self) -> None:
        self._steps: list[CteStep] = []
        self._overlays: list[tuple[str, RelOverlay]] = []

    @property
    def steps(self) -> tuple[CteStep, ...]:
        """Return registered CTE steps."""
        return tuple(self._steps)

    @staticmethod
    def build_jsonb_object(
        columns: Sequence[tuple[str, str] | str],
        *,
        max_pairs_per_call: int = 50,
    ) -> str:
        """Build a PostgreSQL ``jsonb_build_object(...)`` expression respecting the 100-argument function limit."""
        if not columns:
            return "'{}'::jsonb"
        if max_pairs_per_call < 1 or max_pairs_per_call > 50:
            msg = f"max_pairs_per_call must be between 1 and 50 (got {max_pairs_per_call})"
            raise ValueError(msg)
        normalized: list[tuple[str, str]] = [item if isinstance(item, tuple) else (item, item) for item in columns]
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
        self._steps.append(CteStep(name=name, sql=sql, params=dict(params or {}), return_json_agg=return_json_agg))
        return self

    def add_overlay(self, overlay_name: str, overlay: RelOverlay) -> QueryPipeline:
        """Register an intra-statement ``RelOverlay`` CTE."""
        _validate_sql_identifier(overlay_name, "overlay_name")
        self._overlays.append((overlay_name, overlay))
        return self

    def compile(self) -> tuple[str, dict[str, Any]]:
        """Compile all steps and overlays into a single CTE SQL statement and merged parameter dict."""
        if not self._steps:
            msg = "QueryPipeline requires at least one step"
            raise ValueError(msg)
        cte_parts: list[str] = []
        merged_params: dict[str, Any] = {}
        for step in self._steps:
            cte_parts.append(f"{step.name} AS ({step.sql})")
            for k, v in step.params.items():
                if k in merged_params and merged_params[k] != v:
                    msg = f"Conflicting query pipeline parameter: {k!r}"
                    raise ValueError(msg)
                merged_params[k] = v
        for ov_name, overlay in self._overlays:
            cte_parts.append(overlay.to_cte_sql(ov_name))

        result_pairs: list[tuple[str, str]] = [
            (
                step.name,
                f"COALESCE((SELECT jsonb_agg(row_to_json({step.name})) FROM {step.name}), '[]'::jsonb)",
            )
            for step in self._steps
            if step.return_json_agg
        ]
        envelope_expr = self.build_jsonb_object(result_pairs, max_pairs_per_call=25)
        full_sql = f"WITH {', '.join(cte_parts)} SELECT {envelope_expr} AS payload"
        return full_sql, merged_params

    def execute(self, driver: SyncPostgresDriver) -> dict[str, Any]:
        """Execute compiled CTE query synchronously and unpack the result jsonb payload."""
        sql, params = self.compile()
        rows, _ = driver.execute_with_description(sql, params)
        if not rows:
            return {}
        first = rows[0]
        payload = first.get("payload") if isinstance(first, dict) else (first[0] if first else None)
        if isinstance(payload, bytearray):
            payload = bytes(payload)
        if isinstance(payload, (str, bytes)):
            return dict(decode_json(payload))
        return dict(payload or {})

    async def execute_async(self, driver: AsyncPostgresDriver) -> dict[str, Any]:
        """Execute compiled CTE query asynchronously and unpack the result jsonb payload."""
        sql, params = self.compile()
        rows, _ = await driver.execute_with_description(sql, params)
        if not rows:
            return {}
        first = rows[0]
        payload = first.get("payload") if isinstance(first, dict) else (first[0] if first else None)
        if isinstance(payload, bytearray):
            payload = bytes(payload)
        if isinstance(payload, (str, bytes)):
            return dict(decode_json(payload))
        return dict(payload or {})
