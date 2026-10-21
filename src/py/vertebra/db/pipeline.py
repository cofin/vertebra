"""Pipelined and CTE-batched single-RTT PostgreSQL query compiler with RelOverlay read-your-own-writes."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Sequence

__all__ = (
    "CteStep",
    "QueryPipeline",
    "RelOverlay",
    "chunked_jsonb_build_object",
)


def chunked_jsonb_build_object(
    pairs: Sequence[tuple[str, str]],
    *,
    max_pairs_per_call: int = 25,
) -> str:
    """Build a PostgreSQL ``jsonb_build_object(...)`` expression respecting the 100-argument function limit.

    PostgreSQL caps variadic C functions at 100 arguments (50 key-value pairs). Chunking at
    ``max_pairs_per_call`` (default 25 pairs = 50 arguments) and concatenating with ``||``
    prevents ``cannot pass more than 100 arguments to a function`` errors on wide tables.
    """
    if not pairs:
        return "'{}'::jsonb"
    if max_pairs_per_call < 1 or max_pairs_per_call > 50:
        msg = f"max_pairs_per_call must be between 1 and 50 (got {max_pairs_per_call})"
        raise ValueError(msg)
    chunks: list[str] = []
    for offset in range(0, len(pairs), max_pairs_per_call):
        batch = pairs[offset : offset + max_pairs_per_call]
        args = ", ".join(f"'{key}', {expr}" for key, expr in batch)
        chunks.append(f"jsonb_build_object({args})")
    return " || ".join(chunks)


@dataclass(frozen=True, slots=True)
class RelOverlay:
    """Intra-statement read-your-own-writes CTE overlay (`UNION ALL` / `EXCEPT` over base table)."""

    base_table: str
    inserted_cte: str | None = None
    deleted_ids_cte: str | None = None
    id_column: str = "id"

    def to_cte_sql(self, overlay_name: str) -> str:
        """Render the overlay CTE combining uncommitted CTE mutations with the base table."""
        base_select = f"SELECT * FROM {self.base_table}"
        if self.deleted_ids_cte:
            base_select = (
                f"{base_select} WHERE {self.id_column} NOT IN (SELECT {self.id_column} FROM {self.deleted_ids_cte})"
            )
        if self.inserted_cte:
            return f"{overlay_name} AS ({base_select} UNION ALL SELECT * FROM {self.inserted_cte})"
        return f"{overlay_name} AS ({base_select})"


@dataclass(frozen=True, slots=True)
class CteStep:
    """Single named CTE step within a single-RTT ``QueryPipeline``."""

    name: str
    sql: str
    params: dict[str, Any] = field(default_factory=dict)
    return_json_agg: bool = True


class QueryPipeline:
    """Composes multiple read and write steps into a single-RTT PostgreSQL CTE statement."""

    def __init__(self) -> None:
        self._steps: list[CteStep] = []
        self._overlays: list[tuple[str, RelOverlay]] = []

    @property
    def steps(self) -> tuple[CteStep, ...]:
        """Return registered CTE steps."""
        return tuple(self._steps)

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
            merged_params.update(step.params)
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
        envelope_expr = chunked_jsonb_build_object(result_pairs)
        full_sql = f"WITH {', '.join(cte_parts)} SELECT {envelope_expr} AS payload"
        return full_sql, merged_params
