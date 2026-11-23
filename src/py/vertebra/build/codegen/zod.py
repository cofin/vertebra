"""Zod TypeScript validation schema generator with Litestar v2/v3 marker and JSON Schema support."""

from collections.abc import Sequence

from vertebra.build.codegen._emitter import _BaseValidatorEmitter
from vertebra.utils.serializers import encode_json

__all__ = ("ZodGenerator",)

_ZOD_CONSTRAINTS: dict[str, str] = {
    "ge": "gte",
    "gt": "gt",
    "le": "lte",
    "lt": "lt",
    "multiple_of": "multipleOf",
    "min_length": "min",
    "max_length": "max",
}


class ZodGenerator(_BaseValidatorEmitter):
    """Generate Zod schemas from RouteMetadata, TypeNode IR, or JSON Schema dictionaries."""

    __slots__ = ()

    _import_line = 'import { z } from "zod";'
    _prefix = "z"
    _infer_type = "z.infer"
    _uint8_expr = "z.instanceof(Uint8Array)"

    def _wrap_optional(self, expr: str) -> str:
        return f"{expr}.optional()"

    def _render_record(self, value_expr: str) -> str:
        return f"z.record({value_expr})"

    def _apply_constraints(self, base: str, constraints: Sequence[tuple[str, object]]) -> str:
        result = base
        for key, value in constraints:
            method = _ZOD_CONSTRAINTS.get(key)
            if method is not None:
                result += f".{method}({encode_json(value)})"
            elif key == "pattern":
                escaped = str(value).replace("\\", "\\\\").replace("/", "\\/")
                result += f".regex(/{escaped}/)"
        return result
