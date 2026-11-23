"""Valibot v1 tree-shakeable TypeScript validation schema generator."""

from collections.abc import Sequence

from vertebra.build.codegen._emitter import _BaseValidatorEmitter
from vertebra.build.codegen._ir import ObjectType
from vertebra.build.codegen._json_schema import resolve_parameter_node
from vertebra.utils.serializers import encode_json

__all__ = ("ValibotGenerator",)

_VALIBOT_CONSTRAINTS: dict[str, str] = {
    "ge": "minValue",
    "gt": "minValue",
    "le": "maxValue",
    "lt": "maxValue",
    "multiple_of": "multipleOf",
    "min_length": "minLength",
    "max_length": "maxLength",
}


class ValibotGenerator(_BaseValidatorEmitter):
    """Generate Valibot v1 schemas from RouteMetadata, TypeNode IR, or JSON Schema dictionaries."""

    __slots__ = ()

    _import_line = "import * as v from 'valibot';"
    _prefix = "v"
    _infer_type = "v.InferOutput"
    _uint8_expr = "v.instance(Uint8Array)"

    def _emit_object_decl(self, name: str, obj: ObjectType) -> list[str]:
        fields: list[str] = []
        for field_name, field_type in obj.fields.items():
            res = resolve_parameter_node(field_name, field_type)
            expr = self._node_to_expr(res.base_type, res.constraints)
            if field_name in obj.optional_fields:
                expr = f"v.optional({expr})"
            fields.append(f"  {field_name}: {expr},")
        inner = "\n".join(fields)
        return [f"export const {name}Schema = v.object({{\n{inner}\n}});"]

    def _wrap_optional(self, expr: str) -> str:
        return f"v.optional({expr})"

    def _render_record(self, value_expr: str) -> str:
        return f"v.record(v.string(), {value_expr})"

    def _apply_constraints(self, base: str, constraints: Sequence[tuple[str, object]]) -> str:
        if not constraints:
            return base
        pipes: list[str] = [base]
        for key, value in constraints:
            fn = _VALIBOT_CONSTRAINTS.get(key)
            if fn is not None:
                pipes.append(f"v.{fn}({encode_json(value)})")
            elif key == "pattern":
                escaped = str(value).replace("\\", "\\\\").replace("/", "\\/")
                pipes.append(f"v.regex(/{escaped}/)")
        return f"v.pipe({', '.join(pipes)})"
