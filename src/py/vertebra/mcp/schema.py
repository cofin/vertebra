from typing import Any, get_type_hints, get_origin, get_args
import inspect
import dataclasses
from vertebra.mcp.registry import Tool
from vertebra.types.extractor import TypeExtractor
from vertebra.types.ir import (
    TypeNode,
    PrimitiveType,
    ArrayType,
    ObjectType,
    UnionType,
    LiteralType,
    TupleType,
    GenericType,
    AnnotatedType,
    UnknownType
)

class MCPSchemaGenerator:
    def __init__(self):
        self.extractor = TypeExtractor()

    @classmethod
    def generate_tool_schema(cls, tool: Tool) -> dict[str, Any]:
        generator = cls()
        return generator._generate(tool)

    def _generate(self, tool: Tool) -> dict[str, Any]:
        schema = {
            "name": tool.name,
            "description": tool.description or "",
            "inputSchema": self._generate_input_schema(tool.handler),
        }
        return schema

    def _generate_input_schema(self, handler: Any) -> dict[str, Any]:
        sig = inspect.signature(handler)
        type_hints = get_type_hints(handler)
        
        properties = {}
        required = []
        
        for name, param in sig.parameters.items():
            if name == "self" or name == "cls":
                continue
                
            hint = type_hints.get(name, Any)
            
            # Special handling for Dataclasses/Pydantic/Structs if TypeExtractor returns Unknown
            # We try TypeExtractor first
            type_node = self.extractor.from_hint(hint)
            
            if isinstance(type_node, UnknownType):
                # Try runtime introspection for dataclasses
                if dataclasses.is_dataclass(hint):
                    param_schema = self._dataclass_to_schema(hint)
                else:
                    param_schema = {}
            else:
                param_schema = self._node_to_schema(type_node)
            
            # Handle default values
            if param.default is not inspect.Parameter.empty:
                param_schema["default"] = param.default
            else:
                required.append(name)
                
            # Removed param.description check as inspect.Parameter doesn't have it
                
            properties[name] = param_schema

        return {
            "type": "object",
            "properties": properties,
            "required": required,
        }

    def _dataclass_to_schema(self, dc_cls: Any) -> dict[str, Any]:
        props = {}
        required = []
        type_hints = get_type_hints(dc_cls)
        
        for field in dataclasses.fields(dc_cls):
            name = field.name
            hint = type_hints.get(name, Any)
            type_node = self.extractor.from_hint(hint)
            
            if isinstance(type_node, UnknownType) and dataclasses.is_dataclass(hint):
                 # Simple recursion
                 props[name] = self._dataclass_to_schema(hint)
            else:
                 props[name] = self._node_to_schema(type_node)
            
            if field.default == dataclasses.MISSING and field.default_factory == dataclasses.MISSING:
                # Check if type is Optional (Union[T, None])
                is_optional = False
                if isinstance(type_node, UnionType):
                    for v in type_node.variants:
                        if isinstance(v, PrimitiveType) and v.kind == "null":
                            is_optional = True
                
                if not is_optional:
                    required.append(name)
        
        return {
            "type": "object",
            "properties": props,
            "required": required
        }

    def _node_to_schema(self, node: TypeNode) -> dict[str, Any]: # noqa: C901
        if isinstance(node, PrimitiveType):
            if node.kind == "number":
                return {"type": "number"}
            if node.kind == "string":
                return {"type": "string"}
            if node.kind == "boolean":
                return {"type": "boolean"}
            if node.kind == "null":
                return {"type": "null"}
            if node.kind == "Uint8Array":
                return {"type": "string", "contentEncoding": "base64"}
            return {}

        if isinstance(node, ArrayType):
            return {
                "type": "array",
                "items": self._node_to_schema(node.item)
            }

        if isinstance(node, ObjectType):
            props = {}
            req = []
            for name, field_type in node.fields.items():
                props[name] = self._node_to_schema(field_type)
                if name not in node.optional_fields:
                    req.append(name)
            return {
                "type": "object",
                "properties": props,
                "required": req
            }

        if isinstance(node, UnionType):
            # Special case for Optional (Union[T, null])
            # Check if it's effectively an enum
            literals = [v for v in node.variants if isinstance(v, LiteralType)]
            if len(literals) == len(node.variants):
                return {"enum": [l.value for l in literals]}
                
            return {
                "anyOf": [self._node_to_schema(v) for v in node.variants]
            }

        if isinstance(node, LiteralType):
            return {"const": node.value}

        if isinstance(node, GenericType):
            if node.name == "Record":
                return {
                    "type": "object",
                    "additionalProperties": self._node_to_schema(node.params[1])
                }
            if node.name == "Set":
                return {
                    "type": "array",
                    "items": self._node_to_schema(node.params[0]),
                    "uniqueItems": True
                }
            return {}

        if isinstance(node, TupleType):
            return {
                "type": "array",
                "prefixItems": [self._node_to_schema(i) for i in node.items],
                "minItems": len(node.items),
                "maxItems": len(node.items)
            }
            
        if isinstance(node, AnnotatedType):
            return self._node_to_schema(node.base)

        if isinstance(node, UnknownType):
            return {}

        return {}