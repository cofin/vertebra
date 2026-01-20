"""Type extraction utilities for Python annotations."""

from __future__ import annotations

import ast
import hashlib
import types
import typing
import warnings
from typing import Any, cast, get_args, get_origin

import msgspec

from vertebra.types.ir import (
    AnnotatedType,
    ArrayType,
    GenericType,
    LiteralType,
    NamedTypeRef,
    ObjectType,
    PrimitiveType,
    RawType,
    TupleType,
    TypeNode,
    UnionType,
    UnknownType,
)

if typing.TYPE_CHECKING:
    from pathlib import Path


class CachedAst(msgspec.Struct, frozen=True):
    """Cached parsed AST with content digest for invalidation."""

    digest: str
    tree: ast.AST


class CachedClasses(msgspec.Struct, frozen=True):
    """Cached class definitions extracted from AST."""

    digest: str
    classes: dict[str, ast.ClassDef]


class CachedImports(msgspec.Struct, frozen=True):
    """Cached import mappings extracted from AST."""

    digest: str
    imports: dict[str, tuple[str, int]]


class CachedTypeVars(msgspec.Struct, frozen=True):
    """Cached TypeVar definitions with their constraints."""

    digest: str
    type_vars: dict[str, TypeNode]


class ResolveContext(msgspec.Struct, frozen=True):
    """Context for resolving forward references during type extraction."""

    path: "Path"
    imports: dict[str, tuple[str, int]]
    local_classes: set[str]
    search_paths: "tuple[Path, ...]"


class TypeExtractor(msgspec.Struct):
    """Extracts type information from Python source files with caching.

    Note: Not frozen because the caches are updated during extraction.
    """

    ast_cache: "dict[Path, CachedAst]" = msgspec.field(default_factory=dict)
    class_cache: "dict[Path, CachedClasses]" = msgspec.field(default_factory=dict)
    import_cache: "dict[Path, CachedImports]" = msgspec.field(default_factory=dict)
    type_var_cache: "dict[Path, CachedTypeVars]" = msgspec.field(default_factory=dict)
    type_cache: "dict[tuple[Path, str], ObjectType]" = msgspec.field(default_factory=dict)

    def from_hint(self, annotation: Any) -> TypeNode:  # noqa: PLR0911, PLR0912
        # Use identity checks or try/except to handle unhashable types (like msgspec IntType)
        if annotation is None or annotation is type(None):
            return PrimitiveType("null")
        if annotation is int or annotation is float:
            return PrimitiveType("number")
        if annotation is str:
            return PrimitiveType("string")
        if annotation is bool:
            return PrimitiveType("boolean")
        if annotation is bytes or annotation is bytearray or annotation is memoryview:
            return PrimitiveType("Uint8Array")
        if annotation is object or annotation is Any:
            return UnknownType()
        if isinstance(annotation, typing.TypeVar):
            return RawType(annotation.__name__)

        origin = get_origin(annotation)
        args = get_args(annotation)

        if origin is typing.Annotated:
            if not args:
                return UnknownType()
            base = self.from_hint(args[0])
            metadata = tuple(args[1:])
            return AnnotatedType(base=base, metadata=metadata)

        if origin is typing.Union or origin is types.UnionType or isinstance(annotation, types.UnionType):
            return self._from_union(args)

        if origin is list or origin is tuple or origin is dict or origin is set:
            return self._from_collection(origin, args)

        if origin is typing.Literal:
            return self._from_literal(args)
        literal_type = getattr(types, "LiteralType", None)
        if literal_type is not None and origin is literal_type:
            return self._from_literal(args)

        if origin is None and args:
            return self._from_union(args)

        return UnknownType()

    def _from_collection(self, origin: Any, args: tuple[Any, ...]) -> TypeNode:
        if origin is list:
            return ArrayType(self.from_hint(args[0] if args else Any))
        if origin is set:
            return GenericType("Set", [self.from_hint(args[0] if args else Any)])
        if origin is dict:
            key = self.from_hint(args[0] if args else Any)
            value = self.from_hint(args[1] if len(args) > 1 else Any)
            return GenericType("Record", [key, value])
        if origin is tuple:
            return self._from_tuple(args)
        return UnknownType()

    def _from_tuple(self, args: tuple[Any, ...]) -> TypeNode:
        if not args:
            return TupleType(())
        if len(args) == 2 and args[1] is Ellipsis:
            return ArrayType(self.from_hint(args[0]))
        return TupleType(tuple(self.from_hint(arg) for arg in args))

    def _from_union(self, args: tuple[Any, ...]) -> TypeNode:
        if not args:
            return UnknownType()
        variants = [self.from_hint(arg) for arg in args]
        return UnionType(variants)

    def _from_literal(self, args: tuple[Any, ...]) -> TypeNode:
        if not args:
            return UnknownType()
        if len(args) == 1:
            return LiteralType(args[0])
        return UnionType([LiteralType(value) for value in args])

    def from_source_file(self, path: Path) -> ast.AST:
        content = path.read_bytes()
        digest = hashlib.sha256(content).hexdigest()
        cached = self.ast_cache.get(path)
        if cached and cached.digest == digest:
            return cached.tree
        tree = ast.parse(content, filename=str(path))
        self.ast_cache[path] = CachedAst(digest=digest, tree=tree)
        return tree

    def collect_class_defs(self, tree: ast.AST) -> dict[str, ast.ClassDef]:
        classes: dict[str, ast.ClassDef] = {}
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef):
                classes[node.name] = node
        return classes

    def get_class_defs(self, path: Path) -> dict[str, ast.ClassDef]:
        tree = self.from_source_file(path)
        digest = self.ast_cache[path].digest
        cached = self.class_cache.get(path)
        if cached and cached.digest == digest:
            return cached.classes
        classes = self.collect_class_defs(tree)
        self.class_cache[path] = CachedClasses(digest=digest, classes=classes)
        return classes

    def collect_imports(self, tree: ast.AST) -> dict[str, tuple[str, int]]:
        imports: dict[str, tuple[str, int]] = {}
        body = getattr(tree, "body", [])
        for node in body:
            if isinstance(node, ast.Import):
                for alias in node.names:
                    name = alias.asname or alias.name
                    imports[name] = (alias.name, 0)
            elif isinstance(node, ast.ImportFrom):
                for alias in node.names:
                    name = alias.asname or alias.name
                    module_name = node.module or alias.name
                    imports[name] = (module_name, node.level or 0)
        return imports

    def get_imports(self, path: Path) -> dict[str, tuple[str, int]]:
        tree = self.from_source_file(path)
        digest = self.ast_cache[path].digest
        cached = self.import_cache.get(path)
        if cached and cached.digest == digest:
            return cached.imports
        imports = self.collect_imports(tree)
        self.import_cache[path] = CachedImports(digest=digest, imports=imports)
        return imports

    def collect_type_vars(  # noqa: PLR0912
        self, tree: ast.AST, class_names: set[str], context: ResolveContext
    ) -> dict[str, TypeNode]:
        typevar_calls: list[tuple[str, ast.Call]] = []
        typevar_names: set[str] = set()

        body = getattr(tree, "body", [])
        for node in body:
            call: ast.Call | None = None
            targets: list[ast.expr] = []
            if isinstance(node, ast.Assign) and isinstance(node.value, ast.Call):
                call = node.value
                targets = list(node.targets)
            elif isinstance(node, ast.AnnAssign) and isinstance(node.value, ast.Call):
                call = node.value
                targets = [node.target]

            if call is None or self._base_name(call.func).split(".")[-1] != "TypeVar":
                continue

            for target in targets:
                if isinstance(target, ast.Name):
                    typevar_names.add(target.id)
                    typevar_calls.append((target.id, call))

        type_vars: dict[str, TypeNode] = {}
        for name, call in typevar_calls:
            constraints: list[TypeNode] = []
            for arg in call.args[1:]:
                node_ir = self._from_annotation_node(arg, class_names, typevar_names, context)
                if not isinstance(node_ir, UnknownType):
                    constraints.append(node_ir)

            bound: TypeNode | None = None
            for keyword in call.keywords:
                if keyword.arg == "bound":
                    bound = self._from_annotation_node(keyword.value, class_names, typevar_names, context)
                    break

            constraint_node: TypeNode | None = None
            if constraints:
                constraint_node = constraints[0] if len(constraints) == 1 else UnionType(list(constraints))
            elif bound and not isinstance(bound, UnknownType):
                constraint_node = bound

            if constraint_node is not None:
                type_vars[name] = constraint_node

        return type_vars

    def get_type_vars(self, path: Path) -> dict[str, TypeNode]:
        tree = self.from_source_file(path)
        digest = self.ast_cache[path].digest
        cached = self.type_var_cache.get(path)
        if cached and cached.digest == digest:
            return cached.type_vars
        classes = self.get_class_defs(path)
        imports = self.get_imports(path)
        class_names = set(classes.keys())
        context = ResolveContext(path=path, imports=imports, local_classes=class_names, search_paths=(path.parent,))
        type_vars = self.collect_type_vars(tree, class_names, context)
        self.type_var_cache[path] = CachedTypeVars(digest=digest, type_vars=type_vars)
        return type_vars

    def extract_msgspec_structs(self, path: Path) -> dict[str, ObjectType]:
        classes = self.get_class_defs(path)
        imports = self.get_imports(path)
        type_vars = self.get_type_vars(path)
        struct_names = {name for name, cls in classes.items() if self._is_msgspec_struct(cls)}
        context = ResolveContext(path=path, imports=imports, local_classes=struct_names, search_paths=(path.parent,))
        structs: dict[str, ObjectType] = {}
        for name in struct_names:
            structs[name] = self._build_msgspec_struct(classes[name], struct_names, classes, type_vars, context)
            self.type_cache[path, name] = structs[name]
        return structs

    def extract_pydantic_models(self, path: Path) -> dict[str, ObjectType]:
        classes = self.get_class_defs(path)
        imports = self.get_imports(path)
        type_vars = self.get_type_vars(path)
        model_names = {name for name, cls in classes.items() if self._is_pydantic_model(cls)}
        context = ResolveContext(path=path, imports=imports, local_classes=model_names, search_paths=(path.parent,))
        models: dict[str, ObjectType] = {}
        for name in model_names:
            models[name] = self._build_pydantic_model(classes[name], model_names, classes, type_vars, context)
            self.type_cache[path, name] = models[name]
        return models

    def extract_dataclasses(self, path: Path) -> dict[str, ObjectType]:
        classes = self.get_class_defs(path)
        imports = self.get_imports(path)
        type_vars = self.get_type_vars(path)
        dataclass_names = {name for name, cls in classes.items() if self._is_dataclass(cls)}
        context = ResolveContext(path=path, imports=imports, local_classes=dataclass_names, search_paths=(path.parent,))
        models: dict[str, ObjectType] = {}
        for name in dataclass_names:
            models[name] = self._build_dataclass(classes[name], dataclass_names, classes, type_vars, context)
            self.type_cache[path, name] = models[name]
        return models

    def extract_typeddicts(self, path: Path) -> dict[str, ObjectType]:
        classes = self.get_class_defs(path)
        imports = self.get_imports(path)
        type_vars = self.get_type_vars(path)
        typed_dict_names = {name for name, cls in classes.items() if self._is_typeddict(cls)}
        context = ResolveContext(
            path=path, imports=imports, local_classes=typed_dict_names, search_paths=(path.parent,)
        )
        models: dict[str, ObjectType] = {}
        for name in typed_dict_names:
            models[name] = self._build_typeddict(classes[name], typed_dict_names, type_vars, context)
            self.type_cache[path, name] = models[name]
        return models

    def merge_msgspec_runtime_metadata(
        self, structs: dict[str, ObjectType], runtime_classes: dict[str, object]
    ) -> dict[str, ObjectType]:
        merged: dict[str, ObjectType] = {}
        for name, struct in structs.items():
            runtime_cls = runtime_classes.get(name)
            metadata = self._msgspec_runtime_metadata(runtime_cls) if runtime_cls else {}
            if metadata:
                merged[name] = self._apply_field_metadata(struct, metadata)
            else:
                merged[name] = struct
        return merged

    def merge_pydantic_runtime_metadata(
        self, models: dict[str, ObjectType], runtime_classes: dict[str, object]
    ) -> dict[str, ObjectType]:
        merged: dict[str, ObjectType] = {}
        for name, model in models.items():
            runtime_cls = runtime_classes.get(name)
            metadata, computed = self._pydantic_runtime_metadata(runtime_cls) if runtime_cls else ({}, set())
            if computed:
                filtered_fields = {key: value for key, value in model.fields.items() if key not in computed}
                model = ObjectType(
                    fields=filtered_fields,
                    name=model.name,
                    optional_fields=model.optional_fields - computed,
                    type_params=model.type_params,
                    type_param_constraints=model.type_param_constraints,
                )
            if metadata:
                merged[name] = self._apply_field_metadata(model, metadata)
            else:
                merged[name] = model
        return merged

    def extract_pydantic_json_schema(self, model_cls: object) -> tuple[TypeNode, dict[str, ObjectType]]:
        try:
            from pydantic import BaseModel
        except ImportError:
            return UnknownType(), {}
        if not isinstance(model_cls, type) or not issubclass(model_cls, BaseModel):
            return UnknownType(), {}
        schema = model_cls.model_json_schema()
        definitions = schema.get("$defs") or schema.get("definitions") or {}
        named_defs: dict[str, ObjectType] = {}
        for name, defn in cast(dict[str, Any], definitions).items():
            node = self._from_json_schema(defn, definitions, name=name)
            if isinstance(node, ObjectType):
                named_defs[name] = node
        root = self._from_json_schema(schema, definitions, name=schema.get("title"))
        return root, named_defs

    def _is_msgspec_struct(self, node: ast.ClassDef) -> bool:
        for base in node.bases:
            base_name = self._base_name(base)
            if base_name == "Struct" or base_name.endswith(".Struct"):
                return True
        return False

    def _is_pydantic_model(self, node: ast.ClassDef) -> bool:
        for base in node.bases:
            base_name = self._base_name(base)
            if base_name == "BaseModel" or base_name.endswith(".BaseModel"):
                return True
        return False

    def _is_dataclass(self, node: ast.ClassDef) -> bool:
        for decorator in node.decorator_list:
            name = self._base_name(decorator)
            if name == "dataclass" or name.endswith(".dataclass"):
                return True
        return False

    def _is_typeddict(self, node: ast.ClassDef) -> bool:
        for base in node.bases:
            base_name = self._base_name(base)
            if base_name == "TypedDict" or base_name.endswith(".TypedDict"):
                return True
        return False

    def _base_name(self, node: ast.AST) -> str:
        if isinstance(node, ast.Name):
            return node.id
        if isinstance(node, ast.Attribute):
            return f"{self._base_name(node.value)}.{node.attr}"
        return ""

    def _build_msgspec_struct(
        self,
        node: ast.ClassDef,
        struct_names: set[str],
        class_map: dict[str, ast.ClassDef],
        type_vars: dict[str, TypeNode] | None = None,
        context: ResolveContext | None = None,
    ) -> ObjectType:
        fields: dict[str, TypeNode] = {}
        optional_fields: set[str] = set()
        type_params, param_constraints = self._generic_params(node, type_vars)
        type_vars_set = set(type_params)
        for stmt in node.body:
            if isinstance(stmt, ast.AnnAssign) and isinstance(stmt.target, ast.Name):
                field_name = stmt.target.id
                field_type = self._from_annotation_node(stmt.annotation, struct_names, type_vars_set, context)
                fields[field_name] = field_type
                if stmt.value is not None:
                    optional_fields.add(field_name)
        return ObjectType(
            fields=fields,
            name=node.name,
            optional_fields=optional_fields,
            type_params=type_params,
            type_param_constraints=param_constraints,
        )

    def _build_pydantic_model(
        self,
        node: ast.ClassDef,
        model_names: set[str],
        class_map: dict[str, ast.ClassDef],
        type_vars: dict[str, TypeNode] | None = None,
        context: ResolveContext | None = None,
    ) -> ObjectType:
        fields: dict[str, TypeNode] = {}
        optional_fields: set[str] = set()
        type_params, param_constraints = self._generic_params(node, type_vars)
        type_vars_set = set(type_params)
        for stmt in node.body:
            if isinstance(stmt, ast.AnnAssign) and isinstance(stmt.target, ast.Name):
                field_name = stmt.target.id
                field_type = self._from_annotation_node(stmt.annotation, model_names, type_vars_set, context)
                fields[field_name] = field_type
                if stmt.value is not None:
                    optional_fields.add(field_name)
        return ObjectType(
            fields=fields,
            name=node.name,
            optional_fields=optional_fields,
            type_params=type_params,
            type_param_constraints=param_constraints,
        )

    def _build_dataclass(
        self,
        node: ast.ClassDef,
        dataclass_names: set[str],
        class_map: dict[str, ast.ClassDef],
        type_vars: dict[str, TypeNode] | None = None,
        context: ResolveContext | None = None,
    ) -> ObjectType:
        fields: dict[str, TypeNode] = {}
        optional_fields: set[str] = set()
        type_params, param_constraints = self._generic_params(node, type_vars)
        type_vars_set = set(type_params)
        for stmt in node.body:
            if isinstance(stmt, ast.AnnAssign) and isinstance(stmt.target, ast.Name):
                field_name = stmt.target.id
                annotation = stmt.annotation
                if self._is_classvar(annotation) or self._is_initvar(annotation):
                    continue
                field_type = self._from_annotation_node(annotation, dataclass_names, type_vars_set, context)
                fields[field_name] = field_type
                if stmt.value is not None and self._dataclass_has_default(stmt.value):
                    optional_fields.add(field_name)
        return ObjectType(
            fields=fields,
            name=node.name,
            optional_fields=optional_fields,
            type_params=type_params,
            type_param_constraints=param_constraints,
        )

    def _build_typeddict(
        self,
        node: ast.ClassDef,
        typed_dict_names: set[str],
        type_vars: dict[str, TypeNode] | None = None,
        context: ResolveContext | None = None,
    ) -> ObjectType:
        total = True
        for keyword in node.keywords:
            if keyword.arg == "total" and isinstance(keyword.value, ast.Constant):
                total = bool(keyword.value.value)
        fields: dict[str, TypeNode] = {}
        optional_fields: set[str] = set()
        type_params, param_constraints = self._generic_params(node, type_vars)
        type_vars_set = set(type_params)
        for stmt in node.body:
            if isinstance(stmt, ast.AnnAssign) and isinstance(stmt.target, ast.Name):
                field_name = stmt.target.id
                annotation = stmt.annotation
                annotation, required_override = self._unwrap_required(annotation)
                field_type = self._from_annotation_node(annotation, typed_dict_names, type_vars_set, context)
                fields[field_name] = field_type
                if not total:
                    optional_fields.add(field_name)
                if required_override is True and field_name in optional_fields:
                    optional_fields.discard(field_name)
                if required_override is False:
                    optional_fields.add(field_name)
        return ObjectType(
            fields=fields,
            name=node.name,
            optional_fields=optional_fields,
            type_params=type_params,
            type_param_constraints=param_constraints,
        )

    def _msgspec_runtime_metadata(self, struct_cls: object) -> dict[str, tuple[object, ...]]:
        try:
            import msgspec
        except ImportError:
            return {}

        inspect = getattr(msgspec, "inspect", None)
        type_info = getattr(inspect, "type_info", None) if inspect else None
        if not callable(type_info):
            return {}
        try:
            info = type_info(struct_cls)
        except (TypeError, ValueError):
            return {}
        fields = getattr(info, "fields", None)
        if not fields:
            return {}
        metadata: dict[str, tuple[object, ...]] = {}
        for field_info in cast(Any, fields):
            name = getattr(field_info, "name", None)
            if not name:
                continue
            items: list[object] = []
            for key in ("metadata", "constraints", "validation"):
                value = getattr(field_info, key, None)
                if value:
                    items.append({key: value})
            if items:
                metadata[name] = tuple(items)
        return metadata

    def _pydantic_runtime_metadata(self, model_cls: object) -> tuple[dict[str, tuple[object, ...]], set[str]]:
        try:
            from pydantic import BaseModel
        except ImportError:
            return {}, set()
        if not isinstance(model_cls, type) or not issubclass(model_cls, BaseModel):
            return {}, set()
        fields = getattr(model_cls, "model_fields", None)
        computed_fields = getattr(model_cls, "model_computed_fields", None) or {}
        if not fields:
            return {}, set()
        metadata: dict[str, tuple[object, ...]] = {}
        for name, info in cast(dict[str, Any], fields).items():
            items: list[object] = []
            for key in ("description", "alias", "validation_alias", "default"):
                value = getattr(info, key, None)
                if value is not None:
                    items.append({key: value})
            if items:
                metadata[name] = tuple(items)
        return metadata, set(computed_fields.keys())

    def merge_dataclass_runtime_metadata(
        self, models: dict[str, ObjectType], runtime_classes: dict[str, object]
    ) -> dict[str, ObjectType]:
        merged: dict[str, ObjectType] = {}
        for name, model in models.items():
            runtime_cls = runtime_classes.get(name)
            optional = self._dataclass_optional_fields(runtime_cls) if runtime_cls else set()
            filtered_fields = {
                key: value
                for key, value in model.fields.items()
                if key not in self._dataclass_excluded_fields(runtime_cls)
            }
            merged[name] = ObjectType(
                fields=filtered_fields,
                name=model.name,
                optional_fields=model.optional_fields | optional,
                type_params=model.type_params,
                type_param_constraints=model.type_param_constraints,
            )
        return merged

    def _dataclass_optional_fields(self, dataclass_cls: object) -> set[str]:
        fields = getattr(dataclass_cls, "__dataclass_fields__", None) if dataclass_cls else None
        if not fields:
            return set()
        optional: set[str] = set()
        for name, info in cast(dict[str, Any], fields).items():
            default = getattr(info, "default", None)
            default_factory = getattr(info, "default_factory", None)
            if default is not None or default_factory is not None:
                optional.add(name)
        return optional

    def _dataclass_excluded_fields(self, dataclass_cls: object) -> set[str]:
        fields = getattr(dataclass_cls, "__dataclass_fields__", None) if dataclass_cls else None
        if not fields:
            return set()
        excluded: set[str] = set()
        for name, info in cast(dict[str, Any], fields).items():
            if getattr(info, "init", True) is False and name.startswith("_"):
                excluded.add(name)
        return excluded

    def _from_json_schema(  # noqa: PLR0911, PLR0912
        self, schema: dict[str, object], definitions: dict[str, Any], name: str | None = None
    ) -> TypeNode:
        ref = schema.get("$ref")
        if isinstance(ref, str):
            ref_name = self._ref_name(ref)
            return NamedTypeRef(ref_name) if ref_name else UnknownType()

        if "enum" in schema:
            values = cast(list[Any], schema.get("enum") or [])
            node: TypeNode = UnionType([LiteralType(value) for value in values])
            return self._apply_schema_metadata(node, schema)

        if "oneOf" in schema or "anyOf" in schema:
            variants = cast(list[dict[str, Any]], schema.get("oneOf") or schema.get("anyOf") or [])
            node = UnionType([self._from_json_schema(item, definitions) for item in variants])
            return self._apply_schema_metadata(node, schema)

        types_val = schema.get("type")
        if isinstance(types_val, list):
            node = UnionType([self._from_json_schema({"type": t}, definitions) for t in types_val])
            return self._apply_schema_metadata(node, schema)
        if types_val == "object":
            properties = cast(dict[str, Any], schema.get("properties") or {})
            required = set(cast(list[str], schema.get("required") or []))
            fields: dict[str, TypeNode] = {}
            optional_fields: set[str] = set()
            for prop_name, prop_schema in properties.items():
                if not isinstance(prop_schema, dict):
                    continue
                fields[prop_name] = self._from_json_schema(prop_schema, definitions)
                if prop_name not in required:
                    optional_fields.add(prop_name)
            node = ObjectType(fields=fields, name=name, optional_fields=optional_fields)
            return self._apply_schema_metadata(node, schema)
        if types_val == "array":
            items = schema.get("items")
            if isinstance(items, dict):
                node = ArrayType(self._from_json_schema(items, definitions))
            else:
                node = ArrayType(UnknownType())
            return self._apply_schema_metadata(node, schema)
        if types_val == "string":
            return self._apply_schema_metadata(PrimitiveType("string"), schema)
        if types_val in {"integer", "number"}:
            return self._apply_schema_metadata(PrimitiveType("number"), schema)
        if types_val == "boolean":
            return self._apply_schema_metadata(PrimitiveType("boolean"), schema)
        if types_val == "null":
            return self._apply_schema_metadata(PrimitiveType("null"), schema)
        return UnknownType()

    def _apply_schema_metadata(self, node: TypeNode, schema: dict[str, object]) -> TypeNode:
        discriminator = schema.get("discriminator")
        if discriminator is None:
            return node
        metadata = ({"discriminator": discriminator},)
        if isinstance(node, AnnotatedType):
            return AnnotatedType(base=node.base, metadata=node.metadata + metadata)
        return AnnotatedType(base=node, metadata=metadata)

    def _ref_name(self, ref: str) -> str:
        if ref.startswith("#/"):
            return ref.rsplit("/", maxsplit=1)[-1]
        return ref

    def _apply_field_metadata(self, struct: ObjectType, metadata: dict[str, tuple[object, ...]]) -> ObjectType:
        fields: dict[str, TypeNode] = {}
        for name, field_type in struct.fields.items():
            extra = metadata.get(name)
            if extra:
                if isinstance(field_type, AnnotatedType):
                    fields[name] = AnnotatedType(base=field_type.base, metadata=field_type.metadata + extra)
                else:
                    fields[name] = AnnotatedType(base=field_type, metadata=extra)
            else:
                fields[name] = field_type
        return ObjectType(
            fields=fields,
            name=struct.name,
            optional_fields=struct.optional_fields,
            type_params=struct.type_params,
            type_param_constraints=struct.type_param_constraints,
        )

    def _is_classvar(self, annotation: ast.AST) -> bool:
        if isinstance(annotation, ast.Subscript):
            base = self._base_name(annotation.value).split(".")[-1]
            return base == "ClassVar"
        return False

    def _is_initvar(self, annotation: ast.AST) -> bool:
        if isinstance(annotation, ast.Subscript):
            base = self._base_name(annotation.value).split(".")[-1]
            return base == "InitVar"
        return False

    def _dataclass_has_default(self, node: ast.AST) -> bool:
        if isinstance(node, ast.Call) and self._base_name(node.func).split(".")[-1] == "field":
            for keyword in node.keywords:
                if keyword.arg in {"default", "default_factory"} and keyword.value is not None:
                    return True
        return True

    def _unwrap_required(self, annotation: ast.AST) -> tuple[ast.AST, bool | None]:
        if isinstance(annotation, ast.Subscript):
            base = self._base_name(annotation.value).split(".")[-1]
            if base == "Required":
                return annotation.slice, True
            if base == "NotRequired":
                return annotation.slice, False
        return annotation, None

    def _generic_params(
        self, node: ast.ClassDef, type_vars: dict[str, TypeNode] | None = None
    ) -> tuple[tuple[str, ...], dict[str, TypeNode]]:
        params: list[str] = []
        for base in node.bases:
            if not isinstance(base, ast.Subscript):
                continue
            base_name = self._base_name(base.value).split(".")[-1]
            if base_name != "Generic":
                continue
            for arg in self._subscript_args(base.slice):
                if isinstance(arg, ast.Name):
                    params.append(arg.id)
                elif isinstance(arg, ast.Attribute):
                    params.append(arg.attr)
        constraint_map: dict[str, TypeNode] = {}
        if type_vars:
            for name in params:
                constraint = type_vars.get(name)
                if constraint and not isinstance(constraint, UnknownType):
                    constraint_map[name] = constraint
        return tuple(params), constraint_map

    def _resolve_forward_ref(self, name: str, context: ResolveContext) -> TypeNode | None:
        if name in context.local_classes:
            return NamedTypeRef(name)
        target = context.imports.get(name)
        if not target:
            return None
        module, level = target
        path = self._resolve_import_path(context.path, module, level, context.search_paths)
        if path is None:
            return None
        classes = self.get_class_defs(path)
        if name in classes:
            return NamedTypeRef(name)
        return None

    def _resolve_import_path(
        self, current_path: Path, module: str, level: int, search_paths: tuple[Path, ...]
    ) -> Path | None:
        if level > 0:
            base = current_path.parent
            for _ in range(level - 1):
                base = base.parent
            module_path = base / module.replace(".", "/")
            return self._module_to_path(module_path)
        for base in search_paths:
            module_path = base / module.replace(".", "/")
            resolved = self._module_to_path(module_path)
            if resolved:
                return resolved
        return None

    def _module_to_path(self, module_path: Path) -> Path | None:
        candidate = module_path.with_suffix(".py")
        if candidate.exists():
            return candidate
        init_file = module_path / "__init__.py"
        if init_file.exists():
            return init_file
        return None

    def _from_annotation_node(  # noqa: PLR0911, PLR0912
        self,
        node: ast.AST,
        struct_names: set[str],
        type_vars: set[str] | None = None,
        context: ResolveContext | None = None,
    ) -> TypeNode:
        type_vars = type_vars or set()
        if isinstance(node, ast.Name):
            if node.id in type_vars:
                return RawType(node.id)
            if node.id in struct_names:
                return NamedTypeRef(node.id)
            primitive = self._primitive_from_name(node.id)
            if not isinstance(primitive, UnknownType):
                return primitive
            if context:
                resolved = self._resolve_forward_ref(node.id, context)
                if resolved is not None:
                    return resolved
                warnings.warn(f"Unresolved type reference: {node.id}", RuntimeWarning, stacklevel=2)
            return primitive
        if isinstance(node, ast.Attribute):
            name = self._base_name(node)
            return self._primitive_from_name(name.split(".")[-1])
        if isinstance(node, ast.Constant):
            if isinstance(node.value, str):
                if node.value in struct_names:
                    return NamedTypeRef(node.value)
                if context:
                    resolved = self._resolve_forward_ref(node.value, context)
                    if resolved is not None:
                        return resolved
                    warnings.warn(f"Unresolved type reference: {node.value}", RuntimeWarning, stacklevel=2)
                return UnknownType()
            return LiteralType(node.value)
        if isinstance(node, ast.Subscript):
            return self._from_subscript(node, struct_names, type_vars, context)
        return UnknownType()

    def _from_subscript(  # noqa: PLR0911
        self,
        node: ast.Subscript,
        struct_names: set[str],
        type_vars: set[str] | None = None,
        context: ResolveContext | None = None,
    ) -> TypeNode:
        type_vars = type_vars or set()
        base = self._base_name(node.value).split(".")[-1]
        args = self._subscript_args(node.slice)
        if base in {"list", "List"}:
            return ArrayType(self._from_annotation_node(args[0], struct_names, type_vars, context))
        if base in {"set", "Set"}:
            return GenericType("Set", [self._from_annotation_node(args[0], struct_names, type_vars, context)])
        if base in {"dict", "Dict"}:
            key = self._from_annotation_node(args[0], struct_names, type_vars, context)
            value = (
                self._from_annotation_node(args[1], struct_names, type_vars, context)
                if len(args) > 1
                else UnknownType()
            )
            return GenericType("Record", [key, value])
        if base in {"tuple", "Tuple"}:
            if len(args) == 2 and isinstance(args[1], ast.Constant) and args[1].value is Ellipsis:
                return ArrayType(self._from_annotation_node(args[0], struct_names, type_vars, context))
            return TupleType(tuple(self._from_annotation_node(arg, struct_names, type_vars, context) for arg in args))
        if base == "Optional":
            inner = self._from_annotation_node(args[0], struct_names, type_vars, context)
            return UnionType([inner, PrimitiveType("null")])
        if base == "Union":
            return UnionType([self._from_annotation_node(arg, struct_names, type_vars, context) for arg in args])
        if base == "Annotated":
            if not args:
                return UnknownType()
            base_type = self._from_annotation_node(args[0], struct_names, type_vars, context)
            metadata = tuple(self._metadata_from_node(arg) for arg in args[1:])
            return AnnotatedType(base=base_type, metadata=metadata)
        if base == "Literal":
            if len(args) == 1:
                return LiteralType(self._literal_arg(args[0]))
            return UnionType([LiteralType(self._literal_arg(arg)) for arg in args])
        if base in struct_names:
            params = [self._from_annotation_node(arg, struct_names, type_vars, context) for arg in args]
            return GenericType(base, params)
        return UnknownType()

    def _subscript_args(self, node: ast.AST) -> list[ast.AST]:
        if isinstance(node, ast.Tuple):
            return list(node.elts)
        return [node]

    def _literal_arg(self, node: ast.AST) -> object:
        if isinstance(node, ast.Constant):
            return node.value
        if (
            isinstance(node, ast.UnaryOp)
            and isinstance(node.op, ast.USub)
            and isinstance(node.operand, ast.Constant)
            and isinstance(node.operand.value, (int, float))
        ):
            return -node.operand.value
        return None

    def _metadata_from_node(self, node: ast.AST) -> object:
        try:
            return ast.literal_eval(node)
        except (SyntaxError, ValueError):
            try:
                return ast.unparse(node)
            except (TypeError, ValueError):
                return None

    def _primitive_from_name(self, name: str) -> TypeNode:
        primitives = {
            "int": PrimitiveType("number"),
            "float": PrimitiveType("number"),
            "str": PrimitiveType("string"),
            "bool": PrimitiveType("boolean"),
            "bytes": PrimitiveType("Uint8Array"),
        }
        return primitives.get(name, UnknownType())