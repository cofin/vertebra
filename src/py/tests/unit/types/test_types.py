import ast
import typing
from pathlib import Path
from typing import Literal

import pytest

from vertebra.procedures.registry import ProcedureMetadata
from vertebra.types.extractor import TypeExtractor
from vertebra.types.ir import (
    AnnotatedType,
    ArrayType,
    LiteralType,
    NamedTypeRef,
    ObjectType,
    PrimitiveType,
    RawType,
    TupleType,
    UnionType,
    UnknownType,
)
from vertebra.types.typescript import TypeScriptGenerator, format_typescript, validate_typescript

pytestmark = pytest.mark.unit


def test_type_ir_to_typescript() -> None:
    obj = ObjectType(fields={"id": PrimitiveType("number")}, name="User")
    assert obj.to_typescript() == "{ id: number; }"
    arr = ArrayType(PrimitiveType("string"))
    assert arr.to_typescript() == "string[]"
    union = UnionType([PrimitiveType("number"), PrimitiveType("null")])
    assert union.to_typescript() == "number | null"
    literal = LiteralType("ok")
    assert literal.to_typescript() == '"ok"'
    tup = TupleType((PrimitiveType("number"), PrimitiveType("string")))
    assert tup.to_typescript() == "[number, string]"


def test_type_extractor_primitives() -> None:
    extractor = TypeExtractor()
    assert extractor.from_hint(int) == PrimitiveType("number")
    assert extractor.from_hint(str) == PrimitiveType("string")
    assert extractor.from_hint(bool) == PrimitiveType("boolean")
    assert extractor.from_hint(bytes) == PrimitiveType("Uint8Array")
    assert isinstance(extractor.from_hint(object), UnknownType)


def test_type_extractor_annotated() -> None:
    extractor = TypeExtractor()
    annotated = extractor.from_hint(typing.Annotated[int, "min=1"])
    assert isinstance(annotated, AnnotatedType)
    assert annotated.base == PrimitiveType("number")
    assert annotated.metadata == ("min=1",)


def test_type_extractor_collections_and_union() -> None:
    extractor = TypeExtractor()
    assert extractor.from_hint(list[int]) == ArrayType(PrimitiveType("number"))
    assert extractor.from_hint(tuple[int, str]) == TupleType((PrimitiveType("number"), PrimitiveType("string")))
    assert extractor.from_hint(tuple[int, ...]) == ArrayType(PrimitiveType("number"))
    union = extractor.from_hint(int | None)
    assert isinstance(union, UnionType)
    assert LiteralType("ok") in extractor.from_hint(Literal["ok", "bad"]).variants


def test_typescript_generator_basic() -> None:
    user = ObjectType(fields={"id": PrimitiveType("number")}, name="User")
    meta = ProcedureMetadata(
        procedure_id="users.get",
        handler_fn=lambda: None,
        source_file=Path(__file__),
        params={"user_id": PrimitiveType("number")},
        return_type=user,
        serializer="msgspec",
        metadata={},
    )
    output = TypeScriptGenerator().generate([meta])
    assert "export interface User" in output
    assert "export namespace Procedures" in output
    assert "export type get" in output
    formatted = format_typescript(output)
    assert formatted.strip().startswith("// DO NOT EDIT")
    if not validate_typescript(output):
        return


def test_type_extractor_ast_cache_and_classes(tmp_path: Path) -> None:
    extractor = TypeExtractor()
    source = "class User:\n    pass\n\nclass Account:\n    pass\n"
    file_path = tmp_path / "models.py"
    file_path.write_text(source)
    tree = extractor.from_source_file(file_path)
    assert isinstance(tree, ast.AST)
    classes = extractor.collect_class_defs(tree)
    assert set(classes.keys()) == {"User", "Account"}


def test_msgspec_struct_extraction(tmp_path: Path) -> None:
    extractor = TypeExtractor()
    source = (
        "from typing import Annotated\n"
        "import msgspec\n\n"
        "class Account(msgspec.Struct):\n"
        "    id: int\n\n"
        "class User(msgspec.Struct):\n"
        "    account: Account\n"
        '    score: Annotated[int, "min=1"]\n'
        '    name: str = "anon"\n'
    )
    file_path = tmp_path / "models.py"
    file_path.write_text(source)
    structs = extractor.extract_msgspec_structs(file_path)
    assert set(structs.keys()) == {"Account", "User"}
    user = structs["User"]
    assert isinstance(user.fields["account"], NamedTypeRef)
    assert isinstance(user.fields["score"], AnnotatedType)
    assert "name" in user.optional_fields


def test_msgspec_struct_circular_reference(tmp_path: Path) -> None:
    extractor = TypeExtractor()
    source = (
        "from typing import Optional\n"
        "import msgspec\n\n"
        "class Node(msgspec.Struct):\n"
        '    next: Optional["Node"] = None\n'
    )
    file_path = tmp_path / "nodes.py"
    file_path.write_text(source)
    structs = extractor.extract_msgspec_structs(file_path)
    node = structs["Node"]
    field = node.fields["next"]
    assert isinstance(field, UnionType)
    assert any(isinstance(item, NamedTypeRef) for item in field.variants)


def test_msgspec_runtime_merge_noop() -> None:
    extractor = TypeExtractor()
    obj = ObjectType(fields={"id": PrimitiveType("number")}, name="User")
    merged = extractor.merge_msgspec_runtime_metadata({"User": obj}, {})
    assert merged["User"] == obj


def test_pydantic_model_extraction(tmp_path: Path) -> None:
    extractor = TypeExtractor()
    source = (
        "from pydantic import BaseModel, Field\n\n"
        "class User(BaseModel):\n"
        "    id: int\n"
        '    name: str = Field(default="", description="name")\n'
    )
    file_path = tmp_path / "models.py"
    file_path.write_text(source)
    models = extractor.extract_pydantic_models(file_path)
    assert "User" in models
    user = models["User"]
    assert "name" in user.optional_fields


def test_pydantic_runtime_metadata_merge() -> None:
    from pydantic import BaseModel, Field, computed_field

    class User(BaseModel):
        id: int
        name: str = Field(default="", description="name", alias="full_name")

        @computed_field
        @property
        def upper(self) -> str:
            return self.name.upper()

    extractor = TypeExtractor()
    obj = ObjectType(
        fields={"id": PrimitiveType("number"), "name": PrimitiveType("string"), "upper": PrimitiveType("string")},
        name="User",
    )
    merged = extractor.merge_pydantic_runtime_metadata({"User": obj}, {"User": User})
    user = merged["User"]
    assert "upper" not in user.fields
    assert isinstance(user.fields["name"], AnnotatedType)


def test_pydantic_json_schema_conversion() -> None:
    from pydantic import BaseModel

    class Address(BaseModel):
        street: str

    class User(BaseModel):
        id: int
        address: Address

    extractor = TypeExtractor()
    root, defs = extractor.extract_pydantic_json_schema(User)
    assert isinstance(root, ObjectType)
    assert isinstance(root.fields["address"], NamedTypeRef)
    assert "Address" in defs


def test_pydantic_discriminated_union_metadata() -> None:
    from typing import Annotated

    from pydantic import BaseModel, Field

    class Cat(BaseModel):
        kind: Literal["cat"]
        meows: int

    class Dog(BaseModel):
        kind: Literal["dog"]
        barks: int

    pet_type = Annotated[Cat | Dog, Field(discriminator="kind")]

    class Owner(BaseModel):
        pet: pet_type

    extractor = TypeExtractor()
    root, _defs = extractor.extract_pydantic_json_schema(Owner)
    assert isinstance(root, ObjectType)
    pet = root.fields["pet"]
    assert isinstance(pet, AnnotatedType)
    assert any(isinstance(item, dict) and "discriminator" in item for item in pet.metadata)


def test_pydantic_validators_do_not_break_extraction() -> None:
    from pydantic import BaseModel, field_validator

    class User(BaseModel):
        id: int

        @field_validator("id")
        @classmethod
        def validate_id(cls, value: int) -> int:
            return value

    extractor = TypeExtractor()
    obj = ObjectType(fields={"id": PrimitiveType("number")}, name="User")
    merged = extractor.merge_pydantic_runtime_metadata({"User": obj}, {"User": User})
    assert "id" in merged["User"].fields


def test_dataclass_extraction(tmp_path: Path) -> None:
    extractor = TypeExtractor()
    source = (
        "from dataclasses import dataclass, field, InitVar\n"
        "from typing import ClassVar\n\n"
        "@dataclass\n"
        "class User:\n"
        "    id: int\n"
        '    name: str = "anon"\n'
        '    token: ClassVar[str] = "x"\n'
        "    init_only: InitVar[int] = 1\n"
        "    score: int = field(default=0)\n"
    )
    file_path = tmp_path / "models.py"
    file_path.write_text(source)
    models = extractor.extract_dataclasses(file_path)
    user = models["User"]
    assert "token" not in user.fields
    assert "init_only" not in user.fields
    assert "name" in user.optional_fields
    assert "score" in user.optional_fields


def test_generic_dataclass_extraction(tmp_path: Path) -> None:
    extractor = TypeExtractor()
    source = (
        "from dataclasses import dataclass\n"
        "from typing import Generic, TypeVar\n\n"
        'T = TypeVar("T")\n\n'
        "@dataclass\n"
        "class Box(Generic[T]):\n"
        "    value: T\n"
    )
    file_path = tmp_path / "models.py"
    file_path.write_text(source)
    models = extractor.extract_dataclasses(file_path)
    box = models["Box"]
    assert box.type_params == ("T",)
    assert box.fields["value"] == RawType("T")


def test_generic_constraints_extraction(tmp_path: Path) -> None:
    extractor = TypeExtractor()
    source = (
        "from dataclasses import dataclass\n"
        "from typing import Generic, TypeVar\n\n"
        "class Base:\n"
        "    pass\n\n"
        'T = TypeVar("T", int, str)\n'
        'U = TypeVar("U", bound=Base)\n\n'
        "@dataclass\n"
        "class Box(Generic[T, U]):\n"
        "    value: T\n"
        "    extra: U\n"
    )
    file_path = tmp_path / "models.py"
    file_path.write_text(source)
    models = extractor.extract_dataclasses(file_path)
    box = models["Box"]
    assert box.type_params == ("T", "U")
    assert box.type_param_constraints["T"] == UnionType([PrimitiveType("number"), PrimitiveType("string")])
    assert box.type_param_constraints["U"] == NamedTypeRef("Base")


def test_forward_ref_import_resolution(tmp_path: Path) -> None:
    extractor = TypeExtractor()
    models_path = tmp_path / "models.py"
    models_path.write_text("import msgspec\n\nclass User(msgspec.Struct):\n    id: int\n")
    service_path = tmp_path / "service.py"
    service_path.write_text(
        "import msgspec\nfrom models import User\n\nclass Request(msgspec.Struct):\n    user: User\n"
    )
    structs = extractor.extract_msgspec_structs(service_path)
    request = structs["Request"]
    assert isinstance(request.fields["user"], NamedTypeRef)


def test_cache_invalidation(tmp_path: Path) -> None:
    extractor = TypeExtractor()
    file_path = tmp_path / "models.py"
    file_path.write_text("class First:\n    pass\n")
    classes = extractor.get_class_defs(file_path)
    assert "First" in classes
    file_path.write_text("class Second:\n    pass\n")
    classes = extractor.get_class_defs(file_path)
    assert "Second" in classes


def test_typeddict_extraction(tmp_path: Path) -> None:
    extractor = TypeExtractor()
    source = (
        "from typing import TypedDict, Required, NotRequired\n\n"
        "class Movie(TypedDict, total=False):\n"
        "    name: str\n"
        "    year: Required[int]\n"
        "    rating: NotRequired[float]\n"
    )
    file_path = tmp_path / "models.py"
    file_path.write_text(source)
    models = extractor.extract_typeddicts(file_path)
    movie = models["Movie"]
    assert "name" in movie.optional_fields
    assert "rating" in movie.optional_fields
    assert "year" not in movie.optional_fields


def test_typescript_generator_toposort() -> None:
    account = ObjectType(fields={"id": PrimitiveType("number")}, name="Account")
    user = ObjectType(fields={"account": account, "name": PrimitiveType("string")}, name="User")
    meta = ProcedureMetadata(
        procedure_id="users.get",
        handler_fn=lambda: None,
        source_file=Path(__file__),
        params={},
        return_type=user,
        serializer="msgspec",
        metadata={},
    )
    output = TypeScriptGenerator().generate([meta])
    assert output.index("interface Account") < output.index("interface User")


def test_typescript_generator_renders_type_param_constraints() -> None:
    constrained = ObjectType(
        fields={"value": RawType("T")},
        name="Box",
        type_params=("T",),
        type_param_constraints={"T": UnionType([PrimitiveType("number"), PrimitiveType("string")])},
    )
    meta = ProcedureMetadata(
        procedure_id="boxes.get",
        handler_fn=lambda: None,
        source_file=Path(__file__),
        params={},
        return_type=constrained,
        serializer="msgspec",
        metadata={},
    )
    output = TypeScriptGenerator().generate([meta])
    assert "interface Box<T extends number | string>" in output


def test_typescript_generator_dedupes_named_types() -> None:
    user = ObjectType(fields={"id": PrimitiveType("number")}, name="User")
    meta_a = ProcedureMetadata(
        procedure_id="users.get",
        handler_fn=lambda: None,
        source_file=Path(__file__),
        params={},
        return_type=user,
        serializer="msgspec",
        metadata={},
    )
    meta_b = ProcedureMetadata(
        procedure_id="users.list",
        handler_fn=lambda: None,
        source_file=Path(__file__),
        params={},
        return_type=user,
        serializer="msgspec",
        metadata={},
    )
    output = TypeScriptGenerator().generate([meta_a, meta_b])
    assert output.count("interface User") == 1


def test_typescript_generator_rejects_conflicts() -> None:
    user_a = ObjectType(fields={"id": PrimitiveType("number")}, name="User")
    user_b = ObjectType(fields={"id": PrimitiveType("string")}, name="User")
    meta_a = ProcedureMetadata(
        procedure_id="users.get",
        handler_fn=lambda: None,
        source_file=Path(__file__),
        params={},
        return_type=user_a,
        serializer="msgspec",
        metadata={},
    )
    meta_b = ProcedureMetadata(
        procedure_id="users.update",
        handler_fn=lambda: None,
        source_file=Path(__file__),
        params={},
        return_type=user_b,
        serializer="msgspec",
        metadata={},
    )
    with pytest.raises(ValueError):
        TypeScriptGenerator().generate([meta_a, meta_b])


def test_typescript_generator_emits_metadata_comments() -> None:
    annotated = AnnotatedType(base=PrimitiveType("number"), metadata=("min=1",))
    obj = ObjectType(fields={"value": annotated}, name="Metric")
    meta = ProcedureMetadata(
        procedure_id="metrics.get",
        handler_fn=lambda: None,
        source_file=Path(__file__),
        params={},
        return_type=obj,
        serializer="msgspec",
        metadata={},
    )
    output = TypeScriptGenerator().generate([meta])
    assert "metadata:" in output


def test_typescript_generator_cycles_use_type_alias() -> None:
    node = ObjectType(fields={"next": NamedTypeRef("Node")}, name="Node")
    meta = ProcedureMetadata(
        procedure_id="nodes.get",
        handler_fn=lambda: None,
        source_file=Path(__file__),
        params={},
        return_type=node,
        serializer="msgspec",
        metadata={},
    )
    output = TypeScriptGenerator().generate([meta])
    assert "export type Node" in output
