"""Tests for route-driven type extraction and metadata collection."""

from dataclasses import dataclass, field
from typing import Annotated

import msgspec

from vertebra.builder._codegen._extractor import TypeExtractor
from vertebra.builder._codegen._ir import AnnotatedType, ArrayType, ObjectType, PrimitiveType, UnionType
from vertebra.builder._codegen._metadata import RouteCollector
from vertebra.framework._app import VertebraApp


class Address(msgspec.Struct):
    street: str


class User(msgspec.Struct):
    id: int
    address: Address
    name: str = "anon"


class SimpleUser(msgspec.Struct):
    id: int
    name: str


@dataclass
class Product:
    sku: str
    price: float
    tags: list[str] = field(default_factory=list)


def test_from_hint_primitives_and_collections() -> None:
    extractor = TypeExtractor()

    assert extractor.from_hint(int) == PrimitiveType("number")
    assert extractor.from_hint(str) == PrimitiveType("string")
    assert extractor.from_hint(bool) == PrimitiveType("boolean")
    assert extractor.from_hint(list[int]) == ArrayType(PrimitiveType("number"))

    maybe = extractor.from_hint(int | None)
    assert isinstance(maybe, UnionType)


def test_from_hint_preserves_annotated_metadata() -> None:
    extractor = TypeExtractor()

    annotated = extractor.from_hint(Annotated[int, "min=1"])
    assert isinstance(annotated, AnnotatedType)


def test_extract_msgspec_struct_runtime_model() -> None:
    user = TypeExtractor().from_hint(User)

    assert isinstance(user, ObjectType)
    assert user.name == "User"
    assert user.fields["id"] == PrimitiveType("number")
    assert isinstance(user.fields["address"], ObjectType)
    assert "name" in user.optional_fields


def test_extract_dataclass_runtime_model() -> None:
    product = TypeExtractor().from_hint(Product)

    assert isinstance(product, ObjectType)
    assert product.name == "Product"
    assert product.fields["sku"] == PrimitiveType("string")
    assert product.fields["price"] == PrimitiveType("number")
    assert "tags" in product.optional_fields


def test_collect_routes_from_app() -> None:
    app = VertebraApp()

    @app.get("/users/{user_id}")
    def get_user(user_id: int) -> SimpleUser:
        return SimpleUser(id=user_id, name="Ada")

    metadata = RouteCollector().collect(app)

    assert len(metadata) == 1
    route = metadata[0]
    assert route.operation_id == "get.users.by_user_id"
    assert route.params["user_id"] == PrimitiveType("number")
    assert route.path == "/users/{user_id}"
    assert route.method == "GET"
    assert isinstance(route.return_type, ObjectType)


def test_collect_routes_uses_unknown_for_unannotated_values() -> None:
    app = VertebraApp()

    @app.post("/submit")
    def submit(payload, count: int) -> str:  # noqa: ANN001
        return f"{payload}:{count}"

    route = RouteCollector().collect(app)[0]

    assert route.params["count"] == PrimitiveType("number")
    assert route.params["payload"].to_typescript() == "unknown"
