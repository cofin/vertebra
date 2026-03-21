"""Tests for route-driven TypeScript generation."""

from dataclasses import dataclass

import msgspec

from vertebra.builder._codegen._metadata import RouteCollector
from vertebra.builder._codegen._typescript import TypeScriptGenerator, format_typescript
from vertebra.framework._app import VertebraApp


class User(msgspec.Struct):
    id: int
    name: str


@dataclass
class Payload:
    value: int


def test_generate_typescript_from_routes() -> None:
    app = VertebraApp()

    @app.get("/users/{user_id}")
    def get_user(user_id: int) -> User:
        return User(id=user_id, name="Ada")

    routes = RouteCollector().collect(app)
    output = TypeScriptGenerator().generate(routes)

    assert "DO NOT EDIT" in output
    assert "export interface User" in output
    assert "export namespace Procedures" in output
    assert "export type by_user_id" in output
    assert "Promise<User>" in output


def test_generate_typescript_with_dataclass_body() -> None:
    app = VertebraApp()

    @app.post("/payload")
    def create_payload(payload: Payload) -> Payload:
        return payload

    routes = RouteCollector().collect(app)
    output = format_typescript(TypeScriptGenerator().generate(routes))

    assert "interface Payload" in output
    assert "value: number" in output
    assert "export type payload" in output
