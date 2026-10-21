"""Tests for Zod generation from route metadata."""

from dataclasses import dataclass

import msgspec

from vertebra.build.codegen._metadata import RouteCollector
from vertebra.build.codegen.zod import ZodGenerator
from vertebra.framework._app import App


class User(msgspec.Struct):
    id: int
    name: str


@dataclass
class Payload:
    value: int


def test_generate_zod_named_schemas() -> None:
    app = App()

    @app.get("/users/{user_id}")
    def get_user(user_id: int) -> User:
        return User(id=user_id, name="Ada")

    routes = RouteCollector().collect(app)
    output = ZodGenerator().generate(routes)

    assert 'from "zod"' in output
    assert "export const UserSchema = z.object" in output
    assert "export const get_users_by_user_idParams" in output
    assert "z.number()" in output


def test_generate_zod_for_dataclass_payload() -> None:
    app = App()

    @app.post("/payload")
    def create_payload(payload: Payload) -> Payload:
        return payload

    routes = RouteCollector().collect(app)
    output = ZodGenerator().generate(routes)

    assert "PayloadSchema" in output
    assert "z.object" in output
    assert "post_payloadResponse" in output
