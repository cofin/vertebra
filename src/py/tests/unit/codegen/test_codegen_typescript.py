"""Tests for route-driven TypeScript and bidirectional Protobuf v3 generation."""

import asyncio
from dataclasses import dataclass
from typing import Any

import msgspec

from vertebra.build.codegen._metadata import RouteCollector
from vertebra.build.codegen._typescript import TypeScriptGenerator, format_typescript
from vertebra.build.codegen.openapi import OpenAPIGenerator
from vertebra.build.codegen.protobuf import ProtobufGenerator, ProtobufParser
from vertebra.build.codegen.valibot import ValibotGenerator
from vertebra.build.codegen.zod import ZodGenerator
from vertebra.framework._app import App
from vertebra.framework.procedures import ProcedureRouter
from vertebra.utils.serializers import decode_grpc_frame, encode_grpc_frame_into


class User(msgspec.Struct):
    id: int
    name: str


class CreateUserInput(msgspec.Struct, frozen=True, gc=False):
    name: str
    tags: list[str] = msgspec.field(default_factory=list)


@dataclass
class Payload:
    value: int


def test_generate_typescript_from_routes() -> None:
    app = App()

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
    app = App()

    @app.post("/payload")
    def create_payload(payload: Payload) -> Payload:
        return payload

    routes = RouteCollector().collect(app)
    output = format_typescript(TypeScriptGenerator().generate(routes))

    assert "interface Payload" in output
    assert "value: number" in output
    assert "export type payload" in output


def test_bidirectional_protobuf_routes_and_procedures() -> None:
    app = App()
    rpc = ProcedureRouter(prefix="/_rpc")

    @app.get("/users/{user_id}")
    def get_user(user_id: int) -> User:
        return User(id=user_id, name="Ada")

    @rpc.mutation(name="CreateUser")
    async def create_user(payload: CreateUserInput) -> User:
        return User(id=42, name=payload.name)

    app.register(rpc)

    proto_gen = ProtobufGenerator(package="acme.users.v1", service_name="UserService")
    proto_text = proto_gen.generate_from_app(app)

    assert 'syntax = "proto3";' in proto_text
    assert "package acme.users.v1;" in proto_text
    assert "message User {" in proto_text
    assert "message CreateUserInput {" in proto_text
    assert "service UserService {" in proto_text
    assert "rpc GetUser (GetUserRequest) returns (User);" in proto_text
    assert "rpc CreateUser (CreateUserInput) returns (User);" in proto_text

    parsed = ProtobufParser.parse(proto_text)
    assert parsed.package == "acme.users.v1"
    assert "User" in parsed.messages
    assert "CreateUserInput" in parsed.messages
    assert "UserService" in parsed.services

    ts_out = TypeScriptGenerator(declared=dict(parsed.declared_types)).generate(parsed.routes)
    assert "export interface User" in ts_out
    assert "export interface CreateUserInput" in ts_out

    zod_out = ZodGenerator(declared=dict(parsed.declared_types)).generate(parsed.routes)
    assert "UserSchema = z.object" in zod_out

    valibot_out = ValibotGenerator(declared=dict(parsed.declared_types)).generate(parsed.routes)
    assert "UserSchema = v.object" in valibot_out

    openapi_doc = OpenAPIGenerator(declared=dict(parsed.declared_types)).generate(parsed.routes)
    assert "/acme.users.v1.UserService/CreateUser" in openapi_doc["paths"]

    stubs_code = ProtobufGenerator.generate_python_stubs(proto_text)
    assert "class User(msgspec.Struct, frozen=True, gc=False):" in stubs_code
    assert "def create_user_service_router() -> ProcedureRouter:" in stubs_code

    runtime_structs = ProtobufGenerator.load_runtime_types(proto_text)
    dynamic_in_cls = runtime_structs["CreateUserInput"]
    dynamic_out_cls = runtime_structs["User"]

    async def handle_create(req: Any) -> Any:
        return dynamic_out_cls(id=99, name=req.name)

    proto_router = ProcedureRouter.from_proto(
        proto_text,
        handlers={"CreateUser": handle_create},
    )
    frame = encode_grpc_frame_into(
        dynamic_in_cls(name="Grace", tags=["admin"]),
        format="application/grpc+msgpack",
    )
    decoded_req = decode_grpc_frame(
        frame,
        target_type=dynamic_in_cls,
        format="application/grpc+msgpack",
    )
    result = asyncio.run(proto_router.call("CreateUser", decoded_req))
    assert result.id == 99
    assert result.name == "Grace"


def test_websocket_route_extraction_and_ws_url_codegen() -> None:
    from vertebra.build.codegen._manifest import ManifestGenerator
    from vertebra.framework._router import Route
    from vertebra.utils.serializers import decode_json

    app = App()

    @app.get("/users/{user_id:int}")
    def get_user(user_id: int) -> User:
        return User(id=user_id, name="Ada")

    async def chat_room(room_id: str, data: CreateUserInput) -> User:
        return User(id=1, name=f"{room_id}:{data.name}")

    app.add_route(Route(method="WEBSOCKET", path="/ws/rooms/{room_id:str}", handler=chat_room))

    routes = RouteCollector().collect(app)
    ws_routes = [r for r in routes if r.protocol == "websocket"]
    assert len(ws_routes) == 1
    assert ws_routes[0].channel_key == "ws_rooms_by_room_id"
    assert ws_routes[0].receive_schema is not None
    assert ws_routes[0].send_schema is not None

    manifest_data = decode_json(ManifestGenerator().generate(routes))
    assert "websocketRoutes" in manifest_data
    assert "ws_rooms_by_room_id" in manifest_data["websocketRoutes"]
    assert manifest_data["websocketRoutes"]["ws_rooms_by_room_id"]["route"] == "/ws/rooms/{room_id:str}"

    ts_out = TypeScriptGenerator().generate(routes)
    assert 'export type WebSocketRouteName = "ws_rooms_by_room_id";' in ts_out
    assert "export interface WebSocketRoutePathParams {" in ts_out
    assert "export interface WebSocketChannelPayloads {" in ts_out
    assert "receive: CreateUserInput; send: User" in ts_out
    assert "export function wsUrl<K extends WebSocketRouteName>(" in ts_out
