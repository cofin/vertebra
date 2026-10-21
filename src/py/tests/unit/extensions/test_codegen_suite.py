"""Unit tests for vertebra.extensions.codegen (Valibot, Zod, OpenAPI 3.1.0, Protobuf v3, @procedure, Litestar v2/v3 markers)."""

from pathlib import Path
from typing import Annotated

import msgspec

from vertebra.builder._codegen._extractor import TypeExtractor
from vertebra.builder._codegen._metadata import RouteMetadata
from vertebra.extensions.codegen import (
    Body,
    CodegenConfig,
    FromCookie,
    FromHeader,
    FromPath,
    FromQuery,
    JSONBody,
    MsgPackBody,
    MultipartBody,
    NamedDependency,
    OpenAPIGenerator,
    Parameter,
    ProcedureClientGenerator,
    ProcedureRegistry,
    ProtobufGenerator,
    SkipValidation,
    URLEncodedBody,
    ValibotGenerator,
    VertebraCodegenPlugin,
    ZodGenerator,
    procedure,
)


class CreateOrderInput(msgspec.Struct):
    """Order creation payload."""

    sku: str
    quantity: int
    notes: str | None = None


class OrderRecord(msgspec.Struct):
    """Order response payload."""

    order_id: str
    sku: str
    quantity: int
    shipped: bool = False


class InternalService:
    """Dummy injected service dependency."""


class HeaderParameter:
    """Simulated Litestar v3 HeaderParameter metadata object with wire name alias and constraints."""

    def __init__(self, *, name: str, min_length: int) -> None:
        self.name = name
        self.min_length = min_length


def _build_sample_route() -> RouteMetadata:
    extractor = TypeExtractor()

    async def create_order_handler(
        tenant_id: FromPath[str],
        page: Annotated[int, FromQuery[int], Parameter(query="page", ge=1, le=500)],
        trace_token: FromHeader[str],
        session_key: FromCookie[str],
        payload: JSONBody[CreateOrderInput],
        raw_audit: SkipValidation[str],
        hidden_token: Annotated[str, Parameter(include_in_schema=False)],
        db_service: NamedDependency[InternalService],
    ) -> OrderRecord:
        _ = (tenant_id, page, trace_token, session_key, payload, raw_audit, hidden_token, db_service)
        return OrderRecord(order_id="ord-1", sku="sku-1", quantity=1)

    params, return_type = extractor.extract(create_order_handler)
    return RouteMetadata(
        operation_id="post.tenants.by_tenant_id.orders",
        handler_fn=create_order_handler,
        source_file=Path("orders.py"),
        params=params,
        return_type=return_type,
        path="/tenants/{tenant_id:str}/orders",
        method="POST",
        handler_name="create_order_handler",
    )


def _build_msgpack_route() -> RouteMetadata:
    extractor = TypeExtractor()

    async def ingest_binary_handler(
        batch: MsgPackBody[CreateOrderInput],
        legacy_filter: Annotated[str, Parameter(query="filter", min_length=2, max_length=32, pattern="^[a-z]+$")],
        legacy_body_fallback: Annotated[str, Parameter(include_in_schema=False)] = "",
    ) -> OrderRecord:
        _ = (batch, legacy_filter, legacy_body_fallback)
        return OrderRecord(order_id="ord-2", sku="sku-2", quantity=2)

    params, return_type = extractor.extract(ingest_binary_handler)
    return RouteMetadata(
        operation_id="post.ingest",
        handler_fn=ingest_binary_handler,
        source_file=Path("orders.py"),
        params=params,
        return_type=return_type,
        path="/ingest",
        method="POST",
        handler_name="ingest_binary_handler",
    )


def test_valibot_and_zod_generators_respect_litestar_v2_and_v3_markers() -> None:
    """Verify ValibotGenerator and ZodGenerator emit constraints and exclude NamedDependency and hidden params."""
    routes = [_build_sample_route(), _build_msgpack_route()]

    valibot_code = ValibotGenerator().generate(routes)
    assert "import * as v from 'valibot';" in valibot_code
    assert "export const CreateOrderInputSchema = v.object({" in valibot_code
    assert "export type CreateOrderInput = v.InferOutput<typeof CreateOrderInputSchema>;" in valibot_code
    assert "v.pipe(v.number(), v.minValue(1), v.maxValue(500))" in valibot_code
    assert "filter: v.pipe(v.string(), v.minLength(2), v.maxLength(32), v.regex(/^[a-z]+$/))" in valibot_code
    assert "db_service" not in valibot_code
    assert "hidden_token" not in valibot_code
    assert "raw_audit: v.string()" in valibot_code

    zod_code = ZodGenerator().generate(routes)
    assert 'import { z } from "zod";' in zod_code
    assert "export const CreateOrderInputSchema = z.object({" in zod_code
    assert "z.number().gte(1).lte(500)" in zod_code
    assert "filter: z.string().min(2).max(32).regex(/^[a-z]+$/)" in zod_code
    assert "db_service" not in zod_code
    assert "hidden_token" not in zod_code


def test_openapi_310_generator_supports_litestar_v2_and_v3_markers() -> None:
    """Verify OpenAPIGenerator emits OpenAPI 3.1.0 with path/query/header/cookie params and JSON/MsgPack bodies."""
    routes = [_build_sample_route(), _build_msgpack_route()]
    doc = OpenAPIGenerator().generate(routes, title="Commerce API", version="2.1.0")

    assert doc["openapi"] == "3.1.0"
    assert doc["info"] == {"title": "Commerce API", "version": "2.1.0"}

    paths = doc["paths"]
    assert isinstance(paths, dict)
    order_op = paths["/tenants/{tenant_id}/orders"]["post"]
    param_map = {p["name"]: p for p in order_op["parameters"]}

    assert param_map["tenant_id"]["in"] == "path"
    assert param_map["page"]["in"] == "query"
    assert param_map["page"]["schema"]["minimum"] == 1
    assert param_map["page"]["schema"]["maximum"] == 500
    assert param_map["trace_token"]["in"] == "header"
    assert param_map["session_key"]["in"] == "cookie"
    assert param_map["raw_audit"]["in"] == "query"
    assert "db_service" not in param_map
    assert "hidden_token" not in param_map
    assert "payload" not in param_map

    req_content = order_op["requestBody"]["content"]
    assert req_content["application/json"]["schema"] == {"$ref": "#/components/schemas/CreateOrderInput"}

    ingest_op = paths["/ingest"]["post"]
    ingest_content = ingest_op["requestBody"]["content"]
    assert ingest_content["application/x-msgpack"]["schema"] == {"$ref": "#/components/schemas/CreateOrderInput"}

    v2_body_extractor = TypeExtractor()

    async def v2_upload_handler(
        data: Annotated[CreateOrderInput, Body(media_type="application/x-msgpack")],
    ) -> OrderRecord:
        _ = data
        return OrderRecord(order_id="ord-3", sku="sku-3", quantity=3)

    v2_params, v2_ret = v2_body_extractor.extract(v2_upload_handler)
    v2_route = RouteMetadata(
        operation_id="post.v2_upload",
        handler_fn=v2_upload_handler,
        source_file=Path("orders.py"),
        params=v2_params,
        return_type=v2_ret,
        path="/v2-upload",
        method="POST",
        handler_name="v2_upload_handler",
    )
    v2_doc = OpenAPIGenerator().generate([v2_route])
    assert v2_doc["paths"]["/v2-upload"]["post"]["requestBody"]["content"]["application/x-msgpack"]["schema"] == {
        "$ref": "#/components/schemas/CreateOrderInput"
    }

    async def form_handler(
        form_data: MultipartBody[CreateOrderInput],
        url_form: URLEncodedBody[CreateOrderInput],
        correlation_id: Annotated[str, HeaderParameter(name="X-Correlation-Id", min_length=8)],
    ) -> OrderRecord:
        _ = (form_data, url_form, correlation_id)
        return OrderRecord(order_id="ord-4", sku="sku-4", quantity=4)

    form_params, form_ret = v2_body_extractor.extract(form_handler)
    form_route = RouteMetadata(
        operation_id="post.form_upload",
        handler_fn=form_handler,
        source_file=Path("orders.py"),
        params=form_params,
        return_type=form_ret,
        path="/form-upload",
        method="POST",
        handler_name="form_handler",
    )
    form_doc = OpenAPIGenerator().generate([form_route])
    form_op = form_doc["paths"]["/form-upload"]["post"]
    assert "multipart/form-data" in form_op["requestBody"]["content"]
    assert "application/x-www-form-urlencoded" in form_op["requestBody"]["content"]
    header_param = form_op["parameters"][0]
    assert header_param["name"] == "X-Correlation-Id"
    assert header_param["in"] == "header"
    assert header_param["schema"]["minLength"] == 8


def test_protobuf_v3_generator() -> None:
    """Verify ProtobufGenerator renders proto3 messages and service RPC definitions."""
    routes = [_build_sample_route()]
    proto_text = ProtobufGenerator(package="commerce.v1", service_name="OrderService").generate(routes)
    assert 'syntax = "proto3";' in proto_text
    assert "package commerce.v1;" in proto_text
    assert "message CreateOrderInput {" in proto_text
    assert "message OrderRecord {" in proto_text
    assert "service OrderService {" in proto_text
    assert "rpc CreateOrderHandler (" in proto_text
    assert "db_service" not in proto_text
    assert "hidden_token" not in proto_text


def test_procedure_decorator_registry_and_typescript_client_codegen() -> None:
    """Verify @procedure registration, Litestar dependency filtering, and TypeScript RPC client generation."""
    registry = ProcedureRegistry()
    registry.clear()

    @procedure(name="orders.create", mcp=True, grpc=True)
    async def create_order_rpc(
        input_data: JSONBody[CreateOrderInput],
        trace_id: FromHeader[str],
        injected_svc: NamedDependency[InternalService],
        internal_flag: Annotated[str, Parameter(include_in_schema=False)] = "internal",
    ) -> OrderRecord:
        _ = (trace_id, injected_svc, internal_flag)
        return OrderRecord(order_id="ord-100", sku=input_data.sku, quantity=input_data.quantity)

    assert "orders.create" in registry
    info = registry.get("orders.create")
    assert info is not None
    assert info.mcp_enabled is True
    assert info.grpc_enabled is True
    assert "input_data" in info.params
    assert "trace_id" in info.params
    assert "injected_svc" not in info.params
    assert "internal_flag" not in info.params

    ts_client = ProcedureClientGenerator(registry).generate_typescript_client()
    assert "export interface CreateOrderInput {" in ts_client
    assert "export interface OrderRecord {" in ts_client
    assert (
        '"orders.create": (params: { input_data: CreateOrderInput; trace_id: string }) => Promise<OrderRecord>;'
        in ts_client
    )
    assert "injected_svc" not in ts_client
    assert "internal_flag" not in ts_client

    plugin = VertebraCodegenPlugin(CodegenConfig(enable_valibot=True, enable_openapi=True, enable_protobuf=True))
    app_state: dict[str, object] = {}
    plugin.install_state(app_state)
    assert "codegen" in app_state
