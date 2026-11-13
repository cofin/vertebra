"""Fused direct FastAPI APIRoute execution, CompiledDependantPlan solver, and zero-copy DTO bridge."""

from __future__ import annotations

import contextlib
import inspect
from collections.abc import Callable
from types import SimpleNamespace
from typing import Any, Self, get_type_hints
from urllib.parse import parse_qsl

import anyio.to_thread

from vertebra.extensions.fastapi.dependencies import (
    CompiledDependantPlan,
    compile_dependant_plan,
    execute_dependant_plan,
)
from vertebra.extensions.fastapi.offload import has_non_offloadable_middleware
from vertebra.extensions.fastapi.router import (
    RouterTable,
    extract_routes,
)
from vertebra.extensions.starlette.fastpath import (
    BUFFERED_RESPONSE_TYPES,
    get_starlette_request_cls,
    preseed_request_body,
    serialize_buffered_response,
)
from vertebra.server._fastpath import (
    extract_scope_body_fast,
    resolve_relative_path,
    serialize_fastpath_special_result,
    wrap_asgi_fastpath,
)
from vertebra.server._fastpath_core import (
    UNSET_DEFAULT,
    CompiledRouteBase,
    has_custom_exception_handlers,
    scalar_query_converter,
    serialize_starlette_http_exception,
)
from vertebra.utils.serializers import decode_json, encode_json_bytes
from vertebra.utils.type_guards import (
    is_coroutine_callable,
    is_msgspec_struct_type,
    is_pydantic_model_type,
)

__all__ = (
    "BUFFERED_RESPONSE_TYPES",
    "CompiledRoute",
    "FastPath",
    "compile_fastpath_routes",
    "dispatch_handler_direct",
)


class CompiledRoute(CompiledRouteBase, frozen=True, gc=False, kw_only=True):
    """Pre-compiled per-route dispatch metadata for zero-jsonable_encoder FastAPI execution."""

    method: str
    path_template: str
    route: Any
    endpoint: Any
    has_middleware: bool
    has_custom_exception_handlers: bool
    dependant_plan: CompiledDependantPlan | None
    request_param_name: str | None
    path_param_names: tuple[str, ...]
    query_params: tuple[tuple[str, Callable[[str], Any], Any], ...]
    body_param_name: str | None
    body_type: Any
    body_is_msgspec: bool
    body_is_pydantic: bool
    response_serializer: Callable[[Any], bytes] | None
    sync_to_thread: bool = False

    @property
    def needs_request(self) -> bool:
        """Return True when the endpoint or any sub-dependency accepts a Request parameter."""
        if self.request_param_name is not None:
            return True
        return bool(self.dependant_plan is not None and self.dependant_plan.needs_request)

    @property
    def is_bypass_eligible(self) -> bool:
        """Return True when outer middleware and custom exception layers can be bypassed safely."""
        if self.has_middleware or self.has_custom_exception_handlers:
            return False
        return not (self.dependant_plan is not None and self.dependant_plan.has_security_requirements)


class FastPath:
    """Fast-path bypass router dispatching directly to compiled FastAPI APIRoutes."""

    __slots__ = ("_app", "_compiled_routes", "_mount_prefix", "_router_table", "_sync_to_thread")

    def __init__(
        self,
        router_table: Any = None,
        *,
        app: Any = None,
        compiled_routes: dict[int, CompiledRoute] | None = None,
        mount_prefix: str = "",
        sync_to_thread: bool = False,
    ) -> None:
        resolved_app = app
        resolved_table = router_table
        if (
            resolved_app is None
            and resolved_table is not None
            and not isinstance(resolved_table, RouterTable)
            and not hasattr(resolved_table, "match_with_params")
            and hasattr(resolved_table, "routes")
        ):
            resolved_app = resolved_table
            resolved_table = extract_routes(resolved_app)

        self._app = resolved_app
        self._mount_prefix = mount_prefix
        self._sync_to_thread = sync_to_thread
        self._router_table: RouterTable | None = None
        self._compiled_routes: dict[int, CompiledRoute] = {}
        self.bind_app(resolved_app, router_table=resolved_table, compiled_routes=compiled_routes)

    @property
    def router_table(self) -> RouterTable | None:
        """Return the extracted RouterTable."""
        return self._router_table

    @property
    def compiled_routes(self) -> dict[int, CompiledRoute]:
        """Return the compiled route lookup table."""
        return self._compiled_routes

    @classmethod
    def from_app(cls, app: Any, *, mount_prefix: str = "", sync_to_thread: bool = False) -> Self:
        """Construct a bound FastPath directly from a FastAPI application."""
        table = extract_routes(app)
        compiled = compile_fastpath_routes(app, router_table=table)
        return cls(
            table,
            app=app,
            compiled_routes=compiled,
            mount_prefix=mount_prefix,
            sync_to_thread=sync_to_thread,
        )

    def bind_app(
        self,
        app: Any,
        *,
        router_table: Any = None,
        compiled_routes: dict[int, CompiledRoute] | None = None,
    ) -> None:
        """Bind or refresh the target FastAPI application, router table, and compiled routes."""
        if app is not None:
            self._app = app
        if router_table is not None:
            self._router_table = router_table
        elif self._router_table is None and self._app is not None:
            self._router_table = extract_routes(self._app)

        if compiled_routes is not None:
            self._compiled_routes = dict(compiled_routes)
        elif self._router_table is not None:
            self._compiled_routes = _compile_routes_from_table(self._router_table, self._app)

    def wrap_asgi(self, fallback_app: Callable[..., Any]) -> Callable[..., Any]:
        """Wrap an ASGI 3.0 callable so eligible HTTP requests execute via direct FastAPI fastpath."""
        return wrap_asgi_fastpath(self, fallback_app)

    async def dispatch(
        self,
        scope: Any,
        receive: Any,
        send: Any,
        *,
        body: bytes | None = None,
    ) -> tuple[int, list[tuple[bytes, bytes]], bytes | memoryview] | None:
        """Attempt to dispatch the request directly via the compiled FastAPI fastpath."""
        table = self._router_table
        if table is None or not table.routes:
            return None

        scope_type = scope.get("type", "http") if hasattr(scope, "get") else scope["type"]
        if scope_type != "http":
            return None

        method = str(scope.get("method", "GET") if hasattr(scope, "get") else scope["method"]).upper()
        path = resolve_relative_path(scope, self._mount_prefix)

        try:
            matched = table.match_with_params(method, path)
        except (ValueError, TypeError):
            return None
        if matched is None:
            return None

        entry, path_params = matched
        compiled = self._compiled_routes.get(entry.handler_id)
        if compiled is None or not compiled.is_bypass_eligible:
            return None

        if self._app is not None:
            scope["app"] = self._app
            router_obj = getattr(self._app, "router", None)
            if router_obj is not None:
                scope["router"] = router_obj
        scope["path_params"] = path_params
        scope["endpoint"] = compiled.endpoint
        scope["route"] = compiled.route

        resolved_body = await extract_scope_body_fast(scope, receive, body, method=method)
        overrides: dict[Callable[..., Any], Callable[..., Any]] | None = getattr(
            self._app, "dependency_overrides", None
        )
        try:
            return await dispatch_handler_direct(
                compiled,
                scope=scope,
                path_params=path_params,
                body=resolved_body,
                receive=receive,
                send=send,
                dependency_overrides=overrides,
                sync_to_thread=self._sync_to_thread,
            )
        except Exception as exc:
            http_exc_resp = serialize_starlette_http_exception(exc)
            if http_exc_resp is not None:
                return http_exc_resp
            raise


def compile_fastpath_routes(
    app: Any,
    *,
    router_table: RouterTable | None = None,
) -> dict[int, CompiledRoute]:
    """Compile all FastAPI routes into CompiledRoute descriptors keyed by handler_id."""
    table = router_table if router_table is not None else extract_routes(app)
    return _compile_routes_from_table(table, app)


async def dispatch_handler_direct(
    compiled: CompiledRoute,
    *,
    scope: Any,
    path_params: dict[str, Any],
    body: bytes,
    receive: Any,
    send: Any,
    dependency_overrides: dict[Callable[..., Any], Callable[..., Any]] | None = None,
    sync_to_thread: bool = False,
) -> tuple[int, list[tuple[bytes, bytes]], bytes | memoryview] | None:
    """Execute a FastAPI APIRoute with zero-AsyncExitStack Depends() and zero-copy DTO decoding."""
    request: Any = None
    if compiled.needs_request:
        req_cls = get_starlette_request_cls()
        request = req_cls(scope, receive=receive, send=send)
        preseed_request_body(scope, request, body)

    native_req: Any = None
    req_fn = getattr(scope, "request", None)
    if callable(req_fn):
        native_req = req_fn()
        scope["vertebra_request"] = native_req

    raw_qs = scope.get("query_string", b"") if hasattr(scope, "get") else b""
    qs_text = raw_qs.decode("latin-1") if isinstance(raw_qs, bytes) else str(raw_qs)
    query_map: dict[str, str] = dict(parse_qsl(qs_text, keep_blank_values=True)) if qs_text else {}

    kwargs: dict[str, Any] = {}
    if compiled.request_param_name is not None:
        kwargs[compiled.request_param_name] = request
    for p_name in compiled.path_param_names:
        if p_name in path_params:
            kwargs[p_name] = path_params[p_name]
    for q_name, converter, default_val in compiled.query_params:
        if q_name in query_map:
            kwargs[q_name] = converter(query_map[q_name])
        elif default_val is not UNSET_DEFAULT:
            kwargs[q_name] = default_val

    if compiled.body_param_name is not None:
        kwargs[compiled.body_param_name] = _decode_body_payload(
            compiled,
            native_req=native_req,
            body=body,
        )

    plan = compiled.dependant_plan
    if plan is not None and plan.nodes:
        if plan.has_generator_dependencies:
            async with contextlib.AsyncExitStack() as stack:
                dep_kwargs = await execute_dependant_plan(
                    plan,
                    request=request,
                    path_params=path_params,
                    query_params=query_map,
                    dependency_overrides=dependency_overrides,
                    exit_stack=stack,
                )
                kwargs.update(dep_kwargs)
                result = await _invoke_endpoint(compiled, kwargs, sync_to_thread=sync_to_thread)
        else:
            dep_kwargs = await execute_dependant_plan(
                plan,
                request=request,
                path_params=path_params,
                query_params=query_map,
                dependency_overrides=dependency_overrides,
                exit_stack=None,
            )
            kwargs.update(dep_kwargs)
            result = await _invoke_endpoint(compiled, kwargs, sync_to_thread=sync_to_thread)
    else:
        result = await _invoke_endpoint(compiled, kwargs, sync_to_thread=sync_to_thread)

    return await _serialize_fastapi_result(
        compiled,
        result,
        scope=scope,
        receive=receive,
        send=send,
    )


async def _invoke_endpoint(
    compiled: CompiledRoute,
    kwargs: dict[str, Any],
    *,
    sync_to_thread: bool = False,
) -> Any:
    if compiled.is_async:
        return await compiled.endpoint(**kwargs)
    if compiled.sync_to_thread or sync_to_thread:
        return await anyio.to_thread.run_sync(lambda: compiled.endpoint(**kwargs))
    return compiled.endpoint(**kwargs)


def _decode_body_payload(
    compiled: CompiledRoute,
    *,
    native_req: Any,
    body: bytes,
) -> Any:
    body_type = compiled.body_type
    if compiled.body_is_msgspec:
        if native_req is not None and len(body) > 0:
            try:
                return native_req.decode_msgspec(body_type)
            except Exception:
                pass
        return decode_json(body, type=body_type)

    if compiled.body_is_pydantic:
        if native_req is not None and len(body) > 0:
            try:
                return native_req.decode_pydantic_json(body_type)
            except Exception:
                pass
        validator = getattr(body_type, "__pydantic_validator__", None)
        if validator is not None:
            return validator.validate_json(body)

    return decode_json(body)


async def _serialize_fastapi_result(
    compiled: CompiledRoute,
    result: Any,
    *,
    scope: Any,
    receive: Any,
    send: Any,
) -> tuple[int, list[tuple[bytes, bytes]], bytes | memoryview] | None:
    if result is None:
        return compiled.status_code, [], b""

    special = serialize_fastpath_special_result(result, default_status=compiled.status_code)
    if special is not None:
        return special

    buffered = serialize_buffered_response(result, default_status=compiled.status_code)
    if buffered is not None:
        return buffered

    if callable(result) and not isinstance(result, type):
        await result(scope, receive, send)
        return None

    if compiled.response_serializer is not None:
        payload = compiled.response_serializer(result)
    elif hasattr(result, "__pydantic_serializer__"):
        payload = bytes(result.__pydantic_serializer__.to_json(result))
    else:
        payload = bytes(encode_json_bytes(result))

    return (
        compiled.status_code,
        [
            (b"content-type", b"application/json"),
            (b"content-length", str(len(payload)).encode("ascii")),
        ],
        payload,
    )


def _build_response_serializer(response_field: Any, return_annotation: Any) -> Callable[[Any], bytes] | None:
    adapter = getattr(response_field, "_type_adapter", None) if response_field is not None else None
    serializer = getattr(adapter, "serializer", None) if adapter is not None else None
    to_json_fn = getattr(serializer, "to_json", None) if serializer is not None else None
    if callable(to_json_fn):

        def _serialize_adapter(val: Any, _fn: Callable[[Any], Any] = to_json_fn) -> bytes:
            return bytes(_fn(val))

        return _serialize_adapter

    pydantic_ser = getattr(return_annotation, "__pydantic_serializer__", None)
    to_json_direct = getattr(pydantic_ser, "to_json", None) if pydantic_ser is not None else None
    if callable(to_json_direct):

        def _serialize_direct(val: Any, _fn: Callable[[Any], Any] = to_json_direct) -> bytes:
            return bytes(_fn(val))

        return _serialize_direct
    return None


def _compile_routes_from_table(
    router_table: RouterTable,
    app: Any,
) -> dict[int, CompiledRoute]:
    has_mw = has_non_offloadable_middleware(app) if app is not None else False
    has_exc = (
        has_custom_exception_handlers(getattr(app, "exception_handlers", None), ("starlette.", "fastapi."))
        if app is not None
        else False
    )
    compiled: dict[int, CompiledRoute] = {}

    for handler_id, entry in router_table.routes.items():
        endpoint = entry.endpoint
        is_async = is_coroutine_callable(endpoint)
        known_path_names = set(entry.param_types.keys())
        dep_plan = compile_dependant_plan(entry.dependant, path_param_names=known_path_names)
        dep_param_names = {name for name, _ in dep_plan.root_dep_params} if dep_plan is not None else set()

        try:
            sig = inspect.signature(endpoint)
        except (TypeError, ValueError):
            sig = None
        try:
            hints = get_type_hints(endpoint, include_extras=True)
        except Exception:
            hints = {}

        req_param: str | None = None
        path_param_names: list[str] = []
        query_params: list[tuple[str, Callable[[str], Any], Any]] = []
        body_param_name: str | None = None
        body_type: Any = Any

        if entry.body_field is not None:
            bf_name = getattr(entry.body_field, "name", None)
            bf_type = getattr(entry.body_field, "type_", None) or getattr(
                getattr(entry.body_field, "field_info", None), "annotation", None
            )
            if isinstance(bf_name, str):
                body_param_name = bf_name
                body_type = bf_type if bf_type is not None else Any

        if sig is not None:
            for p_name, param in sig.parameters.items():
                if p_name in dep_param_names:
                    continue
                ann = hints.get(p_name, param.annotation)
                default_obj = param.default
                if getattr(default_obj, "dependency", None) is not None and callable(default_obj.dependency):
                    if dep_plan is None or not dep_plan.nodes:
                        synthetic_dep = _build_synthetic_dependant(sig, hints)
                        dep_plan = compile_dependant_plan(synthetic_dep, path_param_names=known_path_names)
                        dep_param_names = (
                            {name for name, _ in dep_plan.root_dep_params} if dep_plan is not None else set()
                        )
                    continue
                if p_name == "request" or getattr(ann, "__name__", "") == "Request":
                    req_param = p_name
                elif p_name in known_path_names:
                    path_param_names.append(p_name)
                elif (
                    p_name == body_param_name
                    or is_msgspec_struct_type(ann)
                    or is_pydantic_model_type(ann)
                    or p_name in {"body", "payload", "data"}
                    and entry.method in {"POST", "PUT", "PATCH"}
                ):
                    body_param_name = p_name
                    body_type = ann
                else:
                    default_val = UNSET_DEFAULT if param.default is inspect.Parameter.empty else param.default
                    query_params.append((p_name, scalar_query_converter(ann), default_val))

        ret_ann = hints.get("return", sig.return_annotation if sig is not None else Any)
        resp_serializer = _build_response_serializer(entry.response_field, ret_ann)

        compiled[handler_id] = CompiledRoute(
            handler_id=handler_id,
            method=entry.method,
            path_template=entry.path_template,
            route=entry.route,
            endpoint=endpoint,
            is_async=is_async,
            status_code=entry.status_code,
            has_middleware=has_mw,
            has_custom_exception_handlers=has_exc,
            dependant_plan=dep_plan,
            request_param_name=req_param,
            path_param_names=tuple(path_param_names),
            query_params=tuple(query_params),
            body_param_name=body_param_name,
            body_type=body_type,
            body_is_msgspec=is_msgspec_struct_type(body_type),
            body_is_pydantic=is_pydantic_model_type(body_type),
            response_serializer=resp_serializer,
            sync_to_thread=bool(getattr(endpoint, "__vertebra_sync_to_thread__", False)),
        )
    return compiled


def _build_synthetic_dependant(sig: inspect.Signature, hints: dict[str, Any]) -> Any:
    """Construct a lightweight Dependant tree from inspect.Signature default Depends() markers."""
    children: list[Any] = []
    for p_name, param in sig.parameters.items():
        dep_marker = param.default
        dep_call = getattr(dep_marker, "dependency", None)
        if not callable(dep_call):
            ann = hints.get(p_name, param.annotation)
            if callable(dep_marker) and hasattr(dep_marker, "use_cache"):
                dep_call = ann if callable(ann) else None
        if callable(dep_call):
            use_cache = bool(getattr(dep_marker, "use_cache", True))
            try:
                sub_sig = inspect.signature(dep_call)
                sub_hints = get_type_hints(dep_call, include_extras=True)
                sub_dep = _build_synthetic_dependant(sub_sig, sub_hints)
                sub_children = sub_dep.dependencies
            except Exception:
                sub_children = ()
            children.append(
                SimpleNamespace(
                    name=p_name,
                    call=dep_call,
                    cache_key=(dep_call, ()),
                    use_cache=use_cache,
                    dependencies=sub_children,
                    security_requirements=(),
                )
            )
    return SimpleNamespace(dependencies=tuple(children), security_requirements=())
