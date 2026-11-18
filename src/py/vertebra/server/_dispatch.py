"""Vertebra request batch partitioning, pre-compiled parameter/DI extraction, and dispatch execution."""

from __future__ import annotations

import asyncio
import concurrent.futures
import contextlib
import dataclasses
import inspect
import logging
import os
import threading
from collections.abc import AsyncIterable, Callable, Coroutine, Iterable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any, cast, get_args, get_origin
from urllib.parse import parse_qsl
from uuid import UUID

import msgspec

from vertebra._vertebra import (
    STREAM_FLAG_ERROR,
    STREAM_FLAG_FINAL,
    STREAM_FLAG_HEADERS_SENT,
    STREAM_FLAG_STREAMING,
    Response,
    drive_native_batch,
    submit_batch_raw,
    submit_response,
    submit_response_raw,
    submit_stream_frame,
)
from vertebra._vertebra import (
    ArrowCursor as RustArrowCursor,
)
from vertebra.db.arrow import ArrowCursor
from vertebra.framework._router import (
    ParamSpec,
    Route,
    _extract_params_from_segments,
    _parse_bool,
)
from vertebra.framework.di import (
    Container,
    ParamBinding,
    inspect_callable_bindings,
)
from vertebra.framework.security import (
    Guard,
    PermissionDeniedError,
    Principal,
    SecurityContext,
)
from vertebra.server._fastpath import serialize_fastpath_special_result
from vertebra.storage._streaming import StreamingStorageResponse
from vertebra.typing import ImproperConfigurationError
from vertebra.utils.serializers import (
    ZeroCopyCodecRegistry,
    decode_json,
    decode_msgpack,
    encode_json_bytes,
)

__all__ = (
    "BatchDispatchPlan",
    "HandlerSpec",
    "_DEFAULT_ASGI_BATCH_TIMEOUT_SECONDS",
    "_JSON_CONTENT_TYPE_COMPACT",
    "_MOUNT_HANDLER_MASK",
    "_MOUNT_INDEX_MASK",
    "_RAW_BUFFER_TYPES",
    "_call_handler",
    "_handler_meta",
    "_is_potential_special_return",
    "_resolve_asgi_batch_timeout",
    "_resolve_handler_args",
    "_submit_coerced",
    "_try_submit_native_special_return",
    "build_error_response",
    "compile_handler_spec",
    "compile_handler_table",
    "dispatch_batch",
    "get_worker_executor",
    "partition_batch",
    "shutdown_worker_executor",
)

log = logging.getLogger(__name__)

_DEFAULT_ASGI_BATCH_TIMEOUT_SECONDS: float = 30.0
_UNSET_BATCH_TIMEOUT = object()
_EXECUTOR_LOCK = threading.Lock()
_WORKER_EXECUTOR: ThreadPoolExecutor | None = None
_REQUEST_PARAMS = frozenset({"request", "req"})
_JSON_CONTENT_TYPE_COMPACT = b"\x0ccontent-type\x10\x00application/json"
_RAW_BUFFER_TYPES = (bytes, bytearray, memoryview)
_MOUNT_HANDLER_MASK = 0xFFFF_0000_0000_0000
_MOUNT_INDEX_MASK = 0xFFFF
_SCALAR_QUERY_TYPES: dict[type[Any], Callable[[str], Any]] = {
    str: str,
    int: int,
    float: float,
    bool: _parse_bool,
    UUID: UUID,
    Decimal: Decimal,
    date: date.fromisoformat,
    datetime: datetime.fromisoformat,
    time: time.fromisoformat,
}


class _AsyncErrorStub(msgspec.Struct, frozen=True, gc=False):
    """Lightweight request identifier carrier avoiding BatchSnapshot retention across await."""

    request_id: int


def _extract_request(request: Any) -> Any:
    """Return the raw request object."""
    return request


def _extract_body(request: Any) -> bytes:
    """Extract body bytes from the request."""
    body = getattr(request, "body", b"")
    return body if isinstance(body, bytes) else bytes(body or b"")


def _extract_headers(request: Any) -> Any:
    """Extract headers from the request."""
    return getattr(request, "headers", None)


def _extract_method(request: Any) -> Any:
    """Extract HTTP method from the request."""
    return getattr(request, "method", "GET")


def _extract_path(request: Any) -> Any:
    """Extract URL path from the request."""
    return getattr(request, "path", "/")


def _extract_query(request: Any) -> Any:
    """Extract query string from the request."""
    return getattr(request, "query", None)


_PARAM_EXTRACTORS: dict[str, Callable[[Any], Any]] = {
    "request": _extract_request,
    "req": _extract_request,
    "body": _extract_body,
    "headers": _extract_headers,
    "method": _extract_method,
    "path": _extract_path,
    "query": _extract_query,
}


class _CompiledParamPlan(msgspec.Struct, frozen=True, gc=False):
    """Pre-compiled extraction plan for a single handler parameter."""

    name: str
    kind: str
    extractor: Callable[[Any, dict[str, Any], dict[str, str]], Any] | None = None
    di_key: str | None = None
    is_async_di: bool = False
    is_gen_di: bool = False


class HandlerSpec(msgspec.Struct, frozen=True, gc=False, cache_hash=True):
    """Pre-compiled dispatch specification and invoker for a route handler."""

    has_params: bool
    """Whether the route handler declares one or more parameters."""
    is_async: bool
    """Whether execution requires awaiting a coroutine (async handler, async DI, or async guard)."""
    param_names: tuple[str, ...]
    """Ordered tuple of parameter names declared on the handler signature."""
    invoker: Callable[..., Any]
    """Pre-compiled single-argument callable `invoker(request)` executing guards, DI, and the handler."""
    sync_to_thread: bool = False
    """When True, offload synchronous handler execution onto the worker thread pool."""
    status_code: int = 200
    """Default HTTP response status code configured on the route."""
    arg_resolver: Callable[[Any], tuple[Any, ...]] | None = None
    """Optional pre-compiled callable resolving positional arguments from a `Request`."""


def compile_handler_spec(
    handler: Callable[..., Any],
    *,
    sync_to_thread: bool | None = None,
    route: Route | None = None,
    container: Container | None = None,
) -> HandlerSpec:
    """Compile parameter extractors, DI plan, guards, and invocation strategy for a handler."""
    bound_route = route or getattr(handler, "__vertebra_bound_route__", None)
    if bound_route is None:
        attached_routes: Sequence[Route] = getattr(handler, "__vertebra_routes__", ())
        if len(attached_routes) == 1:
            bound_route = attached_routes[0]

    bound_container: Container | None = container or getattr(handler, "__vertebra_container__", None)
    if bound_container is None and bound_route is not None and bound_route.dependencies:
        bound_container = Container(bound_route.dependencies)

    guards: tuple[Callable[..., Any], ...] = bound_route.guards if bound_route is not None else ()
    segments: tuple[str | ParamSpec, ...] = bound_route.segments if bound_route is not None else ()
    status_code: int = bound_route.status_code if bound_route is not None else 200
    path_param_names = {seg.name for seg in segments if isinstance(seg, ParamSpec)}

    bindings = inspect_callable_bindings(handler)
    params = tuple(b.name for b in bindings)
    has_params = bool(params)
    handler_is_async = inspect.iscoroutinefunction(handler)
    has_async_guards = any(inspect.iscoroutinefunction(g) for g in guards)

    param_plans: list[_CompiledParamPlan] = []
    needs_path_params = False
    needs_query_map = False
    has_di_params = False
    has_async_di = False
    has_gen_di = False
    all_simple_extractors = not guards

    for binding in bindings:
        di_key: str | None = None
        if bound_container is not None:
            di_key = bound_container.graph.resolve_param_key(binding, auto_register_explicit=True)
        elif binding.is_explicit_inject:
            bound_container = Container()
            di_key = bound_container.graph.resolve_param_key(binding, auto_register_explicit=True)

        if di_key is not None and bound_container is not None and di_key in bound_container.graph.providers:
            is_sub_async = bound_container.graph.is_subgraph_async(di_key)
            is_sub_gen = bound_container.graph.is_subgraph_generator(di_key)
            has_di_params = True
            has_async_di = has_async_di or is_sub_async
            has_gen_di = has_gen_di or is_sub_gen
            all_simple_extractors = False
            param_plans.append(
                _CompiledParamPlan(
                    name=binding.name,
                    kind="di",
                    di_key=di_key,
                    is_async_di=is_sub_async,
                    is_gen_di=is_sub_gen,
                )
            )
            continue

        plan, req_path, req_query, is_simple = _compile_non_di_param(binding, path_param_names)
        needs_path_params = needs_path_params or req_path
        needs_query_map = needs_query_map or req_query
        all_simple_extractors = all_simple_extractors and is_simple
        param_plans.append(plan)

    effective_is_async = handler_is_async or has_async_di or has_async_guards
    resolved_sync_to_thread = (
        bool(getattr(handler, "__vertebra_sync_to_thread__", False)) if sync_to_thread is None else sync_to_thread
    ) and not effective_is_async

    if not has_params and not guards:

        def _invoke_no_args(_request: Any, _h: Any = handler) -> Any:
            return _h()

        return HandlerSpec(
            has_params=False,
            is_async=effective_is_async,
            param_names=(),
            invoker=_invoke_no_args,
            sync_to_thread=resolved_sync_to_thread,
            status_code=status_code,
            arg_resolver=lambda _req: (),
        )

    if len(params) == 1 and params[0] in _REQUEST_PARAMS and all_simple_extractors:
        return HandlerSpec(
            has_params=True,
            is_async=effective_is_async,
            param_names=params,
            invoker=handler,
            sync_to_thread=resolved_sync_to_thread,
            status_code=status_code,
            arg_resolver=lambda req: (req,),
        )

    if all_simple_extractors and not needs_path_params and not needs_query_map and not has_di_params:
        simple_extractors = tuple(
            p.extractor
            if p.extractor is not None
            else _build_simple_wrapper(_PARAM_EXTRACTORS.get(p.name, _extract_request))
            for p in param_plans
        )

        def _resolve_simple_args(
            request: Any,
            _ext: tuple[Callable[[Any, dict[str, Any], dict[str, str]], Any], ...] = simple_extractors,
        ) -> tuple[Any, ...]:
            return tuple(fn(request, {}, {}) for fn in _ext)

        def _invoke_with_simple_extractors(
            request: Any,
            _h: Any = handler,
            _ext: tuple[Callable[[Any, dict[str, Any], dict[str, str]], Any], ...] = simple_extractors,
        ) -> Any:
            return _h(*(fn(request, {}, {}) for fn in _ext))

        return HandlerSpec(
            has_params=has_params,
            is_async=effective_is_async,
            param_names=params,
            invoker=_invoke_with_simple_extractors,
            sync_to_thread=resolved_sync_to_thread,
            status_code=status_code,
            arg_resolver=_resolve_simple_args,
        )

    compiled_plans = tuple(param_plans)

    def _resolve_sync_args(
        request: Any,
        _plans: tuple[_CompiledParamPlan, ...] = compiled_plans,
        _segments: tuple[str | ParamSpec, ...] = segments,
        _needs_path: bool = needs_path_params,
        _needs_query: bool = needs_query_map,
        _container: Container | None = bound_container,
    ) -> tuple[Any, ...]:
        path_map = _extract_params_from_segments(_segments, getattr(request, "path", "/")) if _needs_path else {}
        query_map = _parse_query_dict(getattr(request, "query", None)) if _needs_query else {}
        req_cache: dict[str, Any] = {}
        args: list[Any] = []
        for plan in _plans:
            if plan.kind == "di" and _container is not None and plan.di_key is not None:
                args.append(
                    _container.resolve_sync(
                        plan.di_key,
                        request=request,
                        request_cache=req_cache,
                        extra_context=path_map,
                    )
                )
            elif plan.extractor is not None:
                args.append(plan.extractor(request, path_map, query_map))
            else:
                args.append(request)
        return tuple(args)

    if not effective_is_async:

        def _invoke_compiled_sync(
            request: Any,
            _h: Any = handler,
            _plans: tuple[_CompiledParamPlan, ...] = compiled_plans,
            _segments: tuple[str | ParamSpec, ...] = segments,
            _needs_path: bool = needs_path_params,
            _needs_query: bool = needs_query_map,
            _guards: tuple[Callable[..., Any], ...] = guards,
            _container: Container | None = bound_container,
            _has_gen: bool = has_gen_di,
        ) -> Any:
            path_map = _extract_params_from_segments(_segments, getattr(request, "path", "/")) if _needs_path else {}
            query_map = _parse_query_dict(getattr(request, "query", None)) if _needs_query else {}
            if _guards:
                _enforce_guards_sync(_guards, request, _container, path_map)
            if _has_gen and _container is not None:
                with _container.scope(request=request, extra_context=path_map) as req_scope:
                    call_args = [
                        req_scope.resolve_sync(p.di_key)
                        if (p.kind == "di" and p.di_key is not None)
                        else (p.extractor(request, path_map, query_map) if p.extractor is not None else request)
                        for p in _plans
                    ]
                    return _h(*call_args)

            req_cache: dict[str, Any] = {}
            call_args = [
                _container.resolve_sync(
                    p.di_key,
                    request=request,
                    request_cache=req_cache,
                    extra_context=path_map,
                )
                if (p.kind == "di" and _container is not None and p.di_key is not None)
                else (p.extractor(request, path_map, query_map) if p.extractor is not None else request)
                for p in _plans
            ]
            return _h(*call_args)

        return HandlerSpec(
            has_params=has_params,
            is_async=False,
            param_names=params,
            invoker=_invoke_compiled_sync,
            sync_to_thread=resolved_sync_to_thread,
            status_code=status_code,
            arg_resolver=_resolve_sync_args,
        )

    if handler_is_async and not has_async_di and not has_async_guards and not has_gen_di:

        def _invoke_compiled_sync_to_coro(
            request: Any,
            _h: Any = handler,
            _plans: tuple[_CompiledParamPlan, ...] = compiled_plans,
            _segments: tuple[str | ParamSpec, ...] = segments,
            _needs_path: bool = needs_path_params,
            _needs_query: bool = needs_query_map,
            _guards: tuple[Callable[..., Any], ...] = guards,
            _container: Container | None = bound_container,
            _has_di: bool = has_di_params,
        ) -> Any:
            path_map = _extract_params_from_segments(_segments, getattr(request, "path", "/")) if _needs_path else {}
            query_map = _parse_query_dict(getattr(request, "query", None)) if _needs_query else {}
            if _guards:
                _enforce_guards_sync(_guards, request, _container, path_map)
            if _has_di and _container is not None:
                req_cache: dict[str, Any] = {}
                call_args = [
                    _container.resolve_sync(
                        p.di_key,
                        request=request,
                        request_cache=req_cache,
                        extra_context=path_map,
                    )
                    if (p.kind == "di" and p.di_key is not None)
                    else (p.extractor(request, path_map, query_map) if p.extractor is not None else request)
                    for p in _plans
                ]
                return _h(*call_args)

            call_args = [
                p.extractor(request, path_map, query_map) if p.extractor is not None else request for p in _plans
            ]
            return _h(*call_args)

        return HandlerSpec(
            has_params=has_params,
            is_async=True,
            param_names=params,
            invoker=_invoke_compiled_sync_to_coro,
            sync_to_thread=False,
            status_code=status_code,
            arg_resolver=_resolve_sync_args,
        )

    async def _invoke_compiled_async(
        request: Any,
        _h: Any = handler,
        _h_is_async: bool = handler_is_async,
        _plans: tuple[_CompiledParamPlan, ...] = compiled_plans,
        _segments: tuple[str | ParamSpec, ...] = segments,
        _needs_path: bool = needs_path_params,
        _needs_query: bool = needs_query_map,
        _guards: tuple[Callable[..., Any], ...] = guards,
        _container: Container | None = bound_container,
        _has_di: bool = has_di_params,
    ) -> Any:
        path_map = _extract_params_from_segments(_segments, getattr(request, "path", "/")) if _needs_path else {}
        query_map = _parse_query_dict(getattr(request, "query", None)) if _needs_query else {}
        if _guards:
            await _enforce_guards_async(_guards, request, _container, path_map)
        if _has_di and _container is not None:
            async with _container.scope(request=request, extra_context=path_map) as req_scope:
                call_args: list[Any] = []
                for p in _plans:
                    if p.kind == "di" and p.di_key is not None:
                        call_args.append(await req_scope.resolve(p.di_key))
                    elif p.extractor is not None:
                        call_args.append(p.extractor(request, path_map, query_map))
                    else:
                        call_args.append(request)
                result = _h(*call_args)
                return await result if _h_is_async or inspect.isawaitable(result) else result

        call_args = [p.extractor(request, path_map, query_map) if p.extractor is not None else request for p in _plans]
        result = _h(*call_args)
        return await result if _h_is_async or inspect.isawaitable(result) else result

    return HandlerSpec(
        has_params=has_params,
        is_async=True,
        param_names=params,
        invoker=_invoke_compiled_async,
        sync_to_thread=False,
        status_code=status_code,
        arg_resolver=_resolve_sync_args if not has_async_di else None,
    )


def compile_handler_table(handlers: Mapping[int, Callable[..., Any]]) -> dict[int, HandlerSpec]:
    """Pre-compile dispatch specs for all registered handler callables."""
    return {hid: (fn if isinstance(fn, HandlerSpec) else compile_handler_spec(fn)) for hid, fn in handlers.items()}


def build_error_response(
    exc: Exception,
    request: Any,
    error_handler: Callable[..., Any] | None = None,
    vr_cls: Any = Response,
) -> Any:
    """Build an error Response using the configured error handler, 403 on PermissionDeniedError, or 500."""
    request_id = getattr(request, "request_id", 0)
    if error_handler is not None:
        try:
            custom = error_handler(request, exc)
            if custom is not None:
                return _coerce_response(custom, request_id, vr_cls)
        except TypeError:
            try:
                custom = error_handler(exc, request)
                if custom is not None:
                    return _coerce_response(custom, request_id, vr_cls)
            except Exception:
                log.exception("Error handler failed for request %s", request_id)
        except Exception:
            log.exception("Error handler failed for request %s", request_id)

    status = 403 if isinstance(exc, PermissionDeniedError) else 500
    body_bytes = b"forbidden" if status == 403 else b"internal server error"
    if hasattr(vr_cls, "error"):
        try:
            return vr_cls.error(status, request_id)
        except Exception:
            pass
    return vr_cls(status=status, body=body_bytes, request_id=request_id)


def _coerce_response(raw: Any, request_id: int, vr_cls: Any, *, status_code: int = 200) -> Any:
    """Turn a handler return value into a Response object."""
    if isinstance(raw, vr_cls):
        if getattr(raw, "request_id", 0) == 0:
            raw.request_id = request_id
        return raw
    if isinstance(raw, bytes):
        body = raw
    elif isinstance(raw, bytearray | memoryview):
        if vr_cls is Response:
            return Response.from_buffer(status_code, raw, None, request_id)
        body = bytes(raw)
    elif isinstance(raw, str):
        body = raw.encode("utf-8")
    else:
        unary_tuple = _extract_unary_tuple_response(raw)
        if unary_tuple is not None:
            tup_status, tup_headers, tup_body = unary_tuple
            return vr_cls(
                status=tup_status,
                body=tup_body,
                headers=tup_headers or None,
                request_id=request_id,
            )
        stream_match = _extract_streaming_response(raw, default_status=status_code)
        if stream_match is not None:
            st_status, st_byte_headers, st_body = stream_match
            str_headers = _to_str_header_pairs(st_byte_headers)
            if isinstance(st_body, AsyncIterable):
                collected = asyncio.run(_collect_async_stream_bytes(st_body))
            else:
                collected = _collect_sync_stream_bytes(st_body)
            return vr_cls(
                status=st_status,
                body=collected,
                headers=str_headers or None,
                request_id=request_id,
            )
        special = serialize_fastpath_special_result(raw, default_status=status_code)
        if special is not None:
            sp_status, sp_headers, sp_body = special
            sp_str_headers = _to_str_header_pairs(list(sp_headers)) if sp_headers else None
            if isinstance(sp_body, memoryview):
                if vr_cls is Response:
                    return Response.from_buffer(sp_status, sp_body, sp_str_headers, request_id)
                return vr_cls(status=sp_status, body=bytes(sp_body), headers=sp_str_headers, request_id=request_id)
            return vr_cls(status=sp_status, body=sp_body, headers=sp_str_headers, request_id=request_id)
        body = encode_json_bytes(raw)
    return vr_cls(status=status_code, body=body, request_id=request_id)


def _submit_coerced(
    raw: Any,
    request_id: int,
    vr_cls: Any,
    submit: Callable[[Any], None],
    submit_raw: Callable[..., None] | None = None,
    *,
    status_code: int = 200,
    scheduler: Any = None,
    stream_submitter: Callable[..., None] | None = None,
) -> None:
    """Submit a handler response using raw fastpath, progressive stream frames, or Py<Response> fallback."""
    if submit_raw is not None:
        if isinstance(raw, _RAW_BUFFER_TYPES):
            submit_raw(request_id, status_code, raw)
            return
        if isinstance(raw, str):
            submit_raw(request_id, status_code, raw.encode("utf-8"))
            return
    if isinstance(raw, vr_cls):
        if getattr(raw, "request_id", 0) == 0:
            raw.request_id = request_id
        submit(raw)
        return

    special_outcome = _dispatch_special_return(
        raw,
        request_id,
        vr_cls,
        submit,
        status_code=status_code,
        stream_submitter=stream_submitter,
    )
    if special_outcome is True:
        return
    if inspect.iscoroutine(special_outcome):
        if scheduler is not None:
            _schedule_coros(scheduler, [special_outcome])
            return
        try:
            running_loop = asyncio.get_running_loop()
        except RuntimeError:
            asyncio.run(special_outcome)
            return
        _spawn_tracked_task(running_loop, special_outcome)
        return

    if submit_raw is not None:
        submit_raw(request_id, status_code, encode_json_bytes(raw), _JSON_CONTENT_TYPE_COMPACT)
        return

    submit(_coerce_response(raw, request_id, vr_cls, status_code=status_code))


def _invoke_sync_handler(
    spec: HandlerSpec,
    request: Any,
    error_handler: Callable[..., Any] | None = None,
    vr_cls: Any = Response,
) -> Any:
    """Invoke a synchronous handler and return a coerced Response or error Response."""
    try:
        raw = spec.invoker(request)
        return _coerce_response(raw, getattr(request, "request_id", 0), vr_cls, status_code=spec.status_code)
    except Exception as exc:
        log.exception("Sync handler error for request %s", getattr(request, "request_id", 0))
        return build_error_response(exc, request, error_handler, vr_cls)


async def _invoke_async_handler(
    spec: HandlerSpec,
    request: Any,
    error_handler: Callable[..., Any] | None = None,
    vr_cls: Any = Response,
) -> Any:
    """Invoke an asynchronous handler and return a coerced Response or error Response."""
    req_id = getattr(request, "request_id", 0)
    try:
        raw = await spec.invoker(request)
        if _is_async_storage_response(raw):
            sp_status, sp_headers, sp_body = await raw.to_async_fastpath_tuple()
            sp_str_headers = _to_str_header_pairs(list(sp_headers)) if sp_headers else None
            if isinstance(sp_body, memoryview) and vr_cls is Response:
                return Response.from_buffer(sp_status, sp_body, sp_str_headers, req_id)
            return vr_cls(
                status=sp_status,
                body=bytes(sp_body) if isinstance(sp_body, memoryview) else sp_body,
                headers=sp_str_headers,
                request_id=req_id,
            )
        stream_match = _extract_streaming_response(raw, default_status=spec.status_code)
        if stream_match is not None:
            st_status, st_byte_headers, st_body = stream_match
            str_headers = _to_str_header_pairs(st_byte_headers)
            if isinstance(st_body, AsyncIterable):
                collected = await _collect_async_stream_bytes(st_body)
            else:
                collected = _collect_sync_stream_bytes(st_body)
            return vr_cls(
                status=st_status,
                body=collected,
                headers=str_headers or None,
                request_id=req_id,
            )
        return _coerce_response(raw, req_id, vr_cls, status_code=spec.status_code)
    except Exception as exc:
        log.exception("Async handler error for request %s", req_id)
        return build_error_response(exc, request, error_handler, vr_cls)


async def _await_async_item(
    coro: Coroutine[Any, Any, Any],
    request_id: int,
    err_target: Any,
    error_handler: Callable[..., Any] | None,
    vr_cls: Any,
    active_submit: Callable[[Any], None],
    active_submit_raw: Callable[..., None] | None,
    status_code: int = 200,
    stream_submitter: Callable[..., None] | None = None,
) -> None:
    """Await a pre-invoked handler coroutine without pinning Request across suspension."""
    try:
        raw = await coro
        if active_submit_raw is not None:
            if isinstance(raw, _RAW_BUFFER_TYPES):
                active_submit_raw(request_id, status_code, raw)
                return
            if isinstance(raw, str):
                active_submit_raw(request_id, status_code, raw.encode("utf-8"))
                return
        if isinstance(raw, vr_cls):
            if getattr(raw, "request_id", 0) == 0:
                raw.request_id = request_id
            active_submit(raw)
            return
        special_outcome = _dispatch_special_return(
            raw,
            request_id,
            vr_cls,
            active_submit,
            status_code=status_code,
            stream_submitter=stream_submitter,
        )
        if special_outcome is True:
            return
        if inspect.iscoroutine(special_outcome):
            await special_outcome
            return
        if active_submit_raw is not None:
            active_submit_raw(request_id, status_code, encode_json_bytes(raw), _JSON_CONTENT_TYPE_COMPACT)
            return
        active_submit(_coerce_response(raw, request_id, vr_cls, status_code=status_code))
    except Exception as exc:
        err_resp = build_error_response(exc, err_target, error_handler, vr_cls)
        active_submit(err_resp)


class BatchDispatchPlan(msgspec.Struct, frozen=True, gc=False):
    """Result of partitioning a batch into dispatch groups."""

    sync_native: list[tuple[int, Any, HandlerSpec]] = msgspec.field(default_factory=list)
    """Inline synchronous native handlers `(batch_index, request, spec)`."""
    sync_thread: list[tuple[int, Any, HandlerSpec]] = msgspec.field(default_factory=list)
    """Thread-pool offloaded synchronous handlers `(batch_index, request, spec)`."""
    async_native: list[tuple[int, Any, HandlerSpec]] = msgspec.field(default_factory=list)
    """Asynchronous native coroutine handlers `(batch_index, request, spec)`."""
    mounts: dict[str, list[tuple[int, Any]]] = msgspec.field(default_factory=dict)
    """Mounted ASGI/RSGI/WSGI sub-application requests keyed by mount path prefix."""
    unmatched: list[tuple[int, Any]] = msgspec.field(default_factory=list)
    """Requests with no matching route or mount `(batch_index, request)`."""


def partition_batch(
    batch: Any,
    handlers: Mapping[int, Any],
    mount_runners: Sequence[tuple[str, Any, str]],
) -> BatchDispatchPlan:
    """Classify each request in a batch into sync, threaded sync, async, mount, or unmatched groups."""
    plan = BatchDispatchPlan()
    sync_native = plan.sync_native
    sync_thread = plan.sync_thread
    async_native = plan.async_native
    mounts_map = plan.mounts
    unmatched = plan.unmatched
    get_handler = handlers.get
    mount_count = len(mount_runners)

    for idx, request in enumerate(batch):
        hid = request.handler_id
        handler_obj = get_handler(hid)
        if handler_obj is not None:
            spec = handler_obj if isinstance(handler_obj, HandlerSpec) else compile_handler_spec(handler_obj)
            if spec.is_async:
                async_native.append((idx, request, spec))
            elif spec.sync_to_thread:
                sync_thread.append((idx, request, spec))
            else:
                sync_native.append((idx, request, spec))
            continue

        if mount_count:
            if hid < 0:
                mount_idx = (-hid) - 1
                if 0 <= mount_idx < mount_count:
                    prefix = mount_runners[mount_idx][0]
                    mounts_map.setdefault(prefix, []).append((idx, request))
                    continue
            elif (hid & _MOUNT_HANDLER_MASK) == _MOUNT_HANDLER_MASK:
                mount_idx = hid & _MOUNT_INDEX_MASK
                if 0 <= mount_idx < mount_count:
                    prefix = mount_runners[mount_idx][0]
                    mounts_map.setdefault(prefix, []).append((idx, request))
                    continue

            path = getattr(request, "path", "")
            matched = False
            for prefix, _adapter, _protocol in mount_runners:
                if path.startswith(prefix):
                    mounts_map.setdefault(prefix, []).append((idx, request))
                    matched = True
                    break
            if matched:
                continue

        unmatched.append((idx, request))

    return plan


_BACKGROUND_STREAM_TASKS: set[asyncio.Task[Any]] = set()


def _spawn_tracked_task(
    loop: asyncio.AbstractEventLoop,
    coro: Coroutine[Any, Any, Any],
) -> asyncio.Task[Any]:
    """Create a background task on ``loop`` and retain a strong reference until completion."""
    task = loop.create_task(coro)
    _BACKGROUND_STREAM_TASKS.add(task)
    task.add_done_callback(_BACKGROUND_STREAM_TASKS.discard)
    return task


def _create_tasks_batch(loop: asyncio.AbstractEventLoop, coros: list[Coroutine[Any, Any, Any]]) -> None:
    """Create tracked tasks for a list of coroutines inside the target event loop."""
    for coro in coros:
        _spawn_tracked_task(loop, coro)


async def _gather_coros_inline(coros: list[Coroutine[Any, Any, Any]]) -> None:
    """Await a batch of coroutines inside an active event loop."""
    await asyncio.gather(*coros)


def _schedule_coros(scheduler: Any, coros: list[Coroutine[Any, Any, Any]]) -> None:
    """Schedule coroutines onto a loop or event loop pool with a single cross-thread wake."""
    if not coros:
        return
    if hasattr(scheduler, "spawn_batch"):
        scheduler.spawn_batch(coros)
    elif isinstance(scheduler, asyncio.AbstractEventLoop):
        try:
            running = asyncio.get_running_loop()
        except RuntimeError:
            running = None
        if running is scheduler:
            _create_tasks_batch(scheduler, coros)
        elif not scheduler.is_running():
            scheduler.run_until_complete(_gather_coros_inline(coros))
        else:
            scheduler.call_soon_threadsafe(_create_tasks_batch, scheduler, coros)
    elif hasattr(scheduler, "primary_loop"):
        _schedule_coros(scheduler.primary_loop, coros)
    elif hasattr(scheduler, "loop"):
        _schedule_coros(scheduler.loop, coros)


def get_worker_executor(min_workers: int = 32) -> ThreadPoolExecutor:
    """Return the lazily initialized worker-scoped ThreadPoolExecutor."""
    global _WORKER_EXECUTOR
    with _EXECUTOR_LOCK:
        if _WORKER_EXECUTOR is None:
            _WORKER_EXECUTOR = ThreadPoolExecutor(
                max_workers=max(1, min_workers),
                thread_name_prefix="vertebra-worker",
            )
        return _WORKER_EXECUTOR


def shutdown_worker_executor(wait: bool = True) -> None:
    """Atomically clear and shut down the cached worker-scoped ThreadPoolExecutor."""
    global _WORKER_EXECUTOR
    with _EXECUTOR_LOCK:
        pool = _WORKER_EXECUTOR
        _WORKER_EXECUTOR = None
    if pool is not None:
        pool.shutdown(wait=wait)


def _dispatch_wsgi_concurrent(
    adapter: Any,
    entries: list[tuple[int, Any]],
    results: list[Any],
    vr_cls: Any,
    executor: ThreadPoolExecutor | None,
) -> None:
    """Dispatch WSGI requests concurrently via a thread pool."""
    if len(entries) == 1:
        idx, request = entries[0]
        try:
            results[idx] = adapter(request)
        except Exception:
            log.exception("WSGI handler error for request %s", getattr(request, "request_id", 0))
            results[idx] = build_error_response(RuntimeError("WSGI error"), request, vr_cls=vr_cls)
        return

    pool = executor or get_worker_executor(max(32, len(entries)))
    indexed_futures = [(idx, req, pool.submit(adapter, req)) for idx, req in entries]
    for idx, req, fut in indexed_futures:
        try:
            results[idx] = fut.result()
        except Exception:
            log.exception("WSGI handler error for request %s", getattr(req, "request_id", 0))
            results[idx] = build_error_response(RuntimeError("WSGI error"), req, vr_cls=vr_cls)


def dispatch_batch(
    partitioned: BatchDispatchPlan,
    handlers: Mapping[int, Any],
    mount_runners: Sequence[tuple[str, Any, str]],
    vr_cls: Any = Response,
    scheduler: Any = None,
    *,
    async_mode: bool = False,
    mount_lookup: dict[str, tuple[Any, str]] | None = None,
    submit: Callable[[Any], None] | None = None,
    submit_raw: Callable[..., None] | None = None,
    error_handler: Callable[..., Any] | None = None,
    executor: ThreadPoolExecutor | None = None,
    batch_timeout: float | None | object = _UNSET_BATCH_TIMEOUT,
) -> list[Any] | None:
    """Dispatch a partitioned batch to native handlers and mounts."""
    _ = handlers
    loop = (
        scheduler.primary_loop
        if hasattr(scheduler, "primary_loop")
        else (scheduler.loop if hasattr(scheduler, "loop") else scheduler)
    )

    if not async_mode:
        total = (
            len(partitioned.sync_native)
            + len(partitioned.sync_thread)
            + len(partitioned.async_native)
            + sum(len(v) for v in partitioned.mounts.values())
            + len(partitioned.unmatched)
        )
        results: list[Any] = [None] * total

        for idx, request, spec in partitioned.sync_native:
            results[idx] = _invoke_sync_handler(spec, request, error_handler, vr_cls)

        for idx, request, spec in partitioned.sync_thread:
            results[idx] = _invoke_sync_handler(spec, request, error_handler, vr_cls)

        if partitioned.async_native:
            coros = [
                _invoke_async_handler(spec, request, error_handler, vr_cls)
                for _idx, request, spec in partitioned.async_native
            ]
            if hasattr(scheduler, "schedule_batch"):
                resolved_batch_timeout: float | None = (
                    _resolve_asgi_batch_timeout()
                    if batch_timeout is _UNSET_BATCH_TIMEOUT
                    else cast("float | None", batch_timeout)
                )
                futures = scheduler.schedule_batch(coros)
                _done, not_done = concurrent.futures.wait(futures, timeout=resolved_batch_timeout)
                for fut in not_done:
                    fut.cancel()
                for (idx, _request, _spec), fut in zip(partitioned.async_native, futures, strict=True):
                    if fut in not_done:
                        results[idx] = vr_cls(
                            status=504,
                            body=b"Gateway Timeout",
                            request_id=getattr(_request, "request_id", 0),
                        )
                        continue
                    try:
                        results[idx] = fut.result()
                    except Exception as exc:
                        results[idx] = build_error_response(exc, _request, error_handler, vr_cls)
            elif isinstance(loop, asyncio.AbstractEventLoop):
                for (idx, _request, _spec), coro in zip(partitioned.async_native, coros, strict=True):
                    try:
                        results[idx] = loop.run_until_complete(coro)
                    except Exception as exc:
                        results[idx] = build_error_response(exc, _request, error_handler, vr_cls)

        if partitioned.mounts:
            resolved_lookup = mount_lookup or {
                prefix: (adapter, protocol) for prefix, adapter, protocol in mount_runners
            }
            for prefix, entries in partitioned.mounts.items():
                adapter, protocol = resolved_lookup[prefix]
                if protocol == "asgi":
                    requests_list = [req for _idx, req in entries]
                    responses = adapter(requests_list)
                    for (idx, request), resp in zip(entries, responses, strict=True):
                        results[idx] = vr_cls(
                            status=resp.status,
                            body=bytes(resp.body),
                            headers=getattr(resp, "headers", []),
                            request_id=request.request_id,
                        )
                elif protocol == "rsgi":
                    for idx, request in entries:
                        coro = (
                            adapter.run_single(request)
                            if hasattr(adapter, "run_single")
                            else adapter.handle_request(request)
                        )
                        if hasattr(scheduler, "schedule"):
                            resp = scheduler.schedule(coro).result()
                        elif isinstance(loop, asyncio.AbstractEventLoop) and loop.is_running():
                            resp = asyncio.run_coroutine_threadsafe(coro, loop).result()
                        elif isinstance(loop, asyncio.AbstractEventLoop):
                            resp = loop.run_until_complete(coro)
                        else:
                            resp = asyncio.run(coro)
                        results[idx] = _coerce_response(resp, request.request_id, vr_cls)
                elif protocol == "wsgi":
                    _dispatch_wsgi_concurrent(adapter, entries, results, vr_cls, executor)

        for idx, request in partitioned.unmatched:
            req_id = getattr(request, "request_id", 0)
            if hasattr(vr_cls, "error"):
                try:
                    results[idx] = vr_cls.error(404, req_id)
                    continue
                except Exception:
                    pass
            results[idx] = vr_cls(status=404, body=b"not found", request_id=req_id)

        return results

    active_submit = submit or submit_response
    active_submit_raw = submit_raw if submit_raw is not None else (submit_response_raw if submit is None else None)
    use_native_batch_submit = (submit is None or submit is submit_response) and (
        submit_raw is None or submit_raw is submit_response_raw
    )
    raw_batch: list[tuple[int, int, Any] | tuple[int, int, Any, bytes]] = []

    if partitioned.sync_native:
        if use_native_batch_submit:
            append_raw = raw_batch.append
            for _idx, request, spec in partitioned.sync_native:
                req_id = getattr(request, "request_id", 0)
                status = spec.status_code
                try:
                    raw = spec.invoker(request)
                    if isinstance(raw, _RAW_BUFFER_TYPES):
                        append_raw((req_id, status, raw))
                    elif isinstance(raw, str):
                        append_raw((req_id, status, raw.encode("utf-8")))
                    elif isinstance(raw, vr_cls):
                        if getattr(raw, "request_id", 0) == 0:
                            raw.request_id = req_id
                        active_submit(raw)
                    elif _is_potential_special_return(raw):
                        if raw_batch:
                            submit_batch_raw(raw_batch)
                            raw_batch.clear()
                        _submit_coerced(
                            raw,
                            req_id,
                            vr_cls,
                            active_submit,
                            active_submit_raw,
                            status_code=status,
                            scheduler=scheduler or loop,
                        )
                    else:
                        append_raw((req_id, status, encode_json_bytes(raw), _JSON_CONTENT_TYPE_COMPACT))
                except Exception as exc:
                    err_resp = build_error_response(exc, request, error_handler, vr_cls)
                    active_submit(err_resp)
        else:
            for _idx, request, spec in partitioned.sync_native:
                req_id = getattr(request, "request_id", 0)
                try:
                    raw = spec.invoker(request)
                    _submit_coerced(
                        raw,
                        req_id,
                        vr_cls,
                        active_submit,
                        active_submit_raw,
                        status_code=spec.status_code,
                        scheduler=scheduler or loop,
                    )
                except Exception as exc:
                    err_resp = build_error_response(exc, request, error_handler, vr_cls)
                    active_submit(err_resp)

    if partitioned.sync_thread:
        active_exec = executor or get_worker_executor()
        for _idx, request, spec in partitioned.sync_thread:

            def _dispatch_sync_item(
                req: Any = request,
                s: HandlerSpec = spec,
                eh: Any = error_handler,
            ) -> None:
                try:
                    raw = s.invoker(req)
                    _submit_coerced(
                        raw,
                        getattr(req, "request_id", 0),
                        vr_cls,
                        active_submit,
                        active_submit_raw,
                        status_code=s.status_code,
                        scheduler=scheduler or loop,
                    )
                except Exception as exc:
                    err_resp = build_error_response(exc, req, eh, vr_cls)
                    active_submit(err_resp)

            active_exec.submit(_dispatch_sync_item)

    if partitioned.async_native:
        if use_native_batch_submit and error_handler is None and getattr(scheduler, "size", 1) == 1:
            if raw_batch:
                submit_batch_raw(raw_batch)
                raw_batch.clear()
            native_items: list[tuple[int, Any, int]] = []
            for _idx, request, spec in partitioned.async_native:
                req_id = getattr(request, "request_id", 0)
                try:
                    coro = spec.invoker(request)
                except Exception as exc:
                    err_resp = build_error_response(exc, request, error_handler, vr_cls)
                    active_submit(err_resp)
                    continue
                native_items.append((req_id, coro, spec.status_code))
            if native_items:
                drive_native_batch(
                    native_items,
                    loop_obj=loop if isinstance(loop, asyncio.AbstractEventLoop) else None,
                )
        else:
            async_coros: list[Coroutine[Any, Any, None]] = []
            for _idx, request, spec in partitioned.async_native:
                req_id = getattr(request, "request_id", 0)
                try:
                    coro = spec.invoker(request)
                except Exception as exc:
                    err_resp = build_error_response(exc, request, error_handler, vr_cls)
                    active_submit(err_resp)
                    continue
                err_target = request if error_handler is not None else _AsyncErrorStub(request_id=req_id)
                async_coros.append(
                    _await_async_item(
                        coro,
                        req_id,
                        err_target,
                        error_handler,
                        vr_cls,
                        active_submit,
                        active_submit_raw,
                        spec.status_code,
                    )
                )

            _schedule_coros(scheduler or loop, async_coros)

    if partitioned.mounts:
        resolved_lookup = mount_lookup or {prefix: (adapter, protocol) for prefix, adapter, protocol in mount_runners}
        for prefix, entries in partitioned.mounts.items():
            adapter, protocol = resolved_lookup[prefix]
            if protocol == "asgi":
                adapter.spawn_batch([req for _idx, req in entries])
            elif protocol == "rsgi":
                if use_native_batch_submit and hasattr(adapter, "spawn_batch"):
                    target_loop = scheduler.next_loop() if hasattr(scheduler, "next_loop") else loop
                    adapter.spawn_batch(
                        [req for _idx, req in entries],
                        loop=target_loop if isinstance(target_loop, asyncio.AbstractEventLoop) else None,
                    )
                else:
                    rsgi_coros: list[Coroutine[Any, Any, None]] = []
                    for _idx, request in entries:

                        async def _dispatch_rsgi_item(
                            req: Any = request,
                            a: Any = adapter,
                        ) -> None:
                            try:
                                coro = a.run_single(req) if hasattr(a, "run_single") else a.handle_request(req)
                                raw = await coro
                                _submit_coerced(
                                    raw, getattr(req, "request_id", 0), vr_cls, active_submit, active_submit_raw
                                )
                            except Exception as exc:
                                err_resp = build_error_response(exc, req, error_handler, vr_cls)
                                active_submit(err_resp)

                        rsgi_coros.append(_dispatch_rsgi_item())

                    _schedule_coros(scheduler or loop, rsgi_coros)
            elif protocol == "wsgi":
                if use_native_batch_submit and hasattr(adapter, "spawn_batch"):
                    adapter.spawn_batch([req for _idx, req in entries])
                elif executor is not None:
                    for _idx, request in entries:

                        def _dispatch_wsgi_item(req: Any = request, a: Any = adapter) -> None:
                            try:
                                resp = a(req)
                                active_submit(resp)
                            except Exception as exc:
                                err_resp = build_error_response(exc, req, error_handler, vr_cls)
                                active_submit(err_resp)

                        executor.submit(_dispatch_wsgi_item)
                else:
                    for _idx, request in entries:
                        try:
                            resp = adapter(request)
                            active_submit(resp)
                        except Exception as exc:
                            err_resp = build_error_response(exc, request, error_handler, vr_cls)
                            active_submit(err_resp)

    if partitioned.unmatched:
        if use_native_batch_submit:
            for _idx, request in partitioned.unmatched:
                raw_batch.append((getattr(request, "request_id", 0), 404, b"not found"))
        else:
            for _idx, request in partitioned.unmatched:
                req_id = getattr(request, "request_id", 0)
                if active_submit_raw is not None:
                    active_submit_raw(req_id, 404, b"not found")
                elif hasattr(vr_cls, "error"):
                    try:
                        active_submit(vr_cls.error(404, req_id))
                    except Exception:
                        active_submit(vr_cls(status=404, body=b"not found", request_id=req_id))
                else:
                    active_submit(vr_cls(status=404, body=b"not found", request_id=req_id))

    if raw_batch:
        submit_batch_raw(raw_batch)

    return None


def _handler_meta(handler: Any) -> tuple[bool, bool]:
    """Return has_params and is_async flags for a handler."""
    spec = compile_handler_spec(handler)
    return spec.has_params, spec.is_async


def _resolve_handler_args(handler: Any, request: Any) -> tuple[Any, ...]:
    """Resolve handler argument tuple for a request."""
    spec = handler if isinstance(handler, HandlerSpec) else compile_handler_spec(handler)
    if not spec.param_names:
        return ()
    if spec.arg_resolver is not None:
        return spec.arg_resolver(request)
    return tuple(_PARAM_EXTRACTORS.get(name, _extract_request)(request) for name in spec.param_names)


def _call_handler(handler: Any, request: Any, *, loop: asyncio.AbstractEventLoop | None = None) -> Any:
    """Call a handler with parameter resolution."""
    spec = handler if isinstance(handler, HandlerSpec) else compile_handler_spec(handler)
    result = spec.invoker(request)
    if spec.is_async:
        if loop is None:
            if inspect.iscoroutine(result):
                result.close()
            msg = "async handler called without an event loop"
            raise RuntimeError(msg)
        return loop.run_until_complete(result)
    return result


def _build_simple_wrapper(
    fn: Callable[[Any], Any],
) -> Callable[[Any, dict[str, Any], dict[str, str]], Any]:
    """Adapt a single-arg request extractor to the 3-arg compiled parameter signature."""

    def _wrapped(request: Any, _path_map: dict[str, Any], _query_map: dict[str, str], _fn: Any = fn) -> Any:
        return _fn(request)

    return _wrapped


def _extract_markers_from_raw(raw_annotation: Any) -> tuple[Any, Any, Any]:
    """Extract LocationMarker, ParameterMarker, and BodyMarker metadata from an Annotated type."""
    if get_origin(raw_annotation) is None:
        return None, None, None
    loc_marker: Any = None
    param_marker: Any = None
    body_marker: Any = None
    for item in get_args(raw_annotation)[1:]:
        cls_name = type(item).__name__
        if cls_name == "LocationMarker":
            loc_marker = item
        elif cls_name == "ParameterMarker":
            param_marker = item
        elif cls_name == "BodyMarker":
            body_marker = item
    return loc_marker, param_marker, body_marker


def _compile_non_di_param(
    binding: ParamBinding,
    path_param_names: set[str],
) -> tuple[_CompiledParamPlan, bool, bool, bool]:
    """Compile a non-DI parameter into a _CompiledParamPlan and return (plan, needs_path, needs_query, is_simple)."""
    loc_marker, param_marker, body_marker = _extract_markers_from_raw(binding.raw_annotation)
    base_type = binding.annotation
    param_name = binding.name

    if binding.name in path_param_names or (loc_marker is not None and loc_marker.location == "path"):

        def _extract_path_param(
            _req: Any,
            path_map: dict[str, Any],
            _query_map: dict[str, str],
            _name: str = param_name,
            _has_def: bool = binding.has_default,
            _def: Any = binding.default,
        ) -> Any:
            if _name in path_map:
                return path_map[_name]
            if _has_def:
                return _def
            msg = f"Missing required path parameter {_name!r}"
            raise ValueError(msg)

        return _CompiledParamPlan(name=param_name, kind="path", extractor=_extract_path_param), True, False, False

    is_struct_body = inspect.isclass(base_type) and (
        issubclass(base_type, msgspec.Struct)
        or dataclasses.is_dataclass(base_type)
        or hasattr(base_type, "__pydantic_validator__")
        or hasattr(base_type, "__attrs_attrs__")
        or hasattr(base_type, "FromString")
    )
    if (loc_marker is not None and loc_marker.location == "body") or body_marker is not None or is_struct_body:
        media_type = (
            (loc_marker.media_type if loc_marker is not None and loc_marker.media_type else None)
            or (body_marker.media_type if body_marker is not None else None)
            or "application/json"
        )
        target_type = base_type if base_type is not Any else Any
        adapter_plan = ZeroCopyCodecRegistry.compile(target_type)

        def _extract_typed_body(
            req: Any,
            _path_map: dict[str, Any],
            _query_map: dict[str, str],
            _plan: Any = adapter_plan,
            _default_media: str = media_type,
        ) -> Any:
            req_ct = _lookup_header(getattr(req, "headers", None), "content-type") or _default_media
            if hasattr(req, "decode_msgspec") or hasattr(req, "body_view"):
                return _plan.decode_request(req, format=req_ct)
            raw_bytes = _extract_body(req)
            if "msgpack" in req_ct:
                return decode_msgpack(raw_bytes, type=_plan.target_type)
            return decode_json(raw_bytes, type=_plan.target_type)

        return _CompiledParamPlan(name=param_name, kind="body", extractor=_extract_typed_body), False, False, False

    if (loc_marker is not None and loc_marker.location == "header") or (
        param_marker is not None and param_marker.header is not None
    ):
        header_key = (
            param_marker.header
            if (param_marker is not None and param_marker.header is not None)
            else param_name.replace("_", "-")
        ).lower()
        converter = _SCALAR_QUERY_TYPES.get(base_type, str)
        has_def = binding.has_default or (param_marker is not None and param_marker.default is not None)
        def_val = (
            binding.default if binding.has_default else (param_marker.default if param_marker is not None else None)
        )

        def _extract_header_param(
            req: Any,
            _path_map: dict[str, Any],
            _query_map: dict[str, str],
            _key: str = header_key,
            _conv: Callable[[str], Any] = converter,
            _has_def: bool = has_def,
            _def: Any = def_val,
        ) -> Any:
            val = _lookup_header(getattr(req, "headers", None), _key)
            if val is not None:
                return _conv(val)
            if _has_def:
                return _def
            return None

        return _CompiledParamPlan(name=param_name, kind="header", extractor=_extract_header_param), False, False, False

    if (loc_marker is not None and loc_marker.location == "cookie") or (
        param_marker is not None and param_marker.cookie is not None
    ):
        cookie_key = (
            param_marker.cookie if (param_marker is not None and param_marker.cookie is not None) else param_name
        )
        converter = _SCALAR_QUERY_TYPES.get(base_type, str)
        has_def = binding.has_default or (param_marker is not None and param_marker.default is not None)
        def_val = (
            binding.default if binding.has_default else (param_marker.default if param_marker is not None else None)
        )

        def _extract_cookie_param(
            req: Any,
            _path_map: dict[str, Any],
            _query_map: dict[str, str],
            _key: str = cookie_key,
            _conv: Callable[[str], Any] = converter,
            _has_def: bool = has_def,
            _def: Any = def_val,
        ) -> Any:
            val = _lookup_cookie(getattr(req, "headers", None), _key)
            if val is not None:
                return _conv(val)
            if _has_def:
                return _def
            return None

        return _CompiledParamPlan(name=param_name, kind="cookie", extractor=_extract_cookie_param), False, False, False

    if param_name in _PARAM_EXTRACTORS and loc_marker is None and param_marker is None:
        raw_ext = _PARAM_EXTRACTORS[param_name]
        return (
            _CompiledParamPlan(name=param_name, kind="request_attr", extractor=_build_simple_wrapper(raw_ext)),
            False,
            False,
            True,
        )

    if (
        (loc_marker is not None and loc_marker.location == "query")
        or (param_marker is not None and param_marker.query is not None)
        or base_type in _SCALAR_QUERY_TYPES
        or binding.has_default
    ):
        query_key = param_marker.query if (param_marker is not None and param_marker.query is not None) else param_name
        converter = _SCALAR_QUERY_TYPES.get(base_type, str)
        has_def = binding.has_default or (param_marker is not None and param_marker.default is not None)
        def_val = (
            binding.default if binding.has_default else (param_marker.default if param_marker is not None else None)
        )

        def _extract_query_param(
            _req: Any,
            _path_map: dict[str, Any],
            query_map: dict[str, str],
            _key: str = query_key,
            _conv: Callable[[str], Any] = converter,
            _has_def: bool = has_def,
            _def: Any = def_val,
        ) -> Any:
            if _key in query_map:
                return _conv(query_map[_key])
            if _has_def:
                return _def
            return None

        return _CompiledParamPlan(name=param_name, kind="query", extractor=_extract_query_param), False, True, False

    return (
        _CompiledParamPlan(name=param_name, kind="request", extractor=_build_simple_wrapper(_extract_request)),
        False,
        False,
        True,
    )


def _parse_query_dict(raw_query: Any) -> dict[str, str]:
    """Parse a raw query string or mapping into a key-value dictionary."""
    if not raw_query:
        return {}
    if isinstance(raw_query, Mapping):
        return {str(k): str(v) for k, v in raw_query.items()}
    if isinstance(raw_query, bytes):
        raw_query = raw_query.decode("utf-8", errors="replace")
    if isinstance(raw_query, str):
        qs = raw_query.lstrip("?")
        if not qs:
            return {}
        return dict(parse_qsl(qs, keep_blank_values=True))
    return {}


def _lookup_header(headers: Any, lower_key: str) -> str | None:
    """Case-insensitively look up a header value from a mapping or tuple sequence."""
    if headers is None:
        return None
    if isinstance(headers, Mapping):
        if lower_key in headers:
            return str(headers[lower_key])
        for k, v in headers.items():
            if str(k).lower() == lower_key:
                return str(v)
        return None
    if hasattr(headers, "get"):
        val = headers.get(lower_key)
        if val is not None:
            return str(val)
    if isinstance(headers, Sequence):
        for item in headers:
            if isinstance(item, tuple | list) and len(item) == 2:
                k, v = item
                k_str = k.decode("latin1") if isinstance(k, bytes) else str(k)
                if k_str.lower() == lower_key:
                    return v.decode("latin1") if isinstance(v, bytes) else str(v)
    return None


def _lookup_cookie(headers: Any, cookie_name: str) -> str | None:
    """Extract a named cookie value from the Cookie request header."""
    cookie_header = _lookup_header(headers, "cookie")
    if not cookie_header:
        return None
    for part in cookie_header.split(";"):
        if "=" in part:
            k, v = part.split("=", 1)
            if k.strip() == cookie_name:
                return v.strip()
    return None


def _resolve_security_context_sync(
    request: Any,
    container: Container | None,
    path_map: dict[str, Any],
) -> SecurityContext:
    """Resolve a SecurityContext from the request or DI container for synchronous guard evaluation."""
    existing_ctx = getattr(request, "security_context", None)
    if isinstance(existing_ctx, SecurityContext):
        return existing_ctx
    existing_principal = getattr(request, "principal", None)
    if isinstance(existing_principal, Principal):
        return SecurityContext(principal=existing_principal)
    if container is not None:
        if SecurityContext in container.graph.type_index or "security_context" in container.graph.providers:
            key = container.graph.type_index.get(SecurityContext, "security_context")
            ctx = container.resolve_sync(key, request=request, extra_context=path_map)
            if isinstance(ctx, SecurityContext):
                return ctx
        if Principal in container.graph.type_index or "principal" in container.graph.providers:
            key = container.graph.type_index.get(Principal, "principal")
            principal = container.resolve_sync(key, request=request, extra_context=path_map)
            if isinstance(principal, Principal):
                return SecurityContext(principal=principal)
    return SecurityContext(principal=None)


async def _resolve_security_context_async(
    request: Any,
    container: Container | None,
    path_map: dict[str, Any],
) -> SecurityContext:
    """Resolve a SecurityContext from the request or DI container for asynchronous guard evaluation."""
    existing_ctx = getattr(request, "security_context", None)
    if isinstance(existing_ctx, SecurityContext):
        return existing_ctx
    existing_principal = getattr(request, "principal", None)
    if isinstance(existing_principal, Principal):
        return SecurityContext(principal=existing_principal)
    if container is not None:
        if SecurityContext in container.graph.type_index or "security_context" in container.graph.providers:
            key = container.graph.type_index.get(SecurityContext, "security_context")
            ctx = await container.resolve(key, request=request, extra_context=path_map)
            if isinstance(ctx, SecurityContext):
                return ctx
        if Principal in container.graph.type_index or "principal" in container.graph.providers:
            key = container.graph.type_index.get(Principal, "principal")
            principal = await container.resolve(key, request=request, extra_context=path_map)
            if isinstance(principal, Principal):
                return SecurityContext(principal=principal)
    return SecurityContext(principal=None)


def _enforce_guards_sync(
    guards: tuple[Callable[..., Any], ...],
    request: Any,
    container: Container | None,
    path_map: dict[str, Any],
) -> None:
    """Evaluate synchronous guards on a request, raising PermissionDeniedError on failure."""
    sec_ctx: SecurityContext | None = None
    for guard in guards:
        if isinstance(guard, Guard):
            if sec_ctx is None:
                sec_ctx = _resolve_security_context_sync(request, container, path_map)
            sec_ctx.enforce(guard)
            continue
        bindings = inspect_callable_bindings(guard)
        res = guard(request) if bindings else guard()
        if res is False:
            guard_name = getattr(guard, "__name__", repr(guard))
            msg = f"Guard {guard_name} rejected request"
            raise PermissionDeniedError(msg)


async def _enforce_guards_async(
    guards: tuple[Callable[..., Any], ...],
    request: Any,
    container: Container | None,
    path_map: dict[str, Any],
) -> None:
    """Evaluate synchronous and asynchronous guards on a request, raising PermissionDeniedError on failure."""
    sec_ctx: SecurityContext | None = None
    for guard in guards:
        if isinstance(guard, Guard):
            if sec_ctx is None:
                sec_ctx = await _resolve_security_context_async(request, container, path_map)
            sec_ctx.enforce(guard)
            continue
        bindings = inspect_callable_bindings(guard)
        res = guard(request) if bindings else guard()
        if inspect.isawaitable(res):
            res = await res
        if res is False:
            guard_name = getattr(guard, "__name__", repr(guard))
            msg = f"Guard {guard_name} rejected request"
            raise PermissionDeniedError(msg)


def _resolve_asgi_batch_timeout() -> float | None:
    """Resolve ``VERTEBRA_ASGI_BATCH_TIMEOUT`` (seconds) for buffered sharded batches.

    Unset or empty means the 30 second default. ``0`` or ``none`` (any case) disables the
    timeout. Any other value must be a positive number of seconds.

    Raises:
        ImproperConfigurationError: When the value is negative or not a number.
    """
    raw = os.environ.get("VERTEBRA_ASGI_BATCH_TIMEOUT", "").strip()
    if not raw:
        return _DEFAULT_ASGI_BATCH_TIMEOUT_SECONDS
    if raw.lower() == "none":
        return None
    msg = f"VERTEBRA_ASGI_BATCH_TIMEOUT must be a non-negative number of seconds or 'none', got {raw!r}"
    try:
        timeout = float(raw)
    except ValueError as exc:
        raise ImproperConfigurationError(msg) from exc
    if timeout < 0:
        raise ImproperConfigurationError(msg)
    return None if timeout == 0 else timeout


def _is_potential_special_return(raw: Any) -> bool:
    """Return True when a handler result may be a streaming, Arrow, storage, or tuple response."""
    if raw is None or isinstance(
        raw,
        dict | list | bool | int | float | bytes | bytearray | memoryview | str | Response,
    ):
        return False
    if isinstance(raw, ArrowCursor | RustArrowCursor | StreamingStorageResponse):
        return True
    if isinstance(raw, msgspec.Struct) or dataclasses.is_dataclass(raw):
        return False
    return isinstance(raw, tuple | AsyncIterable) or hasattr(raw, "status_code") or hasattr(raw, "status")


def _is_async_storage_response(raw: Any) -> bool:
    """Return True when raw is a StreamingStorageResponse backed by an unloaded AsyncObjectStore."""
    return (
        isinstance(raw, StreamingStorageResponse)
        and raw.data is None
        and raw.store is not None
        and type(raw.store).__name__ == "AsyncObjectStore"
    )


def _is_valid_response_headers(headers: Any) -> bool:
    """Return True if headers is None, a Mapping, or a 2-tuple header sequence."""
    if headers is None or isinstance(headers, Mapping):
        return True
    if isinstance(headers, Sequence) and not isinstance(headers, str | bytes | bytearray | memoryview):
        return all(
            isinstance(item, tuple | list)
            and len(item) == 2
            and isinstance(item[0], str | bytes)
            and isinstance(item[1], str | bytes)
            for item in headers
        )
    return False


def _normalize_header_pairs(headers: Any) -> list[tuple[bytes, bytes]]:
    """Normalize a mapping or header tuple sequence into raw latin-1 byte pairs."""
    if headers is None:
        return []
    pairs: list[tuple[bytes, bytes]] = []
    items = headers.items() if isinstance(headers, Mapping) else headers
    for k, v in items:
        kb = k if isinstance(k, bytes) else str(k).encode("latin-1")
        vb = v if isinstance(v, bytes) else str(v).encode("latin-1")
        pairs.append((kb, vb))
    return pairs


def _to_str_header_pairs(byte_pairs: list[tuple[bytes, bytes]]) -> list[tuple[str, str]]:
    """Convert raw byte header pairs into latin-1 string pairs for native Response."""
    return [(k.decode("latin-1"), v.decode("latin-1")) for k, v in byte_pairs]


def _is_stream_body(body: Any) -> bool:
    """Return True when body is an AsyncIterable or a non-scalar byte-chunk Iterable."""
    if isinstance(body, AsyncIterable):
        return True
    if isinstance(body, bytes | bytearray | memoryview | str | Mapping):
        return False
    if isinstance(body, Sequence):
        return len(body) == 0 or isinstance(body[0], bytes | bytearray | memoryview)
    return isinstance(body, Iterable)


def _extract_streaming_response(
    raw: Any,
    *,
    default_status: int = 200,
) -> tuple[int, list[tuple[bytes, bytes]], AsyncIterable[Any] | Iterable[Any]] | None:
    """Extract (status, byte_headers, stream_body) when raw represents a streaming response."""
    if isinstance(raw, AsyncIterable):
        return default_status, [], raw
    if isinstance(raw, tuple) and len(raw) == 3:
        st, hdrs, body = raw
        if (
            isinstance(st, int)
            and not isinstance(st, bool)
            and 100 <= st <= 599
            and _is_valid_response_headers(hdrs)
            and _is_stream_body(body)
        ):
            return st, _normalize_header_pairs(hdrs), body
        return None
    if not isinstance(
        raw,
        bytes
        | bytearray
        | memoryview
        | str
        | Mapping
        | Sequence
        | msgspec.Struct
        | Response
        | ArrowCursor
        | RustArrowCursor,
    ) and not dataclasses.is_dataclass(raw):
        raw_status = getattr(raw, "status_code", None)
        if raw_status is None:
            raw_status = getattr(raw, "status", None)
        if isinstance(raw_status, int) and not isinstance(raw_status, bool) and 100 <= raw_status <= 599:
            for attr_name in ("stream", "iterator", "body_iterator", "content", "body"):
                cand = getattr(raw, attr_name, None)
                if cand is not None and _is_stream_body(cand):
                    return raw_status, _normalize_header_pairs(getattr(raw, "headers", None)), cand
    return None


def _extract_unary_tuple_response(raw: Any) -> tuple[int, list[tuple[str, str]], bytes] | None:
    """Extract (status, str_headers, body_bytes) when raw is a unary (status, headers, bytes|str) tuple."""
    if not (isinstance(raw, tuple) and len(raw) == 3):
        return None
    st, hdrs, body = raw
    if not (
        isinstance(st, int)
        and not isinstance(st, bool)
        and 100 <= st <= 599
        and _is_valid_response_headers(hdrs)
        and isinstance(body, bytes | bytearray | memoryview | str)
    ):
        return None
    if isinstance(body, str):
        body_bytes = body.encode("utf-8")
    elif isinstance(body, bytes):
        body_bytes = body
    else:
        body_bytes = bytes(body)
    return st, _to_str_header_pairs(_normalize_header_pairs(hdrs)), body_bytes


def _coerce_stream_chunk(item: Any) -> bytes:
    """Coerce an emitted stream chunk into raw bytes."""
    if isinstance(item, bytes):
        return item
    if isinstance(item, bytearray | memoryview):
        return bytes(item)
    if isinstance(item, str):
        return item.encode("utf-8")
    encode_fn = getattr(item, "encode", None)
    if callable(encode_fn):
        encoded = encode_fn()
        if isinstance(encoded, bytes):
            return encoded
    return bytes(item)


def _collect_sync_stream_bytes(body_iter: Iterable[Any]) -> bytes:
    """Drain a synchronous byte iterable into a single bytes buffer for buffered test callers."""
    iterator = iter(body_iter)
    close_fn = getattr(iterator, "close", None)
    try:
        return b"".join(_coerce_stream_chunk(item) for item in iterator)
    finally:
        if callable(close_fn):
            with contextlib.suppress(Exception):
                close_fn()


async def _collect_async_stream_bytes(body_iter: AsyncIterable[Any]) -> bytes:
    """Drain an asynchronous byte iterable into a single bytes buffer for buffered test callers."""
    aiterator = aiter(body_iter)
    aclose_fn = getattr(aiterator, "aclose", None)
    parts: list[bytes] = []
    try:
        async for item in aiterator:
            parts.append(_coerce_stream_chunk(item))
        return b"".join(parts)
    finally:
        if callable(aclose_fn):
            with contextlib.suppress(Exception):
                res = aclose_fn()
                if inspect.isawaitable(res):
                    await res


def _stream_sync_iterable(
    request_id: int,
    status: int,
    headers: list[tuple[bytes, bytes]],
    body_iter: Iterable[Any],
    *,
    stream_submitter: Callable[..., None] | None = None,
) -> None:
    """Flush a synchronous byte iterable progressively via submit_stream_frame."""
    active_submit = submit_stream_frame if stream_submitter is None else stream_submitter
    iterator = iter(body_iter)
    close_fn = getattr(iterator, "close", None)
    try:
        active_submit(
            request_id,
            status,
            STREAM_FLAG_STREAMING | STREAM_FLAG_HEADERS_SENT,
            headers,
            b"",
        )
        for item in iterator:
            chunk = _coerce_stream_chunk(item)
            if chunk:
                active_submit(
                    request_id,
                    status,
                    STREAM_FLAG_STREAMING,
                    None,
                    chunk,
                )
        active_submit(
            request_id,
            status,
            STREAM_FLAG_STREAMING | STREAM_FLAG_FINAL,
            None,
            b"",
        )
    except ConnectionResetError:
        return
    except Exception:
        log.exception("Streaming sync iterator error for request %s", request_id)
        with contextlib.suppress(ConnectionResetError):
            active_submit(
                request_id,
                status,
                STREAM_FLAG_STREAMING | STREAM_FLAG_ERROR,
                None,
                b"",
            )
    finally:
        if callable(close_fn):
            with contextlib.suppress(Exception):
                close_fn()


async def _stream_async_iterable(
    request_id: int,
    status: int,
    headers: list[tuple[bytes, bytes]],
    body_iter: AsyncIterable[Any],
    *,
    stream_submitter: Callable[..., None] | None = None,
) -> None:
    """Flush an asynchronous byte iterable progressively via submit_stream_frame."""
    active_submit = submit_stream_frame if stream_submitter is None else stream_submitter
    aiterator = aiter(body_iter)
    aclose_fn = getattr(aiterator, "aclose", None)
    headers_sent = False
    terminal_sent = False
    try:
        active_submit(
            request_id,
            status,
            STREAM_FLAG_STREAMING | STREAM_FLAG_HEADERS_SENT,
            headers,
            b"",
        )
        headers_sent = True
        async for item in aiterator:
            chunk = _coerce_stream_chunk(item)
            if chunk:
                active_submit(
                    request_id,
                    status,
                    STREAM_FLAG_STREAMING,
                    None,
                    chunk,
                )
        active_submit(
            request_id,
            status,
            STREAM_FLAG_STREAMING | STREAM_FLAG_FINAL,
            None,
            b"",
        )
        terminal_sent = True
    except ConnectionResetError:
        return
    except asyncio.CancelledError:
        if headers_sent and not terminal_sent:
            with contextlib.suppress(ConnectionResetError):
                active_submit(
                    request_id,
                    status,
                    STREAM_FLAG_STREAMING | STREAM_FLAG_ERROR,
                    None,
                    b"",
                )
        raise
    except Exception:
        log.exception("Streaming async iterator error for request %s", request_id)
        if not terminal_sent:
            with contextlib.suppress(ConnectionResetError):
                active_submit(
                    request_id,
                    status,
                    STREAM_FLAG_STREAMING | STREAM_FLAG_ERROR,
                    None,
                    b"",
                )
    finally:
        if callable(aclose_fn):
            with contextlib.suppress(Exception):
                res = aclose_fn()
                if inspect.isawaitable(res):
                    await res


def _submit_fastpath_tuple(
    sp_status: int,
    sp_headers: Sequence[tuple[bytes, bytes]],
    sp_body: bytes | memoryview,
    request_id: int,
    vr_cls: Any,
    submit: Callable[[Any], None],
) -> None:
    """Submit a pre-serialized (status, byte_headers, body_or_view) tuple."""
    sp_str_headers = _to_str_header_pairs(list(sp_headers)) if sp_headers else None
    if isinstance(sp_body, memoryview) and vr_cls is Response:
        submit(Response.from_buffer(sp_status, sp_body, sp_str_headers, request_id))
        return
    submit(
        vr_cls(
            status=sp_status,
            body=bytes(sp_body) if isinstance(sp_body, memoryview) else sp_body,
            headers=sp_str_headers,
            request_id=request_id,
        )
    )


async def _submit_async_storage_response(
    resp: StreamingStorageResponse,
    request_id: int,
    vr_cls: Any = Response,
    submit: Callable[[Any], None] = submit_response,
) -> None:
    """Resolve an AsyncObjectStore-backed StreamingStorageResponse and submit its payload."""
    sp_status, sp_headers, sp_body = await resp.to_async_fastpath_tuple()
    _submit_fastpath_tuple(sp_status, sp_headers, sp_body, request_id, vr_cls, submit)


def _dispatch_special_return(
    raw: Any,
    request_id: int,
    vr_cls: Any,
    submit: Callable[[Any], None],
    *,
    status_code: int = 200,
    stream_submitter: Callable[..., None] | None = None,
) -> bool | Coroutine[Any, Any, None]:
    """Handle Arrow, StreamingStorageResponse, unary tuples, or sync/async streaming returns."""
    if _is_async_storage_response(raw):
        return _submit_async_storage_response(raw, request_id, vr_cls, submit)

    special = serialize_fastpath_special_result(raw, default_status=status_code)
    if special is not None:
        sp_status, sp_headers, sp_body = special
        _submit_fastpath_tuple(sp_status, sp_headers, sp_body, request_id, vr_cls, submit)
        return True

    unary_tuple = _extract_unary_tuple_response(raw)
    if unary_tuple is not None:
        tup_status, tup_headers, tup_body = unary_tuple
        submit(vr_cls(status=tup_status, body=tup_body, headers=tup_headers or None, request_id=request_id))
        return True

    stream_match = _extract_streaming_response(raw, default_status=status_code)
    if stream_match is not None:
        st_status, st_headers, st_body = stream_match
        if isinstance(st_body, AsyncIterable):
            return _stream_async_iterable(
                request_id,
                st_status,
                st_headers,
                st_body,
                stream_submitter=stream_submitter,
            )
        _stream_sync_iterable(
            request_id,
            st_status,
            st_headers,
            st_body,
            stream_submitter=stream_submitter,
        )
        return True

    return False


def _try_submit_native_special_return(
    raw: Any,
    request_id: int,
    default_status: int = 200,
) -> bool | Coroutine[Any, Any, None]:
    """Handle streaming, Arrow, storage, or tuple native handler returns from drive_native_batch."""
    if isinstance(raw, ArrowCursor | RustArrowCursor | StreamingStorageResponse):
        return _dispatch_special_return(raw, request_id, Response, submit_response, status_code=default_status)
    if isinstance(raw, msgspec.Struct) or dataclasses.is_dataclass(raw):
        return False
    return _dispatch_special_return(raw, request_id, Response, submit_response, status_code=default_status)
