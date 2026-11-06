"""Vertebra request batch partitioning, pre-compiled parameter/DI extraction, and dispatch execution."""

from __future__ import annotations

import asyncio
import concurrent.futures
import dataclasses
import inspect
import logging
import os
from collections.abc import Callable, Coroutine, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any, cast, get_args, get_origin
from urllib.parse import parse_qsl
from uuid import UUID

import msgspec

from vertebra._vertebra import (
    Response,
    drive_native_batch,
    submit_batch_raw,
    submit_response,
    submit_response_raw,
)
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
    "_resolve_asgi_batch_timeout",
    "_resolve_handler_args",
    "build_error_response",
    "compile_handler_spec",
    "compile_handler_table",
    "dispatch_batch",
    "partition_batch",
)

log = logging.getLogger(__name__)

_DEFAULT_ASGI_BATCH_TIMEOUT_SECONDS: float = 30.0
_UNSET_BATCH_TIMEOUT = object()
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
        special = serialize_fastpath_special_result(raw, default_status=status_code)
        if special is not None:
            sp_status, _sp_headers, sp_body = special
            if isinstance(sp_body, memoryview):
                if vr_cls is Response:
                    return Response.from_buffer(sp_status, sp_body, None, request_id)
                return vr_cls(status=sp_status, body=bytes(sp_body), request_id=request_id)
            return vr_cls(status=sp_status, body=sp_body, request_id=request_id)
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
) -> None:
    """Submit a handler response using raw fastpath or Py<Response> fallback."""
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
        special = serialize_fastpath_special_result(raw, default_status=status_code)
        if special is not None:
            sp_status, sp_headers, sp_body = special
            ct = next((v.decode("latin1") for k, v in sp_headers if k == b"content-type"), None)
            if ct is not None:
                submit_raw(request_id, sp_status, sp_body, ct)
            else:
                submit_raw(request_id, sp_status, sp_body)
            return
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
    try:
        raw = await spec.invoker(request)
        return _coerce_response(raw, getattr(request, "request_id", 0), vr_cls, status_code=spec.status_code)
    except Exception as exc:
        log.exception("Async handler error for request %s", getattr(request, "request_id", 0))
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
) -> None:
    """Await a pre-invoked handler coroutine without pinning Request across suspension."""
    try:
        raw = await coro
        _submit_coerced(raw, request_id, vr_cls, active_submit, active_submit_raw, status_code=status_code)
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


def _create_tasks_batch(loop: asyncio.AbstractEventLoop, coros: list[Coroutine[Any, Any, Any]]) -> None:
    """Create tasks for a list of coroutines inside the target event loop."""
    for coro in coros:
        loop.create_task(coro)


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
            for coro in coros:
                scheduler.create_task(coro)
        else:
            scheduler.call_soon_threadsafe(_create_tasks_batch, scheduler, coros)
    elif hasattr(scheduler, "primary_loop"):
        _schedule_coros(scheduler.primary_loop, coros)
    elif hasattr(scheduler, "loop"):
        _schedule_coros(scheduler.loop, coros)


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

    pool = executor or ThreadPoolExecutor(max_workers=max(32, len(entries)), thread_name_prefix="vertebra-wsgi")
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
                    )
                except Exception as exc:
                    err_resp = build_error_response(exc, request, error_handler, vr_cls)
                    active_submit(err_resp)

    if partitioned.sync_thread:
        active_exec = executor or ThreadPoolExecutor(max_workers=32, thread_name_prefix="vertebra-sync")
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
