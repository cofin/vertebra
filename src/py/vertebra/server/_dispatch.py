"""Vertebra request batch partitioning and dispatch execution."""

from __future__ import annotations

import asyncio
import concurrent.futures
import inspect
import logging
from collections.abc import Callable, Coroutine, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import msgspec

from vertebra._vertebra import (
    Response,
    submit_response,
    submit_response_raw,
)
from vertebra.utils.serializers import encode_json_bytes

__all__ = (
    "BatchDispatchPlan",
    "HandlerSpec",
    "_call_handler",
    "_handler_meta",
    "_resolve_handler_args",
    "build_error_response",
    "compile_handler_spec",
    "compile_handler_table",
    "dispatch_batch",
    "partition_batch",
)

log = logging.getLogger(__name__)

_REQUEST_PARAMS = frozenset({"request", "req"})
_JSON_CONTENT_TYPE_COMPACT = b"\x0ccontent-type\x10\x00application/json"


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


class HandlerSpec(msgspec.Struct, frozen=True, gc=False, cache_hash=True):
    """Pre-compiled dispatch specification and invoker for a route handler."""

    has_params: bool
    is_async: bool
    param_names: tuple[str, ...]
    invoker: Callable[..., Any]


def compile_handler_spec(handler: Callable[..., Any]) -> HandlerSpec:
    """Compile parameter extractors and invocation strategy for a handler."""
    sig = inspect.signature(handler)
    params = tuple(sig.parameters.keys())
    has_params = bool(params)
    is_async = inspect.iscoroutinefunction(handler)

    if not has_params:

        def _invoke_no_args(_request: Any, _h: Any = handler) -> Any:
            return _h()

        invoker: Callable[[Any], Any] = _invoke_no_args
    elif len(params) == 1 and params[0] in _REQUEST_PARAMS:
        invoker = handler
    else:
        extractors = tuple(_PARAM_EXTRACTORS.get(name, _extract_request) for name in params)

        def _invoke_with_extractors(
            request: Any,
            _h: Any = handler,
            _ext: tuple[Callable[[Any], Any], ...] = extractors,
        ) -> Any:
            return _h(*(fn(request) for fn in _ext))

        invoker = _invoke_with_extractors

    return HandlerSpec(
        has_params=has_params,
        is_async=is_async,
        param_names=params,
        invoker=invoker,
    )


def compile_handler_table(handlers: Mapping[int, Callable[..., Any]]) -> dict[int, HandlerSpec]:
    """Pre-compile dispatch specs for all registered handler callables."""
    return {hid: compile_handler_spec(fn) for hid, fn in handlers.items()}


def build_error_response(
    exc: Exception,
    request: Any,
    error_handler: Callable[..., Any] | None = None,
    vr_cls: Any = Response,
) -> Any:
    """Build an error Response using the configured error handler or standard 500."""
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

    if hasattr(vr_cls, "error"):
        try:
            return vr_cls.error(500, request_id)
        except Exception:
            pass
    return vr_cls(status=500, body=b"internal server error", request_id=request_id)


def _coerce_response(raw: Any, request_id: int, vr_cls: Any) -> Any:
    """Turn a handler return value into a Response object."""
    if isinstance(raw, vr_cls):
        if getattr(raw, "request_id", 0) == 0:
            raw.request_id = request_id
        return raw
    if isinstance(raw, bytes):
        body = raw
    elif isinstance(raw, str):
        body = raw.encode("utf-8")
    else:
        body = encode_json_bytes(raw)
    return vr_cls(status=200, body=body, request_id=request_id)


def _submit_coerced(
    raw: Any,
    request_id: int,
    vr_cls: Any,
    submit: Callable[[Any], None],
    submit_raw: Callable[..., None] | None = None,
) -> None:
    """Submit a handler response using raw fastpath or Py<Response> fallback."""
    if submit_raw is not None:
        if isinstance(raw, bytes):
            submit_raw(request_id, 200, raw)
            return
        if isinstance(raw, str):
            submit_raw(request_id, 200, raw.encode("utf-8"))
            return
        if isinstance(raw, vr_cls):
            if getattr(raw, "request_id", 0) == 0:
                raw.request_id = request_id
            submit(raw)
            return
        submit_raw(request_id, 200, encode_json_bytes(raw), _JSON_CONTENT_TYPE_COMPACT)
        return

    submit(_coerce_response(raw, request_id, vr_cls))


def _invoke_sync_handler(
    spec: HandlerSpec,
    request: Any,
    error_handler: Callable[..., Any] | None = None,
    vr_cls: Any = Response,
) -> Any:
    """Invoke a synchronous handler and return a coerced Response or error Response."""
    try:
        raw = spec.invoker(request)
        return _coerce_response(raw, getattr(request, "request_id", 0), vr_cls)
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
        return _coerce_response(raw, getattr(request, "request_id", 0), vr_cls)
    except Exception as exc:
        log.exception("Async handler error for request %s", getattr(request, "request_id", 0))
        return build_error_response(exc, request, error_handler, vr_cls)


class BatchDispatchPlan(msgspec.Struct, frozen=True, gc=False):
    """Result of partitioning a batch into dispatch groups."""

    sync_native: list[tuple[int, Any, HandlerSpec]] = msgspec.field(default_factory=list)
    async_native: list[tuple[int, Any, HandlerSpec]] = msgspec.field(default_factory=list)
    mounts: dict[str, list[tuple[int, Any]]] = msgspec.field(default_factory=dict)
    unmatched: list[tuple[int, Any]] = msgspec.field(default_factory=list)


def partition_batch(
    batch: Any,
    handlers: Mapping[int, Any],
    mount_runners: Sequence[tuple[str, Any, str]],
) -> BatchDispatchPlan:
    """Classify each request in a batch into sync, async, mount, or unmatched groups."""
    plan = BatchDispatchPlan()
    sync_native = plan.sync_native
    async_native = plan.async_native
    mounts_map = plan.mounts
    unmatched = plan.unmatched
    get_handler = handlers.get

    for idx, request in enumerate(batch):
        handler_obj = get_handler(request.handler_id)
        if handler_obj is not None:
            spec = handler_obj if isinstance(handler_obj, HandlerSpec) else compile_handler_spec(handler_obj)
            if spec.is_async:
                async_native.append((idx, request, spec))
            else:
                sync_native.append((idx, request, spec))
            continue

        if mount_runners:
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
            + len(partitioned.async_native)
            + sum(len(v) for v in partitioned.mounts.values())
            + len(partitioned.unmatched)
        )
        results: list[Any] = [None] * total

        for idx, request, spec in partitioned.sync_native:
            results[idx] = _invoke_sync_handler(spec, request, error_handler, vr_cls)

        if partitioned.async_native:
            coros = [
                _invoke_async_handler(spec, request, error_handler, vr_cls)
                for _idx, request, spec in partitioned.async_native
            ]
            if hasattr(scheduler, "schedule_batch"):
                futures = scheduler.schedule_batch(coros)
                concurrent.futures.wait(futures)
                for (idx, _request, _spec), fut in zip(partitioned.async_native, futures, strict=True):
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

    if partitioned.sync_native:
        if len(partitioned.sync_native) == 1 and not partitioned.async_native and not partitioned.mounts:
            _idx, request, spec = partitioned.sync_native[0]
            try:
                raw = spec.invoker(request)
                _submit_coerced(raw, getattr(request, "request_id", 0), vr_cls, active_submit, active_submit_raw)
            except Exception as exc:
                err_resp = build_error_response(exc, request, error_handler, vr_cls)
                active_submit(err_resp)
        else:
            active_exec = executor or ThreadPoolExecutor(max_workers=32, thread_name_prefix="vertebra-sync")
            for _idx, request, spec in partitioned.sync_native:

                def _dispatch_sync_item(
                    req: Any = request,
                    s: HandlerSpec = spec,
                    eh: Any = error_handler,
                ) -> None:
                    try:
                        raw = s.invoker(req)
                        _submit_coerced(raw, getattr(req, "request_id", 0), vr_cls, active_submit, active_submit_raw)
                    except Exception as exc:
                        err_resp = build_error_response(exc, req, eh, vr_cls)
                        active_submit(err_resp)

                active_exec.submit(_dispatch_sync_item)

    if partitioned.async_native:
        async_coros: list[Coroutine[Any, Any, None]] = []
        for _idx, request, spec in partitioned.async_native:

            async def _dispatch_async_item(
                req: Any = request,
                s: HandlerSpec = spec,
                eh: Any = error_handler,
            ) -> None:
                try:
                    raw = await s.invoker(req)
                    _submit_coerced(raw, getattr(req, "request_id", 0), vr_cls, active_submit, active_submit_raw)
                except Exception as exc:
                    err_resp = build_error_response(exc, req, eh, vr_cls)
                    active_submit(err_resp)

            async_coros.append(_dispatch_async_item())

        _schedule_coros(scheduler or loop, async_coros)

    if partitioned.mounts:
        resolved_lookup = mount_lookup or {prefix: (adapter, protocol) for prefix, adapter, protocol in mount_runners}
        for prefix, entries in partitioned.mounts.items():
            adapter, protocol = resolved_lookup[prefix]
            if protocol == "asgi":
                adapter.spawn_batch(req for _idx, req in entries)
            elif protocol == "rsgi":
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
                active_exec = executor or ThreadPoolExecutor(max_workers=32, thread_name_prefix="vertebra-wsgi")
                for _idx, request in entries:

                    def _dispatch_wsgi_item(req: Any = request, a: Any = adapter) -> None:
                        try:
                            resp = a(req)
                            active_submit(resp)
                        except Exception as exc:
                            err_resp = build_error_response(exc, req, error_handler, vr_cls)
                            active_submit(err_resp)

                    active_exec.submit(_dispatch_wsgi_item)

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

    return None


def _handler_meta(handler: Any) -> tuple[bool, bool]:
    """Return has_params and is_async flags for a handler."""
    spec = compile_handler_spec(handler)
    return spec.has_params, spec.is_async


def _resolve_handler_args(handler: Any, request: Any) -> tuple[Any, ...]:
    """Resolve handler argument tuple for a request."""
    spec = compile_handler_spec(handler)
    if not spec.param_names:
        return ()
    return tuple(_PARAM_EXTRACTORS.get(name, _extract_request)(request) for name in spec.param_names)


def _call_handler(handler: Any, request: Any, *, loop: asyncio.AbstractEventLoop | None = None) -> Any:
    """Call a handler with parameter resolution."""
    spec = compile_handler_spec(handler)
    result = spec.invoker(request)
    if spec.is_async:
        if loop is None:
            msg = "async handler called without an event loop"
            raise RuntimeError(msg)
        return loop.run_until_complete(result)
    return result
