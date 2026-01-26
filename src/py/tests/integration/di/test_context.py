from __future__ import annotations

import pytest

from vertebra.core.context import VertebraContext


class DummyRequest:
    def __init__(self, path: str) -> None:
        self.path = path


class DummyCli:
    def __init__(self, command: str) -> None:
        self.command = command


@pytest.mark.integration
def test_http_context_supports_request_and_state() -> None:
    request = DummyRequest("/health")
    ctx = VertebraContext.http(request, trace_id="abc123")

    assert ctx.kind == "http"
    assert ctx.request is request
    assert ctx.cli is None
    assert ctx.event is None
    assert ctx.get("trace_id") == "abc123"


@pytest.mark.integration
def test_cli_context_supports_command_and_state() -> None:
    cli = DummyCli("serve")
    ctx = VertebraContext.from_cli(cli, dry_run=True)

    assert ctx.kind == "cli"
    assert ctx.cli is cli
    assert ctx.request is None
    assert ctx.event is None
    assert ctx.get("dry_run") is True


@pytest.mark.integration
def test_event_context_supports_event_payloads() -> None:
    ctx = VertebraContext.from_event("startup", payload={"mode": "native"})

    assert ctx.kind == "event"
    assert ctx.event == "startup"
    assert ctx.request is None
    assert ctx.cli is None
    assert ctx.get("payload") == {"mode": "native"}


@pytest.mark.integration
def test_context_state_updates_are_immutable() -> None:
    request = DummyRequest("/state")
    ctx = VertebraContext.http(request, trace_id="abc123")
    next_ctx = ctx.with_state(span_id="def456")

    assert ctx.get("span_id") is None
    assert next_ctx.get("trace_id") == "abc123"
    assert next_ctx.get("span_id") == "def456"
