"""Unified context wrapper for native app entry points."""

from __future__ import annotations

from typing import Any, Literal

import msgspec

ContextKind = Literal["http", "cli", "event"]


class VertebraContext(msgspec.Struct, frozen=True):
    """Context wrapper for HTTP, CLI, and event execution."""

    kind: ContextKind
    request: Any | None = None
    cli: Any | None = None
    event: Any | None = None
    state: dict[str, Any] = msgspec.field(default_factory=dict)

    @classmethod
    def http(cls, request: Any, **state: Any) -> "VertebraContext":
        return cls(kind="http", request=request, state=state)

    @classmethod
    def cli(cls, cli: Any, **state: Any) -> "VertebraContext":
        return cls(kind="cli", cli=cli, state=state)

    @classmethod
    def event(cls, event: Any, **state: Any) -> "VertebraContext":
        return cls(kind="event", event=event, state=state)

    @property
    def is_http(self) -> bool:
        return self.kind == "http"

    @property
    def is_cli(self) -> bool:
        return self.kind == "cli"

    @property
    def is_event(self) -> bool:
        return self.kind == "event"

    def get(self, key: str, default: Any | None = None) -> Any:
        return self.state.get(key, default)

    def with_state(self, **updates: Any) -> "VertebraContext":
        if not updates:
            return self
        next_state = dict(self.state)
        next_state.update(updates)
        return VertebraContext(
            kind=self.kind,
            request=self.request,
            cli=self.cli,
            event=self.event,
            state=next_state,
        )


__all__ = ("ContextKind", "VertebraContext")
