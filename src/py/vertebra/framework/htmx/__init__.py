"""HTMX request detection, HX-* response header builders, and HTML fragment responses."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal

import msgspec

if TYPE_CHECKING:
    from collections.abc import Mapping

__all__ = (
    "HtmxRequest",
    "HtmxResponse",
    "HxSwap",
)

type HxSwap = Literal[
    "innerHTML",
    "outerHTML",
    "beforebegin",
    "afterbegin",
    "beforeend",
    "afterend",
    "delete",
    "none",
]


@dataclass(frozen=True, slots=True)
class HtmxRequest:
    """Parsed HTMX request headers (`HX-Request`, `HX-Target`, `HX-Trigger`, `HX-Boosted`)."""

    is_htmx: bool = False
    is_boosted: bool = False
    is_history_restore_request: bool = False
    current_url: str | None = None
    prompt: str | None = None
    target: str | None = None
    trigger: str | None = None
    trigger_name: str | None = None

    @classmethod
    def from_headers(cls, headers: Mapping[str, str]) -> HtmxRequest:
        """Construct an ``HtmxRequest`` from case-insensitive HTTP request headers."""
        lower = {k.lower(): v for k, v in headers.items()}
        return cls(
            is_htmx=lower.get("hx-request") == "true",
            is_boosted=lower.get("hx-boosted") == "true",
            is_history_restore_request=lower.get("hx-history-restore-request") == "true",
            current_url=lower.get("hx-current-url"),
            prompt=lower.get("hx-prompt"),
            target=lower.get("hx-target"),
            trigger=lower.get("hx-trigger"),
            trigger_name=lower.get("hx-trigger-name"),
        )


@dataclass(frozen=True, slots=True)
class HtmxResponse:
    """HTML fragment response with typed ``HX-*`` response headers."""

    html: str
    status_code: int = 200
    push_url: str | bool | None = None
    replace_url: str | bool | None = None
    reswap: HxSwap | None = None
    retarget: str | None = None
    reselect: str | None = None
    redirect: str | None = None
    refresh: bool = False
    trigger: str | dict[str, Any] | None = None
    trigger_after_settle: str | dict[str, Any] | None = None
    trigger_after_swap: str | dict[str, Any] | None = None
    extra_headers: dict[str, str] = field(default_factory=dict)

    def to_headers(self) -> dict[str, str]:
        """Build the HTTP response header dictionary including ``HX-*`` directives."""
        headers: dict[str, str] = {"content-type": "text/html; charset=utf-8"}
        headers.update(self.extra_headers)
        if self.push_url is not None:
            headers["HX-Push-Url"] = (
                "false" if self.push_url is False else ("true" if self.push_url is True else self.push_url)
            )
        if self.replace_url is not None:
            headers["HX-Replace-Url"] = (
                "false" if self.replace_url is False else ("true" if self.replace_url is True else self.replace_url)
            )
        if self.reswap is not None:
            headers["HX-Reswap"] = self.reswap
        if self.retarget is not None:
            headers["HX-Retarget"] = self.retarget
        if self.reselect is not None:
            headers["HX-Reselect"] = self.reselect
        if self.redirect is not None:
            headers["HX-Redirect"] = self.redirect
        if self.refresh:
            headers["HX-Refresh"] = "true"
        for hdr_name, val in (
            ("HX-Trigger", self.trigger),
            ("HX-Trigger-After-Settle", self.trigger_after_settle),
            ("HX-Trigger-After-Swap", self.trigger_after_swap),
        ):
            if val is not None:
                headers[hdr_name] = val if isinstance(val, str) else msgspec.json.encode(val).decode("utf-8")
        return headers
