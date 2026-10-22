"""HTMX request detection, HX-* response header builders, and HTML fragment responses."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Literal

import msgspec

from vertebra.utils.serializers import encode_json

if TYPE_CHECKING:
    from collections.abc import Sequence

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


def _tristate(value: bool | str) -> str:
    """Format boolean or string URL into an HTMX header value."""
    if value is False:
        return "false"
    if value is True:
        return "true"
    return value


def _lookup_header(headers: Mapping[str, str], *names: str) -> str | None:
    """Case-insensitive header lookup without full dictionary reallocation."""
    for name in names:
        val = headers.get(name)
        if val is not None:
            return val
    for key, val in headers.items():
        key_lower = key.lower()
        for name in names:
            if key_lower == name.lower():
                return val
    return None


class HtmxRequest(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Parsed HTMX request headers (HX-Request, HX-Target, HX-Trigger, HX-Boosted)."""

    is_htmx: bool = False
    is_boosted: bool = False
    is_history_restore_request: bool = False
    current_url: str | None = None
    prompt: str | None = None
    target: str | None = None
    trigger: str | None = None
    trigger_name: str | None = None

    @classmethod
    def from_headers(cls, headers: Mapping[str, str] | Sequence[tuple[bytes, bytes]]) -> HtmxRequest:
        """Construct an HtmxRequest from case-insensitive HTTP request headers or raw wire tuples."""
        if not isinstance(headers, Mapping):
            raw_map: dict[str, str] = {}
            for k_bytes, v_bytes in headers:
                raw_map[k_bytes.decode("latin1").lower()] = v_bytes.decode("latin1")
            return cls(
                is_htmx=raw_map.get("hx-request") == "true",
                is_boosted=raw_map.get("hx-boosted") == "true",
                is_history_restore_request=raw_map.get("hx-history-restore-request") == "true",
                current_url=raw_map.get("hx-current-url"),
                prompt=raw_map.get("hx-prompt"),
                target=raw_map.get("hx-target"),
                trigger=raw_map.get("hx-trigger"),
                trigger_name=raw_map.get("hx-trigger-name"),
            )

        return cls(
            is_htmx=_lookup_header(headers, "hx-request", "HX-Request") == "true",
            is_boosted=_lookup_header(headers, "hx-boosted", "HX-Boosted") == "true",
            is_history_restore_request=_lookup_header(
                headers, "hx-history-restore-request", "HX-History-Restore-Request"
            )
            == "true",
            current_url=_lookup_header(headers, "hx-current-url", "HX-Current-URL"),
            prompt=_lookup_header(headers, "hx-prompt", "HX-Prompt"),
            target=_lookup_header(headers, "hx-target", "HX-Target"),
            trigger=_lookup_header(headers, "hx-trigger", "HX-Trigger"),
            trigger_name=_lookup_header(headers, "hx-trigger-name", "HX-Trigger-Name"),
        )


class HtmxResponse(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """HTML fragment response with typed HX-* response headers."""

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
    extra_headers: dict[str, str] = msgspec.field(default_factory=dict)

    def to_headers(self) -> dict[str, str]:
        """Build the HTTP response header dictionary including HX-* directives."""
        headers: dict[str, str] = {"content-type": "text/html; charset=utf-8"}
        headers.update(self.extra_headers)
        if self.push_url is not None:
            headers["HX-Push-Url"] = _tristate(self.push_url)
        if self.replace_url is not None:
            headers["HX-Replace-Url"] = _tristate(self.replace_url)
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
                headers[hdr_name] = val if isinstance(val, str) else encode_json(val)
        return headers
