"""Server-Side Rendering (SSR) subdomain supporting QuickJS slots and Unix-socket V8 worker pools."""

from __future__ import annotations

import struct
from typing import Any, Literal

import msgspec

from vertebra.framework._inertia import SsrClient, SsrResult
from vertebra.framework._ssr._backends import create_ssr_client
from vertebra.utils.serializers import encode_json_bytes

__all__ = (
    "SsrClient",
    "SsrConfig",
    "SsrFrame",
    "SsrResult",
    "create_ssr_client",
)


class SsrConfig(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Configuration for QuickJS embedded slots and persistent Unix-socket V8/Node/Bun SSR workers."""

    mode: Literal["quickjs", "unix_socket", "http_sidecar"] = "quickjs"
    bundle_path: str = "dist/ssr/ssr.js"
    pool_size: int = 4
    socket_path: str = "/tmp/vertebra-ssr.sock"
    sidecar_url: str = "http://127.0.0.1:13714"
    timeout_seconds: float = 2.0


class SsrFrame(msgspec.Struct, frozen=True, gc=False, kw_only=True, array_like=True):
    """Length-prefixed IPC frame exchanged with a persistent V8/Node/Bun SSR worker over Unix socket."""

    component: str
    props: dict[str, Any] = msgspec.field(default_factory=dict)

    @classmethod
    def encode(cls, component: str, props: dict[str, Any]) -> bytes:
        """Encode an SSR render request into a 4-byte big-endian length-prefixed JSON wire frame."""
        return cls(component=component, props=props).to_wire_bytes()

    def to_wire_bytes(self) -> bytes:
        """Encode the SSR request as a 4-byte big-endian length-prefixed JSON frame."""
        payload = encode_json_bytes({"component": self.component, "props": self.props})
        return struct.pack(">I", len(payload)) + payload
