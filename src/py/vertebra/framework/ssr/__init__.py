"""Server-Side Rendering (SSR) subdomain supporting QuickJS slots and Unix-socket V8 worker pools."""

from __future__ import annotations

import struct
from dataclasses import dataclass
from typing import Any, Literal

import msgspec

from vertebra.framework._inertia import SsrClient, SsrResult
from vertebra.framework._ssr._backends import create_ssr_client

__all__ = (
    "SsrClient",
    "SsrConfig",
    "SsrResult",
    "UnixSocketSsrFrame",
    "create_ssr_client",
    "encode_ssr_frame",
)


@dataclass(frozen=True, slots=True)
class SsrConfig:
    """Configuration for QuickJS embedded slots and persistent Unix-socket V8/Node/Bun SSR workers."""

    mode: Literal["quickjs", "unix_socket", "http_sidecar"] = "quickjs"
    bundle_path: str = "dist/ssr/ssr.js"
    pool_size: int = 4
    socket_path: str = "/tmp/vertebra-ssr.sock"
    sidecar_url: str = "http://127.0.0.1:13714"
    timeout_seconds: float = 2.0


@dataclass(frozen=True, slots=True)
class UnixSocketSsrFrame:
    """Length-prefixed IPC frame exchanged with a persistent V8/Node/Bun SSR worker over Unix socket."""

    component: str
    props: dict[str, Any]

    def to_wire_bytes(self) -> bytes:
        """Encode the SSR request as a 4-byte big-endian length-prefixed JSON frame."""
        payload = msgspec.json.encode({"component": self.component, "props": self.props})
        return struct.pack(">I", len(payload)) + payload


def encode_ssr_frame(component: str, props: dict[str, Any]) -> bytes:
    """Encode an SSR render request into a 4-byte big-endian length-prefixed JSON wire frame."""
    return UnixSocketSsrFrame(component=component, props=props).to_wire_bytes()
