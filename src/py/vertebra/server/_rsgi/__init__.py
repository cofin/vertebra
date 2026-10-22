"""Vertebra RSGI v1.6 adapter and protocol detection helpers."""

from vertebra.server._protocol import InterfaceMode, detect_interface, is_rsgi_app
from vertebra.server._rsgi._adapter import RsgiAdapter

__all__ = (
    "InterfaceMode",
    "RsgiAdapter",
    "detect_interface",
    "is_rsgi_app",
)
