"""Vertebra RSGI v1.6 adapter and protocol detection helpers."""

from __future__ import annotations

from vertebra.server._rsgi._adapter import RsgiAdapter, detect_interface, is_rsgi_app

__all__ = (
    "RsgiAdapter",
    "detect_interface",
    "is_rsgi_app",
)
