"""Vertebra pub/sub channel backend adapter for FastAPI applications."""

from __future__ import annotations

from vertebra.extensions.starlette.channels import ChannelsBackend, attach_channels

__all__ = (
    "ChannelsBackend",
    "attach_channels",
)
