"""Pub/sub channel bus backed by SHM rings, Valkey, Redis, PostgreSQL LISTEN/NOTIFY, Oracle AQ, and CDC."""

from __future__ import annotations

from vertebra.db.channels import DatabaseChannelBackend

__all__ = ("ChannelBus",)


class ChannelBus(DatabaseChannelBackend):
    """Framework pub/sub channel bus backed by SHM ring buffers, Valkey, Redis, PostgreSQL, or Oracle AQ."""

    __slots__ = ()
