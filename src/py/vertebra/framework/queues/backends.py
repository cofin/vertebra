"""Aggregated queue backend exports across memory, PostgreSQL, Oracle, Redis/Valkey, and Spanner."""

from __future__ import annotations

from vertebra.framework.queues.base import MemoryQueueBackend, QueueBackend
from vertebra.framework.queues.oracle import OracleQueueBackend, OracleQueueExecutor
from vertebra.framework.queues.postgres import PostgreSQLQueueBackend, PostgreSQLQueueExecutor
from vertebra.framework.queues.redis import RedisQueueBackend, ValkeyQueueBackend
from vertebra.framework.queues.spanner import SpannerQueueBackend, SpannerQueueExecutor

__all__ = (
    "MemoryQueueBackend",
    "OracleQueueBackend",
    "OracleQueueExecutor",
    "PostgreSQLQueueBackend",
    "PostgreSQLQueueExecutor",
    "QueueBackend",
    "RedisQueueBackend",
    "SpannerQueueBackend",
    "SpannerQueueExecutor",
    "ValkeyQueueBackend",
)
