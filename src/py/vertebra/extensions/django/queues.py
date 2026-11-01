"""Django 6.0+ django.tasks compatible QueuesBackend."""

from __future__ import annotations

from typing import Any

from vertebra.extensions.flask.queues import QueueBackend, TaskEnvelope

__all__ = (
    "QueuesBackend",
    "TaskEnvelope",
)


class QueuesBackend(QueueBackend):
    """Drop-in ``TASKS["default"]["BACKEND"]`` for Django background tasks."""

    __slots__ = ("_alias", "_options")

    def __init__(self, alias: str = "default", params: dict[str, Any] | None = None) -> None:
        super().__init__()
        self._alias = alias
        self._options = dict(params or {})
