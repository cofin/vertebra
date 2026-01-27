"""Core runtime module for Vertebra.

Provides:
- lifecycle: Vertebra app class and lifecycle management
- supervisor: Process supervisor for worker management
- context: Request context and context variables
- worker: ASGI worker implementation
- entrypoint: CLI entrypoint and main function
- tasks: Background task scheduling
- events: Event bus pub/sub
"""

from vertebra.core.context import ContextKind, VertebraContext
from vertebra.core.events import EventBus, EventError, EventHandler
from vertebra.core.lifecycle import App
from vertebra.core.tasks import BackgroundTaskError, BackgroundTaskHandle, BackgroundTasks

__all__ = [
    "App",
    "BackgroundTaskError",
    "BackgroundTaskHandle",
    "BackgroundTasks",
    "ContextKind",
    "EventBus",
    "EventError",
    "EventHandler",
    "VertebraContext",
]
