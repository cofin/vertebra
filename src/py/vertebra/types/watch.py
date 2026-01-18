"""File watching helpers for Type Bridge regeneration."""

from __future__ import annotations

import threading
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterable

    from vertebra.types.bridge import TypeBridge

try:
    from watchdog.events import FileSystemEventHandler
    from watchdog.observers import Observer
except ImportError:  # pragma: no cover - optional dependency
    FileSystemEventHandler = object  # type: ignore[misc,assignment]
    Observer = None


@dataclass(slots=True)
class WatchOptions:
    paths: tuple[Path, ...]
    debounce_ms: int = 100


class _DebouncedHandler(FileSystemEventHandler):
    def __init__(self, bridge: TypeBridge, debounce_ms: int) -> None:
        super().__init__()
        self._bridge = bridge
        self._debounce_ms = debounce_ms
        self._lock = threading.Lock()
        self._timer: threading.Timer | None = None

    def _schedule(self) -> None:
        with self._lock:
            if self._timer is not None:
                self._timer.cancel()
            self._timer = threading.Timer(self._debounce_ms / 1000.0, self._flush)
            self._timer.daemon = True
            self._timer.start()

    def _flush(self) -> None:
        self._bridge.invalidate()

    def on_modified(self, event: Any) -> None:  # type: ignore[override]
        if event.is_directory:
            return
        if event.src_path.endswith(".py"):
            self._schedule()

    def on_created(self, event: Any) -> None:  # type: ignore[override]
        if event.is_directory:
            return
        if event.src_path.endswith(".py"):
            self._schedule()

    def on_moved(self, event: Any) -> None:  # type: ignore[override]
        if event.is_directory:
            return
        if event.dest_path.endswith(".py"):
            self._schedule()

    def stop(self) -> None:
        with self._lock:
            if self._timer is not None:
                self._timer.cancel()
                self._timer = None


class TypeBridgeWatcher:
    """Watch Python sources and invalidate the type bridge cache."""

    def __init__(
        self,
        bridge: TypeBridge,
        paths: Iterable[Path],
        *,
        debounce_ms: int = 100,
    ) -> None:
        self._bridge = bridge
        self._options = WatchOptions(tuple(Path(p) for p in paths), debounce_ms)
        self._observer = Observer() if Observer else None
        self._handler: _DebouncedHandler | None = None

    @property
    def available(self) -> bool:
        return self._observer is not None

    def start(self) -> None:
        if self._observer is None:
            return
        handler = _DebouncedHandler(self._bridge, self._options.debounce_ms)
        self._handler = handler
        for path in self._options.paths:
            self._observer.schedule(handler, str(path), recursive=True)
        self._observer.start()

    def stop(self) -> None:
        if self._observer is None:
            return
        self._observer.stop()
        self._observer.join(timeout=1)
        if self._handler is not None:
            self._handler.stop()


__all__ = ["TypeBridgeWatcher", "WatchOptions"]
