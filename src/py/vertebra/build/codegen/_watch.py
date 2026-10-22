"""Watch helpers for codegen-driven frontend development backed by Rust SourceWatcher."""

import asyncio
import contextlib
import importlib
import logging
import threading
import time
import urllib.request
from collections.abc import Callable
from pathlib import Path
from typing import Any

from vertebra.utils.serializers import encode_json

__all__ = ("CodegenWatcher", "HmrEventRouter", "HmrNotifier", "classify_file")

logger = logging.getLogger(__name__)

_IGNORED_PATH_PARTS = frozenset(
    {
        ".agent",
        ".agents",
        ".git",
        ".mypy_cache",
        ".pytest_cache",
        ".ruff_cache",
        ".venv",
        "__pycache__",
        "build",
        "dist",
        "node_modules",
        "target",
    }
)

_PYTHON_EXTS = frozenset({".py", ".pyi"})
_FRONTEND_EXTS = frozenset({".ts", ".tsx", ".js", ".jsx", ".css", ".scss", ".less", ".vue", ".svelte"})
_CONFIG_NAMES = frozenset({"pyproject.toml", "package.json", "tsconfig.json"})


def classify_file(filepath: str) -> str:
    """Classify a file into ``'python'``, ``'frontend'``, ``'config'``, or ``'unknown'``."""
    p = Path(filepath)
    if p.name in _CONFIG_NAMES:
        return "config"
    suffix = p.suffix.lower()
    if suffix in _PYTHON_EXTS:
        return "python"
    if suffix in _FRONTEND_EXTS:
        return "frontend"
    return "unknown"


class HmrNotifier:
    """Notify the Rust HMR endpoint after code generation completes."""

    def __init__(self, base_url: str = "http://127.0.0.1:8000") -> None:
        self._url = f"{base_url.rstrip('/')}/__vertebra/hmr/signal"

    def notify(self) -> None:
        """POST to the HMR signal endpoint without surfacing delivery errors."""
        self.notify_reload()

    def notify_reload(self, reason: str = "") -> None:
        """POST a reload notification to the HMR signal endpoint."""
        data = encode_json({"reason": reason}).encode("utf-8") if reason else b""
        request = urllib.request.Request(self._url, data=data, method="POST")
        try:
            with urllib.request.urlopen(request, timeout=1):
                return
        except Exception:
            logger.debug(
                "HMR signal to %s failed (is the server running?)",
                self._url,
                exc_info=True,
            )


class CodegenWatcher:
    """Watch Python source files via native Rust SourceWatcher (with mtime fallback) and rerun a callback."""

    def __init__(
        self,
        project_dir: Path,
        callback: Callable[[], None],
        *,
        poll_interval: float = 0.5,
    ) -> None:
        self.project_dir = project_dir.resolve()
        self.callback = callback
        self.poll_interval = poll_interval
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._native_watcher: Any = None
        self._snapshot = self._take_snapshot()

    @property
    def uses_native_watcher(self) -> bool:
        """Return True when backed by the Rust notify-based SourceWatcher."""
        return self._native_watcher is not None

    def start(self) -> None:
        """Start watching Python source files using native SourceWatcher when available."""
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._native_watcher = self._try_create_native_watcher()
        target = self._native_watch_loop if self._native_watcher is not None else self._watch_loop
        self._thread = threading.Thread(
            target=target,
            name="vertebra-codegen-watch",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        """Stop the watcher thread and release native OS file descriptors."""
        self._stop_event.set()
        if self._native_watcher is not None:
            with contextlib.suppress(Exception):
                self._native_watcher.stop()
        if self._thread is not None:
            self._thread.join(timeout=max(self.poll_interval * 2, 0.2))
        self._native_watcher = None

    def _try_create_native_watcher(self) -> Any:
        try:
            bundler_mod = importlib.import_module("vertebra._bundler")
            watcher_cls = getattr(bundler_mod, "SourceWatcher", None)
            if watcher_cls is None:
                return None
            debounce_ms = max(int(self.poll_interval * 1000), 20)
            watcher = watcher_cls([str(self.project_dir)], ["py", "pyi"], debounce_ms)
            watcher.start()
            return watcher
        except Exception:
            return None

    def _native_watch_loop(self) -> None:
        """Poll events from native SourceWatcher in a background thread."""
        watcher = self._native_watcher
        if watcher is None:
            return
        timeout_ms = max(int(self.poll_interval * 1000), 25)
        try:
            while not self._stop_event.is_set():
                try:
                    changed = watcher.poll_events(timeout_ms)
                except Exception:
                    if self._stop_event.is_set():
                        break
                    logger.warning("CodegenWatcher poll error", exc_info=True)
                    continue
                if not changed:
                    continue
                relevant = [p for p in changed if _should_watch(Path(p))]
                if not relevant:
                    continue
                try:
                    self.callback()
                except Exception:
                    logger.exception("Codegen watcher callback failed")
        finally:
            with contextlib.suppress(Exception):
                watcher.stop()

    def _watch_loop(self) -> None:
        while not self._stop_event.wait(self.poll_interval):
            snapshot = self._take_snapshot()
            if snapshot == self._snapshot:
                continue
            self._snapshot = snapshot
            try:
                self.callback()
            except Exception:
                logger.exception("Codegen watcher callback failed")
            self._snapshot = self._take_snapshot()

    def _take_snapshot(self) -> dict[str, int]:
        snapshot: dict[str, int] = {}
        for path in self.project_dir.rglob("*.py"):
            if not _should_watch(path):
                continue
            try:
                snapshot[str(path)] = path.stat().st_mtime_ns
            except OSError:
                continue
        return snapshot


class HmrEventRouter:
    """Route HMR events from the file watcher based on file type."""

    def __init__(
        self,
        bridge: Any = None,
        on_hmr_update: Callable[[list[str]], None] | None = None,
    ) -> None:
        self._bridge = bridge
        self._on_hmr_update = on_hmr_update
        self._hmr_clients: set[Any] = set()
        self._lock = threading.Lock()
        self._loop: asyncio.AbstractEventLoop | None = None

    @property
    def client_count(self) -> int:
        """Return the number of connected HMR clients."""
        with self._lock:
            return len(self._hmr_clients)

    def subscribe(self, ws: Any) -> None:
        """Register a WebSocket client for HMR broadcasts."""
        with self._lock:
            self._hmr_clients.add(ws)

    def unsubscribe(self, ws: Any) -> None:
        """Remove a WebSocket client from HMR broadcasts."""
        with self._lock:
            self._hmr_clients.discard(ws)

    def route_event(self, changed_files: list[str]) -> None:
        """Route file changes to appropriate handlers."""
        frontend_files: list[str] = []
        python_files: list[str] = []
        config_change = False

        for f in changed_files:
            category = classify_file(f)
            if category == "python":
                python_files.append(f)
            elif category == "frontend":
                frontend_files.append(f)
            elif category == "config":
                config_change = True

        if config_change:
            self._broadcast({"type": "full-reload", "reason": "config change"})
            return

        if python_files and self._bridge is not None:
            self._bridge.invalidate(changed_files=[Path(p) for p in python_files])

        if frontend_files:
            updates = [{"path": f, "timestamp": time.time()} for f in frontend_files]
            self._broadcast({"type": "update", "updates": updates})
            if self._on_hmr_update:
                self._on_hmr_update(frontend_files)

    def set_event_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        """Bind an asyncio event loop for broadcasting to WebSocket clients."""
        self._loop = loop

    def _broadcast(self, message: dict[str, Any]) -> None:
        payload = encode_json(message)
        loop = self._loop
        if loop is None:
            return
        with contextlib.suppress(RuntimeError):
            loop.call_soon_threadsafe(lambda: loop.create_task(self._send_to_clients(payload)))

    async def _send_to_clients(self, payload: str) -> None:
        with self._lock:
            clients = list(self._hmr_clients)
        stale: list[Any] = []
        for client in clients:
            try:
                await client.send_text(payload)
            except Exception:
                stale.append(client)
        if stale:
            with self._lock:
                for c in stale:
                    self._hmr_clients.discard(c)


def _should_watch(path: Path) -> bool:
    return not any(part in _IGNORED_PATH_PARTS for part in path.parts)
