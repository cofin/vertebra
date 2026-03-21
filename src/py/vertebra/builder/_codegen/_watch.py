"""Watch helpers for codegen-driven frontend development."""

import asyncio
import contextlib
import json
import logging
import threading
import time
import urllib.request
from collections.abc import Callable
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_IGNORED_PATH_PARTS = frozenset(
    {
        ".agent",
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

# ---------------------------------------------------------------------------
# File classification
# ---------------------------------------------------------------------------

_PYTHON_EXTS = frozenset({".py", ".pyi"})
_FRONTEND_EXTS = frozenset({".ts", ".tsx", ".js", ".jsx", ".css", ".scss", ".less", ".vue", ".svelte"})
_CONFIG_NAMES = frozenset({"pyproject.toml", "package.json", "tsconfig.json"})


def classify_file(filepath: str) -> str:
    """Classify a file into: ``'python'``, ``'frontend'``, ``'config'``, or ``'unknown'``."""
    p = Path(filepath)
    if p.name in _CONFIG_NAMES:
        return "config"
    suffix = p.suffix.lower()
    if suffix in _PYTHON_EXTS:
        return "python"
    if suffix in _FRONTEND_EXTS:
        return "frontend"
    return "unknown"


# ---------------------------------------------------------------------------
# HMR notifier (HTTP signal to Rust server)
# ---------------------------------------------------------------------------


class HmrNotifier:
    """Notify the Rust HMR endpoint after code generation completes."""

    def __init__(self, base_url: str = "http://127.0.0.1:8000") -> None:
        self._url = f"{base_url.rstrip('/')}/__vertebra/hmr/signal"

    def notify(self) -> None:
        """POST to the HMR signal endpoint without surfacing delivery errors."""
        request = urllib.request.Request(self._url, data=b"", method="POST")  # noqa: S310
        try:
            with urllib.request.urlopen(request, timeout=1):  # noqa: S310
                return
        except Exception:
            logger.debug(
                "HMR signal to %s failed (is the server running?)",
                self._url,
                exc_info=True,
            )


# ---------------------------------------------------------------------------
# Codegen file watcher (polling)
# ---------------------------------------------------------------------------


class CodegenWatcher:
    """Poll Python source files and rerun a callback when they change."""

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
        self._snapshot = self._take_snapshot()

    def start(self) -> None:
        """Start the polling watcher thread."""
        if self._thread is not None and self._thread.is_alive():
            return
        self._thread = threading.Thread(
            target=self._watch_loop,
            name="vertebra-codegen-watch",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        """Stop the watcher thread if it is running."""
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=max(self.poll_interval * 2, 0.1))

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


# ---------------------------------------------------------------------------
# HMR event router
# ---------------------------------------------------------------------------


class HmrEventRouter:
    """Routes HMR events from the file watcher based on file type.

    - ``.py`` files -> TypeBridge.invalidate() (type regeneration)
    - ``.ts``/``.tsx``/``.css`` -> broadcast ``'update'`` to HMR clients
    - Config files -> broadcast ``'full-reload'``

    Pattern: litecore HmrEventRouter
    """

    def __init__(
        self,
        bridge: Any = None,
        on_hmr_update: Callable[[list[str]], None] | None = None,
    ) -> None:
        self._bridge = bridge  # TypeBridge instance (optional)
        self._on_hmr_update = on_hmr_update  # callback for frontend changes
        self._hmr_clients: set[Any] = set()
        self._lock = threading.Lock()
        self._loop: asyncio.AbstractEventLoop | None = None

    # -- Client management ---------------------------------------------------

    def subscribe(self, ws: Any) -> None:
        """Register a WebSocket client for HMR broadcasts."""
        with self._lock:
            self._hmr_clients.add(ws)

    def unsubscribe(self, ws: Any) -> None:
        """Remove a WebSocket client from HMR broadcasts."""
        with self._lock:
            self._hmr_clients.discard(ws)

    @property
    def client_count(self) -> int:
        """Return the number of connected HMR clients."""
        with self._lock:
            return len(self._hmr_clients)

    # -- Event routing -------------------------------------------------------

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

        # Config changes trigger a hard reload and short-circuit.
        if config_change:
            self._broadcast({"type": "full-reload", "reason": "config change"})
            return

        # Python changes invalidate the type bridge for regeneration.
        if python_files and self._bridge is not None:
            self._bridge.invalidate(changed_files=[Path(p) for p in python_files])

        # Frontend changes are broadcast to all HMR clients.
        if frontend_files:
            updates = [{"path": f, "timestamp": time.time()} for f in frontend_files]
            self._broadcast({"type": "update", "updates": updates})
            if self._on_hmr_update:
                self._on_hmr_update(frontend_files)

    # -- Event loop binding --------------------------------------------------

    def set_event_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        """Bind an asyncio event loop for broadcasting to WebSocket clients."""
        self._loop = loop

    # -- Internal broadcast --------------------------------------------------

    def _broadcast(self, message: dict[str, Any]) -> None:
        """Broadcast JSON message to all connected HMR clients."""
        payload = json.dumps(message)
        loop = self._loop
        if loop is None:
            return
        with contextlib.suppress(RuntimeError):
            loop.call_soon_threadsafe(lambda: loop.create_task(self._send_to_clients(payload)))

    async def _send_to_clients(self, payload: str) -> None:
        """Send *payload* to every subscribed client, pruning stale ones."""
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


__all__ = ("CodegenWatcher", "HmrEventRouter", "HmrNotifier", "classify_file")
