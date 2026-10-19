"""Dev TUI for Vertebra — scrollable log panel with fixed header and status bar.

Used by ``vtb dev`` in TTY mode. Falls back to standard scrolling output
when not in a TTY or when ``--no-tui`` is passed.
"""

import re
import threading
import time
from collections import deque
from dataclasses import dataclass
from typing import IO

from rich.console import Console, Group, RenderableType
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.text import Text

from vertebra.cli.theme import ACCENT, ERROR, SECONDARY, SYM_BULLET, WARNING

__all__ = ("DevLogFormatter", "LogBuffer", "LogLine", "run_dev_tui", "stderr_reader")

_DEFAULT_CAPACITY = 500
_WORKER_PREFIX = re.compile(r"^\[vertebra(?:\s+worker\s+(\d+))?\]\s*")
_LEVEL_PATTERNS = {
    "error": re.compile(r"\b(error|exception|traceback|failed)\b", re.IGNORECASE),
    "warning": re.compile(r"\b(warning|warn|deprecated)\b", re.IGNORECASE),
    "debug": re.compile(r"\b(debug)\b", re.IGNORECASE),
}
_LEVEL_STYLES = {
    "error": ERROR,
    "warning": WARNING,
    "info": "",
    "debug": SECONDARY,
}


@dataclass(slots=True)
class LogLine:
    """A parsed log line with metadata."""

    raw: str
    level: str = "info"
    source: str = ""
    message: str = ""


class LogBuffer:
    """Thread-safe ring buffer for log lines."""

    __slots__ = ("_buf", "_lock", "capacity")

    def __init__(self, capacity: int = _DEFAULT_CAPACITY) -> None:
        self.capacity = capacity
        self._buf: deque[LogLine] = deque(maxlen=capacity)
        self._lock = threading.Lock()

    def append(self, line: LogLine) -> None:
        """Append a parsed log line to the ring buffer."""
        with self._lock:
            self._buf.append(line)

    def get_lines(self, n: int | None = None) -> list[LogLine]:
        """Return the last N lines, or all lines when ``n`` is ``None``."""
        with self._lock:
            if n is None:
                return list(self._buf)
            return list(self._buf)[-n:]

    def __len__(self) -> int:
        with self._lock:
            return len(self._buf)


class DevLogFormatter:
    """Parse and colorize log lines for the dev TUI."""

    @staticmethod
    def parse(raw: str) -> LogLine:
        """Parse a raw stderr line into a ``LogLine``."""
        line = raw.rstrip()
        source = ""
        level = "info"

        match = _WORKER_PREFIX.match(line)
        if match:
            worker_id = match.group(1)
            source = f"worker.{worker_id}" if worker_id else "server"
            line = line[match.end() :]

        for lvl, pattern in _LEVEL_PATTERNS.items():
            if pattern.search(line):
                level = lvl
                break

        return LogLine(raw=raw.rstrip(), level=level, source=source, message=line)

    @staticmethod
    def render(line: LogLine) -> Text:
        """Render a ``LogLine`` as a Rich ``Text`` object."""
        text = Text()
        style = _LEVEL_STYLES.get(line.level, "")

        if line.source:
            text.append(f"[{line.source}] ", style=SECONDARY)

        text.append(line.message, style=style)
        return text


def stderr_reader(stream: IO[str], log_buffer: LogBuffer, stop_event: threading.Event) -> None:
    """Read lines from a stream and append them to the log buffer."""
    try:
        for line in iter(stream.readline, ""):
            if stop_event.is_set():
                break
            parsed = DevLogFormatter.parse(line)
            log_buffer.append(parsed)
    except (ValueError, OSError):
        pass


def run_dev_tui(
    *,
    log_buffer: LogBuffer,
    mode: str = "dev",
    host: str = "127.0.0.1",
    port: int = 8000,
    banner_text: str = "",
) -> Live:
    """Create and return a Rich ``Live`` display for the dev TUI."""
    start_time = time.time()
    tui_console = Console(stderr=True)

    def make_layout() -> Layout:
        layout = Layout()
        layout.split_column(
            Layout(name="header", size=3),
            Layout(name="logs"),
            Layout(name="status", size=1),
        )
        if banner_text:
            layout["header"].update(Text(banner_text, style=f"bold {ACCENT}"))
        else:
            layout["header"].update(Text(f"  ▜▄▛ vertebra — {mode}", style=f"bold {ACCENT}"))
        layout["logs"].update(_build_log_panel(log_buffer))
        layout["status"].update(_build_status_bar(start_time, mode, host, port))
        return layout

    return Live(
        make_layout(),
        console=tui_console,
        refresh_per_second=4,
        transient=True,
    )


def _build_log_panel(log_buffer: LogBuffer, max_lines: int = 30) -> Panel:
    """Build the scrollable log panel from buffer contents."""
    lines = log_buffer.get_lines(max_lines)
    content: RenderableType
    if not lines:
        content = Text("  Waiting for output…", style=SECONDARY)
    else:
        rendered = [DevLogFormatter.render(line) for line in lines]
        content = Group(*rendered)

    return Panel(
        content,
        title="[bold]logs[/bold]",
        title_align="left",
        border_style=SECONDARY,
        padding=(0, 1),
    )


def _build_status_bar(start_time: float, mode: str, host: str, port: int) -> Text:
    """Build the bottom status bar."""
    elapsed = time.time() - start_time
    minutes, seconds = divmod(int(elapsed), 60)
    uptime = f"{minutes}:{seconds:02d}"

    text = Text()
    text.append(f"  {SYM_BULLET} ", style=SECONDARY)
    text.append(mode, style=f"bold {ACCENT}")
    text.append(f" {SYM_BULLET} ", style=SECONDARY)
    text.append(f"{host}:{port}", style="")
    text.append(f" {SYM_BULLET} ", style=SECONDARY)
    text.append(f"uptime {uptime}", style=SECONDARY)
    return text
