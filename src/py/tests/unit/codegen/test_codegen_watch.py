"""Tests for codegen watch and HMR notification helpers."""

import threading
import time
import urllib.request
from pathlib import Path

import pytest

from vertebra.build.codegen._watch import CodegenWatcher, HmrNotifier


class _DummyResponse:
    def __enter__(self) -> "_DummyResponse":
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        return None


def test_hmr_notifier_posts_to_signal_endpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    """HmrNotifier.notify() should POST to the Rust signal endpoint."""
    calls: list[urllib.request.Request] = []

    def fake_urlopen(request: urllib.request.Request, *, timeout: float) -> _DummyResponse:
        calls.append(request)
        assert timeout == 1
        return _DummyResponse()

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)

    notifier = HmrNotifier("http://127.0.0.1:9000")
    notifier.notify()

    assert len(calls) == 1
    assert calls[0].full_url == "http://127.0.0.1:9000/__vertebra/hmr/signal"
    assert calls[0].get_method() == "POST"


def test_hmr_notifier_swallows_delivery_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    """Notifier failures should be debug-logged, not raised to the caller."""

    def fake_urlopen(request: urllib.request.Request, *, timeout: float) -> _DummyResponse:
        raise OSError("connection refused")

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)

    notifier = HmrNotifier("http://127.0.0.1:9999")
    notifier.notify()


def test_codegen_watcher_native_detects_changes(tmp_path: Path) -> None:
    """CodegenWatcher with native SourceWatcher should detect file modification and invoke callback."""
    src_file = tmp_path / "app.py"
    src_file.write_text("x = 1\n", encoding="utf-8")

    event = threading.Event()

    def on_change() -> None:
        event.set()

    watcher = CodegenWatcher(tmp_path, on_change, poll_interval=0.05)
    watcher.start()
    try:
        time.sleep(0.1)
        src_file.write_text("x = 2\n", encoding="utf-8")
        assert event.wait(timeout=2.0)
    finally:
        watcher.stop()
