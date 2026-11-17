"""Tests for codegen watch and HMR notification helpers."""

import threading
import urllib.request
from pathlib import Path
from typing import Any

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


def test_codegen_watcher_native_detects_changes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CodegenWatcher with native SourceWatcher should detect file modification and invoke callback."""
    src_file = tmp_path / "app.py"
    src_file.write_text("x = 1\n", encoding="utf-8")

    ready = threading.Event()
    event = threading.Event()
    orig_try_create = CodegenWatcher._try_create_native_watcher

    class _NativeWatcherProxy:
        def __init__(self, inner: Any) -> None:
            self._inner = inner

        def poll_events(self, timeout_ms: int) -> Any:
            ready.set()
            return self._inner.poll_events(timeout_ms)

        def __getattr__(self, name: str) -> Any:
            return getattr(self._inner, name)

    def wrapped_try_create(self: CodegenWatcher) -> Any:
        native = orig_try_create(self)
        if native is not None:
            return _NativeWatcherProxy(native)
        ready.set()
        return None

    monkeypatch.setattr(CodegenWatcher, "_try_create_native_watcher", wrapped_try_create)

    def on_change() -> None:
        event.set()

    watcher = CodegenWatcher(tmp_path, on_change, poll_interval=0.05)
    watcher.start()
    try:
        assert ready.wait(timeout=2.0)
        src_file.write_text("x = 2\n", encoding="utf-8")
        assert event.wait(timeout=2.0)
    finally:
        watcher.stop()
