"""Tests for codegen watch and HMR notification helpers."""

import urllib.request

from vertebra.build.codegen._watch import HmrNotifier


class _DummyResponse:
    def __enter__(self) -> "_DummyResponse":
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        return None


def test_hmr_notifier_posts_to_signal_endpoint(monkeypatch) -> None:
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


def test_hmr_notifier_swallows_delivery_errors(monkeypatch) -> None:
    """Notifier failures should be debug-logged, not raised to the caller."""

    def fake_urlopen(request: urllib.request.Request, *, timeout: float) -> _DummyResponse:
        raise OSError("connection refused")

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)

    notifier = HmrNotifier("http://127.0.0.1:9999")
    notifier.notify()
