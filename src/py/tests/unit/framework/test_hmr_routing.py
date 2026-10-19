"""Tests for HMR event routing and file classification."""

import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from vertebra.builder._codegen._watch import HmrEventRouter, classify_file


def test_classify_file_classify_python_file() -> None:
    assert classify_file("src/app.py") == "python"
    assert classify_file("src/types.pyi") == "python"


def test_classify_file_classify_frontend_file() -> None:
    assert classify_file("src/App.tsx") == "frontend"
    assert classify_file("src/style.css") == "frontend"
    assert classify_file("src/main.js") == "frontend"
    assert classify_file("src/Component.vue") == "frontend"
    assert classify_file("src/Component.svelte") == "frontend"


def test_classify_file_classify_frontend_extra_extensions() -> None:
    assert classify_file("src/index.ts") == "frontend"
    assert classify_file("src/Component.jsx") == "frontend"
    assert classify_file("src/styles.scss") == "frontend"
    assert classify_file("src/theme.less") == "frontend"


def test_classify_file_classify_config_file() -> None:
    assert classify_file("pyproject.toml") == "config"
    assert classify_file("package.json") == "config"
    assert classify_file("tsconfig.json") == "config"


def test_classify_file_classify_config_nested() -> None:
    assert classify_file("some/deep/dir/pyproject.toml") == "config"


def test_classify_file_classify_unknown() -> None:
    assert classify_file("README.md") == "unknown"
    assert classify_file("image.png") == "unknown"
    assert classify_file("data.csv") == "unknown"


def test_classify_file_classify_case_insensitive_extension() -> None:
    assert classify_file("src/App.PY") == "python"
    assert classify_file("src/App.TSX") == "frontend"


def test_hmr_event_router_clients_subscribe_unsubscribe() -> None:
    router = HmrEventRouter()
    mock_ws = object()
    router.subscribe(mock_ws)
    assert router.client_count == 1
    router.unsubscribe(mock_ws)
    assert router.client_count == 0


def test_hmr_event_router_clients_unsubscribe_missing_client_is_noop() -> None:
    router = HmrEventRouter()
    router.unsubscribe(object())
    assert router.client_count == 0


def test_hmr_event_router_clients_multiple_clients() -> None:
    router = HmrEventRouter()
    ws1, ws2, ws3 = object(), object(), object()
    router.subscribe(ws1)
    router.subscribe(ws2)
    router.subscribe(ws3)
    assert router.client_count == 3
    router.unsubscribe(ws2)
    assert router.client_count == 2


def test_hmr_event_router_routing_routes_python_to_bridge() -> None:
    bridge = MagicMock()
    router = HmrEventRouter(bridge=bridge)

    router.route_event(["src/app.py", "src/models.py"])

    bridge.invalidate.assert_called_once()
    call_args = bridge.invalidate.call_args
    changed = call_args[1]["changed_files"]
    assert changed == [Path("src/app.py"), Path("src/models.py")]


def test_hmr_event_router_routing_routes_frontend_to_callback() -> None:
    callback = MagicMock()
    router = HmrEventRouter(on_hmr_update=callback)

    router.route_event(["src/App.tsx", "src/style.css"])

    callback.assert_called_once_with(["src/App.tsx", "src/style.css"])


def test_hmr_event_router_routing_config_triggers_full_reload() -> None:
    bridge = MagicMock()
    callback = MagicMock()
    router = HmrEventRouter(bridge=bridge, on_hmr_update=callback)

    with patch.object(router, "_broadcast") as mock_broadcast:
        router.route_event(["pyproject.toml"])

    mock_broadcast.assert_called_once_with({"type": "full-reload", "reason": "config change"})
    bridge.invalidate.assert_not_called()
    callback.assert_not_called()


def test_hmr_event_router_routing_mixed_files_routes_both() -> None:
    bridge = MagicMock()
    callback = MagicMock()
    router = HmrEventRouter(bridge=bridge, on_hmr_update=callback)

    router.route_event(["src/app.py", "src/App.tsx", "src/style.css"])

    bridge.invalidate.assert_called_once()
    callback.assert_called_once_with(["src/App.tsx", "src/style.css"])


def test_hmr_event_router_routing_unknown_files_ignored() -> None:
    bridge = MagicMock()
    callback = MagicMock()
    router = HmrEventRouter(bridge=bridge, on_hmr_update=callback)

    router.route_event(["README.md", "image.png"])

    bridge.invalidate.assert_not_called()
    callback.assert_not_called()


def test_hmr_event_router_routing_no_bridge_skips_invalidation() -> None:
    router = HmrEventRouter()
    router.route_event(["src/app.py"])


def test_hmr_event_router_routing_empty_changeset_is_noop() -> None:
    bridge = MagicMock()
    router = HmrEventRouter(bridge=bridge)
    router.route_event([])
    bridge.invalidate.assert_not_called()


def test_hmr_event_router_broadcast_sends_to_clients() -> None:
    """Verify _send_to_clients delivers payloads and prunes stale."""
    router = HmrEventRouter()

    good_ws = MagicMock()
    good_ws.send_text = AsyncMock()

    bad_ws = MagicMock()
    bad_ws.send_text = AsyncMock(side_effect=ConnectionError("gone"))

    router.subscribe(good_ws)
    router.subscribe(bad_ws)

    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(router._send_to_clients('{"type":"update"}'))
    finally:
        loop.close()

    good_ws.send_text.assert_called_once_with('{"type":"update"}')
    assert router.client_count == 1


def test_hmr_event_router_broadcast_without_loop_is_noop() -> None:
    router = HmrEventRouter()
    router._broadcast({"type": "update", "updates": []})


def test_hmr_event_router_broadcast_set_event_loop() -> None:
    router = HmrEventRouter()
    loop = MagicMock()
    router.set_event_loop(loop)
    assert router._loop is loop
