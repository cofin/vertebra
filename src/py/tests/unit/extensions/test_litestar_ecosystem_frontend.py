"""Unit tests for Vertebra Vite, AsyncAPI, and HTMX integrations."""

from __future__ import annotations

import subprocess
import sys

import pytest
from litestar import Litestar, get
from litestar.testing import TestClient

from vertebra.extensions.litestar.asyncapi import create_asyncapi_plugin
from vertebra.extensions.litestar.htmx import create_htmx_plugin
from vertebra.extensions.litestar.vite import (
    _StaticAfterRequestHook,
    _StaticBeforeRequestHook,
    create_vite_plugin,
    get_vertebra_client_path,
    get_vertebra_client_vite_aliases,
)
from vertebra.extensions.litestar.vite.plugin import ViteBridge, ViteConfig
from vertebra.typing import MissingDependencyError
from vertebra.utils.module_loader import ensure_module


def test_vite_client_path_and_aliases() -> None:
    """Verify get_vertebra_client_path and Vite alias mappings."""
    client_path = get_vertebra_client_path()
    assert client_path is not None

    aliases = get_vertebra_client_vite_aliases()
    assert "@vertebra/client" in aliases


def test_create_vite_plugin() -> None:
    """Verify create_vite_plugin constructs a slotted VitePlugin subclass with client_aliases."""
    from litestar_vite import ViteConfig as LitestarViteConfig
    from litestar_vite import VitePlugin

    plugin = create_vite_plugin()
    assert isinstance(plugin, VitePlugin)
    assert plugin.client_aliases == get_vertebra_client_vite_aliases()
    assert "@vertebra/client" in plugin.client_aliases
    assert "@vertebra/client/vite" in plugin.client_aliases

    plugin_without_aliases = create_vite_plugin(include_client_alias=False)
    assert isinstance(plugin_without_aliases, VitePlugin)
    assert plugin_without_aliases.client_aliases == {}

    custom_config = LitestarViteConfig(dev_mode=True)
    custom_plugin = create_vite_plugin(config=custom_config, include_client_alias=False)
    assert isinstance(custom_plugin, VitePlugin)
    assert custom_plugin._config is custom_config
    assert custom_plugin.client_aliases == {}


def test_create_asyncapi_plugin() -> None:
    """Verify create_asyncapi_plugin constructs an AsyncAPIPlugin instance."""
    plugin = create_asyncapi_plugin(title="API Specs", version="2.0.0", path="/api-docs")
    assert type(plugin).__name__ == "AsyncAPIPlugin"


def test_create_htmx_plugin() -> None:
    """Verify create_htmx_plugin constructs an HTMXPlugin instance."""
    plugin = create_htmx_plugin()
    assert type(plugin).__name__ == "HTMXPlugin"


def test_missing_dependency_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify MissingDependencyError for decoupled frontend integrations."""
    monkeypatch.setattr("vertebra.utils.module_loader.import_optional", lambda name: None)

    with pytest.raises(MissingDependencyError) as vite_exc:
        ensure_module("litestar_vite", extra="vite")
    assert "vite" in vite_exc.value.install_package

    with pytest.raises(MissingDependencyError) as asyncapi_exc:
        ensure_module("litestar_asyncapi", extra="asyncapi")
    assert "asyncapi" in asyncapi_exc.value.install_package

    with pytest.raises(MissingDependencyError) as htmx_exc:
        ensure_module("litestar_htmx", extra="htmx")
    assert "htmx" in htmx_exc.value.install_package


def test_vite_plugin_and_hooks_litestar_roundtrip() -> None:
    """VitePlugin and static asset hooks serve requests cleanly on a real Litestar app."""
    before_hook = _StaticBeforeRequestHook(asset_url="/static/")
    after_hook = _StaticAfterRequestHook(max_age=3600)
    bridge = ViteBridge(ViteConfig(dev_mode=True, dev_server_url="http://localhost:5173"))

    @get("/static/app.js", before_request=before_hook, after_request=after_hook, sync_to_thread=False)
    def asset_route() -> dict[str, str]:
        return {"tag": bridge.asset_tags("src/main.ts")}

    @get("/static/.vite/manifest.json", before_request=before_hook, sync_to_thread=False)
    def blocked_manifest() -> dict[str, str]:
        return {"should": "not-reach"}

    app = Litestar(route_handlers=[asset_route, blocked_manifest], plugins=[create_vite_plugin()])
    with TestClient(app=app) as client:
        ok_resp = client.get("/static/app.js")
        assert ok_resp.status_code == 200
        assert "http://localhost:5173/src/main.ts" in ok_resp.json()["tag"]
        blocked_resp = client.get("/static/.vite/manifest.json")
        assert blocked_resp.status_code == 404


@pytest.mark.parametrize(
    "module_name",
    [
        "vertebra.extensions.litestar.vite.plugin",
        "vertebra.extensions.litestar.mcp.card",
        "vertebra.extensions.litestar.mcp.plugin",
        "vertebra.extensions.litestar.mcp.tasks",
    ],
)
def test_frontend_and_mcp_modules_import_with_host_packages_blocked(module_name: str) -> None:
    """Vite and MCP Litestar submodules import cleanly when litestar, litestar_vite, and litestar_mcp are blocked."""
    script = (
        "import importlib, sys\n"
        "for mod in ('litestar', 'litestar.response', 'litestar.params', 'litestar_vite', "
        "'litestar_vite.config', 'litestar_vite.config._paths', 'litestar_mcp'):\n"
        "    sys.modules[mod] = None\n"
        f"imported = importlib.import_module({module_name!r})\n"
        "from vertebra.typing import MissingDependencyError\n"
        f"if {module_name!r}.endswith('vite.plugin'):\n"
        "    try:\n"
        "        imported.create_vite_plugin()\n"
        "    except MissingDependencyError:\n"
        "        pass\n"
        "    else:\n"
        "        raise AssertionError('Expected MissingDependencyError for create_vite_plugin')\n"
        f"elif {module_name!r}.endswith('mcp.plugin'):\n"
        "    try:\n"
        "        imported.create_mcp_plugin()\n"
        "    except MissingDependencyError:\n"
        "        pass\n"
        "    else:\n"
        "        raise AssertionError('Expected MissingDependencyError for create_mcp_plugin')\n"
        f"elif {module_name!r}.endswith('mcp.tasks'):\n"
        "    try:\n"
        "        imported.create_shm_mcp_task_config()\n"
        "    except MissingDependencyError:\n"
        "        pass\n"
        "    else:\n"
        "        raise AssertionError('Expected MissingDependencyError for create_shm_mcp_task_config')\n"
        f"elif {module_name!r}.endswith('mcp.card'):\n"
        "    try:\n"
        "        imported.WellKnownAgentCardOffload.from_card({'name': 'a'})\n"
        "    except MissingDependencyError:\n"
        "        pass\n"
        "    else:\n"
        "        raise AssertionError('Expected MissingDependencyError for WellKnownAgentCardOffload')\n"
    )
    proc = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr
