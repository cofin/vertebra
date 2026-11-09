"""Unit tests for Vertebra Vite, AsyncAPI, HTMX, and SAQ integrations."""

from __future__ import annotations

import pytest

from vertebra.extensions.litestar.asyncapi import create_asyncapi_plugin
from vertebra.extensions.litestar.htmx import create_htmx_plugin
from vertebra.extensions.litestar.saq import create_saq_plugin
from vertebra.extensions.litestar.vite import (
    create_vite_plugin,
    get_vertebra_client_path,
    get_vertebra_client_vite_aliases,
)
from vertebra.typing import MissingDependencyError
from vertebra.utils.module_loader import ensure_module


def test_vite_client_path_and_aliases() -> None:
    """Verify get_vertebra_client_path and Vite alias mappings."""
    client_path = get_vertebra_client_path()
    assert client_path is not None

    aliases = get_vertebra_client_vite_aliases()
    assert "@vertebra/client" in aliases


def test_create_vite_plugin() -> None:
    """Verify create_vite_plugin constructs a VitePlugin instance."""
    plugin = create_vite_plugin()
    assert type(plugin).__name__ == "VitePlugin"


def test_create_asyncapi_plugin() -> None:
    """Verify create_asyncapi_plugin constructs an AsyncAPIPlugin instance."""
    plugin = create_asyncapi_plugin(title="API Specs", version="2.0.0", path="/api-docs")
    assert type(plugin).__name__ == "AsyncAPIPlugin"


def test_create_htmx_plugin() -> None:
    """Verify create_htmx_plugin constructs an HTMXPlugin instance."""
    plugin = create_htmx_plugin()
    assert type(plugin).__name__ == "HTMXPlugin"


def test_create_saq_plugin() -> None:
    """Verify create_saq_plugin constructs an SAQPlugin with Redis configuration."""
    plugin = create_saq_plugin(redis_url="redis://localhost:6379")
    assert type(plugin).__name__ == "SAQPlugin"


def test_missing_dependency_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify MissingDependencyError for decoupled frontend and SAQ integrations."""
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

    with pytest.raises(MissingDependencyError) as saq_exc:
        ensure_module("litestar_saq", extra="saq")
    assert "saq" in saq_exc.value.install_package

    with pytest.raises(MissingDependencyError) as saq_plugin_exc:
        create_saq_plugin(redis_url="redis://localhost:6379")
    assert saq_plugin_exc.value.package == "litestar-saq"
    assert saq_plugin_exc.value.install_package == "vertebra-core[saq]"
