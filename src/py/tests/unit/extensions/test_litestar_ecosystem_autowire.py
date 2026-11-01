"""Unit tests for Vertebra litestar-autowire integration."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from vertebra.extensions.litestar.autowire import (
    AutowireBridgeIntegration,
    create_autowire_plugin,
    ensure_autowire_before_route_compiler,
)
from vertebra.extensions.litestar.plugin import LitestarPlugin
from vertebra.typing import MissingDependencyError
from vertebra.utils.module_loader import ensure_module


def test_autowire_bridge_integration_name_and_on_autowire() -> None:
    """Verify AutowireBridgeIntegration marks app state as vertebra_autowired."""
    bridge = AutowireBridgeIntegration()
    assert bridge.name == "vertebra"

    context = MagicMock()
    context.app_config.state = {}
    bridge.on_autowire(context)
    assert context.app_config.state.get("vertebra_autowired") is True


def test_ensure_autowire_before_route_compiler_reordering() -> None:
    """Verify ensure_autowire_before_route_compiler places AutowirePlugin before LitestarPlugin."""
    app_config = MagicMock()
    autowire_plugin = create_autowire_plugin(domain_packages="app.domain")
    litestar_plugin = LitestarPlugin()

    app_config.plugins = [litestar_plugin, autowire_plugin]
    reordered = ensure_autowire_before_route_compiler(app_config)

    assert reordered.plugins[0] is autowire_plugin
    assert reordered.plugins[1] is litestar_plugin


def test_create_autowire_plugin_defaults() -> None:
    """Verify create_autowire_plugin constructs AutowirePlugin with default integrations."""
    plugin = create_autowire_plugin(domain_packages=("app.controllers",))
    assert type(plugin).__name__ == "AutowirePlugin"


def test_missing_dependency_error_on_missing_autowire(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify MissingDependencyError with extra='autowire' when litestar_autowire is absent."""
    monkeypatch.setattr("vertebra.utils.module_loader.import_optional", lambda name: None)
    with pytest.raises(MissingDependencyError) as exc_info:
        ensure_module("litestar_autowire", extra="autowire")
    assert "autowire" in exc_info.value.install_package
