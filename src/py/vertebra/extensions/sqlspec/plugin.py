"""Application plugin for SQLSpec and VertebraSQLSpecStorageAdapter integration."""

from dataclasses import dataclass, field
from typing import Any

from vertebra.extensions.sqlspec.storage import VertebraSQLSpecStorageAdapter
from vertebra.storage import VertebraStore

__all__ = (
    "SQLSPEC_PLUGIN_PRIORITY",
    "SQLSPEC_STATE_KEY",
    "SQLSpecState",
    "VertebraSQLSpecPlugin",
)

SQLSPEC_PLUGIN_PRIORITY: int = -5800
SQLSPEC_STATE_KEY: str = "sqlspec"


@dataclass(slots=True)
class SQLSpecState:
    """Runtime state container registered under state['sqlspec']."""

    pools: dict[str | None, Any] = field(default_factory=dict)
    configs: dict[str | None, Any] = field(default_factory=dict)
    object_store: VertebraSQLSpecStorageAdapter | None = None
    is_available: bool = True


@dataclass(slots=True)
class VertebraSQLSpecPlugin:
    """Plugin binding SQLSpec configurations and VertebraStore adapters into application state."""

    config: Any = None
    store: VertebraStore | None = None
    storage_prefix: str = ""
    storage_key: str = "vertebra"
    priority: int = SQLSPEC_PLUGIN_PRIORITY
    _state: SQLSpecState | None = field(default=None, init=False, repr=False)

    def install_state(self, state_mapping: dict[str, Any]) -> SQLSpecState:
        """Populate and return the SQLSpecState in a mutable state dictionary."""
        adapter = (
            VertebraSQLSpecStorageAdapter(self.store, prefix=self.storage_prefix) if self.store is not None else None
        )
        configs: dict[str | None, Any] = {}
        if self.config is not None:
            configs[self.storage_key] = self.config
        state = SQLSpecState(configs=configs, object_store=adapter, is_available=True)
        self._state = state
        state_mapping[SQLSPEC_STATE_KEY] = state
        return state

    def on_app_init(self, app_config: Any) -> Any:
        """Litestar InitPluginProtocol hook registering SQLSpecState on app_config.state."""
        target_state = getattr(app_config, "state", None)
        if isinstance(target_state, dict):
            self.install_state(target_state)
        elif target_state is not None and hasattr(target_state, "update"):
            holder: dict[str, Any] = {}
            self.install_state(holder)
            target_state.update(holder)
        return app_config
