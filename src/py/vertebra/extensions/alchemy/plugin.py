"""Application plugin for Advanced Alchemy and VertebraAlchemyStorageBackend integration."""

from dataclasses import dataclass, field
from typing import Any

from vertebra.extensions.alchemy.storage import VertebraAlchemyStorageBackend
from vertebra.storage import VertebraStore

__all__ = (
    "AA_PLUGIN_PRIORITY",
    "AA_STATE_KEY",
    "SQLAlchemyState",
    "VertebraAlchemyPlugin",
)

AA_PLUGIN_PRIORITY: int = -5600
AA_STATE_KEY: str = "advanced_alchemy"


@dataclass(slots=True)
class SQLAlchemyState:
    """Runtime state container registered under state['advanced_alchemy']."""

    engines: dict[str | None, Any] = field(default_factory=dict)
    session_makers: dict[str | None, Any] = field(default_factory=dict)
    configs: dict[str | None, Any] = field(default_factory=dict)
    storage_backend: VertebraAlchemyStorageBackend | None = None
    is_available: bool = True


@dataclass(slots=True)
class VertebraAlchemyPlugin:
    """Plugin binding Advanced Alchemy configurations and VertebraStore backends into application state."""

    config: Any = None
    store: VertebraStore | None = None
    storage_key: str = "vertebra"
    storage_prefix: str = ""
    priority: int = AA_PLUGIN_PRIORITY
    _state: SQLAlchemyState | None = field(default=None, init=False, repr=False)

    def install_state(self, state_mapping: dict[str, Any]) -> SQLAlchemyState:
        """Populate and return the SQLAlchemyState in a mutable state dictionary."""
        backend = (
            VertebraAlchemyStorageBackend(
                key=self.storage_key,
                store=self.store,
                prefix=self.storage_prefix,
            )
            if self.store is not None
            else None
        )
        configs: dict[str | None, Any] = {}
        if self.config is not None:
            configs[self.storage_key] = self.config
        state = SQLAlchemyState(configs=configs, storage_backend=backend, is_available=True)
        self._state = state
        state_mapping[AA_STATE_KEY] = state
        return state

    def on_app_init(self, app_config: Any) -> Any:
        """Litestar InitPluginProtocol hook registering SQLAlchemyState on app_config.state."""
        target_state = getattr(app_config, "state", None)
        if isinstance(target_state, dict):
            self.install_state(target_state)
        elif target_state is not None and hasattr(target_state, "update"):
            holder: dict[str, Any] = {}
            self.install_state(holder)
            target_state.update(holder)
        return app_config
