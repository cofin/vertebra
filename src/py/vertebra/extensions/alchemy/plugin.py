"""Application plugin and PEP 695 generic configurations for Advanced Alchemy integration."""

from __future__ import annotations

import contextlib
import importlib
import inspect
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Annotated, Any, cast

from vertebra.extensions.alchemy.broker import BrokerPlugin, BrokerTransport
from vertebra.extensions.alchemy.storage import ObjectStoreBackend
from vertebra.storage import ObjectStore

__all__ = (
    "AA_PLUGIN_PRIORITY",
    "AA_STATE_KEY",
    "AdvancedAlchemy",
    "AlchemyPlugin",
    "AlchemyStorePlugin",
    "SQLAlchemyAsyncConfig",
    "SQLAlchemyPlugin",
    "SQLAlchemyState",
    "SQLAlchemySyncConfig",
    "VertebraAlchemyPlugin",
    "synthesize_skip_validation_annotation",
    "wrap_config",
)

AA_PLUGIN_PRIORITY: int = -5600
AA_STATE_KEY: str = "advanced_alchemy"


def _is_in_memory_uri(connection_string: str | None) -> bool:
    if not connection_string:
        return False
    lowered = connection_string.lower()
    return ":memory:" in lowered or "mode=memory" in lowered


def _inject_engine_pool_defaults(engine_config: dict[str, Any], connection_string: str | None) -> dict[str, Any]:
    merged = dict(engine_config)
    if not _is_in_memory_uri(connection_string):
        merged.setdefault("pool_pre_ping", True)
        merged.setdefault("pool_recycle", 1800)
    return merged


def synthesize_skip_validation_annotation(target_type: Any) -> Any:
    """Wrap target_type in Litestar's SkipValidation when Litestar is installed."""
    try:
        typing_mod = importlib.import_module("litestar.typing")
        skip_val = getattr(typing_mod, "SkipValidation", None)
        if skip_val is not None:
            return Annotated[target_type, skip_val]
    except ImportError:
        pass
    return target_type


@dataclass(slots=True)
class SQLAlchemyAsyncConfig[
    EngineT,
    SessionT,
    SessionMakerT,
]:
    """Generic asynchronous SQLAlchemy configuration preserving EngineT, SessionT, and SessionMakerT."""

    connection_string: str = "sqlite+aiosqlite:///:memory:"
    bind_key: str = "default"
    session_dependency_key: str = "db_session"
    engine_dependency_key: str = "db_engine"
    engine_config: dict[str, Any] = field(default_factory=dict)
    session_config: dict[str, Any] = field(default_factory=dict)
    engine_instance: EngineT | None = None
    session_maker_instance: SessionMakerT | None = None
    is_async: bool = field(default=True, init=False)

    def __post_init__(self) -> None:
        """Populate default pool_pre_ping and pool_recycle settings on pooled engines."""
        self.engine_config = _inject_engine_pool_defaults(self.engine_config, self.connection_string)

    def provide_engine(self) -> EngineT:
        """Return the configured asynchronous engine instance."""
        return cast("EngineT", self.engine_instance)

    def provide_session(self) -> SessionT:
        """Return a typed asynchronous session from the configured sessionmaker."""
        if callable(self.session_maker_instance):
            return cast("SessionT", self.session_maker_instance())
        return cast("SessionT", self.session_maker_instance)


@dataclass(slots=True)
class SQLAlchemySyncConfig[
    EngineT,
    SessionT,
    SessionMakerT,
]:
    """Generic synchronous SQLAlchemy configuration preserving EngineT, SessionT, and SessionMakerT."""

    connection_string: str = "sqlite:///:memory:"
    bind_key: str = "default"
    session_dependency_key: str = "db_session"
    engine_dependency_key: str = "db_engine"
    engine_config: dict[str, Any] = field(default_factory=dict)
    session_config: dict[str, Any] = field(default_factory=dict)
    engine_instance: EngineT | None = None
    session_maker_instance: SessionMakerT | None = None
    is_async: bool = field(default=False, init=False)

    def __post_init__(self) -> None:
        """Populate default pool_pre_ping and pool_recycle settings on pooled engines."""
        self.engine_config = _inject_engine_pool_defaults(self.engine_config, self.connection_string)

    def provide_engine(self) -> EngineT:
        """Return the configured synchronous engine instance."""
        return cast("EngineT", self.engine_instance)

    def provide_session(self) -> SessionT:
        """Return a typed synchronous session from the configured sessionmaker."""
        if callable(self.session_maker_instance):
            return cast("SessionT", self.session_maker_instance())
        return cast("SessionT", self.session_maker_instance)


def wrap_config[ConfigT](config: ConfigT) -> ConfigT:
    """Inject production pool health defaults into an Advanced Alchemy config while preserving ConfigT."""
    conn_str = getattr(config, "connection_string", None)
    if not _is_in_memory_uri(conn_str):
        engine_cfg = getattr(config, "engine_config", None)
        if isinstance(engine_cfg, dict):
            engine_cfg.setdefault("pool_pre_ping", True)
            engine_cfg.setdefault("pool_recycle", 1800)
        elif engine_cfg is not None:
            for attr, val in (("pool_pre_ping", True), ("pool_recycle", 1800)):
                if hasattr(engine_cfg, attr) and getattr(engine_cfg, attr) is None:
                    with contextlib.suppress(AttributeError, TypeError):
                        setattr(engine_cfg, attr, val)
    return config


@dataclass(slots=True)
class SQLAlchemyState:
    """Runtime state container registered under state['advanced_alchemy']."""

    engines: dict[str | None, Any] = field(default_factory=dict)
    session_makers: dict[str | None, Any] = field(default_factory=dict)
    configs: dict[str | None, Any] = field(default_factory=dict)
    storage_backend: ObjectStoreBackend | None = None
    broker: BrokerTransport | None = None
    is_available: bool = True


@dataclass(slots=True)
class AdvancedAlchemy[ConfigT]:
    """Plugin binding Advanced Alchemy generic configurations and ObjectStoreBackend into application state."""

    config: ConfigT | Sequence[ConfigT] | None = None
    store: ObjectStore | None = None
    storage_key: str = "vertebra"
    storage_prefix: str = ""
    broker: BrokerPlugin | None = None
    priority: int = AA_PLUGIN_PRIORITY
    _state: SQLAlchemyState | None = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        """Apply pool health defaults to wrapped config object(s)."""
        if self.config is not None:
            if isinstance(self.config, Sequence) and not isinstance(self.config, (str, bytes)):
                for item in self.config:
                    wrap_config(item)
            else:
                wrap_config(self.config)

    @property
    def configs(self) -> tuple[ConfigT, ...]:
        """Return all registered database configuration instances as a tuple."""
        if self.config is None:
            return ()
        if isinstance(self.config, Sequence) and not isinstance(self.config, (str, bytes)):
            return tuple(self.config)
        return (self.config,)

    def install_state(self, state_mapping: dict[str, Any]) -> SQLAlchemyState:
        """Populate and return the SQLAlchemyState in a mutable state dictionary."""
        backend = (
            ObjectStoreBackend(
                key=self.storage_key,
                store=self.store,
                prefix=self.storage_prefix,
            )
            if self.store is not None
            else None
        )
        configs_map: dict[str | None, Any] = {}
        engines_map: dict[str | None, Any] = {}
        session_makers_map: dict[str | None, Any] = {}

        for idx, cfg in enumerate(self.configs):
            key = getattr(cfg, "bind_key", None) or (self.storage_key if idx == 0 else f"{self.storage_key}_{idx}")
            configs_map[key] = cfg
            engine = getattr(cfg, "engine_instance", None)
            if engine is not None:
                engines_map[key] = engine
            maker = getattr(cfg, "session_maker_instance", None)
            if maker is not None:
                session_makers_map[key] = maker

        broker_transport = self.broker.install_state(state_mapping) if self.broker is not None else None
        state = SQLAlchemyState(
            engines=engines_map,
            session_makers=session_makers_map,
            configs=configs_map,
            storage_backend=backend,
            broker=broker_transport,
            is_available=True,
        )
        self._state = state
        state_mapping[AA_STATE_KEY] = state
        return state

    def on_app_init(self, app_config: Any) -> Any:
        """Litestar InitPluginProtocol hook registering SQLAlchemyState and SkipValidation DI providers."""
        target_state = getattr(app_config, "state", None)
        if isinstance(target_state, dict):
            self.install_state(target_state)
        elif target_state is not None and hasattr(target_state, "update"):
            holder: dict[str, Any] = {}
            self.install_state(holder)
            target_state.update(holder)

        if self.broker is not None:
            self.broker.on_app_init(app_config)

        dependencies = getattr(app_config, "dependencies", None)
        if isinstance(dependencies, dict):
            for cfg in self.configs:
                self._register_config_dependencies(dependencies, cfg)

        return app_config

    def on_cli_init(self, cli: Any) -> None:
        """Litestar CLIPluginProtocol hook for CLI extension registration."""
        _ = cli

    @staticmethod
    def _register_config_dependencies(dependencies: dict[str, Any], cfg: Any) -> None:
        session_key = getattr(cfg, "session_dependency_key", "db_session")
        engine_key = getattr(cfg, "engine_dependency_key", "db_engine")
        provide_session_fn = getattr(cfg, "provide_session", None)
        provide_engine_fn = getattr(cfg, "provide_engine", None)

        provide_cls: type[Any] | None = None
        try:
            provide_cls = importlib.import_module("litestar.di").Provide
        except ImportError:
            provide_cls = None

        if callable(provide_session_fn) and session_key not in dependencies:
            wrapped_session = AdvancedAlchemy._wrap_skip_validation_provider(provide_session_fn)
            dependencies[session_key] = (
                provide_cls(wrapped_session, sync_to_thread=False) if provide_cls is not None else wrapped_session
            )
        if callable(provide_engine_fn) and engine_key not in dependencies:
            wrapped_engine = AdvancedAlchemy._wrap_skip_validation_provider(provide_engine_fn)
            dependencies[engine_key] = (
                provide_cls(wrapped_engine, sync_to_thread=False) if provide_cls is not None else wrapped_engine
            )

    @staticmethod
    def _wrap_skip_validation_provider(fn: Any) -> Any:
        sig = inspect.signature(fn)
        ret_ann = sig.return_annotation if sig.return_annotation is not inspect.Signature.empty else Any
        skip_ann = synthesize_skip_validation_annotation(ret_ann)

        def provider() -> Any:
            return fn()

        provider_obj: Any = provider
        provider_obj.__signature__ = sig.replace(return_annotation=skip_ann)
        provider_obj.__annotations__ = {"return": skip_ann}
        return provider


AlchemyPlugin = AdvancedAlchemy
SQLAlchemyPlugin = AdvancedAlchemy
AlchemyStorePlugin = AdvancedAlchemy
VertebraAlchemyPlugin = AdvancedAlchemy
