"""Application plugin and PEP 695 generic configurations for Advanced Alchemy integration."""

from __future__ import annotations

import builtins
import contextlib
import inspect
import logging
from collections.abc import Sequence
from typing import Annotated, Any, ClassVar, cast

import msgspec

from vertebra.extensions.alchemy.broker import BrokerPlugin, BrokerTransport
from vertebra.extensions.alchemy.storage import ObjectStoreBackend
from vertebra.storage import ObjectStore
from vertebra.utils.module_loader import import_optional

logger = logging.getLogger(__name__)

__all__ = (
    "AlchemyPlugin",
    "AsyncSessionConfig",
    "DatabaseService",
    "SQLAlchemyAsyncConfig",
    "SQLAlchemyState",
    "SQLAlchemySyncConfig",
    "SyncSessionConfig",
)


class _BaseSessionConfig(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Shared base configuration for synchronous and asynchronous SQLAlchemy engines."""

    bind_key: str = "default"
    dsn: str = ""
    pool_size: int = 5
    max_overflow: int = 10
    pool_timeout: float = 30.0
    pool_recycle: int = 1800
    pool_pre_ping: bool = True
    echo: bool = False
    execution_options: dict[str, Any] = msgspec.field(default_factory=dict)

    def effective_engine_config(self) -> dict[str, Any]:
        """Return the dictionary of engine keyword arguments."""
        cfg: dict[str, Any] = {
            "echo": self.echo,
            "execution_options": dict(self.execution_options),
        }
        if not _is_in_memory_uri(self.dsn):
            cfg.update(
                {
                    "pool_size": self.pool_size,
                    "max_overflow": self.max_overflow,
                    "pool_timeout": self.pool_timeout,
                    "pool_recycle": self.pool_recycle,
                    "pool_pre_ping": self.pool_pre_ping,
                }
            )
        return cfg


class SyncSessionConfig(_BaseSessionConfig, frozen=True, gc=False, kw_only=True):
    """Synchronous session configuration inheriting from _BaseSessionConfig."""


class AsyncSessionConfig(_BaseSessionConfig, frozen=True, gc=False, kw_only=True):
    """Asynchronous session configuration inheriting from _BaseSessionConfig."""


class DatabaseService[ModelT]:
    """Repository service performing CRUD and pagination queries over a SQLAlchemy session."""

    __slots__ = ("_session", "model_type")

    def __init__(self, session: Any, model_type: type[ModelT]) -> None:
        """Initialize service with session and target model type."""
        self._session = session
        self.model_type = model_type

    async def count(self, *filters: Any) -> int:
        """Return count of matching records using SELECT count(*) when backed by SQLAlchemy."""
        if hasattr(self._session, "execute"):
            try:
                sqlalchemy_mod = import_optional("sqlalchemy")

                if sqlalchemy_mod is not None and (
                    hasattr(self.model_type, "__tablename__") or hasattr(self.model_type, "__table__")
                ):
                    stmt = sqlalchemy_mod.select(sqlalchemy_mod.func.count()).select_from(self.model_type)
                    for f in filters:
                        stmt = stmt.where(f)
                else:
                    stmt = "SELECT count(*)"
                res = self._session.execute(stmt)
                if inspect.isawaitable(res):
                    res = await res
                if hasattr(res, "scalar_one"):
                    return int(res.scalar_one())
            except (AttributeError, TypeError, ValueError, RuntimeError, OSError) as exc:
                logger.debug("DatabaseService.count failed SQL execution, falling back to list", exc_info=exc)
        items = await self.list(*filters)
        return len(items)

    async def list(self, *filters: Any, limit: int | None = None, offset: int | None = None) -> builtins.list[ModelT]:
        """List matching records."""
        if hasattr(self._session, "execute"):
            try:
                sqlalchemy_mod = import_optional("sqlalchemy")

                if sqlalchemy_mod is not None and (
                    hasattr(self.model_type, "__tablename__") or hasattr(self.model_type, "__table__")
                ):
                    stmt = sqlalchemy_mod.select(self.model_type)
                    for f in filters:
                        stmt = stmt.where(f)
                    if limit is not None:
                        stmt = stmt.limit(limit)
                    if offset is not None:
                        stmt = stmt.offset(offset)
                else:
                    stmt = "SELECT *"
                res = self._session.execute(stmt)
                if inspect.isawaitable(res):
                    res = await res
                if hasattr(res, "scalars"):
                    scalars = res.scalars()
                    all_res = scalars.all()
                    if inspect.isawaitable(all_res):
                        all_res = await all_res
                    return builtins.list(all_res)
                if hasattr(res, "all"):
                    all_res = res.all()
                    if inspect.isawaitable(all_res):
                        all_res = await all_res
                    return builtins.list(all_res)
            except (AttributeError, TypeError, ValueError, RuntimeError, OSError) as exc:
                logger.debug("DatabaseService.list failed SQL execution", exc_info=exc)
        return []

    async def list_and_count(
        self, *filters: Any, limit: int | None = None, offset: int | None = None
    ) -> tuple[builtins.list[ModelT], int]:
        """List matching records with total count using window function count().over()."""
        if hasattr(self._session, "execute"):
            try:
                sqlalchemy_mod = import_optional("sqlalchemy")

                if sqlalchemy_mod is not None and (
                    hasattr(self.model_type, "__tablename__") or hasattr(self.model_type, "__table__")
                ):
                    count_col = sqlalchemy_mod.func.count().over().label("_total_count")
                    stmt = sqlalchemy_mod.select(self.model_type, count_col)
                    for f in filters:
                        stmt = stmt.where(f)
                    if limit is not None:
                        stmt = stmt.limit(limit)
                    if offset is not None:
                        stmt = stmt.offset(offset)
                else:
                    stmt = "SELECT *, count(*) OVER() FROM table"
                res = self._session.execute(stmt)
                if inspect.isawaitable(res):
                    res = await res
                if hasattr(res, "all"):
                    rows = res.all()
                    if inspect.isawaitable(rows):
                        rows = await rows
                    if not rows:
                        total = await self.count(*filters)
                        return [], total
                    items = [row[0] for row in rows]
                    total = int(rows[0][1])
                    return items, total
            except (AttributeError, TypeError, ValueError, RuntimeError, OSError) as exc:
                logger.debug("DatabaseService.list_and_count failed SQL execution, falling back", exc_info=exc)
        items = await self.list(*filters, limit=limit, offset=offset)
        total = await self.count(*filters)
        return items, total


class SQLAlchemyAsyncConfig[
    EngineT,
    SessionT,
    SessionMakerT,
]:
    """Generic asynchronous SQLAlchemy configuration preserving EngineT, SessionT, and SessionMakerT."""

    __slots__ = (
        "bind_key",
        "connection_string",
        "engine_config",
        "engine_dependency_key",
        "engine_instance",
        "is_async",
        "session_config",
        "session_dependency_key",
        "session_maker_instance",
    )

    def __init__(
        self,
        connection_string: str = "sqlite+aiosqlite:///:memory:",
        bind_key: str = "default",
        session_dependency_key: str = "db_session",
        engine_dependency_key: str = "db_engine",
        engine_config: dict[str, Any] | None = None,
        session_config: dict[str, Any] | None = None,
        engine_instance: EngineT | None = None,
        session_maker_instance: SessionMakerT | None = None,
    ) -> None:
        self.connection_string = connection_string
        self.bind_key = bind_key
        self.session_dependency_key = session_dependency_key
        self.engine_dependency_key = engine_dependency_key
        self.engine_config = _inject_engine_pool_defaults(engine_config or {}, connection_string)
        self.session_config: dict[str, Any] = dict(session_config or {})
        self.engine_instance = engine_instance
        self.session_maker_instance = session_maker_instance
        self.is_async = True

    def provide_engine(self) -> EngineT:
        """Return the configured asynchronous engine instance."""
        return cast("EngineT", self.engine_instance)

    def provide_session(self) -> SessionT:
        """Return a typed asynchronous session from the configured sessionmaker."""
        if callable(self.session_maker_instance):
            return cast("SessionT", self.session_maker_instance())
        return cast("SessionT", self.session_maker_instance)


class SQLAlchemySyncConfig[
    EngineT,
    SessionT,
    SessionMakerT,
]:
    """Generic synchronous SQLAlchemy configuration preserving EngineT, SessionT, and SessionMakerT."""

    __slots__ = (
        "bind_key",
        "connection_string",
        "engine_config",
        "engine_dependency_key",
        "engine_instance",
        "is_async",
        "session_config",
        "session_dependency_key",
        "session_maker_instance",
    )

    def __init__(
        self,
        connection_string: str = "sqlite:///:memory:",
        bind_key: str = "default",
        session_dependency_key: str = "db_session",
        engine_dependency_key: str = "db_engine",
        engine_config: dict[str, Any] | None = None,
        session_config: dict[str, Any] | None = None,
        engine_instance: EngineT | None = None,
        session_maker_instance: SessionMakerT | None = None,
    ) -> None:
        self.connection_string = connection_string
        self.bind_key = bind_key
        self.session_dependency_key = session_dependency_key
        self.engine_dependency_key = engine_dependency_key
        self.engine_config = _inject_engine_pool_defaults(engine_config or {}, connection_string)
        self.session_config: dict[str, Any] = dict(session_config or {})
        self.engine_instance = engine_instance
        self.session_maker_instance = session_maker_instance
        self.is_async = False

    def provide_engine(self) -> EngineT:
        """Return the configured synchronous engine instance."""
        return cast("EngineT", self.engine_instance)

    def provide_session(self) -> SessionT:
        """Return a typed synchronous session from the configured sessionmaker."""
        if callable(self.session_maker_instance):
            return cast("SessionT", self.session_maker_instance())
        return cast("SessionT", self.session_maker_instance)


class SQLAlchemyState:
    """Runtime state container registered under state['advanced_alchemy']."""

    __slots__ = (
        "broker",
        "configs",
        "engines",
        "is_available",
        "session_makers",
        "storage_backend",
    )

    def __init__(
        self,
        engines: dict[str | None, Any] | None = None,
        session_makers: dict[str | None, Any] | None = None,
        configs: dict[str | None, Any] | None = None,
        storage_backend: ObjectStoreBackend | None = None,
        broker: BrokerTransport | None = None,
        is_available: bool = True,
    ) -> None:
        self.engines: dict[str | None, Any] = dict(engines or {})
        self.session_makers: dict[str | None, Any] = dict(session_makers or {})
        self.configs: dict[str | None, Any] = dict(configs or {})
        self.storage_backend = storage_backend
        self.broker = broker
        self.is_available = is_available


class AlchemyPlugin[ConfigT]:
    """Plugin binding Advanced Alchemy generic configurations and ObjectStoreBackend into application state."""

    PLUGIN_PRIORITY: ClassVar[int] = -5600
    STATE_KEY: ClassVar[str] = "advanced_alchemy"
    __slots__ = (
        "_state",
        "broker",
        "config",
        "priority",
        "storage_key",
        "storage_prefix",
        "store",
    )

    def __init__(
        self,
        config: ConfigT | Sequence[ConfigT] | None = None,
        store: ObjectStore | None = None,
        storage_key: str = "vertebra",
        storage_prefix: str = "",
        broker: BrokerPlugin | None = None,
        priority: int = PLUGIN_PRIORITY,
    ) -> None:
        self.config = config
        self.store = store
        self.storage_key = storage_key
        self.storage_prefix = storage_prefix
        self.broker = broker
        self.priority = priority
        self._state: SQLAlchemyState | None = None
        if self.config is not None:
            if isinstance(self.config, Sequence) and not isinstance(self.config, (str, bytes)):
                for item in self.config:
                    self.wrap_config(item)
            else:
                self.wrap_config(self.config)

    @property
    def configs(self) -> tuple[ConfigT, ...]:
        """Return all registered database configuration instances as a tuple."""
        if self.config is None:
            return ()
        if isinstance(self.config, Sequence) and not isinstance(self.config, (str, bytes)):
            return tuple(self.config)
        return (self.config,)

    @staticmethod
    def wrap_config[T](config: T) -> T:
        """Inject production pool health defaults into an Advanced Alchemy config while preserving T."""
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

    @staticmethod
    def synthesize_skip_validation_annotation(target_type: Any) -> Any:
        """Wrap target_type in Litestar's SkipValidation when Litestar is installed."""
        typing_mod = import_optional("litestar.typing")
        if typing_mod is not None:
            skip_val = getattr(typing_mod, "SkipValidation", None)
            if skip_val is not None:
                return Annotated[target_type, skip_val]
        return target_type

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
        state_mapping[self.STATE_KEY] = state
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

    @classmethod
    def _register_config_dependencies(cls, dependencies: dict[str, Any], cfg: Any) -> None:
        session_key = getattr(cfg, "session_dependency_key", "db_session")
        engine_key = getattr(cfg, "engine_dependency_key", "db_engine")
        provide_session_fn = getattr(cfg, "provide_session", None)
        provide_engine_fn = getattr(cfg, "provide_engine", None)

        di_mod = import_optional("litestar.di")
        provide_cls: type[Any] | None = getattr(di_mod, "Provide", None) if di_mod is not None else None

        if callable(provide_session_fn) and session_key not in dependencies:
            wrapped_session = cls._wrap_skip_validation_provider(provide_session_fn)
            dependencies[session_key] = (
                provide_cls(wrapped_session, sync_to_thread=False) if provide_cls is not None else wrapped_session
            )
        if callable(provide_engine_fn) and engine_key not in dependencies:
            wrapped_engine = cls._wrap_skip_validation_provider(provide_engine_fn)
            dependencies[engine_key] = (
                provide_cls(wrapped_engine, sync_to_thread=False) if provide_cls is not None else wrapped_engine
            )

    @classmethod
    def _wrap_skip_validation_provider(cls, fn: Any) -> Any:
        sig = inspect.signature(fn)
        ret_ann = sig.return_annotation if sig.return_annotation is not inspect.Signature.empty else Any
        skip_ann = cls.synthesize_skip_validation_annotation(ret_ann)

        def provider() -> Any:
            return fn()

        provider_obj: Any = provider
        provider_obj.__signature__ = sig.replace(return_annotation=skip_ann)
        provider_obj.__annotations__ = {"return": skip_ann}
        return provider


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
