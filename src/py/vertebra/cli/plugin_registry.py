"""Pluggable CLI extension registry, VertebraEnv discovery, and CLIPluginProtocol support."""

from __future__ import annotations

import contextlib
import functools
import importlib.metadata
import inspect
import os
import sys
import tomllib
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol, cast, runtime_checkable

import click
import msgspec

from vertebra.cli.errors import EXIT_CONFIG_ERROR, CLIError
from vertebra.framework._app import App
from vertebra.utils.module_loader import import_app

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Sequence

__all__ = (
    "AUTODISCOVER_PATHS",
    "CLIPlugin",
    "CLIPluginProtocol",
    "VertebraEnv",
    "VertebraExtensionGroup",
    "VertebraGroup",
    "discover_entry_point_commands",
    "ensure_vertebra_integration",
    "enter_app_server_lifespan",
    "register_app_cli_plugins",
    "resolve_cli_app_path",
)

AUTODISCOVER_PATHS: tuple[str, ...] = (
    "app.py",
    "application.py",
    "asgi.py",
    "wsgi.py",
    "app/__init__.py",
    "application/__init__.py",
)

_AUTODISCOVER_ATTRS: tuple[str, ...] = (
    "app",
    "application",
    "create_app",
    "make_app",
)


@runtime_checkable
class CLIPluginProtocol(Protocol):
    """Protocol implemented by plugins that contribute subcommands or overrides to the CLI."""

    def on_cli_init(self, cli: click.Group) -> None:
        """Register plugin subcommands or options on the root Click group."""
        ...


class CLIPlugin(CLIPluginProtocol):
    """Base class for plugins that extend the Vertebra CLI and server lifespan."""

    __slots__ = ()

    def on_cli_init(self, cli: click.Group) -> None:
        """Register or override commands on the root Click group."""
        _ = cli

    @contextlib.contextmanager
    def server_lifespan(self, app: Any) -> Iterator[None]:
        """Wrap the supervised server lifecycle around ``app``."""
        _ = app
        yield


class VertebraEnv(msgspec.Struct, frozen=True, gc=False):
    """Resolved application environment injected into CLI commands and plugins."""

    app_path: str
    app: Any
    cwd: Path
    host: str | None = None
    port: int | None = None
    debug: bool = False
    pdb: bool = False
    is_app_factory: bool = False

    @classmethod
    def resolve_app_path(
        cls,
        app_path: str | None = None,
        app_dir: Path | None = None,
    ) -> str | None:
        """Resolve an application ``module:attr`` import path from explicit arg, env, pyproject.toml, or autodiscovery."""
        if app_path and app_path.strip():
            return app_path.strip()

        for env_key in ("VERTEBRA_APP", "LITESTAR_APP"):
            env_val = os.environ.get(env_key, "").strip()
            if env_val:
                return env_val

        cwd = (app_dir or Path.cwd()).resolve()
        pyproject_app = _read_pyproject_app_path(cwd)
        if pyproject_app:
            return pyproject_app

        return _autodiscover_app_path(cwd)

    @classmethod
    def from_env(
        cls,
        app_path: str | None = None,
        app_dir: Path | None = None,
        *,
        native: bool = True,
    ) -> VertebraEnv:
        """Load the target application and assemble a ``VertebraEnv``."""
        cwd = (app_dir or Path.cwd()).resolve()
        cwd_str = str(cwd)
        if cwd_str not in sys.path:
            sys.path.insert(0, cwd_str)

        resolved_path = cls.resolve_app_path(app_path, cwd)
        if not resolved_path:
            raise CLIError(
                "Could not resolve application import path.",
                hint="Pass --app MODULE:ATTR, set VERTEBRA_APP, or configure [tool.vertebra] app in pyproject.toml.",
                exit_code=EXIT_CONFIG_ERROR,
            )

        raw_target = import_app(resolved_path)
        app, is_factory = _instantiate_if_factory(raw_target)
        ensure_vertebra_integration(app, native=native)
        debug_flag = bool(getattr(app, "debug", False)) or os.environ.get("LITESTAR_DEBUG") == "1"
        pdb_flag = bool(getattr(app, "pdb_on_exception", False)) or os.environ.get("LITESTAR_PDB") == "1"
        raw_port = os.environ.get("VERTEBRA_PORT") or os.environ.get("LITESTAR_PORT")
        port_val = int(raw_port) if raw_port and raw_port.isdigit() else None
        host_val = os.environ.get("VERTEBRA_HOST") or os.environ.get("LITESTAR_HOST")
        return cls(
            app_path=resolved_path,
            app=app,
            cwd=cwd,
            host=host_val,
            port=port_val,
            debug=debug_flag,
            pdb=pdb_flag,
            is_app_factory=is_factory,
        )


class VertebraGroup(click.Group):
    """Click group that injects ``app``, ``env``, and ``ctx`` into plugin command callbacks."""

    _is_vertebra_cli: bool = True
    group_class = type

    def __init__(
        self,
        name: str | None = None,
        commands: dict[str, click.Command] | Sequence[click.Command] | None = None,
        **attrs: Any,
    ) -> None:
        """Initialize the VertebraGroup and wrap any initial commands for argument injection."""
        super().__init__(name=name, commands=commands, **attrs)
        self._registered_plugin_ids: set[int] = set()
        for cmd in self.commands.values():
            self._wrap_command_tree(cmd)

    def add_command(self, cmd: click.Command, name: str | None = None) -> None:
        """Register a Click command or subgroup and enable ``app`` / ``env`` callback injection."""
        self._wrap_command_tree(cmd)
        super().add_command(cmd, name=name)

    def _wrap_command_tree(self, cmd: click.Command) -> None:
        if isinstance(cmd, click.Group):
            if cmd.callback is not None:
                cmd.callback = _inject_cli_args(cmd.callback, command_params=cmd.params)
            for sub in cmd.commands.values():
                self._wrap_command_tree(sub)
        elif cmd.callback is not None:
            cmd.callback = _inject_cli_args(cmd.callback, command_params=cmd.params)


class VertebraExtensionGroup(VertebraGroup):
    """Root ``vtb`` Click group that lazily loads ``VertebraEnv`` and activates ``CLIPluginProtocol`` plugins."""

    def __init__(
        self,
        name: str | None = None,
        commands: dict[str, click.Command] | Sequence[click.Command] | None = None,
        **attrs: Any,
    ) -> None:
        """Initialize the root extension group."""
        super().__init__(name=name, commands=commands, **attrs)
        self._prepared: bool = False

    def make_context(
        self,
        info_name: str | None,
        args: list[str],
        parent: click.Context | None = None,
        **extra: Any,
    ) -> click.Context:
        """Create the Click context and prepare entry-point and application CLI plugins."""
        ctx = super().make_context(info_name, args, parent, **extra)
        self._prepare(ctx)
        return ctx

    def list_commands(self, ctx: click.Context) -> list[str]:
        """Prepare plugins and return all registered command names."""
        self._prepare(ctx)
        return super().list_commands(ctx)

    def get_command(self, ctx: click.Context, cmd_name: str) -> click.Command | None:
        """Prepare plugins and look up ``cmd_name``."""
        self._prepare(ctx)
        return super().get_command(ctx, cmd_name)

    def format_help(self, ctx: click.Context, formatter: click.HelpFormatter) -> None:
        """Prepare plugins and format CLI help output."""
        self._prepare(ctx)
        super().format_help(ctx, formatter)

    def _prepare(self, ctx: click.Context) -> None:
        if self._prepared and isinstance(ctx.obj, VertebraEnv):
            return
        self._prepared = True
        discover_entry_point_commands(self)

        app_path = cast("str | None", ctx.params.get("app_path"))
        app_dir = cast("Path | None", ctx.params.get("app_dir"))
        env: VertebraEnv | None = ctx.obj if isinstance(ctx.obj, VertebraEnv) else None

        if env is None:
            candidate_path = VertebraEnv.resolve_app_path(app_path, app_dir)
            if candidate_path is not None:
                with contextlib.suppress(Exception):
                    env = VertebraEnv.from_env(candidate_path, app_dir)
                    ctx.obj = env

        if env is not None:
            register_app_cli_plugins(self, env.app)


def discover_entry_point_commands(
    cli_group: click.Group,
    *,
    group: str = "vertebra.cli",
) -> list[str]:
    """Load and attach Click commands or hooks exposed via Python package entry points."""
    registered: list[str] = []
    try:
        eps = importlib.metadata.entry_points(group=group)
    except Exception:
        return registered

    for ep in eps:
        with contextlib.suppress(Exception):
            loaded = ep.load()
            if isinstance(loaded, click.Command):
                cmd_name = loaded.name or ep.name
                if cmd_name not in cli_group.commands:
                    cli_group.add_command(loaded, name=cmd_name)
                    registered.append(cmd_name)
            elif isinstance(loaded, CLIPluginProtocol) or hasattr(loaded, "on_cli_init"):
                loaded.on_cli_init(cli_group)
                registered.append(ep.name)
            elif callable(loaded):
                result = loaded(cli_group)
                if isinstance(result, click.Command):
                    cmd_name = result.name or ep.name
                    if cmd_name not in cli_group.commands:
                        cli_group.add_command(result, name=cmd_name)
                registered.append(ep.name)
    return registered


def register_app_cli_plugins(cli_group: click.Group, app: Any) -> list[str]:
    """Invoke ``on_cli_init(cli_group)`` on any plugin attached to a Vertebra or host-framework application."""
    activated: list[str] = []
    if app is None:
        return activated

    seen_ids: set[int]
    if isinstance(cli_group, VertebraGroup):
        seen_ids = cli_group._registered_plugin_ids
    elif hasattr(cli_group, "__dict__"):
        seen_ids = cli_group.__dict__.setdefault("_registered_plugin_ids", set())
    else:
        seen_ids = set()

    for plugin in _collect_app_plugins(app):
        plugin_id = id(plugin)
        if plugin_id in seen_ids:
            continue
        hook = getattr(plugin, "on_cli_init", None)
        if callable(hook):
            seen_ids.add(plugin_id)
            hook(cli_group)
            activated.append(type(plugin).__name__)
    return activated


def ensure_vertebra_integration(app: Any, *, native: bool = True) -> Any:
    """Automatically attach and finalize ``VertebraPlugin`` on supported host-framework applications."""
    if app is None:
        return None

    if isinstance(app, App):
        for _prefix, mounted_app, _protocol in app.mounts:
            ensure_vertebra_integration(mounted_app, native=native)
        return app

    cls_name = type(app).__name__
    mod_name = getattr(type(app), "__module__", "") or ""

    if cls_name == "Litestar" or mod_name.startswith("litestar"):
        _ensure_litestar_integration(app, native=native)
    elif (
        cls_name == "FastAPI"
        or mod_name.startswith("fastapi")
        or (hasattr(app, "dependency_overrides") and hasattr(app, "routes") and hasattr(app, "router"))
    ):
        _ensure_fastapi_integration(app, native=native)
    elif (
        cls_name == "Starlette"
        or mod_name.startswith("starlette")
        or (hasattr(app, "user_middleware") and hasattr(app, "routes") and hasattr(app, "state"))
    ):
        _ensure_starlette_integration(app, native=native)
    elif (
        cls_name == "Flask"
        or mod_name.startswith("flask")
        or (hasattr(app, "url_map") and hasattr(app, "view_functions"))
    ):
        _ensure_flask_integration(app, native=native)

    return app


@contextlib.contextmanager
def enter_app_server_lifespan(
    app_or_path: Any,
    *,
    app_path: str | None = None,
    native: bool = True,
) -> Iterator[str | None]:
    """Enter registered ``server_lifespan`` context managers and yield ``VERTEBRA_OFFLOAD_CONFIG`` JSON."""
    resolved_app: Any = None
    resolved_path: str | None = app_path
    if isinstance(app_or_path, str):
        resolved_path = app_or_path
        with contextlib.suppress(Exception):
            raw_target = import_app(app_or_path)
            resolved_app, _ = _instantiate_if_factory(raw_target)
    else:
        resolved_app = app_or_path

    if resolved_app is None:
        yield os.environ.get("VERTEBRA_OFFLOAD_CONFIG")
        return

    ensure_vertebra_integration(resolved_app, native=native)

    with contextlib.ExitStack() as stack:
        _enter_framework_lifespans(stack, resolved_app, app_path=resolved_path)
        offload_json = _extract_offload_config_json(resolved_app) or os.environ.get("VERTEBRA_OFFLOAD_CONFIG")
        prev_offload = os.environ.get("VERTEBRA_OFFLOAD_CONFIG")
        if isinstance(offload_json, str) and offload_json:
            os.environ["VERTEBRA_OFFLOAD_CONFIG"] = offload_json
        try:
            yield offload_json if isinstance(offload_json, str) and offload_json else None
        finally:
            if prev_offload is None:
                os.environ.pop("VERTEBRA_OFFLOAD_CONFIG", None)
            else:
                os.environ["VERTEBRA_OFFLOAD_CONFIG"] = prev_offload


def resolve_cli_app_path(
    explicit_app: str | None = None,
    *,
    ctx: click.Context | None = None,
) -> str:
    """Resolve the target ``module:attr`` string from a positional argument, ``--app``, env, or autodiscovery."""
    if explicit_app and explicit_app.strip():
        return explicit_app.strip()

    active_ctx = ctx or click.get_current_context(silent=True)
    app_path_opt: str | None = None
    app_dir_opt: Path | None = None
    if active_ctx is not None:
        if isinstance(active_ctx.obj, VertebraEnv):
            return active_ctx.obj.app_path
        root_ctx = active_ctx.find_root()
        if isinstance(root_ctx.obj, VertebraEnv):
            return root_ctx.obj.app_path
        app_path_opt = cast("str | None", root_ctx.params.get("app_path"))
        app_dir_opt = cast("Path | None", root_ctx.params.get("app_dir"))

    resolved = VertebraEnv.resolve_app_path(app_path_opt, app_dir_opt)
    if resolved:
        return resolved

    raise CLIError(
        "Missing application import path.",
        hint="Pass APP (e.g. 'vtb run myapp:app'), '--app MODULE:ATTR', set VERTEBRA_APP, or configure [tool.vertebra] app in pyproject.toml.",
        exit_code=EXIT_CONFIG_ERROR,
    )


def _inject_cli_args(
    func: Callable[..., Any],
    *,
    command_params: Sequence[click.Parameter] | None = None,
) -> Callable[..., Any]:
    if getattr(func, "__vertebra_cli_wrapped__", False):
        return func

    try:
        sig = inspect.signature(func)
    except (TypeError, ValueError):
        return func

    click_param_names = {p.name for p in (command_params or ()) if p.name is not None}
    needs_app = "app" in sig.parameters and "app" not in click_param_names
    needs_env = "env" in sig.parameters and "env" not in click_param_names
    needs_ctx = "ctx" in sig.parameters and "ctx" not in click_param_names
    if not (needs_app or needs_env or needs_ctx):
        return func

    @click.pass_context
    @functools.wraps(func)
    def wrapped(ctx: click.Context, /, *args: Any, **kwargs: Any) -> Any:
        env = _resolve_context_env(ctx)
        if needs_app and "app" not in kwargs:
            if env is None:
                env = _require_context_env(ctx)
            kwargs["app"] = env.app
        if needs_env and "env" not in kwargs:
            if env is None:
                env = _require_context_env(ctx)
            kwargs["env"] = env
        if needs_ctx and "ctx" not in kwargs:
            kwargs["ctx"] = ctx
        return func(*args, **kwargs)

    cast("Any", wrapped).__vertebra_cli_wrapped__ = True
    return wrapped


def _resolve_context_env(ctx: click.Context) -> VertebraEnv | None:
    for candidate_ctx in (ctx, ctx.find_root()):
        obj = candidate_ctx.obj
        if callable(obj):
            with contextlib.suppress(Exception):
                obj = obj()
                candidate_ctx.obj = obj
        if isinstance(obj, VertebraEnv):
            return obj
        if obj is not None and hasattr(obj, "app") and hasattr(obj, "app_path"):
            return VertebraEnv(
                app_path=str(obj.app_path),
                app=obj.app,
                cwd=getattr(obj, "cwd", Path.cwd()),
                host=getattr(obj, "host", None),
                port=getattr(obj, "port", None),
                debug=bool(getattr(obj, "debug", False)),
                pdb=bool(getattr(obj, "pdb", False)),
                is_app_factory=bool(getattr(obj, "is_app_factory", False)),
            )
    return None


def _require_context_env(ctx: click.Context) -> VertebraEnv:
    root_ctx = ctx.find_root()
    app_path = cast("str | None", root_ctx.params.get("app_path"))
    app_dir = cast("Path | None", root_ctx.params.get("app_dir"))
    env = VertebraEnv.from_env(app_path, app_dir)
    root_ctx.obj = env
    ctx.obj = env
    return env


def _collect_app_plugins(app: Any) -> list[Any]:
    collected: list[Any] = []
    plugins_attr = getattr(app, "plugins", None)
    if plugins_attr is not None:
        cli_plugins = getattr(plugins_attr, "cli", None)
        if cli_plugins is not None:
            collected.extend(cli_plugins)
        else:
            raw = getattr(plugins_attr, "plugins", plugins_attr)
            if isinstance(raw, (list, tuple, set, frozenset)):
                collected.extend(raw)

    state = getattr(app, "state", None)
    state_plugin = _get_state_attr(state, "vertebra_plugin")
    if state_plugin is not None and state_plugin not in collected:
        collected.append(state_plugin)

    mounts = getattr(app, "mounts", None)
    if isinstance(mounts, (list, tuple)):
        for entry in mounts:
            if isinstance(entry, tuple) and len(entry) >= 2:
                collected.extend(_collect_app_plugins(entry[1]))

    return collected


def _ensure_litestar_integration(app: Any, *, native: bool) -> None:
    from vertebra.extensions.litestar.plugin import VertebraPlugin

    state = getattr(app, "state", None)
    plugin = _get_state_attr(state, "vertebra_plugin")
    if not isinstance(plugin, VertebraPlugin):
        for candidate in _collect_app_plugins(app):
            if isinstance(candidate, VertebraPlugin):
                plugin = candidate
                break
    if not isinstance(plugin, VertebraPlugin) and native:
        plugin = VertebraPlugin()
        if state is not None:
            _set_state_attr(state, "vertebra_config", plugin.config)
            _set_state_attr(state, "vertebra_plugin", plugin)
    if isinstance(plugin, VertebraPlugin) and native:
        plugin.ensure_finalized(app)


def _ensure_fastapi_integration(app: Any, *, native: bool) -> None:
    from vertebra.extensions.fastapi.plugin import VertebraPlugin

    state = getattr(app, "state", None)
    plugin = _get_state_attr(state, "vertebra_plugin")
    if not isinstance(plugin, VertebraPlugin) and native:
        plugin = VertebraPlugin(app)
    elif isinstance(plugin, VertebraPlugin) and native:
        plugin.ensure_finalized(app)


def _ensure_starlette_integration(app: Any, *, native: bool) -> None:
    from vertebra.extensions.starlette.plugin import VertebraPlugin

    state = getattr(app, "state", None)
    plugin = _get_state_attr(state, "vertebra_plugin")
    if not isinstance(plugin, VertebraPlugin) and native:
        plugin = VertebraPlugin(app)
    elif isinstance(plugin, VertebraPlugin) and native:
        plugin.ensure_finalized(app)


def _ensure_flask_integration(app: Any, *, native: bool) -> None:
    from vertebra.extensions.flask.plugin import VertebraPlugin

    extensions = getattr(app, "extensions", None)
    plugin = extensions.get("vertebra") if isinstance(extensions, dict) else getattr(app, "vertebra", None)
    if not isinstance(plugin, VertebraPlugin) and native:
        VertebraPlugin(app)


def _enter_framework_lifespans(
    stack: contextlib.ExitStack,
    app: Any,
    *,
    app_path: str | None = None,
) -> None:
    if isinstance(app, App):
        stack.enter_context(app.lifespan())
        for _prefix, mounted_app, _protocol in app.mounts:
            _enter_framework_lifespans(stack, mounted_app, app_path=app_path)
        return

    cls_name = type(app).__name__
    mod_name = getattr(type(app), "__module__", "") or ""
    if cls_name == "Litestar" or mod_name.startswith("litestar"):
        from vertebra.extensions.litestar.plugin import enter_litestar_server_lifespan

        stack.enter_context(enter_litestar_server_lifespan(app, app_path=app_path))
        return
    if cls_name == "FastAPI" or mod_name.startswith("fastapi"):
        from vertebra.extensions.fastapi.plugin import enter_fastapi_server_lifespan

        stack.enter_context(enter_fastapi_server_lifespan(app))
        return
    if cls_name == "Starlette" or mod_name.startswith("starlette"):
        from vertebra.extensions.starlette.plugin import enter_starlette_server_lifespan

        stack.enter_context(enter_starlette_server_lifespan(app))
        return

    lifespan_fn = getattr(app, "lifespan", None)
    if callable(lifespan_fn):
        with contextlib.suppress(TypeError):
            cm = lifespan_fn()
            if hasattr(cm, "__enter__"):
                stack.enter_context(cm)


def _extract_offload_config_json(app: Any) -> str | None:
    if isinstance(app, App):
        for _prefix, mounted_app, _protocol in app.mounts:
            nested = _extract_offload_config_json(mounted_app)
            if nested:
                return nested
        return None

    state = getattr(app, "state", None)
    offload_json = _get_state_attr(state, "vertebra_offload_config_json")
    if isinstance(offload_json, str) and offload_json:
        return offload_json

    extensions = getattr(app, "extensions", None)
    flask_plugin = extensions.get("vertebra") if isinstance(extensions, dict) else getattr(app, "vertebra", None)
    if flask_plugin is not None:
        bundle = getattr(flask_plugin, "offload_bundle", None)
        to_json_fn = getattr(bundle, "to_server_config_json", None)
        if callable(to_json_fn):
            rendered = to_json_fn()
            if isinstance(rendered, str) and rendered:
                return rendered

    return None


def _get_state_attr(state: Any, key: str) -> Any:
    if state is None:
        return None
    if isinstance(state, dict):
        return state.get(key)
    if hasattr(state, "get"):
        with contextlib.suppress(Exception):
            val = state.get(key)
            if val is not None:
                return val
    return getattr(state, key, None)


def _set_state_attr(state: Any, key: str, value: Any) -> None:
    if isinstance(state, dict):
        state[key] = value
        return
    try:
        setattr(state, key, value)
    except (AttributeError, TypeError):
        if hasattr(state, "__setitem__"):
            state[key] = value


def _instantiate_if_factory(target: Any) -> tuple[Any, bool]:
    if isinstance(target, App) or not inspect.isfunction(target):
        return target, False
    try:
        sig = inspect.signature(target)
    except (TypeError, ValueError):
        return target, False
    required_positional = [
        p
        for p in sig.parameters.values()
        if p.default is inspect.Parameter.empty
        and p.kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
    ]
    if len(required_positional) == 0 and len(sig.parameters) <= 1 and not inspect.iscoroutinefunction(target):
        return target(), True
    return target, False


def _read_pyproject_app_path(project_dir: Path) -> str | None:
    pyproject_path = project_dir / "pyproject.toml"
    if not pyproject_path.is_file():
        return None
    with contextlib.suppress(Exception):
        data = tomllib.loads(pyproject_path.read_text(encoding="utf-8"))
        tool = data.get("tool")
        if isinstance(tool, dict):
            for section_key in ("vertebra", "vtb", "litestar"):
                sec = tool.get(section_key)
                if isinstance(sec, dict):
                    app_val = sec.get("app")
                    if isinstance(app_val, str) and app_val.strip():
                        return app_val.strip()
    return None


def _autodiscover_app_path(project_dir: Path) -> str | None:
    for rel_path in AUTODISCOVER_PATHS:
        candidate = project_dir / rel_path
        if not candidate.is_file():
            continue
        module_name = rel_path.split("/", 1)[0] if rel_path.endswith("/__init__.py") else candidate.stem
        source = candidate.read_text(encoding="utf-8", errors="ignore")
        for attr in _AUTODISCOVER_ATTRS:
            if f"{attr} =" in source or f"{attr}:" in source or f"def {attr}(" in source:
                return f"{module_name}:{attr}"
    return None
