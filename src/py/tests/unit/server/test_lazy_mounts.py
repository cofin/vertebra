"""Unit tests for lazy import-string mounts, longest-prefix fallback, and CLI --mount composition."""

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from click.testing import CliRunner

from vertebra.cli import run as cli_run_mod
from vertebra.cli._launch import ServerLaunchEnv
from vertebra.cli.plugin_registry import ensure_vertebra_integration, enter_app_server_lifespan
from vertebra.framework._app import App
from vertebra.server._dispatch import partition_batch
from vertebra.server._mounts import (
    MOUNT_ENTRYPOINT_APP_PATH,
    MountSpec,
    build_mounted_app_from_env,
    deserialize_mount_specs,
    load_mounts_pyproject_config,
    merge_mount_specs,
    parse_mount_spec,
    resolve_mount_target,
    serialize_mount_specs,
)
from vertebra.server.worker import Worker, WorkerStartupError, _instantiate_mount_runners
from vertebra.utils.module_loader import import_app


def _write_temp_mount_module(tmp_path: Path, name: str, source: str) -> str:
    """Write a temporary Python module and return its importable module name."""
    file_path = tmp_path / f"{name}.py"
    file_path.write_text(source, encoding="utf-8")
    return name


def test_app_mount_string_is_lazy_by_default_and_eager_when_requested(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify App.mount() defers importing string targets by default and resolves when lazy=False."""
    monkeypatch.syspath_prepend(str(tmp_path))
    mod_name = _write_temp_mount_module(
        tmp_path,
        "lazy_mount_probe_mod",
        (
            "async def asgi_app(scope, receive, send):\n"
            "    await send({'type': 'http.response.start', 'status': 200, 'headers': []})\n"
            "    await send({'type': 'http.response.body', 'body': b'ok'})\n"
            "\n"
            "def wsgi_app(environ, start_response):\n"
            "    start_response('200 OK', [('Content-Type', 'text/plain')])\n"
            "    return [b'wsgi']\n"
            "\n"
            "def create_wsgi():\n"
            "    return wsgi_app\n"
        ),
    )
    sys.modules.pop(mod_name, None)

    app = App()
    app.mount("/api", f"{mod_name}:asgi_app", protocol="asgi")
    app.mount("/legacy", f"{mod_name}:create_wsgi()", protocol="wsgi")

    assert mod_name not in sys.modules
    assert app.mounts == (
        ("/api", f"{mod_name}:asgi_app", "asgi"),
        ("/legacy", f"{mod_name}:create_wsgi()", "wsgi"),
    )

    ensure_vertebra_integration(app, native=True)
    with enter_app_server_lifespan(app, native=True):
        assert mod_name not in sys.modules

    eager_app = App()
    eager_app.mount("/legacy", f"{mod_name}:create_wsgi()", protocol="wsgi", lazy=False)
    assert mod_name in sys.modules
    assert callable(eager_app.mounts[0][1])

    sys.modules.pop(mod_name, None)


def test_app_mount_rejects_malformed_string_target_eagerly() -> None:
    """Verify App.mount() rejects malformed import strings immediately even when lazy=True."""
    app = App()
    with pytest.raises(ValueError, match="module:attr"):
        app.mount("/api", "not_a_valid_target", protocol="asgi")

    with pytest.raises(ValueError, match="non-empty"):
        app.mount("/api", "pkg:", protocol="asgi")


def test_instantiate_mount_runners_resolves_mixed_lazy_asgi_wsgi_rsgi(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify _instantiate_mount_runners resolves lazy ASGI, WSGI, and RSGI string mounts."""
    monkeypatch.syspath_prepend(str(tmp_path))
    mod_name = _write_temp_mount_module(
        tmp_path,
        "mixed_mount_apps_mod",
        (
            "async def asgi_app(scope, receive, send):\n"
            "    await send({'type': 'http.response.start', 'status': 200, 'headers': []})\n"
            "    await send({'type': 'http.response.body', 'body': b'asgi'})\n"
            "\n"
            "def wsgi_app(environ, start_response):\n"
            "    start_response('200 OK', [('Content-Type', 'text/plain')])\n"
            "    return [b'wsgi']\n"
            "\n"
            "async def rsgi_app(scope, proto):\n"
            "    proto.response_bytes(200, [], b'rsgi')\n"
        ),
    )
    sys.modules.pop(mod_name, None)

    app = App()
    app.mount("/api", f"{mod_name}:asgi_app", protocol="asgi")
    app.mount("/legacy", f"{mod_name}:wsgi_app", protocol="wsgi")
    app.mount("/stream", f"{mod_name}:rsgi_app", protocol="rsgi")
    assert mod_name not in sys.modules

    runners = _instantiate_mount_runners(app.mounts)
    assert mod_name in sys.modules
    assert [(prefix, proto) for prefix, _runner, proto in runners] == [
        ("/api", "asgi"),
        ("/legacy", "wsgi"),
        ("/stream", "rsgi"),
    ]
    sys.modules.pop(mod_name, None)


def test_worker_startup_raises_worker_startup_error_on_bad_lazy_mount() -> None:
    """Verify Worker.run() raises WorkerStartupError with stage 'import' when a lazy mount fails."""
    worker = Worker(
        handlers={},
        mounts=[("/broken", "nonexistent_vertebra_mount_pkg_xyz:app", "asgi")],
    )
    with pytest.raises(WorkerStartupError) as exc_info:
        worker.run()
    assert exc_info.value.prefix == "/broken"
    assert exc_info.value.phase == "import"


def test_partition_batch_matches_longest_prefix_with_path_boundaries() -> None:
    """Verify fallback prefix matching selects the longest boundary-safe mount regardless of order."""
    mount_runners: list[tuple[str, Any, str]] = [
        ("", object(), "asgi"),
        ("/api", object(), "asgi"),
        ("/api/v2", object(), "wsgi"),
    ]
    batch = [
        SimpleNamespace(handler_id=0, path="/api/v2/users"),
        SimpleNamespace(handler_id=0, path="/api/items"),
        SimpleNamespace(handler_id=0, path="/apiv2/other"),
        SimpleNamespace(handler_id=0, path="/home"),
    ]
    plan = partition_batch(batch, {}, mount_runners)

    assert [req.path for _idx, req in plan.mounts["/api/v2"]] == ["/api/v2/users"]
    assert [req.path for _idx, req in plan.mounts["/api"]] == ["/api/items"]
    assert [req.path for _idx, req in plan.mounts[""]] == ["/apiv2/other", "/home"]
    assert plan.unmatched == []


def test_parse_serialize_merge_and_pyproject_mounts(tmp_path: Path) -> None:
    """Verify CLI mount parsing, JSON roundtrip, pyproject.toml loading, and override merging."""
    spec_asgi = parse_mount_spec("/api=pkg.asgi:app")
    spec_wsgi = parse_mount_spec("/legacy/=pkg.wsgi:create_app()@wsgi")
    spec_root = parse_mount_spec("/=pkg.root:app@asgi")

    assert spec_asgi == MountSpec(prefix="/api", target="pkg.asgi:app", protocol="auto")
    assert spec_wsgi == MountSpec(prefix="/legacy", target="pkg.wsgi:create_app()", protocol="wsgi")
    assert spec_root == MountSpec(prefix="", target="pkg.root:app", protocol="asgi")

    roundtripped = deserialize_mount_specs(serialize_mount_specs([spec_asgi, spec_wsgi, spec_root]))
    assert roundtripped == (spec_asgi, spec_wsgi, spec_root)

    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text(
        (
            "[tool.vertebra.mounts]\n"
            '"/api" = "pkg.asgi:app@asgi"\n'
            '"/legacy" = { target = "pkg.wsgi:app", protocol = "wsgi" }\n'
        ),
        encoding="utf-8",
    )
    loaded = load_mounts_pyproject_config(tmp_path)
    assert loaded == (
        MountSpec(prefix="/api", target="pkg.asgi:app", protocol="asgi"),
        MountSpec(prefix="/legacy", target="pkg.wsgi:app", protocol="wsgi"),
    )

    merged = merge_mount_specs(loaded, [MountSpec(prefix="/api", target="override.asgi:app", protocol="asgi")])
    assert merged == (
        MountSpec(prefix="/api", target="override.asgi:app", protocol="asgi"),
        MountSpec(prefix="/legacy", target="pkg.wsgi:app", protocol="wsgi"),
    )

    with pytest.raises(ValueError, match="PREFIX=MODULE:ATTR"):
        parse_mount_spec("missing_equals")
    with pytest.raises(ValueError, match="Invalid mount protocol"):
        parse_mount_spec("/api=pkg.asgi:app@graphql")


def test_build_mounted_app_from_env_and_module_entrypoint(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify vertebra.server._mounts:app constructs a lazy composite App from VERTEBRA_MOUNTS."""
    monkeypatch.syspath_prepend(str(tmp_path))
    base_mod = _write_temp_mount_module(
        tmp_path,
        "base_native_app_mod",
        (
            "from vertebra.framework._app import App\n"
            "app = App()\n"
            "@app.get('/health')\n"
            "def health() -> bytes:\n"
            "    return b'ok'\n"
        ),
    )
    sys.modules.pop(base_mod, None)

    specs = (
        MountSpec(prefix="/api", target="unloaded_api_mod:app", protocol="asgi"),
        MountSpec(prefix="/legacy", target="unloaded_wsgi_mod:app", protocol="wsgi"),
    )
    env = ServerLaunchEnv(
        app_path=MOUNT_ENTRYPOINT_APP_PATH,
        host="127.0.0.1",
        port=8000,
        mounts=specs,
        base_app_path=f"{base_mod}:app",
    ).to_environ(base={})

    assert env["VERTEBRA_APP"] == MOUNT_ENTRYPOINT_APP_PATH
    assert env["VERTEBRA_BASE_APP"] == f"{base_mod}:app"
    assert deserialize_mount_specs(env["VERTEBRA_MOUNTS"]) == specs

    monkeypatch.setenv("VERTEBRA_MOUNTS", env["VERTEBRA_MOUNTS"])
    monkeypatch.setenv("VERTEBRA_BASE_APP", env["VERTEBRA_BASE_APP"])

    composite = import_app(MOUNT_ENTRYPOINT_APP_PATH)
    assert isinstance(composite, App)
    assert len(composite.routes) == 1
    assert composite.mounts == (
        ("/api", "unloaded_api_mod:app", "asgi"),
        ("/legacy", "unloaded_wsgi_mod:app", "wsgi"),
    )
    assert "unloaded_api_mod" not in sys.modules
    assert "unloaded_wsgi_mod" not in sys.modules

    standalone = build_mounted_app_from_env({"VERTEBRA_MOUNTS": env["VERTEBRA_MOUNTS"]})
    assert isinstance(standalone, App)
    assert standalone.mounts == (
        ("/api", "unloaded_api_mod:app", "asgi"),
        ("/legacy", "unloaded_wsgi_mod:app", "wsgi"),
    )
    sys.modules.pop(base_mod, None)


def test_cli_run_accepts_repeatable_mount_option(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify vtb run --mount configures VERTEBRA_MOUNTS without requiring a positional APP."""
    captured: dict[str, Any] = {}

    def _fake_supervisor(
        *,
        server_cmd: list[str],
        env: dict[str, str],
        drain_timeout_ms: int,
        shutdown_timeout_ms: int,
    ) -> int:
        captured["env"] = dict(env)
        return 0

    monkeypatch.setattr(cli_run_mod, "build_server_command", lambda: ["vertebra-server"])
    monkeypatch.setattr(cli_run_mod, "run_supervisor", _fake_supervisor)

    runner = CliRunner()
    result = runner.invoke(
        cli_run_mod.run,
        [
            "--mount",
            "/api=pkg.asgi:app@asgi",
            "--mount",
            "/legacy=pkg.wsgi:app@wsgi",
            "--no-banner",
        ],
    )
    assert result.exit_code == 0, result.output
    assert captured["env"]["VERTEBRA_APP"] == MOUNT_ENTRYPOINT_APP_PATH
    assert deserialize_mount_specs(captured["env"]["VERTEBRA_MOUNTS"]) == (
        MountSpec(prefix="/api", target="pkg.asgi:app", protocol="asgi"),
        MountSpec(prefix="/legacy", target="pkg.wsgi:app", protocol="wsgi"),
    )


def test_resolve_mount_target_rejects_non_callable_factory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify resolve_mount_target raises ValueError when a '()' factory target is not callable."""
    monkeypatch.syspath_prepend(str(tmp_path))
    mod_name = _write_temp_mount_module(tmp_path, "non_callable_factory_mod", "NOT_CALLABLE = 123\n")
    sys.modules.pop(mod_name, None)

    with pytest.raises(ValueError, match="not callable"):
        resolve_mount_target(f"{mod_name}:NOT_CALLABLE()", prefix="/bad")
    sys.modules.pop(mod_name, None)
