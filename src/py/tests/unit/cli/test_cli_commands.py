"""Unit tests for newly added and hardened CLI commands and flags."""

import io
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from vertebra import App
from vertebra import supervisor as supervisor_pkg
from vertebra._vertebra import Response
from vertebra.build.packaging.standalone import StandaloneBuilder
from vertebra.build.packaging.targets import BuildTarget
from vertebra.cli import cli
from vertebra.cli import run as cli_run_mod
from vertebra.cli._launch import ServerLaunchEnv
from vertebra.cli.banner.renderer import render_banner
from vertebra.cli.errors import CLIError
from vertebra.cli.plugin_registry import ensure_vertebra_integration
from vertebra.cli.theme import Console
from vertebra.framework._app import _make_handler_id
from vertebra.server.worker import Worker, WorkerStartupError
from vertebra.supervisor import _supervisor as supervisor_mod
from vertebra.supervisor import _worker as supervisor_worker


def test_vtb_init_default_scaffolding(tmp_path: Path) -> None:
    """Verify vtb init creates pyproject.toml and app.py for default template."""
    runner = CliRunner()
    app_dir = tmp_path / "my_app"
    result = runner.invoke(cli, ["init", str(app_dir)])
    assert result.exit_code == 0
    assert app_dir.is_dir()
    pyproject = (app_dir / "pyproject.toml").read_text(encoding="utf-8")
    assert 'name = "' in pyproject
    assert 'dependencies = ["vertebra-core"]' in pyproject
    assert "[tool.vertebra]" in pyproject
    assert 'app = "app:app"' in pyproject
    assert "entry =" not in pyproject

    app_py = (app_dir / "app.py").read_text(encoding="utf-8")
    assert "from vertebra import App" in app_py
    assert '@app.get("/health")' in app_py


def test_vtb_init_web_template_scaffolding(tmp_path: Path) -> None:
    """Verify vtb init --template web creates src/main.ts and sets entry."""
    runner = CliRunner()
    app_dir = tmp_path / "web_app"
    result = runner.invoke(cli, ["init", str(app_dir), "--template", "web"])
    assert result.exit_code == 0
    assert app_dir.is_dir()
    pyproject = (app_dir / "pyproject.toml").read_text(encoding="utf-8")
    assert 'entry = "src/main.ts"' in pyproject
    assert (app_dir / "src" / "main.ts").is_file()


def test_vtb_init_refuses_non_empty_directory(tmp_path: Path) -> None:
    """Verify vtb init fails when target directory is non-empty."""
    runner = CliRunner()
    app_dir = tmp_path / "existing_app"
    app_dir.mkdir()
    (app_dir / "existing.txt").write_text("content", encoding="utf-8")

    result = runner.invoke(cli, ["init", str(app_dir)])
    assert result.exit_code != 0


def test_server_launch_env_to_environ() -> None:
    """Verify ServerLaunchEnv populates all required VERTEBRA_* environment variables."""
    launch_env = ServerLaunchEnv(
        app_path="demo:app",
        host="0.0.0.0",
        port=9000,
        workers=4,
        log_level="debug",
        dev=True,
        native=True,
    )
    env = launch_env.to_environ()
    assert env["VERTEBRA_APP"] == "demo:app"
    assert env["LITESTAR_APP"] == "demo:app"
    assert env["VERTEBRA_HOST"] == "0.0.0.0"
    assert env["VERTEBRA_PORT"] == "9000"
    assert env["VERTEBRA_WORKERS"] == "4"
    assert env["VERTEBRA_LOG_LEVEL"] == "debug"
    assert env["VERTEBRA_DEV"] == "1"
    assert env["VERTEBRA_NATIVE"] == "1"
    assert "PYTHONPATH" in env


def test_cli_error_show_with_custom_file() -> None:
    """Verify CLIError.show respects custom file argument."""
    stream = io.StringIO()
    err = CLIError("Test failure message", hint="Try doing X")
    err.show(file=stream)
    rendered = stream.getvalue()
    assert "Test failure message" in rendered
    assert "Try doing X" in rendered


def test_standalone_builder_records_profile(tmp_path: Path) -> None:
    """Verify StandaloneBuilder records profile in build-manifest.json."""
    project_dir = tmp_path / "app"
    project_dir.mkdir()
    (project_dir / "pyproject.toml").write_text('[project]\nname = "test"\n', encoding="utf-8")
    (project_dir / "main.py").write_text("def run(): pass\n", encoding="utf-8")

    target = BuildTarget.detect_host()
    out = tmp_path / "out"
    builder = StandaloneBuilder(project_dir, target=target, entrypoint="main:run", profile="debug")
    builder.build(out)

    manifest_text = (out / "build-manifest.json").read_text(encoding="utf-8")
    assert '"profile": "debug"' in manifest_text


def test_render_banner_without_log_level() -> None:
    """Verify render_banner executes cleanly without log_level."""
    stream = io.StringIO()
    vc = Console(file=stream, force_terminal=True)
    render_banner(mode="serve", host="127.0.0.1", port=8000, workers=1, vc=vc)
    output = stream.getvalue()
    assert len(output) > 0


def test_vtb_run_preserves_env_workers_topology_and_threads(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify omitting --workers, --topology, and --threads preserves VERTEBRA_* env vars."""
    captured: dict[str, Any] = {}

    def fake_build_cmd() -> list[str]:
        return ["/bin/true"]

    def fake_supervisor(*, server_cmd: list[str], env: dict[str, str], **_kwargs: Any) -> int:
        captured["cmd"] = server_cmd
        captured["env"] = dict(env)
        return 0

    monkeypatch.setattr(cli_run_mod, "build_server_command", fake_build_cmd)
    monkeypatch.setattr(cli_run_mod, "run_supervisor", fake_supervisor)
    monkeypatch.setenv("VERTEBRA_WORKERS", "4")
    monkeypatch.setenv("VERTEBRA_TOPOLOGY", "shm")
    monkeypatch.setenv("VERTEBRA_THREADS", "2")

    runner = CliRunner()
    result = runner.invoke(cli, ["run", "demo:app", "--no-banner"])
    assert result.exit_code == 0, result.output
    assert captured["env"]["VERTEBRA_WORKERS"] == "4"
    assert captured["env"]["VERTEBRA_TOPOLOGY"] == "shm"
    assert captured["env"]["VERTEBRA_THREADS"] == "2"

    monkeypatch.setattr("vertebra.cli._launch.is_free_threaded_runtime", lambda: False)
    auto_result = runner.invoke(cli, ["run", "demo:app", "--topology", "auto", "--workers", "4", "--no-banner"])
    assert auto_result.exit_code == 0, auto_result.output
    assert captured["env"]["VERTEBRA_TOPOLOGY"] == "shm"
    assert captured["env"]["VERTEBRA_WORKERS"] == "4"


def test_vtb_run_jit_flag_sets_python_jit_on_gil_and_skips_freethreaded(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify --jit configures PYTHON_JIT=1 on GIL builds and skips free-threaded builds."""
    captured: dict[str, Any] = {}

    def fake_build_cmd() -> list[str]:
        return ["/bin/true"]

    def fake_supervisor(*, server_cmd: list[str], env: dict[str, str], **_kwargs: Any) -> int:
        captured["cmd"] = server_cmd
        captured["env"] = dict(env)
        return 0

    monkeypatch.setattr(cli_run_mod, "build_server_command", fake_build_cmd)
    monkeypatch.setattr(cli_run_mod, "run_supervisor", fake_supervisor)
    monkeypatch.setattr(cli_run_mod, "is_free_threaded_runtime", lambda: False)

    runner = CliRunner()
    jit_on = runner.invoke(cli, ["run", "demo:app", "--jit", "--no-banner"])
    assert jit_on.exit_code == 0, jit_on.output
    assert captured["env"]["PYTHON_JIT"] == "1"

    jit_off = runner.invoke(cli, ["run", "demo:app", "--no-jit", "--no-banner"])
    assert jit_off.exit_code == 0, jit_off.output
    assert captured["env"]["PYTHON_JIT"] == "0"

    monkeypatch.setattr(cli_run_mod, "is_free_threaded_runtime", lambda: True)
    monkeypatch.delenv("PYTHON_JIT", raising=False)
    jit_ft = runner.invoke(cli, ["run", "demo:app", "--jit", "--no-banner"])
    assert jit_ft.exit_code == 0, jit_ft.output
    assert captured["env"].get("PYTHON_JIT") != "1"


def test_worker_main_asgi_app_avoids_double_mount(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify VERTEBRA_NATIVE=0 routes App once as ASGI without double-mounting or native handlers."""
    captured: list[tuple[dict[int, Any], list[tuple[str, Any, str]]]] = []

    async def raw_asgi(scope: dict[str, Any], receive: Any, send: Any) -> None:
        _ = (scope, receive, send)

    mounted_app = App()
    mounted_app.mount("/", raw_asgi, protocol="asgi")

    native_app = App()

    @native_app.get("/hello")
    def hello() -> bytes:
        return b"ok"

    def fake_run_worker(handlers: dict[int, Any], *, mounts: list[tuple[str, Any, str]], **_kwargs: Any) -> None:
        captured.append((dict(handlers), list(mounts)))

    monkeypatch.setattr(supervisor_worker, "run_worker", fake_run_worker)

    monkeypatch.setenv("VERTEBRA_APP", "demo:mounted")
    monkeypatch.setenv("VERTEBRA_NATIVE", "0")
    monkeypatch.setattr(supervisor_worker, "import_app", lambda _path: mounted_app)
    supervisor_worker.main()
    handlers_mounted, mounts_mounted = captured[-1]
    assert handlers_mounted == {}
    assert len(mounts_mounted) == 1
    assert mounts_mounted[0][1] is raw_asgi

    monkeypatch.setenv("VERTEBRA_APP", "demo:native")
    monkeypatch.setenv("VERTEBRA_NATIVE", "0")
    monkeypatch.setattr(supervisor_worker, "import_app", lambda _path: native_app)
    supervisor_worker.main()
    handlers_native_off, mounts_native_off = captured[-1]
    assert handlers_native_off == {}
    assert len(mounts_native_off) == 1
    assert callable(mounts_native_off[0][1])

    monkeypatch.setenv("VERTEBRA_APP", "demo:raw")
    monkeypatch.setenv("VERTEBRA_NATIVE", "1")
    monkeypatch.setattr(supervisor_worker, "import_app", lambda _path: raw_asgi)
    supervisor_worker.main()
    handlers_raw, mounts_raw = captured[-1]
    assert handlers_raw == {}
    assert len(mounts_raw) == 1
    assert mounts_raw[0][1] is raw_asgi


def test_find_server_binary_honors_cargo_target_dir_and_newest_release(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify find_server_binary prioritizes CARGO_TARGET_DIR and freshest release artifacts."""
    monkeypatch.delenv("VERTEBRA_SERVER_BINARY", raising=False)
    fake_repo = tmp_path / "repo"
    stale_bin = fake_repo / "src" / "rs" / "target" / "release" / "vertebra-server"
    stale_bin.parent.mkdir(parents=True)
    stale_bin.write_bytes(b"stale")
    os.utime(stale_bin, ns=(1_000_000_000, 1_000_000_000))

    cargo_target = tmp_path / "cargo-out"
    fresh_bin = cargo_target / "release" / "vertebra-server"
    fresh_bin.parent.mkdir(parents=True)
    fresh_bin.write_bytes(b"fresh")
    os.utime(fresh_bin, ns=(2_000_000_000, 2_000_000_000))

    monkeypatch.setattr(supervisor_mod, "find_repo_root", lambda _start: fake_repo)
    monkeypatch.setenv("CARGO_TARGET_DIR", str(cargo_target))
    assert supervisor_mod.find_server_binary() == str(fresh_bin)


def test_resolve_worker_targets_registers_dev_welcome_route_when_vertebra_dev_enabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify resolve_worker_targets finalizes the GET / welcome route when VERTEBRA_DEV=1."""
    monkeypatch.setenv("VERTEBRA_DEV", "1")
    app = App()

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    handlers, mounts = supervisor_worker.resolve_worker_targets(app, native=True)
    root_id = _make_handler_id("GET", "/")
    assert root_id in handlers
    assert mounts == []
    resp = handlers[root_id]()
    assert isinstance(resp, Response)
    assert resp.status == 200
    assert (b"content-type", b"text/html; charset=utf-8") in resp.headers
    assert b"/health" in bytes(resp.body)


def test_resolve_worker_targets_omits_dev_welcome_route_in_production(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify resolve_worker_targets omits the GET / welcome route when debug=False and VERTEBRA_DEV is unset."""
    monkeypatch.delenv("VERTEBRA_DEV", raising=False)
    app = App(debug=False)

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    handlers, _ = supervisor_worker.resolve_worker_targets(app, native=True)
    assert _make_handler_id("GET", "/") not in handlers


@pytest.mark.anyio
async def test_resolve_worker_targets_asgi_fallback_serves_dev_welcome_route(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify resolve_worker_targets(native=False) ASGI fallback serves GET / in dev mode."""
    monkeypatch.setenv("VERTEBRA_DEV", "1")
    app = App()

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    _, mounts = supervisor_worker.resolve_worker_targets(app, native=False)
    assert len(mounts) == 1
    asgi_adapter = mounts[0][1]

    sent: list[dict[str, Any]] = []

    async def receive() -> dict[str, Any]:
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(msg: dict[str, Any]) -> None:
        sent.append(msg)

    await asgi_adapter({"type": "http", "method": "GET", "path": "/"}, receive, send)
    start_msg = next(m for m in sent if m["type"] == "http.response.start")
    body_msg = next(m for m in sent if m["type"] == "http.response.body")
    assert start_msg["status"] == 200
    assert (b"content-type", b"text/html; charset=utf-8") in start_msg["headers"]
    assert b"VERTEBRA" in body_msg["body"]


def test_worker_init_finalizes_dev_welcome_route_on_app_instance(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify Worker(app) calls ensure_dev_welcome_route on App instances."""
    monkeypatch.delenv("VERTEBRA_DEV", raising=False)
    app = App(debug=True)
    worker = Worker(app)
    assert _make_handler_id("GET", "/") in worker._handlers


def test_vertebra_extension_group_plugin_cli_and_auto_integration(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify `vtb --app ...` loads CLIPlugin commands with injected app/env, supports optional APP on `vtb run`, and auto-attaches VertebraPlugin."""
    app_module = tmp_path / "custom_cli_app.py"
    app_module.write_text(
        "import click\n"
        "from vertebra import App\n"
        "from vertebra.cli.plugin_registry import CLIPlugin, VertebraEnv\n\n"
        "class SeedPlugin(CLIPlugin):\n"
        "    def on_cli_init(self, cli: click.Group) -> None:\n"
        "        @cli.command('seed-data')\n"
        "        def seed_data(app: App, env: VertebraEnv) -> None:\n"
        "            click.echo(f'seeded:{type(app).__name__}:{env.app_path}')\n\n"
        "app = App(plugins=[SeedPlugin()])\n"
        "@app.get('/ok')\n"
        "def ok() -> dict[str, str]:\n"
        "    return {'ok': 'yes'}\n",
        encoding="utf-8",
    )

    runner = CliRunner()
    plugin_res = runner.invoke(
        cli,
        ["--app-dir", str(tmp_path), "--app", "custom_cli_app:app", "seed-data"],
    )
    assert plugin_res.exit_code == 0, plugin_res.output
    assert "seeded:App:custom_cli_app:app" in plugin_res.output

    captured: dict[str, Any] = {}

    def fake_build_cmd() -> list[str]:
        return ["/bin/true"]

    def fake_supervisor(*, server_cmd: list[str], env: dict[str, str], **_kwargs: Any) -> int:
        captured["cmd"] = server_cmd
        captured["env"] = dict(env)
        return 0

    monkeypatch.setattr(cli_run_mod, "build_server_command", fake_build_cmd)
    monkeypatch.setattr(cli_run_mod, "run_supervisor", fake_supervisor)

    run_res = runner.invoke(
        cli,
        ["--app-dir", str(tmp_path), "--app", "custom_cli_app:app", "run", "--no-banner"],
    )
    assert run_res.exit_code == 0, run_res.output
    assert captured["env"]["VERTEBRA_APP"] == "custom_cli_app:app"

    litestar_app_mod = tmp_path / "unmodified_ls.py"
    litestar_app_mod.write_text(
        "from litestar import Litestar, get\n"
        "from litestar.config.compression import CompressionConfig\n\n"
        "@get('/auto', sync_to_thread=False)\n"
        "def auto_route() -> dict[str, str]:\n"
        "    return {'auto': 'attached'}\n\n"
        "app = Litestar(\n"
        "    route_handlers=[auto_route],\n"
        "    compression_config=CompressionConfig(backend='gzip', minimum_size=128),\n"
        ")\n",
        encoding="utf-8",
    )

    ls_run_res = runner.invoke(
        cli,
        ["--app-dir", str(tmp_path), "--app", "unmodified_ls:app", "run", "--no-banner"],
    )
    assert ls_run_res.exit_code == 0, ls_run_res.output
    assert captured["env"]["VERTEBRA_APP"] == "unmodified_ls:app"
    assert "VERTEBRA_OFFLOAD_CONFIG" in captured["env"]

    class _FakeStarletteApp:
        def __init__(self) -> None:
            self.routes: list[Any] = []
            self.user_middleware: list[Any] = []
            self.exception_handlers: dict[Any, Any] = {}
            self.state = type("State", (), {})()
            self.router = type("Router", (), {"routes": self.routes})()

        async def __call__(self, _scope: Any, _receive: Any, _send: Any) -> None:
            return None

    fake_st = _FakeStarletteApp()
    ensure_vertebra_integration(fake_st, native=True)
    assert getattr(fake_st, "__vertebra_fastpath__", None) is not None


@pytest.mark.parametrize(
    "export_name",
    [
        "build_server_command",
        "find_repo_root",
        "find_server_binary",
        "run_supervisor",
        "worker_main",
    ],
)
def test_supervisor_package_lazy_exports(export_name: str) -> None:
    """Verify vertebra.supervisor lazily exports expected supervisor helpers."""
    assert export_name in supervisor_pkg.__all__
    exported = getattr(supervisor_pkg, export_name)
    assert callable(exported)


def test_supervisor_import_in_fresh_interpreter_and_worker_module_help() -> None:
    """Fresh import of vertebra.supervisor has no circular import and -m _worker --help emits no runpy warning."""
    import_proc = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "from vertebra.supervisor import build_server_command, find_repo_root, "
                "find_server_binary, run_supervisor, worker_main; "
                "assert all(callable(fn) for fn in (build_server_command, find_repo_root, find_server_binary, run_supervisor, worker_main))"
            ),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert import_proc.returncode == 0, import_proc.stderr

    help_proc = subprocess.run(
        [sys.executable, "-W", "error::RuntimeWarning", "-m", "vertebra.supervisor._worker", "--help"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert help_proc.returncode == 0, help_proc.stderr
    assert "RuntimeWarning" not in help_proc.stderr


def test_supervisor_worker_main_exits_78_on_worker_startup_error(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Verify supervisor._worker.main exits with code 78 and logs traceback on WorkerStartupError."""
    monkeypatch.setenv("VERTEBRA_APP", "dummy:app")
    monkeypatch.setattr(supervisor_worker, "import_app", lambda _path: object())
    monkeypatch.setattr(supervisor_worker, "resolve_worker_targets", lambda *_args, **_kwargs: ({}, []))
    monkeypatch.setattr(
        supervisor_worker,
        "run_worker",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(WorkerStartupError("/api", "lifespan")),
    )
    monkeypatch.setattr(sys, "argv", ["vertebra-worker"])

    with caplog.at_level("ERROR"), pytest.raises(SystemExit) as exc_info:
        supervisor_worker.main()

    assert exc_info.value.code == supervisor_worker.EXIT_STARTUP_FAILURE == 78
    assert any("Worker startup failed" in record.getMessage() for record in caplog.records)


@pytest.mark.parametrize(
    "conflicting_flag",
    [["--with-workers"], ["--oneshot-job", "sync_all"]],
)
def test_vtb_run_worker_only_rejects_conflicting_flags(
    conflicting_flag: list[str],
) -> None:
    """vtb run --worker-only rejects --with-workers and --oneshot-job with exit code 2."""
    runner = CliRunner()
    res = runner.invoke(
        cli,
        ["run", "sample_app:app", "--worker-only", "--no-banner", *conflicting_flag],
    )
    assert res.exit_code == 2
    assert "cannot be combined" in res.output


def test_vtb_run_worker_only_invokes_execute_worker_cli_without_server_spawn(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """vtb run --worker-only dispatches to execute_worker_cli with run_until_stopped=True and health_probe."""
    recorded: dict[str, Any] = {}

    def fake_execute_worker_cli(
        app_path: str,
        *,
        queues: str,
        concurrency: str,
        drain_timeout_s: float,
        oneshot_job: str | None = None,
        payload: Any = None,
        run_until_stopped: bool = False,
        health_probe: tuple[str, int] | None = None,
    ) -> int:
        recorded["app_path"] = app_path
        recorded["queues"] = queues
        recorded["concurrency"] = concurrency
        recorded["drain_timeout_s"] = drain_timeout_s
        recorded["oneshot_job"] = oneshot_job
        recorded["payload"] = payload
        recorded["run_until_stopped"] = run_until_stopped
        recorded["health_probe"] = health_probe
        return 0

    def fail_supervisor(**_kwargs: Any) -> int:
        msg = "run_supervisor must not be called in --worker-only mode"
        raise AssertionError(msg)

    monkeypatch.setattr("vertebra.cli.workers.execute_worker_cli", fake_execute_worker_cli)
    monkeypatch.setattr(cli_run_mod, "run_supervisor", fail_supervisor)

    runner = CliRunner()
    res = runner.invoke(
        cli,
        [
            "run",
            "sample_app:app",
            "--worker-only",
            "--host",
            "127.0.0.1",
            "--port",
            "9099",
            "--queues",
            "default,emails",
            "--lane-concurrency",
            "default=4,emails=2",
            "--drain-timeout",
            "15000",
            "--no-banner",
        ],
    )
    assert res.exit_code == 0, res.output
    assert recorded == {
        "app_path": "sample_app:app",
        "queues": "default,emails",
        "concurrency": "4,2",
        "drain_timeout_s": 15.0,
        "oneshot_job": None,
        "payload": None,
        "run_until_stopped": True,
        "health_probe": ("127.0.0.1", 9099),
    }


def test_embedded_python_runtime_detection_guard_and_env_apply(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify packaged binary detection, static-interpreter guard, and loader path injection."""
    pkg_bin_dir = Path(supervisor_mod.__file__).resolve().parent.parent / "bin"
    pkg_bin = pkg_bin_dir / "vertebra-server"
    dev_bin = tmp_path / "target" / "release" / "vertebra-server"

    monkeypatch.delenv("VERTEBRA_SERVER_BINARY", raising=False)
    assert supervisor_mod.is_packaged_server_binary(pkg_bin) is True
    assert supervisor_mod.is_packaged_server_binary(dev_bin) is False

    monkeypatch.setenv("VERTEBRA_SERVER_BINARY", str(pkg_bin))
    assert supervisor_mod.is_packaged_server_binary(pkg_bin) is False
    monkeypatch.delenv("VERTEBRA_SERVER_BINARY", raising=False)

    monkeypatch.setattr(supervisor_mod.sys, "platform", "linux")
    monkeypatch.setattr(supervisor_mod.sys, "base_prefix", str(tmp_path / "pyhome"))
    monkeypatch.setattr(
        supervisor_mod.sysconfig,
        "get_config_var",
        lambda key: {
            "LIBDIR": str(tmp_path / "pyhome" / "lib"),
            "LDLIBRARY": "libpython3.12.so.1.0",
            "ENABLE_SHARED": 1,
        }.get(key),
    )
    detected = supervisor_mod.EmbeddedPythonRuntime.detect()
    assert detected.is_shared is True
    assert detected.python_home == tmp_path / "pyhome"
    detected.require_shared_library(pkg_bin)

    standalone_lib = tmp_path / "standalone-home" / "lib" / "libpython3.12.so"
    standalone_lib.parent.mkdir(parents=True)
    standalone_lib.write_bytes(b"ELF")
    monkeypatch.setattr(supervisor_mod.sys, "base_prefix", str(tmp_path / "standalone-home"))
    monkeypatch.setattr(
        supervisor_mod.sysconfig,
        "get_config_var",
        lambda key: {
            "LIBDIR": str(standalone_lib.parent),
            "LDLIBRARY": "libpython3.12.so",
            "ENABLE_SHARED": None,
        }.get(key),
    )
    assert supervisor_mod.EmbeddedPythonRuntime.detect().is_shared is True

    monkeypatch.setattr(
        supervisor_mod.sysconfig,
        "get_config_var",
        lambda key: {
            "LIBDIR": str(tmp_path / "missing-home" / "lib"),
            "LDLIBRARY": "libpython3.12.so",
            "ENABLE_SHARED": None,
        }.get(key),
    )
    assert supervisor_mod.EmbeddedPythonRuntime.detect().is_shared is False

    monkeypatch.setattr(supervisor_mod.sys, "base_prefix", str(tmp_path / "pyhome"))

    linux_env: dict[str, str] = {"LD_LIBRARY_PATH": "/existing/lib"}
    detected.apply(linux_env)
    assert linux_env["PYTHONHOME"] == str(tmp_path / "pyhome")
    assert linux_env["LD_LIBRARY_PATH"] == f"{tmp_path / 'pyhome' / 'lib'}{os.pathsep}/existing/lib"

    mac_rt = supervisor_mod.EmbeddedPythonRuntime(
        python_home=tmp_path / "pyhome",
        library_dir=tmp_path / "pyhome" / "lib",
        library_name="libpython3.12.dylib",
        is_shared=True,
        platform="darwin",
    )
    mac_env: dict[str, str] = {"DYLD_LIBRARY_PATH": "/opt/lib"}
    mac_rt.apply(mac_env)
    assert mac_env["DYLD_LIBRARY_PATH"] == f"{tmp_path / 'pyhome' / 'lib'}{os.pathsep}/opt/lib"

    win_rt = supervisor_mod.EmbeddedPythonRuntime(
        python_home=tmp_path / "pyhome",
        library_dir=tmp_path / "pyhome",
        library_name="python312.dll",
        is_shared=True,
        platform="win32",
    )
    win_env: dict[str, str] = {"PATH": "/usr/bin"}
    win_rt.apply(win_env)
    assert win_env["PATH"] == f"{tmp_path / 'pyhome'}{os.pathsep}/usr/bin"
    win_rt.require_shared_library(pkg_bin)

    static_rt = supervisor_mod.EmbeddedPythonRuntime(
        python_home=tmp_path / "static-home",
        library_dir=None,
        library_name="libpython3.12.a",
        is_shared=False,
        platform="linux",
    )
    static_rt.require_shared_library(dev_bin)
    with pytest.raises(SystemExit, match="shared libpython"):
        static_rt.require_shared_library(pkg_bin)

    monkeypatch.setattr(supervisor_mod, "find_server_binary", lambda: str(pkg_bin))
    monkeypatch.setattr(supervisor_mod.EmbeddedPythonRuntime, "detect", classmethod(lambda cls: static_rt))
    runner = CliRunner()
    app_file = tmp_path / "sample_cli_app.py"
    app_file.write_text('"""Minimal app."""\nfrom vertebra import App\napp = App()\n')
    monkeypatch.syspath_prepend(str(tmp_path))
    res = runner.invoke(cli, ["run", "sample_cli_app:app", "--no-banner"])
    assert res.exit_code == 2
    assert "shared libpython" in res.output

    recorded_env: dict[str, str] = {}

    class _FakePopen:
        def __init__(self, _cmd: list[str], env: dict[str, str]) -> None:
            recorded_env.update(env)

        def wait(self, timeout: float | None = None) -> int:
            return 0

    monkeypatch.setattr(supervisor_mod.EmbeddedPythonRuntime, "detect", classmethod(lambda cls: detected))
    monkeypatch.setattr(supervisor_mod.subprocess, "Popen", _FakePopen)
    exit_code = supervisor_mod.run_supervisor(server_cmd=[str(dev_bin)], env={"CUSTOM_VAR": "1"})
    assert exit_code == 0
    assert recorded_env["PYTHONHOME"] == str(tmp_path / "pyhome")
    assert recorded_env["LD_LIBRARY_PATH"].startswith(str(tmp_path / "pyhome" / "lib"))
