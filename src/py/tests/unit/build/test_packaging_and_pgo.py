"""Unit tests for vertebra.build (targets, standalone, onefile), CLI flags, and tools/pgo."""

import ast
import os
import re
import subprocess
import sys
import tomllib
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from click.testing import CliRunner
from sqlalchemy.engine.interfaces import Dialect
from tools.pgo import python_pgo as tools_python_pgo
from tools.pgo import rust_pgo as tools_rust_pgo

from vertebra.build.packaging import (
    EXTENSION_MANIFESTS,
    ONEFILE_MAGIC,
    ONEFILE_TRAILER_SIZE,
    SUPPORTED_PYTHON_VERSIONS,
    BuildTarget,
    OnefileBuilder,
    PGOPipelineResult,
    StandaloneBuilder,
    find_llvm_profdata,
    resolve_cargo_release_dir,
    run_default_pgo_workload,
    run_pgo_pipeline,
)
from vertebra.build.packaging import pgo as packaging_pgo
from vertebra.build.packaging.pgo import (
    CPYTHON_PGO_FLAGS,
    PLATFORM_MATRIX,
    PythonBuildInfo,
    RustPGOConfig,
    build_flags,
    build_instrumented_extensions,
    build_optimized,
    build_optimized_extension,
    build_optimized_extensions,
    check_pgo_availability,
    check_profdata_size,
    detect_python_build,
    get_platform_support,
    merge_profiles,
    validate_profdata,
)
from vertebra.cli import cli
from vertebra.cli.build import build as build_command
from vertebra.utils.module_loader import ensure_module
from vertebra.utils.serializers import decode_json


def test_build_target_resolution_and_pbs_urls() -> None:
    """Verify host detection, cross-target lookup, and 3.12 / 3.14t python-build-standalone URLs."""
    host = BuildTarget.detect_host()
    assert host.os_name in {"linux", "darwin", "windows"}
    assert host.rust_triple

    linux_ft = BuildTarget.resolve("linux-x86_64", python_version="3.14t")
    assert linux_ft.rust_triple == "x86_64-unknown-linux-gnu"
    assert linux_ft.is_freethreaded is True
    assert "freethreaded" in linux_ft.pbs_archive_name

    mac_arm = BuildTarget.resolve("darwin-aarch64", python_version="3.12")
    assert mac_arm.rust_triple == "aarch64-apple-darwin"
    assert mac_arm.is_freethreaded is False
    assert mac_arm.pbs_url.startswith("https://github.com/astral-sh/python-build-standalone/releases/download/")


def test_standalone_and_onefile_builder_roundtrip(tmp_path: Path) -> None:
    """Verify StandaloneBuilder stages bytecode and OnefileBuilder appends and verifies the VTB1FILE trailer."""
    project_dir = tmp_path / "sample_app"
    pkg_dir = project_dir / "app"
    pkg_dir.mkdir(parents=True)
    (project_dir / "pyproject.toml").write_text('[project]\nname = "sample-app"\nversion = "0.1.0"\n')
    (pkg_dir / "__init__.py").write_text('"""Sample package."""\nVALUE = 42\n')
    (pkg_dir / "main.py").write_text('"""Entrypoint."""\ndef run() -> int:\n    return 42\n')

    target = BuildTarget.resolve("linux-x86_64", python_version="3.12")
    dist_dir = tmp_path / "dist" / "standalone"
    standalone = StandaloneBuilder(project_dir=project_dir, target=target, entrypoint="app.main:run")
    staged = standalone.build(dist_dir)

    assert (staged / "manifest.json").exists()
    assert (staged / "__main__.py").exists()
    pyc_files = list((staged / "app").rglob("*.pyc"))
    assert len(pyc_files) >= 2

    onefile_out = tmp_path / "dist" / "sample-app-bin"
    onefile = OnefileBuilder(dist_dir=staged, target=target)
    binary_path = onefile.build(onefile_out)

    assert binary_path.exists()
    raw_bytes = binary_path.read_bytes()
    assert len(raw_bytes) > ONEFILE_TRAILER_SIZE
    assert raw_bytes[-ONEFILE_TRAILER_SIZE : -ONEFILE_TRAILER_SIZE + 8] == ONEFILE_MAGIC

    meta = OnefileBuilder.inspect_trailer(binary_path)
    assert meta["payload_size"] > 0
    assert len(str(meta["sha256"])) == 64

    extract_dir = tmp_path / "extracted"
    onefile.extract_to(binary_path, extract_dir)
    assert (extract_dir / "manifest.json").exists()
    assert (extract_dir / "__main__.py").exists()

    cache_home = tmp_path / "xdg_cache"
    env = dict(os.environ)
    env["XDG_CACHE_HOME"] = str(cache_home)
    proc = subprocess.run([str(binary_path)], env=env, capture_output=True, text=True, check=False)
    assert proc.returncode == 42, proc.stderr


def test_cli_build_standalone_onefile_and_target_flags(tmp_path: Path) -> None:
    """Verify vtb build CLI supports --standalone, --onefile, and --target options and rejects --pgo."""
    project_dir = tmp_path / "cli_project"
    app_dir = project_dir / "service"
    app_dir.mkdir(parents=True)
    (project_dir / "pyproject.toml").write_text('[project]\nname = "cli-service"\nversion = "0.1.0"\n')
    (app_dir / "__init__.py").write_text('"""Service package."""\n')

    runner = CliRunner()
    output_bin = tmp_path / "out" / "cli-service"
    result = runner.invoke(
        build_command,
        [
            "--project-dir",
            str(project_dir),
            "--standalone",
            "--onefile",
            "--target",
            "linux-x86_64",
            "--output",
            str(output_bin),
        ],
    )
    assert result.exit_code == 0, result.output
    assert output_bin.exists()

    pgo_rejected = runner.invoke(
        build_command,
        [
            "--project-dir",
            str(project_dir),
            "--standalone",
            "--pgo",
        ],
    )
    assert pgo_rejected.exit_code != 0
    assert "No such option" in pgo_rejected.output and "--pgo" in pgo_rejected.output

    root_out = tmp_path / "out_root" / "cli-service"
    root_result = runner.invoke(
        cli,
        [
            "build",
            "--project-dir",
            str(project_dir),
            "--standalone",
            "--onefile",
            "--target",
            "linux-x86_64",
            "--output",
            str(root_out),
        ],
    )
    assert root_result.exit_code == 0, root_result.output
    assert root_out.exists()


def test_rust_and_python_pgo_tooling(tmp_path: Path) -> None:
    """Verify RustPGOConfig flag generation, profraw validation, and CPython PGO inspection."""
    profile_dir = tmp_path / "profiles"
    cfg = RustPGOConfig(crate_name="vertebra-http", profile_dir=profile_dir)

    gen_flags = build_flags(cfg, pgo_mode="generate", target_cpu="native")
    assert f"-Cprofile-generate={profile_dir}" in gen_flags
    assert "-Ctarget-cpu=native" in gen_flags

    profdata_file = tmp_path / "merged.profdata"
    use_flags = build_flags(cfg, pgo_mode="use", profdata_path=profdata_file, target_cpu="generic")
    assert f"-Cprofile-use={profdata_file}" in use_flags
    assert "-Cllvm-args=-pgo-warn-missing-function" in use_flags
    assert "-Ctarget-cpu=generic" in use_flags

    with pytest.raises(ValueError, match="profdata_path is required"):
        build_flags(cfg, pgo_mode="use", profdata_path=None)

    assert check_profdata_size(profdata_file) == 0
    with pytest.raises(FileNotFoundError, match=r"No \.profraw files"):
        merge_profiles(profile_dir, profdata_file, llvm_profdata="llvm-profdata")

    py_info = detect_python_build()
    assert isinstance(py_info, PythonBuildInfo)
    assert py_info.version
    avail = check_pgo_availability()
    assert {"pgo", "lto", "bolt", "debug", "tail_call_interp", "jit_available", "jit_enabled", "free_threaded"} <= set(
        avail.keys()
    )
    assert "--enable-optimizations" in CPYTHON_PGO_FLAGS
    assert "--with-tail-call-interp" in CPYTHON_PGO_FLAGS
    assert "--enable-experimental-jit" in CPYTHON_PGO_FLAGS
    assert len(PLATFORM_MATRIX) >= 4
    support = get_platform_support()
    assert support.cpython_pgo is True


@pytest.mark.parametrize("mod", [packaging_pgo, tools_python_pgo])
def test_python_build_info_detects_315_tail_call_jit_freethreaded_and_frame_pointers(
    monkeypatch: pytest.MonkeyPatch,
    mod: Any,
) -> None:
    """Verify PythonBuildInfo detects Python 3.15 tail-call interpreter, JIT, free-threading, and frame pointers."""
    synthetic_vars = {
        "CONFIG_ARGS": "'--enable-optimizations' '--with-lto' '--with-tail-call-interp' '--enable-experimental-jit'",
        "Py_TAIL_CALL_INTERP": "1",
        "Py_GIL_DISABLED": "1",
        "CFLAGS_NODIST": "-fno-omit-frame-pointer -mno-omit-leaf-frame-pointer",
    }
    fake_jit = SimpleNamespace(is_available=lambda: True, is_enabled=lambda: True)

    monkeypatch.setattr(mod, "_query_sysconfig", lambda _exe: dict(synthetic_vars))
    monkeypatch.setattr(mod, "_query_version", lambda _exe: "3.15.0rc3")
    monkeypatch.setattr(mod.sys, "_jit", fake_jit, raising=False)
    info = mod.detect_python_build()
    assert info.version == "3.15.0rc3"
    assert info.is_pgo is True
    assert info.is_lto is True
    assert info.is_tail_call_interp is True
    assert info.is_jit_available is True
    assert info.is_jit_enabled is True
    assert info.is_free_threaded is True
    assert info.has_frame_pointers is True


@pytest.mark.parametrize(
    "target",
    ["cp312-*", "cp313-*", "cp314-*", "cp314t-*", "cp315-*", "cp315t-*"],
)
def test_release_workflow_python_315_matrix(target: str) -> None:
    """Verify release.yaml covers supported Python and free-threaded wheel targets."""
    repo_root = Path(__file__).resolve().parents[5]
    release_yaml = (repo_root / ".github" / "workflows" / "release.yaml").read_text(encoding="utf-8")
    assert target in release_yaml


def test_resolve_cargo_release_dir_and_multi_extension_manifests(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify CARGO_TARGET_DIR resolution, real workspace manifests, and multi-extension maturin invocations."""
    cfg = RustPGOConfig(profile_dir=tmp_path / "raw")
    assert cfg.manifest_path.is_file()
    assert len(EXTENSION_MANIFESTS) == 5
    assert all(manifest.is_file() for manifest in EXTENSION_MANIFESTS)
    assert not hasattr(packaging_pgo, "resolve_llvm_profdata")
    assert tools_rust_pgo.RustPGOConfig is RustPGOConfig
    assert tools_rust_pgo.run_pgo_pipeline is run_pgo_pipeline

    monkeypatch.delenv("CARGO_TARGET_DIR", raising=False)
    default_rel = resolve_cargo_release_dir(cfg)
    assert default_rel == cfg.manifest_path.parent.parent / "target" / "release"

    custom_target = tmp_path / "cargo-out"
    monkeypatch.setenv("CARGO_TARGET_DIR", str(custom_target))
    assert resolve_cargo_release_dir(cfg) == custom_target / "release"
    assert resolve_cargo_release_dir(cfg, env={"CARGO_TARGET_DIR": "build/custom-rs"}).name == "release"

    recorded_cmds: list[tuple[list[str], str]] = []

    def fake_run(
        cmd: list[str],
        *,
        check: bool = False,
        env: dict[str, str] | None = None,
        cwd: Path | None = None,
        capture_output: bool = False,
        text: bool = False,
    ) -> subprocess.CompletedProcess[str]:
        if cmd[0] == "llvm-profdata" or "llvm-profdata" in Path(cmd[0]).name:
            return subprocess.CompletedProcess(cmd, 0, stdout="ok", stderr="")
        rustflags = (env or {}).get("RUSTFLAGS", "")
        recorded_cmds.append((list(cmd), rustflags))
        assert check is True
        assert cwd is not None
        return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

    monkeypatch.setattr(packaging_pgo.subprocess, "run", fake_run)
    build_instrumented_extensions(cfg)
    assert len(recorded_cmds) == 5
    assert "-m" not in recorded_cmds[0][0]
    assert "--features" in recorded_cmds[0][0]
    assert "extension-module" in recorded_cmds[0][0]
    assert f"-Cprofile-generate={cfg.profile_dir}" in recorded_cmds[0][1]
    assert all("-m" in recorded_cmds[i][0] and str(EXTENSION_MANIFESTS[i]) in recorded_cmds[i][0] for i in range(1, 5))

    recorded_cmds.clear()
    valid_profdata = tmp_path / "valid.profdata"
    valid_profdata.write_bytes(b"PROFDATA")
    cfg_with_bin = RustPGOConfig(profile_dir=tmp_path / "raw", llvm_profdata_bin="llvm-profdata")
    build_optimized_extensions(cfg_with_bin, valid_profdata)
    assert len(recorded_cmds) == 5
    assert all(f"-Cprofile-use={valid_profdata}" in flags for _, flags in recorded_cmds)


def test_build_optimized_falls_back_when_profdata_invalid(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify validate_profdata rejects missing/corrupt files and build_optimized* falls back cleanly."""
    missing = tmp_path / "missing.profdata"
    assert validate_profdata(missing, llvm_profdata="/nonexistent/llvm-profdata") is False

    empty = tmp_path / "empty.profdata"
    empty.write_bytes(b"")
    assert validate_profdata(empty, llvm_profdata="/nonexistent/llvm-profdata") is False

    corrupt = tmp_path / "corrupt.profdata"
    corrupt.write_bytes(b"not-valid-llvm-profdata")
    assert validate_profdata(corrupt, llvm_profdata="/nonexistent/llvm-profdata") is False

    executed: list[tuple[list[str], dict[str, str] | None]] = []

    def fake_run(
        cmd: list[str],
        *,
        check: bool = False,
        env: dict[str, str] | None = None,
        cwd: Path | None = None,
        capture_output: bool = False,
        text: bool = False,
    ) -> subprocess.CompletedProcess[str]:
        if len(cmd) >= 2 and cmd[1] == "show":
            raise subprocess.CalledProcessError(1, cmd, stderr="corrupt profile")
        executed.append((list(cmd), env))
        return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

    monkeypatch.setattr(packaging_pgo.subprocess, "run", fake_run)
    monkeypatch.setenv("CARGO_TARGET_DIR", str(tmp_path / "target-dir"))
    cfg = RustPGOConfig(profile_dir=tmp_path / "raw", llvm_profdata_bin="llvm-profdata")

    out_dir = build_optimized(cfg, corrupt)
    assert out_dir == tmp_path / "target-dir" / "release"
    assert executed[-1][1] is None

    ext_dir = build_optimized_extension(cfg, corrupt)
    assert ext_dir == tmp_path / "target-dir" / "release"
    assert executed[-1][1] is None


def test_two_phase_pgo_pipeline_and_cli_orchestration(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify run_pgo_pipeline orchestrates generate, workload, merge, validate, use, and CLI flags."""
    raw_dir = tmp_path / "profiles" / "raw"
    merged_out = tmp_path / "profiles" / "merged.profdata"
    baseline_prof = tmp_path / "profiles" / "baseline.profdata"
    baseline_prof.parent.mkdir(parents=True, exist_ok=True)
    baseline_prof.write_bytes(b"BASELINE_PROFDATA")

    monkeypatch.setenv("CARGO_TARGET_DIR", str(tmp_path / "rs-target"))
    cfg = RustPGOConfig(profile_dir=raw_dir, llvm_profdata_bin="llvm-profdata")
    phases: list[str] = []

    def fake_run(
        cmd: list[str],
        *,
        check: bool = False,
        env: dict[str, str] | None = None,
        cwd: Path | None = None,
        capture_output: bool = False,
        text: bool = False,
    ) -> subprocess.CompletedProcess[str]:
        exe = Path(cmd[0]).name
        rustflags = (env or {}).get("RUSTFLAGS", "")
        if exe == "cargo":
            mode = (
                "gen" if "-Cprofile-generate=" in rustflags else ("use" if "-Cprofile-use=" in rustflags else "plain")
            )
            phases.append(f"cargo:{mode}")
        elif exe == "maturin":
            mode = (
                "gen" if "-Cprofile-generate=" in rustflags else ("use" if "-Cprofile-use=" in rustflags else "plain")
            )
            phases.append(f"maturin:{mode}")
        elif exe == "llvm-profdata":
            subcmd = cmd[1]
            phases.append(f"profdata:{subcmd}")
            if subcmd == "merge":
                out_idx = cmd.index("-o") + 1
                out_file = Path(cmd[out_idx])
                out_file.parent.mkdir(parents=True, exist_ok=True)
                out_file.write_bytes(b"MERGED_LLVM_PROFDATA")
        return subprocess.CompletedProcess(cmd, 0, stdout="Total functions: 12\n", stderr="")

    monkeypatch.setattr(packaging_pgo.subprocess, "run", fake_run)

    observed_profile_env: list[str] = []

    def fake_workload(active_cfg: RustPGOConfig, release_dir: Path) -> None:
        observed_profile_env.append(os.environ.get("LLVM_PROFILE_FILE", ""))
        assert release_dir == tmp_path / "rs-target" / "release"
        phases.append("workload")
        (active_cfg.profile_dir / "vtb-1001.profraw").write_bytes(b"RAW_PROFILE")

    result = run_pgo_pipeline(
        cfg,
        profdata_output=merged_out,
        baseline_profdata=baseline_prof,
        workload_runner=fake_workload,
    )
    assert isinstance(result, PGOPipelineResult)
    assert result.used_pgo is True
    assert result.profdata_valid is True
    assert result.profdata_bytes == len(b"MERGED_LLVM_PROFDATA")
    assert result.release_dir == tmp_path / "rs-target" / "release"
    assert len(result.extension_manifests) == 5
    assert observed_profile_env and observed_profile_env[0].endswith(".profraw")
    assert phases == [
        "cargo:gen",
        "maturin:gen",
        "maturin:gen",
        "maturin:gen",
        "maturin:gen",
        "maturin:gen",
        "workload",
        "profdata:merge",
        "profdata:merge",
        "profdata:show",
        "cargo:use",
        "maturin:use",
        "maturin:use",
        "maturin:use",
        "maturin:use",
        "maturin:use",
    ]

    phases.clear()
    exit_code = tools_rust_pgo.main(
        [
            "--profile-dir",
            str(raw_dir),
            "--profdata",
            str(merged_out),
            "--llvm-profdata",
            "llvm-profdata",
            "--use-existing",
        ]
    )
    assert exit_code == 0
    assert phases == [
        "profdata:show",
        "cargo:use",
        "maturin:use",
        "maturin:use",
        "maturin:use",
        "maturin:use",
        "maturin:use",
    ]


def test_live_llvm_profdata_rejects_corrupt_data_when_installed(tmp_path: Path) -> None:
    """Exercise the real host llvm-profdata binary against corrupt input when available."""
    try:
        llvm_bin = find_llvm_profdata()
    except FileNotFoundError:
        pytest.skip("llvm-profdata is not installed on this host")

    assert Path(llvm_bin).is_file()
    corrupt = tmp_path / "bad.profdata"
    corrupt.write_bytes(b"corrupt-profdata-bytes")
    assert validate_profdata(corrupt, llvm_profdata=llvm_bin) is False


def test_build_target_python_315_musl_and_version_validation() -> None:
    """Verify BuildTarget supports Python 3.15/3.15t, musl triples, and rejects unsupported versions."""
    assert SUPPORTED_PYTHON_VERSIONS == ("3.12", "3.13", "3.14", "3.14t", "3.15", "3.15t")
    assert not hasattr(OnefileBuilder, "extract_archive")

    py315 = BuildTarget.resolve("linux-x86_64-musl", python_version="3.15")
    assert py315.rust_triple == "x86_64-unknown-linux-musl"
    assert py315.libc == "musl"
    assert py315.is_freethreaded is False
    assert "cpython-3.15.0+" in py315.pbs_archive_name

    py315t = BuildTarget.resolve("linux-aarch64-musl", python_version="3.15t")
    assert py315t.rust_triple == "aarch64-unknown-linux-musl"
    assert py315t.libc == "musl"
    assert py315t.is_freethreaded is True
    assert "cpython-3.15.0+" in py315t.pbs_archive_name
    assert "freethreaded+pgo+lto" in py315t.pbs_archive_name

    with pytest.raises(ValueError, match="Unsupported Python runtime version"):
        BuildTarget.resolve("linux-x86_64", python_version="3.11")

    with pytest.raises(ValueError, match="Unsupported Python runtime version"):
        BuildTarget.detect_host(python_version="2.7")


def test_standalone_and_cli_server_binary_staging(tmp_path: Path) -> None:
    """Verify StandaloneBuilder, OnefileBuilder, and vtb build stage server binaries."""
    project_dir = tmp_path / "staged_app"
    pkg_dir = project_dir / "app"
    pkg_dir.mkdir(parents=True)
    (project_dir / "pyproject.toml").write_text('[project]\nname = "staged-app"\nversion = "0.2.0"\n')
    (pkg_dir / "__init__.py").write_text('"""Staged app package."""\n')
    (pkg_dir / "main.py").write_text(
        '"""Entrypoint verifying bundled server binary path."""\n'
        "import os\n"
        "from pathlib import Path\n\n"
        "def run() -> int:\n"
        '    srv = os.environ.get("VERTEBRA_SERVER_BINARY", "")\n'
        "    return 0 if srv and Path(srv).is_file() else 7\n"
    )

    fake_server = tmp_path / "vertebra-server"
    fake_server.write_bytes(b"#!/bin/sh\nexit 0\n")

    runner = CliRunner()
    onefile_bin = tmp_path / "dist_out" / "staged-app-onefile"
    result = runner.invoke(
        build_command,
        [
            "--project-dir",
            str(project_dir),
            "--standalone",
            "--onefile",
            "--entrypoint",
            "app.main:run",
            "--target",
            "linux-x86_64-musl",
            "--python-version",
            "3.15t",
            "--server-binary",
            str(fake_server),
            "--output",
            str(onefile_bin),
        ],
    )
    assert result.exit_code == 0, result.output
    assert onefile_bin.is_file()

    extracted = tmp_path / "extracted_onefile"
    OnefileBuilder.extract_to(onefile_bin, extracted)
    staged_server = extracted / "bin" / "vertebra-server"
    assert staged_server.is_file()
    assert os.access(staged_server, os.X_OK)

    manifest = decode_json((extracted / "manifest.json").read_bytes())
    assert "pgo_enabled" not in manifest
    assert manifest["server_binary"] == "bin/vertebra-server"
    assert manifest["target"]["python_version"] == "3.15t"
    assert manifest["target"]["rust_triple"] == "x86_64-unknown-linux-musl"

    cache_home = tmp_path / "onefile_cache"
    env = dict(os.environ)
    env["XDG_CACHE_HOME"] = str(cache_home)
    env.pop("VERTEBRA_SERVER_BINARY", None)
    proc = subprocess.run([str(onefile_bin)], env=env, capture_output=True, text=True, check=False)
    assert proc.returncode == 0, proc.stderr

    bad_ver = runner.invoke(
        build_command,
        [
            "--project-dir",
            str(project_dir),
            "--target",
            "backend",
            "--python-version",
            "3.11",
        ],
    )
    assert bad_ver.exit_code != 0
    assert "Unsupported Python runtime version" in bad_ver.output


def test_pyproject_sqlalchemy_dialect_entry_points_set_is_complete() -> None:
    """Verify pyproject.toml declares the exact set of public SQLAlchemy dialect entry points."""
    repo_root = Path(__file__).resolve().parents[5]
    pyproject_data = tomllib.loads((repo_root / "pyproject.toml").read_text(encoding="utf-8"))
    entry_points: dict[str, str] = pyproject_data["project"]["entry-points"]["sqlalchemy.dialects"]
    expected_keys = {
        f"{engine}.{suffix}"
        for engine in ("postgresql", "oracle", "mysql", "sqlite", "mssql", "duckdb", "bigquery", "spanner", "spangres")
        for suffix in ("vertebra", "vtb", "vertebra_async", "vtb_async")
    }
    assert set(entry_points.keys()) == expected_keys


@pytest.mark.parametrize(
    "engine",
    ["postgresql", "oracle", "mysql", "sqlite", "mssql", "duckdb", "bigquery", "spanner", "spangres"],
)
@pytest.mark.parametrize("suffix", ["vertebra", "vtb", "vertebra_async", "vtb_async"])
def test_pyproject_sqlalchemy_dialect_entry_points_resolve(engine: str, suffix: str) -> None:
    """Verify each public SQLAlchemy dialect alias resolves to a Dialect subclass."""
    repo_root = Path(__file__).resolve().parents[5]
    pyproject_data = tomllib.loads((repo_root / "pyproject.toml").read_text(encoding="utf-8"))
    entry_points: dict[str, str] = pyproject_data["project"]["entry-points"]["sqlalchemy.dialects"]
    ep_name = f"{engine}.{suffix}"
    target_spec = entry_points[ep_name]
    mod_name, _, cls_name = target_spec.partition(":")
    module = ensure_module(mod_name)
    resolved_cls = getattr(module, cls_name)
    assert issubclass(resolved_cls, Dialect), ep_name
    assert "Vtb" not in cls_name


@pytest.mark.parametrize(
    ("pkg_slug", "expected_module"),
    [
        ("bigquery", "vertebra._bigquery"),
        ("bundler", "vertebra._bundler"),
        ("spanner", "vertebra._spanner"),
        ("storage", "vertebra._storage"),
    ],
)
def test_optional_extension_pyproject_isolation_and_bundler_extra(
    pkg_slug: str,
    expected_module: str,
) -> None:
    """Verify optional extension manifests isolate src/py/vertebra and sync with root pyproject.toml."""
    repo_root = Path(__file__).resolve().parents[5]
    ext_toml = tomllib.loads(
        (repo_root / "src" / "rs" / f"vertebra-{pkg_slug}" / "pyproject.toml").read_text(encoding="utf-8")
    )
    project = ext_toml["project"]
    assert project["name"] == f"vertebra-{pkg_slug}"
    assert project["description"]
    assert project["license"] == {"text": "MIT"}
    maturin_cfg = ext_toml["tool"]["maturin"]
    assert maturin_cfg["python-source"] == "../../py"
    assert maturin_cfg["module-name"] == expected_module
    other_stubs = [
        f"**/py/vertebra/_{other}.pyi"
        for other in ("bigquery", "bundler", "spanner", "storage", "vertebra")
        if other != pkg_slug
    ]
    assert maturin_cfg["exclude"] == [
        "**/py/vertebra/**/*.py",
        "**/py/vertebra/**/*.so",
        "**/py/vertebra/py.typed",
        *other_stubs,
    ]
    assert all(not pattern.startswith("!") for pattern in maturin_cfg["exclude"])

    root_toml = tomllib.loads((repo_root / "pyproject.toml").read_text(encoding="utf-8"))
    version = root_toml["project"]["version"]
    opt_deps = root_toml["project"]["optional-dependencies"]
    pinned = f"vertebra-{pkg_slug}=={version}"
    assert opt_deps[pkg_slug] == [pinned]
    assert pinned in opt_deps["all"]
    assert f"vertebra-{pkg_slug}" in root_toml["tool"]["uv"]["sources"]


def test_run_default_pgo_workload_executes_extensions_routes_and_fast_fail(tmp_path: Path) -> None:
    """Verify run_default_pgo_workload runs real PyO3 extension probe, generated routes, VERTEBRA_APP, and fast exit failure."""
    profile_dir = tmp_path / "raw_profiles"
    release_dir = tmp_path / "release_bin"
    release_dir.mkdir(parents=True)
    cfg = RustPGOConfig(profile_dir=profile_dir)

    server_script = release_dir / "vertebra-server"
    marker_path = tmp_path / "server_training_report.json"
    shebang = chr(35) + f"!{sys.executable}\n"
    server_script.write_text(
        f"{shebang}"
        '"""Synthetic vertebra-server verifying generated pgo_train_app and serving loopback HTTP."""\n'
        "from http.server import BaseHTTPRequestHandler, HTTPServer\n"
        "import importlib\n"
        "import os\n"
        "from pathlib import Path\n"
        "import signal\n"
        "import sys\n"
        "from vertebra._vertebra import Request\n"
        "from vertebra.utils.serializers import format_json\n\n"
        'app_spec = os.environ.get("VERTEBRA_APP", "")\n'
        'mod_name, _, attr_name = app_spec.partition(":")\n'
        "if not mod_name or not attr_name:\n"
        '    sys.stderr.write("missing VERTEBRA_APP\\n")\n'
        "    sys.exit(3)\n"
        "mod = importlib.import_module(mod_name)\n"
        "train_app = getattr(mod, attr_name)\n"
        "route_map = {r.path: r.handler for r in train_app.routes}\n"
        'r_index = route_map["/"](Request.from_parts("GET", "/"))\n'
        'r_json = route_map["/json"](Request.from_parts("GET", "/json"))\n'
        'r_echo = route_map["/echo"](Request.from_parts("POST", "/echo", b"echo-bytes"))\n'
        "counts = {'GET:/': 0, 'GET:/json': 0, 'POST:/echo': 0}\n\n"
        "class Handler(BaseHTTPRequestHandler):\n"
        "    def log_message(self, fmt: str, *args: object) -> None:\n"
        "        return\n\n"
        "    def do_GET(self) -> None:\n"
        "        key = f'GET:{self.path}'\n"
        "        counts[key] = counts.get(key, 0) + 1\n"
        "        self.send_response(200)\n"
        "        self.send_header('Content-Length', '2')\n"
        "        self.end_headers()\n"
        "        self.wfile.write(b'ok')\n\n"
        "    def do_POST(self) -> None:\n"
        "        length = int(self.headers.get('Content-Length', '0'))\n"
        "        data = self.rfile.read(length)\n"
        "        key = f'POST:{self.path}'\n"
        "        counts[key] = counts.get(key, 0) + 1\n"
        "        self.send_response(200)\n"
        "        self.send_header('Content-Length', str(len(data)))\n"
        "        self.end_headers()\n"
        "        self.wfile.write(data)\n\n"
        'host = os.environ["VERTEBRA_HOST"]\n'
        'port = int(os.environ["VERTEBRA_PORT"])\n'
        "httpd = HTTPServer((host, port), Handler)\n\n"
        "def _on_term(_signum: int, _frame: object) -> None:\n"
        "    report = {\n"
        '        "vertebra_app": app_spec,\n'
        '        "llvm_profile_file": os.environ.get("LLVM_PROFILE_FILE", ""),\n'
        '        "index_status": r_index.status,\n'
        '        "index_body": r_index.body.decode("utf-8"),\n'
        '        "json_payload": r_json,\n'
        '        "echo_body": r_echo.body.decode("utf-8"),\n'
        '        "counts": counts,\n'
        "    }\n"
        f"    Path({str(marker_path)!r}).write_text(format_json(report), encoding='utf-8')\n"
        "    sys.exit(0)\n\n"
        "signal.signal(signal.SIGTERM, _on_term)\n"
        "httpd.serve_forever()\n",
        encoding="utf-8",
    )
    server_script.chmod(0o755)

    run_default_pgo_workload(cfg, release_dir, requests_per_route=3)

    assert marker_path.is_file()
    report = decode_json(marker_path.read_bytes())
    assert report["vertebra_app"] == "pgo_train_app:app"
    assert report["llvm_profile_file"].endswith(".profraw")
    assert report["index_status"] == 200
    assert report["index_body"] == "ok"
    assert report["json_payload"] == {"status": "ok", "runtime": "vertebra"}
    assert report["echo_body"] == "echo-bytes"
    assert report["counts"]["GET:/"] >= 4
    assert report["counts"]["GET:/json"] == 3
    assert report["counts"]["POST:/echo"] == 3

    crashing_dir = tmp_path / "crashing_release"
    crashing_dir.mkdir(parents=True)
    crashing_server = crashing_dir / "vertebra-server"
    crashing_server.write_text(
        f"{shebang}"
        '"""Immediate crash stub."""\n'
        "import sys\n"
        'sys.stderr.write("fatal startup failure\\n")\n'
        "sys.exit(2)\n",
        encoding="utf-8",
    )
    crashing_server.chmod(0o755)

    with pytest.raises(RuntimeError, match="exited prematurely with code 2: fatal startup failure"):
        run_default_pgo_workload(cfg, crashing_dir, requests_per_route=1)


@pytest.mark.parametrize(
    ("ep_spec", "expected_type"),
    [
        ("app:app", "App"),
        ("app:asgi_app", "function"),
        ("app:wsgi_app", "function"),
    ],
)
def test_standalone_and_onefile_launch_bundled_server_for_app_asgi_and_wsgi(
    tmp_path: Path,
    ep_spec: str,
    expected_type: str,
) -> None:
    """Verify packaged standalone and onefile bundles launch bin/vertebra-server for App, ASGI, and WSGI targets."""
    project_dir = tmp_path / "web_project"
    project_dir.mkdir(parents=True)
    (project_dir / "pyproject.toml").write_text('[project]\nname = "web-project"\nversion = "0.1.0"\n')
    (project_dir / "app.py").write_text(
        '"""Sample Vertebra, ASGI, and WSGI entrypoints."""\n'
        "from typing import Any\n"
        "from vertebra import App\n\n"
        "app = App()\n\n"
        '@app.get("/health")\n'
        "def health() -> str:\n"
        '    return "healthy"\n\n'
        "async def asgi_app(scope: dict[str, Any], receive: Any, send: Any) -> None:\n"
        "    _ = (scope, receive, send)\n\n"
        "def wsgi_app(environ: dict[str, Any], start_response: Any) -> list[bytes]:\n"
        "    _ = (environ, start_response)\n"
        '    return [b"ok"]\n',
        encoding="utf-8",
    )

    marker_file = tmp_path / "launched_server.json"
    shebang = chr(35) + f"!{sys.executable}\n"
    fake_server = tmp_path / "vertebra-server"
    fake_server.write_text(
        f"{shebang}"
        '"""Fake vertebra-server recording VERTEBRA_APP and verifying PYTHONPATH import."""\n'
        "import importlib\n"
        "import os\n"
        "from pathlib import Path\n"
        "import sys\n"
        "from vertebra.utils.serializers import format_json\n\n"
        'spec = os.environ.get("VERTEBRA_APP", "")\n'
        'mod_name, _, attr_name = spec.partition(":")\n'
        "mod = importlib.import_module(mod_name)\n"
        "loaded = getattr(mod, attr_name)\n"
        "payload = {\n"
        '    "vertebra_app": spec,\n'
        '    "type_name": type(loaded).__name__,\n'
        '    "argv": sys.argv[1:],\n'
        "}\n"
        f"Path({str(marker_file)!r}).write_text(format_json(payload), encoding='utf-8')\n"
        "sys.exit(0)\n",
        encoding="utf-8",
    )
    fake_server.chmod(0o755)

    repo_py = str(Path(__file__).resolve().parents[3])
    runner = CliRunner()
    onefile_bin = tmp_path / "web-onefile"
    res = runner.invoke(
        build_command,
        [
            "--project-dir",
            str(project_dir),
            "--standalone",
            "--onefile",
            "--entrypoint",
            ep_spec,
            "--target",
            "linux-x86_64",
            "--python-version",
            "3.15",
            "--server-binary",
            str(fake_server),
            "--output",
            str(onefile_bin),
        ],
    )
    assert res.exit_code == 0, res.output

    env = dict(os.environ)
    env["XDG_CACHE_HOME"] = str(tmp_path / "cache")
    env["PYTHONPATH"] = repo_py
    env.pop("VERTEBRA_SERVER_BINARY", None)
    env.pop("VERTEBRA_APP", None)
    proc = subprocess.run([str(onefile_bin)], env=env, capture_output=True, text=True, check=False)
    assert proc.returncode == 0, proc.stderr
    assert marker_file.is_file()
    recorded = decode_json(marker_file.read_bytes())
    assert recorded["vertebra_app"] == ep_spec
    assert recorded["type_name"] == expected_type


def test_standalone_missing_server_binary_reports_error(tmp_path: Path) -> None:
    """Verify StandaloneBuilder __main__.py exits non-zero when vertebra-server binary is missing."""
    project_dir = tmp_path / "web_project"
    project_dir.mkdir(parents=True)
    (project_dir / "pyproject.toml").write_text('[project]\nname = "web-project"\nversion = "0.1.0"\n')
    (project_dir / "app.py").write_text(
        '"""Sample Vertebra app."""\nfrom vertebra import App\napp = App()\n',
        encoding="utf-8",
    )
    repo_py = str(Path(__file__).resolve().parents[3])
    no_bin_dist = tmp_path / "dist_no_bin"
    target = BuildTarget.resolve("linux-x86_64", python_version="3.15")
    StandaloneBuilder(project_dir=project_dir, target=target, entrypoint="app:app").build(no_bin_dist)
    missing_env = dict(os.environ)
    missing_env["PYTHONPATH"] = repo_py
    missing_env["VERTEBRA_SERVER_BINARY"] = str(tmp_path / "does-not-exist-vertebra-server")
    missing_proc = subprocess.run(
        [sys.executable, str(no_bin_dist / "__main__.py")],
        env=missing_env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert missing_proc.returncode == 1
    assert "vertebra-server binary not found" in missing_proc.stderr


_DEPENDENCY_CALLEES: frozenset[str] = frozenset({"ensure_module", "MissingDependencyError"})
_BRACKETED_DISTRIBUTION = re.compile(r"^(?P<dist>[A-Za-z0-9_.-]+)\[[^\]]+\]$")


def test_optional_dependency_literals_name_declared_extras() -> None:
    """Every optional-dependency literal under src/py/vertebra names a declared extra and a real package."""
    repo_root = Path(__file__).resolve().parents[5]
    pyproject_data = tomllib.loads((repo_root / "pyproject.toml").read_text(encoding="utf-8"))
    declared = frozenset(pyproject_data["project"]["optional-dependencies"])
    violations = _optional_dependency_violations(
        repo_root / "src" / "py" / "vertebra",
        declared,
        relative_to=repo_root,
    )
    assert violations == []


def test_optional_dependency_gate_reports_each_malformed_literal(tmp_path: Path) -> None:
    """The gate flags undeclared extras, sentence packages, non-vertebra-core distributions, and bare extensions."""
    mod_file = tmp_path / "pkg" / "mod.py"
    mod_file.parent.mkdir(parents=True, exist_ok=True)
    mod_file.write_text(
        "\n".join(
            [
                'ensure_module("wasmtime", extra="wasm")',
                'module_loader.ensure_module("numpy", package="numpy is required", extra="arrow")',
                'MissingDependencyError("litestar-mcp", install_package="vertebra[mcp]", extra="mcp")',
                'MissingDependencyError("x", extra="sqlspec")',
                'ensure_module("y", extra=extra_name, install_package="psycopg[binary,pool]")',
                'print("z", extra="wasm")',
                'MissingDependencyError("pandas is required")',
                'MissingDependencyError("redis", install_package="redis is required")',
                'ensure_module("vertebra.agent.tools")',
                'ensure_module("vertebra._bundler", package="vertebra-core[bundler]", extra="bundler")',
                'MissingDependencyError("vertebra._storage", install_package="vertebra-core[storage]", extra="storage")',
                'MissingDependencyError("vertebra._vertebra.ArrowCursor", install_package="vertebra-core")',
                'ensure_module("pandas is required")',
                'ensure_module("vtb[mcp]")',
                'ensure_module("vertebra._bundler")',
                'MissingDependencyError("vertebra._storage")',
                'ensure_module(module_name="vertebra._bundler")',
                'ensure_module("vertebra._bundler", package=None)',
                'MissingDependencyError("vertebra._storage", install_package=None)',
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    violations = _optional_dependency_violations(
        tmp_path,
        frozenset({"arrow", "bundler", "mcp", "sqlspec", "storage"}),
        relative_to=tmp_path,
    )
    assert violations == [
        "pkg/mod.py:13::ensure_module module_name='pandas is required'",
        "pkg/mod.py:14::ensure_module module_name='vtb[mcp]'",
        "pkg/mod.py:15::ensure_module module_name='vertebra._bundler'",
        "pkg/mod.py:16::MissingDependencyError package='vertebra._storage'",
        "pkg/mod.py:17::ensure_module module_name='vertebra._bundler'",
        "pkg/mod.py:18::ensure_module module_name='vertebra._bundler'",
        "pkg/mod.py:19::MissingDependencyError package='vertebra._storage'",
        "pkg/mod.py:1::ensure_module extra='wasm'",
        "pkg/mod.py:2::ensure_module package='numpy is required'",
        "pkg/mod.py:3::MissingDependencyError install_package='vertebra[mcp]'",
        "pkg/mod.py:7::MissingDependencyError package='pandas is required'",
        "pkg/mod.py:8::MissingDependencyError install_package='redis is required'",
    ]


def _optional_dependency_violations(
    source_root: Path,
    declared: frozenset[str],
    *,
    relative_to: Path,
) -> list[str]:
    """Return malformed optional-dependency literals under ``source_root``.

    Args:
        source_root: Directory scanned recursively for ``*.py`` files.
        declared: Keys of ``[project.optional-dependencies]``.
        relative_to: Base directory for the reported paths.

    Returns:
        Sorted ``"<path>:<line>::<callee> <keyword>=<repr(value)>"`` entries, empty when every literal is valid.
    """
    entries: list[str] = []
    for path in sorted(source_root.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        rel_path = path.relative_to(relative_to).as_posix()
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                entries.extend(_call_dependency_violations(node, rel_path, declared))
    return sorted(entries)


def _is_non_none_kw(kw: ast.keyword, names: frozenset[str]) -> bool:
    """Return True when ``kw`` names an override in ``names`` whose value is not a literal ``None``."""
    return kw.arg in names and not (isinstance(kw.value, ast.Constant) and kw.value.value is None)


def _call_dependency_violations(
    node: ast.Call,
    rel_path: str,
    declared: frozenset[str],
) -> list[str]:
    """Check one ast.Call node for optional-dependency literal violations."""
    callee = _callee_name(node.func)
    if callee not in _DEPENDENCY_CALLEES:
        return []
    violations: list[str] = []
    has_install_override = any(_is_non_none_kw(kw, frozenset({"install_package", "extra"})) for kw in node.keywords)
    has_hint_override = has_install_override or any(_is_non_none_kw(kw, frozenset({"package"})) for kw in node.keywords)
    if node.args and isinstance(node.args[0], ast.Constant) and isinstance(node.args[0].value, str):
        pos_val = node.args[0].value
        if callee == "MissingDependencyError" and (
            _is_invalid_package_literal(pos_val) or (pos_val.startswith("vertebra._") and not has_install_override)
        ):
            violations.append(f"{rel_path}:{node.lineno}::{callee} package={pos_val!r}")
        elif (
            callee == "ensure_module"
            and not has_hint_override
            and (_is_invalid_package_literal(pos_val) or pos_val.startswith("vertebra._"))
        ):
            violations.append(f"{rel_path}:{node.lineno}::{callee} module_name={pos_val!r}")
    for kw in node.keywords:
        if kw.arg is None or not isinstance(kw.value, ast.Constant) or not isinstance(kw.value.value, str):
            continue
        val = kw.value.value
        if kw.arg == "extra" and val not in declared:
            violations.append(f"{rel_path}:{node.lineno}::{callee} extra={val!r}")
        elif kw.arg == "install_package" and (_is_invalid_package_literal(val) or val.startswith("vertebra._")):
            violations.append(f"{rel_path}:{node.lineno}::{callee} install_package={val!r}")
        elif kw.arg == "package" and (
            _is_invalid_package_literal(val) or (val.startswith("vertebra._") and not has_install_override)
        ):
            violations.append(f"{rel_path}:{node.lineno}::{callee} package={val!r}")
        elif (
            callee == "ensure_module"
            and kw.arg == "module_name"
            and not has_hint_override
            and (_is_invalid_package_literal(val) or val.startswith("vertebra._"))
        ):
            violations.append(f"{rel_path}:{node.lineno}::{callee} module_name={val!r}")
    return violations


def _is_invalid_package_literal(value: str) -> bool:
    """Return True if a package or install_package literal contains whitespace or a non-vertebra-core bracketed dist."""
    if any(ch.isspace() for ch in value):
        return True
    match = _BRACKETED_DISTRIBUTION.match(value)
    if match is None:
        return False
    dist = match.group("dist")
    return (dist == "vtb" or dist.startswith("vertebra")) and dist != "vertebra-core"


def _callee_name(func: ast.expr) -> str | None:
    """Return the trailing name of a call target (``ast.Name.id`` or ``ast.Attribute.attr``)."""
    if isinstance(func, ast.Name):
        return func.id
    if isinstance(func, ast.Attribute):
        return func.attr
    return None
