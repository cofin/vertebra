"""Unit tests for vertebra.build (targets, standalone, onefile), CLI flags, and tools/pgo."""

import ast
import json
import os
import re
import subprocess
import sys
import tomllib
import types
import zipfile
import zlib
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest
import yaml
from click.testing import CliRunner
from sqlalchemy.engine.interfaces import Dialect
from tools.pgo import python_pgo as tools_python_pgo
from tools.pgo import rust_pgo as tools_rust_pgo
from tools.pgo.python_pgo import (
    CPYTHON_PGO_FLAGS,
    PLATFORM_MATRIX,
    PythonBuildInfo,
    check_pgo_availability,
    detect_python_build,
    get_platform_support,
)
from tools.pgo.rust_pgo import (
    EXTENSION_MANIFESTS,
    PGOPipelineResult,
    RustPGOConfig,
    build_flags,
    build_instrumented_extensions,
    build_optimized,
    build_optimized_extension,
    build_optimized_extensions,
    check_profdata_size,
    discover_extension_manifests,
    extension_module_name,
    find_llvm_profdata,
    merge_profiles,
    resolve_cargo_release_dir,
    run_default_pgo_workload,
    run_pgo_pipeline,
    validate_profdata,
)
from tools.scripts import build_ci_wheels as build_ci_wheels_mod
from tools.scripts import build_server_binary as build_server_binary_mod
from tools.scripts import check_extras as check_extras_mod
from tools.scripts import smoke_vtb_run as smoke_vtb_run_mod
from tools.scripts.build_server_binary import ServerBinaryBuilder, ServerBinaryTarget
from tools.scripts.smoke_vtb_run import SmokeScenario, VtbRunSmoke

import vertebra.build.packaging as packaging_mod
from vertebra.agent.mcp.skills import FilesystemSkillSource, MCPSkillsConfig, SkillCatalog
from vertebra.build.packaging import (
    ONEFILE_MAGIC,
    ONEFILE_TRAILER_SIZE,
    SUPPORTED_PYTHON_VERSIONS,
    BuildTarget,
    OnefileBuilder,
    StandaloneBuilder,
)
from vertebra.cli import cli
from vertebra.cli.build import build as build_command
from vertebra.typing import MissingDependencyError
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

    sample_pyc = staged / "app" / "app" / "main.pyc"
    pyc_flags = int.from_bytes(sample_pyc.read_bytes()[4:8], "little")
    assert pyc_flags == 0b11

    os.utime(pkg_dir / "main.py", (1_000_000_000, 1_000_000_000))
    rebuilt = StandaloneBuilder(project_dir=project_dir, target=target, entrypoint="app.main:run").build(
        tmp_path / "dist" / "standalone-rebuild"
    )
    rebuilt_binary = OnefileBuilder(dist_dir=rebuilt, target=target).build(tmp_path / "dist" / "sample-app-bin-2")
    assert (rebuilt / "app" / "app" / "main.pyc").read_bytes() == sample_pyc.read_bytes()
    assert rebuilt_binary.read_bytes() == raw_bytes


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


def test_python_build_info_detects_315_tail_call_jit_freethreaded_and_frame_pointers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify PythonBuildInfo detects Python 3.15 tail-call interpreter, JIT, free-threading, and frame pointers."""
    mod = tools_python_pgo
    synthetic_vars = {
        "CONFIG_ARGS": "'--enable-optimizations' '--with-lto' '--with-tail-call-interp' '--enable-experimental-jit'",
        "Py_TAIL_CALL_INTERP": "1",
        "Py_GIL_DISABLED": "1",
        "CFLAGS_NODIST": "-fno-omit-frame-pointer -mno-omit-leaf-frame-pointer",
    }
    fake_jit = SimpleNamespace(is_available=lambda: True, is_enabled=lambda: True)

    monkeypatch.setattr(mod, "_query_sysconfig", lambda _exe: dict(synthetic_vars))
    monkeypatch.setattr(mod, "_query_version", lambda _exe: "3.15.0rc3")
    monkeypatch.setattr("tools.pgo.python_pgo.sys._jit", fake_jit, raising=False)
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
    """Verify CARGO_TARGET_DIR resolution, discovered workspace manifests, and multi-extension maturin invocations."""
    cfg = RustPGOConfig(profile_dir=tmp_path / "raw")
    assert cfg.manifest_path.is_file()
    repo_root = Path(__file__).resolve().parents[5]
    rs_root = repo_root / "src" / "rs"
    expected_optional = sorted(
        p.parent / "Cargo.toml"
        for p in rs_root.glob("vertebra-*/pyproject.toml")
        if (p.parent / "Cargo.toml").is_file()
    )
    assert expected_optional
    assert (rs_root / "vertebra-py" / "Cargo.toml", *expected_optional) == EXTENSION_MANIFESTS
    assert discover_extension_manifests(rs_root) == EXTENSION_MANIFESTS
    assert all(manifest.is_file() for manifest in EXTENSION_MANIFESTS)
    assert extension_module_name(rs_root / "vertebra-py" / "Cargo.toml") == "vertebra._vertebra"
    assert extension_module_name(rs_root / "vertebra-storage" / "Cargo.toml") == "vertebra._storage"
    assert not (repo_root / "src" / "py" / "vertebra" / "build" / "packaging" / "pgo.py").exists()
    extension_count = len(EXTENSION_MANIFESTS)

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

    monkeypatch.setattr("tools.pgo.rust_pgo.subprocess.run", fake_run)
    build_instrumented_extensions(cfg)
    assert len(recorded_cmds) == extension_count
    assert "-m" not in recorded_cmds[0][0]
    assert "--features" in recorded_cmds[0][0]
    assert "extension-module" in recorded_cmds[0][0]
    assert f"-Cprofile-generate={cfg.profile_dir}" in recorded_cmds[0][1]
    assert all(
        "-m" in recorded_cmds[i][0] and str(EXTENSION_MANIFESTS[i]) in recorded_cmds[i][0]
        for i in range(1, extension_count)
    )

    recorded_cmds.clear()
    valid_profdata = tmp_path / "valid.profdata"
    valid_profdata.write_bytes(b"PROFDATA")
    cfg_with_bin = RustPGOConfig(profile_dir=tmp_path / "raw", llvm_profdata_bin="llvm-profdata")
    build_optimized_extensions(cfg_with_bin, valid_profdata)
    assert len(recorded_cmds) == extension_count
    assert all(f"-Cprofile-use={valid_profdata}" in flags for _, flags in recorded_cmds)


@pytest.mark.parametrize(
    "name",
    ["pgo", "RustPGOConfig", "run_pgo_pipeline", "EXTENSION_MANIFESTS", "PythonBuildInfo"],
    ids=["pgo", "RustPGOConfig", "run_pgo_pipeline", "EXTENSION_MANIFESTS", "PythonBuildInfo"],
)
def test_packaging_module_excludes_pgo_symbol(name: str) -> None:
    """Verify PGO symbols remain excluded from vertebra.build.packaging and its __all__."""
    assert not hasattr(packaging_mod, name)
    assert name not in packaging_mod.__all__


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

    monkeypatch.setattr("tools.pgo.rust_pgo.subprocess.run", fake_run)
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

    monkeypatch.setattr("tools.pgo.rust_pgo.subprocess.run", fake_run)

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
    extension_count = len(EXTENSION_MANIFESTS)
    assert isinstance(result, PGOPipelineResult)
    assert result.used_pgo is True
    assert result.profdata_valid is True
    assert result.profdata_bytes == len(b"MERGED_LLVM_PROFDATA")
    assert result.release_dir == tmp_path / "rs-target" / "release"
    assert result.extension_manifests == EXTENSION_MANIFESTS
    assert observed_profile_env and observed_profile_env[0].endswith(".profraw")
    assert phases == [
        "cargo:gen",
        *["maturin:gen"] * extension_count,
        "workload",
        "profdata:merge",
        "profdata:merge",
        "profdata:show",
        "cargo:use",
        *["maturin:use"] * extension_count,
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
    assert phases == ["profdata:show", "cargo:use", *["maturin:use"] * extension_count]


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
        ("duckdb", "vertebra._duckdb"),
        ("mssql", "vertebra._mssql"),
        ("mysql", "vertebra._mysql"),
        ("oracle", "vertebra._oracle"),
        ("postgres", "vertebra._postgres"),
        ("redis", "vertebra._redis"),
        ("spanner", "vertebra._spanner"),
        ("sqlite", "vertebra._sqlite"),
        ("storage", "vertebra._storage"),
        ("valkey", "vertebra._valkey"),
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
        for other in (
            "bigquery",
            "bundler",
            "duckdb",
            "mssql",
            "mysql",
            "oracle",
            "postgres",
            "redis",
            "spanner",
            "sqlite",
            "storage",
            "valkey",
            "vertebra",
        )
        if other != pkg_slug
    ]
    expected_prefix = [
        "**/py/vertebra/**/*.py",
        "**/py/vertebra/**/*.so",
        "**/py/vertebra/py.typed",
    ]
    assert maturin_cfg["exclude"] == [*expected_prefix, *other_stubs]
    assert all(not pattern.startswith("!") for pattern in maturin_cfg["exclude"])

    root_toml = tomllib.loads((repo_root / "pyproject.toml").read_text(encoding="utf-8"))
    version = root_toml["project"]["version"]
    opt_deps = root_toml["project"]["optional-dependencies"]
    pinned = f"vertebra-{pkg_slug}=={version}"
    assert opt_deps[pkg_slug] == [pinned]
    assert "all" not in opt_deps
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


def test_build_rs_and_maturin_wheel_include_for_relocatable_server() -> None:
    """Verify build.rs omits host rpath and pyproject.toml includes packaged server binaries in wheels."""
    repo_root = Path(__file__).resolve().parents[5]
    build_rs = (repo_root / "src" / "rs" / "vertebra-http" / "build.rs").read_text(encoding="utf-8")
    assert "-Wl,-rpath" not in build_rs
    assert "pyo3_build_config::use_pyo3_cfgs()" in build_rs

    pyproject = tomllib.loads((repo_root / "pyproject.toml").read_text(encoding="utf-8"))
    maturin_include = pyproject["tool"]["maturin"]["include"]
    assert {"path": "src/py/vertebra/bin/*", "format": "wheel"} in maturin_include


def test_server_binary_builder_targets_python_fallback_macos_and_repair(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify ServerBinaryTarget, shared-Python fallback, cargo/otool/repair commands, and staging."""
    mac_target = ServerBinaryTarget.from_host(
        python_version="3.14t",
        target_platform="darwin",
        archflags="-arch arm64 -arch x86_64",
    )
    assert mac_target.free_threaded is True
    assert mac_target.rust_target == "aarch64-apple-darwin"
    assert mac_target.libpython_dylib == "libpython3.14t.dylib"
    assert mac_target.delocate_archs == "arm64,x86_64"

    linux_target = ServerBinaryTarget.from_host(
        python_version="3.12",
        free_threaded=False,
        target_platform="linux",
        archflags="",
    )
    assert linux_target.libpython_soname == "libpython3.12.so.1.0"
    assert linux_target.binary_name == "vertebra-server"

    win_target = ServerBinaryTarget.from_host(
        python_version="3.14t",
        target_platform="windows",
        archflags="",
    )
    assert win_target.binary_name == "vertebra-server.exe"
    assert win_target.libpython_soname == "python314t.dll"

    builder = ServerBinaryBuilder(repo_root=tmp_path, target=linux_target)
    cargo_cmd = builder.cargo_command()
    assert "--bin" in cargo_cmd and "vertebra-server" in cargo_cmd
    assert "--release" in cargo_cmd
    assert "vertebra-http/python,vertebra-http/tls,vertebra-http/compression,vertebra-http/http3" in cargo_cmd

    assert builder.repair_command(tmp_path / "vertebra.whl", tmp_path / "repaired") == [
        "auditwheel",
        "repair",
        "-w",
        str(tmp_path / "repaired"),
        "--exclude",
        "libpython3.12.so.1.0",
        str(tmp_path / "vertebra.whl"),
    ]
    mac_builder = ServerBinaryBuilder(repo_root=tmp_path, target=mac_target)
    assert mac_builder.repair_command(tmp_path / "vertebra.whl", tmp_path / "repaired") == [
        "delocate-wheel",
        "--require-archs",
        "arm64,x86_64",
        "-w",
        str(tmp_path / "repaired"),
        "-v",
        "--exclude",
        "libpython",
        "--ignore-missing-dependencies",
        str(tmp_path / "vertebra.whl"),
    ]

    valid_server_otool = (
        "vertebra/bin/vertebra-server:\n"
        "\t/System/Library/Frameworks/CoreFoundation.framework/Versions/A/CoreFoundation (compatibility version 150.0.0)\n"
        "\t@rpath/libpython3.14t.dylib (compatibility version 3.14.0)\n"
        "\t/usr/lib/libSystem.B.dylib (compatibility version 1.0.0)\n"
    )
    assert mac_builder.verify_macos_otool_dependencies("vertebra/bin/vertebra-server", valid_server_otool) == []

    valid_ext_otool = (
        "vertebra/_vertebra.cpython-314t-darwin.so:\n"
        "\t@rpath/lib_vertebra.dylib (compatibility version 0.0.0)\n"
        "\t@loader_path/.dylibs/libfoo.dylib (compatibility version 1.0.0)\n"
        "\t/usr/lib/libSystem.B.dylib (compatibility version 1.0.0)\n"
    )
    valid_ext_id = "vertebra/_vertebra.cpython-314t-darwin.so:\n\t@rpath/lib_vertebra.dylib\n"
    assert (
        mac_builder.verify_macos_otool_dependencies(
            "vertebra/_vertebra.cpython-314t-darwin.so",
            valid_ext_otool,
            id_output=valid_ext_id,
        )
        == []
    )

    bad_homebrew_otool = (
        "vertebra/bin/vertebra-server:\n"
        "\t@rpath/libpython3.14t.dylib (compatibility version 3.14.0)\n"
        "\t/opt/homebrew/opt/openssl@3/lib/libssl.3.dylib (compatibility version 3.0.0)\n"
    )
    with pytest.raises(RuntimeError, match=r"libssl\.3\.dylib"):
        mac_builder.verify_macos_otool_dependencies("vertebra/bin/vertebra-server", bad_homebrew_otool)

    stale_framework_otool = (
        "vertebra/bin/vertebra-server:\n"
        "\t/Library/Frameworks/PythonT.framework/Versions/3.14/PythonT (compatibility version 3.14.0)\n"
    )
    with pytest.raises(RuntimeError, match="PythonT"):
        mac_builder.verify_macos_otool_dependencies("vertebra/bin/vertebra-server", stale_framework_otool)

    otool_sample = (
        f"{tmp_path / 'vertebra-server'}:\n"
        "\t/Library/Frameworks/Python.framework/Versions/3.14/lib/libpython3.14t.dylib (compatibility version 3.14.0)\n"
        "\t/usr/lib/libSystem.B.dylib (compatibility version 1.0.0)\n"
    )
    mac_cmds = mac_builder.macos_install_name_changes(tmp_path / "vertebra-server", otool_output=otool_sample)
    assert mac_cmds == [
        [
            "install_name_tool",
            "-change",
            "/Library/Frameworks/Python.framework/Versions/3.14/lib/libpython3.14t.dylib",
            "@rpath/libpython3.14t.dylib",
            str(tmp_path / "vertebra-server"),
        ],
        ["codesign", "--force", "--sign", "-", str(tmp_path / "vertebra-server")],
    ]

    framework_otool_sample = (
        f"{tmp_path / 'vertebra-server'}:\n"
        "\t/Library/Frameworks/PythonT.framework/Versions/3.14/PythonT (compatibility version 3.14.0)\n"
        "\t/usr/lib/libSystem.B.dylib (compatibility version 1.0.0)\n"
    )
    framework_cmds = mac_builder.macos_install_name_changes(
        tmp_path / "vertebra-server",
        otool_output=framework_otool_sample,
    )
    assert framework_cmds == [
        [
            "install_name_tool",
            "-change",
            "/Library/Frameworks/PythonT.framework/Versions/3.14/PythonT",
            "@rpath/libpython3.14t.dylib",
            str(tmp_path / "vertebra-server"),
        ],
        ["codesign", "--force", "--sign", "-", str(tmp_path / "vertebra-server")],
    ]

    static_py = tmp_path / "static-python"
    shared_py = tmp_path / "uv-shared-python"
    recorded_uv_cmds: list[list[str]] = []
    monkeypatch.setattr("tools.scripts.build_server_binary.shutil.which", lambda _name: "/usr/bin/uv")
    monkeypatch.setattr(
        build_server_binary_mod,
        "_probe_python_shared_info",
        lambda exe: (Path(exe) == shared_py, "/opt/py/lib", "libpython3.12.so.1.0"),
    )

    def _fake_uv_find(cmd: list[str], **_kwargs: object) -> SimpleNamespace:
        recorded_uv_cmds.append(list(cmd))
        return SimpleNamespace(
            returncode=0,
            stdout=f"{shared_py}\n" if "--managed-python" in cmd else "",
            stderr="",
        )

    monkeypatch.setattr("tools.scripts.build_server_binary.subprocess.run", _fake_uv_find)
    assert builder.select_build_python(static_py) == shared_py
    assert recorded_uv_cmds == [["/usr/bin/uv", "python", "find", "--managed-python", "cpython-3.12"]]

    monkeypatch.setattr("tools.scripts.build_server_binary.shutil.which", lambda _name: None)
    with pytest.raises(SystemExit, match="statically linked"):
        builder.select_build_python(static_py)

    recorded_cargo: dict[str, object] = {}
    built_exe = tmp_path / "src" / "rs" / "target" / linux_target.rust_target / "release" / "vertebra-server"
    built_exe.parent.mkdir(parents=True)
    built_exe.write_bytes(b"ELF-SERVER")

    monkeypatch.setattr(
        build_server_binary_mod, "_probe_python_shared_info", lambda _exe: (True, "/lib", "libpython3.12.so")
    )

    def fake_cargo_run(cmd: list[str], *, env: dict[str, str], **_kwargs: object) -> SimpleNamespace:
        recorded_cargo["cmd"] = cmd
        recorded_cargo["PYO3_PYTHON"] = env["PYO3_PYTHON"]
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr("tools.scripts.build_server_binary.subprocess.run", fake_cargo_run)
    staged = builder.build(build_python=shared_py)
    assert staged == tmp_path / "src" / "py" / "vertebra" / "bin" / "vertebra-server"
    assert staged.read_bytes() == b"ELF-SERVER"
    assert recorded_cargo["PYO3_PYTHON"] == str(shared_py)


@pytest.mark.parametrize(
    ("enable_shared", "create_lib", "ldlibrary", "pythonframework", "platform_name", "expected_shared"),
    [
        (1, False, "libpython3.12.so", None, "linux", True),
        (None, True, "libpython3.12.so", None, "linux", True),
        (None, False, "libpython3.12.so", None, "linux", False),
        (0, True, "libpython3.12.so", None, "linux", False),
        (1, True, "libpython3.12.a", None, "linux", False),
        (0, False, "Python.framework/Versions/3.12/Python", "Python", "darwin", True),
        (0, False, "PythonT.framework/Versions/3.14/PythonT", "PythonT", "darwin", True),
        (0, False, "libpython3.12.a", "Python", "darwin", False),
    ],
)
def test_probe_python_shared_info_handles_standalone_and_static_builds(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    enable_shared: int | None,
    create_lib: bool,
    ldlibrary: str,
    pythonframework: str | None,
    platform_name: str,
    expected_shared: bool,
) -> None:
    """Verify _probe_python_shared_info detects standalone, framework, and static Python builds."""
    libdir = tmp_path / "lib"
    libdir.mkdir(parents=True)
    if create_lib:
        (libdir / ldlibrary).write_bytes(b"ELF")
    payload = {
        "enable_shared": enable_shared,
        "libdir": str(libdir),
        "ldlibrary": ldlibrary,
        "pythonframework": pythonframework,
        "platform": platform_name,
    }
    monkeypatch.setattr(
        "tools.scripts.build_server_binary.subprocess.run",
        lambda *_args, **_kwargs: SimpleNamespace(returncode=0, stdout=json.dumps(payload), stderr=""),
    )
    is_shared, probed_libdir, probed_ldlibrary = build_server_binary_mod._probe_python_shared_info(
        tmp_path / "bin" / "python"
    )
    assert is_shared is expected_shared
    assert probed_libdir == str(libdir)
    assert probed_ldlibrary == ldlibrary


def test_standalone_builder_include_agent_skills_staging_and_refusal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify --include-agent-skills stages app skills cleanly, refuses Vertebra repo root, and sets VERTEBRA_APP_ROOT."""
    repo_root = Path(__file__).resolve().parents[5]
    target = BuildTarget.resolve("linux-x86_64", python_version="3.12")
    with pytest.raises(ValueError, match="Refusing to bundle Vertebra repository"):
        StandaloneBuilder(
            project_dir=repo_root,
            target=target,
            include_agent_skills=True,
        ).build(tmp_path / "blocked_dist")

    runner = CliRunner()
    cli_refused = runner.invoke(
        build_command,
        [
            "--project-dir",
            str(repo_root),
            "--standalone",
            "--include-agent-skills",
            "--target",
            "linux-x86_64",
            "--output",
            str(tmp_path / "blocked_cli_dist"),
        ],
    )
    assert cli_refused.exit_code != 0
    assert "Refusing to bundle Vertebra repository" in cli_refused.output

    app_project = tmp_path / "app_repo"
    (app_project / "pyproject.toml").parent.mkdir(parents=True)
    (app_project / "pyproject.toml").write_text('[project]\nname = "skills-app"\nversion = "0.1.0"\n')
    (app_project / "app").mkdir()
    (app_project / "app" / "__init__.py").write_text('"""App."""\n')
    (app_project / "app" / "main.py").write_text(
        '"""App entrypoint checking VERTEBRA_APP_ROOT and skills."""\n'
        "from vertebra.agent.mcp.skills import FilesystemSkillSource, MCPSkillsConfig, SkillCatalog\n\n"
        "def run() -> int:\n"
        "    catalog = SkillCatalog.from_config(MCPSkillsConfig())\n"
        "    direct = FilesystemSkillSource(['.agents/skills']).load_skills()\n"
        "    ok = catalog.get_skill('billing-helper') is not None and len(direct) == 1\n"
        "    return 0 if ok else 9\n"
    )
    skill_dir = app_project / ".agents" / "skills" / "billing-helper"
    skill_dir.mkdir(parents=True)
    (skill_dir / "SKILL.md").write_text("---\nname: billing-helper\ndescription: Billing workflow\n---\nBody text.\n")
    (skill_dir / "__pycache__").mkdir()
    (skill_dir / "__pycache__" / "cached.pyc").write_bytes(b"pyc")
    (skill_dir / ".DS_Store").write_bytes(b"ds")
    (app_project / ".agents" / "bundles").mkdir()
    (app_project / ".agents" / "bundles" / "internal.md").write_text("do not copy")

    default_dist = StandaloneBuilder(
        project_dir=app_project,
        target=target,
        entrypoint="app.main:run",
    ).build(tmp_path / "dist_default")
    assert not (default_dist / "app" / ".agents" / "skills").exists()
    default_manifest = decode_json((default_dist / "manifest.json").read_bytes())
    assert "agent_skills_staged" not in default_manifest

    staged_dist = tmp_path / "dist_with_skills"
    cli_ok = runner.invoke(
        build_command,
        [
            "--project-dir",
            str(app_project),
            "--standalone",
            "--include-agent-skills",
            "--entrypoint",
            "app.main:run",
            "--target",
            "linux-x86_64",
            "--output",
            str(staged_dist),
        ],
    )
    assert cli_ok.exit_code == 0, cli_ok.output
    assert (staged_dist / "app" / ".agents" / "skills" / "billing-helper" / "SKILL.md").is_file()
    assert not (staged_dist / "app" / ".agents" / "skills" / "billing-helper" / "__pycache__").exists()
    assert not (staged_dist / "app" / ".agents" / "skills" / "billing-helper" / ".DS_Store").exists()
    assert not (staged_dist / "app" / ".agents" / "bundles").exists()
    skills_manifest = decode_json((staged_dist / "manifest.json").read_bytes())
    assert skills_manifest["agent_skills_staged"] is True

    unrelated_cwd = tmp_path / "unrelated_cwd"
    unrelated_cwd.mkdir()
    monkeypatch.setenv("VERTEBRA_APP_ROOT", str(staged_dist / "app"))
    monkeypatch.chdir(unrelated_cwd)
    catalog = SkillCatalog.from_config(MCPSkillsConfig())
    assert catalog.get_skill("billing-helper") is not None
    assert len(FilesystemSkillSource([".agents/skills"]).load_skills()) == 1

    onefile_bin = tmp_path / "skills-app-onefile"
    OnefileBuilder(dist_dir=staged_dist, target=target).build(onefile_bin)
    env = dict(os.environ)
    env.pop("VERTEBRA_APP_ROOT", None)
    env["PATH"] = f"{Path(sys.executable).parent}{os.pathsep}{env.get('PATH', '')}"
    env["XDG_CACHE_HOME"] = str(tmp_path / "onefile_cache")
    proc = subprocess.run([str(onefile_bin)], cwd=unrelated_cwd, env=env, capture_output=True, text=True, check=False)
    assert proc.returncode == 0, proc.stderr


def test_smoke_vtb_run_scenarios_wheel_inspection_and_early_exit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify SmokeScenario matrix, wheel binary/libpython checks, sample app generation, and early-exit errors."""
    linux_scenarios = VtbRunSmoke.default_scenarios(platform="linux")
    assert [item.name for item in linux_scenarios] == [
        "embedded_unary",
        "progressive_sse",
        "shm_workers",
        "worker_only_healthz",
    ]
    win_scenarios = VtbRunSmoke.default_scenarios(platform="win32")
    assert [item.name for item in win_scenarios] == [
        "embedded_unary",
        "progressive_sse",
        "worker_only_healthz",
    ]

    clean_whl = tmp_path / "vertebra_core-0.3.0-cp312-cp312-manylinux_x86_64.whl"
    with zipfile.ZipFile(clean_whl, "w") as zf:
        zf.writestr("vertebra/__init__.py", "")
        zf.writestr("vertebra/bin/vertebra-server", b"ELF")
    assert VtbRunSmoke.verify_wheel_archive(clean_whl, platform="linux") == ["vertebra/bin/vertebra-server"]

    missing_bin_whl = tmp_path / "missing_bin.whl"
    with zipfile.ZipFile(missing_bin_whl, "w") as zf:
        zf.writestr("vertebra/__init__.py", "")
    with pytest.raises(ValueError, match="missing packaged server binary"):
        VtbRunSmoke.verify_wheel_archive(missing_bin_whl, platform="linux")

    bundled_libpython_whl = tmp_path / "bundled_libpython.whl"
    with zipfile.ZipFile(bundled_libpython_whl, "w") as zf:
        zf.writestr("vertebra/bin/vertebra-server", b"ELF")
        zf.writestr("vertebra_core.libs/libpython3.12.so.1.0", b"SO")
    with pytest.raises(ValueError, match="unexpectedly bundled libpython"):
        VtbRunSmoke.verify_wheel_archive(bundled_libpython_whl, platform="linux")

    smoke = VtbRunSmoke(vtb_executable=tmp_path / "vtb", workdir=tmp_path / "app")
    app_file = smoke.write_sample_app()
    assert app_file.is_file()
    assert "asgi_sse_app" in app_file.read_text(encoding="utf-8")

    class _ExitedProc:
        returncode = 2

        def poll(self) -> int:
            return self.returncode

        def communicate(self, timeout: float | None = None) -> tuple[str, str]:
            _ = timeout
            return "boot output", "missing shared libpython"

    monkeypatch.setattr("tools.scripts.smoke_vtb_run.subprocess.Popen", lambda *_a, **_kw: _ExitedProc())
    scenario = SmokeScenario(name="embedded_unary", args=("run", "smoke_app:app"), request_path="/health")
    with pytest.raises(RuntimeError, match="missing shared libpython"):
        smoke.run_scenario(scenario)

    empty_dir = tmp_path / "empty_wheelhouse"
    empty_dir.mkdir()
    with pytest.raises(FileNotFoundError, match="found 0"):
        smoke_vtb_run_mod.main(["--vtb", str(tmp_path / "vtb"), "--wheel-dir", str(empty_dir), "--verify-wheel-only"])

    multi_dir = tmp_path / "multi_wheelhouse"
    multi_dir.mkdir()
    for tag in ("cp312-cp312", "cp313-cp313", "cp314-cp314", "cp314t-cp314t", "cp315-cp315", "cp315t-cp315t"):
        whl = multi_dir / f"vertebra_core-0.3.0-{tag}-manylinux_x86_64.whl"
        with zipfile.ZipFile(whl, "w") as zf:
            zf.writestr("vertebra/__init__.py", "")
            zf.writestr("vertebra/bin/vertebra-server", b"ELF")
    assert (
        smoke_vtb_run_mod.main(["--vtb", str(tmp_path / "vtb"), "--wheel-dir", str(multi_dir), "--verify-wheel-only"])
        == 0
    )

    bad_extra_whl = multi_dir / "vertebra_core-0.3.0-cp315t-cp315t-manylinux_x86_64.whl"
    with zipfile.ZipFile(bad_extra_whl, "w") as zf:
        zf.writestr("vertebra/bin/vertebra-server", b"ELF")
        zf.writestr("vertebra_core.libs/libpython3.15t.so.1.0", b"SO")
    with pytest.raises(ValueError, match="unexpectedly bundled libpython"):
        smoke_vtb_run_mod.main(["--vtb", str(tmp_path / "vtb"), "--wheel-dir", str(multi_dir), "--verify-wheel-only"])


def test_makefile_and_ci_release_workflows_wire_core_wheel_smoke_and_arm64() -> None:
    """Verify Makefile core-wheel-check, PR core-wheel-smoke, cibuildwheel hooks, and release-binaries linux-arm64."""
    repo_root = Path(__file__).resolve().parents[5]
    makefile_text = (repo_root / "Makefile").read_text(encoding="utf-8")
    assert ".PHONY: core-wheel-check" in makefile_text
    assert "build_server_binary.py build" in makefile_text
    assert "smoke_vtb_run.py" in makefile_text
    assert "/tmp/vtb-core-wheel" in makefile_text
    assert "/tmp/vtb-core-smoke-venv" in makefile_text

    ci_doc = yaml.safe_load((repo_root / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8"))
    ci_verify = ci_doc["jobs"]["verify-wheels"]
    assert ci_verify["runs-on"] == "ubuntu-latest"
    ci_run_cmds = "\n".join(str(step.get("run", "")) for step in ci_verify["steps"])
    assert "build_ci_wheels.py verify" in ci_run_cmds
    build_native_cmds = "\n".join(str(step.get("run", "")) for step in ci_doc["jobs"]["build-native"]["steps"])
    assert "build_ci_wheels.py build" in build_native_cmds
    assert "--with-server" in build_native_cmds

    rel_doc = yaml.safe_load((repo_root / ".github" / "workflows" / "release.yaml").read_text(encoding="utf-8"))
    build_steps = rel_doc["jobs"]["build_wheels"]["steps"]
    core_step = next(step for step in build_steps if step.get("name") == "Build core wheel")
    companion_step = next(step for step in build_steps if step.get("name") == "Build wheels")
    assert "matrix.package.name == 'core'" in core_step["if"]
    assert "build_server_binary.py build" in core_step["env"]["CIBW_BEFORE_BUILD"]
    assert "build_server_binary.py repair" in core_step["env"]["CIBW_REPAIR_WHEEL_COMMAND_LINUX"]
    assert "build_server_binary.py repair" in core_step["env"]["CIBW_REPAIR_WHEEL_COMMAND_MACOS"]
    assert "CIBW_CONTAINER_ENGINE" not in core_step["env"]
    assert core_step["env"]["CIBW_CONTAINER_ENGINE_LINUX"].startswith("docker; create_args:")
    assert "create_args=" not in core_step["env"]["CIBW_CONTAINER_ENGINE_LINUX"]
    assert "CARGO_HOME" not in core_step["env"]["CIBW_ENVIRONMENT_LINUX"]
    assert "uv python install" in core_step["env"]["CIBW_BEFORE_ALL_LINUX"]
    assert core_step["env"]["CIBW_SKIP"] == "*-musllinux_*"
    assert "matrix.package.name != 'core'" in companion_step["if"]
    assert "CIBW_BEFORE_BUILD" not in companion_step.get("env", {})
    assert "CIBW_CONTAINER_ENGINE" not in companion_step["env"]
    assert companion_step["env"]["CIBW_CONTAINER_ENGINE_LINUX"].startswith("docker; create_args:")
    assert "create_args=" not in companion_step["env"]["CIBW_CONTAINER_ENGINE_LINUX"]
    assert companion_step["env"]["CIBW_TEST_ENVIRONMENT"] == "PIP_NO_DEPS=1"
    assert companion_step["env"]["CIBW_SKIP"] == "*-musllinux_*"

    test_wheels = rel_doc["jobs"]["test_wheels"]
    assert test_wheels["strategy"]["matrix"]["os"] == [
        "ubuntu-latest",
        "ubuntu-24.04-arm",
        "macos-latest",
        "windows-latest",
    ]
    tw_cmds = "\n".join(str(step.get("run", "")) for step in test_wheels["steps"])
    assert "smoke_vtb_run.py" in tw_cmds

    rel_bin_doc = yaml.safe_load(
        (repo_root / ".github" / "workflows" / "release-binaries.yml").read_text(encoding="utf-8")
    )
    targets = {entry["target"]: entry for entry in rel_bin_doc["jobs"]["build"]["strategy"]["matrix"]["include"]}
    assert "linux-arm64" in targets
    assert targets["linux-arm64"]["os"] == "ubuntu-24.04-arm"
    assert targets["linux-arm64"]["bin_name"] == "vertebra-server-linux-arm64"
    assert targets["darwin-arm64"]["os"] == "macos-latest"


def test_check_extras_bidirectional_parity_on_repo() -> None:
    """Verify check_extras reports zero mismatches on the finished repository."""
    repo_root = Path(__file__).resolve().parents[5]
    assert check_extras_mod.find_extras_mismatches(repo_root) == []
    assert check_extras_mod.main(["--repo-root", str(repo_root)]) == 0


@pytest.mark.parametrize(
    ("extra_toml", "source_body", "expected_fragment"),
    [
        (
            "",
            'from vertebra.utils.module_loader import import_optional\nimport_optional("missing_pkg")\n',
            "missing_pkg",
        ),
        (
            'unused_extra = ["unused-pkg>=1.0"]\n',
            "VALUE = 1\n",
            "unused_extra",
        ),
        (
            'extra_a = ["pkg-a>=1.0"]\n',
            'import importlib\nimportlib.import_module("zstandard")\n',
            "zstandard",
        ),
    ],
)
def test_check_extras_flags_unmapped_imports_and_unused_extras(
    tmp_path: Path,
    extra_toml: str,
    source_body: str,
    expected_fragment: str,
) -> None:
    """Verify check_extras flags both missing extras and extras lacking importers."""
    (tmp_path / "pyproject.toml").write_text(
        f'[project]\nname = "sample"\nversion = "0.1.0"\n[project.optional-dependencies]\n{extra_toml}',
        encoding="utf-8",
    )
    src_pkg = tmp_path / "src" / "py" / "vertebra"
    src_pkg.mkdir(parents=True)
    (src_pkg / "mod.py").write_text(source_body, encoding="utf-8")

    mismatches = check_extras_mod.find_extras_mismatches(tmp_path)
    assert mismatches
    assert any(expected_fragment in item for item in mismatches)
    assert check_extras_mod.main(["--repo-root", str(tmp_path)]) == 1


def test_onefile_builder_compression_defaults_zstd_and_backtick_escaping(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify OnefileBuilder defaults to zlib, supports zstd via CLI, and escapes shell backticks."""
    dist_dir = tmp_path / "dist"
    dist_dir.mkdir()
    (dist_dir / "__main__.py").write_text("import sys\nsys.exit(0)\n", encoding="utf-8")
    (dist_dir / "manifest.json").write_text("{}", encoding="utf-8")
    target = BuildTarget.resolve("linux-x86_64", python_version="3.12")

    class _FakeZstdCompressor:
        def __init__(self, *, level: int = 19) -> None:
            self.level = level

        def compress(self, data: bytes) -> bytes:
            return zlib.compress(data, level=1)

    class _FakeZstdDecompressor:
        def decompress(self, data: bytes) -> bytes:
            return zlib.decompress(data)

    fake_zstd: Any = types.ModuleType("zstandard")
    fake_zstd.ZstdCompressor = _FakeZstdCompressor
    fake_zstd.ZstdDecompressor = _FakeZstdDecompressor

    with pytest.raises(ValueError, match="Unsupported onefile compression"):
        OnefileBuilder(dist_dir, target, compression=cast("Any", "lz4"))

    monkeypatch.setitem(sys.modules, "zstandard", fake_zstd)
    default_bin = tmp_path / "default-bin"
    OnefileBuilder(dist_dir, target).build(default_bin)
    raw_default = default_bin.read_bytes()
    meta_default = OnefileBuilder.inspect_trailer(default_bin)
    payload_default = raw_default[meta_default["stub_size"] : meta_default["stub_size"] + meta_default["payload_size"]]
    assert payload_default[:4] == b"ZLIB"

    zstd_bin = tmp_path / "zstd-bin"
    OnefileBuilder(dist_dir, target, compression="zstd").build(zstd_bin)
    raw_zstd = zstd_bin.read_bytes()
    meta_zstd = OnefileBuilder.inspect_trailer(zstd_bin)
    payload_zstd = raw_zstd[meta_zstd["stub_size"] : meta_zstd["stub_size"] + meta_zstd["payload_size"]]
    assert payload_zstd[:4] == b"ZSTD"
    extracted_zstd = tmp_path / "extracted_zstd"
    OnefileBuilder.extract_to(zstd_bin, extracted_zstd)
    assert (extracted_zstd / "__main__.py").is_file()

    project_dir = tmp_path / "cli_comp_app"
    (project_dir / "app").mkdir(parents=True)
    (project_dir / "pyproject.toml").write_text('[project]\nname = "comp-app"\nversion = "0.1.0"\n', encoding="utf-8")
    (project_dir / "app" / "__init__.py").write_text('"""App."""\n', encoding="utf-8")
    cli_zstd_bin = tmp_path / "cli-zstd-bin"
    res = CliRunner().invoke(
        build_command,
        [
            "--project-dir",
            str(project_dir),
            "--standalone",
            "--onefile",
            "--compression",
            "zstd",
            "--target",
            "linux-x86_64",
            "--output",
            str(cli_zstd_bin),
        ],
    )
    assert res.exit_code == 0, res.output
    cli_meta = OnefileBuilder.inspect_trailer(cli_zstd_bin)
    cli_raw = cli_zstd_bin.read_bytes()
    cli_payload = cli_raw[cli_meta["stub_size"] : cli_meta["stub_size"] + cli_meta["payload_size"]]
    assert cli_payload[:4] == b"ZSTD"

    monkeypatch.delitem(sys.modules, "zstandard", raising=False)
    monkeypatch.setattr("vertebra.build.packaging.onefile.ensure_module", _raise_missing_zstd)
    with pytest.raises(MissingDependencyError, match="zstandard") as exc_info:
        OnefileBuilder(dist_dir, target, compression="zstd").build(tmp_path / "missing-zstd-bin")
    assert exc_info.value.install_package == "vertebra-core[zstandard]"

    sample_code = 'import sys\nsys.stdout.write("`backticks`")\n'
    quoted = OnefileBuilder._quote_python_code(sample_code)
    proc = subprocess.run(
        ["/bin/sh", "-c", f"{sys.executable} -c {quoted}"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr
    assert proc.stdout == "`backticks`"


def test_build_ci_wheels_parsing_staging_and_server_profiles(tmp_path: Path) -> None:
    """Verify CI wheel helper package filtering, maturin command flags, and binary staging."""
    assert build_ci_wheels_mod.parse_packages(None) == build_ci_wheels_mod.ALL_PACKAGES
    assert build_ci_wheels_mod.parse_packages("all") == build_ci_wheels_mod.ALL_PACKAGES
    assert build_ci_wheels_mod.parse_packages("core, postgres, core") == ("core", "postgres")
    with pytest.raises(ValueError, match="Unknown package"):
        build_ci_wheels_mod.parse_packages("core,nonexistent")

    cmd = build_ci_wheels_mod._maturin_build_command(
        maturin_bin="maturin",
        repo_root=tmp_path,
        package="postgres",
        wheel_dir=tmp_path / "wheels",
        interpreter=tmp_path / "venv" / "bin" / "python",
        profile="dev",
        strip=True,
        skip_auditwheel=True,
    )
    assert "--profile" in cmd and "dev" in cmd
    assert "--strip" in cmd
    assert "--manifest-path" in cmd
    assert str(tmp_path / "src" / "rs" / "vertebra-postgres" / "Cargo.toml") in cmd

    fake_wheel = tmp_path / "vertebra_core-0.3.0-cp312-cp312-linux_x86_64.whl"
    with zipfile.ZipFile(fake_wheel, "w") as archive:
        archive.writestr("vertebra/_vertebra.cpython-312-x86_64-linux-gnu.so", b"FAKE_SO")
        archive.writestr("vertebra/bin/vertebra-server", b"FAKE_BIN")
        archive.writestr("vertebra/__init__.py", b"__version__ = '0.3.0'\n")
    stage_dir = tmp_path / "pkg"
    staged = build_ci_wheels_mod.stage_wheel_binaries([fake_wheel], stage_dir)
    assert sorted(p.relative_to(stage_dir).as_posix() for p in staged) == [
        "_vertebra.cpython-312-x86_64-linux-gnu.so",
        "bin/vertebra-server",
    ]
    assert not (stage_dir / "__init__.py").exists()

    target = ServerBinaryTarget.from_host()
    builder = ServerBinaryBuilder(repo_root=tmp_path, target=target)
    rel_cmd = builder.cargo_command(profile="release", target_triple=target.rust_target)
    assert "--release" in rel_cmd and "--target" in rel_cmd
    ci_cmd = builder.cargo_command(profile="ci", target_triple="")
    assert "--profile" in ci_cmd and "ci" in ci_cmd
    assert "--target" not in ci_cmd


def test_build_ci_wheels_resolves_symlinked_interpreter_for_pyo3_python(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify build_ci_distributions resolves symlinked venv interpreters so PYO3_PYTHON stays unique per version."""
    real_py = tmp_path / "cpython-3.13" / "bin" / "python3.13"
    real_py.parent.mkdir(parents=True)
    real_py.write_text("#!/bin/sh\n", encoding="utf-8")

    venv_bin = tmp_path / ".venv" / "bin"
    venv_bin.mkdir(parents=True)
    symlink_py = venv_bin / "python"
    symlink_py.symlink_to(real_py)
    fake_maturin = venv_bin / "maturin"
    fake_maturin.write_text("#!/bin/sh\n", encoding="utf-8")

    wheel_dir = tmp_path / "dist" / "wheels"
    recorded_cmds: list[list[str]] = []
    recorded_envs: list[dict[str, str]] = []

    def _fake_run(
        cmd: list[str],
        *,
        cwd: Path | None = None,
        env: dict[str, str] | None = None,
        check: bool = False,
    ) -> subprocess.CompletedProcess[str]:
        _ = (cwd, check)
        recorded_cmds.append(list(cmd))
        recorded_envs.append(dict(env or {}))
        wheel_dir.mkdir(parents=True, exist_ok=True)
        (wheel_dir / "vertebra_core-0.3.0-cp313-cp313-linux_x86_64.whl").write_bytes(b"PK")
        return subprocess.CompletedProcess(cmd, 0)

    monkeypatch.setattr(subprocess, "run", _fake_run)

    built = build_ci_wheels_mod.build_ci_distributions(
        tmp_path,
        wheel_dir=wheel_dir,
        packages=("core",),
        interpreter=symlink_py,
    )
    assert len(built) == 1
    assert recorded_cmds[0][0] == str(fake_maturin)
    interp_idx = recorded_cmds[0].index("--interpreter") + 1
    assert recorded_cmds[0][interp_idx] == str(real_py.resolve())
    assert recorded_envs[0]["PYO3_PYTHON"] == str(real_py.resolve())


def _raise_missing_zstd(
    module_name: str,
    *,
    package: str | None = None,
    extra: str | None = None,
    install_package: str | None = None,
) -> object:
    """Test helper raising MissingDependencyError for absent zstandard."""
    _ = module_name
    raise MissingDependencyError(package or "zstandard", install_package=install_package, extra=extra)
