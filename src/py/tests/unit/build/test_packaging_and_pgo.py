"""Unit tests for vertebra.build (targets, standalone, onefile), CLI flags, and tools/pgo."""

import os
import subprocess
from pathlib import Path

import pytest
from click.testing import CliRunner
from tools.pgo.python_pgo import (
    CPYTHON_PGO_FLAGS,
    PLATFORM_MATRIX,
    PythonBuildInfo,
    check_pgo_availability,
    detect_python_build,
    get_platform_support,
)
from tools.pgo.rust_pgo import (
    RustPgoConfig,
    build_flags,
    check_profdata_size,
    merge_profiles,
)

from vertebra.build import (
    ONEFILE_MAGIC,
    ONEFILE_TRAILER_SIZE,
    BuildTarget,
    OnefileBuilder,
    StandaloneBuilder,
)
from vertebra.cli import cli
from vertebra.cli.build import build as build_command


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


def test_cli_build_standalone_onefile_target_and_pgo_flags(tmp_path: Path) -> None:
    """Verify vtb build CLI supports --standalone, --onefile, --target, and --pgo options."""
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
            "--pgo",
            "--output",
            str(output_bin),
        ],
    )
    assert result.exit_code == 0, result.output
    assert output_bin.exists()

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
    """Verify RustPgoConfig flag generation, profraw validation, and CPython PGO inspection."""
    profile_dir = tmp_path / "profiles"
    cfg = RustPgoConfig(crate_name="vertebra-http", profile_dir=profile_dir)

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
    assert set(avail.keys()) == {"pgo", "lto", "bolt", "debug"}
    assert "--enable-optimizations" in CPYTHON_PGO_FLAGS
    assert len(PLATFORM_MATRIX) >= 4
    support = get_platform_support()
    assert support.cpython_pgo is True
