"""Unit tests for vertebra.build (targets, standalone, onefile), CLI flags, and tools/pgo."""

import os
import subprocess
import tomllib
from pathlib import Path
from types import SimpleNamespace

import pytest
from click.testing import CliRunner
from tools.pgo import python_pgo as tools_python_pgo
from tools.pgo import rust_pgo as tools_rust_pgo

from vertebra.build.packaging import (
    EXTENSION_MANIFESTS,
    ONEFILE_MAGIC,
    ONEFILE_TRAILER_SIZE,
    SUPPORTED_PYTHON_VERSIONS,
    BuildTarget,
    OnefileBuilder,
    PgoPipelineResult,
    StandaloneBuilder,
    find_llvm_profdata,
    resolve_cargo_release_dir,
    run_pgo_pipeline,
)
from vertebra.build.packaging import pgo as packaging_pgo
from vertebra.build.packaging.pgo import (
    CPYTHON_PGO_FLAGS,
    PLATFORM_MATRIX,
    PythonBuildInfo,
    RustPgoConfig,
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
from vertebra.extensions.sqlalchemy.dialects import DialectDescriptor
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
    synthetic_vars = {
        "CONFIG_ARGS": "'--enable-optimizations' '--with-lto' '--with-tail-call-interp' '--enable-experimental-jit'",
        "Py_TAIL_CALL_INTERP": "1",
        "Py_GIL_DISABLED": "1",
        "CFLAGS_NODIST": "-fno-omit-frame-pointer -mno-omit-leaf-frame-pointer",
    }
    fake_jit = SimpleNamespace(is_available=lambda: True, is_enabled=lambda: True)

    for mod in (packaging_pgo, tools_python_pgo):
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


def test_release_workflow_cibuildwheel_v4_and_python_315_matrix() -> None:
    """Verify release.yaml builds Python 3.12-3.15 and free-threaded 3.14t/3.15t wheels with cibuildwheel v4.3.4."""
    repo_root = Path(__file__).resolve().parents[5]
    release_yaml = (repo_root / ".github" / "workflows" / "release.yaml").read_text(encoding="utf-8")
    assert "pypa/cibuildwheel@v4.3.4" in release_yaml
    for target in ("cp312-*", "cp313-*", "cp314-*", "cp314t-*", "cp315-*", "cp315t-*"):
        assert target in release_yaml


def test_resolve_cargo_release_dir_and_multi_extension_manifests(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify CARGO_TARGET_DIR resolution, real workspace manifests, and multi-extension maturin invocations."""
    cfg = RustPgoConfig(profile_dir=tmp_path / "raw")
    assert cfg.manifest_path.is_file()
    assert len(EXTENSION_MANIFESTS) == 3
    assert all(manifest.is_file() for manifest in EXTENSION_MANIFESTS)
    assert not hasattr(packaging_pgo, "resolve_llvm_profdata")
    assert tools_rust_pgo.RustPgoConfig is RustPgoConfig
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
    assert len(recorded_cmds) == 3
    assert "-m" not in recorded_cmds[0][0]
    assert "--features" in recorded_cmds[0][0]
    assert "extension-module" in recorded_cmds[0][0]
    assert f"-Cprofile-generate={cfg.profile_dir}" in recorded_cmds[0][1]
    assert "-m" in recorded_cmds[1][0]
    assert str(EXTENSION_MANIFESTS[1]) in recorded_cmds[1][0]
    assert "-m" in recorded_cmds[2][0]
    assert str(EXTENSION_MANIFESTS[2]) in recorded_cmds[2][0]

    recorded_cmds.clear()
    valid_profdata = tmp_path / "valid.profdata"
    valid_profdata.write_bytes(b"PROFDATA")
    cfg_with_bin = RustPgoConfig(profile_dir=tmp_path / "raw", llvm_profdata_bin="llvm-profdata")
    build_optimized_extensions(cfg_with_bin, valid_profdata)
    assert len(recorded_cmds) == 3
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
    cfg = RustPgoConfig(profile_dir=tmp_path / "raw", llvm_profdata_bin="llvm-profdata")

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
    cfg = RustPgoConfig(profile_dir=raw_dir, llvm_profdata_bin="llvm-profdata")
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

    def fake_workload(active_cfg: RustPgoConfig, release_dir: Path) -> None:
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
    assert isinstance(result, PgoPipelineResult)
    assert result.used_pgo is True
    assert result.profdata_valid is True
    assert result.profdata_bytes == len(b"MERGED_LLVM_PROFDATA")
    assert result.release_dir == tmp_path / "rs-target" / "release"
    assert len(result.extension_manifests) == 3
    assert observed_profile_env and observed_profile_env[0].endswith(".profraw")
    assert phases == [
        "cargo:gen",
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


def test_standalone_and_cli_pgo_profdata_and_server_binary_staging(tmp_path: Path) -> None:
    """Verify StandaloneBuilder, OnefileBuilder, and vtb build stage server binaries and PGO metadata."""
    project_dir = tmp_path / "pgo_app"
    pkg_dir = project_dir / "app"
    pkg_dir.mkdir(parents=True)
    (project_dir / "pyproject.toml").write_text('[project]\nname = "pgo-app"\nversion = "0.2.0"\n')
    (pkg_dir / "__init__.py").write_text('"""PGO app package."""\n')
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
    fake_profdata = tmp_path / "sample.profdata"
    fake_profdata.write_bytes(b"unvalidated-profdata-payload")

    runner = CliRunner()
    onefile_bin = tmp_path / "dist_out" / "pgo-app-onefile"
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
            "--pgo",
            "--profdata",
            str(fake_profdata),
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
    assert manifest["pgo_enabled"] is True
    assert manifest["server_binary"] == "bin/vertebra-server"
    assert manifest["target"]["python_version"] == "3.15t"
    assert manifest["target"]["rust_triple"] == "x86_64-unknown-linux-musl"
    assert manifest["profdata_bytes"] == len(b"unvalidated-profdata-payload")
    assert len(manifest["profdata_sha256"]) == 64
    assert manifest["profdata_valid"] is False
    assert isinstance(manifest["python_build"], dict)
    assert "is_tail_call_interp" in manifest["python_build"]

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


def test_pyproject_sqlalchemy_dialect_entry_points_resolve() -> None:
    """Verify all 12 pyproject.toml SQLAlchemy dialect entry points resolve to DialectDescriptor subclasses."""
    repo_root = Path(__file__).resolve().parents[5]
    pyproject_data = tomllib.loads((repo_root / "pyproject.toml").read_text(encoding="utf-8"))
    entry_points: dict[str, str] = pyproject_data["project"]["entry-points"]["sqlalchemy.dialects"]

    expected_keys = {
        f"{engine}.{suffix}"
        for engine in ("postgresql", "oracle", "mysql", "sqlite", "mssql", "duckdb")
        for suffix in ("vertebra", "vtb")
    }
    assert set(entry_points.keys()) == expected_keys

    for ep_name, target_spec in entry_points.items():
        mod_name, _, cls_name = target_spec.partition(":")
        module = ensure_module(mod_name)
        resolved_cls = getattr(module, cls_name)
        assert issubclass(resolved_cls, DialectDescriptor), ep_name
        assert "Vtb" not in cls_name
