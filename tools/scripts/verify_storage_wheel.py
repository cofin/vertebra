"""Build and verify standalone companion wheel isolation across storage, cloud, and bundler crates."""

from __future__ import annotations

import argparse
import sys
import tempfile
from collections.abc import Sequence
from pathlib import Path
from typing import Literal, get_args

import msgspec

from tools.scripts._wheel_verify import (
    build_dist,
    verify_core_wheel_excludes,
    verify_extension_wheel_contents,
    verify_isolated_wheel_pair,
    verify_sdist_contents,
)

CompanionPackage = Literal["storage", "bigquery", "spanner", "bundler"]

__all__ = (
    "COMPANION_SPECS",
    "CompanionPackage",
    "CompanionWheelSpec",
    "main",
    "verify_companion_sdist_contents",
    "verify_companion_wheel_contents",
    "verify_core_wheel_excludes_companion",
    "verify_isolated_venv_contract",
)


class CompanionWheelSpec(msgspec.Struct, frozen=True, gc=False):
    """Packaging and isolated-venv verification specification for a companion extension."""

    package: CompanionPackage
    crate_dir: str
    module_name: str
    stub_entry: str
    binary_prefix: str
    core_sentinel: str
    install_target: str
    exports: tuple[str, ...]
    leaked_core_attr: str
    expect_gil_used: bool
    preinstall_probe: str
    postinstall_probe: str


COMPANION_SPECS: dict[CompanionPackage, CompanionWheelSpec] = {
    "storage": CompanionWheelSpec(
        package="storage",
        crate_dir="src/rs/vertebra-storage",
        module_name="vertebra._storage",
        stub_entry="vertebra/_storage.pyi",
        binary_prefix="vertebra/_storage",
        core_sentinel="vertebra/storage/_store.py",
        install_target="vertebra-core[storage]",
        exports=("SyncObjectStore", "AsyncObjectStore"),
        leaked_core_attr="SyncObjectStore",
        expect_gil_used=False,
        preinstall_probe=(
            "from vertebra.storage import SyncObjectStore\n"
            "try:\n"
            "    SyncObjectStore('memory://')\n"
            "except MissingDependencyError as exc:\n"
            "    assert exc.install_package == 'vertebra-core[storage]', exc.install_package\n"
            "    assert 'vertebra-core[storage]' in str(exc), str(exc)\n"
            "else:\n"
            "    raise AssertionError('SyncObjectStore did not raise MissingDependencyError')\n"
        ),
        postinstall_probe="assert mod.SyncObjectStore('memory://').scheme == 'memory'\n",
    ),
    "bigquery": CompanionWheelSpec(
        package="bigquery",
        crate_dir="src/rs/vertebra-bigquery",
        module_name="vertebra._bigquery",
        stub_entry="vertebra/_bigquery.pyi",
        binary_prefix="vertebra/_bigquery",
        core_sentinel="vertebra/db/adapters/bigquery/driver.py",
        install_target="vertebra-core[bigquery]",
        exports=("SyncBigQueryClient", "AsyncBigQueryClient", "GIL_USED"),
        leaked_core_attr="SyncBigQueryClient",
        expect_gil_used=True,
        preinstall_probe=(
            "from vertebra.db.adapters.bigquery import BigQueryConfig, SyncBigQueryDriver\n"
            "driver = SyncBigQueryDriver(BigQueryConfig(project_id='test-project'))\n"
            "try:\n"
            "    driver.execute('SELECT 1')\n"
            "except MissingDependencyError as exc:\n"
            "    assert exc.install_package == 'vertebra-core[bigquery]', exc.install_package\n"
            "    assert 'vertebra-core[bigquery]' in str(exc), str(exc)\n"
            "else:\n"
            "    raise AssertionError('SyncBigQueryDriver did not raise MissingDependencyError')\n"
        ),
        postinstall_probe="assert hasattr(mod, 'SyncBigQueryClient') and hasattr(mod, 'AsyncBigQueryClient')\n",
    ),
    "spanner": CompanionWheelSpec(
        package="spanner",
        crate_dir="src/rs/vertebra-spanner",
        module_name="vertebra._spanner",
        stub_entry="vertebra/_spanner.pyi",
        binary_prefix="vertebra/_spanner",
        core_sentinel="vertebra/db/adapters/spanner/driver.py",
        install_target="vertebra-core[spanner]",
        exports=("SyncSpannerClient", "AsyncSpannerClient", "GIL_USED"),
        leaked_core_attr="SyncSpannerClient",
        expect_gil_used=True,
        preinstall_probe=(
            "from vertebra.db.adapters.spanner import SpannerConfig, SyncSpannerDriver\n"
            "driver = SyncSpannerDriver(SpannerConfig(database='projects/p/instances/i/databases/d'))\n"
            "try:\n"
            "    driver.execute('SELECT 1')\n"
            "except MissingDependencyError as exc:\n"
            "    assert exc.install_package == 'vertebra-core[spanner]', exc.install_package\n"
            "    assert 'vertebra-core[spanner]' in str(exc), str(exc)\n"
            "else:\n"
            "    raise AssertionError('SyncSpannerDriver did not raise MissingDependencyError')\n"
        ),
        postinstall_probe="assert hasattr(mod, 'SyncSpannerClient') and hasattr(mod, 'AsyncSpannerClient')\n",
    ),
    "bundler": CompanionWheelSpec(
        package="bundler",
        crate_dir="src/rs/vertebra-bundler",
        module_name="vertebra._bundler",
        stub_entry="vertebra/_bundler.pyi",
        binary_prefix="vertebra/_bundler",
        core_sentinel="vertebra/build/_orchestrator.py",
        install_target="vertebra-core[bundler]",
        exports=("Bundler", "BundleConfig", "GIL_USED", "minify_css"),
        leaked_core_attr="Bundler",
        expect_gil_used=True,
        preinstall_probe=(
            "from vertebra.utils.module_loader import ensure_module\n"
            "try:\n"
            "    ensure_module('vertebra._bundler', package='vertebra-core[bundler]', extra='bundler')\n"
            "except MissingDependencyError as exc:\n"
            "    assert exc.install_package == 'vertebra-core[bundler]', exc.install_package\n"
            "    assert 'vertebra-core[bundler]' in str(exc), str(exc)\n"
            "else:\n"
            "    raise AssertionError('ensure_module did not raise MissingDependencyError for bundler')\n"
        ),
        postinstall_probe="assert 'body{color:red}' in mod.minify_css('body { color: red; }')\n",
    ),
}


def verify_core_wheel_excludes_companion(wheel_path: Path, *, package: CompanionPackage) -> None:
    """Assert `vertebra_core-*.whl` ships the Python wrapper without companion binaries or stubs."""
    spec = COMPANION_SPECS[package]
    verify_core_wheel_excludes(
        wheel_path,
        required_entries=(spec.core_sentinel,),
        stub_entry=spec.stub_entry,
        binary_prefix=spec.binary_prefix,
        package_label=package,
    )


def verify_companion_wheel_contents(wheel_path: Path, *, package: CompanionPackage) -> None:
    """Assert `vertebra_<pkg>-*.whl` ships solely its native binary and matching `.pyi` stub."""
    spec = COMPANION_SPECS[package]
    verify_extension_wheel_contents(
        wheel_path,
        label=package,
        stub_entry=spec.stub_entry,
        binary_prefix=spec.binary_prefix,
    )


def verify_companion_sdist_contents(sdist_path: Path, *, package: CompanionPackage) -> None:
    """Assert `vertebra_<pkg>-*.tar.gz` contains the crate manifest, build script, source, and stub."""
    spec = COMPANION_SPECS[package]
    verify_sdist_contents(
        sdist_path,
        label=package,
        required_suffixes=("Cargo.toml", "build.rs", "src/lib.rs", spec.stub_entry),
    )


def verify_isolated_venv_contract(
    core_wheel: Path,
    companion_wheel: Path,
    *,
    package: CompanionPackage,
) -> None:
    """Verify outside-checkout `vertebra-core` fails loud pre-install and works cleanly post-install."""
    spec = COMPANION_SPECS[package]
    preinstall_code = (
        "import importlib.util, pathlib, sys\n"
        "import vertebra\n"
        "from vertebra.typing import MissingDependencyError\n"
        "venv_root = pathlib.Path(sys.prefix).resolve()\n"
        "assert pathlib.Path(vertebra.__file__).resolve().is_relative_to(venv_root), vertebra.__file__\n"
        f"assert importlib.util.find_spec({spec.module_name!r}) is None\n"
        f"{spec.preinstall_probe}"
    )
    gil_check = "assert mod.GIL_USED is False\n" if spec.expect_gil_used else ""
    postinstall_code = (
        "import importlib, pathlib, sys\n"
        f"mod = importlib.import_module({spec.module_name!r})\n"
        "from vertebra import _vertebra\n"
        "venv_root = pathlib.Path(sys.prefix).resolve()\n"
        "assert pathlib.Path(mod.__file__).resolve().is_relative_to(venv_root), mod.__file__\n"
        f"{gil_check}"
        f"for export_name in {spec.exports!r}:\n"
        "    assert hasattr(mod, export_name), export_name\n"
        f"assert not hasattr(_vertebra, {spec.leaked_core_attr!r})\n"
        f"{spec.postinstall_probe}"
    )
    verify_isolated_wheel_pair(
        temp_prefix=f"vtb-{package}-venv-",
        core_wheel=core_wheel,
        ext_wheel=companion_wheel,
        preinstall_code=preinstall_code,
        postinstall_code=postinstall_code,
    )


def main(argv: Sequence[str] | None = None) -> int:
    """Build or accept core and companion distributions and verify wheel isolation."""
    valid_packages = get_args(CompanionPackage)
    parser = argparse.ArgumentParser(description="Verify standalone companion wheel isolation.")
    parser.add_argument(
        "--package",
        choices=valid_packages,
        default="storage",
        help="Companion package to verify (default: storage).",
    )
    parser.add_argument("--all", action="store_true", help="Verify all companion packages in sequence.")
    parser.add_argument("--core-wheel", type=Path, default=None, help="Prebuilt vertebra_core-*.whl to verify.")
    parser.add_argument(
        "--companion-wheel",
        type=Path,
        default=None,
        help="Prebuilt companion wheel (*.whl) to verify (single-package mode only).",
    )
    parser.add_argument(
        "--companion-sdist",
        type=Path,
        default=None,
        help="Prebuilt companion sdist (*.tar.gz) to verify (single-package mode only).",
    )
    args = parser.parse_args(argv)

    packages: tuple[CompanionPackage, ...] = valid_packages if args.all else (args.package,)
    if args.all and (args.companion_wheel is not None or args.companion_sdist is not None):
        msg = "--companion-wheel and --companion-sdist cannot be combined with --all"
        raise ValueError(msg)

    repo_root = Path(__file__).resolve().parents[2]
    with tempfile.TemporaryDirectory(prefix="vtb-companion-wheel-check-") as tmp:
        out_dir = Path(tmp)
        core_wheel = args.core_wheel or build_dist(repo_root, repo_root, out_dir / "core", kind="wheel")
        for pkg in packages:
            spec = COMPANION_SPECS[pkg]
            companion_wheel = args.companion_wheel or build_dist(
                repo_root,
                repo_root / spec.crate_dir,
                out_dir / f"{pkg}-whl",
                kind="wheel",
            )
            companion_sdist = args.companion_sdist or build_dist(
                repo_root,
                repo_root / spec.crate_dir,
                out_dir / f"{pkg}-sdist",
                kind="sdist",
            )
            verify_core_wheel_excludes_companion(core_wheel, package=pkg)
            verify_companion_wheel_contents(companion_wheel, package=pkg)
            verify_companion_sdist_contents(companion_sdist, package=pkg)
            verify_isolated_venv_contract(core_wheel, companion_wheel, package=pkg)
            sys.stdout.write(f"Verified vertebra-core and vertebra-{pkg} wheel isolation and fail-loud contract.\n")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
