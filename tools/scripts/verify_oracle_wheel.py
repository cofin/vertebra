"""Build and verify standalone vertebra-oracle wheel isolation and pure-Rust linkage."""

from __future__ import annotations

import argparse
import sys
import tarfile
import tempfile
import zipfile
from pathlib import Path

from tools.scripts._wheel_verify import (
    NATIVE_SUFFIXES,
    assert_zero_native_linkage,
    build_dist,
    verify_isolated_wheel_pair,
)

__all__ = (
    "main",
    "verify_core_wheel_excludes_oracle_binary_and_stub",
    "verify_isolated_venv_install_and_fail_loud_contract",
    "verify_oracle_sdist_contents",
    "verify_oracle_wheel_contents_and_pure_rust_linkage",
)

FORBIDDEN_NATIVE_LIBS: tuple[str, ...] = (
    "libclntsh",
    "libnnz",
    "libodpic",
    "libociicus",
)
SIBLING_STUBS: frozenset[str] = frozenset(
    {
        "vertebra/_vertebra.pyi",
        "vertebra/_bigquery.pyi",
        "vertebra/_bundler.pyi",
        "vertebra/_spanner.pyi",
        "vertebra/_storage.pyi",
    }
)


def verify_core_wheel_excludes_oracle_binary_and_stub(wheel_path: Path) -> None:
    """Assert `vertebra_core-*.whl` packs the Python Oracle adapter without `_oracle` binary or stub."""
    with zipfile.ZipFile(wheel_path) as zf:
        names = set(zf.namelist())

    if "vertebra/db/adapters/oracle/driver.py" not in names:
        msg = f"Core wheel {wheel_path.name} is missing vertebra/db/adapters/oracle/driver.py"
        raise RuntimeError(msg)

    leaked = sorted(
        name
        for name in names
        if name == "vertebra/_oracle.pyi"
        or name.startswith("vertebra/_oracle.")
        or (
            name.startswith("vertebra/_")
            and name.endswith(NATIVE_SUFFIXES)
            and not name.startswith("vertebra/_vertebra.")
        )
    )
    if leaked:
        msg = f"Core wheel {wheel_path.name} leaked optional extension artifacts: {leaked}"
        raise RuntimeError(msg)


def verify_oracle_wheel_contents_and_pure_rust_linkage(wheel_path: Path) -> None:
    """Assert `vertebra_oracle-*.whl` ships only `_oracle` binary + stub with zero Instant Client linkage."""
    with zipfile.ZipFile(wheel_path) as zf:
        names = zf.namelist()
        vtb_entries = sorted(name for name in names if name.startswith("vertebra/"))
        if "vertebra/_oracle.pyi" not in vtb_entries:
            msg = f"Oracle wheel {wheel_path.name} is missing vertebra/_oracle.pyi: {vtb_entries}"
            raise RuntimeError(msg)

        binaries = [
            name for name in vtb_entries if name.startswith("vertebra/_oracle") and name.endswith(NATIVE_SUFFIXES)
        ]
        if len(binaries) != 1:
            msg = f"Expected exactly 1 vertebra/_oracle shared library in {wheel_path.name}, found: {binaries}"
            raise RuntimeError(msg)

        unexpected = [
            name
            for name in vtb_entries
            if name not in {"vertebra/_oracle.pyi", binaries[0]} or name in SIBLING_STUBS or name.endswith(".py")
        ]
        if unexpected:
            msg = f"Oracle wheel {wheel_path.name} contains unexpected vertebra/ entries: {unexpected}"
            raise RuntimeError(msg)

        with tempfile.TemporaryDirectory(prefix="vtb-ora-link-") as tmp:
            extracted_lib = Path(zf.extract(binaries[0], path=tmp))
            assert_zero_native_linkage(extracted_lib, forbidden_libs=FORBIDDEN_NATIVE_LIBS, tech_label="Oracle")


def verify_oracle_sdist_contents(sdist_path: Path) -> None:
    """Assert `vertebra_oracle-*.tar.gz` contains the Rust crate manifest, build script, source, and stub."""
    with tarfile.open(sdist_path, "r:gz") as tf:
        names = tf.getnames()

    required_suffixes = ("Cargo.toml", "build.rs", "src/lib.rs", "vertebra/_oracle.pyi")
    missing = [suffix for suffix in required_suffixes if not any(name.endswith(suffix) for name in names)]
    if missing:
        msg = f"Oracle sdist {sdist_path.name} is missing required entries {missing}: {names}"
        raise RuntimeError(msg)


def verify_isolated_venv_install_and_fail_loud_contract(
    core_wheel: Path,
    oracle_wheel: Path,
) -> None:
    """Verify outside-checkout `vertebra-core` fails loud with `MissingDependencyError` until `vertebra-oracle` installs."""
    preinstall_code = (
        "import importlib.util, pathlib, sys\n"
        "import vertebra\n"
        "import vertebra.db.adapters.oracle as ora_pkg\n"
        "from vertebra.db.adapters.oracle.driver import AsyncOracleDriver, SyncOracleDriver\n"
        "from vertebra.typing import MissingDependencyError\n"
        "venv_root = pathlib.Path(sys.prefix).resolve()\n"
        "assert pathlib.Path(vertebra.__file__).resolve().is_relative_to(venv_root), vertebra.__file__\n"
        "assert pathlib.Path(ora_pkg.__file__).resolve().is_relative_to(venv_root), ora_pkg.__file__\n"
        "assert importlib.util.find_spec('vertebra._oracle') is None\n"
        "for cls in (SyncOracleDriver, AsyncOracleDriver):\n"
        "    try:\n"
        "        cls()\n"
        "    except MissingDependencyError as exc:\n"
        "        assert exc.package == 'vertebra-core[oracle]', exc.package\n"
        "        assert exc.install_package == 'vertebra-core[oracle]', exc.install_package\n"
        "        assert 'vertebra-core[oracle]' in exc.hint, exc.hint\n"
        "        assert 'vertebra-core[oracle]' in str(exc), str(exc)\n"
        "    else:\n"
        "        raise AssertionError(f'{cls.__name__} did not raise MissingDependencyError')\n"
    )
    postinstall_code = (
        "import pathlib, socket, sys\n"
        "import vertebra._oracle as mod\n"
        "from vertebra import _vertebra\n"
        "from vertebra.db.driver import ConnectionFailure, InterfaceError\n"
        "venv_root = pathlib.Path(sys.prefix).resolve()\n"
        "assert pathlib.Path(mod.__file__).resolve().is_relative_to(venv_root), mod.__file__\n"
        "assert mod.GIL_USED is False\n"
        "assert mod.ORACLESQL_NATIVE_TTC_PIPELINE_SUPPORTED is False\n"
        "assert not hasattr(_vertebra, 'SyncOracleClient')\n"
        "sync_client = mod.SyncOracleClient(host='localhost', service_name='FREEPDB1')\n"
        "async_client = mod.AsyncOracleClient(host='localhost', service_name='FREEPDB1')\n"
        "sync_pool = mod.SyncOraclePool(host='localhost', service_name='FREEPDB1', min_size=0, max_size=2)\n"
        "async_pool = mod.AsyncOraclePool(host='localhost', service_name='FREEPDB1', min_size=0, max_size=2)\n"
        "sync_client.set_session_attrs(client_identifier='wheel-check', module='vertebra')\n"
        "assert sync_pool.stats()['max_size'] == 2\n"
        "assert async_pool.stats()['max_size'] == 2\n"
        "try:\n"
        "    mod.SyncOracleClient(host='memory', service_name='FREEPDB1')\n"
        "except InterfaceError:\n"
        "    pass\n"
        "else:\n"
        "    raise AssertionError('SyncOracleClient accepted synthetic memory host')\n"
        "probe_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)\n"
        "probe_sock.bind(('127.0.0.1', 0))\n"
        "closed_port = int(probe_sock.getsockname()[1])\n"
        "probe_sock.close()\n"
        "dead_client = mod.SyncOracleClient(\n"
        "    host='127.0.0.1',\n"
        "    port=closed_port,\n"
        "    service_name='FREEPDB1',\n"
        "    connect_timeout=0.5,\n"
        ")\n"
        "try:\n"
        "    dead_client.connect()\n"
        "except ConnectionFailure as exc:\n"
        "    assert exc.sqlstate == '08006', exc.sqlstate\n"
        "    assert exc.is_transient is True\n"
        "else:\n"
        "    raise AssertionError('SyncOracleClient.connect() did not raise ConnectionFailure on dead port')\n"
        "finally:\n"
        "    dead_client.close()\n"
        "sync_client.close()\n"
        "sync_pool.close()\n"
        "assert sync_client.closed is True\n"
        "assert sync_pool.closed is True\n"
        "assert async_client.closed is False\n"
        "assert async_pool.closed is False\n"
    )
    verify_isolated_wheel_pair(
        temp_prefix="vtb-ora-venv-",
        core_wheel=core_wheel,
        ext_wheel=oracle_wheel,
        preinstall_code=preinstall_code,
        postinstall_code=postinstall_code,
    )


def main(argv: list[str] | None = None) -> int:
    """Build or accept `vertebra-core` and `vertebra-oracle` distributions and verify full wheel isolation."""
    parser = argparse.ArgumentParser(description="Verify standalone vertebra-oracle wheel isolation.")
    parser.add_argument("--core-wheel", type=Path, default=None, help="Prebuilt vertebra_core-*.whl to verify.")
    parser.add_argument("--oracle-wheel", type=Path, default=None, help="Prebuilt vertebra_oracle-*.whl to verify.")
    parser.add_argument("--oracle-sdist", type=Path, default=None, help="Prebuilt vertebra_oracle-*.tar.gz to verify.")
    args = parser.parse_args(argv)

    repo_root = Path(__file__).resolve().parents[2]
    with tempfile.TemporaryDirectory(prefix="vtb-oracle-wheel-check-") as tmp:
        out_dir = Path(tmp)
        core_wheel = args.core_wheel or build_dist(repo_root, repo_root, out_dir / "core", kind="wheel")
        oracle_wheel = args.oracle_wheel or build_dist(
            repo_root,
            repo_root / "src" / "rs" / "vertebra-oracle",
            out_dir / "oracle-whl",
            kind="wheel",
        )
        oracle_sdist = args.oracle_sdist or build_dist(
            repo_root,
            repo_root / "src" / "rs" / "vertebra-oracle",
            out_dir / "oracle-sdist",
            kind="sdist",
        )

        verify_core_wheel_excludes_oracle_binary_and_stub(core_wheel)
        verify_oracle_wheel_contents_and_pure_rust_linkage(oracle_wheel)
        verify_oracle_sdist_contents(oracle_sdist)
        verify_isolated_venv_install_and_fail_loud_contract(core_wheel, oracle_wheel)

    sys.stdout.write(
        "Verified vertebra-core and vertebra-oracle wheel isolation, pure-Rust linkage, and fail-loud contract.\n"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
