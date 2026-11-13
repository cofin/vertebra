"""Build and verify standalone vertebra-postgres wheel isolation and pure-Rust linkage."""

from __future__ import annotations

import argparse
import sys
import tempfile
from pathlib import Path

from tools.scripts._wheel_verify import (
    build_dist,
    verify_core_wheel_excludes,
    verify_extension_wheel_contents,
    verify_isolated_wheel_pair,
    verify_sdist_contents,
)

__all__ = (
    "FORBIDDEN_NATIVE_LIBS",
    "POSTGRES_EXPORTED_CLASSES",
    "main",
    "verify_core_wheel_excludes_postgres_binary_and_stub",
    "verify_isolated_venv_install_and_fail_loud_contract",
    "verify_postgres_sdist_contents",
    "verify_postgres_wheel_contents",
)

FORBIDDEN_NATIVE_LIBS: tuple[str, ...] = (
    "libpq",
    "libecpg",
)
POSTGRES_EXPORTED_CLASSES: tuple[str, ...] = (
    "SyncPostgresClient",
    "AsyncPostgresClient",
    "SyncPostgresPool",
    "AsyncPostgresPool",
    "SyncPostgresPortal",
    "AsyncPostgresPortal",
    "SyncPostgresArrowStream",
    "AsyncPostgresArrowStream",
    "SyncPostgresCopyOut",
    "SyncPostgresCopyIn",
    "AsyncPostgresCopyOut",
    "AsyncPostgresCopyIn",
    "AsyncPostgresListener",
    "AsyncPostgresReplication",
)


def verify_core_wheel_excludes_postgres_binary_and_stub(wheel_path: Path) -> None:
    """Assert `vertebra_core-*.whl` packs the Python PostgreSQL adapter without `_postgres` binary or stub."""
    verify_core_wheel_excludes(
        wheel_path,
        required_entries=(
            "vertebra/db/adapters/postgres/driver.py",
            "vertebra/db/adapters/postgres/_sync.py",
            "vertebra/db/adapters/postgres/_async.py",
        ),
        stub_entry="vertebra/_postgres.pyi",
        binary_prefix="vertebra/_postgres",
    )


def verify_postgres_wheel_contents(wheel_path: Path) -> None:
    """Assert `vertebra_postgres-*.whl` ships only `_postgres` binary + stub with zero `libpq` linkage."""
    verify_extension_wheel_contents(
        wheel_path,
        label="PostgreSQL",
        stub_entry="vertebra/_postgres.pyi",
        binary_prefix="vertebra/_postgres",
        forbidden_libs=FORBIDDEN_NATIVE_LIBS,
        link_temp_prefix="vtb-pg-link-",
    )


def verify_postgres_sdist_contents(sdist_path: Path) -> None:
    """Assert `vertebra_postgres-*.tar.gz` contains crate manifest, build script, source, and stub."""
    verify_sdist_contents(
        sdist_path,
        label="PostgreSQL",
        required_suffixes=("Cargo.toml", "build.rs", "src/lib.rs", "vertebra/_postgres.pyi"),
    )


def verify_isolated_venv_install_and_fail_loud_contract(
    core_wheel: Path,
    postgres_wheel: Path,
) -> None:
    """Verify outside-checkout `vertebra-core` fails loud until `vertebra-postgres` installs."""
    classes_literal = ", ".join(repr(name) for name in POSTGRES_EXPORTED_CLASSES)
    preinstall_code = (
        "import asyncio, importlib.util, pathlib, sys\n"
        "import vertebra\n"
        "import vertebra.db.adapters.postgres as pg_pkg\n"
        "from vertebra.db.adapters.postgres import AsyncPostgreSQLDriver, SyncPostgreSQLDriver\n"
        "from vertebra.framework.queues.postgres import PostgreSQLQueueBackend\n"
        "from vertebra.typing import MissingDependencyError\n"
        "venv_root = pathlib.Path(sys.prefix).resolve()\n"
        "assert pathlib.Path(vertebra.__file__).resolve().is_relative_to(venv_root), vertebra.__file__\n"
        "assert pathlib.Path(pg_pkg.__file__).resolve().is_relative_to(venv_root), pg_pkg.__file__\n"
        "assert importlib.util.find_spec('vertebra._postgres') is None\n"
        "def _expect_missing(label, probe):\n"
        "    try:\n"
        "        probe()\n"
        "    except MissingDependencyError as exc:\n"
        "        assert exc.package == 'vertebra-core[postgres]', exc.package\n"
        "        assert exc.install_package == 'vertebra-core[postgres]', exc.install_package\n"
        "        assert 'vertebra-core[postgres]' in exc.hint, exc.hint\n"
        "        assert 'vertebra-core[postgres]' in str(exc), str(exc)\n"
        "    else:\n"
        "        raise AssertionError(f'{label} did not raise MissingDependencyError')\n"
        "_expect_missing('SyncPostgreSQLDriver.connect', lambda: SyncPostgreSQLDriver().connect())\n"
        "_expect_missing('AsyncPostgreSQLDriver.connect', lambda: asyncio.run(AsyncPostgreSQLDriver().connect()))\n"
        "_expect_missing('PostgreSQLQueueBackend', lambda: PostgreSQLQueueBackend())\n"
    )
    postinstall_code = (
        "import pathlib, socket, sys\n"
        "import vertebra._postgres as mod\n"
        "from vertebra import _vertebra\n"
        "from vertebra.db.driver import ConnectionFailure\n"
        f"expected_classes = ({classes_literal})\n"
        "venv_root = pathlib.Path(sys.prefix).resolve()\n"
        "assert pathlib.Path(mod.__file__).resolve().is_relative_to(venv_root), mod.__file__\n"
        "assert mod.GIL_USED is False\n"
        "assert isinstance(mod.FREE_THREADED, bool)\n"
        "assert isinstance(mod.__version__, str) and mod.__version__\n"
        "for cls_name in expected_classes:\n"
        "    cls = getattr(mod, cls_name, None)\n"
        "    assert isinstance(cls, type), f'Missing _postgres class {cls_name}'\n"
        "    assert not hasattr(_vertebra, cls_name), f'_vertebra leaked {cls_name}'\n"
        "sync_client = mod.SyncPostgresClient(user='postgres', database='postgres', sslmode='disable')\n"
        "async_client = mod.AsyncPostgresClient(user='postgres', database='postgres', sslmode='disable')\n"
        "sync_pool = mod.SyncPostgresPool(user='postgres', database='postgres', sslmode='disable', min_size=0, max_size=2)\n"
        "async_pool = mod.AsyncPostgresPool(user='postgres', database='postgres', sslmode='disable', min_size=0, max_size=2)\n"
        "sync_pool.validate_configuration(user='postgres', database='postgres', sslmode='disable', min_size=0, max_size=4)\n"
        "async_pool.validate_configuration(user='postgres', database='postgres', sslmode='disable', min_size=0, max_size=4)\n"
        "assert sync_pool.closed is False and async_pool.closed is False\n"
        "try:\n"
        "    mod.SyncPostgresClient(user='postgres', database='postgres', unix_socket='/tmp/.s.PGSQL.5432', sslmode='require')\n"
        "except ValueError:\n"
        "    pass\n"
        "else:\n"
        "    raise AssertionError('SyncPostgresClient accepted unix_socket with sslmode=require')\n"
        "probe_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)\n"
        "probe_sock.bind(('127.0.0.1', 0))\n"
        "closed_port = int(probe_sock.getsockname()[1])\n"
        "probe_sock.close()\n"
        "dead_client = mod.SyncPostgresClient(\n"
        "    user='postgres',\n"
        "    database='postgres',\n"
        "    host='127.0.0.1',\n"
        "    port=closed_port,\n"
        "    sslmode='disable',\n"
        "    connect_timeout=0.5,\n"
        ")\n"
        "try:\n"
        "    dead_client.connect()\n"
        "except ConnectionFailure as exc:\n"
        "    assert exc.is_transient is True\n"
        "else:\n"
        "    raise AssertionError('SyncPostgresClient.connect() did not raise ConnectionFailure on dead port')\n"
        "finally:\n"
        "    dead_client.close()\n"
        "sync_client.close()\n"
        "sync_pool.close()\n"
        "assert sync_client.closed is True\n"
        "assert sync_pool.closed is True\n"
        "assert async_client.closed is True\n"
        "assert async_pool.closed is False\n"
    )
    verify_isolated_wheel_pair(
        temp_prefix="vtb-pg-venv-",
        core_wheel=core_wheel,
        ext_wheel=postgres_wheel,
        preinstall_code=preinstall_code,
        postinstall_code=postinstall_code,
    )


def main(argv: list[str] | None = None) -> int:
    """Build or accept `vertebra-core` and `vertebra-postgres` distributions and verify wheel isolation."""
    parser = argparse.ArgumentParser(description="Verify standalone vertebra-postgres wheel isolation.")
    parser.add_argument("--core-wheel", type=Path, default=None, help="Prebuilt vertebra_core-*.whl to verify.")
    parser.add_argument("--postgres-wheel", type=Path, default=None, help="Prebuilt vertebra_postgres-*.whl to verify.")
    parser.add_argument(
        "--postgres-sdist", type=Path, default=None, help="Prebuilt vertebra_postgres-*.tar.gz to verify."
    )
    args = parser.parse_args(argv)

    repo_root = Path(__file__).resolve().parents[2]
    with tempfile.TemporaryDirectory(prefix="vtb-postgres-wheel-check-") as tmp:
        out_dir = Path(tmp)
        core_wheel = args.core_wheel or build_dist(repo_root, repo_root, out_dir / "core", kind="wheel")
        postgres_wheel = args.postgres_wheel or build_dist(
            repo_root,
            repo_root / "src" / "rs" / "vertebra-postgres",
            out_dir / "postgres-whl",
            kind="wheel",
        )
        postgres_sdist = args.postgres_sdist or build_dist(
            repo_root,
            repo_root / "src" / "rs" / "vertebra-postgres",
            out_dir / "postgres-sdist",
            kind="sdist",
        )

        verify_core_wheel_excludes_postgres_binary_and_stub(core_wheel)
        verify_postgres_wheel_contents(postgres_wheel)
        verify_postgres_sdist_contents(postgres_sdist)
        verify_isolated_venv_install_and_fail_loud_contract(core_wheel, postgres_wheel)

    sys.stdout.write(
        "Verified vertebra-core and vertebra-postgres wheel isolation, pure-Rust linkage, and 14-class contract.\n"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
