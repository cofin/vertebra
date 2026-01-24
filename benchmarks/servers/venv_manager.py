"""Benchmark virtual environment manager.

Creates and manages a separate venv for benchmark dependencies to avoid
polluting the main project environment.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

# Benchmark venv location
BENCH_VENV_DIR = Path(__file__).parent.parent / ".venv"
BENCH_VENV_PYTHON = BENCH_VENV_DIR / "bin" / "python"
BENCH_VENV_PIP = BENCH_VENV_DIR / "bin" / "pip"


def venv_exists() -> bool:
    """Check if benchmark venv exists."""
    return BENCH_VENV_PYTHON.exists()


def create_venv() -> None:
    """Create the benchmark virtual environment."""
    if venv_exists():
        print(f"Benchmark venv already exists at {BENCH_VENV_DIR}")
        return

    print(f"Creating benchmark venv at {BENCH_VENV_DIR}...")
    subprocess.run([sys.executable, "-m", "venv", str(BENCH_VENV_DIR)], check=True)
    print("Benchmark venv created successfully")


def install_deps(packages: list[str] | None = None) -> None:
    """Install benchmark dependencies into the venv.

    Args:
        packages: Specific packages to install, or None for all benchmark deps
    """
    if not venv_exists():
        create_venv()

    if packages is None:
        # Default benchmark dependencies
        packages = [
            "httpx",  # For health checks
            "uvicorn[standard]",  # Uvicorn with uvloop + httptools
            "granian",  # Rust ASGI server
            "hypercorn[uvloop]",  # HTTP/2 capable server
            "daphne",  # Django Channels server
            "starlette",  # Minimal framework
            "fastapi",  # Feature-rich framework
            "blacksheep",  # High-performance framework
            "litestar",  # msgspec-powered framework
            "sanic",  # Standalone + ASGI modes
        ]

    print(f"Installing packages: {', '.join(packages)}")
    subprocess.run([str(BENCH_VENV_PIP), "install", "--upgrade", "pip"], check=True, capture_output=True)
    subprocess.run([str(BENCH_VENV_PIP), "install", *packages], check=True)
    print("Dependencies installed successfully")


def get_venv_python() -> str:
    """Get path to benchmark venv Python executable."""
    if not venv_exists():
        msg = f"Benchmark venv not found at {BENCH_VENV_DIR}. Run 'make bench-setup' first."
        raise RuntimeError(msg)
    return str(BENCH_VENV_PYTHON)


def get_venv_env() -> dict[str, str]:
    """Get environment variables for running in benchmark venv."""
    env = os.environ.copy()
    env["VIRTUAL_ENV"] = str(BENCH_VENV_DIR)
    env["PATH"] = f"{BENCH_VENV_DIR / 'bin'}:{env.get('PATH', '')}"
    # Remove any existing venv from PATH to avoid conflicts
    return env


def run_in_venv(cmd: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
    """Run a command in the benchmark venv.

    Args:
        cmd: Command to run (first element should be executable name)
        **kwargs: Additional arguments to subprocess.run

    Returns:
        CompletedProcess result
    """
    env = get_venv_env()
    return subprocess.run(cmd, env=env, check=False, **kwargs)


def _cmd_create(_args: object) -> int:
    """Handle 'create' command."""
    create_venv()
    return 0


def _cmd_install(args: object) -> int:
    """Handle 'install' command."""
    packages = getattr(args, "packages", None)
    install_deps(packages if packages else None)
    return 0


def _cmd_path(_args: object) -> int:
    """Handle 'path' command."""
    if venv_exists():
        print(get_venv_python())
        return 0
    print("Benchmark venv not found", file=sys.stderr)
    return 1


def _cmd_check(_args: object) -> int:
    """Handle 'check' command."""
    if venv_exists():
        print(f"Benchmark venv exists at {BENCH_VENV_DIR}")
        return 0
    print(f"Benchmark venv not found at {BENCH_VENV_DIR}")
    return 1


def main() -> int:
    """CLI for managing benchmark venv."""
    import argparse

    parser = argparse.ArgumentParser(description="Benchmark venv manager")
    subparsers = parser.add_subparsers(dest="command", help="Commands")

    # create command
    subparsers.add_parser("create", help="Create benchmark venv")

    # install command
    install_parser = subparsers.add_parser("install", help="Install dependencies")
    install_parser.add_argument(
        "packages", nargs="*", help="Specific packages to install (default: all benchmark deps)"
    )

    # path command
    subparsers.add_parser("path", help="Print path to venv Python")

    # check command
    subparsers.add_parser("check", help="Check if venv exists")

    args = parser.parse_args()

    commands = {"create": _cmd_create, "install": _cmd_install, "path": _cmd_path, "check": _cmd_check}

    if args.command in commands:
        return commands[args.command](args)

    parser.print_help()
    return 1


if __name__ == "__main__":
    sys.exit(main())
