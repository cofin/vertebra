"""Command line interface for Vertebra."""

import argparse
import os
import shutil
from pathlib import Path

from vertebra.supervisor import build_broker_command, build_bun_command, run_supervisor

BANNER = r"""
   __      __          __          __
  / /   __/ /____ ____/ /____ ____/ /_
 / /__ / _  / _ `/ _  / __/ _ `/ _  / /
/____//_,_/\_,_/\_,_/\__/\_,_/\_,_/_/
"""


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="vertebra", description="Vertebra CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    serve_parser = subparsers.add_parser("serve", help="Run the Bun-backed ASGI server")
    serve_parser.add_argument("app", help="ASGI app path (module:attr)")
    serve_parser.add_argument("--host", default="127.0.0.1")
    serve_parser.add_argument("--port", type=int, default=8000)
    serve_parser.add_argument("--workers", type=int, default=1)
    serve_parser.add_argument("--config", dest="config_path")
    serve_parser.add_argument("--log-level", default="info")
    serve_parser.add_argument("--broker-config", dest="broker_config_path")

    args = parser.parse_args(argv)
    if args.command == "serve":
        return serve(
            app=args.app,
            host=args.host,
            port=args.port,
            workers=args.workers,
            config_path=args.config_path,
            log_level=args.log_level,
            broker_config_path=args.broker_config_path,
        )
    return 0


def serve(
    *,
    app: str,
    host: str,
    port: int,
    workers: int,
    config_path: str | None,
    log_level: str,
    broker_config_path: str | None,
) -> int:
    bun = shutil.which("bun")
    if not bun:
        raise SystemExit("bun binary not found; install Bun or add it to PATH")

    env = os.environ.copy()
    env["VERTEBRA_PYTHON_APP"] = app
    env["VERTEBRA_HOST"] = host
    env["VERTEBRA_PORT"] = str(port)
    env["VERTEBRA_WORKERS"] = str(workers)
    env["VERTEBRA_LOG_LEVEL"] = log_level
    if config_path:
        env["VERTEBRA_CONFIG"] = config_path
    broker_config = broker_config_path or env.get("VERTEBRA_BROKER_CONFIG") or "vertebra-broker.toml"
    env["VERTEBRA_BROKER_CONFIG"] = broker_config

    engine = find_engine()
    engine_path = Path(engine)
    if engine_path.name == "vertebra-engine":
        bun_cmd = [str(engine_path)]
    else:
        bun_cmd = [bun, engine]

    broker_cmd = build_broker_command(broker_config)

    print_banner(host=host, port=port, workers=workers, log_level=log_level)
    return run_supervisor(bun_cmd=bun_cmd, broker_cmd=broker_cmd, broker_config=broker_config, env=env)


def find_engine() -> str:
    package_root = Path(__file__).resolve().parent
    bundled = package_root / "bin" / "vertebra-engine"
    if bundled.exists():
        return str(bundled)
    repo_root = find_repo_root(package_root)
    if repo_root:
        entry = repo_root / "src" / "ts" / "src" / "server" / "serve.ts"
        if entry.exists():
            return str(entry)
    raise SystemExit("Unable to locate vertebra engine; expected bundled binary or source entrypoint")


def find_repo_root(start: Path) -> Path | None:
    for parent in [start, *start.parents]:
        if (parent / "pyproject.toml").exists():
            return parent
    return None


def print_banner(*, host: str, port: int, workers: int, log_level: str) -> None:
    print(BANNER)
    print(f"Vertebra v0.1.0  |  http://{host}:{port}")
    print(f"Workers: {workers}  |  Log: {log_level}")


if __name__ == "__main__":
    raise SystemExit(main())
