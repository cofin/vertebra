"""Server environment assembly for CLI run and dev commands."""

import os
import sys
from collections.abc import Mapping

import msgspec

__all__ = ("ServerLaunchEnv",)


class ServerLaunchEnv(msgspec.Struct, frozen=True, gc=False):
    """Server environment configuration for running or developing Vertebra applications."""

    app_path: str
    host: str
    port: int
    workers: int
    log_level: str
    dev: bool = False
    native: bool = True

    def to_environ(self, base: Mapping[str, str] | None = None) -> dict[str, str]:
        """Assemble environment variables dictionary."""
        env = dict(base) if base is not None else os.environ.copy()
        env["VERTEBRA_APP"] = self.app_path
        env["LITESTAR_APP"] = self.app_path
        env["VERTEBRA_PYTHON_APP"] = self.app_path
        env["VERTEBRA_PYTHON_BIN"] = sys.executable

        cwd = os.getcwd()
        existing = env.get("PYTHONPATH", "")
        env["PYTHONPATH"] = f"{cwd}{os.pathsep}{existing}" if existing else cwd

        env["VERTEBRA_HOST"] = self.host
        env["VERTEBRA_PORT"] = str(self.port)
        env["VERTEBRA_WORKERS"] = str(self.workers)
        env["VERTEBRA_LOG_LEVEL"] = self.log_level
        env["VERTEBRA_NATIVE"] = "1" if self.native else "0"

        if self.dev:
            env["VERTEBRA_DEV"] = "1"

        return env
