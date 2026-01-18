"""Container runtime abstraction for Docker and Podman.

Provides a unified interface for container operations and auto-detects
Docker or Podman at runtime. Used by integration tests to avoid hard
dependencies on docker compose.
"""

from __future__ import annotations

import shutil
import subprocess
from enum import Enum


class RuntimeType(str, Enum):
    """Container runtime types."""

    DOCKER = "docker"
    PODMAN = "podman"
    NONE = "none"


class ContainerRuntime:
    """Unified interface for Docker and Podman container operations."""

    def __init__(self) -> None:
        self._runtime_type = self.detect_runtime()
        self._command = self._runtime_type.value if self._runtime_type != RuntimeType.NONE else None

    @staticmethod
    def detect_runtime() -> RuntimeType:
        """Detect which container runtime is available."""
        if shutil.which("docker"):
            try:
                result = subprocess.run(["docker", "--version"], capture_output=True, timeout=5, check=False)
                if result.returncode == 0:
                    return RuntimeType.DOCKER
            except (subprocess.TimeoutExpired, FileNotFoundError):
                pass

        if shutil.which("podman"):
            try:
                result = subprocess.run(["podman", "--version"], capture_output=True, timeout=5, check=False)
                if result.returncode == 0:
                    return RuntimeType.PODMAN
            except (subprocess.TimeoutExpired, FileNotFoundError):
                pass

        return RuntimeType.NONE

    def is_available(self) -> bool:
        """Return True if a container runtime is available."""
        return self._runtime_type != RuntimeType.NONE

    def get_runtime_type(self) -> RuntimeType:
        """Return the detected runtime type."""
        return self._runtime_type

    def get_runtime_command(self) -> str:
        """Return the base runtime command."""
        if not self.is_available():
            msg = (
                "No container runtime available. Please install Docker or Podman.\n"
                "Docker: https://docs.docker.com/get-docker/\n"
                "Podman: https://podman.io/getting-started/installation"
            )
            raise NoRuntimeAvailableError(msg)
        return self._command  # type: ignore[return-value]

    def run_command(
        self, args: list[str], *, capture_output: bool = True, check: bool = True, timeout: int | None = None
    ) -> tuple[int, str, str]:
        """Run a container runtime command."""
        cmd = self.get_runtime_command()
        full_cmd = [cmd, *args]

        result = subprocess.run(full_cmd, capture_output=capture_output, timeout=timeout, check=check, text=True)
        stdout = result.stdout if capture_output else ""
        stderr = result.stderr if capture_output else ""
        return result.returncode, stdout, stderr

    def container_exists(self, container_name: str) -> bool:
        """Return True if a container exists."""
        try:
            _, stdout, _ = self.run_command(
                ["ps", "-a", "--filter", f"name=^{container_name}$", "--format", "{{.Names}}"], check=False
            )
            return container_name in stdout.strip()
        except (subprocess.CalledProcessError, NoRuntimeAvailableError):
            return False

    def container_running(self, container_name: str) -> bool:
        """Return True if a container is running."""
        try:
            _, stdout, _ = self.run_command(
                ["ps", "--filter", f"name=^{container_name}$", "--format", "{{.Names}}"], check=False
            )
            return container_name in stdout.strip()
        except (subprocess.CalledProcessError, NoRuntimeAvailableError):
            return False

    def list_containers(self, all: bool = True) -> list[str]:
        """List all containers."""
        args = ["ps", "--format", "{{.Names}}"]
        if all:
            args.append("-a")
        _, stdout, _ = self.run_command(args, check=False)
        return stdout.strip().split("\n") if stdout else []

    def volume_exists(self, volume_name: str) -> bool:
        """Return True if a volume exists."""
        try:
            _, stdout, _ = self.run_command(
                ["volume", "ls", "--filter", f"name=^{volume_name}$", "--format", "{{.Name}}"], check=False
            )
            return volume_name in stdout.strip()
        except (subprocess.CalledProcessError, NoRuntimeAvailableError):
            return False


class ContainerRuntimeError(Exception):
    """Base exception for container runtime errors."""


class NoRuntimeAvailableError(ContainerRuntimeError):
    """Raised when no container runtime is detected."""
