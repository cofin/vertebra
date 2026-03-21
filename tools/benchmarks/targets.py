from __future__ import annotations

import os
import shutil
import sys
from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class Target:
    """A server target to benchmark."""

    name: str
    cmd: list[str]
    host: str = "127.0.0.1"
    port: int = 8000
    env: dict[str, str] = field(default_factory=dict)
    description: str = ""
    category: str = "vertebra"


def _py(*args: str) -> list[str]:
    """Build a command list using the current Python interpreter."""
    return [sys.executable, *args]


def _vertebra_run(*args: str, workers: int | None = None, env_extra: dict[str, str] | None = None) -> list[str]:
    """Build a ``vtb run`` command using the console script in the current venv."""
    venv_bin = os.path.dirname(sys.executable)
    vtb = os.path.join(venv_bin, "vtb")
    if not os.path.isfile(vtb):
        found = shutil.which("vtb")
        if found:
            vtb = found
        else:
            msg = "vtb CLI not found — run `make install`"
            raise FileNotFoundError(msg)
    return [vtb, "run", *args, "--no-banner"]


def _ft_server_binary() -> str:
    """Return path to the 3.14t-linked vertebra-server binary.

    Built by ``make bench-ft`` via ``PYO3_PYTHON=... cargo build --target-dir target-ft``.
    """
    repo = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    binary = os.path.join(repo, "src", "rs", "target-ft", "release", "vertebra-server")
    return binary


def _ft_env() -> dict[str, str]:
    """Environment variables needed to run the 3.14t server binary."""
    uv = shutil.which("uv")
    if uv is None:
        return {}
    try:
        import subprocess

        py_path = subprocess.check_output([uv, "python", "find", "3.14t"], text=True).strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return {}
    lib_dir = os.path.join(os.path.dirname(py_path), "..", "lib")
    repo = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    src_py = os.path.join(repo, "src", "py")
    venv_ft = os.path.join(repo, ".venv-ft")

    # Find the site-packages inside .venv-ft for the 3.14t interpreter
    site_packages = ""
    if os.path.isdir(venv_ft):
        import glob

        matches = glob.glob(os.path.join(venv_ft, "lib", "python*", "site-packages"))
        if matches:
            site_packages = matches[0]

    paths = [p for p in [site_packages, src_py, repo] if p]
    return {
        "LD_LIBRARY_PATH": os.path.abspath(lib_dir),
        "PYTHONPATH": os.pathsep.join(paths),
    }


def _ft_run(*args: str) -> list[str]:
    """Build a command to run the 3.14t-linked vertebra-server binary."""
    binary = _ft_server_binary()
    if not os.path.isfile(binary):
        return ["echo", f"3.14t server binary not found at {binary}"]
    return [binary, *args]


# Port assignments:
#   8000-8002  - vertebra single-worker (native/asgi/wsgi)
#   8003-8005  - baselines (granian/uvicorn/spikard)
#   8010-8013  - vertebra multi-worker (current Python)
#   8020-8023  - vertebra free-threaded 3.14t

ALL_TARGETS: dict[str, Target] = {
    # ── Single-worker (default) ─────────────────────────────────────────
    "vertebra-native": Target(
        name="vertebra-native",
        cmd=_vertebra_run("tools.benchmarks.apps.vertebra_app:native_app", "--host", "127.0.0.1", "--port", "8000"),
        port=8000,
        description="Vertebra native (single worker)",
    ),
    "vertebra-asgi": Target(
        name="vertebra-asgi",
        cmd=_vertebra_run(
            "tools.benchmarks.apps.vertebra_app:asgi_app", "--asgi", "--host", "127.0.0.1", "--port", "8001"
        ),
        port=8001,
        description="Vertebra ASGI adapter",
    ),
    "vertebra-wsgi": Target(
        name="vertebra-wsgi",
        cmd=_vertebra_run("tools.benchmarks.apps.vertebra_app:wsgi_app", "--host", "127.0.0.1", "--port", "8002"),
        port=8002,
        description="Vertebra WSGI adapter",
    ),
    # ── Multi-worker (current Python) ───────────────────────────────────
    "vertebra-native-4w": Target(
        name="vertebra-native-4w",
        cmd=_vertebra_run("tools.benchmarks.apps.vertebra_app:native_app", "--host", "127.0.0.1", "--port", "8011"),
        port=8011,
        env={"VERTEBRA_WORKERS": "4"},
        description="Vertebra native, 4 workers",
    ),
    "vertebra-asgi-4w": Target(
        name="vertebra-asgi-4w",
        cmd=_vertebra_run(
            "tools.benchmarks.apps.vertebra_app:asgi_app", "--asgi", "--host", "127.0.0.1", "--port", "8012"
        ),
        port=8012,
        env={"VERTEBRA_WORKERS": "4"},
        description="Vertebra ASGI, 4 workers",
    ),
    "vertebra-wsgi-4w": Target(
        name="vertebra-wsgi-4w",
        cmd=_vertebra_run("tools.benchmarks.apps.vertebra_app:wsgi_app", "--host", "127.0.0.1", "--port", "8013"),
        port=8013,
        env={"VERTEBRA_WORKERS": "4"},
        description="Vertebra WSGI, 4 workers",
    ),
    # ── Free-threaded Python 3.14t ──────────────────────────────────────
    # These use a separate server binary linked against libpython3.14t,
    # built by `make bench-ft`. The _ft_env() provides LD_LIBRARY_PATH
    # and PYTHONPATH so the binary finds the right runtime and modules.
    "vertebra-ft-1w": Target(
        name="vertebra-ft-1w",
        cmd=_ft_run(),
        port=8020,
        env={
            **_ft_env(),
            "VERTEBRA_APP": "tools.benchmarks.apps.vertebra_app:native_app",
            "VERTEBRA_HOST": "127.0.0.1",
            "VERTEBRA_PORT": "8020",
        },
        description="Vertebra native, 1w, Python 3.14t",
        category="freethreaded",
    ),
    "vertebra-ft-4w": Target(
        name="vertebra-ft-4w",
        cmd=_ft_run(),
        port=8022,
        env={
            **_ft_env(),
            "VERTEBRA_APP": "tools.benchmarks.apps.vertebra_app:native_app",
            "VERTEBRA_HOST": "127.0.0.1",
            "VERTEBRA_PORT": "8022",
            "VERTEBRA_WORKERS": "4",
        },
        description="Vertebra native, 4w, Python 3.14t",
        category="freethreaded",
    ),
    "vertebra-ft-asgi-4w": Target(
        name="vertebra-ft-asgi-4w",
        cmd=_ft_run(),
        port=8021,
        env={
            **_ft_env(),
            "VERTEBRA_APP": "tools.benchmarks.apps.vertebra_app:asgi_app",
            "VERTEBRA_HOST": "127.0.0.1",
            "VERTEBRA_PORT": "8021",
            "VERTEBRA_WORKERS": "4",
        },
        description="Vertebra ASGI, 4w, Python 3.14t",
        category="freethreaded",
    ),
    "vertebra-ft-wsgi-4w": Target(
        name="vertebra-ft-wsgi-4w",
        cmd=_ft_run(),
        port=8023,
        env={
            **_ft_env(),
            "VERTEBRA_APP": "tools.benchmarks.apps.vertebra_app:wsgi_app",
            "VERTEBRA_HOST": "127.0.0.1",
            "VERTEBRA_PORT": "8023",
            "VERTEBRA_WORKERS": "4",
        },
        description="Vertebra WSGI, 4w, Python 3.14t",
        category="freethreaded",
    ),
    # ── Baselines ───────────────────────────────────────────────────────
    "granian-asgi": Target(
        name="granian-asgi",
        cmd=_py(
            "-m",
            "granian",
            "tools.benchmarks.apps.asgi:app",
            "--interface",
            "asgi",
            "--host",
            "127.0.0.1",
            "--port",
            "8003",
        ),
        port=8003,
        description="Granian ASGI",
        category="baseline",
    ),
    "uvicorn-httptools": Target(
        name="uvicorn-httptools",
        cmd=_py(
            "-m",
            "uvicorn",
            "tools.benchmarks.apps.asgi:app",
            "--host",
            "127.0.0.1",
            "--port",
            "8004",
            "--http",
            "httptools",
            "--loop",
            "uvloop",
        ),
        port=8004,
        description="Uvicorn httptools",
        category="baseline",
    ),
    "spikard": Target(
        name="spikard",
        cmd=_py("-m", "tools.benchmarks.apps.spikard_app"),
        port=8005,
        env={"SPIKARD_HOST": "127.0.0.1", "SPIKARD_PORT": "8005"},
        description="Spikard",
        category="baseline",
    ),
}

VERTEBRA_TARGETS: list[str] = [name for name, t in ALL_TARGETS.items() if t.category == "vertebra"]
BASELINE_TARGETS: list[str] = [name for name, t in ALL_TARGETS.items() if t.category == "baseline"]
FREETHREADED_TARGETS: list[str] = [name for name, t in ALL_TARGETS.items() if t.category == "freethreaded"]
