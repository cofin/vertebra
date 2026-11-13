"""Standalone relocatable directory builder for Vertebra applications."""

import py_compile
import shutil
from pathlib import Path

from vertebra.build.packaging.targets import BuildTarget
from vertebra.utils.serializers import format_json

__all__ = ("StandaloneBuilder",)

_EXCLUDED_DIRS: frozenset[str] = frozenset(
    {
        ".git",
        ".hg",
        ".jj",
        ".venv",
        ".venv-ft",
        ".mypy_cache",
        ".pytest_cache",
        ".ruff_cache",
        "node_modules",
        "dist",
        "build",
    }
)


class StandaloneBuilder:
    """Build a standalone relocatable directory distribution for a Vertebra application."""

    __slots__ = (
        "entrypoint",
        "include_agent_skills",
        "profile",
        "project_dir",
        "server_binary",
        "target",
    )

    def __init__(
        self,
        project_dir: Path,
        target: BuildTarget,
        entrypoint: str = "app:app",
        *,
        server_binary: Path | None = None,
        profile: str = "release",
        include_agent_skills: bool = False,
    ) -> None:
        self.project_dir = project_dir
        self.target = target
        self.entrypoint = entrypoint
        self.server_binary = server_binary
        self.profile = profile
        self.include_agent_skills = include_agent_skills

    @staticmethod
    def _is_vertebra_repo_root(project_root: Path) -> bool:
        """Return True when ``project_root`` is Vertebra's own framework source repository."""
        return (project_root / "src" / "rs" / "vertebra-core").exists() and (
            project_root / "src" / "py" / "vertebra"
        ).exists()

    def build(self, output_dir: Path) -> Path:
        """Stage application sources, compile hash-checked .pyc files, and write manifest.json and __main__.py.

        Bytecode uses `PycInvalidationMode.CHECKED_HASH` and app-relative source paths, so `.pyc`
        files carry neither mtimes nor build-machine paths and repeated builds of the same tree
        produce identical bytes.
        Lockfiles (`*.lock`, `pylock.toml`, `pyproject.toml`) are staged as-is for whichever
        package manager the project uses.
        """
        project_root = self.project_dir.resolve()
        if not project_root.exists():
            msg = f"Project directory does not exist: {project_root}"
            raise FileNotFoundError(msg)
        if self.include_agent_skills and self._is_vertebra_repo_root(project_root):
            msg = "Refusing to bundle Vertebra repository's own .agents/skills into an application distribution."
            raise ValueError(msg)

        dist_root = output_dir.resolve()
        if dist_root.exists():
            shutil.rmtree(dist_root)
        app_stage = dist_root / "app"
        app_stage.mkdir(parents=True, exist_ok=True)

        for child in sorted(project_root.iterdir()):
            if child.name in _EXCLUDED_DIRS or child.name.startswith("."):
                continue
            dest = app_stage / child.name
            if child.is_dir():
                shutil.copytree(
                    child,
                    dest,
                    ignore=shutil.ignore_patterns(
                        "__pycache__",
                        "*.pyc",
                        "*.pyo",
                        ".pytest_cache",
                        "node_modules",
                        "target",
                    ),
                )
            elif child.suffix in {".py", ".toml", ".json", ".lock"}:
                shutil.copy2(child, dest)

        compiled_count = 0
        for py_file in sorted(app_stage.rglob("*.py")):
            cfile = py_file.with_suffix(".pyc")
            py_compile.compile(
                str(py_file),
                cfile=str(cfile),
                dfile=py_file.relative_to(app_stage).as_posix(),
                doraise=True,
                invalidation_mode=py_compile.PycInvalidationMode.CHECKED_HASH,
            )
            compiled_count += 1

        agent_skills_staged = False
        if self.include_agent_skills:
            skills_src = project_root / ".agents" / "skills"
            if skills_src.is_dir():
                skills_dest = app_stage / ".agents" / "skills"
                skills_dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.copytree(
                    skills_src,
                    skills_dest,
                    ignore=shutil.ignore_patterns("__pycache__", "*.pyc", ".DS_Store"),
                )
                agent_skills_staged = True

        staged_binary_rel: str | None = None
        if self.server_binary is not None:
            binary_src = self.server_binary.resolve()
            if not binary_src.is_file():
                msg = f"Server binary does not exist: {binary_src}"
                raise FileNotFoundError(msg)
            bin_dir = dist_root / "bin"
            bin_dir.mkdir(parents=True, exist_ok=True)
            staged_bin = bin_dir / binary_src.name
            shutil.copy2(binary_src, staged_bin)
            staged_bin.chmod(0o755)
            staged_binary_rel = f"bin/{binary_src.name}"

        manifest: dict[str, object] = {
            "entrypoint": self.entrypoint,
            "profile": self.profile,
            "target": {
                "os": self.target.os_name,
                "arch": self.target.arch,
                "libc": self.target.libc,
                "python_version": self.target.python_version,
                "rust_triple": self.target.rust_triple,
                "pbs_url": self.target.pbs_url,
            },
            "compiled_modules": compiled_count,
        }
        if staged_binary_rel is not None:
            manifest["server_binary"] = staged_binary_rel
        if agent_skills_staged:
            manifest["agent_skills_staged"] = True

        formatted = format_json(manifest, indent=2)
        (dist_root / "manifest.json").write_text(formatted + "\n")
        (dist_root / "build-manifest.json").write_text(formatted + "\n")
        (dist_root / "__main__.py").write_text(self._render_bootstrap(server_binary_rel=staged_binary_rel))
        return dist_root

    def _render_bootstrap(self, *, server_binary_rel: str | None = None) -> str:
        module_name, _, attr_name = self.entrypoint.partition(":")
        target_attr = attr_name or "app"
        entrypoint_spec = f"{module_name}:{target_attr}"
        server_env_line = (
            f'    os.environ.setdefault("VERTEBRA_SERVER_BINARY", str(root_dir / {server_binary_rel!r}))\n'
            if server_binary_rel is not None
            else ""
        )
        return (
            '"""Auto-generated Vertebra standalone bootstrap."""\n\n'
            "import importlib\n"
            "import inspect\n"
            "import os\n"
            "from pathlib import Path\n"
            "import subprocess\n"
            "import sys\n\n\n"
            "def _accepts_zero_positional_args(fn: object) -> bool:\n"
            "    if not callable(fn) or hasattr(fn, '__rsgi__'):\n"
            "        return False\n"
            "    if inspect.iscoroutinefunction(fn) or (\n"
            "        not inspect.isfunction(fn)\n"
            "        and not inspect.ismethod(fn)\n"
            "        and inspect.iscoroutinefunction(getattr(fn, '__call__', None))\n"
            "    ):\n"
            "        return False\n"
            "    try:\n"
            "        sig = inspect.signature(fn)\n"
            "    except (TypeError, ValueError):\n"
            "        return False\n"
            "    for param in sig.parameters.values():\n"
            "        if (\n"
            "            param.kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)\n"
            "            and param.default is inspect.Parameter.empty\n"
            "        ):\n"
            "            return False\n"
            "    return True\n\n\n"
            "def _launch_server_for_app(app_dir: Path, entrypoint_spec: str) -> int:\n"
            '    server_bin = os.environ.get("VERTEBRA_SERVER_BINARY", "")\n'
            "    if not server_bin:\n"
            "        try:\n"
            "            from vertebra.supervisor._supervisor import find_server_binary\n\n"
            "            server_bin = find_server_binary()\n"
            "        except (ImportError, SystemExit):\n"
            '            server_bin = ""\n'
            "    if not server_bin or not Path(server_bin).is_file():\n"
            '        sys.stderr.write("vertebra-server binary not found; bundle with --server-binary or set VERTEBRA_SERVER_BINARY\\n")\n'
            "        return 1\n"
            "    srv_env = os.environ.copy()\n"
            '    srv_env.setdefault("VERTEBRA_APP", entrypoint_spec)\n'
            '    srv_env.setdefault("VERTEBRA_APP_ROOT", str(app_dir))\n'
            '    existing_py_path = srv_env.get("PYTHONPATH", "")\n'
            "    srv_env['PYTHONPATH'] = (\n"
            '        f"{app_dir}{os.pathsep}{existing_py_path}" if existing_py_path else str(app_dir)\n'
            "    )\n"
            "    return subprocess.run([server_bin, *sys.argv[1:]], env=srv_env, check=False).returncode\n\n\n"
            "def main() -> int:\n"
            "    root_dir = Path(__file__).resolve().parent\n"
            '    app_dir = root_dir / "app"\n'
            '    os.environ.setdefault("VERTEBRA_APP_ROOT", str(app_dir))\n'
            f"{server_env_line}"
            "    sys.path.insert(0, str(app_dir))\n"
            f"    module = importlib.import_module({module_name!r})\n"
            f"    target = getattr(module, {target_attr!r})\n"
            "    if _accepts_zero_positional_args(target):\n"
            "        ret = target()\n"
            "        return ret if isinstance(ret, int) else 0\n"
            "    runner = None if inspect.isclass(target) else getattr(target, 'run', None)\n"
            "    if _accepts_zero_positional_args(runner):\n"
            "        ret = runner()\n"
            "        return ret if isinstance(ret, int) else 0\n"
            f"    return _launch_server_for_app(app_dir, {entrypoint_spec!r})\n\n\n"
            'if __name__ == "__main__":\n'
            "    sys.exit(main())\n"
        )
