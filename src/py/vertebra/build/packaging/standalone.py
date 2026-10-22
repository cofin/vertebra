"""Standalone relocatable directory builder for Vertebra applications."""

import py_compile
import shutil
from pathlib import Path

from vertebra.build.packaging.pgo import RustPgoConfig, detect_python_pgo_lto
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

    __slots__ = ("entrypoint", "pgo_enabled", "profile", "project_dir", "target")

    def __init__(
        self,
        project_dir: Path,
        target: BuildTarget,
        entrypoint: str = "app:app",
        *,
        pgo_enabled: bool = False,
        profile: str = "release",
    ) -> None:
        self.project_dir = project_dir
        self.target = target
        self.entrypoint = entrypoint
        self.pgo_enabled = pgo_enabled
        self.profile = profile

    def build(self, output_dir: Path) -> Path:
        """Stage application sources, compile .pyc files, and write manifest.json and __main__.py."""
        project_root = self.project_dir.resolve()
        if not project_root.exists():
            msg = f"Project directory does not exist: {project_root}"
            raise FileNotFoundError(msg)

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
            py_compile.compile(str(py_file), cfile=str(cfile), doraise=True)
            compiled_count += 1

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
            "pgo_enabled": self.pgo_enabled,
            "compiled_modules": compiled_count,
        }

        if self.pgo_enabled:
            py_pgo, py_lto = detect_python_pgo_lto()
            rust_pgo = RustPgoConfig(profile_dir=output_dir / "pgo-profiles")
            manifest["python_pgo_enabled"] = py_pgo
            manifest["python_lto_enabled"] = py_lto
            manifest["rustflags"] = rust_pgo.instrument_rustflags()

        formatted = format_json(manifest, indent=2)
        (dist_root / "manifest.json").write_text(formatted + "\n")
        (dist_root / "build-manifest.json").write_text(formatted + "\n")
        (dist_root / "__main__.py").write_text(self._render_bootstrap())
        return dist_root

    def _render_bootstrap(self) -> str:
        module_name, _, attr_name = self.entrypoint.partition(":")
        target_attr = attr_name or "app"
        return (
            '"""Auto-generated Vertebra standalone bootstrap."""\n\n'
            "import importlib\n"
            "from pathlib import Path\n"
            "import sys\n\n\n"
            "def main() -> int:\n"
            '    app_dir = Path(__file__).resolve().parent / "app"\n'
            "    sys.path.insert(0, str(app_dir))\n"
            f'    module = importlib.import_module("{module_name}")\n'
            f'    target = getattr(module, "{target_attr}")\n'
            "    if callable(target):\n"
            "        ret = target()\n"
            "        return ret if isinstance(ret, int) else 0\n"
            "    runner = getattr(target, 'run', None)\n"
            "    if callable(runner):\n"
            "        ret = runner()\n"
            "        return ret if isinstance(ret, int) else 0\n"
            "    return 0\n\n\n"
            'if __name__ == "__main__":\n'
            "    sys.exit(main())\n"
        )
