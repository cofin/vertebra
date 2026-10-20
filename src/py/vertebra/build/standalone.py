"""Standalone relocatable directory builder for Vertebra applications."""

import json
import py_compile
import shutil
from dataclasses import dataclass
from pathlib import Path

from vertebra.build.targets import BuildTargetTuple

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
        "target",
        "__pycache__",
    }
)


@dataclass(slots=True)
class StandaloneBuilder:
    """Assemble a relocatable standalone distribution with compiled .pyc bytecode and entrypoint bootstrap."""

    project_dir: Path
    target: BuildTargetTuple
    entrypoint: str = "app:app"
    pgo_enabled: bool = False

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

        manifest = {
            "entrypoint": self.entrypoint,
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
        (dist_root / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
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
            f'    mod = importlib.import_module("{module_name}")\n'
            f'    target = getattr(mod, "{target_attr}", None)\n'
            "    if callable(target):\n"
            "        res = target()\n"
            "        return int(res) if isinstance(res, int) else 0\n"
            "    return 0\n\n\n"
            'if __name__ == "__main__":\n'
            "    raise SystemExit(main())\n"
        )
