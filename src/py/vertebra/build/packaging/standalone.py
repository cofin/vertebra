"""Standalone relocatable directory builder for Vertebra applications."""

import hashlib
import py_compile
import shutil
from pathlib import Path

import msgspec

from vertebra.build.packaging.pgo import RustPgoConfig, detect_python_build, validate_profdata
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
        "pgo_enabled",
        "profdata_path",
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
        pgo_enabled: bool = False,
        profdata_path: Path | None = None,
        server_binary: Path | None = None,
        profile: str = "release",
    ) -> None:
        self.project_dir = project_dir
        self.target = target
        self.entrypoint = entrypoint
        self.pgo_enabled = pgo_enabled or (profdata_path is not None)
        self.profdata_path = profdata_path
        self.server_binary = server_binary
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
            "pgo_enabled": self.pgo_enabled,
            "compiled_modules": compiled_count,
        }
        if staged_binary_rel is not None:
            manifest["server_binary"] = staged_binary_rel

        if self.pgo_enabled:
            py_info = detect_python_build()
            rust_pgo = RustPgoConfig(profile_dir=dist_root / "pgo-profiles")
            manifest["python_pgo_enabled"] = py_info.is_pgo
            manifest["python_lto_enabled"] = py_info.is_lto
            manifest["python_build"] = msgspec.to_builtins(py_info)
            if self.profdata_path is not None:
                resolved_profdata = self.profdata_path.resolve()
                if not resolved_profdata.is_file():
                    msg = f"PGO profdata file does not exist: {resolved_profdata}"
                    raise FileNotFoundError(msg)
                raw_profdata = resolved_profdata.read_bytes()
                profdata_valid = validate_profdata(resolved_profdata)
                manifest["profdata_bytes"] = len(raw_profdata)
                manifest["profdata_sha256"] = hashlib.sha256(raw_profdata).hexdigest()
                manifest["profdata_valid"] = profdata_valid
                manifest["pgo_mode"] = "use" if profdata_valid else "fallback"
                manifest["rustflags"] = rust_pgo.use_rustflags(resolved_profdata) if profdata_valid else ""
            else:
                manifest["pgo_mode"] = "generate"
                manifest["rustflags"] = rust_pgo.instrument_rustflags()

        formatted = format_json(manifest, indent=2)
        (dist_root / "manifest.json").write_text(formatted + "\n")
        (dist_root / "build-manifest.json").write_text(formatted + "\n")
        (dist_root / "__main__.py").write_text(self._render_bootstrap(server_binary_rel=staged_binary_rel))
        return dist_root

    def _render_bootstrap(self, *, server_binary_rel: str | None = None) -> str:
        module_name, _, attr_name = self.entrypoint.partition(":")
        target_attr = attr_name or "app"
        os_import = "import os\n" if server_binary_rel is not None else ""
        server_env_line = (
            f'    os.environ.setdefault("VERTEBRA_SERVER_BINARY", str(root_dir / "{server_binary_rel}"))\n'
            if server_binary_rel is not None
            else ""
        )
        return (
            '"""Auto-generated Vertebra standalone bootstrap."""\n\n'
            "import importlib\n"
            f"{os_import}"
            "from pathlib import Path\n"
            "import sys\n\n\n"
            "def main() -> int:\n"
            "    root_dir = Path(__file__).resolve().parent\n"
            '    app_dir = root_dir / "app"\n'
            f"{server_env_line}"
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
