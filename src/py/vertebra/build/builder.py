"""Build orchestrator."""

import asyncio
import importlib
import os
import subprocess
from pathlib import Path

from vertebra.build.bundler import Bundler
from vertebra.build.config import BuildConfig
from vertebra.types.bridge import TypeBridge


class Builder:
    """Orchestrates Vertebra build process."""

    def __init__(self, project_dir: Path) -> None:
        self.project_dir = project_dir
        self.config = BuildConfig.load(project_dir)

    async def generate_types(self) -> Path:
        """Generate TypeScript types from Python procedures."""
        if not self.config.app:
            msg = "Missing [tool.vertebra].app in pyproject.toml"
            raise ValueError(msg)

        module_path, _, _ = self.config.app.partition(":")
        importlib.import_module(module_path)

        types_path = self.project_dir / "src" / "ts" / "src" / "client" / "types.d.ts"
        types_path.parent.mkdir(parents=True, exist_ok=True)
        output = TypeBridge().generate()
        types_path.write_text(output, encoding="utf-8")
        return types_path

    async def compile_engine(self, target: str) -> Path:
        """Compile Bun engine binary for target platform."""
        package_root = self.project_dir / "src" / "py" / "vertebra"
        outdir = package_root / "bin"
        outdir.mkdir(parents=True, exist_ok=True)

        profile = self.config.targets.get(target, None)
        profile_value = profile.profile if profile and profile.profile else self.config.profile

        build_cli = Path(__file__).resolve().parent / "tools" / "build-cli.ts"
        cmd = [
            "bun",
            "run",
            str(build_cli),
            "--target",
            target,
            "--profile",
            profile_value,
            "--outdir",
            str(outdir),
        ]

        env = os.environ.copy()
        source_dir = self.project_dir / "src" / "ts"
        if source_dir.exists():
            env["VERTEBRA_ENGINE_SOURCE_DIR"] = str(source_dir)
        result = subprocess.run(cmd, cwd=self.project_dir, capture_output=True, text=True, env=env)
        if result.returncode != 0:
            raise RuntimeError(f"Engine build failed: {result.stderr.strip()}")

        engine_name = "vertebra-engine.exe" if "windows" in target else "vertebra-engine"
        engine_path = outdir / engine_name
        if not engine_path.exists():
            raise FileNotFoundError(f"Engine binary not found: {engine_path}")
        return engine_path

    async def build_wheel(self) -> Path:
        """Build Python wheel with embedded engine."""
        await self.compile_engine(target="native")
        dist_dir = self.project_dir / "dist"
        dist_dir.mkdir(parents=True, exist_ok=True)

        cmd = ["uv", "build", "--wheel", "--out-dir", str(dist_dir)]
        result = subprocess.run(cmd, cwd=self.project_dir, capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(f"Wheel build failed: {result.stderr.strip()}")

        wheels = sorted(dist_dir.glob("*.whl"), key=lambda p: p.stat().st_mtime, reverse=True)
        if not wheels:
            raise FileNotFoundError("No wheel produced in dist directory")
        return wheels[0]

    async def build_one_file(
        self, output: Path, target: str, profile: str, clean: bool = False
    ) -> None:
        """Build standalone single-file executable."""
        bundler = Bundler(
            project_dir=self.project_dir,
            output_dir=output,
            target=target,
            profile=profile,
            clean=clean,
        )
        await bundler.build()
