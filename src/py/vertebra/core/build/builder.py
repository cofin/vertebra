"""Build orchestrator."""

import asyncio
import importlib
import os
import subprocess
from pathlib import Path

from vertebra.core.build.bundler import Bundler
from vertebra.core.build.config import BuildConfig
from vertebra.core.codegen.bridge import TypeBridge


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

    async def compile_server(self, target: str) -> Path:
        """Compile Rust server binary for target platform."""
        package_root = self.project_dir / "src" / "py" / "vertebra"
        outdir = package_root / "bin"
        outdir.mkdir(parents=True, exist_ok=True)

        # Locate Cargo.toml
        manifest_path = self.project_dir / "src" / "rs" / "Cargo.toml"
        if not manifest_path.exists():
            if (self.project_dir / "Cargo.toml").exists():
                manifest_path = self.project_dir / "Cargo.toml"
            else:
                raise RuntimeError("Could not locate Cargo.toml in src/rs or root")

        cmd = [
            "cargo",
            "build",
            "--release",
            "--manifest-path",
            str(manifest_path),
            "-p",
            "vertebra-server",
        ]

        if "windows" in target:
            cmd.extend(["--target", "x86_64-pc-windows-msvc"])
        elif "darwin" in target:
            if "arm64" in target:
                cmd.extend(["--target", "aarch64-apple-darwin"])
            else:
                cmd.extend(["--target", "x86_64-apple-darwin"])

        env = os.environ.copy()
        result = subprocess.run(cmd, cwd=self.project_dir, capture_output=True, text=True, env=env)
        if result.returncode != 0:
            raise RuntimeError(f"Server build failed: {result.stderr.strip()}")

        # Locate the binary
        binary_name = "vertebra-server.exe" if "windows" in target else "vertebra-server"
        
        # Check workspace target or root target
        target_dir = manifest_path.parent / "target" / "release"
        if "--target" in cmd:
            idx = cmd.index("--target")
            target_triple = cmd[idx + 1]
            target_dir = manifest_path.parent / "target" / target_triple / "release"

        binary_path = target_dir / binary_name
        if not binary_path.exists():
             # Fallback to root target
             fallback_path = self.project_dir / "target" / "release" / binary_name
             if fallback_path.exists():
                 binary_path = fallback_path
             else:
                 raise FileNotFoundError(f"Server binary not found: {binary_path}")

        dest_path = outdir / binary_name
        import shutil
        shutil.copy2(binary_path, dest_path)
        return dest_path

    async def build_wheel(self) -> Path:
        """Build Python wheel with embedded server."""
        await self.compile_server(target="native")
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
        from vertebra.core.build.onefile import OneFileBuilder
        builder = OneFileBuilder(
            project_dir=self.project_dir,
            output_path=output,
            target=target,
            profile=profile,
        )
        await builder.build()
