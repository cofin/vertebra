"""
Orchestrates the creation of single-file executables for Vertebra applications.
"""
import os
import shutil
import subprocess
from pathlib import Path
from typing import Literal

from rich.console import Console

from vertebra.core.build.standalone import StandaloneManager

console = Console()

TargetProfile = Literal["baseline", "modern"]

class Bundler:
    """
    Bundles a Python application, the Vertebra engine (Rust), and a portable Python runtime
    into a standalone executable (or distribution folder).
    """

    def __init__(
        self,
        project_dir: Path,
        output_dir: Path,
        target: str,
        profile: TargetProfile = "modern",
        clean: bool = False,
    ) -> None:
        self.project_dir = project_dir
        self.output_dir = output_dir
        self.target = target
        self.profile = profile
        self.clean = clean
        self.manager = StandaloneManager()

    async def build(self) -> Path:
        """
        Execute the build process.

        Returns:
            Path to the final executable or distribution directory.
        """
        console.log(f"[bold green]Starting build for target={self.target} profile={self.profile}[/bold green]")

        # 1. Prepare output directory
        self._prepare_output_dir()

        # 2. Download and extract Python runtime (PBS)
        python_dir = await self._setup_python_runtime()

        # 3. Install dependencies into the bundled environment
        await self._install_dependencies(python_dir)

        # 4. Copy application code
        self._copy_app_code(python_dir)

        # 5. Bundle the Engine (Rust binary)
        await self._bundle_server(python_dir)

        console.log(f"[bold green]Build complete:[/bold green] {self.output_dir}")
        return self.output_dir

    def _prepare_output_dir(self) -> None:
        if self.output_dir.exists() and self.clean:
            console.log(f"Cleaning output directory: {self.output_dir}")
            shutil.rmtree(self.output_dir)

        self.output_dir.mkdir(parents=True, exist_ok=True)

    async def _setup_python_runtime(self) -> Path:
        """Downloads PBS and extracts it to output_dir/python."""
        archive_path = await self.manager.download(self.target, self.profile)

        # We extract directly into the output directory
        # PBS usually extracts to a 'python' subdirectory
        python_home = self.manager.extract(archive_path, self.output_dir)
        return python_home

    async def _install_dependencies(self, python_dir: Path) -> None:
        """
        Installs dependencies from project's pyproject.toml into the bundled site-packages.
        Uses `uv` or `pip` to install into the target directory.
        """
        # Site packages location varies by OS, but PBS standardizes this somewhat.
        # On Linux/macOS: lib/python3.x/site-packages
        # On Windows: Lib/site-packages

        # For now, assuming POSIX layout for the 'python' dir from PBS
        # We need to find the site-packages dir.
        # A simple heuristic: look for 'site-packages'

        site_packages = list(python_dir.rglob("site-packages"))
        if not site_packages:
            raise RuntimeError("Could not locate site-packages in extracted Python runtime.")

        target_site_packages = site_packages[0]
        console.log(f"Installing dependencies to: {target_site_packages}")

        # construct requirements source (pyproject.toml or requirements.txt)
        # For this prototype, we assume we can just install the current project path
        # pip install . -t <target>

        # Note: In a real offline build, we'd need to have these wheels cached or
        # use a lockfile to fetch them. For now, we allow network access for this step
        # or assume the user has configured their environment.

        # We use the SYSTEM 'uv' or 'pip' to install into the target directory.
        # This avoids needing to boot the bundled python just to run pip.

        cmd = [
            "uv",
            "pip",
            "install",
            ".",
            "--target",
            str(target_site_packages),
            "--system",  # Use system python's uv to install to target
        ]

        # If uv is not available, fallback to pip?
        # For Vertebra, we assume uv is present per instructions.

        console.log(f"Running: {' '.join(cmd)}")
        env = os.environ.copy()
        process = subprocess.run(cmd, cwd=self.project_dir, capture_output=True, text=True, env=env)

        if process.returncode != 0:
            console.print(f"[red]Dependency installation failed:[/red]\n{process.stderr}")
            raise RuntimeError("Dependency installation failed")

    def _copy_app_code(self, python_dir: Path) -> None:
        """
        Copies the application code into the bundle.
        Actually, `pip install . --target` above handles this if the project is a package.
        If it's just scripts, we might need manual copying.

        For Vertebra apps, they are usually packages.
        So this step might be redundant if Step 3 does `pip install .`
        But we might want to clean up or organize things.
        """
        src_root = self.project_dir / "src" / "py"
        if not src_root.exists():
            return

        site_packages = list(python_dir.rglob("site-packages"))
        if not site_packages:
            raise RuntimeError("Could not locate site-packages for app copy.")

        target_site_packages = site_packages[0]
        console.log(f"Copying app code from {src_root} to {target_site_packages}")

        for item in src_root.iterdir():
            if item.name in {"__pycache__", "tests"}:
                continue
            dest = target_site_packages / item.name
            if dest.exists():
                if dest.is_dir():
                    shutil.rmtree(dest)
                else:
                    dest.unlink()
            if item.is_dir():
                shutil.copytree(item, dest)
            else:
                shutil.copy2(item, dest)

    async def _bundle_server(self, python_dir: Path) -> None:
        """Build and bundle the Rust server binary into the distribution."""
        bin_dir = python_dir / "bin"
        bin_dir.mkdir(parents=True, exist_ok=True)

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

        console.log(f"Building server: {' '.join(cmd)}")
        env = os.environ.copy()

        if not shutil.which("cargo"):
            raise RuntimeError("Cargo not found. Please install Rust.")

        process = subprocess.run(cmd, cwd=self.project_dir, capture_output=True, text=True, env=env)

        if process.returncode != 0:
            console.print(f"[red]Server build failed:[/red]\n{process.stderr}")
            raise RuntimeError("Server build failed")

        # Locate the binary
        # Target directory is typically relative to the workspace root (where Cargo.toml is)
        target_dir = manifest_path.parent / "target" / "release"
        binary_name = "vertebra-server.exe" if "windows" in self.target else "vertebra-server"
        binary_path = target_dir / binary_name

        if not binary_path.exists():
             # Check project root target as fallback?
             fallback_path = self.project_dir / "target" / "release" / binary_name
             if fallback_path.exists():
                 binary_path = fallback_path
             else:
                 raise RuntimeError(f"Server binary not found at {binary_path} or {fallback_path}")

        console.log(f"Copying binary from {binary_path} to {bin_dir}")
        shutil.copy2(binary_path, bin_dir / binary_name)
