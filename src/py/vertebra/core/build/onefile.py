"""
PyApp single-file builder for Vertebra.
"""
import os
import shutil
import subprocess
import tarfile
import tempfile
from pathlib import Path
from typing import Optional

from rich.console import Console
from vertebra.core.build.bundler import Bundler

console = Console()

class OneFileBuilder:
    """
    Builds a single-file executable using PyApp.
    """

    def __init__(
        self,
        project_dir: Path,
        output_path: Path,
        target: str,
        profile: str = "modern",
    ) -> None:
        self.project_dir = project_dir
        self.output_path = output_path
        self.target = target
        self.profile = profile

    async def build(self) -> Path:
        """
        Build the single-file executable.
        """
        console.log(f"[bold blue]Building one-file executable for {self.target}...[/bold blue]")
        
        with tempfile.TemporaryDirectory() as tmp_dir:
            temp_path = Path(tmp_dir)
            dist_dir = temp_path / "dist"
            
            # 1. Use Bundler to create a distribution directory
            bundler = Bundler(
                project_dir=self.project_dir,
                output_dir=dist_dir,
                target=self.target,
                profile=self.profile,
                clean=True
            )
            await bundler.build()
            
            # 2. Create a tarball of the distribution
            archive_path = temp_path / "distribution.tar.gz"
            with tarfile.open(archive_path, "w:gz") as tar:
                tar.add(dist_dir, arcname=".")
            
            # 3. Use PyApp to wrap the archive
            # We need the pyapp binary for the host platform
            pyapp_bin = self._ensure_pyapp()
            
            # Configure PyApp
            env = os.environ.copy()
            env["PYAPP_DISTRIBUTION_PATH"] = str(archive_path)
            env["PYAPP_DISTRIBUTION_EMBED"] = "1"
            env["PYAPP_SKIP_INSTALL"] = "true"
            env["PYAPP_FULL_ISOLATION"] = "true"
            env["PYAPP_EXEC_SPEC"] = "vertebra.cli:main"
            
            # Build the final binary
            cmd = [str(pyapp_bin), "build", str(self.output_path)]
            
            result = subprocess.run(cmd, env=env, capture_output=True, text=True)
            if result.returncode != 0:
                console.print(f"[red]PyApp build failed:[/red]\n{result.stderr}")
                raise RuntimeError("PyApp build failed")
                
        console.log(f"[bold green]One-file build complete:[/bold green] {self.output_path}")
        return self.output_path

    def _ensure_pyapp(self) -> Path:
        """Ensure PyApp binary is available."""
        # For now, assume it's installed globally or in path
        # Real implementation might download it
        pyapp = shutil.which("pyapp")
        if not pyapp:
            # Check ~/.cargo/bin or similar
            home_bin = Path.home() / ".cargo" / "bin" / "pyapp"
            if home_bin.exists():
                return home_bin
            raise RuntimeError("pyapp not found. Please install via 'cargo install pyapp'")
        return Path(pyapp)
