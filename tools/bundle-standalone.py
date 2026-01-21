"Standalone binary bundler for Vertebra.

Uses python-build-standalone to create a portable Python runtime,
bundles the application and its dependencies, and includes the Bun engine.
"

import os
import shutil
import subprocess
import sys
import tarfile
import urllib.request
from pathlib import Path

# Configuration
PYTHON_VERSION = "3.12.3"
PB_RELEASE = "20240415"
PLATFORM = "x86_64-unknown-linux-gnu" # Default, should be dynamic

def get_pb_url(platform: str) -> str:
    return f"https://github.com/indygreg/python-build-standalone/releases/download/{PB_RELEASE}/cpython-{PYTHON_VERSION}+{PB_RELEASE}-{platform}-install_only.tar.gz"

def bundle(output_path: str, target_platform: str | None = None):
    platform = target_platform or PLATFORM
    repo_root = Path(__file__).resolve().parent.parent
    dist_dir = repo_root / "dist" / "standalone"
    dist_dir.mkdir(parents=True, exist_ok=True)
    
    # 1. Download and extract Python
    url = get_pb_url(platform)
    archive_path = dist_dir / f"python-{platform}.tar.gz"
    if not archive_path.exists():
        print(f"Downloading Python from {url}...")
        urllib.request.urlretrieve(url, archive_path)
    
    python_dir = dist_dir / f"python-{platform}"
    if python_dir.exists():
        shutil.rmtree(python_dir)
    python_dir.mkdir()
    
    print(f"Extracting Python...")
    with tarfile.open(archive_path, "r:gz") as tar:
        tar.extractall(path=python_dir)
    
    # The extracted structure usually has a 'python' directory
    base_python = python_dir / "python"
    
    # 2. Install dependencies into the standalone Python
    print("Installing dependencies...")
    python_exe = base_python / "bin" / "python3"
    
    # We'll use the current environment's requirements or pyproject.toml
    # For now, let's just install the current project in editable mode or as a wheel
    subprocess.run([str(python_exe), "-m", "pip", "install", str(repo_root)], check=True)
    
    # 3. Copy Bun engine
    print("Bundling Bun engine...")
    engine_src = repo_root / "dist" / "engine" / f"vertebra-engine-local" # Should match target
    if not engine_src.exists():
        print("Warning: local engine binary not found. Please run 'make js-build' first.")
    else:
        engine_dest = base_python / "bin" / "vertebra-engine"
        shutil.copy2(engine_src, engine_dest)
    
    # 4. Create entrypoint script
    print("Creating entrypoint...")
    entrypoint = base_python / "bin" / "vertebra-app"
    with open(entrypoint, "w") as f:
        f.write("#!/bin/bash\n")
        f.write(f'exec "$(dirname \"$0\")/python3" -m vertebra "$@"\n')
    entrypoint.chmod(0o755)
    
    print(f"Standalone bundle created at {base_python}")
    print(f"To run: {entrypoint} serve <app>")

if __name__ == "__main__":
    bundle("dist/vertebra")
