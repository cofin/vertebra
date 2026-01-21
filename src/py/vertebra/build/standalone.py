"""
Manage python-build-standalone releases for Vertebra single-file builds.

This module handles:
1. Mapping target platforms/profiles to PBS release assets.
2. Downloading and caching the standalone Python distributions.
3. Verifying checksums (optional but recommended).
"""
import hashlib
import os
import tarfile
from pathlib import Path
from typing import Literal

import httpx
from rich.console import Console

console = Console()

# Latest stable release as of Jan 2026 (placeholder version matching PRD)
STANDALONE_VERSION = "20260114"
STANDALONE_PYTHON_VERSION = "3.12.8"
STANDALONE_BASE_URL = f"https://github.com/astral-sh/python-build-standalone/releases/download/{STANDALONE_VERSION}"

# Mapping: {target}-{profile} -> filename
# Note: "native" profile uses the system python and is not handled here.
STANDALONE_RELEASES = {
    # Linux x64
    "linux-x64-baseline": f"cpython-{STANDALONE_PYTHON_VERSION}+{STANDALONE_VERSION}-x86_64-unknown-linux-gnu-install_only.tar.gz",
    "linux-x64-modern": f"cpython-{STANDALONE_PYTHON_VERSION}+{STANDALONE_VERSION}-x86_64_v3-unknown-linux-gnu-install_only.tar.gz",

    # Linux ARM64 (no baseline/modern distinction usually, mapping both to same for safety)
    "linux-arm64-baseline": f"cpython-{STANDALONE_PYTHON_VERSION}+{STANDALONE_VERSION}-aarch64-unknown-linux-gnu-install_only.tar.gz",
    "linux-arm64-modern": f"cpython-{STANDALONE_PYTHON_VERSION}+{STANDALONE_VERSION}-aarch64-unknown-linux-gnu-install_only.tar.gz",

    # macOS x64
    "darwin-x64-baseline": f"cpython-{STANDALONE_PYTHON_VERSION}+{STANDALONE_VERSION}-x86_64-apple-darwin-install_only.tar.gz",
    "darwin-x64-modern": f"cpython-{STANDALONE_PYTHON_VERSION}+{STANDALONE_VERSION}-x86_64_v3-apple-darwin-install_only.tar.gz",

    # macOS ARM64
    "darwin-arm64-baseline": f"cpython-{STANDALONE_PYTHON_VERSION}+{STANDALONE_VERSION}-aarch64-apple-darwin-install_only.tar.gz",
    "darwin-arm64-modern": f"cpython-{STANDALONE_PYTHON_VERSION}+{STANDALONE_VERSION}-aarch64-apple-darwin-install_only.tar.gz",

    # Windows x64
    "windows-x64-baseline": f"cpython-{STANDALONE_PYTHON_VERSION}+{STANDALONE_VERSION}-x86_64-pc-windows-msvc-install_only.tar.gz",
    "windows-x64-modern": f"cpython-{STANDALONE_PYTHON_VERSION}+{STANDALONE_VERSION}-x86_64-pc-windows-msvc-install_only.tar.gz",
}

TargetProfile = Literal["baseline", "modern"]


class StandaloneManager:
    """Manages the lifecycle of the standalone Python runtime."""

    def __init__(self, cache_dir: Path | None = None) -> None:
        if cache_dir is None:
            # Default to ~/.cache/vertebra/pbs
            # TODO: Respect XDG_CACHE_HOME
            cache_dir = Path.home() / ".cache" / "vertebra" / "pbs"

        self.cache_dir = cache_dir
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def get_release_filename(self, target: str, profile: TargetProfile) -> str:
        """Get the filename for the requested target and profile."""
        key = f"{target}-{profile}"
        if key not in STANDALONE_RELEASES:
            # Fallback for arm64 where profile might not matter as much, or explicit mapping fails
            # Try to find a generic match or fail
            if "arm64" in target:
                 # Try modern as default for arm64 if exact match fail (though we mapped both above)
                 key = f"{target}-modern"

        if key not in STANDALONE_RELEASES:
             raise ValueError(f"No standalone release found for target='{target}' profile='{profile}'")

        return STANDALONE_RELEASES[key]

    async def download(self, target: str, profile: TargetProfile) -> Path:
        """
        Download the standalone Python distribution.

        Returns:
            Path to the downloaded .tar.gz file in the cache.
        """
        filename = self.get_release_filename(target, profile)
        url = f"{STANDALONE_BASE_URL}/{filename}"
        destination = self.cache_dir / filename

        if destination.exists():
            # TODO: Checksum verification could go here
            console.log(f"[green]Using cached standalone Python:[/green] {filename}")
            return destination

        console.log(f"[cyan]Downloading standalone Python:[/cyan] {url}")

        # Download with httpx
        async with httpx.AsyncClient(follow_redirects=True) as client:
            try:
                async with client.stream("GET", url) as response:
                    response.raise_for_status()
                    total_size = int(response.headers.get("content-length", 0))

                    with open(destination, "wb") as f:
                        # Simple progress logging
                        downloaded = 0
                        async for chunk in response.aiter_bytes():
                            f.write(chunk)
                            downloaded += len(chunk)
                            # In a real CLI, we'd update a progress bar here

            except httpx.HTTPError as e:
                # Cleanup partial download
                if destination.exists():
                    destination.unlink()
                raise RuntimeError(f"Failed to download standalone Python: {e}") from e

        console.log(f"[green]Download complete:[/green] {filename}")
        return destination

    def extract(self, archive_path: Path, output_dir: Path) -> Path:
        """
        Extract the standalone distribution.

        Returns:
            The path to the extracted 'python' directory.
        """
        console.log(f"[cyan]Extracting:[/cyan] {archive_path}")

        # PBS archives usually contain a top-level 'python/' directory
        # We assume tar.gz for now
        with tarfile.open(archive_path, "r:gz") as tar:
            # security check? tarfile.data_filter in newer python versions
            tar.extractall(path=output_dir, filter="data")

        extracted_python = output_dir / "python"
        if not extracted_python.exists():
             raise RuntimeError(f"Archive {archive_path} did not contain expected 'python' directory.")

        return extracted_python
