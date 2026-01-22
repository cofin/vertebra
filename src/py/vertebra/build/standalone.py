"""Standalone Python runtime management."""

import hashlib
import logging
from pathlib import Path

import httpx

logger = logging.getLogger(__name__)

STANDALONE_VERSION = "20240415"
STANDALONE_BASE_URL = f"https://github.com/indygreg/python-build-standalone/releases/download/{STANDALONE_VERSION}"

STANDALONE_RELEASES = {
    "linux-x64-baseline": f"cpython-3.12.3+{STANDALONE_VERSION}-x86_64-unknown-linux-gnu-install_only.tar.gz",
    "linux-x64-modern": f"cpython-3.12.3+{STANDALONE_VERSION}-x86_64_v3-unknown-linux-gnu-install_only.tar.gz",
    "linux-arm64": f"cpython-3.12.3+{STANDALONE_VERSION}-aarch64-unknown-linux-gnu-install_only.tar.gz",
    "darwin-x64-baseline": f"cpython-3.12.3+{STANDALONE_VERSION}-x86_64-apple-darwin-install_only.tar.gz",
    "darwin-x64-modern": f"cpython-3.12.3+{STANDALONE_VERSION}-x86_64_v3-apple-darwin-install_only.tar.gz",
    "darwin-arm64": f"cpython-3.12.3+{STANDALONE_VERSION}-aarch64-apple-darwin-install_only.tar.gz",
    "windows-x64": f"cpython-3.12.3+{STANDALONE_VERSION}-x86_64-pc-windows-msvc-shared-install_only.tar.gz",
}


async def download_standalone(target: str, profile: str, cache_dir: Path) -> Path:
    """Download python-build-standalone release for target platform."""
    key = target
    if "x64" in target and profile != "native":
        key = f"{target}-{profile}"
        if key not in STANDALONE_RELEASES:
            pass

    if key not in STANDALONE_RELEASES:
        if "x64" in target:
            key = f"{target}-modern"
        
    filename = STANDALONE_RELEASES.get(key)
    if not filename:
        msg = f"Unsupported target/profile: {target}/{profile}"
        raise ValueError(msg)

    url = f"{STANDALONE_BASE_URL}/{filename}"
    dest = cache_dir / filename

    if dest.exists():
        logger.debug("Using cached standalone runtime: %s", dest)
        return dest

    logger.info("Downloading standalone runtime: %s", url)
    cache_dir.mkdir(parents=True, exist_ok=True)
    
    async with httpx.AsyncClient(follow_redirects=True) as client:
        async with client.stream("GET", url) as response:
            response.raise_for_status()
            with dest.open("wb") as f:
                async for chunk in response.aiter_bytes():
                    f.write(chunk)

    return dest


class StandaloneManager:
    """Manages Python standalone runtimes."""

    def __init__(self, cache_dir: Path | None = None) -> None:
        self.cache_dir = cache_dir or Path.home() / ".cache" / "vertebra" / "pbs"

    async def get(self, target: str, profile: str) -> Path:
        """Get or download the requested runtime."""
        return await download_standalone(target, profile, self.cache_dir)
