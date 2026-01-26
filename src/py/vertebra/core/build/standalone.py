"""Standalone Python runtime management."""
import asyncio
import logging
import tarfile
import urllib.request
from pathlib import Path

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


def get_release_filename(target: str, profile: str) -> str:
    key = target
    if "x64" in target and profile != "native":
        key = f"{target}-{profile}"
    if key not in STANDALONE_RELEASES and "x64" in target:
        key = f"{target}-modern"
    filename = STANDALONE_RELEASES.get(key)
    if not filename:
        msg = f"Unsupported target/profile: {target}/{profile}"
        raise ValueError(msg)
    return filename


async def download_standalone(target: str, profile: str, cache_dir: Path) -> Path:
    """Download python-build-standalone release for target platform."""
    filename = get_release_filename(target, profile)

    url = f"{STANDALONE_BASE_URL}/{filename}"
    dest = cache_dir / filename

    if dest.exists():
        logger.debug("Using cached standalone runtime: %s", dest)
        return dest

    logger.info("Downloading standalone runtime: %s", url)
    cache_dir.mkdir(parents=True, exist_ok=True)

    # Use urllib from standard library to avoid httpx dependency
    # This is a build-time operation so blocking is acceptable
 

    def _download_sync() -> None:
        with urllib.request.urlopen(url, timeout=300) as response, dest.open("wb") as f:  # noqa: S310
            while chunk := response.read(8192):
                f.write(chunk)

    await asyncio.to_thread(_download_sync)

    return dest


class StandaloneManager:
    """Manages Python standalone runtimes."""

    def __init__(self, cache_dir: Path | None = None) -> None:
        self.cache_dir = cache_dir or Path.home() / ".cache" / "vertebra" / "pbs"

    async def get(self, target: str, profile: str) -> Path:
        """Get or download the requested runtime."""
        return await download_standalone(target, profile, self.cache_dir)

    def get_release_filename(self, target: str, profile: str) -> str:
        """Resolve a target/profile into a PBS release filename."""
        return get_release_filename(target, profile)

    async def download(self, target: str, profile: str) -> Path:
        """Download runtime archive (cached)."""
        return await download_standalone(target, profile, self.cache_dir)

    def extract(self, archive_path: Path, output_dir: Path) -> Path:
        """Extract PBS archive into output_dir/python and return python home."""
        python_root = output_dir / "python"
        if python_root.exists():
            return python_root
        python_root.mkdir(parents=True, exist_ok=True)

        if not archive_path.exists():
            msg = f"Standalone archive missing: {archive_path}"
            raise FileNotFoundError(msg)

        if archive_path.suffixes[-2:] != [".tar", ".gz"]:
            msg = f"Unsupported archive format: {archive_path}"
            raise ValueError(msg)

        with tarfile.open(archive_path, "r:gz") as tar:
            for member in tar.getmembers():
                member_path = python_root / member.name
                if not member_path.resolve().is_relative_to(python_root.resolve()):
                    msg = f"Blocked unsafe path in archive: {member.name}"
                    raise ValueError(msg)
            tar.extractall(python_root)

        extracted = python_root / "python"
        return extracted if extracted.exists() else python_root
