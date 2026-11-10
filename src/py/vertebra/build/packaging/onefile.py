"""Self-extracting single-binary builder with zstd compression and VTB1FILE SHA-256 footer."""

import hashlib
import inspect
import io
import os
import tarfile
import zlib
from pathlib import Path
from typing import Any

from vertebra.build.packaging import _onefile_extract
from vertebra.build.packaging._onefile_extract import (
    TRAILER_MAGIC,
    TRAILER_SIZE,
    TRAILER_STRUCT,
    extract_payload_archive,
    inspect_trailer_bytes,
)
from vertebra.build.packaging.targets import BuildTarget
from vertebra.utils.module_loader import import_optional

__all__ = (
    "ONEFILE_MAGIC",
    "ONEFILE_TRAILER_SIZE",
    "OnefileBuilder",
)

ONEFILE_MAGIC: bytes = TRAILER_MAGIC
ONEFILE_TRAILER_STRUCT = TRAILER_STRUCT
ONEFILE_TRAILER_SIZE: int = TRAILER_SIZE


class OnefileBuilder:
    """Package a standalone distribution directory into a single-file executable with a VTB1FILE footer."""

    __slots__ = ("dist_dir", "target")

    def __init__(self, dist_dir: Path, target: BuildTarget) -> None:
        if not dist_dir.exists():
            msg = f"Standalone distribution folder not found: {dist_dir}"
            raise ValueError(msg)
        self.dist_dir = dist_dir
        self.target = target

    def build(self, output_path: Path) -> Path:
        """Create a single-file executable containing a launcher stub, compressed tar archive, and 48-byte footer."""
        payload = self._create_compressed_archive()
        digest = hashlib.sha256(payload).digest()
        hex_digest = digest.hex()

        stub = self._launcher_stub(hex_digest[:16])
        trailer = ONEFILE_TRAILER_STRUCT.pack(ONEFILE_MAGIC, len(payload), digest)

        out = output_path.resolve()
        out.parent.mkdir(parents=True, exist_ok=True)
        with out.open("wb") as handle:
            handle.write(stub)
            handle.write(payload)
            handle.write(trailer)

        out.chmod(0o755)
        return out

    @classmethod
    def inspect_trailer(cls, binary_path: Path) -> dict[str, Any]:
        """Read and verify the 48-byte VTB1FILE footer at the end of a onefile executable."""
        raw = binary_path.read_bytes()
        payload_size, stub_size, hex_digest = inspect_trailer_bytes(raw)
        return {
            "payload_size": payload_size,
            "sha256": hex_digest,
            "stub_size": stub_size,
        }

    @classmethod
    def extract_to(cls, binary_path: Path, destination: Path) -> Path:
        """Verify the VTB1FILE trailer and extract the embedded archive into destination."""
        return extract_payload_archive(binary_path, destination)

    def _create_compressed_archive(self) -> bytes:
        """Archive the distribution with normalized tar headers so identical trees yield identical payloads."""
        tar_buffer = io.BytesIO()
        with tarfile.open(fileobj=tar_buffer, mode="w") as tar:
            for item in sorted(self.dist_dir.iterdir()):
                tar.add(item, arcname=item.name, filter=self._normalize_tar_member)
        tar_bytes = tar_buffer.getvalue()
        return self._compress_payload(tar_bytes)

    @classmethod
    def _normalize_tar_member(cls, info: tarfile.TarInfo) -> tarfile.TarInfo:
        """Strip ownership and pin mtimes to `SOURCE_DATE_EPOCH` (or `0`) for reproducible archives."""
        info.uid = 0
        info.gid = 0
        info.uname = ""
        info.gname = ""
        info.mtime = cls._source_date_epoch()
        return info

    @staticmethod
    def _source_date_epoch() -> int:
        """Return the `SOURCE_DATE_EPOCH` timestamp, or `0` when unset or invalid."""
        raw = os.environ.get("SOURCE_DATE_EPOCH", "").strip()
        return int(raw) if raw.isdigit() else 0

    @classmethod
    def _compress_payload(cls, raw_tar: bytes) -> bytes:
        zstd = cls._load_zstd()
        if zstd is not None:
            compressor = zstd.ZstdCompressor(level=19)
            return b"ZSTD" + bytes(compressor.compress(raw_tar))
        return b"ZLIB" + zlib.compress(raw_tar, level=9)

    @staticmethod
    def _load_zstd() -> Any:
        return import_optional("zstandard")

    @classmethod
    def _launcher_stub(cls, short_hash: str) -> bytes:
        extract_source = inspect.getsource(_onefile_extract)
        shebang = chr(35) + "!/bin/sh\n"
        script = (
            f"{shebang}"
            "set -e\n"
            f'VTB_HASH="{short_hash}"\n'
            'CACHE_ROOT="${VERTEBRA_CACHE_DIR:-${XDG_CACHE_HOME:-${XDG_RUNTIME_DIR:-/tmp}}/vertebra}/onefile/$VTB_HASH"\n'
            'if ! mkdir -p -m 0700 "$CACHE_ROOT" 2>/dev/null; then\n'
            '  CACHE_ROOT="${TMPDIR:-/tmp}/vertebra/onefile/$VTB_HASH"\n'
            '  mkdir -p -m 0700 "$CACHE_ROOT"\n'
            "fi\n"
            'if [ ! -f "$CACHE_ROOT/__main__.py" ]; then\n'
            f'  python3 -c {cls._quote_python_code(extract_source)} "$0" "$CACHE_ROOT"\n'
            "fi\n"
            'exec python3 "$CACHE_ROOT/__main__.py" "$@"\n'
            "__VTB_PAYLOAD__\n"
        )
        return script.encode("utf-8")

    @staticmethod
    def _quote_python_code(code: str) -> str:
        escaped = code.replace("\\", "\\\\").replace('"', '\\"').replace("$", "\\$")
        return f'"{escaped}"'
