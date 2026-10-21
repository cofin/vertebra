"""Self-extracting single-binary builder with zstd compression and VTB1FILE SHA-256 footer."""

import hashlib
import importlib
import io
import struct
import tarfile
import zlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from vertebra.build.packaging.targets import BuildTargetTuple

__all__ = (
    "ONEFILE_MAGIC",
    "ONEFILE_TRAILER_SIZE",
    "OnefileBuilder",
)

ONEFILE_MAGIC: bytes = b"VTB1FILE"
ONEFILE_TRAILER_STRUCT = struct.Struct("<8sQ32s")
ONEFILE_TRAILER_SIZE: int = ONEFILE_TRAILER_STRUCT.size


@dataclass(slots=True)
class OnefileBuilder:
    """Package a standalone distribution directory into a single-file executable with a VTB1FILE footer."""

    dist_dir: Path
    target: BuildTargetTuple

    def __post_init__(self) -> None:
        if not self.dist_dir.exists():
            msg = f"Standalone distribution folder not found: {self.dist_dir}"
            raise ValueError(msg)

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
        if len(raw) < ONEFILE_TRAILER_SIZE:
            msg = f"File too small to contain VTB1FILE trailer: {binary_path}"
            raise ValueError(msg)
        magic, payload_size, digest = ONEFILE_TRAILER_STRUCT.unpack(raw[-ONEFILE_TRAILER_SIZE:])
        if magic != ONEFILE_MAGIC:
            msg = f"Invalid onefile trailer magic: {magic!r}"
            raise ValueError(msg)
        payload_start = len(raw) - ONEFILE_TRAILER_SIZE - payload_size
        if payload_start < 0:
            msg = "Corrupt onefile payload length in trailer"
            raise ValueError(msg)
        payload = raw[payload_start : len(raw) - ONEFILE_TRAILER_SIZE]
        actual_digest = hashlib.sha256(payload).digest()
        if actual_digest != digest:
            msg = "Onefile SHA-256 payload digest mismatch"
            raise ValueError(msg)
        return {
            "payload_size": int(payload_size),
            "sha256": digest.hex(),
            "stub_size": payload_start,
        }

    @classmethod
    def extract_to(cls, binary_path: Path, destination: Path) -> Path:
        """Verify the VTB1FILE trailer and extract the embedded archive into destination."""
        info = cls.inspect_trailer(binary_path)
        raw = binary_path.read_bytes()
        stub_size = int(info["stub_size"])
        payload_size = int(info["payload_size"])
        payload = raw[stub_size : stub_size + payload_size]

        tar_bytes = cls._decompress_payload(payload)
        dest_root = destination.resolve()
        dest_root.mkdir(parents=True, exist_ok=True)
        with tarfile.open(fileobj=io.BytesIO(tar_bytes), mode="r:") as tar:
            for member in tar.getmembers():
                target_path = (dest_root / member.name).resolve()
                if dest_root not in target_path.parents and target_path != dest_root:
                    msg = f"Unsafe member path in onefile archive: {member.name}"
                    raise RuntimeError(msg)
                tar.extract(member, path=dest_root, filter="data")
        return dest_root

    def _create_compressed_archive(self) -> bytes:
        tar_buffer = io.BytesIO()
        with tarfile.open(fileobj=tar_buffer, mode="w") as tar:
            for item in sorted(self.dist_dir.iterdir()):
                tar.add(item, arcname=item.name)
        tar_bytes = tar_buffer.getvalue()
        return self._compress_payload(tar_bytes)

    @classmethod
    def _compress_payload(cls, raw_tar: bytes) -> bytes:
        zstd = cls._load_zstd()
        if zstd is not None:
            compressor = zstd.ZstdCompressor(level=19)
            return b"ZSTD" + bytes(compressor.compress(raw_tar))
        return b"ZLIB" + zlib.compress(raw_tar, level=9)

    @classmethod
    def _decompress_payload(cls, payload: bytes) -> bytes:
        tag = payload[:4]
        body = payload[4:]
        if tag == b"ZSTD":
            zstd = cls._load_zstd()
            if zstd is None:
                msg = "zstandard package is required to decompress ZSTD onefile payloads"
                raise RuntimeError(msg)
            decompressor = zstd.ZstdDecompressor()
            return bytes(decompressor.decompress(body))
        if tag == b"ZLIB":
            return zlib.decompress(body)
        msg = f"Unknown onefile compression header: {tag!r}"
        raise ValueError(msg)

    @staticmethod
    def _load_zstd() -> Any:
        try:
            return importlib.import_module("zstandard")
        except ImportError:
            return None

    @staticmethod
    def _launcher_stub(short_hash: str) -> bytes:
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
            "  python3 -c '"
            "import hashlib, io, pathlib, struct, sys, tarfile, zlib\n"
            "p = pathlib.Path(sys.argv[1]).read_bytes()\n"
            "dest = pathlib.Path(sys.argv[2]).resolve()\n"
            'magic, sz, digest = struct.unpack("<8sQ32s", p[-48:])\n'
            'assert magic == b"VTB1FILE"\n'
            "payload = p[len(p) - 48 - sz : len(p) - 48]\n"
            "assert hashlib.sha256(payload).digest() == digest\n"
            "tag, body = payload[:4], payload[4:]\n"
            'if tag == b"ZSTD":\n'
            "    import zstandard\n"
            "    raw_tar = zstandard.ZstdDecompressor().decompress(body)\n"
            "else:\n"
            "    raw_tar = zlib.decompress(body)\n"
            'with tarfile.open(fileobj=io.BytesIO(raw_tar), mode="r:") as t:\n'
            '    t.extractall(path=dest, filter="data")\n'
            '\' "$0" "$CACHE_ROOT"\n'
            "fi\n"
            'exec python3 "$CACHE_ROOT/__main__.py" "$@"\n'
            "__VTB_PAYLOAD__\n"
        )
        return script.encode("utf-8")
