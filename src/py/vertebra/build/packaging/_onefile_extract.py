"""Extraction and trailer verification logic for VTB1FILE executables."""

import hashlib
import importlib
import io
import pathlib
import struct
import sys
import tarfile
import zlib
from pathlib import Path

TRAILER_MAGIC: bytes = b"VTB1FILE"
TRAILER_STRUCT: struct.Struct = struct.Struct("<8sQ32s")
TRAILER_SIZE: int = TRAILER_STRUCT.size


def inspect_trailer_bytes(payload: bytes) -> tuple[int, int, str]:
    """Inspect and verify the 48-byte trailer from binary bytes.

    Returns:
        A tuple of (payload_size, stub_size, hex_digest).
    """
    if len(payload) < TRAILER_SIZE:
        msg = f"Binary too small to contain VTB1FILE trailer: {len(payload)} bytes"
        raise ValueError(msg)
    magic, payload_size, digest = TRAILER_STRUCT.unpack(payload[-TRAILER_SIZE:])
    if magic != TRAILER_MAGIC:
        msg = f"Invalid onefile trailer magic: {magic!r}"
        raise ValueError(msg)
    payload_start = len(payload) - TRAILER_SIZE - payload_size
    if payload_start < 0:
        msg = "Corrupt onefile payload length in trailer"
        raise ValueError(msg)
    body = payload[payload_start : len(payload) - TRAILER_SIZE]
    actual_digest = hashlib.sha256(body).digest()
    if actual_digest != digest:
        msg = "Onefile SHA-256 payload digest mismatch"
        raise ValueError(msg)
    return int(payload_size), payload_start, digest.hex()


def decompress_payload(payload: bytes) -> bytes:
    """Decompress a ZSTD or ZLIB payload slice."""
    tag = payload[:4]
    body = payload[4:]
    if tag == b"ZSTD":
        zstd = importlib.import_module("zstandard")
        decompressor = zstd.ZstdDecompressor()
        return bytes(decompressor.decompress(body))
    if tag == b"ZLIB":
        return zlib.decompress(body)
    msg = f"Unknown onefile compression header: {tag!r}"
    raise ValueError(msg)


def extract_payload_archive(binary_path: Path, cache_root: Path) -> Path:
    """Verify trailer and extract archive to cache_root."""
    raw = binary_path.read_bytes()
    payload_size, stub_size, _ = inspect_trailer_bytes(raw)
    payload = raw[stub_size : stub_size + payload_size]
    tar_bytes = decompress_payload(payload)

    dest_root = cache_root.resolve()
    dest_root.mkdir(parents=True, exist_ok=True)
    with tarfile.open(fileobj=io.BytesIO(tar_bytes), mode="r:") as tar:
        for member in tar.getmembers():
            target_path = (dest_root / member.name).resolve()
            if dest_root not in target_path.parents and target_path != dest_root:
                msg = f"Unsafe member path in onefile archive: {member.name}"
                raise RuntimeError(msg)
            tar.extract(member, path=dest_root, filter="data")
    return dest_root


if __name__ == "__main__":
    if len(sys.argv) >= 3:
        extract_payload_archive(pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2]))
