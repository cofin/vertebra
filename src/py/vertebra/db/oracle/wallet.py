"""Zero-Instant-Client Oracle Wallet (.zip / ewallet.pem / ewallet.p12) and TNS parser."""

from __future__ import annotations

import hashlib
import importlib
import io
import os
import shutil
import ssl
import tempfile
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

__all__ = (
    "ExtractedWallet",
    "build_wallet_ssl_context",
    "extract_wallet_archive",
    "parse_tnsnames_ora",
    "resolve_tns_alias",
)


def parse_tnsnames_ora(contents: str) -> dict[str, str]:
    """Parse an Oracle tnsnames.ora file into an uppercase alias -> descriptor mapping."""
    cleaned_lines: list[str] = []
    for line in contents.splitlines():
        comment_idx = line.find("#")
        cleaned_lines.append(line[:comment_idx] if comment_idx >= 0 else line)
    cleaned = "\n".join(cleaned_lines)

    entries: dict[str, str] = {}
    n = len(cleaned)
    i = 0
    while i < n:
        while i < n and cleaned[i].isspace():
            i += 1
        if i >= n:
            break
        alias_start = i
        while i < n and cleaned[i] != "=":
            i += 1
        if i >= n:
            break
        raw_aliases = cleaned[alias_start:i].strip()
        i += 1
        while i < n and cleaned[i].isspace():
            i += 1
        if i >= n or cleaned[i] != "(":
            msg = f"Expected '(' after '=' for TNS alias {raw_aliases!r}"
            raise ValueError(msg)
        desc_start = i
        depth = 0
        while i < n:
            ch = cleaned[i]
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
                if depth == 0:
                    i += 1
                    break
            i += 1
        if depth != 0:
            msg = f"Unbalanced parentheses in TNS descriptor for {raw_aliases!r}"
            raise ValueError(msg)
        descriptor = "".join(ch for ch in cleaned[desc_start:i] if not ch.isspace())
        for alias in raw_aliases.split(","):
            trimmed = alias.strip()
            if trimmed:
                entries[trimmed.upper()] = descriptor
    return entries


def resolve_tns_alias(contents: str, alias: str) -> str:
    """Resolve a TNS alias from tnsnames.ora content into its canonical `(DESCRIPTION=...)` string."""
    parsed = parse_tnsnames_ora(contents)
    key = alias.strip().upper()
    if key not in parsed:
        msg = f"TNS alias {alias!r} not found in tnsnames.ora (available: {sorted(parsed)})"
        raise KeyError(msg)
    return parsed[key]


@dataclass(frozen=True, slots=True)
class ExtractedWallet:
    """Per-pool extracted Oracle Cloud / Autonomous Database wallet metadata."""

    config_dir: Path
    wallet_location: Path
    pem_path: Path | None
    ca_cert_path: Path | None
    aliases: dict[str, str]

    def resolve_dsn(self, alias_or_dsn: str) -> str:
        """Resolve a TNS alias from the extracted wallet or return the DSN unchanged."""
        upper = alias_or_dsn.strip().upper()
        return self.aliases.get(upper, alias_or_dsn)


def extract_wallet_archive(
    wallet_source: str | Path | bytes,
    *,
    cache_root: str | Path | None = None,
    wallet_password: str | None = None,
) -> ExtractedWallet:
    """Extract a `Wallet_*.zip` archive or wallet directory into a `0o700` per-pool directory.

    Never mutates `os.environ["TNS_ADMIN"]`, ensuring multiple pools with distinct wallets
    can coexist safely within the same process.
    """
    base_dir = Path(cache_root) if cache_root is not None else Path(tempfile.gettempdir()) / "vertebra-wallets"
    base_dir.mkdir(parents=True, exist_ok=True)
    os.chmod(base_dir, 0o700)

    if isinstance(wallet_source, bytes):
        digest = hashlib.sha256(wallet_source).hexdigest()[:16]
        target_dir = base_dir / f"wallet-{digest}"
        target_dir.mkdir(parents=True, exist_ok=True)
        os.chmod(target_dir, 0o700)
        with zipfile.ZipFile(io.BytesIO(wallet_source)) as zf:
            _safe_extract_zip(zf, target_dir)
    else:
        src_path = Path(wallet_source).expanduser().resolve()
        if not src_path.exists():
            msg = f"Oracle wallet source not found: {src_path}"
            raise FileNotFoundError(msg)
        digest = hashlib.sha256(str(src_path).encode("utf-8")).hexdigest()[:16]
        target_dir = base_dir / f"wallet-{digest}"
        target_dir.mkdir(parents=True, exist_ok=True)
        os.chmod(target_dir, 0o700)
        if src_path.is_file() and zipfile.is_zipfile(src_path):
            with zipfile.ZipFile(src_path) as zf:
                _safe_extract_zip(zf, target_dir)
        elif src_path.is_dir():
            for item in src_path.iterdir():
                if item.is_file():
                    dest = target_dir / item.name
                    shutil.copy2(item, dest)
                    os.chmod(dest, 0o600)
        else:
            msg = f"Unsupported Oracle wallet source: {src_path}"
            raise ValueError(msg)

    pem_path = target_dir / "ewallet.pem"
    p12_path = target_dir / "ewallet.p12"
    if not pem_path.exists() and p12_path.exists():
        _convert_p12_to_pem(p12_path, pem_path, wallet_password=wallet_password)

    ca_cert_path = target_dir / "adb_container.cert"
    tns_file = target_dir / "tnsnames.ora"
    aliases = parse_tnsnames_ora(tns_file.read_text(encoding="utf-8")) if tns_file.exists() else {}

    return ExtractedWallet(
        config_dir=target_dir,
        wallet_location=target_dir,
        pem_path=pem_path if pem_path.exists() else None,
        ca_cert_path=ca_cert_path if ca_cert_path.exists() else None,
        aliases=aliases,
    )


def build_wallet_ssl_context(
    wallet: ExtractedWallet,
    *,
    wallet_password: str | None = None,
    ssl_check_hostname: bool = True,
) -> ssl.SSLContext:
    """Build a Thin-Mode mTLS SSLContext for an ExtractedWallet without global TNS_ADMIN."""
    ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    if not ssl_check_hostname:
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    if wallet.ca_cert_path is not None and wallet.ca_cert_path.exists():
        ctx.load_verify_locations(cafile=str(wallet.ca_cert_path))
    if wallet.pem_path is not None and wallet.pem_path.exists():
        ctx.load_verify_locations(cafile=str(wallet.pem_path))
        ctx.load_cert_chain(
            certfile=str(wallet.pem_path),
            keyfile=str(wallet.pem_path),
            password=wallet_password,
        )
    return ctx


def _safe_extract_zip(zf: zipfile.ZipFile, target_dir: Path) -> None:
    root = target_dir.resolve()
    for member in zf.infolist():
        if member.is_dir():
            continue
        filename = Path(member.filename).name
        if not filename or filename.startswith("."):
            continue
        dest = (target_dir / filename).resolve()
        if root not in dest.parents:
            msg = f"Rejected zip member outside target wallet directory: {member.filename}"
            raise ValueError(msg)
        dest.write_bytes(zf.read(member))
        os.chmod(dest, 0o600)


def _convert_p12_to_pem(p12_path: Path, pem_path: Path, *, wallet_password: str | None) -> None:
    try:
        serialization_mod: Any = importlib.import_module("cryptography.hazmat.primitives.serialization")
        pkcs12_mod: Any = importlib.import_module("cryptography.hazmat.primitives.serialization.pkcs12")
    except ImportError:
        return

    pw_bytes = wallet_password.encode("utf-8") if wallet_password else None
    private_key, cert, additional_certs = pkcs12_mod.load_key_and_certificates(p12_path.read_bytes(), pw_bytes)
    chunks: list[bytes] = []
    if private_key is not None:
        enc = serialization_mod.BestAvailableEncryption(pw_bytes) if pw_bytes else serialization_mod.NoEncryption()
        chunks.append(
            private_key.private_bytes(
                serialization_mod.Encoding.PEM,
                serialization_mod.PrivateFormat.PKCS8,
                enc,
            )
        )
    if cert is not None:
        chunks.append(cert.public_bytes(serialization_mod.Encoding.PEM))
    for extra in additional_certs or ():
        chunks.append(extra.public_bytes(serialization_mod.Encoding.PEM))
    if chunks:
        pem_path.write_bytes(b"".join(chunks))
        os.chmod(pem_path, 0o600)
