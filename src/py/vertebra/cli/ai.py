"""Consolidated vtb ai CLI group for embedding model management (vtb ai models) and MCP (vtb ai mcp)."""

from __future__ import annotations

import hashlib
import mmap
import os
import re
import sys
from pathlib import Path
from typing import Any, TextIO
from urllib.parse import urlparse

import anyio
import click
import msgspec

from vertebra.agent.mcp.bridge import run_stdio_http_bridge
from vertebra.agent.mcp.client import mcp_request_headers
from vertebra.agent.mcp.service import MCPService
from vertebra.agent.mcp.stdio import run_stdio_server
from vertebra.embeddings._models import ModelSpec, get_model_spec, list_model_specs
from vertebra.storage import SyncObjectStore
from vertebra.utils.serializers import decode_json, encode_json_bytes, format_json

__all__ = (
    "ModelBundleManifest",
    "ModelFileEntry",
    "ai",
    "bridge",
    "extract_mcp_headers_from_jsonrpc",
    "list_models",
    "mcp",
    "models",
    "pull_model",
    "run_mcp_stdio_bridge",
    "serve",
    "stage_model",
    "warm_model",
)

_SLUG_RE = re.compile(r"[^a-zA-Z0-9._-]+")


class ModelFileEntry(msgspec.Struct, frozen=True, gc=False):
    """Metadata and SHA-256 digest for a single file within a model bundle."""

    path: str
    size: int
    sha256: str


class ModelBundleManifest(msgspec.Struct, frozen=True, gc=False):
    """Deterministic manifest describing a pulled or staged embedding model bundle."""

    model: str
    provider: str
    quantization: str
    dimensions: int
    files: tuple[ModelFileEntry, ...]


def extract_mcp_headers_from_jsonrpc(payload: bytes) -> dict[str, str]:
    """Extract Mcp-Method, Mcp-Name, and MCP-Protocol-Version headers from a JSON-RPC frame."""
    return mcp_request_headers(payload)


def run_mcp_stdio_bridge(
    *,
    url: str,
    bearer_token: str | None = None,
    timeout_s: float = 30.0,
    max_message_bytes: int = 4_194_304,
    stdin: TextIO | None = None,
    stdout: TextIO | None = None,
    transport: Any = None,
) -> int:
    """Forward newline-delimited JSON-RPC frames from stdin to an HTTP MCP endpoint."""
    return run_stdio_http_bridge(
        url=url,
        bearer_token=bearer_token,
        timeout_s=timeout_s,
        max_message_bytes=max_message_bytes,
        stdin=stdin,
        stdout=stdout,
        transport=transport,
    )


@click.group()
def ai() -> None:
    """AI embedding model management and Model Context Protocol (MCP) tooling."""


@ai.group()
def models() -> None:
    """List, pull, stage, and warm local or cloud-staged embedding models."""


@models.command("list")
@click.option(
    "--provider",
    type=click.Choice(["vertex", "gemini", "gemma", "fastembed", "static"], case_sensitive=False),
    default=None,
    help="Filter models by provider",
)
@click.option("--json", "as_json", is_flag=True, default=False, help="Emit catalog as JSON")
@click.option("--cache-dir", type=click.Path(), default=None, help="Local model cache directory")
def list_models(provider: str | None, as_json: bool, cache_dir: str | None) -> None:
    """Display the built-in embedding model catalog and local cache / SHM status."""
    resolved_cache = _resolve_cache_dir(cache_dir)
    shm_root = _resolve_shm_root(resolved_cache)
    specs = list_model_specs(provider)

    records: list[dict[str, Any]] = []
    for spec in specs:
        slug = _model_slug(spec.name)
        is_cached = (resolved_cache / slug / "manifest.json").is_file()
        is_shm = (shm_root / f"vtb-model-{slug}" / "manifest.json").is_file()
        records.append(
            {
                "name": spec.name,
                "provider": spec.provider,
                "default_dimensions": spec.default_dimensions,
                "supported_dimensions": list(spec.supported_dimensions),
                "max_tokens": spec.max_tokens,
                "quantizations": list(spec.quantizations),
                "mirror_url": spec.mirror_url,
                "cached": is_cached,
                "shm_warmed": is_shm,
            }
        )

    if as_json:
        click.echo(format_json(records))
        return

    for item in records:
        dims = ",".join(str(d) for d in item["supported_dimensions"])
        quants = ",".join(item["quantizations"])
        status = "shm" if item["shm_warmed"] else ("cached" if item["cached"] else "remote")
        click.echo(f"{item['name']} ({item['provider']}) dims=[{dims}] quant=[{quants}] status={status}")


@models.command("pull")
@click.argument("model")
@click.option(
    "--quant",
    type=click.Choice(["int8", "q4", "bf16", "fp32"], case_sensitive=False),
    default=None,
    help="Target model quantization",
)
@click.option("--cache-dir", type=click.Path(), default=None, help="Local model cache directory")
@click.option("--source", "source_uri", default=None, help="Override source mirror or bucket URI")
def pull_model(
    model: str,
    quant: str | None,
    cache_dir: str | None,
    source_uri: str | None,
) -> None:
    """Pull model artifacts from an unauthenticated mirror or cloud bucket into the local cache."""
    spec = _require_model_spec(model)
    resolved_quant = _resolve_quantization(spec, quant)
    resolved_cache = _resolve_cache_dir(cache_dir)
    effective_source = source_uri or os.environ.get("VERTEBRA_MODEL_SOURCE") or spec.mirror_url
    if not effective_source:
        msg = f"Model {model!r} is a zero-weight cloud model and has no local artifacts to pull."
        raise click.ClickException(msg)

    target_dir, manifest = _pull_bundle_to_cache(
        spec=spec,
        quantization=resolved_quant,
        source_uri=effective_source,
        cache_dir=resolved_cache,
    )
    click.echo(f"Pulled {manifest.model} ({manifest.quantization}) to {target_dir}")


@models.command("stage")
@click.argument("model")
@click.option("--to", "to_uri", required=True, help="Destination cloud or local store URI (gs://, s3://, file://)")
@click.option(
    "--quant",
    type=click.Choice(["int8", "q4", "bf16", "fp32"], case_sensitive=False),
    default=None,
    help="Target model quantization",
)
@click.option("--cache-dir", type=click.Path(), default=None, help="Local model cache directory")
@click.option("--source", "source_uri", default=None, help="Optional source mirror URI when not yet pulled")
def stage_model(
    model: str,
    to_uri: str,
    quant: str | None,
    cache_dir: str | None,
    source_uri: str | None,
) -> None:
    """Stage a verified model bundle and deterministic manifest.json to a cloud bucket."""
    spec = _require_model_spec(model)
    resolved_quant = _resolve_quantization(spec, quant)
    resolved_cache = _resolve_cache_dir(cache_dir)
    slug = _model_slug(spec.name)
    local_dir = resolved_cache / slug
    manifest_path = local_dir / "manifest.json"

    if not manifest_path.is_file():
        effective_source = source_uri or os.environ.get("VERTEBRA_MODEL_SOURCE") or spec.mirror_url
        if not effective_source:
            msg = f"Model {model!r} is not present in {local_dir} and no --source URI was provided."
            raise click.ClickException(msg)
        local_dir, manifest = _pull_bundle_to_cache(
            spec=spec,
            quantization=resolved_quant,
            source_uri=effective_source,
            cache_dir=resolved_cache,
        )
    else:
        manifest = _verify_local_bundle(local_dir)

    dst_store, dst_prefix = _open_uri_store(to_uri)
    for entry in manifest.files:
        payload = (local_dir / entry.path).read_bytes()
        dst_store.put(_join_store_key(dst_prefix, entry.path), payload)
    dst_store.put(_join_store_key(dst_prefix, "manifest.json"), encode_json_bytes(manifest))
    click.echo(f"Staged {manifest.model} ({len(manifest.files)} files) to {to_uri}")


@models.command("warm")
@click.argument("model")
@click.option(
    "--from", "from_uri", default=None, help="Staged cloud or local store URI (defaults to VERTEBRA_MODEL_SOURCE)"
)
@click.option(
    "--shm", is_flag=True, default=False, help="Map verified model artifacts into /dev/shm (1x RAM across workers)"
)
@click.option("--cache-dir", type=click.Path(), default=None, help="Local model cache directory")
def warm_model(
    model: str,
    from_uri: str | None,
    shm: bool,
    cache_dir: str | None,
) -> None:
    """Fetch a staged model bundle, verify SHA-256 digests, and optionally warm /dev/shm."""
    spec = _require_model_spec(model)
    resolved_cache = _resolve_cache_dir(cache_dir)
    slug = _model_slug(spec.name)
    local_dir = resolved_cache / slug
    effective_source = from_uri or os.environ.get("VERTEBRA_MODEL_SOURCE")

    if effective_source:
        local_dir, manifest = _pull_bundle_to_cache(
            spec=spec,
            quantization=spec.quantizations[0],
            source_uri=effective_source,
            cache_dir=resolved_cache,
        )
    else:
        manifest = _verify_local_bundle(local_dir)

    if shm:
        shm_root = _resolve_shm_root(resolved_cache)
        shm_target = shm_root / f"vtb-model-{slug}"
        shm_target.mkdir(parents=True, exist_ok=True)
        for entry in manifest.files:
            src_file = local_dir / entry.path
            dst_file = shm_target / entry.path
            dst_file.parent.mkdir(parents=True, exist_ok=True)
            dst_file.write_bytes(src_file.read_bytes())
            if dst_file.stat().st_size > 0:
                with dst_file.open("rb") as handle, mmap.mmap(handle.fileno(), 0, access=mmap.ACCESS_READ) as mm:
                    _ = mm[0]
        (shm_target / "manifest.json").write_bytes(encode_json_bytes(manifest))
        click.echo(f"Warmed {manifest.model} into shared memory at {shm_target}")
        return

    click.echo(f"Warmed {manifest.model} in local cache at {local_dir}")


@ai.group()
def mcp() -> None:
    """Model Context Protocol (MCP) utilities, server, and stdio bridge."""


@mcp.command()
@click.option("--url", required=True, help="Remote MCP HTTP/SSE endpoint URL")
@click.option("--token", "bearer_token", default=None, help="Bearer / OAuth2 / OIDC access token")
@click.option("--timeout", "timeout_s", type=float, default=30.0, help="Per-request HTTP timeout in seconds")
def bridge(url: str, bearer_token: str | None, timeout_s: float) -> None:
    """Bridge local newline-delimited JSON-RPC 2.0 stdio to a remote HTTP/SSE MCP endpoint."""
    exit_code = run_mcp_stdio_bridge(url=url, bearer_token=bearer_token, timeout_s=timeout_s)
    if exit_code != 0:
        sys.exit(exit_code)


@mcp.command()
@click.option("--name", default="vertebra-mcp", help="Server name")
def serve(name: str) -> None:
    """Run an in-process MCP service over standard input/output (stdio)."""
    del name
    service = MCPService()
    exit_code = anyio.run(lambda: run_stdio_server(service))
    if exit_code != 0:
        sys.exit(exit_code)


def _model_slug(name: str) -> str:
    """Convert a canonical model identifier into a filesystem and /dev/shm safe slug."""
    return _SLUG_RE.sub("-", name.strip()).strip("-").lower()


def _resolve_cache_dir(cache_dir: str | None) -> Path:
    """Resolve the local embedding model cache directory."""
    raw = cache_dir or os.environ.get("VERTEBRA_MODEL_CACHE_DIR")
    if raw:
        return Path(raw).expanduser().resolve()
    return Path.home() / ".cache" / "vertebra" / "models"


def _resolve_shm_root(cache_dir: Path) -> Path:
    """Resolve the shared-memory root directory (/dev/shm on Linux or cache fallback)."""
    override = os.environ.get("VERTEBRA_SHM_DIR")
    if override:
        return Path(override).expanduser().resolve()
    dev_shm = Path("/dev/shm")
    if dev_shm.is_dir():
        return dev_shm
    return cache_dir / ".shm"


def _require_model_spec(model: str) -> ModelSpec:
    """Return the registered ModelSpec for model or raise a ClickException."""
    spec = get_model_spec(model)
    if spec is None:
        msg = f"Unknown model {model!r}. Run 'vtb ai models list' to see supported models."
        raise click.ClickException(msg)
    return spec


def _resolve_quantization(spec: ModelSpec, requested: str | None) -> str:
    """Validate and resolve the requested quantization for a ModelSpec."""
    quant = (requested or spec.quantizations[0]).lower()
    if spec.forbid_fp16 and quant == "fp16":
        msg = f"Model {spec.name!r} forbids fp16 quantization due to activation overflow; use int8, q4, bf16, or fp32."
        raise click.ClickException(msg)
    return quant


def _open_uri_store(uri: str) -> tuple[SyncObjectStore, str]:
    """Open a SyncObjectStore and relative key prefix for a cloud or local URI."""
    if "://" not in uri:
        return SyncObjectStore.from_local(Path(uri).expanduser().resolve()), ""
    parsed = urlparse(uri)
    scheme = parsed.scheme.lower()
    if scheme == "file":
        root = Path(parsed.netloc + parsed.path if parsed.netloc else parsed.path)
        return SyncObjectStore.from_local(root), ""
    if scheme in {"gs", "gcs"}:
        return SyncObjectStore.from_gcs(parsed.netloc), parsed.path.strip("/")
    if scheme == "s3":
        return SyncObjectStore.from_s3(parsed.netloc), parsed.path.strip("/")
    if scheme in {"http", "https"}:
        return SyncObjectStore.from_http(uri), ""
    base_url = f"{scheme}://{parsed.netloc}" if parsed.netloc else uri
    return SyncObjectStore(base_url), parsed.path.strip("/")


def _join_store_key(prefix: str, rel_path: str) -> str:
    """Join an optional bucket prefix and relative artifact path."""
    clean_rel = rel_path.lstrip("/")
    return f"{prefix}/{clean_rel}" if prefix else clean_rel


def _pull_bundle_to_cache(
    *,
    spec: ModelSpec,
    quantization: str,
    source_uri: str,
    cache_dir: Path,
) -> tuple[Path, ModelBundleManifest]:
    """Download model artifacts from source_uri into cache_dir, verifying SHA-256 digests."""
    store, prefix = _open_uri_store(source_uri)
    slug = _model_slug(spec.name)
    target_dir = cache_dir / slug
    target_dir.mkdir(parents=True, exist_ok=True)

    manifest_key = _join_store_key(prefix, "manifest.json")
    remote_manifest: ModelBundleManifest | None = None
    try:
        raw_manifest = store.get(manifest_key)
        remote_manifest = decode_json(raw_manifest, type=ModelBundleManifest)
    except (FileNotFoundError, OSError, msgspec.DecodeError):
        remote_manifest = None

    if remote_manifest is not None:
        for entry in remote_manifest.files:
            try:
                data = store.get(_join_store_key(prefix, entry.path))
            except (FileNotFoundError, OSError) as exc:
                msg = f"Failed to fetch {entry.path!r} from {source_uri}: {exc}"
                raise click.ClickException(msg) from exc
            actual_sha = hashlib.sha256(data).hexdigest()
            if actual_sha != entry.sha256 or len(data) != entry.size:
                msg = (
                    f"SHA-256 digest mismatch for {entry.path!r}: "
                    f"expected {entry.sha256} ({entry.size}B), got {actual_sha} ({len(data)}B)"
                )
                raise click.ClickException(msg)
            dst_path = target_dir / entry.path
            dst_path.parent.mkdir(parents=True, exist_ok=True)
            dst_path.write_bytes(data)
        (target_dir / "manifest.json").write_bytes(encode_json_bytes(remote_manifest))
        return target_dir, remote_manifest

    files_to_pull = spec.default_files or ("tokenizer.json", "model.onnx")
    entries: list[ModelFileEntry] = []
    for rel_path in files_to_pull:
        try:
            data = store.get(_join_store_key(prefix, rel_path))
        except (FileNotFoundError, OSError) as exc:
            msg = f"Failed to pull {rel_path!r} from {source_uri}: {exc}"
            raise click.ClickException(msg) from exc
        digest = hashlib.sha256(data).hexdigest()
        dst_path = target_dir / rel_path
        dst_path.parent.mkdir(parents=True, exist_ok=True)
        dst_path.write_bytes(data)
        entries.append(ModelFileEntry(path=rel_path, size=len(data), sha256=digest))

    manifest = ModelBundleManifest(
        model=spec.name,
        provider=spec.provider,
        quantization=quantization,
        dimensions=spec.default_dimensions,
        files=tuple(entries),
    )
    (target_dir / "manifest.json").write_bytes(encode_json_bytes(manifest))
    return target_dir, manifest


def _verify_local_bundle(local_dir: Path) -> ModelBundleManifest:
    """Verify that a local model directory contains manifest.json and matching SHA-256 files."""
    manifest_path = local_dir / "manifest.json"
    if not manifest_path.is_file():
        msg = f"Model bundle manifest not found at {manifest_path}"
        raise click.ClickException(msg)
    manifest: ModelBundleManifest = decode_json(manifest_path.read_bytes(), type=ModelBundleManifest)
    for entry in manifest.files:
        file_path = local_dir / entry.path
        if not file_path.is_file():
            msg = f"Missing model artifact {entry.path!r} in {local_dir}"
            raise click.ClickException(msg)
        data = file_path.read_bytes()
        actual_sha = hashlib.sha256(data).hexdigest()
        if actual_sha != entry.sha256 or len(data) != entry.size:
            msg = f"SHA-256 digest mismatch for local artifact {entry.path!r}"
            raise click.ClickException(msg)
    return manifest
