"""Multicloud ObjectStore standalone benchmark comparators across RustFS, Floci, Azurite, R2, Local, and Memory."""

from __future__ import annotations

import argparse
import asyncio
import sys
from collections.abc import Sequence
from datetime import UTC, datetime
from functools import partial
from pathlib import Path
from typing import Any, Final, Literal, cast

import msgspec

from tools.benchmarks.adapters.base import (
    ADAPTER_SCHEMA_VERSION,
    DEFAULT_MAX_LATENCY_RATIO,
    DEFAULT_MIN_THROUGHPUT_RATIO,
    AdapterBenchmarkCase,
    AdapterBenchmarkReport,
    DriverMode,
    assert_free_threaded_gil_disabled,
    compare_adapter_reports,
    compute_adapter_case_stats,
    format_adapter_report_bytes,
    parse_adapter_report_bytes,
    redact_target_uri,
    sample_async,
    sample_sync,
)
from vertebra.storage import (
    AsyncObjectStore,
    AzureConfig,
    GCSConfig,
    LocalConfig,
    MemoryConfig,
    OCIConfig,
    R2Config,
    S3Config,
    StorageConfig,
    SyncObjectStore,
)
from vertebra.utils.module_loader import import_optional

PAYLOAD_SIZES: Final[dict[str, int]] = {"4kib": 4096, "1mib": 1_048_576}

type StorageBackendKind = Literal["memory", "local", "rustfs", "floci_s3", "floci_gcs", "floci_oci", "azurite", "r2"]

_BACKEND_ALIASES: Final[dict[str, StorageBackendKind]] = {
    "memory": "memory",
    "local": "local",
    "rustfs": "rustfs",
    "floci_s3": "floci_s3",
    "floci_aws": "floci_s3",
    "floci_gcs": "floci_gcs",
    "floci_oci": "floci_oci",
    "azurite": "azurite",
    "r2": "r2",
    "floci_r2": "r2",
}

_OBSTORE_BACKENDS: Final[frozenset[StorageBackendKind]] = frozenset(
    {"memory", "local", "rustfs", "floci_s3", "floci_gcs", "azurite"}
)
_S3_COMPAT_BACKENDS: Final[frozenset[StorageBackendKind]] = frozenset({"rustfs", "floci_s3", "r2"})

__all__ = (
    "PAYLOAD_SIZES",
    "StorageBackendKind",
    "StorageBackendSpec",
    "build_storage_backend_spec",
    "capture_storage_report",
    "main",
)


class StorageBackendSpec(msgspec.Struct, frozen=True, gc=False):
    """Typed storage backend specification for standalone ObjectStore benchmarks."""

    backend: StorageBackendKind
    config: StorageConfig
    endpoint_redacted: str
    provider_label: str


def build_storage_backend_spec(
    backend: str,
    *,
    bucket: str = "bench-bucket",
    root: str | Path | None = None,
    endpoint: str | None = None,
    region: str = "us-east-1",
    account: str = "devstoreaccount1",
    namespace: str = "bench",
    access_key_id: str | None = None,
    secret_access_key: str | None = None,
) -> StorageBackendSpec:
    """Build a typed `StorageBackendSpec` for a supported storage backend while rejecting MinIO."""
    raw = backend.strip().lower()
    if raw == "minio":
        msg = "MinIO is not supported; use 'rustfs' or 'floci_s3' for S3-compatible benchmarks"
        raise ValueError(msg)

    canonical = _BACKEND_ALIASES.get(raw)
    if canonical is None:
        msg = f"Unsupported storage benchmark backend: {backend!r}"
        raise ValueError(msg)

    if canonical == "memory":
        return StorageBackendSpec(
            backend="memory",
            config=MemoryConfig(namespace=namespace),
            endpoint_redacted=redact_target_uri(f"memory://{namespace}"),
            provider_label="Memory",
        )
    if canonical == "local":
        resolved_root = Path(root) if root is not None else Path.cwd() / ".bench-storage"
        return StorageBackendSpec(
            backend="local",
            config=LocalConfig(root=resolved_root),
            endpoint_redacted=redact_target_uri(f"file://{resolved_root.resolve().as_posix()}"),
            provider_label="Local",
        )
    if canonical in {"rustfs", "floci_s3"}:
        s3_cfg = S3Config(
            bucket=bucket,
            region=region,
            endpoint=endpoint,
            access_key_id=access_key_id,
            secret_access_key=secret_access_key,
            allow_http=True,
        )
        return StorageBackendSpec(
            backend=canonical,
            config=s3_cfg,
            endpoint_redacted=redact_target_uri(endpoint or f"s3://{bucket}"),
            provider_label="RustFS (S3)" if canonical == "rustfs" else "Floci (AWS S3)",
        )
    if canonical == "floci_gcs":
        gcs_cfg = GCSConfig(
            bucket=bucket,
            endpoint=endpoint,
            allow_http=True,
            skip_signature=True,
        )
        return StorageBackendSpec(
            backend="floci_gcs",
            config=gcs_cfg,
            endpoint_redacted=redact_target_uri(endpoint or f"gs://{bucket}"),
            provider_label="Floci (GCS)",
        )
    if canonical == "floci_oci":
        oci_cfg = OCIConfig(
            bucket=bucket,
            namespace=namespace,
            region=region,
            endpoint=endpoint,
        )
        return StorageBackendSpec(
            backend="floci_oci",
            config=oci_cfg,
            endpoint_redacted=redact_target_uri(endpoint or f"oci://{bucket}@{namespace}"),
            provider_label="Floci (OCI)",
        )
    if canonical == "azurite":
        azure_cfg = AzureConfig(
            container=bucket,
            account=account,
            access_key=secret_access_key,
            endpoint=endpoint,
            use_emulator=endpoint is None,
        )
        default_az_endpoint = f"http://127.0.0.1:4577/{account}/{bucket}"
        return StorageBackendSpec(
            backend="azurite",
            config=azure_cfg,
            endpoint_redacted=redact_target_uri(endpoint or default_az_endpoint),
            provider_label="Azurite (Azure Blob)",
        )

    r2_cfg = R2Config(
        bucket=bucket,
        account_id=account,
        endpoint=endpoint,
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
        allow_http=bool(endpoint and endpoint.startswith("http://")),
    )
    return StorageBackendSpec(
        backend="r2",
        config=r2_cfg,
        endpoint_redacted=redact_target_uri(endpoint or r2_cfg.resolved_endpoint()),
        provider_label="Cloudflare R2",
    )


async def capture_storage_report(
    spec: StorageBackendSpec,
    *,
    iterations: int = 50,
    warmup: int = 5,
    payload_sizes: Sequence[str] = ("4kib", "1mib"),
    include_range_read: bool = True,
    include_external: bool = True,
    strict_free_threading: bool = False,
) -> AdapterBenchmarkReport:
    """Run storage PUT, GET, and optional range-read benchmarks and return a versioned report."""
    if iterations < 1 or warmup < 0:
        msg = f"iterations must be >= 1 and warmup must be >= 0 (got iterations={iterations}, warmup={warmup})"
        raise ValueError(msg)
    if not payload_sizes:
        msg = "payload_sizes must not be empty"
        raise ValueError(msg)

    resolved_sizes: list[tuple[str, int]] = []
    for label in payload_sizes:
        if label not in PAYLOAD_SIZES:
            msg = f"Unsupported payload size {label!r}; expected one of {tuple(PAYLOAD_SIZES)}"
            raise ValueError(msg)
        resolved_sizes.append((label, PAYLOAD_SIZES[label]))

    gil_enabled = assert_free_threaded_gil_disabled(strict=strict_free_threading)
    sync_store = SyncObjectStore(spec.config)
    async_store = sync_store.to_async() if isinstance(spec.config, MemoryConfig) else AsyncObjectStore(spec.config)

    cases: list[AdapterBenchmarkCase] = []
    payloads = {label: _build_payload(size_bytes) for label, size_bytes in resolved_sizes}

    for label, size_bytes in resolved_sizes:
        payload = payloads[label]
        key = f"bench/{spec.backend}/{label}.bin"
        range_len = min(4096, size_bytes)

        sync_put_ns = sample_sync(partial(sync_store.put, key, payload), iterations=iterations, warmup=warmup)
        cases.append(
            compute_adapter_case_stats(
                case=f"put_{label}",
                driver="vertebra-storage-sync",
                category="storage",
                driver_mode="sync_blocking",
                latencies_ns=sync_put_ns,
                gil_enabled=gil_enabled,
            )
        )

        sync_get_ns = sample_sync(partial(sync_store.get, key), iterations=iterations, warmup=warmup)
        cases.append(
            compute_adapter_case_stats(
                case=f"get_{label}",
                driver="vertebra-storage-sync",
                category="storage",
                driver_mode="sync_blocking",
                latencies_ns=sync_get_ns,
                gil_enabled=gil_enabled,
            )
        )

        if include_range_read:
            sync_range_ns = sample_sync(
                partial(sync_store.get_range, key, 0, range_len),
                iterations=iterations,
                warmup=warmup,
            )
            cases.append(
                compute_adapter_case_stats(
                    case=f"range_read_{label}",
                    driver="vertebra-storage-sync",
                    category="storage",
                    driver_mode="sync_blocking",
                    latencies_ns=sync_range_ns,
                    gil_enabled=gil_enabled,
                )
            )

        async_get_ns = await sample_async(partial(async_store.get, key), iterations=iterations, warmup=warmup)
        async_put_ns = await sample_async(
            partial(async_store.put, key, payload),
            iterations=iterations,
            warmup=warmup,
        )
        cases.append(
            compute_adapter_case_stats(
                case=f"put_{label}",
                driver="vertebra-storage-async",
                category="storage",
                driver_mode="async_native",
                latencies_ns=async_put_ns,
                gil_enabled=gil_enabled,
            )
        )
        cases.append(
            compute_adapter_case_stats(
                case=f"get_{label}",
                driver="vertebra-storage-async",
                category="storage",
                driver_mode="async_native",
                latencies_ns=async_get_ns,
                gil_enabled=gil_enabled,
            )
        )
        if include_range_read:
            async_range_ns = await sample_async(
                partial(async_store.get_range, key, 0, range_len),
                iterations=iterations,
                warmup=warmup,
            )
            cases.append(
                compute_adapter_case_stats(
                    case=f"range_read_{label}",
                    driver="vertebra-storage-async",
                    category="storage",
                    driver_mode="async_native",
                    latencies_ns=async_range_ns,
                    gil_enabled=gil_enabled,
                )
            )

    if include_external:
        cases.extend(
            await _run_external_comparators(
                spec,
                resolved_sizes=resolved_sizes,
                payloads=payloads,
                iterations=iterations,
                warmup=warmup,
                include_range_read=include_range_read,
                gil_enabled=gil_enabled,
            )
        )
        gil_enabled = assert_free_threaded_gil_disabled(strict=strict_free_threading)

    return AdapterBenchmarkReport(
        schema_version=ADAPTER_SCHEMA_VERSION,
        category="storage",
        engine=spec.backend,
        generated_at=datetime.now(tz=UTC).isoformat(),
        target_redacted=spec.endpoint_redacted,
        gil_enabled=gil_enabled,
        cases=tuple(cases),
    )


def main(argv: list[str] | None = None) -> int:
    """Run CLI `capture` or `compare` commands for multicloud ObjectStore benchmarks."""
    parser = argparse.ArgumentParser(description="Multicloud ObjectStore benchmark harness and threshold comparator.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    capture_cmd = subparsers.add_parser("capture", help="Run storage benchmark suite and emit JSON.")
    capture_cmd.add_argument(
        "--backend",
        required=True,
        help="Storage backend kind (memory, local, rustfs, floci_s3, floci_gcs, floci_oci, azurite, r2).",
    )
    capture_cmd.add_argument("--root", type=Path, default=None, help="Root directory for local filesystem backend.")
    capture_cmd.add_argument("--bucket", default="bench-bucket", help="Bucket or container name.")
    capture_cmd.add_argument("--endpoint", default=None, help="Optional custom endpoint URL.")
    capture_cmd.add_argument("--region", default="us-east-1", help="Cloud region name.")
    capture_cmd.add_argument("--account", default="devstoreaccount1", help="Azure account or R2 account ID.")
    capture_cmd.add_argument("--namespace", default="bench", help="Memory or OCI namespace.")
    capture_cmd.add_argument("--iterations", type=int, default=50, help="Measured iterations per workload.")
    capture_cmd.add_argument("--warmup", type=int, default=5, help="Warmup iterations per workload.")
    capture_cmd.add_argument("--sizes", default="4kib,1mib", help="Comma-separated payload size labels (4kib,1mib).")
    capture_cmd.add_argument("--no-range", action="store_true", help="Skip range_read workload cases.")
    capture_cmd.add_argument("--no-external", action="store_true", help="Skip optional external comparator libraries.")
    capture_cmd.add_argument(
        "--strict-free-threading",
        action="store_true",
        help="Fail if the Python GIL is active during execution.",
    )
    capture_cmd.add_argument("--output", type=Path, default=None, help="Optional output file path.")

    compare_cmd = subparsers.add_parser("compare", help="Compare candidate and baseline schema_version=1 reports.")
    compare_cmd.add_argument("--candidate", type=Path, required=True, help="Candidate JSON report path.")
    compare_cmd.add_argument("--baseline", type=Path, required=True, help="Baseline JSON report path.")
    compare_cmd.add_argument(
        "--driver-mode",
        choices=("async_native", "sync_blocking"),
        default=None,
        help="Optional driver_mode filter for stratified comparisons.",
    )
    compare_cmd.add_argument("--candidate-driver", default=None, help="Candidate driver label within report.")
    compare_cmd.add_argument("--baseline-driver", default=None, help="Baseline driver label within report.")
    compare_cmd.add_argument(
        "--min-throughput-ratio",
        type=float,
        default=DEFAULT_MIN_THROUGHPUT_RATIO,
        help="Minimum allowed candidate/baseline ops/sec ratio.",
    )
    compare_cmd.add_argument(
        "--max-latency-ratio",
        type=float,
        default=DEFAULT_MAX_LATENCY_RATIO,
        help="Maximum allowed candidate/baseline p99 latency ratio.",
    )

    args = parser.parse_args(argv)
    if args.command == "capture":
        sizes = tuple(part.strip() for part in args.sizes.split(",") if part.strip())
        spec = build_storage_backend_spec(
            args.backend,
            bucket=args.bucket,
            root=args.root,
            endpoint=args.endpoint,
            region=args.region,
            account=args.account,
            namespace=args.namespace,
        )
        report = asyncio.run(
            capture_storage_report(
                spec,
                iterations=args.iterations,
                warmup=args.warmup,
                payload_sizes=sizes,
                include_range_read=not args.no_range,
                include_external=not args.no_external,
                strict_free_threading=args.strict_free_threading,
            )
        )
        encoded = format_adapter_report_bytes(report)
        if args.output is not None:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_bytes(encoded + b"\n")
        else:
            sys.stdout.write(encoded.decode("utf-8") + "\n")
        return 0

    candidate_report = parse_adapter_report_bytes(args.candidate.read_bytes())
    baseline_report = parse_adapter_report_bytes(args.baseline.read_bytes())
    driver_mode = cast("DriverMode | None", args.driver_mode)
    outcomes = compare_adapter_reports(
        candidate_report,
        baseline_report,
        candidate_driver=args.candidate_driver,
        baseline_driver=args.baseline_driver,
        driver_mode=driver_mode,
        min_throughput_ratio=args.min_throughput_ratio,
        max_latency_ratio=args.max_latency_ratio,
    )
    failures = [outcome for outcome in outcomes if not outcome.passed]
    for outcome in outcomes:
        status = "PASS" if outcome.passed else "FAIL"
        line = (
            f"[{status}] {outcome.case} ({outcome.candidate_driver} vs {outcome.baseline_driver}): "
            f"throughput_ratio={outcome.throughput_ratio:.3f}, p99_ratio={outcome.p99_ratio:.3f}"
        )
        if outcome.reason:
            line += f" ({outcome.reason})"
        sys.stdout.write(line + "\n")
    return 1 if failures else 0


def _build_payload(size_bytes: int) -> bytes:
    seed = b"vertebra-storage-bench-0123456789"
    repeats = (size_bytes // len(seed)) + 1
    return (seed * repeats)[:size_bytes]


async def _run_external_comparators(
    spec: StorageBackendSpec,
    *,
    resolved_sizes: Sequence[tuple[str, int]],
    payloads: dict[str, bytes],
    iterations: int,
    warmup: int,
    include_range_read: bool,
    gil_enabled: bool,
) -> list[AdapterBenchmarkCase]:
    cases: list[AdapterBenchmarkCase] = []
    if spec.backend in _OBSTORE_BACKENDS:
        cases.extend(
            await _run_obstore_comparator(
                spec,
                resolved_sizes=resolved_sizes,
                payloads=payloads,
                iterations=iterations,
                warmup=warmup,
                include_range_read=include_range_read,
                gil_enabled=gil_enabled,
            )
        )
    if spec.backend in _S3_COMPAT_BACKENDS:
        cases.extend(
            _run_boto3_comparator(
                spec,
                resolved_sizes=resolved_sizes,
                payloads=payloads,
                iterations=iterations,
                warmup=warmup,
                include_range_read=include_range_read,
                gil_enabled=gil_enabled,
            )
        )
        cases.extend(
            await _run_aiobotocore_comparator(
                spec,
                resolved_sizes=resolved_sizes,
                payloads=payloads,
                iterations=iterations,
                warmup=warmup,
                include_range_read=include_range_read,
                gil_enabled=gil_enabled,
            )
        )
    if spec.backend == "azurite":
        cases.extend(
            await _run_azure_blob_comparator(
                spec,
                resolved_sizes=resolved_sizes,
                payloads=payloads,
                iterations=iterations,
                warmup=warmup,
                include_range_read=include_range_read,
                gil_enabled=gil_enabled,
            )
        )
    return cases


def _build_obstore_handle(obstore: Any, spec: StorageBackendSpec) -> Any | None:
    store_mod = getattr(obstore, "store", None)
    if store_mod is None:
        return None
    cfg = spec.config
    if isinstance(cfg, MemoryConfig):
        return store_mod.MemoryStore()
    if isinstance(cfg, LocalConfig):
        root = Path(cfg.root).resolve()
        root.mkdir(parents=True, exist_ok=True)
        return store_mod.LocalStore(str(root))
    if isinstance(cfg, S3Config):
        opts: dict[str, Any] = {"region": cfg.region, "allow_http": cfg.allow_http}
        if cfg.endpoint:
            opts["endpoint"] = cfg.endpoint
        if cfg.access_key_id:
            opts["access_key_id"] = cfg.access_key_id
        if cfg.secret_access_key:
            opts["secret_access_key"] = cfg.secret_access_key
        return store_mod.S3Store(bucket=cfg.bucket, **opts)
    if isinstance(cfg, GCSConfig):
        opts = {"allow_http": cfg.allow_http, "skip_signature": cfg.skip_signature}
        if cfg.endpoint:
            opts["endpoint"] = cfg.endpoint
        return store_mod.GCSStore(bucket=cfg.bucket, **opts)
    if isinstance(cfg, AzureConfig):
        opts = {"account_name": cfg.account, "use_emulator": cfg.use_emulator}
        if cfg.access_key:
            opts["access_key"] = cfg.access_key
        if cfg.endpoint:
            opts["endpoint"] = cfg.endpoint
        return store_mod.AzureStore(container_name=cfg.container, **opts)
    return None


async def _run_obstore_comparator(
    spec: StorageBackendSpec,
    *,
    resolved_sizes: Sequence[tuple[str, int]],
    payloads: dict[str, bytes],
    iterations: int,
    warmup: int,
    include_range_read: bool,
    gil_enabled: bool,
) -> list[AdapterBenchmarkCase]:
    obstore = import_optional("obstore")
    if obstore is None:
        return []
    store = _build_obstore_handle(obstore, spec)
    if store is None:
        return []

    cases: list[AdapterBenchmarkCase] = []
    for label, size_bytes in resolved_sizes:
        payload = payloads[label]
        key = f"bench/obstore/{label}.bin"
        range_len = min(4096, size_bytes)

        def _sync_get(k: str = key) -> bytes:
            return bytes(obstore.get(store, k).bytes())

        sync_put_ns = sample_sync(
            partial(obstore.put, store, key, payload),
            iterations=iterations,
            warmup=warmup,
        )
        sync_get_ns = sample_sync(_sync_get, iterations=iterations, warmup=warmup)
        cases.append(
            compute_adapter_case_stats(
                case=f"put_{label}",
                driver="obstore-sync",
                category="storage",
                driver_mode="sync_blocking",
                latencies_ns=sync_put_ns,
                gil_enabled=gil_enabled,
            )
        )
        cases.append(
            compute_adapter_case_stats(
                case=f"get_{label}",
                driver="obstore-sync",
                category="storage",
                driver_mode="sync_blocking",
                latencies_ns=sync_get_ns,
                gil_enabled=gil_enabled,
            )
        )
        if include_range_read:
            sync_range_ns = sample_sync(
                partial(obstore.get_range, store, key, start=0, length=range_len),
                iterations=iterations,
                warmup=warmup,
            )
            cases.append(
                compute_adapter_case_stats(
                    case=f"range_read_{label}",
                    driver="obstore-sync",
                    category="storage",
                    driver_mode="sync_blocking",
                    latencies_ns=sync_range_ns,
                    gil_enabled=gil_enabled,
                )
            )

        async def _async_get(k: str = key) -> bytes:
            result = await obstore.get_async(store, k)
            return bytes(await result.bytes_async())

        async_put_ns = await sample_async(
            partial(obstore.put_async, store, key, payload),
            iterations=iterations,
            warmup=warmup,
        )
        async_get_ns = await sample_async(_async_get, iterations=iterations, warmup=warmup)
        cases.append(
            compute_adapter_case_stats(
                case=f"put_{label}",
                driver="obstore-async",
                category="storage",
                driver_mode="async_native",
                latencies_ns=async_put_ns,
                gil_enabled=gil_enabled,
            )
        )
        cases.append(
            compute_adapter_case_stats(
                case=f"get_{label}",
                driver="obstore-async",
                category="storage",
                driver_mode="async_native",
                latencies_ns=async_get_ns,
                gil_enabled=gil_enabled,
            )
        )
        if include_range_read:
            async_range_ns = await sample_async(
                partial(obstore.get_range_async, store, key, start=0, length=range_len),
                iterations=iterations,
                warmup=warmup,
            )
            cases.append(
                compute_adapter_case_stats(
                    case=f"range_read_{label}",
                    driver="obstore-async",
                    category="storage",
                    driver_mode="async_native",
                    latencies_ns=async_range_ns,
                    gil_enabled=gil_enabled,
                )
            )
    return cases


def _s3_client_kwargs(spec: StorageBackendSpec) -> tuple[str, dict[str, Any]] | None:
    cfg = spec.config
    if isinstance(cfg, S3Config):
        kwargs: dict[str, Any] = {
            "region_name": cfg.region,
            "endpoint_url": cfg.endpoint,
            "aws_access_key_id": cfg.access_key_id,
            "aws_secret_access_key": cfg.secret_access_key,
        }
        return cfg.bucket, {k: v for k, v in kwargs.items() if v is not None}
    if isinstance(cfg, R2Config):
        kwargs = {
            "region_name": "auto",
            "endpoint_url": cfg.resolved_endpoint(),
            "aws_access_key_id": cfg.access_key_id,
            "aws_secret_access_key": cfg.secret_access_key,
        }
        return cfg.bucket, {k: v for k, v in kwargs.items() if v is not None}
    return None


def _run_boto3_comparator(
    spec: StorageBackendSpec,
    *,
    resolved_sizes: Sequence[tuple[str, int]],
    payloads: dict[str, bytes],
    iterations: int,
    warmup: int,
    include_range_read: bool,
    gil_enabled: bool,
) -> list[AdapterBenchmarkCase]:
    boto3 = import_optional("boto3")
    if boto3 is None:
        return []
    s3_info = _s3_client_kwargs(spec)
    if s3_info is None:
        return []
    bucket, kwargs = s3_info
    client = boto3.client("s3", **kwargs)

    cases: list[AdapterBenchmarkCase] = []
    for label, size_bytes in resolved_sizes:
        payload = payloads[label]
        key = f"bench/boto3/{label}.bin"
        range_header = f"bytes=0-{min(4096, size_bytes) - 1}"

        def _boto3_get(k: str = key) -> bytes:
            return bytes(client.get_object(Bucket=bucket, Key=k)["Body"].read())

        def _boto3_range(k: str = key, rh: str = range_header) -> bytes:
            return bytes(client.get_object(Bucket=bucket, Key=k, Range=rh)["Body"].read())

        put_ns = sample_sync(
            partial(client.put_object, Bucket=bucket, Key=key, Body=payload),
            iterations=iterations,
            warmup=warmup,
        )
        get_ns = sample_sync(_boto3_get, iterations=iterations, warmup=warmup)
        cases.append(
            compute_adapter_case_stats(
                case=f"put_{label}",
                driver="boto3",
                category="storage",
                driver_mode="sync_blocking",
                latencies_ns=put_ns,
                gil_enabled=gil_enabled,
            )
        )
        cases.append(
            compute_adapter_case_stats(
                case=f"get_{label}",
                driver="boto3",
                category="storage",
                driver_mode="sync_blocking",
                latencies_ns=get_ns,
                gil_enabled=gil_enabled,
            )
        )
        if include_range_read:
            range_ns = sample_sync(_boto3_range, iterations=iterations, warmup=warmup)
            cases.append(
                compute_adapter_case_stats(
                    case=f"range_read_{label}",
                    driver="boto3",
                    category="storage",
                    driver_mode="sync_blocking",
                    latencies_ns=range_ns,
                    gil_enabled=gil_enabled,
                )
            )
    return cases


async def _run_aiobotocore_comparator(
    spec: StorageBackendSpec,
    *,
    resolved_sizes: Sequence[tuple[str, int]],
    payloads: dict[str, bytes],
    iterations: int,
    warmup: int,
    include_range_read: bool,
    gil_enabled: bool,
) -> list[AdapterBenchmarkCase]:
    aiobotocore_session = import_optional("aiobotocore.session")
    if aiobotocore_session is None:
        return []
    s3_info = _s3_client_kwargs(spec)
    if s3_info is None:
        return []
    bucket, kwargs = s3_info

    cases: list[AdapterBenchmarkCase] = []
    session = aiobotocore_session.get_session()
    async with session.create_client("s3", **kwargs) as client:
        for label, size_bytes in resolved_sizes:
            payload = payloads[label]
            key = f"bench/aiobotocore/{label}.bin"
            range_header = f"bytes=0-{min(4096, size_bytes) - 1}"

            async def _get(k: str = key) -> bytes:
                response = await client.get_object(Bucket=bucket, Key=k)
                return bytes(await response["Body"].read())

            async def _get_range(k: str = key, rh: str = range_header) -> bytes:
                response = await client.get_object(Bucket=bucket, Key=k, Range=rh)
                return bytes(await response["Body"].read())

            put_ns = await sample_async(
                partial(client.put_object, Bucket=bucket, Key=key, Body=payload),
                iterations=iterations,
                warmup=warmup,
            )
            get_ns = await sample_async(_get, iterations=iterations, warmup=warmup)
            cases.append(
                compute_adapter_case_stats(
                    case=f"put_{label}",
                    driver="aiobotocore",
                    category="storage",
                    driver_mode="async_native",
                    latencies_ns=put_ns,
                    gil_enabled=gil_enabled,
                )
            )
            cases.append(
                compute_adapter_case_stats(
                    case=f"get_{label}",
                    driver="aiobotocore",
                    category="storage",
                    driver_mode="async_native",
                    latencies_ns=get_ns,
                    gil_enabled=gil_enabled,
                )
            )
            if include_range_read:
                range_ns = await sample_async(_get_range, iterations=iterations, warmup=warmup)
                cases.append(
                    compute_adapter_case_stats(
                        case=f"range_read_{label}",
                        driver="aiobotocore",
                        category="storage",
                        driver_mode="async_native",
                        latencies_ns=range_ns,
                        gil_enabled=gil_enabled,
                    )
                )
    return cases


async def _run_azure_blob_comparator(
    spec: StorageBackendSpec,
    *,
    resolved_sizes: Sequence[tuple[str, int]],
    payloads: dict[str, bytes],
    iterations: int,
    warmup: int,
    include_range_read: bool,
    gil_enabled: bool,
) -> list[AdapterBenchmarkCase]:
    if not isinstance(spec.config, AzureConfig):
        return []
    cfg = spec.config
    account_url = cfg.endpoint or f"http://127.0.0.1:4577/{cfg.account}"
    cases: list[AdapterBenchmarkCase] = []

    azure_blob = import_optional("azure.storage.blob")
    if azure_blob is not None:
        service_client = azure_blob.BlobServiceClient(account_url=account_url, credential=cfg.access_key)
        for label, size_bytes in resolved_sizes:
            payload = payloads[label]
            key = f"bench/azure-sync/{label}.bin"
            range_len = min(4096, size_bytes)
            blob_client = service_client.get_blob_client(container=cfg.container, blob=key)

            def _azure_sync_get(bc: Any = blob_client) -> bytes:
                return bytes(bc.download_blob().readall())

            def _azure_sync_range(bc: Any = blob_client, rl: int = range_len) -> bytes:
                return bytes(bc.download_blob(offset=0, length=rl).readall())

            put_ns = sample_sync(
                partial(blob_client.upload_blob, payload, overwrite=True),
                iterations=iterations,
                warmup=warmup,
            )
            get_ns = sample_sync(_azure_sync_get, iterations=iterations, warmup=warmup)
            cases.append(
                compute_adapter_case_stats(
                    case=f"put_{label}",
                    driver="azure-storage-blob-sync",
                    category="storage",
                    driver_mode="sync_blocking",
                    latencies_ns=put_ns,
                    gil_enabled=gil_enabled,
                )
            )
            cases.append(
                compute_adapter_case_stats(
                    case=f"get_{label}",
                    driver="azure-storage-blob-sync",
                    category="storage",
                    driver_mode="sync_blocking",
                    latencies_ns=get_ns,
                    gil_enabled=gil_enabled,
                )
            )
            if include_range_read:
                range_ns = sample_sync(_azure_sync_range, iterations=iterations, warmup=warmup)
                cases.append(
                    compute_adapter_case_stats(
                        case=f"range_read_{label}",
                        driver="azure-storage-blob-sync",
                        category="storage",
                        driver_mode="sync_blocking",
                        latencies_ns=range_ns,
                        gil_enabled=gil_enabled,
                    )
                )

    azure_blob_aio = import_optional("azure.storage.blob.aio")
    if azure_blob_aio is not None:
        async with azure_blob_aio.BlobServiceClient(account_url=account_url, credential=cfg.access_key) as async_client:
            for label, size_bytes in resolved_sizes:
                payload = payloads[label]
                key = f"bench/azure-async/{label}.bin"
                range_len = min(4096, size_bytes)
                blob_client = async_client.get_blob_client(container=cfg.container, blob=key)

                async def _async_get(bc: Any = blob_client) -> bytes:
                    stream = await bc.download_blob()
                    return bytes(await stream.readall())

                async def _async_range(bc: Any = blob_client, rl: int = range_len) -> bytes:
                    stream = await bc.download_blob(offset=0, length=rl)
                    return bytes(await stream.readall())

                put_ns = await sample_async(
                    partial(blob_client.upload_blob, payload, overwrite=True),
                    iterations=iterations,
                    warmup=warmup,
                )
                get_ns = await sample_async(_async_get, iterations=iterations, warmup=warmup)
                cases.append(
                    compute_adapter_case_stats(
                        case=f"put_{label}",
                        driver="azure-storage-blob-async",
                        category="storage",
                        driver_mode="async_native",
                        latencies_ns=put_ns,
                        gil_enabled=gil_enabled,
                    )
                )
                cases.append(
                    compute_adapter_case_stats(
                        case=f"get_{label}",
                        driver="azure-storage-blob-async",
                        category="storage",
                        driver_mode="async_native",
                        latencies_ns=get_ns,
                        gil_enabled=gil_enabled,
                    )
                )
                if include_range_read:
                    range_ns = await sample_async(_async_range, iterations=iterations, warmup=warmup)
                    cases.append(
                        compute_adapter_case_stats(
                            case=f"range_read_{label}",
                            driver="azure-storage-blob-async",
                            category="storage",
                            driver_mode="async_native",
                            latencies_ns=range_ns,
                            gil_enabled=gil_enabled,
                        )
                    )
    return cases


if __name__ == "__main__":
    raise SystemExit(main())
