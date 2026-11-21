"""Unit tests for multicloud ObjectStore standalone benchmark comparators."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from tools.benchmarks.adapters.base import (
    ADAPTER_SCHEMA_VERSION,
    format_adapter_report_bytes,
    parse_adapter_report_bytes,
)
from tools.benchmarks.adapters.storage import (
    PAYLOAD_SIZES,
    build_storage_backend_spec,
    capture_storage_report,
    main,
)

from vertebra.storage import (
    AzureConfig,
    GCSConfig,
    LocalConfig,
    MemoryConfig,
    OCIConfig,
    R2Config,
    S3Config,
)


@pytest.mark.parametrize(
    ("backend_input", "expected_backend", "expected_config_type"),
    [
        ("memory", "memory", MemoryConfig),
        ("local", "local", LocalConfig),
        ("rustfs", "rustfs", S3Config),
        ("floci_s3", "floci_s3", S3Config),
        ("floci_aws", "floci_s3", S3Config),
        ("floci_gcs", "floci_gcs", GCSConfig),
        ("floci_oci", "floci_oci", OCIConfig),
        ("azurite", "azurite", AzureConfig),
        ("r2", "r2", R2Config),
        ("floci_r2", "r2", R2Config),
    ],
)
def test_build_storage_backend_spec_supports_multicloud_and_rejects_minio(
    tmp_path: Path,
    backend_input: str,
    expected_backend: str,
    expected_config_type: type[object],
) -> None:
    """Verify typed multicloud config construction, credential redaction, and MinIO rejection."""
    secret = "test-token-placeholder"
    endpoint = f"http://benchuser:{secret}@127.0.0.1:9000/test-bucket?sig={secret}"
    spec = build_storage_backend_spec(
        backend_input,
        bucket="bench-bucket",
        root=tmp_path,
        endpoint=endpoint if backend_input not in {"memory", "local"} else None,
        region="us-east-1",
        account="devstoreaccount1",
        namespace="bench-ns",
        access_key_id="bench-access-key",
        secret_access_key=secret,
    )
    assert spec.backend == expected_backend
    assert isinstance(spec.config, expected_config_type)
    assert secret not in spec.endpoint_redacted
    assert "?" not in spec.endpoint_redacted
    assert spec.provider_label

    if isinstance(spec.config, GCSConfig):
        assert spec.config.skip_signature is True
        assert spec.config.allow_http is True
    if isinstance(spec.config, R2Config):
        assert spec.config.allow_http is True
    if isinstance(spec.config, AzureConfig):
        assert spec.config.use_emulator is False

    with pytest.raises(ValueError, match="MinIO is not supported"):
        build_storage_backend_spec("minio")
    with pytest.raises(ValueError, match="MinIO is not supported"):
        build_storage_backend_spec("MINIO")
    with pytest.raises(ValueError, match="Unsupported storage benchmark backend"):
        build_storage_backend_spec("unknown-backend")


def test_build_storage_backend_spec_azurite_defaults_to_emulator() -> None:
    """Verify Azurite spec enables emulator mode when no explicit endpoint is provided."""
    spec = build_storage_backend_spec("azurite")
    assert isinstance(spec.config, AzureConfig)
    assert spec.config.use_emulator is True
    assert "4577" in spec.endpoint_redacted


def test_capture_storage_report_memory_runs_offline() -> None:
    """Run live offline MemoryConfig capture proving sync/async to_async() store sharing."""
    spec = build_storage_backend_spec("memory", namespace="offline-mem")
    report = asyncio.run(
        capture_storage_report(
            spec,
            iterations=4,
            warmup=1,
            payload_sizes=("4kib",),
            include_external=False,
        )
    )

    assert report.schema_version == ADAPTER_SCHEMA_VERSION
    assert report.category == "storage"
    assert report.engine == "memory"

    by_driver_and_case = {(entry.driver, entry.case): entry for entry in report.cases}
    expected_cases = ("put_4kib", "get_4kib", "range_read_4kib")
    for case_name in expected_cases:
        sync_entry = by_driver_and_case[("vertebra-storage-sync", case_name)]
        async_entry = by_driver_and_case[("vertebra-storage-async", case_name)]
        assert sync_entry.driver_mode == "sync_blocking"
        assert async_entry.driver_mode == "async_native"
        assert sync_entry.iterations == 4
        assert async_entry.iterations == 4
        assert sync_entry.ops_per_sec > 0.0
        assert async_entry.ops_per_sec > 0.0

    roundtripped = parse_adapter_report_bytes(format_adapter_report_bytes(report))
    assert roundtripped == report


def test_capture_storage_report_local_runs_offline(tmp_path: Path) -> None:
    """Run live offline LocalConfig capture across 4 KiB and 1 MiB put, get, and range_read."""
    spec = build_storage_backend_spec("local", root=tmp_path)
    report = asyncio.run(
        capture_storage_report(
            spec,
            iterations=3,
            warmup=1,
            payload_sizes=("4kib", "1mib"),
            include_external=False,
        )
    )

    assert report.schema_version == ADAPTER_SCHEMA_VERSION
    assert report.category == "storage"
    assert report.engine == "local"
    assert PAYLOAD_SIZES == {"4kib": 4096, "1mib": 1_048_576}

    sync_cases = {entry.case for entry in report.cases if entry.driver == "vertebra-storage-sync"}
    async_cases = {entry.case for entry in report.cases if entry.driver == "vertebra-storage-async"}
    expected = {
        "put_4kib",
        "get_4kib",
        "range_read_4kib",
        "put_1mib",
        "get_1mib",
        "range_read_1mib",
    }
    assert sync_cases == expected
    assert async_cases == expected
    assert len(report.cases) == 12

    parsed = parse_adapter_report_bytes(format_adapter_report_bytes(report))
    assert len(parsed.cases) == 12


@pytest.mark.parametrize(
    ("iterations", "warmup", "payload_sizes"),
    [
        (0, 1, ("4kib",)),
        (3, -1, ("4kib",)),
        (3, 1, ()),
        (3, 1, ("8kib",)),
    ],
)
def test_capture_storage_report_rejects_invalid_arguments(
    iterations: int,
    warmup: int,
    payload_sizes: tuple[str, ...],
) -> None:
    """Verify capture_storage_report rejects invalid iteration counts, warmups, and payload sizes."""
    spec = build_storage_backend_spec("memory")
    with pytest.raises(ValueError):
        asyncio.run(
            capture_storage_report(
                spec,
                iterations=iterations,
                warmup=warmup,
                payload_sizes=payload_sizes,
                include_external=False,
            )
        )


def test_storage_cli_capture_and_compare_offline(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """Verify CLI capture and compare commands run offline against MemoryConfig and return 0."""
    report_path = tmp_path / "storage-memory-report.json"
    capture_rc = main(
        [
            "capture",
            "--backend",
            "memory",
            "--iterations",
            "3",
            "--warmup",
            "1",
            "--sizes",
            "4kib",
            "--no-external",
            "--output",
            str(report_path),
        ]
    )
    assert capture_rc == 0
    assert report_path.is_file()

    parsed = parse_adapter_report_bytes(report_path.read_bytes())
    assert parsed.category == "storage"
    assert parsed.engine == "memory"

    compare_rc = main(
        [
            "compare",
            "--candidate",
            str(report_path),
            "--baseline",
            str(report_path),
            "--driver-mode",
            "async_native",
        ]
    )
    assert compare_rc == 0
    captured = capsys.readouterr()
    assert "[PASS]" in captured.out
    assert "vertebra-storage-async" in captured.out
    assert "vertebra-storage-sync" not in captured.out
