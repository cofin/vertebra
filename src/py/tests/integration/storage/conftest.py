"""Floci open-source cloud storage emulator fixtures for integration testing."""

from __future__ import annotations

import os
import socket
from pathlib import Path

import pytest

from vertebra.storage import (
    AzureConfig,
    GCSConfig,
    LocalConfig,
    MemoryConfig,
    ObjectStore,
    OCIConfig,
    R2Config,
    S3Config,
)


def _is_port_open(host: str, port: int, timeout: float = 0.25) -> bool:
    """Return True if a TCP port is reachable on host."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


@pytest.fixture
def floci_s3_config() -> S3Config:
    """Return S3Config targeting a local Floci AWS S3 emulator (port 4566)."""
    endpoint = os.environ.get("FLOCI_S3_ENDPOINT", "http://127.0.0.1:4566")
    return S3Config(
        bucket="vertebra-test",
        region="us-east-1",
        endpoint=endpoint,
        access_key_id="test",
        secret_access_key="test",
        allow_http=True,
    )


@pytest.fixture
def floci_azure_config() -> AzureConfig:
    """Return AzureConfig targeting a local Floci Azure Blob emulator (port 4577)."""
    endpoint = os.environ.get("FLOCI_AZURE_ENDPOINT", "http://127.0.0.1:4577/devstoreaccount1")
    return AzureConfig(
        container="vertebra-test",
        account="devstoreaccount1",
        access_key="Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==",
        endpoint=endpoint,
        use_emulator=True,
    )


@pytest.fixture
def floci_gcs_config() -> GCSConfig:
    """Return GCSConfig targeting a local Floci GCS emulator (port 4588)."""
    iam_endpoint = os.environ.get("FLOCI_GCS_ENDPOINT", "http://127.0.0.1:4588")
    return GCSConfig(
        bucket="vertebra-test",
        service_account_email="test@vertebra.iam.gserviceaccount.com",
        iam_endpoint=iam_endpoint,
        access_token="floci-test-token",
    )


@pytest.fixture
def floci_r2_config() -> R2Config:
    """Return R2Config targeting a local Floci S3/R2 emulator (port 4566)."""
    endpoint = os.environ.get("FLOCI_R2_ENDPOINT", "http://127.0.0.1:4566")
    return R2Config(
        bucket="vertebra-r2",
        account_id="testaccount",
        access_key_id="test",
        secret_access_key="test",
        endpoint=endpoint,
        allow_http=True,
    )


@pytest.fixture
def floci_oci_config() -> OCIConfig:
    """Return OCIConfig targeting a local mock OCI Object Storage REST v2 endpoint."""
    endpoint = os.environ.get("FLOCI_OCI_ENDPOINT", "http://127.0.0.1:4599")
    return OCIConfig(
        bucket="vertebra-oci",
        namespace="testns",
        region="us-ashburn-1",
        endpoint=endpoint,
    )


@pytest.fixture
def floci_available() -> dict[str, bool]:
    """Probe availability of local Floci S3, Azure, and GCS emulator ports."""
    return {
        "s3": _is_port_open("127.0.0.1", 4566),
        "azure": _is_port_open("127.0.0.1", 4577),
        "gcs": _is_port_open("127.0.0.1", 4588),
    }


@pytest.fixture
def local_object_store(tmp_path: Path) -> ObjectStore:
    """Return a local filesystem ObjectStore for integration tests."""
    return ObjectStore.from_config(LocalConfig(root=tmp_path))


@pytest.fixture
def memory_object_store() -> ObjectStore:
    """Return an in-memory ObjectStore for integration tests."""
    return ObjectStore.from_config(MemoryConfig(namespace="integration"))
