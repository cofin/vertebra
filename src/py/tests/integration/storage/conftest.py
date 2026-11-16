"""Cloud storage emulator and pytest-databases container fixtures for integration testing."""

from __future__ import annotations

import os
import socket
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from vertebra.storage import (
    AzureConfig,
    GCSConfig,
    LocalConfig,
    MemoryConfig,
    OCIConfig,
    R2Config,
    S3Config,
    SyncObjectStore,
)

if TYPE_CHECKING:
    from pytest_databases.docker.azure_blob import AzureBlobService
    from pytest_databases.docker.floci import FlociAWSService
    from pytest_databases.docker.minio import MinioService
    from pytest_databases.docker.rustfs import RustfsService


def _is_port_open(host: str, port: int, timeout: float = 0.25) -> bool:
    """Return True if a TCP port is reachable on host."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


@pytest.fixture
def rustfs_s3_config(rustfs_service: RustfsService, rustfs_default_bucket_name: str) -> S3Config:
    """Return S3Config targeting the pytest-databases RustFS container."""
    return S3Config(
        bucket=rustfs_default_bucket_name,
        region="us-east-1",
        endpoint=rustfs_service.endpoint_url,
        access_key_id=rustfs_service.access_key,
        secret_access_key=rustfs_service.secret_key,
        allow_http=True,
    )


@pytest.fixture
def rustfs_object_store(rustfs_s3_config: S3Config) -> SyncObjectStore:
    """Return an ObjectStore backed by the pytest-databases RustFS container."""
    return SyncObjectStore.from_config(rustfs_s3_config)


@pytest.fixture
def minio_s3_config(minio_service: MinioService, minio_default_bucket_name: str) -> S3Config:
    """Return S3Config targeting the pytest-databases MinIO container."""
    return S3Config(
        bucket=minio_default_bucket_name,
        region="us-east-1",
        endpoint=minio_service.endpoint_url,
        access_key_id=minio_service.access_key,
        secret_access_key=minio_service.secret_key,
        allow_http=True,
    )


@pytest.fixture
def minio_object_store(minio_s3_config: S3Config) -> SyncObjectStore:
    """Return an ObjectStore backed by the pytest-databases MinIO container."""
    return SyncObjectStore.from_config(minio_s3_config)


@pytest.fixture
def floci_container_s3_config(floci_aws_service: FlociAWSService) -> S3Config:
    """Return S3Config targeting the pytest-databases Floci container."""
    return S3Config(
        bucket=f"{floci_aws_service.resource_prefix}vertebra-test",
        region=floci_aws_service.region,
        endpoint=floci_aws_service.endpoint_url,
        access_key_id=floci_aws_service.access_key,
        secret_access_key=floci_aws_service.secret_key,
        allow_http=True,
    )


@pytest.fixture
def floci_object_store(floci_container_s3_config: S3Config) -> SyncObjectStore:
    """Return an ObjectStore backed by the pytest-databases Floci container."""
    return SyncObjectStore.from_config(floci_container_s3_config)


@pytest.fixture
def azurite_azure_config(azure_blob_service: AzureBlobService, azure_blob_default_container_name: str) -> AzureConfig:
    """Return AzureConfig targeting the pytest-databases Azurite Azure Blob container."""
    return AzureConfig(
        container=azure_blob_default_container_name,
        account=azure_blob_service.account_name,
        access_key=azure_blob_service.account_key,
        endpoint=azure_blob_service.account_url,
        use_emulator=True,
    )


@pytest.fixture
def azurite_object_store(azurite_azure_config: AzureConfig) -> SyncObjectStore:
    """Return an ObjectStore backed by the pytest-databases Azurite Azure Blob container."""
    return SyncObjectStore.from_config(azurite_azure_config)


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
def local_object_store(tmp_path: Path) -> SyncObjectStore:
    """Return a local filesystem ObjectStore for integration tests."""
    return SyncObjectStore.from_config(LocalConfig(root=tmp_path))


@pytest.fixture
def memory_object_store() -> SyncObjectStore:
    """Return an in-memory ObjectStore for integration tests."""
    return SyncObjectStore.from_config(MemoryConfig(namespace="integration"))
