# Object Storage

Vertebra provides a unified, GIL-free object storage layer (`vertebra.storage` / `vertebra-storage`) backed by the Rust `object_store` crate and a dedicated multi-threaded Tokio runtime. Both synchronous (`put`, `get`, `head`, `delete`, `list`, `copy`) and asynchronous (`put_async`, `get_async`, `head_async`, `delete_async`, `list_async`, `copy_async`) methods operate over zero-copy Python `Buffer` views across 8 cloud and local backends.

## Storage Backend Matrix

All storage configurations inherit from `msgspec.Struct` (`frozen=True, gc=False`) and compile into canonical backend URLs via `ObjectStore.from_config(...)`:

| Backend | Config Class | Canonical URI Scheme | Key Capabilities & Auth Modes |
| --- | --- | --- | --- |
| **AWS S3 / S3-Compatible** | `S3Config` | `s3://<bucket>` | AWS IAM keys, STS session tokens, virtual-hosted or path style, MinIO/LocalStack/Ceph endpoints |
| **Google Cloud Storage** | `GcsConfig` | `gs://<bucket>` | Service account JSON keys, Application Default Credentials, keyless IAM `signBlob` V4 signing |
| **Azure Blob / ADLS Gen2** | `AzureConfig` | `az://<container>` | Shared account key, SAS tokens, Microsoft Entra ID service principals, Azurite emulator |
| **Cloudflare R2** | `R2Config` | `r2://<bucket>` | Account ID endpoint resolution, `eu` / `fedramp` jurisdictions, uniform multipart chunking |
| **Oracle Cloud (OCI)** | `OciConfig` | `oci://<bucket>@<ns>` | Native OCI REST v2 (`draft-cavage-http-signatures-08` RSA-SHA256 signing without S3 shim) |
| **HTTP / WebDAV** | `HttpConfig` | `https://<origin>` | Read/write over standard HTTP and WebDAV origins with custom static headers |
| **Local Filesystem** | `LocalConfig` | `file://<root>` | Atomic local file persistence with automatic parent directory creation |
| **In-Memory** | `MemoryConfig` | `memory://` | Zero-I/O isolated namespaces for fast unit and integration tests |

## Core Operations

Use `ObjectStore.from_config` to instantiate a backend across synchronous and asynchronous operations:

```python
from vertebra.storage import MemoryConfig, ObjectStore

store = ObjectStore.from_config(MemoryConfig(namespace="uploads"))
store.put("reports/q1.json", b'{"revenue": 125000}')
payload = store.get("reports/q1.json")
meta = store.head("reports/q1.json")
```

## Google Cloud Storage & Application Default Credentials (ADC)

Vertebra provides first-class, zero-config integration with Google Cloud Storage (`GCSConfig`). When deployed in Google Cloud environments (Google Kubernetes Engine, Cloud Run, Cloud Functions, Compute Engine) or during local development with `gcloud`, Vertebra automatically resolves credentials via Google Application Default Credentials (ADC) without requiring manual secret configuration:

```python
import os
from vertebra.storage import GCSConfig, ObjectStore

store = ObjectStore.from_config(GCSConfig(bucket="app-assets-production"))

sa_store = ObjectStore.from_config(
    GCSConfig(
        bucket="app-assets-production",
        service_account_path="/var/secrets/google/key.json",
    )
)

json_store = ObjectStore.from_config(
    GCSConfig(
        bucket="app-assets-production",
        service_account_key=os.environ["GCS_SERVICE_ACCOUNT_JSON"],
    )
)

signing_store = ObjectStore.from_config(
    GcsConfig(
        bucket="app-assets-production",
        service_account_email="backend-service@my-project.iam.gserviceaccount.com",
    )
)
```

- **Zero-Config ADC**: Leaving `service_account_path` and `service_account_key` unset activates native Google Application Default Credentials. The underlying Rust runtime discovers credentials from the GCP metadata server or local user credentials automatically.
- **Keyless V4 Presigning**: Pass `service_account_email` to sign upload and download URLs using Google Cloud IAM's `signBlob` API, eliminating the need to expose long-lived private keys inside runtime containers.
- **DuckDB httpfs Secrets**: Generate DuckDB secret creation DDL matching GCS access tokens via `build_duckdb_gcs_secret_sql`.

## Presigned URLs (`SigningConfig`)

Offload large client uploads and downloads directly to your storage origin using `SigningConfig` (`presign_get`, `presign_put`, `verify_presigned_url`), or compute Azure Blob SAS URLs (`SigningConfig.sign_azure_sas`) and OCI RSA-SHA256 request headers (`SigningConfig.sign_oci_headers`):

```python
from vertebra.storage import ObjectStore, SigningConfig

store = ObjectStore.from_memory(
    namespace="uploads",
    signing=SigningConfig(
        provider="hmac",
        hmac_secret="hmac-signing-key",
        endpoint="https://cdn.example.com",
    ),
)
download_url = store.presign_get("avatars/user-1.png", expires_in_secs=900)
upload_url = store.presign_put("avatars/user-1.png", expires_in_secs=300)
is_valid = store.verify_presigned_url(download_url, method="GET")
```

## Random-Access Range Reads (`ReadableFile`) & PyArrow

`store.open_readable(key)` returns a seekable `ReadableFile` supporting `read_at(offset, length)` (HTTP `Range: bytes=start-end` under the hood). Pass `ReadableFile` directly to PyArrow or Parquet readers to inspect metadata footers and column chunks without downloading entire multi-gigabyte objects:

```python
from vertebra.storage import ObjectStore

store = ObjectStore.from_memory(namespace="analytics")
store.put("events/2026-10.parquet", b"PAR1...footer...PAR1")

with store.open_readable("events/2026-10.parquet") as reader:
    magic_header = reader.read_at(0, 4)
    magic_footer = reader.read_at(reader.size - 4, 4)
```

## Advanced Alchemy Model File Attachments

Integrate storage directly with database models using Advanced Alchemy's `FileObject` and Vertebra's `ObjectStoreBackend`:

```python
from advanced_alchemy.types.file_object import FileObject
from vertebra.extensions.alchemy.storage import (
    ObjectStoreBackend,
    register_storage_backend,
)
from vertebra.storage import ObjectStore

store = ObjectStore("memory://")
backend = register_storage_backend("documents", store=store)

file_obj = FileObject(backend=backend, filename="contract.pdf")
backend.save_object(file_obj, b"%PDF-1.4...")
```

See [`vertebra.storage` API Reference](../reference/storage.md) for full configuration classes and `ObjectStore` methods.
