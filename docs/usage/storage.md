# Object Storage Overview & Backends

Vertebra provides a unified, GIL-free object storage layer (`vertebra.storage` / `vertebra-storage`) backed by `object_store` and a dedicated multi-threaded Tokio runtime. Synchronous methods on `SyncObjectStore` release the Python GIL via `py.detach()`, while methods on `AsyncObjectStore` return a `RustStorageAwaitable` resolved on the Tokio runtime via `AsyncCompletionQueue` and `Notifier` (`loop.add_reader`) without blocking the Python event loop.

See the focused guides for [GCS, Active ADC & Presigning](storage-auth-and-signing.md), [Range Reads, Parquet & Attachments](storage-parquet-and-orm.md), and the [`vertebra.storage` API Reference](../reference/storage.md).

## Storage Backend Matrix

All storage configurations inherit from `msgspec.Struct` (`frozen=True, gc=False`) and compile into canonical backend URLs via `SyncObjectStore.from_config(...)` or `AsyncObjectStore.from_config(...)`:

| Backend | Config Class | Canonical URI Scheme | Key Capabilities & Auth Modes |
| --- | --- | --- | --- |
| **AWS S3 / S3-Compatible** | `S3Config` | `s3://<bucket>` | AWS IAM keys, STS session tokens, virtual-hosted or path style, MinIO/LocalStack/Ceph endpoints |
| **Google Cloud Storage** | `GCSConfig` | `gs://<bucket>` | Active `300s`-skew ADC token cache (`RefreshingGcpCredentialProvider`), service account JSON, IAM `signBlob` V4 signing |
| **Azure Blob / ADLS Gen2** | `AzureConfig` | `az://<container>` | Shared account key, SAS tokens, Microsoft Entra ID service principals, Azurite emulator |
| **Cloudflare R2** | `R2Config` | `r2://<bucket>` | Account ID endpoint resolution, `eu` / `fedramp` jurisdictions, uniform `8 MiB` multipart chunking |
| **Oracle Cloud (OCI)** | `OCIConfig` | `oci://<bucket>@<ns>` | Native OCI REST v2 (`draft-cavage-http-signatures-08` RSA-SHA256 signing without S3 shim) |
| **HTTP / WebDAV** | `HTTPConfig` | `https://<origin>` | Read/write over standard HTTP and WebDAV origins with custom static headers |
| **Local Filesystem** | `LocalConfig` | `file://<root>` | Atomic local file persistence with automatic parent directory creation |
| **In-Memory** | `MemoryConfig` | `memory://` | Zero-I/O isolated namespaces for fast unit and integration tests |

## Core Sync & Async Operations

Instantiate synchronous (`SyncObjectStore`) or asynchronous (`AsyncObjectStore`) backends with separate classes and clean, un-suffixed method names (`put`, `get`, `get_view`, `head`, `delete`, `list`, `copy`):

```python
from vertebra.storage import MemoryConfig, SyncObjectStore

store = SyncObjectStore.from_config(MemoryConfig(namespace="uploads"))
store.put("reports/q1.json", b'{"revenue": 125000}')
payload = store.get("reports/q1.json")
view = store.get_view("reports/q1.json")
meta = store.head("reports/q1.json")
```

`get_view` returns a zero-copy `memoryview` over the underlying Rust `StorageBuffer`.
