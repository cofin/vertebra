# Storage authentication and signed URLs

Configure signing on an explicit store and generate method-specific URLs:

```python
from vertebra.storage import MemoryConfig, SigningConfig, SyncObjectStore

store = SyncObjectStore(
    MemoryConfig(namespace="uploads"),
    signing=SigningConfig(
        provider="hmac", hmac_secret="documentation-example-key",
        endpoint="https://cdn.example.com",
    ),
)
download_url = store.presign_get("avatars/user-1.png", expires_in_secs=900)
upload_url = store.presign_put("avatars/user-1.png", expires_in_secs=300)
print(store.verify_presigned_url(download_url, method="GET"))
```

Output: `True`. HMAC URLs require your application to serve the configured origin and validate the URL before serving an object. The memory store here makes signing locally reproducible.

`presign_get` and `presign_put` return strings and take `expires_in_secs`; `sign(key, expires_in=3600, method="GET")` is the general method; all three default to one-hour expiry. Async stores expose the same methods as awaitables. `verify_presigned_url` verifies HMAC and S3 SigV4 locally; GCS, Azure, and OCI verification raises `NotImplementedError`.

## Google Cloud credentials and token caches

Use ADC when deployed with a Google Cloud identity, or supply explicit credentials:

```python
import os
from vertebra.storage import GCSConfig, SyncObjectStore

config = GCSConfig(
    bucket="app-assets",
    application_credentials=os.environ["GOOGLE_APPLICATION_CREDENTIALS"],
    service_account_email=os.environ["GCS_SIGNING_SERVICE_ACCOUNT"],
)
store = SyncObjectStore(config)
```

`application_credentials` accepts an ADC file. A service-account JSON key can instead be supplied through `service_account_path` or inline `service_account_key`. Without explicit credentials, ADC resolution uses the runtime's configured identity. `access_token` provides an explicit bearer token when the caller manages its lifecycle.

The token cache types come from the core `vertebra._vertebra` extension and are re-exported from `vertebra.storage`:

```python
from vertebra.storage import AsyncGcpTokenCache, GcpTokenSnapshot, SyncGcpTokenCache
```

`GCSConfig.sync_token_cache()` and `async_token_cache()` build them with GCS scopes; both factories accept `allow_ambient_adc` and `scopes`. Without an explicit cache, each GCS store builds its own.

Pass one cache as `token_cache=` to several stores so they share a single token and refresh schedule:

```python
import os
from vertebra.storage import GCSConfig, SyncObjectStore

config = GCSConfig(
    bucket="app-assets",
    service_account_email=os.environ["GCS_SIGNING_SERVICE_ACCOUNT"],
)
cache = config.sync_token_cache()
assets = SyncObjectStore(config, token_cache=cache)
reports = SyncObjectStore(GCSConfig(bucket="app-reports"), token_cache=cache)
```

The cache reaches the storage extension as a named C-ABI capsule, so both modules use the same native cache rather than copies. A store uses it for every GCS request: reads, writes, listings, server-side `compose`, and the IAM `signBlob` call behind keyless presigning. `cache.to_async()` and `cache.to_sync()` return handles over the same cache, and an `AsyncObjectStore` given a `SyncGcpTokenCache` converts it the same way. Converting a store with `.to_async()` / `.to_sync()` shares its engine and its cache.

The cache refreshes tokens before expiry using `refresh_skew_seconds` (default `300`) and coalesces concurrent refreshes. When IAM signing or `compose` receives a `401`, the store discards the rejected token and retries once with a fresh one. `quota_project_id` is sent as the `x-goog-user-project` header on storage and IAM requests.

To stop the store from falling back to ambient credentials (environment variables, the gcloud ADC file, or the metadata server), pass `options={"allow_ambient_adc": "false"}`; only explicitly configured credentials are then used. `skip_signature=True` sends unauthenticated requests for public buckets and emulators. When `STORAGE_EMULATOR_HOST` is set and no `endpoint` or explicit credentials are configured, the store targets the emulator without signing.

## GCS V4: local RSA or IAM signing

A configured service-account identity enables `GOOG4-RSA-SHA256` URLs. With a service-account private key, signing runs locally. With `service_account_email` or `target_service_account` and no local key, signing calls IAM Credentials `signBlob` using ADC or the explicit access token:

```python
import os
from vertebra.storage import GCSConfig, SyncObjectStore

store = SyncObjectStore(GCSConfig(
    bucket="app-assets",
    service_account_email=os.environ["GCS_SIGNING_SERVICE_ACCOUNT"],
))
upload_url = store.presign_put("incoming/report.parquet", expires_in_secs=900)
```

The configured identity needs permission to invoke `signBlob` for the signing service account. `target_service_account` selects keyless impersonation. Both synchronous and asynchronous signing use native credential handling. GCS V4 expiry must be between 1 and 604800 seconds.

`GCSConfig(endpoint=...)` changes the origin of both data requests and presigned URLs, which is useful behind a private gateway or against an emulator. The signature covers the custom host, so the URL is valid only at that origin.

## S3 SigV4 and R2

S3 configurations with explicit access keys derive URL signing automatically:

```python
import os
from vertebra.storage import S3Config, SyncObjectStore

store = SyncObjectStore(S3Config(
    bucket="app-assets", region="us-east-1",
    access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
    secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
))
download_url = store.presign_get("reports/latest.parquet", expires_in_secs=900)
```

SigV4 uses RFC 3986 path encoding and `UNSIGNED-PAYLOAD`; expiry is 1–604800 seconds. Custom S3-compatible origins use `endpoint`; local HTTP endpoints require `allow_http=True`. With `virtual_hosted_style=True`, presigned URLs put the bucket in the host (`https://app-assets.s3.us-east-1.amazonaws.com/key`) instead of the path; a custom `endpoint` must then be a DNS name, because an IP address cannot carry a bucket subdomain. `R2Config` derives SigV4 signing from its access keys and resolved account/jurisdiction endpoint, using region `auto`; presigned R2 URLs always target the R2 S3 API host, even when `custom_domain` is set. An explicit `SigningConfig(provider="s3", ...)` can override signing configuration.

## Azure Blob SAS

`AzureConfig(account=..., container=..., access_key=...)` derives SAS signing from its Base64 shared key. For URL generation without constructing a store:

```python
import os
from vertebra.storage import SigningConfig

upload_url = SigningConfig.sign_azure_sas(
    account="storageaccount", access_key=os.environ["AZURE_STORAGE_KEY"],
    container="incoming", blob_name="report.parquet", method="PUT",
    expires_in_secs=900,
)
```

GET URLs grant read permission; PUT URLs grant create/write permission. `SigningConfig(provider="azure", account=..., container=..., azure_access_key=...)` configures the same operation on a store. SAS tokens and service-principal credentials also configure Azure object access; generating a new service SAS requires the shared signing key.

## OCI pre-authenticated requests

Use an existing OCI PAR token or URL prefix with a native OCI store:

```python
import os
from vertebra.storage import OCIConfig, SyncObjectStore

store = SyncObjectStore(OCIConfig(
    bucket="app-assets", namespace="tenancy", region="us-ashburn-1",
    par_url_prefix=os.environ["OCI_PAR_URL_PREFIX"],
))
download_url = store.presign_get("reports/latest.parquet", expires_in_secs=900)
```

The prefix ends in `/o/`; generated URLs append the encoded object key. The PAR's granted access and provider expiry remain authoritative: `expires_in_secs` must be positive and does not extend an existing PAR. `OCIConfig` also accepts tenancy/user OCIDs, key fingerprint, and an RSA private key for native OCI API authentication. `SigningConfig.sign_oci_headers(...)` produces RSA-SHA256 Cavage request headers; PAR URL generation uses `par_token` or `par_url_prefix`.

See [object storage](storage.md), [Parquet and framework adapters](storage-parquet-and-orm.md), and the [storage API reference](../reference/storage.md).
