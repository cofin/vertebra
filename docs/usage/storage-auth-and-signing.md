# Storage authentication and signed URLs

Configure signing on a store to generate method-specific presigned URLs:

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

Output: True. HMAC URLs are verified by your own application before serving an object.

`presign_get`, `presign_put`, and the general `sign` method all default to one-hour expiry and have awaitable equivalents on async stores. `verify_presigned_url` verifies HMAC and S3 SigV4 locally; calling it on GCS, Azure, or OCI raises `NotImplementedError`.

## Google Cloud credentials and token caches

Use Application Default Credentials (ADC) in Google Cloud environments, or pass explicit credentials:

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

Credential resolution follows a strict order:

1. Explicit options (access token, service account key or path, or application credentials, plus optional target service account impersonation).
2. Ambient ADC file (`GOOGLE_APPLICATION_CREDENTIALS`, then the local gcloud ADC path).
3. Ambient environment bearer tokens (`GOOGLE_OAUTH_ACCESS_TOKEN` or `CLOUDSDK_AUTH_ACCESS_TOKEN`).
4. GCE metadata server (`metadata.google.internal`).

Import the token cache types from `vertebra.storage`:

```python
from vertebra.storage import (
    AsyncGcpIdTokenCache,
    AsyncGcpTokenCache,
    GcpIdTokenSnapshot,
    GcpTokenSnapshot,
    SyncGcpIdTokenCache,
    SyncGcpTokenCache,
)
```

Build a cache from a GCS config with `sync_token_cache()` or `async_token_cache()`, then pass it as `token_cache` to share one token and refresh schedule across multiple stores:

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

Cache and store behavior:

- **Shared handles**: Converting a cache between sync and async shares the same underlying token state. Passing a sync cache to an async store converts it automatically, and converting a store between sync and async preserves its shared cache.
- **Proactive refresh**: Tokens refresh before expiry (default 300 seconds skew), coalescing concurrent refreshes. If IAM signing or server-side compose gets HTTP 401, the store drops the rejected token and retries once.
- **Quota project**: Setting `quota_project_id` sends the `x-goog-user-project` header on storage and IAM calls.
- **Ambient opt-out and emulators**: Pass `options={"allow_ambient_adc": "false"}` to require explicit credentials. Set `skip_signature=True` for public buckets or emulators. When `STORAGE_EMULATOR_HOST` is set without an explicit endpoint or credentials, the store targets the emulator unsigned.

### Audience-bound ID tokens (Cloud Run and IAP)

Use `SyncGcpIdTokenCache` or `AsyncGcpIdTokenCache` to mint and cache OIDC ID tokens keyed by audience via `get_token`:

```python
from vertebra.embeddings import RemoteEmbedder
from vertebra.storage import SyncGcpIdTokenCache

id_cache = SyncGcpIdTokenCache()
snapshot = id_cache.get_token("https://embed-svc-xyz.a.run.app")
embedder = RemoteEmbedder(
    endpoint="https://embed-svc-xyz.a.run.app",
    id_token_cache=id_cache,
)
```

- **Service-account keys, GCE metadata, and impersonation**: Mint tokens for any audience, including Cloud Run URLs and IAP OAuth client IDs.
- **Developer user ADC**: Google's OAuth endpoint only issues ID tokens whose audience matches the ADC credential's own client ID. Pass `target_service_account` to impersonate a service account for Cloud Run, or add the ADC client ID to your IAP backend's programmatic clients allowlist.

## GCS V4: local RSA or IAM signing

Configuring a service-account identity enables GCS V4 presigned URLs (1 to 604,800 seconds). With a local private key, signing runs locally. With a service account email or target impersonation account and no local key, signing calls IAM Credentials `signBlob` using ADC or an explicit bearer token:

```python
import os
from vertebra.storage import GCSConfig, SyncObjectStore

store = SyncObjectStore(GCSConfig(
    bucket="app-assets",
    service_account_email=os.environ["GCS_SIGNING_SERVICE_ACCOUNT"],
))
upload_url = store.presign_put("incoming/report.parquet", expires_in_secs=900)
```

The calling identity must have permission to invoke `signBlob` for the signing account. Setting a custom `endpoint` changes the host for both data requests and presigned URLs, so generated URLs are valid only at that custom origin.

## S3 SigV4 and R2

`S3Config` with explicit access keys derives SigV4 presigning automatically (1 to 604,800 seconds):

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

- **Endpoints**: Set `endpoint` for S3-compatible services, and `allow_http=True` for local HTTP endpoints.
- **Virtual-hosted style**: Setting `virtual_hosted_style=True` places the bucket in the host subdomain. Any custom endpoint must then be a DNS name rather than an IP address.
- **Cloudflare R2**: `R2Config` derives SigV4 signing using region auto and always targets the R2 S3 API host, even when a custom domain is configured.
- **Overrides**: Pass an explicit `SigningConfig` to override derived settings.

## Azure Blob SAS

`AzureConfig` with an access key derives SAS signing automatically. You can also sign URLs directly without constructing a store:

```python
import os
from vertebra.storage import SigningConfig

upload_url = SigningConfig.sign_azure_sas(
    account="storageaccount", access_key=os.environ["AZURE_STORAGE_KEY"],
    container="incoming", blob_name="report.parquet", method="PUT",
    expires_in_secs=900,
)
```

- GET URLs grant read permission; PUT URLs grant create and write permission.
- Generating a new service SAS requires the Base64 shared key.
- Existing SAS tokens and service principals work for object access; service principals require tenant ID, client ID, and client secret together. Partial or blank credentials raise `ImproperConfigurationError`.

## OCI pre-authenticated requests

Use an existing OCI PAR token or URL prefix with `OCIConfig`:

```python
import os
from vertebra.storage import OCIConfig, SyncObjectStore

store = SyncObjectStore(OCIConfig(
    bucket="app-assets", namespace="tenancy", region="us-ashburn-1",
    par_url_prefix=os.environ["OCI_PAR_URL_PREFIX"],
))
download_url = store.presign_get("reports/latest.parquet", expires_in_secs=900)
```

The prefix ends in `/o/`, and generated URLs append the encoded object key. The PAR's existing permissions and expiry stay authoritative; the expiry argument must be positive but does not extend the PAR.

For native OCI API authentication, pass tenancy OCID, user OCID, key fingerprint, and RSA private key together (partial or blank sets raise `ImproperConfigurationError`). Use `SigningConfig.sign_oci_headers` to generate signed request headers.

See [object storage](storage.md), [Parquet and framework adapters](storage-parquet-and-orm.md), and the [storage API reference](../reference/storage.md).
