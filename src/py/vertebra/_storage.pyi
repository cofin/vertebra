"""Type stubs for the vertebra._storage native extension module."""

from collections.abc import Buffer
from typing import Any, final

__all__ = [
    "GcpTokenCache",
    "GcpTokenSnapshot",
    "ObjectMeta",
    "ObjectStore",
    "RustTaskAwaitable",
    "StorageBuffer",
    "py_chunk_r2_uniform_parts",
    "py_sign_aws_sigv4_headers",
    "py_sign_azure_sas_url",
    "py_sign_gcs_v4_iam_url",
    "py_sign_gcs_v4_iam_url_async",
    "py_sign_hmac_url",
    "py_sign_oci_cavage_headers",
    "py_sign_s3_v4_url",
    "py_verify_hmac_url",
    "py_verify_s3_v4_url",
]

@final
class GcpTokenSnapshot:
    def __new__(
        cls,
        *,
        access_token: str,
        expires_at_unix_ms: int,
        generation: int,
        quota_project_id: str | None = None,
    ) -> GcpTokenSnapshot: ...
    @property
    def access_token(self) -> str: ...
    @property
    def expires_at_unix_ms(self) -> int: ...
    @property
    def generation(self) -> int: ...
    @property
    def quota_project_id(self) -> str | None: ...
    def __repr__(self) -> str: ...

@final
class GcpTokenCache:
    def __new__(cls, options: dict[str, str]) -> GcpTokenCache: ...
    @property
    def generation(self) -> int: ...
    @property
    def refresh_skew_ms(self) -> int: ...
    def needs_refresh(self, *, now_unix_ms: int | None = None) -> bool: ...
    def invalidate(self, failed_generation: int) -> None: ...
    def get_token(self, *, force_refresh: bool = False) -> GcpTokenSnapshot: ...
    def get_token_async(self, *, force_refresh: bool = False) -> RustTaskAwaitable: ...

@final
class StorageBuffer:
    def __new__(cls) -> StorageBuffer: ...
    def __buffer__(self, flags: int, /) -> memoryview: ...
    def __len__(self) -> int: ...
    def to_bytes(self) -> bytes: ...

@final
class RustTaskAwaitable:
    def __new__(cls) -> RustTaskAwaitable: ...
    def __await__(self) -> Any: ...

@final
class ObjectMeta:
    def __new__(cls) -> ObjectMeta: ...
    @property
    def path(self) -> str: ...
    @property
    def size(self) -> int: ...
    @property
    def last_modified(self) -> str: ...
    @property
    def etag(self) -> str | None: ...
    @property
    def version(self) -> str | None: ...
    def __repr__(self) -> str: ...

@final
class ObjectStore:
    @property
    def scheme(self) -> str: ...
    def __new__(cls, url: str, options: dict[str, str] | None = None) -> ObjectStore: ...
    def put(self, path: str, data: Buffer | bytes) -> ObjectMeta: ...
    def put_async(self, path: str, data: Buffer | bytes) -> RustTaskAwaitable: ...
    def put_file(self, path: str, file_path: str) -> ObjectMeta: ...
    def put_file_async(self, path: str, file_path: str) -> RustTaskAwaitable: ...
    def put_multipart(self, path: str, parts: list[Buffer | bytes]) -> ObjectMeta: ...
    def put_multipart_async(self, path: str, parts: list[Buffer | bytes]) -> RustTaskAwaitable: ...
    def get(self, path: str) -> bytes: ...
    def get_buffer(self, path: str) -> StorageBuffer: ...
    def get_async(self, path: str) -> RustTaskAwaitable: ...
    def get_buffer_async(self, path: str) -> RustTaskAwaitable: ...
    def get_range(self, path: str, offset: int, length: int) -> bytes: ...
    def get_range_async(self, path: str, offset: int, length: int) -> RustTaskAwaitable: ...
    def head(self, path: str) -> ObjectMeta: ...
    def head_async(self, path: str) -> RustTaskAwaitable: ...
    def exists(self, path: str) -> bool: ...
    def exists_async(self, path: str) -> RustTaskAwaitable: ...
    def delete(self, path: str) -> None: ...
    def delete_async(self, path: str) -> RustTaskAwaitable: ...
    def copy(self, src: str, dst: str) -> None: ...
    def copy_async(self, src: str, dst: str) -> RustTaskAwaitable: ...
    def list(self, prefix: str | None = None) -> list[ObjectMeta]: ...
    def list_async(self, prefix: str | None = None) -> RustTaskAwaitable: ...

def py_sign_hmac_url(
    base_url: str,
    secret: str,
    method: str,
    expires_in_secs: int = 3600,
    timestamp: int | None = None,
) -> str: ...
def py_verify_hmac_url(
    signed_url: str,
    secret: str,
    method: str,
    timestamp: int,
) -> bool: ...
def py_sign_s3_v4_url(
    bucket: str,
    region: str,
    access_key_id: str,
    secret_access_key: str,
    method: str,
    object_path: str,
    expires_in_secs: int = 3600,
    timestamp: int | None = None,
    endpoint: str | None = None,
) -> str: ...
def py_verify_s3_v4_url(
    bucket: str,
    region: str,
    access_key_id: str,
    secret_access_key: str,
    method: str,
    signed_url: str,
    timestamp: int,
    endpoint: str | None = None,
) -> bool: ...
def py_sign_gcs_v4_iam_url(
    bucket: str,
    service_account_email: str,
    method: str,
    object_path: str,
    expires_in_secs: int = 3600,
    timestamp: int | None = None,
    iam_endpoint: str = "https://iamcredentials.googleapis.com",
    access_token: str | None = None,
    private_key_pem: str | None = None,
) -> str: ...
def py_sign_gcs_v4_iam_url_async(
    bucket: str,
    service_account_email: str,
    method: str,
    object_path: str,
    expires_in_secs: int = 3600,
    timestamp: int | None = None,
    iam_endpoint: str = "https://iamcredentials.googleapis.com",
    access_token: str | None = None,
    private_key_pem: str | None = None,
) -> RustTaskAwaitable: ...
def py_sign_oci_cavage_headers(
    tenancy_ocid: str,
    user_ocid: str,
    fingerprint: str,
    private_key_pem: str,
    method: str,
    url: str,
    body: bytes | None = None,
    content_type: str | None = None,
    timestamp: int | None = None,
) -> dict[str, str]: ...
def py_sign_azure_sas_url(
    account: str,
    container: str,
    access_key: str,
    method: str,
    blob_path: str,
    expires_in_secs: int = 3600,
    timestamp: int | None = None,
    endpoint: str | None = None,
) -> str: ...
def py_chunk_r2_uniform_parts(
    data: Buffer | bytes,
    part_size: int = 8388608,
) -> list[StorageBuffer]: ...
def py_sign_aws_sigv4_headers(
    service: str,
    region: str,
    access_key_id: str,
    secret_access_key: str,
    method: str,
    url: str,
    payload: bytes,
    content_type: str = "application/json",
    timestamp: int | None = None,
    session_token: str | None = None,
) -> dict[str, str]: ...
