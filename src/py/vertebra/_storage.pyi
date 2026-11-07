"""Type stubs for the vertebra._storage native extension module."""

from collections.abc import Buffer
from typing import Any, final

__all__ = [
    "AsyncMultipartUpload",
    "AsyncObjectStore",
    "AsyncStreamReader",
    "ObjectMeta",
    "PreconditionError",
    "RustTaskAwaitable",
    "StorageBuffer",
    "SyncMultipartUpload",
    "SyncObjectStore",
    "SyncStreamReader",
    "assemble_gcs_iam_v4_url",
    "build_gcs_v4_canonical_request",
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
    "sign_oci_par_url",
]

class PreconditionError(PermissionError):
    """An atomic conditional write conflicted with the current object state."""

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
    @property
    def content_type(self) -> str | None: ...
    @property
    def attributes(self) -> dict[str, str]: ...
    def __repr__(self) -> str: ...

@final
class SyncStreamReader:
    def __new__(cls) -> SyncStreamReader: ...
    def __iter__(self) -> SyncStreamReader: ...
    def __next__(self) -> StorageBuffer: ...
    def next_chunk(self) -> StorageBuffer | None: ...
    def close(self) -> None: ...

@final
class AsyncStreamReader:
    def __new__(cls) -> AsyncStreamReader: ...
    def __aiter__(self) -> AsyncStreamReader: ...
    def __anext__(self) -> RustTaskAwaitable: ...
    def next_chunk(self) -> RustTaskAwaitable: ...
    def aclose(self) -> RustTaskAwaitable: ...

@final
class SyncMultipartUpload:
    def __new__(cls) -> SyncMultipartUpload: ...
    def write_part(self, data: Buffer | bytes) -> None: ...
    def complete(self) -> ObjectMeta: ...
    def abort(self) -> None: ...

@final
class AsyncMultipartUpload:
    def __new__(cls) -> AsyncMultipartUpload: ...
    def write_part(self, data: Buffer | bytes) -> RustTaskAwaitable: ...
    def complete(self) -> RustTaskAwaitable: ...
    def abort(self) -> RustTaskAwaitable: ...

@final
class SyncObjectStore:
    def __new__(cls, url: str, options: dict[str, str] | None = None) -> SyncObjectStore: ...
    @property
    def scheme(self) -> str: ...
    def to_async(self) -> AsyncObjectStore: ...
    def put(
        self,
        path: str,
        data: Buffer | bytes,
        *,
        content_type: str | None = None,
        attributes: dict[str, str] | None = None,
        if_not_exists: bool = False,
        if_match: str | None = None,
        if_none_match: str | None = None,
    ) -> ObjectMeta: ...
    def put_file(self, path: str, file_path: str) -> ObjectMeta: ...
    def create_multipart(
        self,
        path: str,
        *,
        content_type: str | None = None,
        attributes: dict[str, str] | None = None,
        if_not_exists: bool = False,
        if_match: str | None = None,
        if_none_match: str | None = None,
    ) -> SyncMultipartUpload: ...
    def put_multipart(self, path: str, parts: list[Buffer | bytes]) -> ObjectMeta: ...
    def get(self, path: str) -> bytes: ...
    def get_buffer(self, path: str) -> StorageBuffer: ...
    def get_stream(self, path: str, *, chunk_size: int | None = None) -> SyncStreamReader: ...
    def get_range(self, path: str, offset: int, length: int) -> bytes: ...
    def head(self, path: str) -> ObjectMeta: ...
    def exists(self, path: str) -> bool: ...
    def delete(self, path: str) -> None: ...
    def copy(self, src: str, dst: str) -> None: ...
    def rename(self, src: str, dst: str, *, if_not_exists: bool = False) -> None: ...
    def list_page(
        self,
        prefix: str | None = None,
        offset: str | None = None,
        limit: int = 1000,
    ) -> tuple[list[ObjectMeta], str | None]: ...
    def list_with_delimiter(
        self,
        prefix: str | None = None,
    ) -> tuple[list[ObjectMeta], list[str]]: ...
    def list(self, prefix: str | None = None) -> list[ObjectMeta]: ...

@final
class AsyncObjectStore:
    def __new__(cls, url: str, options: dict[str, str] | None = None) -> AsyncObjectStore: ...
    @property
    def scheme(self) -> str: ...
    def to_sync(self) -> SyncObjectStore: ...
    def put(
        self,
        path: str,
        data: Buffer | bytes,
        *,
        content_type: str | None = None,
        attributes: dict[str, str] | None = None,
        if_not_exists: bool = False,
        if_match: str | None = None,
        if_none_match: str | None = None,
    ) -> RustTaskAwaitable: ...
    def put_file(self, path: str, file_path: str) -> RustTaskAwaitable: ...
    def create_multipart(
        self,
        path: str,
        *,
        content_type: str | None = None,
        attributes: dict[str, str] | None = None,
        if_not_exists: bool = False,
        if_match: str | None = None,
        if_none_match: str | None = None,
    ) -> RustTaskAwaitable: ...
    def put_multipart(self, path: str, parts: list[Buffer | bytes]) -> RustTaskAwaitable: ...
    def get(self, path: str) -> RustTaskAwaitable: ...
    def get_buffer(self, path: str) -> RustTaskAwaitable: ...
    def get_stream(self, path: str, *, chunk_size: int | None = None) -> AsyncStreamReader: ...
    def get_range(self, path: str, offset: int, length: int) -> RustTaskAwaitable: ...
    def head(self, path: str) -> RustTaskAwaitable: ...
    def exists(self, path: str) -> RustTaskAwaitable: ...
    def delete(self, path: str) -> RustTaskAwaitable: ...
    def copy(self, src: str, dst: str) -> RustTaskAwaitable: ...
    def rename(self, src: str, dst: str, *, if_not_exists: bool = False) -> RustTaskAwaitable: ...
    def list(self, prefix: str | None = None) -> RustTaskAwaitable: ...
    def list_page(
        self,
        prefix: str | None = None,
        offset: str | None = None,
        limit: int = 1000,
    ) -> RustTaskAwaitable: ...
    def list_with_delimiter(
        self,
        prefix: str | None = None,
    ) -> RustTaskAwaitable: ...

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
def sign_oci_par_url(
    bucket: str,
    namespace: str,
    region: str,
    par_token: str,
    object_path: str,
    expires_in_secs: int = 3600,
    endpoint: str | None = None,
) -> str: ...
def build_gcs_v4_canonical_request(
    bucket: str,
    service_account_email: str,
    method: str,
    object_path: str,
    expires_in_secs: int = 3600,
    timestamp: int | None = None,
) -> tuple[str, str, str, str]: ...
def assemble_gcs_iam_v4_url(
    canonical_uri: str,
    canonical_querystring: str,
    signature_hex: str,
    endpoint: str | None = None,
) -> str: ...
