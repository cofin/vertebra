"""Unit tests for default UTC timestamp handling across storage URL signing functions."""

from __future__ import annotations

import base64
import calendar
import time
from urllib.parse import parse_qs, urlparse

from vertebra.storage import ObjectStore, SigningConfig


def test_signing_functions_default_to_current_utc_timestamp() -> None:
    """Verify signing functions default to current UTC timestamp rather than a static epoch."""
    before_ts = int(time.time())
    native_mod = ObjectStore._get_native_module()

    if native_mod is not None:
        hmac_url = native_mod.py_sign_hmac_url(
            "https://cdn.example.com/assets/file.bin",
            "test-hmac-secret-key-32-bytes-ok",
            "GET",
            600,
        )
        hmac_params = parse_qs(urlparse(hmac_url).query)
        hmac_expires = int(hmac_params["X-Vtb-Expires"][0])
        assert hmac_expires >= before_ts + 600
        assert native_mod.py_verify_hmac_url(
            hmac_url,
            "test-hmac-secret-key-32-bytes-ok",
            "GET",
            before_ts + 10,
        )

        s3_url = native_mod.py_sign_s3_v4_url(
            "artifacts",
            "us-east-1",
            "AKIAIOSFODNN7EXAMPLE",
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            "GET",
            "models/weights.bin",
            900,
        )
        s3_params = parse_qs(urlparse(s3_url).query)
        amz_date = s3_params["X-Amz-Date"][0]
        s3_signed_ts = int(calendar.timegm(time.strptime(amz_date, "%Y%m%dT%H%M%SZ")))
        assert s3_signed_ts >= before_ts - 2

        azure_key = base64.b64encode(b"0123456789abcdef0123456789abcdef").decode("ascii")
        sas_url = native_mod.py_sign_azure_sas_url(
            "devstoreaccount1",
            "datasets",
            azure_key,
            "GET",
            "table.parquet",
            1200,
        )
        sas_params = parse_qs(urlparse(sas_url).query)
        sas_expiry_ts = int(calendar.timegm(time.strptime(sas_params["se"][0], "%Y-%m-%dT%H:%M:%SZ")))
        assert sas_expiry_ts >= before_ts + 1198

    store = ObjectStore(
        "memory://",
        signing=SigningConfig(
            provider="hmac",
            hmac_secret="test-hmac-secret-key-32-bytes-ok",
            endpoint="https://cdn.example.com",
        ),
    )
    presigned = store.presign_get("assets/file.bin", expires_in_secs=600)
    presigned_params = parse_qs(urlparse(presigned).query)
    assert int(presigned_params["X-Vtb-Expires"][0]) >= before_ts + 600
    assert store.verify_presigned_url(presigned, method="GET")
