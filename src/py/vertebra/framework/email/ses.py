"""AWS SES v2 transactional email transport with SigV4 signing."""

from __future__ import annotations

import base64
import contextlib
import datetime
import hashlib
import hmac
import time
import urllib.parse
import uuid
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from vertebra.framework.email.base import EmailReceipt
from vertebra.framework.email.providers import (
    HttpTransportClient,
    _RetryableRestTransport,
)
from vertebra.framework.email.smtp import SMTPEmailTransport
from vertebra.utils.serializers import decode_json, encode_json_bytes

_py_sign_headers: Any = None
with contextlib.suppress(ImportError, AttributeError):
    from vertebra._storage import py_sign_aws_sigv4_headers as _py_sign_headers

if TYPE_CHECKING:
    from vertebra.framework.email.base import EmailConfig, EmailMessage

__all__ = ("SESEmailTransport",)


class SESEmailTransport(_RetryableRestTransport):
    """Transactional email transport for AWS SES v2 REST API."""

    __slots__ = ("_endpoint_url", "_smtp_helper")

    def __init__(
        self,
        config: EmailConfig,
        *,
        client: HttpTransportClient | None = None,
    ) -> None:
        """Initialize SES v2 transport."""
        super().__init__(config, "ses", client=client)
        if config.api_base_url:
            base = config.api_base_url.rstrip("/")
            self._endpoint_url = f"{base}/v2/email/outbound-emails"
        else:
            self._endpoint_url = f"https://email.{config.aws_region}.amazonaws.com/v2/email/outbound-emails"
        self._smtp_helper = SMTPEmailTransport(config)

    def _build_payload(self, message: EmailMessage) -> dict[str, Any]:
        """Convert EmailMessage into SES v2 outbound email request payload."""
        dest: dict[str, list[str]] = {
            "ToAddresses": list(message.to),
        }
        if message.cc:
            dest["CcAddresses"] = list(message.cc)
        if message.bcc:
            dest["BccAddresses"] = list(message.bcc)

        payload: dict[str, Any] = {
            "FromEmailAddress": message.from_email,
            "Destination": dest,
        }
        if message.reply_to:
            payload["ReplyToAddresses"] = [message.reply_to]
        if self._config.configuration_set_name:
            payload["ConfigurationSetName"] = self._config.configuration_set_name
        if message.tags:
            payload["EmailTags"] = [{"Name": k, "Value": v} for k, v in message.tags.items()]

        if message.attachments or message.headers:
            mime_msg = self._smtp_helper.build_mime_message(message)
            raw_bytes = mime_msg.as_bytes()
            b64_raw = base64.b64encode(raw_bytes).decode("ascii")
            payload["Content"] = {"Raw": {"Data": b64_raw}}
        else:
            body_dict: dict[str, Any] = {}
            if message.text_body:
                body_dict["Text"] = {"Data": message.text_body, "Charset": "UTF-8"}
            if message.html_body:
                body_dict["Html"] = {"Data": message.html_body, "Charset": "UTF-8"}
            if not body_dict:
                body_dict["Text"] = {"Data": "", "Charset": "UTF-8"}

            payload["Content"] = {
                "Simple": {
                    "Subject": {"Data": message.subject, "Charset": "UTF-8"},
                    "Body": body_dict,
                }
            }

        return payload

    def _sign_headers(
        self,
        endpoint_url: str,
        body_bytes: bytes,
        *,
        timestamp: int | None = None,
    ) -> dict[str, str]:
        """Sign request headers using vertebra._storage or Python fallback."""
        access_key = self._config.aws_access_key_id or ""
        secret_key = self._config.aws_secret_access_key or ""
        session_token = self._config.aws_session_token

        if _py_sign_headers is not None:
            try:
                res: dict[str, str] = _py_sign_headers(
                    "ses",
                    self._config.aws_region,
                    access_key,
                    secret_key,
                    "POST",
                    endpoint_url,
                    body_bytes,
                    "application/json",
                    timestamp,
                    session_token,
                )
                return res
            except Exception:
                pass

        return self._fallback_sigv4(
            endpoint_url,
            body_bytes,
            access_key,
            secret_key,
            session_token,
            timestamp,
        )

    def _fallback_sigv4(
        self,
        endpoint_url: str,
        body_bytes: bytes,
        access_key: str,
        secret_key: str,
        session_token: str | None,
        timestamp: int | None,
    ) -> dict[str, str]:
        """Pure-Python AWS SigV4 header signing fallback."""
        now_ts = int(time.time()) if timestamp is None else timestamp
        dt = datetime.datetime.fromtimestamp(now_ts, tz=datetime.UTC)
        amz_date = dt.strftime("%Y%m%dT%H%M%SZ")
        datestamp = dt.strftime("%Y%m%d")

        parsed = urllib.parse.urlparse(endpoint_url)
        host = parsed.netloc

        payload_hash = hashlib.sha256(body_bytes).hexdigest()
        canonical_headers = (
            f"content-type:application/json\nhost:{host}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{amz_date}\n"
        )
        if session_token:
            canonical_headers += f"x-amz-security-token:{session_token}\n"
            signed_headers = "content-type;host;x-amz-content-sha256;x-amz-date;x-amz-security-token"
        else:
            signed_headers = "content-type;host;x-amz-content-sha256;x-amz-date"

        canonical_uri = parsed.path or "/"
        canonical_querystring = parsed.query or ""
        canonical_request = (
            f"POST\n{canonical_uri}\n{canonical_querystring}\n{canonical_headers}\n{signed_headers}\n{payload_hash}"
        )

        credential_scope = f"{datestamp}/{self._config.aws_region}/ses/aws4_request"
        string_to_sign = (
            f"AWS4-HMAC-SHA256\n{amz_date}\n{credential_scope}\n"
            f"{hashlib.sha256(canonical_request.encode()).hexdigest()}"
        )

        k_date = hmac.new(
            f"AWS4{secret_key}".encode(),
            datestamp.encode(),
            hashlib.sha256,
        ).digest()
        k_region = hmac.new(
            k_date,
            self._config.aws_region.encode(),
            hashlib.sha256,
        ).digest()
        k_service = hmac.new(k_region, b"ses", hashlib.sha256).digest()
        k_signing = hmac.new(k_service, b"aws4_request", hashlib.sha256).digest()
        signature = hmac.new(
            k_signing,
            string_to_sign.encode(),
            hashlib.sha256,
        ).hexdigest()

        auth_header = (
            f"AWS4-HMAC-SHA256 Credential={access_key}/{credential_scope}, "
            f"SignedHeaders={signed_headers}, Signature={signature}"
        )

        headers = {
            "Authorization": auth_header,
            "content-type": "application/json",
            "host": host,
            "x-amz-date": amz_date,
            "x-amz-content-sha256": payload_hash,
        }
        if session_token:
            headers["x-amz-security-token"] = session_token
        return headers

    async def send(
        self,
        message: EmailMessage,
        *,
        timestamp: int | None = None,
    ) -> EmailReceipt:
        """Dispatch single message via AWS SES v2 outbound-emails endpoint."""
        payload = self._build_payload(message)
        body = encode_json_bytes(payload)
        headers = self._sign_headers(self._endpoint_url, body, timestamp=timestamp)
        resp = await self._request_with_retry(
            "POST",
            self._endpoint_url,
            headers=headers,
            body=body,
        )

        data = decode_json(resp.body)
        raw_dict = data if isinstance(data, dict) else {}
        msg_id = str(raw_dict.get("MessageId") or uuid.uuid4().hex)
        return EmailReceipt(
            message_id=msg_id,
            provider="ses",
            accepted_recipients=tuple(message.to),
            raw_response=raw_dict,
        )

    async def send_batch(self, messages: Sequence[EmailMessage]) -> list[EmailReceipt]:
        """Dispatch multiple messages sequentially."""
        receipts: list[EmailReceipt] = []
        for msg in messages:
            receipt = await self.send(msg)
            receipts.append(receipt)
        return receipts
