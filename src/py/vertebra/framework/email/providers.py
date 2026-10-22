"""REST API transports and webhook verification for transactional email providers."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

import msgspec

from vertebra.framework.email.base import (
    EmailConfig,
    EmailDeliveryError,
    EmailMessage,
    EmailReceipt,
    WebhookEvent,
)
from vertebra.utils.serializers import decode_json, encode_json_bytes

if TYPE_CHECKING:
    pass

__all__ = (
    "HttpResponseData",
    "HttpTransportClient",
    "MailgunEmailTransport",
    "ResendEmailTransport",
    "SendGridEmailTransport",
    "UrllibHttpTransportClient",
    "WebhookVerifier",
)


class HttpResponseData(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Raw HTTP response data returned by HttpTransportClient."""

    status_code: int
    headers: dict[str, str]
    body: bytes


@runtime_checkable
class HttpTransportClient(Protocol):
    """Protocol for asynchronous HTTP client execution."""

    async def request(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str],
        body: bytes,
        timeout_seconds: float = 10.0,
    ) -> HttpResponseData:
        """Execute HTTP request and return status, headers, and body."""
        ...


class UrllibHttpTransportClient:
    """Standard library HTTP client executing requests in background thread."""

    __slots__ = ()

    def _sync_request(
        self,
        method: str,
        url: str,
        headers: Mapping[str, str],
        body: bytes,
        timeout_seconds: float,
    ) -> HttpResponseData:
        """Execute blocking urllib HTTP call."""
        req = urllib.request.Request(
            url,
            data=body if method.upper() in ("POST", "PUT", "PATCH") else None,
            headers=dict(headers),
            method=method.upper(),
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:
                resp_headers = {k.lower(): v for k, v in resp.headers.items()}
                return HttpResponseData(
                    status_code=resp.status,
                    headers=resp_headers,
                    body=resp.read(),
                )
        except urllib.error.HTTPError as exc:
            exc_headers = {k.lower(): v for k, v in exc.headers.items()}
            return HttpResponseData(
                status_code=exc.code,
                headers=exc_headers,
                body=exc.read(),
            )
        except urllib.error.URLError as exc:
            msg = f"Network connection failed: {exc.reason}"
            raise OSError(msg) from exc

    async def request(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str],
        body: bytes,
        timeout_seconds: float = 10.0,
    ) -> HttpResponseData:
        """Run blocking urllib call on worker thread."""
        return await asyncio.to_thread(
            self._sync_request,
            method,
            url,
            headers,
            body,
            timeout_seconds,
        )


class _RetryableRestTransport:
    """Shared base class providing exponential backoff and Retry-After retry logic."""

    __slots__ = ("_client", "_config", "_provider_name")

    def __init__(
        self,
        config: EmailConfig,
        provider_name: str,
        *,
        client: HttpTransportClient | None = None,
    ) -> None:
        """Initialize transport with configuration and client."""
        self._config = config
        self._provider_name = provider_name
        self._client: HttpTransportClient = client or UrllibHttpTransportClient()

    async def _request_with_retry(
        self,
        method: str,
        url: str,
        *,
        headers: dict[str, str],
        body: bytes,
    ) -> HttpResponseData:
        """Dispatch HTTP request with bounded retry and backoff."""
        max_retries = max(0, self._config.max_retries)
        last_error: Exception | None = None

        for attempt in range(max_retries + 1):
            try:
                resp = await self._client.request(
                    method,
                    url,
                    headers=headers,
                    body=body,
                    timeout_seconds=self._config.timeout_seconds,
                )
                if 200 <= resp.status_code < 300:
                    return resp

                is_retryable = resp.status_code in (429, 500, 502, 503, 504)
                if not is_retryable or attempt >= max_retries:
                    err_msg = (
                        f"{self._provider_name} request failed with status {resp.status_code}: "
                        f"{resp.body.decode('utf-8', errors='replace')}"
                    )
                    raise EmailDeliveryError(
                        err_msg,
                        provider=self._provider_name,
                        status_code=resp.status_code,
                        retryable=is_retryable,
                    )

                backoff = self._compute_backoff(resp.headers, attempt)
                await asyncio.sleep(backoff)
            except EmailDeliveryError:
                raise
            except (OSError, TimeoutError) as exc:
                last_error = exc
                if attempt >= max_retries:
                    raise EmailDeliveryError(
                        f"{self._provider_name} connection failed after {attempt} retries: {exc}",
                        provider=self._provider_name,
                        status_code=None,
                        retryable=True,
                    ) from exc
                backoff = self._config.initial_backoff_seconds * (2**attempt)
                await asyncio.sleep(backoff)

        msg = f"{self._provider_name} request exhausted retry budget"
        raise EmailDeliveryError(
            msg,
            provider=self._provider_name,
            status_code=None,
            retryable=True,
        ) from last_error

    def _compute_backoff(self, headers: Mapping[str, str], attempt: int) -> float:
        """Parse Retry-After header or return exponential backoff."""
        retry_after = headers.get("retry-after")
        if retry_after:
            try:
                return max(0.0, float(retry_after.strip()))
            except ValueError:
                pass
        return float(self._config.initial_backoff_seconds * (2**attempt))

    async def close(self) -> None:
        """No-op resource release for stateless REST transport."""


class ResendEmailTransport(_RetryableRestTransport):
    """Transactional email transport for Resend REST API."""

    __slots__ = ("_base_url",)

    def __init__(
        self,
        config: EmailConfig,
        *,
        client: HttpTransportClient | None = None,
    ) -> None:
        """Initialize Resend transport validating API key."""
        if not config.api_key:
            msg = "ResendEmailTransport requires EmailConfig.api_key"
            raise ValueError(msg)
        super().__init__(config, "resend", client=client)
        self._base_url = (config.api_base_url or "https://api.resend.com").rstrip("/")

    def _build_payload(self, message: EmailMessage) -> dict[str, Any]:
        """Convert EmailMessage into Resend JSON payload."""
        payload: dict[str, Any] = {
            "from": message.from_email,
            "to": list(message.to),
            "subject": message.subject,
        }
        if message.cc:
            payload["cc"] = list(message.cc)
        if message.bcc:
            payload["bcc"] = list(message.bcc)
        if message.reply_to:
            payload["reply_to"] = message.reply_to
        if message.text_body:
            payload["text"] = message.text_body
        if message.html_body:
            payload["html"] = message.html_body
        if message.headers:
            payload["headers"] = dict(message.headers)
        if message.attachments:
            payload["attachments"] = [
                {
                    "filename": a.filename,
                    "content": base64.b64encode(a.content).decode("ascii"),
                }
                for a in message.attachments
            ]
        if message.tags:
            payload["tags"] = [{"name": k, "value": v} for k, v in message.tags.items()]
        return payload

    async def send(self, message: EmailMessage) -> EmailReceipt:
        """Dispatch single message via Resend /emails endpoint."""
        url = f"{self._base_url}/emails"
        headers: dict[str, str] = {
            "authorization": f"Bearer {self._config.api_key}",
            "content-type": "application/json",
        }
        if message.idempotency_key:
            headers["idempotency-key"] = message.idempotency_key

        payload = self._build_payload(message)
        body = encode_json_bytes(payload)
        resp = await self._request_with_retry("POST", url, headers=headers, body=body)

        data = decode_json(resp.body)
        raw_dict = data if isinstance(data, dict) else {}
        msg_id = str(raw_dict.get("id") or uuid.uuid4().hex)
        return EmailReceipt(
            message_id=msg_id,
            provider="resend",
            accepted_recipients=tuple(message.to),
            raw_response=raw_dict,
        )

    async def send_batch(self, messages: Sequence[EmailMessage]) -> list[EmailReceipt]:
        """Dispatch multiple messages via native Resend /emails/batch endpoint."""
        if not messages:
            return []

        url = f"{self._base_url}/emails/batch"
        headers: dict[str, str] = {
            "authorization": f"Bearer {self._config.api_key}",
            "content-type": "application/json",
        }
        payload = [self._build_payload(m) for m in messages]
        body = encode_json_bytes(payload)
        resp = await self._request_with_retry("POST", url, headers=headers, body=body)

        data = decode_json(resp.body)
        items: list[Any] = []
        if isinstance(data, dict) and "data" in data and isinstance(data["data"], list):
            items = data["data"]
        elif isinstance(data, list):
            items = data

        receipts: list[EmailReceipt] = []
        for idx, msg in enumerate(messages):
            item_dict: dict[str, Any] = {}
            if idx < len(items) and isinstance(items[idx], dict):
                item_dict = items[idx]
            msg_id = str(item_dict.get("id") or uuid.uuid4().hex)
            receipts.append(
                EmailReceipt(
                    message_id=msg_id,
                    provider="resend",
                    accepted_recipients=tuple(msg.to),
                    raw_response=item_dict,
                )
            )
        return receipts


class SendGridEmailTransport(_RetryableRestTransport):
    """Transactional email transport for SendGrid v3 mail/send API."""

    __slots__ = ("_base_url",)

    def __init__(
        self,
        config: EmailConfig,
        *,
        client: HttpTransportClient | None = None,
    ) -> None:
        """Initialize SendGrid transport validating API key."""
        if not config.api_key:
            msg = "SendGridEmailTransport requires EmailConfig.api_key"
            raise ValueError(msg)
        super().__init__(config, "sendgrid", client=client)
        self._base_url = (config.api_base_url or "https://api.sendgrid.com").rstrip("/")

    def _build_payload(self, message: EmailMessage) -> dict[str, Any]:
        """Construct SendGrid v3 JSON payload."""
        personalization: dict[str, Any] = {
            "to": [{"email": addr} for addr in message.to],
        }
        if message.cc:
            personalization["cc"] = [{"email": addr} for addr in message.cc]
        if message.bcc:
            personalization["bcc"] = [{"email": addr} for addr in message.bcc]
        if message.metadata:
            personalization["custom_args"] = dict(message.metadata)

        contents: list[dict[str, str]] = []
        if message.text_body:
            contents.append({"type": "text/plain", "value": message.text_body})
        if message.html_body:
            contents.append({"type": "text/html", "value": message.html_body})
        if not contents:
            contents.append({"type": "text/plain", "value": ""})

        payload: dict[str, Any] = {
            "personalizations": [personalization],
            "from": {"email": message.from_email},
            "subject": message.subject,
            "content": contents,
        }
        if message.reply_to:
            payload["reply_to"] = {"email": message.reply_to}
        if message.headers:
            payload["headers"] = dict(message.headers)
        if message.attachments:
            payload["attachments"] = [
                {
                    "content": base64.b64encode(a.content).decode("ascii"),
                    "filename": a.filename,
                    "type": a.content_type,
                }
                for a in message.attachments
            ]
        return payload

    async def send(self, message: EmailMessage) -> EmailReceipt:
        """Dispatch single message via SendGrid v3 /mail/send endpoint."""
        url = f"{self._base_url}/v3/mail/send"
        headers: dict[str, str] = {
            "authorization": f"Bearer {self._config.api_key}",
            "content-type": "application/json",
        }
        payload = self._build_payload(message)
        body = encode_json_bytes(payload)
        resp = await self._request_with_retry("POST", url, headers=headers, body=body)

        msg_id = resp.headers.get("x-message-id") or uuid.uuid4().hex
        return EmailReceipt(
            message_id=msg_id,
            provider="sendgrid",
            accepted_recipients=tuple(message.to),
            raw_response={"status_code": resp.status_code, "x_message_id": msg_id},
        )

    async def send_batch(self, messages: Sequence[EmailMessage]) -> list[EmailReceipt]:
        """Dispatch multiple messages sequentially."""
        receipts: list[EmailReceipt] = []
        for msg in messages:
            receipt = await self.send(msg)
            receipts.append(receipt)
        return receipts


class MailgunEmailTransport(_RetryableRestTransport):
    """Transactional email transport for Mailgun v3 messages API."""

    __slots__ = ("_auth_header", "_base_url", "_domain")

    def __init__(
        self,
        config: EmailConfig,
        *,
        client: HttpTransportClient | None = None,
    ) -> None:
        """Initialize Mailgun transport validating API key and domain."""
        if not config.api_key:
            msg = "MailgunEmailTransport requires EmailConfig.api_key"
            raise ValueError(msg)
        if not config.mailgun_domain:
            msg = "MailgunEmailTransport requires EmailConfig.mailgun_domain"
            raise ValueError(msg)
        super().__init__(config, "mailgun", client=client)
        self._domain = config.mailgun_domain
        self._base_url = (config.api_base_url or "https://api.mailgun.net").rstrip("/")
        encoded_creds = base64.b64encode(f"api:{config.api_key}".encode()).decode("ascii")
        self._auth_header = f"Basic {encoded_creds}"

    def _build_multipart(
        self,
        message: EmailMessage,
    ) -> tuple[str, bytes]:
        """Encode form fields and attachments into multipart/form-data payload."""
        boundary = f"----VertebraBoundary{uuid.uuid4().hex}"
        chunks: list[bytes] = []

        def append_field(name: str, val: str) -> None:
            part = (f'--{boundary}\r\nContent-Disposition: form-data; name="{name}"\r\n\r\n{val}\r\n').encode()
            chunks.append(part)

        append_field("from", message.from_email)
        for recipient in message.to:
            append_field("to", recipient)
        for recipient in message.cc:
            append_field("cc", recipient)
        for recipient in message.bcc:
            append_field("bcc", recipient)
        append_field("subject", message.subject)
        if message.text_body:
            append_field("text", message.text_body)
        if message.html_body:
            append_field("html", message.html_body)
        if message.reply_to:
            append_field("h:Reply-To", message.reply_to)
        if message.idempotency_key:
            append_field("h:X-Idempotency-Key", message.idempotency_key)

        for k, v in message.headers.items():
            append_field(f"h:{k}", v)
        for k, v in message.tags.items():
            append_field("o:tag", f"{k}:{v}")
        for k, v in message.metadata.items():
            append_field(f"v:{k}", v)

        for att in message.attachments:
            disp_type = "inline" if att.content_id else "attachment"
            part_header = (
                f"--{boundary}\r\n"
                f'Content-Disposition: form-data; name="{disp_type}"; filename="{att.filename}"\r\n'
                f"Content-Type: {att.content_type}\r\n\r\n"
            ).encode()
            chunks.append(part_header)
            chunks.append(att.content)
            chunks.append(b"\r\n")

        chunks.append(f"--{boundary}--\r\n".encode())
        return f"multipart/form-data; boundary={boundary}", b"".join(chunks)

    async def send(self, message: EmailMessage) -> EmailReceipt:
        """Dispatch single message via Mailgun messages endpoint."""
        url = f"{self._base_url}/v3/{self._domain}/messages"
        content_type, body = self._build_multipart(message)
        headers: dict[str, str] = {
            "authorization": self._auth_header,
            "content-type": content_type,
        }
        resp = await self._request_with_retry("POST", url, headers=headers, body=body)

        data = decode_json(resp.body)
        raw_dict = data if isinstance(data, dict) else {}
        msg_id = str(raw_dict.get("id") or uuid.uuid4().hex)
        return EmailReceipt(
            message_id=msg_id,
            provider="mailgun",
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


class WebhookVerifier:
    """Constant-time HMAC cryptographic verifier for provider incoming webhooks."""

    __slots__ = ()

    @staticmethod
    def _normalize_payload(payload: bytes | str) -> tuple[bytes, str]:
        """Convert payload to both raw bytes and decoded string."""
        if isinstance(payload, bytes):
            return payload, payload.decode("utf-8", errors="replace")
        return payload.encode("utf-8"), payload

    @staticmethod
    def verify_resend(
        payload: bytes | str,
        headers: Mapping[str, str],
        secret: str,
        *,
        now: int | None = None,
        tolerance_seconds: int = 300,
    ) -> WebhookEvent:
        """Verify Svix-signed Resend webhook using HMAC-SHA256."""
        header_map = {k.lower(): v for k, v in headers.items()}
        svix_id = header_map.get("svix-id")
        svix_timestamp = header_map.get("svix-timestamp")
        svix_signature = header_map.get("svix-signature")

        if not svix_id or not svix_timestamp or not svix_signature:
            msg = "Missing required Svix webhook headers"
            raise ValueError(msg)

        now_ts = int(time.time()) if now is None else now
        try:
            ts_int = int(svix_timestamp)
        except ValueError as exc:
            msg = "Invalid Svix timestamp header"
            raise ValueError(msg) from exc

        if abs(now_ts - ts_int) > tolerance_seconds:
            msg = "Resend webhook timestamp expired or out of tolerance"
            raise ValueError(msg)

        raw_bytes, _ = WebhookVerifier._normalize_payload(payload)
        signed_content = f"{svix_id}.{svix_timestamp}.".encode() + raw_bytes

        clean_secret = secret[6:] if secret.startswith("whsec_") else secret
        try:
            key_bytes = base64.b64decode(clean_secret)
        except Exception:
            key_bytes = clean_secret.encode("utf-8")

        expected_sig = base64.b64encode(hmac.new(key_bytes, signed_content, hashlib.sha256).digest()).decode("ascii")

        sig_tokens = svix_signature.split()
        matched = False
        for token in sig_tokens:
            parts = token.split(",", 1)
            sig_val = parts[1] if len(parts) == 2 and parts[0] == "v1" else token
            if hmac.compare_digest(sig_val, expected_sig):
                matched = True
                break

        if not matched:
            msg = "Invalid Resend Svix webhook signature"
            raise ValueError(msg)

        body_data = decode_json(raw_bytes)
        body_dict = body_data if isinstance(body_data, dict) else {}
        event_type = str(body_dict.get("type", "unknown"))
        data_section = body_dict.get("data")
        data_dict = data_section if isinstance(data_section, dict) else {}
        msg_id = str(data_dict.get("email_id") or svix_id)
        recipient = data_dict.get("to")
        recip_str = recipient[0] if isinstance(recipient, list) and recipient else None

        return WebhookEvent(
            provider="resend",
            event_type=event_type,
            message_id=msg_id,
            recipient=recip_str,
            timestamp=ts_int,
            payload=body_dict,
        )

    @staticmethod
    def verify_sendgrid(
        payload: bytes | str,
        headers: Mapping[str, str],
        secret: str,
        *,
        now: int | None = None,
        tolerance_seconds: int = 300,
    ) -> list[WebhookEvent]:
        """Verify SendGrid delivery webhook HMAC signature."""
        header_map = {k.lower(): v for k, v in headers.items()}
        signature = header_map.get("x-twilio-email-event-webhook-signature")
        timestamp = header_map.get("x-twilio-email-event-webhook-timestamp")

        if not signature or not timestamp:
            msg = "Missing required SendGrid webhook headers"
            raise ValueError(msg)

        now_ts = int(time.time()) if now is None else now
        try:
            ts_int = int(timestamp)
        except ValueError as exc:
            msg = "Invalid SendGrid timestamp header"
            raise ValueError(msg) from exc

        if abs(now_ts - ts_int) > tolerance_seconds:
            msg = "SendGrid webhook timestamp expired or out of tolerance"
            raise ValueError(msg)

        raw_bytes, _ = WebhookVerifier._normalize_payload(payload)
        signed_payload = f"{timestamp}".encode() + raw_bytes
        key_bytes = secret.encode("utf-8")
        digest = hmac.new(key_bytes, signed_payload, hashlib.sha256).digest()
        expected_b64 = base64.b64encode(digest).decode("ascii")
        expected_hex = digest.hex()

        if not (hmac.compare_digest(signature, expected_b64) or hmac.compare_digest(signature, expected_hex)):
            msg = "Invalid SendGrid webhook signature"
            raise ValueError(msg)

        data = decode_json(raw_bytes)
        items: list[Any] = data if isinstance(data, list) else [data]
        events: list[WebhookEvent] = []
        for item in items:
            item_dict = item if isinstance(item, dict) else {}
            events.append(
                WebhookEvent(
                    provider="sendgrid",
                    event_type=str(item_dict.get("event", "unknown")),
                    message_id=str(item_dict.get("sg_message_id", "")),
                    recipient=item_dict.get("email"),
                    timestamp=int(item_dict.get("timestamp") or ts_int),
                    payload=item_dict,
                )
            )
        return events

    @staticmethod
    def verify_mailgun(
        body: Mapping[str, Any] | bytes | str,
        secret: str,
        *,
        now: int | None = None,
        tolerance_seconds: int = 300,
    ) -> WebhookEvent:
        """Verify Mailgun webhook signature block."""
        data_dict: dict[str, Any] = {}
        if isinstance(body, (bytes, str)):
            raw_bytes, _ = WebhookVerifier._normalize_payload(body)
            decoded = decode_json(raw_bytes)
            if isinstance(decoded, dict):
                data_dict = decoded
        elif isinstance(body, Mapping):
            data_dict = dict(body)

        sig_data = data_dict.get("signature")
        if not isinstance(sig_data, dict):
            msg = "Missing signature dictionary in Mailgun payload"
            raise ValueError(msg)

        timestamp = sig_data.get("timestamp")
        token = sig_data.get("token")
        provided_sig = sig_data.get("signature")

        if not timestamp or not token or not provided_sig:
            msg = "Incomplete Mailgun signature fields"
            raise ValueError(msg)

        now_ts = int(time.time()) if now is None else now
        try:
            ts_int = int(timestamp)
        except ValueError as exc:
            msg = "Invalid Mailgun timestamp"
            raise ValueError(msg) from exc

        if abs(now_ts - ts_int) > tolerance_seconds:
            msg = "Mailgun webhook timestamp expired or out of tolerance"
            raise ValueError(msg)

        signed_content = f"{timestamp}{token}".encode()
        expected_sig = hmac.new(
            secret.encode("utf-8"),
            signed_content,
            hashlib.sha256,
        ).hexdigest()

        if not hmac.compare_digest(str(provided_sig), expected_sig):
            msg = "Invalid Mailgun webhook signature"
            raise ValueError(msg)

        event_data = data_dict.get("event-data")
        event_dict = event_data if isinstance(event_data, dict) else {}
        msg_id = ""
        recipient = event_dict.get("recipient")
        msg_headers = event_dict.get("message", {})
        if isinstance(msg_headers, dict):
            msg_id = str(msg_headers.get("headers", {}).get("message-id", ""))

        return WebhookEvent(
            provider="mailgun",
            event_type=str(event_dict.get("event", "unknown")),
            message_id=msg_id or str(token),
            recipient=recipient,
            timestamp=ts_int,
            payload=data_dict,
        )
