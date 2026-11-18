"""Amazon SES email backend using Rust SigV4 signer without botocore."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import msgspec

from vertebra._storage import py_sign_aws_sigv4_headers
from vertebra.utils.module_loader import ensure_module
from vertebra.utils.serializers import encode_json_bytes

if TYPE_CHECKING:
    from litestar_email.backends.base import BaseEmailBackend
    from litestar_email.message import EmailMessage
    from litestar_email.transports.base import HTTPTransport

__all__ = (
    "SESEmailBackend",
    "SESEmailConfig",
)

_CACHED_SES_BACKEND_CLS: type[Any] | None = None


class SESEmailConfig(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Configuration for Amazon SES email backend using native Rust SigV4 signing."""

    region: str = "us-east-1"
    aws_access_key_id: str | None = None
    aws_secret_access_key: str | None = None
    aws_session_token: str | None = None
    timeout: int = 30
    http_transport: str = "httpx"


class _SESEmailBackendImpl:
    """Amazon SES email backend using Rust py_sign_aws_sigv4_headers with zero botocore dependency."""

    fail_silently: bool
    _resolve_from: Any

    def __init__(
        self,
        config: SESEmailConfig | None = None,
        *,
        fail_silently: bool = False,
        default_from_email: str | None = None,
        default_from_name: str | None = None,
    ) -> None:
        """Initialize the SES email backend with optional configuration."""
        base_mod = ensure_module("litestar_email.backends.base", package="litestar-email", extra="email")
        base_mod.BaseEmailBackend.__init__(
            self,
            fail_silently=fail_silently,
            default_from_email=default_from_email,
            default_from_name=default_from_name,
        )
        self._config = config if config is not None else SESEmailConfig()
        self._transport: HTTPTransport | None = None

    @property
    def config(self) -> SESEmailConfig:
        """Return the active SESEmailConfig."""
        return self._config

    async def open(self) -> bool:
        """Open an HTTP transport for sending emails."""
        if self._transport is not None:
            return False

        trans_mod = ensure_module("litestar_email.transports", package="litestar-email", extra="email")
        transport = trans_mod.get_transport(self._config.http_transport)
        self._transport = transport
        await transport.open(timeout=float(self._config.timeout))
        return True

    async def close(self) -> None:
        """Close the HTTP transport."""
        if self._transport is not None:
            try:
                await self._transport.close()
            except Exception:
                if not self.fail_silently:
                    raise
            finally:
                self._transport = None

    async def send_messages(self, messages: list[EmailMessage]) -> int:
        """Send messages via Amazon SES API v2."""
        if not messages:
            return 0

        exc_mod = ensure_module("litestar_email.exceptions", package="litestar-email", extra="email")
        new_connection = await self.open()

        try:
            num_sent = 0
            for message in messages:
                try:
                    await self._send_message(message)
                    num_sent += 1
                except (exc_mod.EmailRateLimitError, exc_mod.EmailAuthenticationError):
                    raise
                except exc_mod.EmailDeliveryError:
                    if not self.fail_silently:
                        raise
                except Exception as exc:
                    if not self.fail_silently:
                        msg = f"Failed to send email to {message.to} via Amazon SES"
                        raise exc_mod.EmailDeliveryError(msg) from exc
            return num_sent
        finally:
            if new_connection:
                await self.close()

    async def _send_message(self, message: EmailMessage) -> None:
        """Send a single message via Amazon SES API v2 using Rust SigV4 headers."""
        if self._transport is None:
            msg = "SES transport not initialized"
            raise RuntimeError(msg)

        exc_mod = ensure_module("litestar_email.exceptions", package="litestar-email", extra="email")
        if getattr(message, "attachments", None):
            msg = (
                "Amazon SES backend does not support attachments via Simple content type. Use SMTP backend or raw MIME."
            )
            raise exc_mod.EmailDeliveryError(msg)

        resolve_from = self._resolve_from
        _, _, from_formatted = resolve_from(message)
        body_section: dict[str, Any] = {}
        if getattr(message, "body", None):
            body_section["Text"] = {"Data": message.body}
        for content, mimetype in getattr(message, "alternatives", ()):
            if mimetype == "text/html":
                body_section["Html"] = {"Data": content}
                break

        if not body_section:
            msg = "Amazon SES requires either a text body or an HTML alternative"
            raise exc_mod.EmailDeliveryError(msg)

        payload: dict[str, Any] = {
            "FromEmailAddress": from_formatted,
            "Destination": {"ToAddresses": list(message.to)},
            "Content": {
                "Simple": {
                    "Subject": {"Data": message.subject},
                    "Body": body_section,
                }
            },
        }
        if getattr(message, "cc", None):
            payload["Destination"]["CcAddresses"] = list(message.cc)
        if getattr(message, "bcc", None):
            payload["Destination"]["BccAddresses"] = list(message.bcc)
        if getattr(message, "reply_to", None):
            payload["ReplyToAddresses"] = list(message.reply_to)

        url = f"https://email.{self._config.region}.amazonaws.com/v2/email/outbound-emails"
        body = encode_json_bytes(payload)

        headers = self._sign_request(url, body)
        headers.setdefault("Content-Type", "application/json")

        response = await self._transport.post(
            url,
            content=body,
            headers=headers,
        )

        if response.status_code == 429:
            retry_after = response.get_header("Retry-After")
            retry_seconds = int(retry_after) if retry_after else None
            msg = "Amazon SES API rate limit exceeded"
            raise exc_mod.EmailRateLimitError(msg, retry_after=retry_seconds)

        if response.status_code == 403:
            error_detail = await response.text()
            msg = f"Amazon SES authentication failed: {error_detail}"
            raise exc_mod.EmailAuthenticationError(msg)

        if response.status_code >= 400:
            error_detail = await response.text()
            msg = f"Amazon SES API error: {response.status_code} - {error_detail}"
            raise exc_mod.EmailDeliveryError(msg)

    def _sign_request(self, url: str, body: bytes) -> dict[str, str]:
        """Sign the request using native Rust SigV4 signer without botocore."""
        headers = py_sign_aws_sigv4_headers(
            "ses",
            self._config.region,
            self._config.aws_access_key_id or "",
            self._config.aws_secret_access_key or "",
            "POST",
            url,
            body,
            "application/json",
            session_token=self._config.aws_session_token,
        )
        return dict(headers)


if TYPE_CHECKING:

    class SESEmailBackend(_SESEmailBackendImpl, BaseEmailBackend):
        """Amazon SES email backend using Rust py_sign_aws_sigv4_headers with zero botocore dependency."""


def __getattr__(name: str) -> Any:
    """Lazily bind ``SESEmailBackend`` to ``litestar_email.backends.base.BaseEmailBackend``."""
    global _CACHED_SES_BACKEND_CLS
    if name == "SESEmailBackend":
        if _CACHED_SES_BACKEND_CLS is None:
            base_mod = ensure_module("litestar_email.backends.base", package="litestar-email", extra="email")
            base_cls = base_mod.BaseEmailBackend
            _CACHED_SES_BACKEND_CLS = type(
                "SESEmailBackend",
                (_SESEmailBackendImpl, base_cls),
                {
                    "__module__": __name__,
                    "__doc__": _SESEmailBackendImpl.__doc__,
                    "__slots__": (),
                },
            )
        return _CACHED_SES_BACKEND_CLS
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)


def __dir__() -> list[str]:
    """Return module attributes including lazily bound ``SESEmailBackend``."""
    return sorted(set(globals()) | set(__all__))
