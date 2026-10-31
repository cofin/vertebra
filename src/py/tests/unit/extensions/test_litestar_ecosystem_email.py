"""Unit tests for Vertebra litestar-email SES backend with native SigV4 signing."""

from __future__ import annotations

import sys
from unittest.mock import AsyncMock, MagicMock

import pytest

from vertebra.extensions.litestar.email import SESEmailBackend, SESEmailConfig
from vertebra.typing import MissingDependencyError
from vertebra.utils.module_loader import ensure_module


def test_ses_email_config_attributes() -> None:
    """Verify SESEmailConfig default and custom attributes."""
    cfg = SESEmailConfig(
        region="us-west-2",
        aws_access_key_id="dummy-access-key",
        aws_secret_access_key="dummy-secret-key",
        timeout=15,
    )
    assert cfg.region == "us-west-2"
    assert cfg.aws_access_key_id == "dummy-access-key"
    assert cfg.aws_secret_access_key == "dummy-secret-key"
    assert cfg.timeout == 15


def test_zero_botocore_dependency() -> None:
    """Verify SESEmailBackend signs headers without requiring botocore."""
    cfg = SESEmailConfig(
        aws_access_key_id="dummy-access-key",
        aws_secret_access_key="dummy-secret-key",
    )
    backend = SESEmailBackend(cfg)
    headers = backend._sign_request(
        "https://email.us-east-1.amazonaws.com/v2/email/outbound-emails",
        b"{}",
    )
    header_keys = [k.lower() for k in headers]
    assert "authorization" in header_keys
    assert "x-amz-date" in header_keys
    assert "botocore" not in sys.modules


@pytest.mark.anyio
async def test_ses_email_backend_open_close_lifecycle() -> None:
    """Verify open and close manage transport lifecycle."""
    cfg = SESEmailConfig(
        aws_access_key_id="dummy-access-key",
        aws_secret_access_key="dummy-secret-key",
    )
    backend = SESEmailBackend(cfg)
    first_open = await backend.open()
    assert first_open is True
    assert backend._transport is not None

    second_open = await backend.open()
    assert second_open is False

    await backend.close()
    assert backend._transport is None


@pytest.mark.anyio
async def test_ses_email_backend_send_messages() -> None:
    """Verify send_messages delivers Simple email payloads via mocked HTTP transport."""
    cfg = SESEmailConfig(
        aws_access_key_id="dummy-access-key",
        aws_secret_access_key="dummy-secret-key",
    )
    backend = SESEmailBackend(cfg, default_from_email="noreply@example.com")

    transport_mock = MagicMock()
    response_mock = MagicMock()
    response_mock.status_code = 200
    transport_mock.post = AsyncMock(return_value=response_mock)
    transport_mock.close = AsyncMock()

    await backend.open()
    backend._transport = transport_mock

    message = MagicMock()
    message.from_email = None
    message.from_name = None
    message.to = ["recipient@example.com"]
    message.subject = "Test Subject"
    message.body = "Hello world text body"
    message.alternatives = ()
    message.attachments = ()
    message.cc = None
    message.bcc = None
    message.reply_to = None

    sent_count = await backend.send_messages([message])
    assert sent_count == 1
    transport_mock.post.assert_awaited_once()

    await backend.close()


def test_missing_dependency_error_on_missing_email(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify MissingDependencyError with extra='email' when litestar_email is absent."""
    monkeypatch.setattr("vertebra.utils.module_loader.import_optional", lambda name: None)
    with pytest.raises(MissingDependencyError) as exc_info:
        ensure_module("litestar_email", extra="email")
    assert "email" in exc_info.value.install_package
