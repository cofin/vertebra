"""Unit tests for the Python-to-Rust Pub/Sub publish bridge across Embedded and SHM modes."""

from __future__ import annotations

import struct
from collections.abc import Buffer, Callable
from typing import Any, cast

import pytest

import vertebra._vertebra as _native
from vertebra._vertebra import LeaseBroker, init_transport
from vertebra.server._channels import pubsub_publish

_SHM_PUBSUB_CHANNEL = "__vtb_pubsub__"
_CHAN_MAX_PAYLOAD_BYTES = 256


def _native_pubsub_publish() -> Callable[[str, Buffer | str, str | None], int | None]:
    return cast(
        Callable[[str, Buffer | str, str | None], int | None],
        vars(_native)["pubsub_publish"],
    )


def _decode_shm_pubsub_frame(raw: bytes) -> tuple[str, str | None, bytes]:
    topic_len, key_len = struct.unpack("<HH", raw[:4])
    topic_end = 4 + topic_len
    key_end = topic_end + key_len
    topic = raw[4:topic_end].decode("utf-8")
    key = raw[topic_end:key_end].decode("utf-8") if key_len > 0 else None
    return topic, key, raw[key_end:]


@pytest.mark.parametrize("via_channels_wrapper", [False, True])
@pytest.mark.parametrize(
    ("payload_factory", "expected_bytes"),
    [
        (lambda: "hello-text", b"hello-text"),
        (lambda: b"hello-bytes", b"hello-bytes"),
        (lambda: bytearray(b"hello-bytearray"), b"hello-bytearray"),
        (lambda: memoryview(b"hello-readonly-view"), b"hello-readonly-view"),
        (lambda: memoryview(bytearray(b"hello-writable-view")), b"hello-writable-view"),
    ],
)
@pytest.mark.parametrize("idempotency_key", [None, "idem-key-1"])
def test_pubsub_publish_shm_mode_frames_and_delivers_payloads(
    monkeypatch: pytest.MonkeyPatch,
    via_channels_wrapper: bool,
    payload_factory: Callable[[], Buffer | str],
    expected_bytes: bytes,
    idempotency_key: str | None,
) -> None:
    """Verify SHM mode encodes pubsub frames onto ``__vtb_pubsub__`` for all buffer and string inputs."""
    broker = LeaseBroker(pool_size=2, max_overflow=1, stmt_capacity=8)
    assert broker.shm_fd is not None
    monkeypatch.setenv("VERTEBRA_TRANSPORT_MODE", "shm")
    monkeypatch.setenv("VERTEBRA_BROKER_SHM_FD", str(broker.shm_fd))
    monkeypatch.setenv("VERTEBRA_BROKER_SHM_SIZE", str(broker.shm_size))

    publish_fn = pubsub_publish if via_channels_wrapper else _native_pubsub_publish()
    result = publish_fn("orders.created", payload_factory(), idempotency_key)
    assert result is None

    frames = broker.drain_channel(_SHM_PUBSUB_CHANNEL)
    assert len(frames) == 1
    channel_name, raw_payload, _ts, lease_id = frames[0]
    assert channel_name == _SHM_PUBSUB_CHANNEL
    assert lease_id is None
    decoded_topic, decoded_key, decoded_body = _decode_shm_pubsub_frame(raw_payload)
    assert decoded_topic == "orders.created"
    assert decoded_key == idempotency_key
    assert decoded_body == expected_bytes


@pytest.mark.parametrize(
    ("topic", "payload", "idempotency_key", "should_raise"),
    [
        ("t", b"x" * (_CHAN_MAX_PAYLOAD_BYTES - 5), None, False),
        ("t", b"x" * (_CHAN_MAX_PAYLOAD_BYTES - 4), None, True),
        ("orders", "p" * 300, None, True),
        ("t", b"x" * (_CHAN_MAX_PAYLOAD_BYTES - 10), "long-key", True),
    ],
)
def test_pubsub_publish_shm_mode_enforces_256_byte_frame_limit(
    monkeypatch: pytest.MonkeyPatch,
    topic: str,
    payload: bytes | str,
    idempotency_key: str | None,
    should_raise: bool,
) -> None:
    """Verify SHM mode raises ``ValueError`` whenever the encoded frame exceeds 256 bytes."""
    broker = LeaseBroker(pool_size=2, max_overflow=1, stmt_capacity=8)
    assert broker.shm_fd is not None
    monkeypatch.setenv("VERTEBRA_TRANSPORT_MODE", "shm")
    monkeypatch.setenv("VERTEBRA_BROKER_SHM_FD", str(broker.shm_fd))
    monkeypatch.setenv("VERTEBRA_BROKER_SHM_SIZE", str(broker.shm_size))

    if should_raise:
        with pytest.raises(ValueError, match="256"):
            pubsub_publish(topic, payload, idempotency_key)
        assert broker.drain_channel(_SHM_PUBSUB_CHANNEL) == []
    else:
        assert pubsub_publish(topic, payload, idempotency_key) is None
        frames = broker.drain_channel(_SHM_PUBSUB_CHANNEL)
        assert len(frames) == 1


@pytest.mark.parametrize("payload", ["embedded-text", b"x" * 512])
def test_pubsub_publish_embedded_mode_allows_large_payload_without_sink(
    monkeypatch: pytest.MonkeyPatch,
    payload: bytes | str,
) -> None:
    """Verify Embedded mode routes through ``BatchBridge`` and tolerates payloads larger than 256 bytes."""
    monkeypatch.delenv("VERTEBRA_TRANSPORT_MODE", raising=False)
    monkeypatch.delenv("VERTEBRA_BROKER_SHM_FD", raising=False)
    monkeypatch.delenv("VERTEBRA_BROKER_SHM_SIZE", raising=False)
    init_transport("inprocess", 4096, 16, 16)

    assert pubsub_publish("alerts", payload, "embedded-key") is None


def test_pubsub_publish_rejects_non_buffer_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify non-buffer, non-string payloads raise ``TypeError``."""
    monkeypatch.delenv("VERTEBRA_TRANSPORT_MODE", raising=False)
    init_transport("inprocess", 4096, 16, 16)

    with pytest.raises(TypeError):
        pubsub_publish("alerts", cast(Any, 12345))
