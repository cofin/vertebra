"""Valkey RESP3 wire codecs and cluster helpers."""

from __future__ import annotations

from vertebra.db.redis.core import (
    ClusterRedirect,
    PubSubMessage,
    RedisCore,
    Resp3Error,
    Resp3Parser,
    crc16_xmodem,
    encode_hello3_handshake,
    encode_resp_command,
    hash_slot,
    parse_cluster_redirect,
    redis_hash_slot,
)

__all__ = (
    "ClusterRedirect",
    "PubSubMessage",
    "Resp3Error",
    "Resp3Parser",
    "ValkeyCore",
    "crc16_xmodem",
    "encode_hello3_handshake",
    "encode_resp_command",
    "hash_slot",
    "parse_cluster_redirect",
    "redis_hash_slot",
)


class ValkeyCore(RedisCore):
    """RESP3 command encoding, CRC16-XMODEM hash slots, and cluster redirection parsing for Valkey."""
