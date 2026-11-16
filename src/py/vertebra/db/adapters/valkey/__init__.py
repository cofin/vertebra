"""Valkey 8.0+ / 9.0+ RESP3 driver, Cluster/Sentinel support, and GCP Memorystore for Valkey IAM."""

from __future__ import annotations

from typing import TYPE_CHECKING

from vertebra.utils.module_loader import lazy_exports

if TYPE_CHECKING:
    from vertebra.db.adapters.valkey.config import ValkeyConfig
    from vertebra.db.adapters.valkey.core import (
        ClusterRedirect,
        PubSubMessage,
        Resp3Error,
        Resp3Parser,
        ValkeyCore,
        crc16_xmodem,
        encode_hello3_handshake,
        encode_resp_command,
        hash_slot,
        parse_cluster_redirect,
        redis_hash_slot,
        valkey_hash_slot,
    )
    from vertebra.db.adapters.valkey.driver import AsyncValkeyDriver, SyncValkeyDriver
    from vertebra.db.adapters.valkey.notify import (
        AsyncValkeyNotificationSource,
        SyncValkeyNotificationSource,
        ValkeyNotifyConfig,
    )

__all__ = (
    "AsyncValkeyDriver",
    "AsyncValkeyNotificationSource",
    "ClusterRedirect",
    "PubSubMessage",
    "Resp3Error",
    "Resp3Parser",
    "SyncValkeyDriver",
    "SyncValkeyNotificationSource",
    "ValkeyConfig",
    "ValkeyCore",
    "ValkeyNotifyConfig",
    "crc16_xmodem",
    "encode_hello3_handshake",
    "encode_resp_command",
    "hash_slot",
    "parse_cluster_redirect",
    "redis_hash_slot",
    "valkey_hash_slot",
)

__lazy_modules__ = [
    "vertebra.db.adapters.valkey.config",
    "vertebra.db.adapters.valkey.core",
    "vertebra.db.adapters.valkey.driver",
    "vertebra.db.adapters.valkey.notify",
]

_EXPORT_MAP: dict[str, tuple[str, str]] = {
    "AsyncValkeyDriver": ("vertebra.db.adapters.valkey.driver", "AsyncValkeyDriver"),
    "AsyncValkeyNotificationSource": ("vertebra.db.adapters.valkey.notify", "AsyncValkeyNotificationSource"),
    "ClusterRedirect": ("vertebra.db.adapters.valkey.core", "ClusterRedirect"),
    "PubSubMessage": ("vertebra.db.adapters.valkey.core", "PubSubMessage"),
    "Resp3Error": ("vertebra.db.adapters.valkey.core", "Resp3Error"),
    "Resp3Parser": ("vertebra.db.adapters.valkey.core", "Resp3Parser"),
    "SyncValkeyDriver": ("vertebra.db.adapters.valkey.driver", "SyncValkeyDriver"),
    "SyncValkeyNotificationSource": ("vertebra.db.adapters.valkey.notify", "SyncValkeyNotificationSource"),
    "ValkeyConfig": ("vertebra.db.adapters.valkey.config", "ValkeyConfig"),
    "ValkeyCore": ("vertebra.db.adapters.valkey.core", "ValkeyCore"),
    "ValkeyNotifyConfig": ("vertebra.db.adapters.valkey.notify", "ValkeyNotifyConfig"),
    "crc16_xmodem": ("vertebra.db.adapters.valkey.core", "crc16_xmodem"),
    "encode_hello3_handshake": ("vertebra.db.adapters.valkey.core", "encode_hello3_handshake"),
    "encode_resp_command": ("vertebra.db.adapters.valkey.core", "encode_resp_command"),
    "hash_slot": ("vertebra.db.adapters.valkey.core", "hash_slot"),
    "parse_cluster_redirect": ("vertebra.db.adapters.valkey.core", "parse_cluster_redirect"),
    "redis_hash_slot": ("vertebra.db.adapters.valkey.core", "redis_hash_slot"),
    "valkey_hash_slot": ("vertebra.db.adapters.valkey.core", "valkey_hash_slot"),
}

__getattr__, __dir__ = lazy_exports(__name__, __all__, _EXPORT_MAP)
