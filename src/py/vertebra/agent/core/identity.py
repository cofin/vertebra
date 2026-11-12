"""Explicit trusted proxy policies for agent caller identity."""

import ipaddress
from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING, Protocol

import msgspec

import vertebra.agent.core.context as context_module

if TYPE_CHECKING:
    from vertebra.agent.core.context import CallerIdentity

__all__ = ["CallableIdentitySource", "IdentityRequest", "IdentitySource", "MtlsIdentitySource"]


class IdentityRequest(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Normalized request headers and immediate peer for an identity policy."""

    headers: Mapping[str, str]
    peer: str | None = None


class IdentitySource(Protocol):
    """Application policy resolving a caller from trusted request metadata."""

    def resolve(self, request: IdentityRequest) -> "CallerIdentity | None":
        """Resolve identity, or return None when the request establishes none."""
        ...


class MtlsIdentitySource(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Trust a certificate subject header only from explicitly configured proxies."""

    trusted_proxies: tuple[str, ...]
    header: str = "x-vertebra-mtls-subject"

    def __post_init__(self) -> None:
        if not self.trusted_proxies:
            raise ValueError("MtlsIdentitySource requires at least one trusted proxy CIDR")
        for value in self.trusted_proxies:
            try:
                ipaddress.ip_network(value, strict=False)
            except ValueError as exc:
                raise ValueError(f"Invalid trusted proxy CIDR: {value!r}") from exc
        msgspec.structs.force_setattr(self, "header", self.header.lower())

    def resolve(self, request: IdentityRequest) -> "CallerIdentity | None":
        """Resolve the certificate subject only for peers inside trusted networks."""
        if not request.peer or not (subject := request.headers.get(self.header)):
            return None
        try:
            peer = ipaddress.ip_address(request.peer)
        except ValueError:
            return None
        if any(peer in ipaddress.ip_network(value, strict=False) for value in self.trusted_proxies):
            return context_module.CallerIdentity(client_cert_subject=subject, auth_scheme="mtls")
        return None


class CallableIdentitySource(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Delegate caller resolution to an application-owned synchronous policy."""

    resolver: Callable[[IdentityRequest], "CallerIdentity | None"]

    def __post_init__(self) -> None:
        if not callable(self.resolver):
            raise TypeError("CallableIdentitySource.resolver must be callable")

    def resolve(self, request: IdentityRequest) -> "CallerIdentity | None":
        """Resolve identity with the configured application policy."""
        return self.resolver(request)
