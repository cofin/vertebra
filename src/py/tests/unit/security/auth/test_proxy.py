from typing import Any

import msgspec
import pytest

from vertebra.security.auth.models import VertebraUserMixin
from vertebra.security.auth.proxy import LazyUserProxy


class User(VertebraUserMixin, msgspec.Struct):
    id: str
    username: str
    email: str


async def fetch_user(user_id: str, context: Any = None) -> User:
    return User(id=user_id, username="fetched", email="fetched@example.com")


@pytest.mark.anyio
async def test_lazy_proxy_claims() -> None:
    claims = {"id": "123", "username": "claim_user"}
    proxy = LazyUserProxy(User, claims, fetch_user)

    assert proxy.is_authenticated is True
    assert proxy.id == "123"
    assert proxy.username == "claim_user"

    # Missing field raises error before load
    with pytest.raises(AttributeError, match="not loaded"):
        _ = proxy.email


@pytest.mark.anyio
async def test_lazy_proxy_load() -> None:
    claims = {"id": "123"}
    proxy = LazyUserProxy(User, claims, fetch_user)

    await proxy.load()

    assert proxy.username == "fetched"
    assert proxy.email == "fetched@example.com"
    # ID should match
    assert proxy.id == "123"


@pytest.mark.anyio
async def test_lazy_proxy_refresh() -> None:
    claims = {"id": "123"}
    call_count = 0

    async def mock_fetch(user_id: str, context: Any = None) -> User:
        nonlocal call_count
        call_count += 1
        return User(id=user_id, username=f"user_{call_count}", email="test@example.com")

    proxy = LazyUserProxy(User, claims, mock_fetch)
    await proxy.load()
    assert proxy.username == "user_1"

    await proxy.refresh()
    assert proxy.username == "user_2"
