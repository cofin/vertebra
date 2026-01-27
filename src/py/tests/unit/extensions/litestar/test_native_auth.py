from vertebra.extensions.litestar.native import VertebraRequest
from vertebra.security.auth.models import AnonymousUser


def test_native_request_user_hydration() -> None:
    # 1. With claims
    msg = {"auth_claims": {"id": "user-123", "username": "test"}}
    req = VertebraRequest(msg)
    user = req.user
    # LazyUserProxy wraps the mixin, need to check if it behaves like it
    # But wait, logic says: self._user = LazyUserProxy(VertebraUserMixin, ...)
    # LazyUserProxy should proxy attributes.

    assert user.is_authenticated is True
    assert user.id == "user-123"
    assert user.username == "test"

    # 2. Without claims
    msg = {}
    req = VertebraRequest(msg)
    user = req.user
    assert isinstance(user, AnonymousUser)
    assert user.is_authenticated is False
