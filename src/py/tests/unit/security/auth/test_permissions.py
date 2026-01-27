from vertebra.security.auth.models import AnonymousUser, VertebraUserMixin


class UserWithScopes(VertebraUserMixin):
    id: str
    scopes: list[str] = []
    permissions: list[str] = []


def test_has_scope() -> None:
    user = UserWithScopes(id="1", scopes=["read", "write"])
    assert user.has_scope("read") is True
    assert user.has_scope("write") is True
    assert user.has_scope("admin") is False


def test_has_perm() -> None:
    user = UserWithScopes(id="1", permissions=["users:read", "users:write"])
    assert user.has_perm("users:read") is True
    assert user.has_perm("users:write") is True
    assert user.has_perm("users:delete") is False


def test_anonymous_permissions() -> None:
    anon = AnonymousUser()
    # Anonymous user should return False for any scope/perm checks
    # Assuming we implement these methods on the base mixin
    assert anon.has_scope("read") is False
    assert anon.has_perm("read") is False


def test_missing_fields_defaults() -> None:
    class SimpleUser(VertebraUserMixin):
        id: str

    user = SimpleUser(id="1")
    # Should default to False if fields missing
    assert user.has_scope("read") is False
    assert user.has_perm("read") is False
