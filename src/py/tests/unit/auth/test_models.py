import msgspec
import pytest
from vertebra.auth.models import VertebraUserMixin, AnonymousUser

class User(VertebraUserMixin, msgspec.Struct):
    id: str
    username: str

def test_user_mixin_properties():
    user = User(id="123", username="test")
    assert user.is_authenticated is True
    assert user.is_anonymous is False
    assert user.id == "123"

def test_anonymous_user():
    user = AnonymousUser()
    assert user.is_authenticated is False
    assert user.is_anonymous is True
    assert user.id is None
