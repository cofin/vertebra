from vertebra.auth.state import get_user, set_user, reset_user
from vertebra.auth.models import AnonymousUser, VertebraUserMixin
import msgspec

class User(VertebraUserMixin, msgspec.Struct):
    id: str

def test_default_anonymous():
    user = get_user()
    assert isinstance(user, AnonymousUser)
    assert user.is_authenticated is False

def test_set_get_reset():
    u = User(id="1")
    token = set_user(u)
    assert get_user() == u
    
    reset_user(token)
    assert isinstance(get_user(), AnonymousUser)
