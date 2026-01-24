from contextvars import ContextVar, Token

from vertebra.auth.models import AnonymousUser, VertebraUserMixin

_user_ctx: ContextVar[VertebraUserMixin | None] = ContextVar("vertebra_user", default=None)


def get_user() -> VertebraUserMixin:
    """Get the current user from context."""
    user = _user_ctx.get()
    if user is None:
        return AnonymousUser()
    return user


def set_user(user: VertebraUserMixin) -> Token[VertebraUserMixin]:
    """Set the current user in context."""
    return _user_ctx.set(user)


def reset_user(token: Token[VertebraUserMixin]) -> None:
    """Reset the user context."""
    _user_ctx.reset(token)
