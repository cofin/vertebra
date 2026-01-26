from typing import Any

import msgspec


class VertebraUserMixin(msgspec.Struct):
    """Mixin for User models providing standard auth properties."""

    @property
    def is_authenticated(self) -> bool:
        """Return True if the user is authenticated."""
        return True

    @property
    def is_anonymous(self) -> bool:
        """Return True if the user is anonymous."""
        return False


class AnonymousUser(VertebraUserMixin):
    """Represents an unauthenticated user."""

    id: Any = None

    @property
    def is_authenticated(self) -> bool:
        return False

    @property
    def is_anonymous(self) -> bool:
        return True
