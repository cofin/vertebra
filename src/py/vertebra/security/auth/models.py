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

    def has_scope(self, scope: str) -> bool:
        """Check if the user has a specific scope.

        Looks for a 'scopes' attribute (list/set/tuple) on the model.
        """
        scopes = getattr(self, "scopes", None)
        if scopes and isinstance(scopes, (list, tuple, set)):
            return scope in scopes
        return False

    def has_perm(self, perm: str) -> bool:
        """Check if the user has a specific permission.

        Looks for a 'permissions' attribute (list/set/tuple) on the model.
        """
        perms = getattr(self, "permissions", None)
        if perms and isinstance(perms, (list, tuple, set)):
            return perm in perms
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
