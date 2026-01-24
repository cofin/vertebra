from collections.abc import Callable, Coroutine
from typing import Any, TypeVar

from vertebra.auth.models import VertebraUserMixin

UserT = TypeVar("UserT", bound=VertebraUserMixin)
Fetcher = Callable[[str, Any], Coroutine[Any, Any, UserT]]


class LazyUserProxy[UserT: VertebraUserMixin]:
    """Lazy-loading proxy for User models."""

    def __init__(
        self, model_cls: type[UserT], claims: dict[str, Any], fetcher: Fetcher[UserT], *, context: Any | None = None
    ) -> None:
        self._model_cls = model_cls
        self._claims = claims
        self._fetcher = fetcher
        self._context = context
        self._wrapped: UserT | None = None

    @property
    def is_authenticated(self) -> bool:
        # Always authenticated if we have claims (AnonymousUser is handled separately)
        return True

    @property
    def is_anonymous(self) -> bool:
        return False

    def __getattr__(self, name: str) -> Any:
        if name in self._claims:
            return self._claims[name]

        if self._wrapped is not None:
            return getattr(self._wrapped, name)

        msg = f"Attribute '{name}' not found in claims and user not loaded. Await .load() to fetch full user profile."
        raise AttributeError(msg)

    async def load(self) -> UserT:
        """Fetch the full user object."""
        if self._wrapped is None:
            user_id = self._claims.get("id")
            self._wrapped = await self._fetcher(user_id, self._context)
        return self._wrapped

    async def refresh(self) -> UserT:
        """Force re-fetch of the user object."""
        self._wrapped = None
        return await self.load()
