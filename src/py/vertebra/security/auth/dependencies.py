from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from litestar.connection import ASGIConnection


async def provide_user(request: "ASGIConnection") -> Any:
    """Dependency provider for current user.

    Compatible with both standard ASGI (Litestar Request) and Native (VertebraRequest).
    """
    return request.user
