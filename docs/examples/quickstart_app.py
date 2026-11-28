"""A native Vertebra application with typed routes and an OpenAPI schema."""

# --8<-- [start:example]
import msgspec

from vertebra import App, get


class HealthStatus(msgspec.Struct):
    """Health check payload."""

    status: str
    service: str


@get("/health")
async def health() -> HealthStatus:
    """Report that the service is up."""
    return HealthStatus(status="ok", service="vertebra")


@get("/users/{user_id:int}")
async def get_user(user_id: int) -> dict[str, int | str]:
    """Return a user summary."""
    return {"id": user_id, "name": f"User {user_id}"}


app = App(route_handlers=[health, get_user])
app.enable_docs()
# --8<-- [end:example]


__all__ = ("HealthStatus", "app", "get_user", "health")
