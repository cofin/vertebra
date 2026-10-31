"""Quickstart Vertebra application example with typed routes and OpenAPI 3.1 schema."""

import msgspec

from vertebra import App, extract_path_params, handler_id

__all__ = (
    "HealthStatus",
    "create_app",
    "main",
)


# --8<-- [start:example]
class HealthStatus(msgspec.Struct, frozen=True, gc=False):
    """Health check payload returned by the quickstart application."""

    status: str
    service: str


def create_app() -> App:
    """Create and configure the quickstart Vertebra application."""
    app = App()

    @app.get("/health")
    def health() -> HealthStatus:
        return HealthStatus(status="ok", service="vertebra")

    @app.get("/users/{user_id:int}")
    def get_user(user_id: int) -> dict[str, int | str]:
        return {"id": user_id, "role": "admin"}

    app.enable_docs()
    return app


def main() -> None:
    """Verify route indexing, typed path extraction, and OpenAPI generation."""
    app = create_app()

    health_id = handler_id("GET", "/health")
    assert health_id in app.handler_index
    assert app.handler_index[health_id].path == "/health"

    params = extract_path_params("/users/{user_id:int}", "/users/42")
    assert params == {"user_id": 42}


# --8<-- [end:example]


if __name__ == "__main__":
    main()
