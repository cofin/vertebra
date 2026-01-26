from __future__ import annotations

from vertebra.web.controllers import Controller, ControllerRoute
from vertebra.web.routing import get, post


class UserController(Controller):
    path = "/users"
    guards = ("auth",)
    middleware = ("log",)

    @get("/{user_id:int}")
    def get_user(self, user_id: int) -> str:
        return str(user_id)

    @post("/")
    def create_user(self) -> str:
        return "ok"


def test_controller_route_reflection() -> None:
    routes = UserController.routes()
    assert [route.method for route in routes] == ["GET", "POST"]
    assert [route.path for route in routes] == ["/users/{user_id:int}", "/users/"]


def test_controller_routes_include_guards_and_middleware() -> None:
    routes = UserController.controller_routes()
    assert all(isinstance(route, ControllerRoute) for route in routes)
    assert [route.guards for route in routes] == [("auth",), ("auth",)]
    assert [route.middleware for route in routes] == [("log",), ("log",)]


def test_controller_path_empty() -> None:
    class RootController(Controller):
        @get("/health")
        def health(self) -> str:
            return "ok"

    routes = RootController.routes()
    assert [route.path for route in routes] == ["/health"]
