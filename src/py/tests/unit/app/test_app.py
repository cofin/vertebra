from __future__ import annotations

import pytest

from vertebra.app import Vertebra
from vertebra.controllers import Controller
from vertebra.procedures import ProcedureRegistry
from vertebra.routing import Router, get
from vertebra.state import NativeState

pytestmark = pytest.mark.unit


class ExampleState(NativeState):
    value: int = 0


class HealthController(Controller):
    path = "/health"
    guards = ("auth",)
    middleware = ("log",)

    @get("/ready")
    def ready(self) -> str:
        return "ok"


def test_app_registers_router_and_controller() -> None:
    router = Router("/api")

    @get("/ping")
    def ping() -> str:
        return "pong"

    router.register(ping)

    app = Vertebra()
    app.register(router)
    app.register(HealthController)

    paths = [route.path for route in app.routes]
    assert paths == ["/api/ping", "/health/ready"]

    controller_routes = app.controller_routes
    assert len(controller_routes) == 1
    assert controller_routes[0].guards == ("auth",)
    assert controller_routes[0].middleware == ("log",)


def test_app_state_initialization_registers_once() -> None:
    registry = ProcedureRegistry.instance()
    registry.clear()

    app = Vertebra()
    state = app.state(ExampleState, name="state.example")
    assert state is app.state(ExampleState)

    meta = registry.get("state.example")
    assert meta is not None


@pytest.mark.anyio
async def test_app_lifecycle_hooks_run_in_order() -> None:
    app = Vertebra()
    calls: list[str] = []

    @app.on_startup
    def sync_startup() -> None:
        calls.append("startup-sync")

    @app.on_startup
    async def async_startup() -> None:
        calls.append("startup-async")

    @app.on_shutdown
    async def async_shutdown() -> None:
        calls.append("shutdown-async")

    @app.on_shutdown
    def sync_shutdown() -> None:
        calls.append("shutdown-sync")

    await app.startup()
    await app.shutdown()

    assert calls == [
        "startup-sync",
        "startup-async",
        "shutdown-async",
        "shutdown-sync",
    ]
