"""Tests for route manifest generation."""

import json

from vertebra.builder._codegen._manifest import ManifestGenerator
from vertebra.builder._codegen._metadata import RouteCollector
from vertebra.framework._app import App


def test_manifest_uses_route_metadata() -> None:
    app = App()

    @app.get("/hello")
    def hello() -> str:
        return "world"

    routes = RouteCollector().collect(app)
    output = ManifestGenerator().generate(routes)
    data = json.loads(output)

    assert data["version"] == 1
    entry = data["routes"]["get.hello"]
    assert entry["method"] == "GET"
    assert entry["route"] == "/hello"
    assert entry["paramsSchema"] == "get_helloParams"
    assert entry["responseSchema"] == "get_helloResponse"


def test_manifest_handles_multiple_routes() -> None:
    app = App()

    @app.get("/items")
    def list_items() -> list[str]:
        return []

    @app.post("/items")
    def create_item(name: str) -> str:
        return name

    routes = RouteCollector().collect(app)
    data = json.loads(ManifestGenerator().generate(routes))

    assert len(data["routes"]) == 2
    assert "get.items" in data["routes"]
    assert "post.items" in data["routes"]
