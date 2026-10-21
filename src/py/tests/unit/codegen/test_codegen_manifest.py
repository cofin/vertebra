"""Tests for route manifest generation."""

from vertebra.build.codegen._manifest import ManifestGenerator
from vertebra.build.codegen._metadata import RouteCollector
from vertebra.framework._app import App
from vertebra.utils.serializers import decode_json


def test_manifest_uses_route_metadata() -> None:
    app = App()

    @app.get("/hello")
    def hello() -> str:
        return "world"

    routes = RouteCollector().collect(app)
    output = ManifestGenerator().generate(routes)
    data = decode_json(output)

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
    data = decode_json(ManifestGenerator().generate(routes))

    assert len(data["routes"]) == 2
    assert "get.items" in data["routes"]
    assert "post.items" in data["routes"]
