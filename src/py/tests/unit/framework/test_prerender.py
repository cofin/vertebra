"""Tests for SSG prerendering."""

from pathlib import Path
from unittest.mock import MagicMock

from vertebra.framework._ssr._backends import SsrResult
from vertebra.framework._ssr._prerender import prerender_routes
from vertebra.utils.serializers import decode_json


def _mock_ssr_client(head: "list[str] | None" = None, body: str = "<div>rendered</div>") -> MagicMock:
    client = MagicMock()
    client.render.return_value = SsrResult(
        head=head or ["<title>Test</title>"],
        body=body,
    )
    return client


def test_prerender_routes_single_route(tmp_path: Path) -> None:
    client = _mock_ssr_client()
    routes = [{"path": "/", "component": "Home", "props": {}}]
    written = prerender_routes(ssr_client=client, routes=routes, outdir=tmp_path)
    assert len(written) == 1
    assert (tmp_path / "index.html").exists()
    html = (tmp_path / "index.html").read_text()
    assert "<div>rendered</div>" in html
    assert "<title>Test</title>" in html


def test_prerender_routes_nested_route(tmp_path: Path) -> None:
    client = _mock_ssr_client()
    routes = [{"path": "/about/team", "component": "Team", "props": {}}]
    written = prerender_routes(ssr_client=client, routes=routes, outdir=tmp_path)
    assert len(written) == 1
    assert (tmp_path / "about" / "team" / "index.html").exists()


def test_prerender_routes_multiple_routes(tmp_path: Path) -> None:
    client = _mock_ssr_client()
    routes = [
        {"path": "/", "component": "Home", "props": {}},
        {"path": "/about", "component": "About", "props": {}},
        {"path": "/pricing", "component": "Pricing", "props": {}},
    ]
    written = prerender_routes(ssr_client=client, routes=routes, outdir=tmp_path)
    assert len(written) == 3


def test_prerender_routes_with_root_template(tmp_path: Path) -> None:
    client = _mock_ssr_client()
    template = "<html><head><!--ssr-head--></head><body><!--ssr-body--></body></html>"
    routes = [{"path": "/", "component": "Home", "props": {}}]
    written = prerender_routes(
        ssr_client=client,
        routes=routes,
        outdir=tmp_path,
        root_template=template,
    )
    html = written[0].read_text()
    assert "<title>Test</title>" in html
    assert "<div>rendered</div>" in html
    assert "<!--ssr-head-->" not in html
    assert "<!--ssr-body-->" not in html


def test_prerender_routes_render_failure_continues(tmp_path: Path) -> None:
    client = MagicMock()
    client.render.side_effect = [
        RuntimeError("fail"),
        SsrResult(head=[], body="<div>ok</div>"),
    ]
    routes = [
        {"path": "/bad", "component": "Bad", "props": {}},
        {"path": "/good", "component": "Good", "props": {}},
    ]
    written = prerender_routes(ssr_client=client, routes=routes, outdir=tmp_path)
    assert len(written) == 1


def test_prerender_routes_page_object_has_version(tmp_path: Path) -> None:
    client = _mock_ssr_client()
    routes = [{"path": "/", "component": "Home", "props": {}}]
    prerender_routes(
        ssr_client=client,
        routes=routes,
        outdir=tmp_path,
        version="abc123",
    )
    call_args = client.render.call_args[0][0]
    assert call_args["version"] == "abc123"


def test_prerender_routes_page_object_has_errors_in_props(tmp_path: Path) -> None:
    client = _mock_ssr_client()
    routes = [{"path": "/", "component": "Home", "props": {"user": "test"}}]
    prerender_routes(ssr_client=client, routes=routes, outdir=tmp_path)
    call_args = client.render.call_args[0][0]
    assert call_args["props"]["errors"] == {}
    assert call_args["props"]["user"] == "test"


def test_prerender_routes_html_contains_page_data(tmp_path: Path) -> None:
    client = _mock_ssr_client()
    routes = [{"path": "/", "component": "Home", "props": {"user": "test"}}]
    prerender_routes(ssr_client=client, routes=routes, outdir=tmp_path)
    html = (tmp_path / "index.html").read_text()
    assert 'data-page="app"' in html
    page_data = decode_json(html.split('type="application/json">')[1].split("</script>")[0])
    assert page_data["component"] == "Home"
    assert page_data["props"]["user"] == "test"


def test_prerender_routes_empty_routes_list(tmp_path: Path) -> None:
    client = _mock_ssr_client()
    written = prerender_routes(ssr_client=client, routes=[], outdir=tmp_path)
    assert written == []
    client.render.assert_not_called()
