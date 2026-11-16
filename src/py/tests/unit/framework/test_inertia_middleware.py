"""Tests for the Inertia.js v3 protocol middleware."""

from typing import Any

import pytest

from vertebra.framework.inertia import (
    AsyncInertiaMiddleware,
    DeepMergeProp,
    DeferredProp,
    InertiaConfig,
    InertiaResponse,
    MergeProp,
    OnceProp,
    OptionalProp,
    ScrollProp,
    SyncInertiaMiddleware,
    inertia,
)
from vertebra.framework.ssr import AsyncSSRClient, SSRConfig, SSRResult, SyncSSRClient
from vertebra.utils.serializers import decode_json


class _TestRequest:
    def __init__(self, method: str = "GET", path: str = "/dashboard", headers: dict[str, str] | None = None) -> None:
        self.method = method
        self.path = path
        self.headers = headers or {}


def _make_request(
    method: str = "GET",
    path: str = "/dashboard",
    headers: dict[str, str] | None = None,
) -> _TestRequest:
    """Build a Request with the given headers."""
    return _TestRequest(method=method, path=path, headers=headers)


class _MockSSRClient:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.last_page: dict[str, Any] | None = None

    def render(self, page: dict[str, Any]) -> SSRResult:
        self.last_page = page
        if self.fail:
            raise ConnectionError("SSR server down")
        return SSRResult(
            head=["<title>SSR Page</title>"],
            body='<div id="app">SSR Content</div>',
        )

    def invalidate(self, paths: list[str] | None = None) -> None:
        _ = paths

    def stop(self) -> None:
        return None

    @staticmethod
    def from_config(config: SSRConfig) -> SyncSSRClient | None:
        _ = config
        return None


def _make_mock_ssr_client(*, fail: bool = False) -> _MockSSRClient:
    """Build a fake SSR client for testing."""
    return _MockSSRClient(fail=fail)


def test_inertia_config_defaults() -> None:
    cfg = InertiaConfig()
    assert cfg.enabled is False
    assert cfg.root_template == "index.html"
    assert cfg.version is None
    assert cfg.ssr is None
    assert cfg.ssr_enabled is False
    assert cfg.shared_data == {}


def test_inertia_config_custom_values() -> None:
    cfg = InertiaConfig(
        enabled=True,
        version="abc123",
        ssr=SSRConfig(),
        shared_data={"app_name": "Test"},
    )
    assert cfg.enabled is True
    assert cfg.version == "abc123"
    assert cfg.ssr_enabled is True
    assert cfg.shared_data == {"app_name": "Test"}


def test_prop_type_helpers_defer_creates_deferred_prop() -> None:
    prop = DeferredProp(lambda: [1, 2, 3], group="stats")
    assert isinstance(prop, DeferredProp)
    assert prop.group == "stats"
    assert prop.once is False
    assert prop.resolver() == [1, 2, 3]


def test_prop_type_helpers_defer_default_group() -> None:
    prop = DeferredProp(lambda: None)
    assert prop.group == "default"


def test_prop_type_helpers_merge_creates_merge_prop() -> None:
    prop = MergeProp(lambda: [1], prepend=True)
    assert isinstance(prop, MergeProp)
    assert prop.prepend is True
    assert prop.resolver() == [1]


def test_prop_type_helpers_deep_merge_creates_deep_merge_prop() -> None:
    prop = DeepMergeProp(lambda: {"a": 1})
    assert isinstance(prop, DeepMergeProp)
    assert prop.resolver() == {"a": 1}


def test_prop_type_helpers_once_creates_once_prop() -> None:
    prop = OnceProp(lambda: "token", expires_in=3600)
    assert isinstance(prop, OnceProp)
    assert prop.expires_in == 3600
    assert prop.resolver() == "token"


def test_prop_type_helpers_scroll_creates_scroll_prop() -> None:
    prop = ScrollProp(lambda: [], page_name="page", current_page=2, next_page=3)
    assert isinstance(prop, ScrollProp)
    assert prop.page_name == "page"
    assert prop.current_page == 2
    assert prop.next_page == 3
    assert prop.previous_page is None


def test_prop_type_helpers_optional_creates_optional_prop() -> None:
    prop = OptionalProp(lambda: "lazy")
    assert isinstance(prop, OptionalProp)
    assert prop.resolver() == "lazy"


def test_prop_type_helpers_inertia_convenience() -> None:
    resp = inertia("Pages/Home", {"title": "Home"}, clear_history=True)
    assert isinstance(resp, InertiaResponse)
    assert resp.component == "Pages/Home"
    assert resp.props == {"title": "Home"}
    assert resp.clear_history is True
    assert resp.encrypt_history is False


@pytest.mark.parametrize(
    ("headers", "expected"),
    [
        ({"X-Inertia": "true"}, True),
        ({}, False),
        ({"X-Inertia": "True"}, True),
    ],
    ids=["header-present", "header-absent", "case-insensitive-value"],
)
def test_is_inertia_request(headers: "dict[str, str]", expected: bool) -> None:
    mw = SyncInertiaMiddleware()
    req = _make_request(headers=headers)
    assert mw.is_inertia_request(req) is expected


def test_build_page_object_basic() -> None:
    mw = SyncInertiaMiddleware(InertiaConfig(version="v1"))
    req = _make_request()
    resp = InertiaResponse(component="Pages/Dashboard", props={"count": 42})
    page = mw.build_page_object(req, resp)
    assert page["component"] == "Pages/Dashboard"
    assert page["props"] == {"count": 42}
    assert page["url"] == "/dashboard"
    assert page["version"] == "v1"


def test_build_page_object_encrypt_history_included_when_true() -> None:
    mw = SyncInertiaMiddleware(InertiaConfig(version="v1"))
    req = _make_request()
    resp = InertiaResponse(component="Pages/Secret", encrypt_history=True)
    page = mw.build_page_object(req, resp)
    assert page["encryptHistory"] is True


def test_build_page_object_clear_history_included_when_true() -> None:
    mw = SyncInertiaMiddleware(InertiaConfig(version="v1"))
    req = _make_request()
    resp = InertiaResponse(component="Pages/Logout", clear_history=True)
    page = mw.build_page_object(req, resp)
    assert page["clearHistory"] is True


def test_build_page_object_optional_fields_omitted_when_false() -> None:
    mw = SyncInertiaMiddleware(InertiaConfig(version="v1"))
    req = _make_request()
    resp = InertiaResponse(component="Pages/Home")
    page = mw.build_page_object(req, resp)
    assert "encryptHistory" not in page
    assert "clearHistory" not in page


def test_build_page_object_shared_data_merged_into_props() -> None:
    cfg = InertiaConfig(version="v1", shared_data={"app_name": "Vertebra", "locale": "en"})
    mw = SyncInertiaMiddleware(cfg)
    req = _make_request()
    resp = InertiaResponse(component="Pages/Home", props={"title": "Home"})
    page = mw.build_page_object(req, resp)
    assert page["props"]["app_name"] == "Vertebra"
    assert page["props"]["locale"] == "en"
    assert page["props"]["title"] == "Home"


def test_build_page_object_request_props_override_shared_data() -> None:
    cfg = InertiaConfig(version="v1", shared_data={"locale": "en"})
    mw = SyncInertiaMiddleware(cfg)
    req = _make_request()
    resp = InertiaResponse(component="Pages/Home", props={"locale": "fr"})
    page = mw.build_page_object(req, resp)
    assert page["props"]["locale"] == "fr"


def test_partial_reloads_includes_only_requested_props() -> None:
    mw = SyncInertiaMiddleware(InertiaConfig(version="v1"))
    req = _make_request(
        headers={
            "X-Inertia": "true",
            "X-Inertia-Partial-Component": "Pages/Dashboard",
            "X-Inertia-Partial-Data": "count",
        }
    )
    resp = InertiaResponse(
        component="Pages/Dashboard",
        props={"count": 42, "users": ["alice", "bob"]},
    )
    page = mw.build_page_object(req, resp)
    assert "count" in page["props"]
    assert "users" not in page["props"]


def test_partial_reloads_exclude_removes_props() -> None:
    mw = SyncInertiaMiddleware(InertiaConfig(version="v1"))
    req = _make_request(
        headers={
            "X-Inertia": "true",
            "X-Inertia-Partial-Component": "Pages/Dashboard",
            "X-Inertia-Partial-Except": "users",
        }
    )
    resp = InertiaResponse(
        component="Pages/Dashboard",
        props={"count": 42, "users": ["alice"]},
    )
    page = mw.build_page_object(req, resp)
    assert "count" in page["props"]
    assert "users" not in page["props"]


def test_partial_reloads_wrong_component_includes_all() -> None:
    mw = SyncInertiaMiddleware(InertiaConfig(version="v1"))
    req = _make_request(
        headers={
            "X-Inertia": "true",
            "X-Inertia-Partial-Component": "Pages/Other",
            "X-Inertia-Partial-Data": "count",
        }
    )
    resp = InertiaResponse(
        component="Pages/Dashboard",
        props={"count": 42, "users": ["alice"]},
    )
    page = mw.build_page_object(req, resp)
    assert "count" in page["props"]
    assert "users" in page["props"]


def test_deferred_props_excluded_from_initial_response() -> None:
    mw = SyncInertiaMiddleware(InertiaConfig(version="v1"))
    req = _make_request()
    resp = InertiaResponse(
        component="Pages/Dashboard",
        props={
            "count": 42,
            "stats": DeferredProp(lambda: {"visits": 100}, group="analytics"),
        },
    )
    page = mw.build_page_object(req, resp)
    assert "stats" not in page["props"]
    assert page["deferredProps"] == {"analytics": ["stats"]}


def test_deferred_props_resolved_during_partial_reload() -> None:
    mw = SyncInertiaMiddleware(InertiaConfig(version="v1"))
    req = _make_request(
        headers={
            "X-Inertia": "true",
            "X-Inertia-Partial-Component": "Pages/Dashboard",
            "X-Inertia-Partial-Data": "stats",
        }
    )
    resp = InertiaResponse(
        component="Pages/Dashboard",
        props={
            "count": 42,
            "stats": DeferredProp(lambda: {"visits": 100}),
        },
    )
    page = mw.build_page_object(req, resp)
    assert page["props"]["stats"] == {"visits": 100}


def test_merge_props_metadata_in_page_object() -> None:
    mw = SyncInertiaMiddleware(InertiaConfig(version="v1"))
    req = _make_request()
    resp = InertiaResponse(
        component="Pages/Feed",
        props={"posts": MergeProp(lambda: [{"id": 1}])},
    )
    page = mw.build_page_object(req, resp)
    assert page["props"]["posts"] == [{"id": 1}]
    assert page["mergeProps"] == ["posts"]


def test_merge_props_prepend_metadata() -> None:
    mw = SyncInertiaMiddleware(InertiaConfig(version="v1"))
    req = _make_request()
    resp = InertiaResponse(
        component="Pages/Feed",
        props={"notifications": MergeProp(lambda: [{"id": 1}], prepend=True)},
    )
    page = mw.build_page_object(req, resp)
    assert page["prependProps"] == ["notifications"]


def test_merge_props_deep_merge_metadata() -> None:
    mw = SyncInertiaMiddleware(InertiaConfig(version="v1"))
    req = _make_request()
    resp = InertiaResponse(
        component="Pages/Settings",
        props={"preferences": DeepMergeProp(lambda: {"theme": "dark"})},
    )
    page = mw.build_page_object(req, resp)
    assert page["props"]["preferences"] == {"theme": "dark"}
    assert page["deepMergeProps"] == ["preferences"]


def test_once_props_included_initially() -> None:
    mw = SyncInertiaMiddleware(InertiaConfig(version="v1"))
    req = _make_request()
    resp = InertiaResponse(
        component="Pages/Home",
        props={"token": OnceProp(lambda: "abc123", expires_in=3600)},
    )
    page = mw.build_page_object(req, resp)
    assert page["props"]["token"] == "abc123"
    assert "token" in page["onceProps"]
    assert page["onceProps"]["token"]["expiresAt"] == 3600


def test_once_props_skipped_when_cached() -> None:
    mw = SyncInertiaMiddleware(InertiaConfig(version="v1"))
    req = _make_request(
        headers={
            "X-Inertia-Except-Once-Props": "token",
        }
    )
    resp = InertiaResponse(
        component="Pages/Home",
        props={"token": OnceProp(lambda: "abc123"), "name": "Test"},
    )
    page = mw.build_page_object(req, resp)
    assert "token" not in page["props"]
    assert page["props"]["name"] == "Test"


def test_scroll_props_in_page_object() -> None:
    mw = SyncInertiaMiddleware(InertiaConfig(version="v1"))
    req = _make_request()
    resp = InertiaResponse(
        component="Pages/Feed",
        props={
            "items": ScrollProp(
                lambda: [1, 2, 3],
                current_page=1,
                next_page=2,
            ),
        },
    )
    page = mw.build_page_object(req, resp)
    assert page["props"]["items"] == [1, 2, 3]
    assert page["scrollProps"]["items"]["currentPage"] == 1
    assert page["scrollProps"]["items"]["nextPage"] == 2
    assert page["scrollProps"]["items"]["previousPage"] is None


def test_optional_props_excluded_on_initial_load() -> None:
    mw = SyncInertiaMiddleware(InertiaConfig(version="v1"))
    req = _make_request()
    resp = InertiaResponse(
        component="Pages/Home",
        props={"heavy": OptionalProp(lambda: "expensive"), "name": "Test"},
    )
    page = mw.build_page_object(req, resp)
    assert "heavy" not in page["props"]
    assert page["props"]["name"] == "Test"


def test_optional_props_resolved_during_partial_reload() -> None:
    mw = SyncInertiaMiddleware(InertiaConfig(version="v1"))
    req = _make_request(
        headers={
            "X-Inertia": "true",
            "X-Inertia-Partial-Component": "Pages/Home",
            "X-Inertia-Partial-Data": "heavy",
        }
    )
    resp = InertiaResponse(
        component="Pages/Home",
        props={"heavy": OptionalProp(lambda: "expensive"), "name": "Test"},
    )
    page = mw.build_page_object(req, resp)
    assert page["props"]["heavy"] == "expensive"


def test_version_conflict_mismatch_returns_409() -> None:
    mw = SyncInertiaMiddleware(InertiaConfig(version="v2"))
    req = _make_request(
        headers={
            "X-Inertia": "true",
            "X-Inertia-Version": "v1",
        }
    )
    resp = InertiaResponse(component="Pages/Home")
    result = mw.process(req, resp)
    assert result.status == 409
    assert result.headers.get("X-Inertia-Location") == "/dashboard"


def test_version_conflict_match_no_conflict() -> None:
    mw = SyncInertiaMiddleware(InertiaConfig(version="v1"))
    req = _make_request(
        headers={
            "X-Inertia": "true",
            "X-Inertia-Version": "v1",
        }
    )
    resp = InertiaResponse(component="Pages/Home")
    result = mw.process(req, resp)
    assert result.status == 200


def test_version_conflict_only_on_get() -> None:
    mw = SyncInertiaMiddleware(InertiaConfig(version="v2"))
    req = _make_request(
        method="POST",
        headers={
            "X-Inertia": "true",
            "X-Inertia-Version": "v1",
        },
    )
    resp = InertiaResponse(component="Pages/Home")
    result = mw.process(req, resp)
    assert result.status == 200


@pytest.mark.parametrize(
    ("method", "status", "expected"),
    [
        ("PUT", 302, True),
        ("PATCH", 302, True),
        ("DELETE", 302, True),
        ("GET", 302, False),
        ("PUT", 301, True),
    ],
    ids=[
        "302-to-303-for-put",
        "302-to-303-for-patch",
        "302-to-303-for-delete",
        "302-kept-for-get",
        "301-to-303-for-put",
    ],
)
def test_redirect_fixup(method: str, status: int, expected: bool) -> None:
    mw = SyncInertiaMiddleware()
    assert mw.should_redirect_303(method, status) is expected


def test_json_response_inertia_request_returns_json() -> None:
    mw = SyncInertiaMiddleware(InertiaConfig(version="v1"))
    req = _make_request(headers={"X-Inertia": "true", "X-Inertia-Version": "v1"})
    resp = InertiaResponse(component="Pages/Home", props={"title": "Home"})
    result = mw.process(req, resp)
    assert result.status == 200
    assert result.headers.get("Content-Type") == "application/json"
    assert result.headers.get("X-Inertia") == "true"
    assert result.headers.get("Vary") == "X-Inertia"
    page = decode_json(result.body)
    assert page["component"] == "Pages/Home"
    assert page["props"]["title"] == "Home"
    assert page["version"] == "v1"


def test_html_response_initial_load_returns_html() -> None:
    mw = SyncInertiaMiddleware(InertiaConfig(version="v1"))
    req = _make_request()
    resp = InertiaResponse(component="Pages/Home", props={"title": "Home"})
    result = mw.process(req, resp)
    assert result.status == 200
    assert "text/html" in (result.headers.get("Content-Type") or "")
    html = result.body.decode("utf-8")
    assert '<script data-page="app" type="application/json">' in html
    assert '"component":"Pages/Home"' in html or '"component": "Pages/Home"' in html


def test_html_response_contains_v3_script_tag() -> None:
    mw = SyncInertiaMiddleware(InertiaConfig(version="v1"))
    req = _make_request()
    resp = InertiaResponse(component="Pages/Home")
    result = mw.process(req, resp)
    html = result.body.decode("utf-8")
    assert 'data-page="app"' in html
    assert 'type="application/json"' in html


def test_ssr_integration_renders_head_and_body() -> None:
    client = _make_mock_ssr_client()
    cfg = InertiaConfig(version="v1", ssr=SSRConfig())
    mw = SyncInertiaMiddleware(cfg, ssr_client=client)
    req = _make_request()
    resp = InertiaResponse(component="Pages/Home")
    result = mw.process(req, resp)
    html = result.body.decode("utf-8")
    assert "<title>SSR Page</title>" in html
    assert "SSR Content" in html
    assert client.last_page is not None


def test_ssr_integration_csr_fallback_on_failure() -> None:
    client = _make_mock_ssr_client(fail=True)
    cfg = InertiaConfig(version="v1", ssr=SSRConfig())
    mw = SyncInertiaMiddleware(cfg, ssr_client=client)
    req = _make_request()
    resp = InertiaResponse(component="Pages/Home")
    result = mw.process(req, resp)
    assert result.status == 200
    html = result.body.decode("utf-8")
    assert '<script data-page="app" type="application/json">' in html
    assert "<title>SSR Page</title>" not in html


def test_ssr_integration_not_used_for_inertia_xhr() -> None:
    client = _make_mock_ssr_client()
    cfg = InertiaConfig(version="v1", ssr=SSRConfig())
    mw = SyncInertiaMiddleware(cfg, ssr_client=client)
    req = _make_request(headers={"X-Inertia": "true", "X-Inertia-Version": "v1"})
    resp = InertiaResponse(component="Pages/Home")
    result = mw.process(req, resp)
    assert result.headers.get("Content-Type") == "application/json"
    assert client.last_page is None


def test_error_bag_header_passed_through() -> None:
    """The error bag header is read by resolve_props (via request headers)."""
    mw = SyncInertiaMiddleware(InertiaConfig(version="v1"))
    req = _make_request(headers={"X-Inertia-Error-Bag": "login"})
    resp = InertiaResponse(
        component="Pages/Login",
        props={"errors": {"login": {"email": "required"}}},
    )
    page = mw.build_page_object(req, resp)
    assert page["props"]["errors"]["login"]["email"] == "required"


def test_version_auto_detect_dev_version_when_no_manifest() -> None:
    mw = SyncInertiaMiddleware(InertiaConfig(manifest_path="/nonexistent/manifest.json"))
    assert mw.get_version() == "dev"


def test_version_auto_detect_explicit_version_takes_precedence() -> None:
    mw = SyncInertiaMiddleware(InertiaConfig(version="abc123"))
    assert mw.get_version() == "abc123"


def test_callable_props_resolved() -> None:
    mw = SyncInertiaMiddleware(InertiaConfig(version="v1"))
    req = _make_request()
    resp = InertiaResponse(
        component="Pages/Home",
        props={"timestamp": lambda: 1234567890},
    )
    page = mw.build_page_object(req, resp)
    assert page["props"]["timestamp"] == 1234567890


@pytest.mark.anyio
async def test_async_inertia_middleware_process() -> None:
    class _AsyncMockSSRClient:
        def __init__(self, *, fail: bool = False) -> None:
            self.fail = fail
            self.last_page: dict[str, Any] | None = None

        async def render(self, page: dict[str, Any]) -> SSRResult:
            self.last_page = page
            if self.fail:
                raise ConnectionError("Async SSR server down")
            return SSRResult(
                head=["<title>Async SSR Page</title>"],
                body='<div id="app">Async SSR Content</div>',
            )

        def invalidate(self, paths: list[str] | None = None) -> None:
            _ = paths

        def stop(self) -> None:
            return None

        @staticmethod
        def from_config(config: SSRConfig) -> AsyncSSRClient | None:
            _ = config
            return None

    client = _AsyncMockSSRClient()
    cfg = InertiaConfig(version="v1", ssr=SSRConfig())
    mw = AsyncInertiaMiddleware(cfg, ssr_client=client)

    conflict_req = _make_request(headers={"X-Inertia": "true", "X-Inertia-Version": "v0"})
    conflict_res = await mw.process(conflict_req, InertiaResponse(component="Pages/Home"))
    assert conflict_res.status == 409

    json_req = _make_request(headers={"X-Inertia": "true", "X-Inertia-Version": "v1"})
    json_res = await mw.process(json_req, InertiaResponse(component="Pages/Home", props={"title": "Async"}))
    assert json_res.status == 200
    assert decode_json(json_res.body)["props"]["title"] == "Async"
    assert client.last_page is None

    html_res = await mw.process(_make_request(), InertiaResponse(component="Pages/Home"))
    html_text = html_res.body.decode("utf-8")
    assert "<title>Async SSR Page</title>" in html_text
    assert "Async SSR Content" in html_text
    assert client.last_page is not None

    fail_mw = AsyncInertiaMiddleware(cfg, ssr_client=_AsyncMockSSRClient(fail=True))
    fallback_res = await fail_mw.process(_make_request(), InertiaResponse(component="Pages/Home"))
    assert fallback_res.status == 200
    assert "<title>Async SSR Page</title>" not in fallback_res.body.decode("utf-8")
