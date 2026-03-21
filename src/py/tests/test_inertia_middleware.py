"""Tests for the Inertia.js v3 protocol middleware."""

import json

from vertebra.framework._inertia import (
    DeepMergeProp,
    DeferredProp,
    InertiaConfig,
    InertiaMiddleware,
    InertiaResponse,
    MergeProp,
    OnceProp,
    OptionalProp,
    ScrollProp,
    SsrResult,
    deep_merge,
    defer,
    inertia,
    merge,
    once,
    optional,
    scroll,
)
from vertebra.framework._types import Headers, Request

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_request(
    method: str = "GET",
    path: str = "/dashboard",
    headers: dict[str, str] | None = None,
) -> Request:
    """Build a Request with the given headers."""
    pairs = [(k, v) for k, v in (headers or {}).items()]
    return Request(method=method, path=path, headers=Headers(_data=pairs))


# ---------------------------------------------------------------------------
# InertiaConfig
# ---------------------------------------------------------------------------


class TestInertiaConfig:
    def test_defaults(self) -> None:
        cfg = InertiaConfig()
        assert cfg.enabled is False
        assert cfg.root_template == "index.html"
        assert cfg.version is None
        assert cfg.ssr_enabled is False
        assert cfg.ssr_mode == "native"
        assert cfg.ssr_url == "http://127.0.0.1:13714"
        assert cfg.ssr_timeout == 2.0
        assert cfg.ssr_pool_size == 4
        assert cfg.ssr_entry is None
        assert cfg.sidecar_port == 13714
        assert cfg.shared_data == {}

    def test_custom_values(self) -> None:
        cfg = InertiaConfig(
            enabled=True,
            version="abc123",
            ssr_enabled=True,
            shared_data={"app_name": "Test"},
        )
        assert cfg.enabled is True
        assert cfg.version == "abc123"
        assert cfg.ssr_enabled is True
        assert cfg.shared_data == {"app_name": "Test"}


# ---------------------------------------------------------------------------
# Prop type helpers
# ---------------------------------------------------------------------------


class TestPropTypeHelpers:
    def test_defer_creates_deferred_prop(self) -> None:
        prop = defer(lambda: [1, 2, 3], group="stats")
        assert isinstance(prop, DeferredProp)
        assert prop.group == "stats"
        assert prop.once is False
        assert prop.resolver() == [1, 2, 3]

    def test_defer_default_group(self) -> None:
        prop = defer(lambda: None)
        assert prop.group == "default"

    def test_merge_creates_merge_prop(self) -> None:
        prop = merge(lambda: [1], prepend=True)
        assert isinstance(prop, MergeProp)
        assert prop.prepend is True
        assert prop.resolver() == [1]

    def test_deep_merge_creates_deep_merge_prop(self) -> None:
        prop = deep_merge(lambda: {"a": 1})
        assert isinstance(prop, DeepMergeProp)
        assert prop.resolver() == {"a": 1}

    def test_once_creates_once_prop(self) -> None:
        prop = once(lambda: "token", expires_in=3600)
        assert isinstance(prop, OnceProp)
        assert prop.expires_in == 3600
        assert prop.resolver() == "token"

    def test_scroll_creates_scroll_prop(self) -> None:
        prop = scroll(lambda: [], page_name="page", current_page=2, next_page=3)
        assert isinstance(prop, ScrollProp)
        assert prop.page_name == "page"
        assert prop.current_page == 2
        assert prop.next_page == 3
        assert prop.previous_page is None

    def test_optional_creates_optional_prop(self) -> None:
        prop = optional(lambda: "lazy")
        assert isinstance(prop, OptionalProp)
        assert prop.resolver() == "lazy"

    def test_inertia_convenience(self) -> None:
        resp = inertia("Pages/Home", {"title": "Home"}, clear_history=True)
        assert isinstance(resp, InertiaResponse)
        assert resp.component == "Pages/Home"
        assert resp.props == {"title": "Home"}
        assert resp.clear_history is True
        assert resp.encrypt_history is False


# ---------------------------------------------------------------------------
# Request detection
# ---------------------------------------------------------------------------


class TestIsInertiaRequest:
    def test_inertia_header_present(self) -> None:
        mw = InertiaMiddleware()
        req = _make_request(headers={"X-Inertia": "true"})
        assert mw.is_inertia_request(req) is True

    def test_inertia_header_absent(self) -> None:
        mw = InertiaMiddleware()
        req = _make_request()
        assert mw.is_inertia_request(req) is False

    def test_inertia_header_case_insensitive_value(self) -> None:
        mw = InertiaMiddleware()
        req = _make_request(headers={"X-Inertia": "True"})
        assert mw.is_inertia_request(req) is True


# ---------------------------------------------------------------------------
# Page object construction
# ---------------------------------------------------------------------------


class TestBuildPageObject:
    def test_basic_page_object(self) -> None:
        mw = InertiaMiddleware(InertiaConfig(version="v1"))
        req = _make_request()
        resp = InertiaResponse(component="Pages/Dashboard", props={"count": 42})
        page = mw.build_page_object(req, resp)
        assert page["component"] == "Pages/Dashboard"
        assert page["props"] == {"count": 42}
        assert page["url"] == "/dashboard"
        assert page["version"] == "v1"

    def test_encrypt_history_included_when_true(self) -> None:
        mw = InertiaMiddleware(InertiaConfig(version="v1"))
        req = _make_request()
        resp = InertiaResponse(component="Pages/Secret", encrypt_history=True)
        page = mw.build_page_object(req, resp)
        assert page["encryptHistory"] is True

    def test_clear_history_included_when_true(self) -> None:
        mw = InertiaMiddleware(InertiaConfig(version="v1"))
        req = _make_request()
        resp = InertiaResponse(component="Pages/Logout", clear_history=True)
        page = mw.build_page_object(req, resp)
        assert page["clearHistory"] is True

    def test_optional_fields_omitted_when_false(self) -> None:
        mw = InertiaMiddleware(InertiaConfig(version="v1"))
        req = _make_request()
        resp = InertiaResponse(component="Pages/Home")
        page = mw.build_page_object(req, resp)
        assert "encryptHistory" not in page
        assert "clearHistory" not in page

    def test_shared_data_merged_into_props(self) -> None:
        cfg = InertiaConfig(version="v1", shared_data={"app_name": "Vertebra", "locale": "en"})
        mw = InertiaMiddleware(cfg)
        req = _make_request()
        resp = InertiaResponse(component="Pages/Home", props={"title": "Home"})
        page = mw.build_page_object(req, resp)
        assert page["props"]["app_name"] == "Vertebra"
        assert page["props"]["locale"] == "en"
        assert page["props"]["title"] == "Home"

    def test_request_props_override_shared_data(self) -> None:
        cfg = InertiaConfig(version="v1", shared_data={"locale": "en"})
        mw = InertiaMiddleware(cfg)
        req = _make_request()
        resp = InertiaResponse(component="Pages/Home", props={"locale": "fr"})
        page = mw.build_page_object(req, resp)
        assert page["props"]["locale"] == "fr"


# ---------------------------------------------------------------------------
# Partial reloads
# ---------------------------------------------------------------------------


class TestPartialReloads:
    def test_partial_reload_includes_only_requested_props(self) -> None:
        mw = InertiaMiddleware(InertiaConfig(version="v1"))
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

    def test_partial_exclude_removes_props(self) -> None:
        mw = InertiaMiddleware(InertiaConfig(version="v1"))
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

    def test_partial_reload_wrong_component_includes_all(self) -> None:
        mw = InertiaMiddleware(InertiaConfig(version="v1"))
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
        # Different component -> all props included
        assert "count" in page["props"]
        assert "users" in page["props"]


# ---------------------------------------------------------------------------
# Deferred props
# ---------------------------------------------------------------------------


class TestDeferredProps:
    def test_deferred_excluded_from_initial_response(self) -> None:
        mw = InertiaMiddleware(InertiaConfig(version="v1"))
        req = _make_request()
        resp = InertiaResponse(
            component="Pages/Dashboard",
            props={
                "count": 42,
                "stats": defer(lambda: {"visits": 100}, group="analytics"),
            },
        )
        page = mw.build_page_object(req, resp)
        assert "stats" not in page["props"]
        assert page["deferredProps"] == {"analytics": ["stats"]}

    def test_deferred_resolved_during_partial_reload(self) -> None:
        mw = InertiaMiddleware(InertiaConfig(version="v1"))
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
                "stats": defer(lambda: {"visits": 100}),
            },
        )
        page = mw.build_page_object(req, resp)
        assert page["props"]["stats"] == {"visits": 100}


# ---------------------------------------------------------------------------
# Merge props
# ---------------------------------------------------------------------------


class TestMergeProps:
    def test_merge_props_metadata_in_page_object(self) -> None:
        mw = InertiaMiddleware(InertiaConfig(version="v1"))
        req = _make_request()
        resp = InertiaResponse(
            component="Pages/Feed",
            props={"posts": merge(lambda: [{"id": 1}])},
        )
        page = mw.build_page_object(req, resp)
        assert page["props"]["posts"] == [{"id": 1}]
        assert page["mergeProps"] == ["posts"]

    def test_prepend_props_metadata(self) -> None:
        mw = InertiaMiddleware(InertiaConfig(version="v1"))
        req = _make_request()
        resp = InertiaResponse(
            component="Pages/Feed",
            props={"notifications": merge(lambda: [{"id": 1}], prepend=True)},
        )
        page = mw.build_page_object(req, resp)
        assert page["prependProps"] == ["notifications"]

    def test_deep_merge_props_metadata(self) -> None:
        mw = InertiaMiddleware(InertiaConfig(version="v1"))
        req = _make_request()
        resp = InertiaResponse(
            component="Pages/Settings",
            props={"preferences": deep_merge(lambda: {"theme": "dark"})},
        )
        page = mw.build_page_object(req, resp)
        assert page["props"]["preferences"] == {"theme": "dark"}
        assert page["deepMergeProps"] == ["preferences"]


# ---------------------------------------------------------------------------
# Once props
# ---------------------------------------------------------------------------


class TestOnceProps:
    def test_once_props_included_initially(self) -> None:
        mw = InertiaMiddleware(InertiaConfig(version="v1"))
        req = _make_request()
        resp = InertiaResponse(
            component="Pages/Home",
            props={"token": once(lambda: "abc123", expires_in=3600)},
        )
        page = mw.build_page_object(req, resp)
        assert page["props"]["token"] == "abc123"
        assert "token" in page["onceProps"]
        assert page["onceProps"]["token"]["expiresAt"] == 3600

    def test_once_props_skipped_when_cached(self) -> None:
        mw = InertiaMiddleware(InertiaConfig(version="v1"))
        req = _make_request(
            headers={
                "X-Inertia-Except-Once-Props": "token",
            }
        )
        resp = InertiaResponse(
            component="Pages/Home",
            props={"token": once(lambda: "abc123"), "name": "Test"},
        )
        page = mw.build_page_object(req, resp)
        assert "token" not in page["props"]
        assert page["props"]["name"] == "Test"


# ---------------------------------------------------------------------------
# Scroll props
# ---------------------------------------------------------------------------


class TestScrollProps:
    def test_scroll_props_in_page_object(self) -> None:
        mw = InertiaMiddleware(InertiaConfig(version="v1"))
        req = _make_request()
        resp = InertiaResponse(
            component="Pages/Feed",
            props={
                "items": scroll(
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


# ---------------------------------------------------------------------------
# Optional props
# ---------------------------------------------------------------------------


class TestOptionalProps:
    def test_optional_excluded_on_initial_load(self) -> None:
        mw = InertiaMiddleware(InertiaConfig(version="v1"))
        req = _make_request()
        resp = InertiaResponse(
            component="Pages/Home",
            props={"heavy": optional(lambda: "expensive"), "name": "Test"},
        )
        page = mw.build_page_object(req, resp)
        assert "heavy" not in page["props"]
        assert page["props"]["name"] == "Test"

    def test_optional_resolved_during_partial_reload(self) -> None:
        mw = InertiaMiddleware(InertiaConfig(version="v1"))
        req = _make_request(
            headers={
                "X-Inertia": "true",
                "X-Inertia-Partial-Component": "Pages/Home",
                "X-Inertia-Partial-Data": "heavy",
            }
        )
        resp = InertiaResponse(
            component="Pages/Home",
            props={"heavy": optional(lambda: "expensive"), "name": "Test"},
        )
        page = mw.build_page_object(req, resp)
        assert page["props"]["heavy"] == "expensive"


# ---------------------------------------------------------------------------
# Version conflict
# ---------------------------------------------------------------------------


class TestVersionConflict:
    def test_version_mismatch_returns_409(self) -> None:
        mw = InertiaMiddleware(InertiaConfig(version="v2"))
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

    def test_version_match_no_conflict(self) -> None:
        mw = InertiaMiddleware(InertiaConfig(version="v1"))
        req = _make_request(
            headers={
                "X-Inertia": "true",
                "X-Inertia-Version": "v1",
            }
        )
        resp = InertiaResponse(component="Pages/Home")
        result = mw.process(req, resp)
        assert result.status == 200

    def test_version_conflict_only_on_get(self) -> None:
        mw = InertiaMiddleware(InertiaConfig(version="v2"))
        req = _make_request(
            method="POST",
            headers={
                "X-Inertia": "true",
                "X-Inertia-Version": "v1",
            },
        )
        resp = InertiaResponse(component="Pages/Home")
        result = mw.process(req, resp)
        # POST should not trigger 409
        assert result.status == 200


# ---------------------------------------------------------------------------
# Redirect fixup
# ---------------------------------------------------------------------------


class TestRedirectFixup:
    def test_302_to_303_for_put(self) -> None:
        mw = InertiaMiddleware()
        assert mw.should_redirect_303("PUT", 302) is True

    def test_302_to_303_for_patch(self) -> None:
        mw = InertiaMiddleware()
        assert mw.should_redirect_303("PATCH", 302) is True

    def test_302_to_303_for_delete(self) -> None:
        mw = InertiaMiddleware()
        assert mw.should_redirect_303("DELETE", 302) is True

    def test_302_kept_for_get(self) -> None:
        mw = InertiaMiddleware()
        assert mw.should_redirect_303("GET", 302) is False

    def test_301_to_303_for_put(self) -> None:
        mw = InertiaMiddleware()
        assert mw.should_redirect_303("PUT", 301) is True


# ---------------------------------------------------------------------------
# JSON response for Inertia XHR
# ---------------------------------------------------------------------------


class TestJsonResponse:
    def test_inertia_request_returns_json(self) -> None:
        mw = InertiaMiddleware(InertiaConfig(version="v1"))
        req = _make_request(headers={"X-Inertia": "true", "X-Inertia-Version": "v1"})
        resp = InertiaResponse(component="Pages/Home", props={"title": "Home"})
        result = mw.process(req, resp)
        assert result.status == 200
        assert result.headers.get("Content-Type") == "application/json"
        assert result.headers.get("X-Inertia") == "true"
        assert result.headers.get("Vary") == "X-Inertia"
        page = json.loads(result.body)
        assert page["component"] == "Pages/Home"
        assert page["props"]["title"] == "Home"
        assert page["version"] == "v1"


# ---------------------------------------------------------------------------
# HTML response for initial page load
# ---------------------------------------------------------------------------


class TestHtmlResponse:
    def test_initial_load_returns_html(self) -> None:
        mw = InertiaMiddleware(InertiaConfig(version="v1"))
        req = _make_request()
        resp = InertiaResponse(component="Pages/Home", props={"title": "Home"})
        result = mw.process(req, resp)
        assert result.status == 200
        assert "text/html" in (result.headers.get("Content-Type") or "")
        html = result.body.decode("utf-8")
        assert '<script data-page="app" type="application/json">' in html
        assert '"component": "Pages/Home"' in html

    def test_html_contains_v3_script_tag(self) -> None:
        mw = InertiaMiddleware(InertiaConfig(version="v1"))
        req = _make_request()
        resp = InertiaResponse(component="Pages/Home")
        result = mw.process(req, resp)
        html = result.body.decode("utf-8")
        assert 'data-page="app"' in html
        assert 'type="application/json"' in html


# ---------------------------------------------------------------------------
# SSR integration
# ---------------------------------------------------------------------------


class _MockSsrClient:
    """Fake SSR client for testing."""

    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.last_page: dict | None = None

    def render(self, page: dict) -> SsrResult:
        self.last_page = page
        if self.fail:
            raise ConnectionError("SSR server down")
        return SsrResult(
            head=["<title>SSR Page</title>"],
            body='<div id="app">SSR Content</div>',
        )


class TestSsrIntegration:
    def test_ssr_renders_head_and_body(self) -> None:
        client = _MockSsrClient()
        cfg = InertiaConfig(version="v1", ssr_enabled=True)
        mw = InertiaMiddleware(cfg, ssr_client=client)
        req = _make_request()
        resp = InertiaResponse(component="Pages/Home")
        result = mw.process(req, resp)
        html = result.body.decode("utf-8")
        assert "<title>SSR Page</title>" in html
        assert "SSR Content" in html
        assert client.last_page is not None

    def test_csr_fallback_on_ssr_failure(self) -> None:
        client = _MockSsrClient(fail=True)
        cfg = InertiaConfig(version="v1", ssr_enabled=True)
        mw = InertiaMiddleware(cfg, ssr_client=client)
        req = _make_request()
        resp = InertiaResponse(component="Pages/Home")
        result = mw.process(req, resp)
        assert result.status == 200
        html = result.body.decode("utf-8")
        # Should still have the script tag (CSR fallback)
        assert '<script data-page="app" type="application/json">' in html
        # Should NOT have SSR content
        assert "<title>SSR Page</title>" not in html

    def test_ssr_not_used_for_inertia_xhr(self) -> None:
        client = _MockSsrClient()
        cfg = InertiaConfig(version="v1", ssr_enabled=True)
        mw = InertiaMiddleware(cfg, ssr_client=client)
        req = _make_request(headers={"X-Inertia": "true", "X-Inertia-Version": "v1"})
        resp = InertiaResponse(component="Pages/Home")
        result = mw.process(req, resp)
        assert result.headers.get("Content-Type") == "application/json"
        # SSR client should not have been called
        assert client.last_page is None


# ---------------------------------------------------------------------------
# Error bag handling
# ---------------------------------------------------------------------------


class TestErrorBag:
    def test_error_bag_header_passed_through(self) -> None:
        """The error bag header is read by resolve_props (via request headers)."""
        mw = InertiaMiddleware(InertiaConfig(version="v1"))
        req = _make_request(headers={"X-Inertia-Error-Bag": "login"})
        resp = InertiaResponse(
            component="Pages/Login",
            props={"errors": {"login": {"email": "required"}}},
        )
        page = mw.build_page_object(req, resp)
        # Error bag data is in props as normal; the header tells the client
        # which bag to display
        assert page["props"]["errors"]["login"]["email"] == "required"


# ---------------------------------------------------------------------------
# Version auto-detection
# ---------------------------------------------------------------------------


class TestVersionAutoDetect:
    def test_dev_version_when_no_manifest(self) -> None:
        mw = InertiaMiddleware(InertiaConfig(manifest_path="/nonexistent/manifest.json"))
        assert mw.get_version() == "dev"

    def test_explicit_version_takes_precedence(self) -> None:
        mw = InertiaMiddleware(InertiaConfig(version="abc123"))
        assert mw.get_version() == "abc123"


# ---------------------------------------------------------------------------
# Callable props
# ---------------------------------------------------------------------------


class TestCallableProps:
    def test_callable_props_resolved(self) -> None:
        mw = InertiaMiddleware(InertiaConfig(version="v1"))
        req = _make_request()
        resp = InertiaResponse(
            component="Pages/Home",
            props={"timestamp": lambda: 1234567890},
        )
        page = mw.build_page_object(req, resp)
        assert page["props"]["timestamp"] == 1234567890
