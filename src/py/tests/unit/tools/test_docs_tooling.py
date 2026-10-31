"""Behavioral unit tests for Zensical documentation post-build and verification tools."""

from pathlib import Path

import pytest
from tools.docs.postbuild import (
    build_postbuild_artifacts,
    compact_site_html,
    emit_legacy_html_redirects,
    generate_llms_files,
    sync_rustdoc_artifacts,
)
from tools.docs.postbuild import main as postbuild_main
from tools.docs.verify_docs_artifacts import (
    DocsVerificationError,
    verify_site_artifacts,
    verify_typescript_exports,
)
from tools.docs.verify_docs_artifacts import main as verify_main

__all__ = (
    "test_build_postbuild_artifacts_and_cli_main",
    "test_compact_site_html_preserves_pre_and_textarea_indentation",
    "test_emit_legacy_html_redirects_preserves_hashes",
    "test_generate_llms_files_expands_snippets_and_orders_nav",
    "test_generate_llms_files_fallback_nav_uses_frontmatter_title",
    "test_sync_rustdoc_artifacts_copies_when_present",
    "test_verify_site_artifacts_accepts_clean_site_and_exemptions",
    "test_verify_site_artifacts_rejects_bloated_secondary_toc",
    "test_verify_site_artifacts_rejects_broken_same_page_anchors",
    "test_verify_site_artifacts_rejects_leaked_docstrings_and_tmp_path",
    "test_verify_site_artifacts_rejects_made_with_zensical",
    "test_verify_site_artifacts_rejects_missing_inventories",
    "test_verify_site_artifacts_rejects_outer_badge_border",
    "test_verify_site_artifacts_rejects_oversized_pages",
    "test_verify_typescript_exports_detects_missing_subpath_and_symbol",
)


def test_generate_llms_files_expands_snippets_and_orders_nav(tmp_path: Path) -> None:
    """Verify generate_llms_files expands snippet markers and preserves nav ordering."""
    config_path, docs_dir, site_dir = _write_sample_project(tmp_path)

    generate_llms_files(config_path=config_path, docs_dir=docs_dir, site_dir=site_dir)

    llms_txt = (site_dir / "llms.txt").read_text(encoding="utf-8")
    llms_full = (site_dir / "llms-full.txt").read_text(encoding="utf-8")

    assert llms_txt.startswith("# Vertebra\n\n> Polyglot application backbone for Python, Rust, and TypeScript.")
    assert "https://vertebra.litestar.dev/" in llms_txt
    assert "https://vertebra.litestar.dev/getting-started/" in llms_txt
    assert "https://vertebra.litestar.dev/getting-started/quickstart/" in llms_txt
    assert llms_txt.index("/getting-started/") < llms_txt.index("/getting-started/quickstart/")

    assert "    def main() -> int:" in llms_full
    assert "        return 42" in llms_full
    assert "--8<--" not in llms_full
    assert "Standalone demo." not in llms_full
    assert 'if __name__ == "__main__":' not in llms_full
    assert "<script>console.log(1);</script>" not in llms_full
    assert "hide:" not in llms_full
    assert "frontmatter internal comment" not in llms_full


def test_generate_llms_files_fallback_nav_uses_frontmatter_title(tmp_path: Path) -> None:
    """Verify generate_llms_files falls back to YAML frontmatter title when nav is empty."""
    docs_dir = tmp_path / "docs"
    site_dir = tmp_path / "site"
    docs_dir.mkdir(parents=True)
    site_dir.mkdir(parents=True)
    (docs_dir / "guide.md").write_text(
        "---\ntitle: Frontmatter Guide Title\nhide:\n  - toc\n---\n\nBody paragraph.\n",
        encoding="utf-8",
    )
    config_path = tmp_path / "zensical.toml"
    config_path.write_text(
        '[project]\nsite_name = "Vertebra"\nsite_url = "https://vertebra.litestar.dev/"\nnav = []\n',
        encoding="utf-8",
    )

    generate_llms_files(config_path=config_path, docs_dir=docs_dir, site_dir=site_dir)

    llms_txt = (site_dir / "llms.txt").read_text(encoding="utf-8")
    llms_full = (site_dir / "llms-full.txt").read_text(encoding="utf-8")
    assert "- [Frontmatter Guide Title](https://vertebra.litestar.dev/guide/)" in llms_txt
    assert "hide:" not in llms_full


def test_emit_legacy_html_redirects_preserves_hashes(tmp_path: Path) -> None:
    """Verify emit_legacy_html_redirects emits fragment-preserving flat HTML stubs."""
    config_path, _, site_dir = _write_sample_project(tmp_path)
    server_dir = site_dir / "reference" / "server"
    server_dir.mkdir(parents=True, exist_ok=True)
    (server_dir / "index.html").write_text("<!doctype html><html><body>Server</body></html>\n", encoding="utf-8")

    emit_legacy_html_redirects(config_path=config_path, site_dir=site_dir)

    backpressure_stub = (site_dir / "backpressure.html").read_text(encoding="utf-8")
    assert "url=/architecture/http-ingress/" in backpressure_stub
    assert 'href="https://vertebra.litestar.dev/architecture/http-ingress/"' in backpressure_stub
    assert "window.location.hash" in backpressure_stub

    parallelism_stub = (site_dir / "parallelism.html").read_text(encoding="utf-8")
    assert "/architecture/pyo3-and-workers/#worker-lanes" in parallelism_stub
    assert "window.location.hash || " in parallelism_stub

    root_stub = (site_dir / "index_legacy.html").read_text(encoding="utf-8")
    assert "url=/" in root_stub

    server_stub = (site_dir / "reference" / "server.html").read_text(encoding="utf-8")
    assert "url=/reference/server/" in server_stub
    assert 'href="https://vertebra.litestar.dev/reference/server/"' in server_stub
    assert "window.location.hash" in server_stub


def test_sync_rustdoc_artifacts_copies_when_present(tmp_path: Path) -> None:
    """Verify sync_rustdoc_artifacts copies cargo doc output into site/rustdoc when present."""
    site_dir = tmp_path / "site"
    rust_doc_dir = tmp_path / "target" / "doc"
    assert sync_rustdoc_artifacts(rust_doc_dir=rust_doc_dir, site_dir=site_dir) is False

    (rust_doc_dir / "vertebra_core").mkdir(parents=True)
    (rust_doc_dir / "vertebra_core" / "index.html").write_text("<html>Rustdoc</html>", encoding="utf-8")

    assert sync_rustdoc_artifacts(rust_doc_dir=rust_doc_dir, site_dir=site_dir) is True
    assert (site_dir / "rustdoc" / "vertebra_core" / "index.html").is_file()


def test_build_postbuild_artifacts_and_cli_main(tmp_path: Path) -> None:
    """Verify build_postbuild_artifacts and CLI entrypoint generate all expected outputs."""
    config_path, docs_dir, site_dir = _write_sample_project(tmp_path)
    assets_dir = docs_dir / "assets"
    assets_dir.mkdir(parents=True, exist_ok=True)
    (assets_dir / "favicon.ico").write_bytes(b"\x00\x00\x01\x00")
    (assets_dir / "favicon.svg").write_text("<svg></svg>\n", encoding="utf-8")

    redirects_count = build_postbuild_artifacts(config_path=config_path, docs_dir=docs_dir, site_dir=site_dir)
    assert redirects_count >= 1
    assert (site_dir / "llms.txt").is_file()
    assert (site_dir / "llms-full.txt").is_file()
    assert (site_dir / "favicon.ico").is_file()
    assert (site_dir / "favicon.svg").is_file()
    assert (site_dir / "backpressure.html").is_file()

    rc = postbuild_main(["--config", str(config_path), "--docs-dir", str(docs_dir), "--site-dir", str(site_dir)])
    assert rc == 0

    rc_zensical_flag = postbuild_main(
        ["--site-dir", str(site_dir), "--docs-dir", str(docs_dir), "--zensical-config", str(config_path)]
    )
    assert rc_zensical_flag == 0


def test_verify_site_artifacts_accepts_clean_site_and_exemptions(tmp_path: Path) -> None:
    """Verify verify_site_artifacts passes clean output and honors changelog/rustdoc exemptions."""
    site_dir = tmp_path / "site"
    _write_clean_site(site_dir)

    rustdoc_dir = site_dir / "rustdoc"
    rustdoc_dir.mkdir(parents=True)
    (rustdoc_dir / "index.html").write_text(
        "<!doctype html><html><body>" + ("x" * 200_000) + "</body></html>", encoding="utf-8"
    )

    changelog_dir = site_dir / "changelog"
    changelog_dir.mkdir(parents=True)
    many_toc = "".join(f'<li><a class="md-nav__link" href="#v{i}">v{i}</a></li>' for i in range(25))
    many_h2 = "".join(f'<h2 id="v{i}">v{i}</h2>' for i in range(25))
    (changelog_dir / "index.html").write_text(
        f'<!doctype html><html><body><nav class="md-nav md-nav--secondary"><ul>{many_toc}</ul></nav>'
        f"{many_h2}" + ("y" * 200_000) + "</body></html>",
        encoding="utf-8",
    )

    assert verify_site_artifacts(site_dir) == []
    assert verify_main(["--site-dir", str(site_dir)]) == 0


def test_verify_site_artifacts_rejects_missing_inventories(tmp_path: Path) -> None:
    """Verify verify_site_artifacts fails when objects.inv is missing."""
    site_dir = tmp_path / "site"
    _write_clean_site(site_dir)
    (site_dir / "objects.inv").unlink()

    with pytest.raises(DocsVerificationError, match=r"objects\.inv"):
        verify_site_artifacts(site_dir)
    assert verify_main(["--site-dir", str(site_dir)]) == 1


@pytest.mark.parametrize(
    "leaked_phrase",
    [
        "Represent a PEP 604 union type",
        "avoid mypyc segmentation faults",
        "prevent ABI boundary violations",
        "NameError: name 'tmp_path' is not defined",
        'db_path = tmp_path / "demo.db"',
        '--8<-- "docs/examples/quickstart_app.py:example"',
        '--8 < --"docs/examples/quickstart_app.py:example"',
    ],
)
def test_verify_site_artifacts_rejects_leaked_docstrings_and_tmp_path(tmp_path: Path, leaked_phrase: str) -> None:
    """Verify verify_site_artifacts fails on leaked internal phrases or unexpanded snippets."""
    site_dir = tmp_path / "site"
    _write_clean_site(site_dir)
    leak_page = site_dir / "reference" / "leak.html"
    leak_page.write_text(f"<!doctype html><html><body><p>{leaked_phrase}</p></body></html>", encoding="utf-8")

    with pytest.raises(DocsVerificationError) as exc_info:
        verify_site_artifacts(site_dir)
    assert any("leak.html" in item for item in exc_info.value.violations)


def test_verify_site_artifacts_rejects_oversized_pages(tmp_path: Path) -> None:
    """Verify verify_site_artifacts enforces the reference HTML byte budget."""
    site_dir = tmp_path / "site"
    _write_clean_site(site_dir)
    bloated_ref = site_dir / "reference" / "bloated.html"
    bloated_ref.write_text("<!doctype html><html><body>" + ("a" * 125_000) + "</body></html>", encoding="utf-8")

    with pytest.raises(DocsVerificationError, match=r"bloated\.html"):
        verify_site_artifacts(site_dir)


def test_verify_site_artifacts_rejects_bloated_secondary_toc(tmp_path: Path) -> None:
    """Verify verify_site_artifacts enforces the secondary On-this-page TOC link limit."""
    site_dir = tmp_path / "site"
    _write_clean_site(site_dir)
    toc_links = "".join(f'<li><a class="md-nav__link" href="#m-{idx}">m_{idx}</a></li>' for idx in range(15))
    headings = "".join(f'<h3 id="m-{idx}">m_{idx}</h3>' for idx in range(15))
    bloated_toc = site_dir / "reference" / "methods.html"
    bloated_toc.write_text(
        f'<!doctype html><html><body><nav class="md-nav md-nav--secondary"><ul>{toc_links}</ul></nav>{headings}</body></html>',
        encoding="utf-8",
    )

    with pytest.raises(DocsVerificationError, match=r"methods\.html"):
        verify_site_artifacts(site_dir)


def test_verify_site_artifacts_rejects_broken_same_page_anchors(tmp_path: Path) -> None:
    """Verify verify_site_artifacts flags same-page anchor links whose target ID is missing."""
    site_dir = tmp_path / "site"
    _write_clean_site(site_dir)
    broken_page = site_dir / "usage" / "broken.html"
    broken_page.parent.mkdir(parents=True, exist_ok=True)
    broken_page.write_text(
        '<!doctype html><html><body><h1 id="present">Present</h1><a href="#missing-id">Broken</a></body></html>',
        encoding="utf-8",
    )

    with pytest.raises(DocsVerificationError, match="#missing-id"):
        verify_site_artifacts(site_dir)


def test_compact_site_html_preserves_pre_and_textarea_indentation(tmp_path: Path) -> None:
    """Verify compact_site_html preserves indentation inside pre and textarea elements."""
    site_dir = tmp_path / "site"
    site_dir.mkdir(parents=True)
    sample_html = site_dir / "sample.html"
    sample_html.write_text(
        "<!doctype html>\n"
        "<html>\n"
        "  <body>\n"
        "    <div>\n"
        "      <p>Outer text</p>\n"
        "    </div>\n"
        "    <pre><code>def foo():\n    return 42\n</code></pre>\n"
        "    <textarea data-python-editor>with app.test_client() as c:\n    c.get('/')\n</textarea>\n"
        "  </body>\n"
        "</html>\n",
        encoding="utf-8",
    )

    compacted_count = compact_site_html(site_dir)
    assert compacted_count == 1

    compacted_text = sample_html.read_text(encoding="utf-8")
    assert "<pre><code>def foo():\n    return 42\n</code></pre>" in compacted_text
    assert "<textarea data-python-editor>with app.test_client() as c:\n    c.get('/')\n</textarea>" in compacted_text
    assert "  <div>" not in compacted_text
    assert "<div>\n<p>Outer text</p>\n</div>" in compacted_text


def test_verify_site_artifacts_rejects_made_with_zensical(tmp_path: Path) -> None:
    """Verify built artifacts fail verification when Made with Zensical footer leak is present."""
    site_dir = tmp_path / "site"
    _write_clean_site(site_dir)
    leaked_page = site_dir / "leak.html"
    leaked_page.write_text(
        "<!doctype html><html><body><footer>Made with Zensical</footer></body></html>", encoding="utf-8"
    )

    with pytest.raises(DocsVerificationError, match="Made with Zensical"):
        verify_site_artifacts(site_dir)


def test_verify_site_artifacts_rejects_outer_badge_border(tmp_path: Path) -> None:
    """Verify built artifacts fail verification when logo or favicon contains an outer badge border."""
    site_dir = tmp_path / "site"
    _write_clean_site(site_dir)
    assets_dir = site_dir / "assets"
    assets_dir.mkdir(parents=True, exist_ok=True)
    (assets_dir / "logo.svg").write_text(
        '<svg><rect x="8" y="8" width="240" height="240" rx="52"/></svg>', encoding="utf-8"
    )

    with pytest.raises(DocsVerificationError, match="outer badge border rectangle"):
        verify_site_artifacts(site_dir)


def test_verify_typescript_exports_detects_missing_subpath_and_symbol(tmp_path: Path) -> None:
    """Verify verify_typescript_exports checks package.json subpaths and index.ts exports."""
    pkg_json = tmp_path / "package.json"
    pkg_json.write_text(
        '{"name": "@vertebra/client", "exports": {".": "./dist/index.js", "./react": "./dist/react/index.js"}}',
        encoding="utf-8",
    )
    index_ts = tmp_path / "index.ts"
    index_ts.write_text(
        'export { HttpClient, type ClientConfig } from "./client";\nexport { LiveStateClient } from "./live";\n',
        encoding="utf-8",
    )
    ts_doc = tmp_path / "typescript-client.md"
    ts_doc.write_text(
        "# TypeScript Client\n\nImports: `@vertebra/client` and `@vertebra/client/react`.\n"
        "Classes: `HttpClient`, `ClientConfig`, `LiveStateClient`.\n",
        encoding="utf-8",
    )

    assert verify_typescript_exports(ts_doc_path=ts_doc, package_json_path=pkg_json, index_ts_path=index_ts) == []

    ts_doc.write_text("# TypeScript Client\n\nImports: `@vertebra/client`.\nClasses: `HttpClient`.\n", encoding="utf-8")
    violations = verify_typescript_exports(ts_doc_path=ts_doc, package_json_path=pkg_json, index_ts_path=index_ts)
    assert any("@vertebra/client/react" in v for v in violations)
    assert any("LiveStateClient" in v for v in violations)


def _write_sample_project(root: Path) -> tuple[Path, Path, Path]:
    """Create a synthetic Zensical workspace for post-build testing."""
    docs_dir = root / "docs"
    site_dir = root / "site"
    examples_dir = docs_dir / "examples"
    includes_dir = root / "tools" / "docs" / "includes"
    (docs_dir / "getting-started").mkdir(parents=True)
    examples_dir.mkdir(parents=True)
    includes_dir.mkdir(parents=True)
    site_dir.mkdir(parents=True)

    (examples_dir / "demo.py").write_text(
        '"""Standalone demo."""\n\n\n'
        "# --8<-- [start:example]\n"
        "def main() -> int:\n"
        '    """Return answer."""\n'
        "    return 42\n"
        "# --8<-- [end:example]\n\n\n"
        'if __name__ == "__main__":\n'
        "    main()\n",
        encoding="utf-8",
    )
    (includes_dir / "widget.html").write_text(
        '<div id="vertebra-widget"><script>console.log(1);</script></div>\n', encoding="utf-8"
    )
    (docs_dir / "index.md").write_text("# Vertebra\n\nPolyglot application backbone.\n", encoding="utf-8")
    (docs_dir / "getting-started" / "index.md").write_text(
        "# Getting Started\n\nStart building with Vertebra.\n", encoding="utf-8"
    )
    (docs_dir / "getting-started" / "quickstart.md").write_text(
        '# Quickstart\n\nRun your first route:\n\n=== "App"\n\n    ```python\n    --8<-- "docs/examples/demo.py:example"\n    ```\n',
        encoding="utf-8",
    )
    (docs_dir / "widget.md").write_text(
        "---\n# frontmatter internal comment\nhide:\n  - toc\n---\n\n"
        '# Widget Demo\n\nInteractive demo.\n\n--8<-- "tools/docs/includes/widget.html"\n',
        encoding="utf-8",
    )

    config_path = root / "zensical.toml"
    config_path.write_text(
        """
[project]
site_name = "Vertebra"
site_description = "Polyglot application backbone for Python, Rust, and TypeScript."
site_url = "https://vertebra.litestar.dev/"
docs_dir = "docs"
site_dir = "site"
nav = [
  { "Home" = "index.md" },
  { "Getting Started" = [
    "getting-started/index.md",
    { "Quickstart" = "getting-started/quickstart.md" },
  ] },
  { "Widget" = "widget.md" },
]

[project.plugins.redirects.redirect_maps]
"backpressure.md" = "architecture/http-ingress.md"
"parallelism.md" = "architecture/pyo3-and-workers.md#worker-lanes"
"index_legacy.md" = "index.md"
""".strip()
        + "\n",
        encoding="utf-8",
    )
    return config_path, docs_dir, site_dir


def _write_clean_site(site_dir: Path) -> None:
    """Populate a valid built Zensical site directory."""
    site_dir.mkdir(parents=True, exist_ok=True)
    (site_dir / "objects.inv").write_bytes(b"Sphinx inventory version 2\n")
    (site_dir / "llms.txt").write_text("# Vertebra\n", encoding="utf-8")
    (site_dir / "llms-full.txt").write_text("# Vertebra Full\n", encoding="utf-8")

    toc_links = "".join(f'<li><a class="md-nav__link" href="#sec-{idx}">Section {idx}</a></li>' for idx in range(6))
    headings = "".join(f'<h2 id="sec-{idx}">Section {idx}</h2>' for idx in range(6))
    (site_dir / "index.html").write_text(
        f"<!doctype html><html><body>"
        f'<nav class="md-nav md-nav--secondary"><ul>{toc_links}</ul></nav>'
        f'<main><h1 id="top">Vertebra</h1>{headings}<a href="#sec-2">Jump</a><a href="#">Top</a></main>'
        f"</body></html>",
        encoding="utf-8",
    )

    ref_dir = site_dir / "reference" / "server"
    ref_dir.mkdir(parents=True)
    (ref_dir / "index.html").write_text(
        "<!doctype html><html><body>"
        '<nav class="md-nav md-nav--secondary"><ul>'
        '<li><a class="md-nav__link" href="#worker">Worker</a></li>'
        "</ul></nav>"
        '<h1 id="server">Server</h1>'
        '<h2 id="worker">Worker</h2>'
        '<h4 id="worker-run">run</h4>'
        '<a href="#worker-run">run</a>'
        "</body></html>",
        encoding="utf-8",
    )
