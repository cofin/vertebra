"""HTML quality, inventory, TOC budget, anchor integrity, and TypeScript export sync verifier for built Zensical docs."""

import argparse
import re
import sys
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import unquote

from vertebra.utils.serializers import decode_json

__all__ = (
    "BANNED_PHRASES",
    "DocsVerificationError",
    "main",
    "verify_site_artifacts",
    "verify_typescript_exports",
)

BANNED_PHRASES: tuple[str, ...] = (
    "Represent a PEP 604 union type",
    "mypyc segmentation",
    "ABI boundary violations",
    "NameError: name 'tmp_path'",
    "tmp_path /",
    "Made with Zensical",
    "--8<--",
    "--8 < --",
    "litestar-org/vertebra",
    "vertebra-io/vertebra",
    "vertebra.litestar.dev",
)

_EMPTY_ATTRIBUTE_ROW_PATTERN = re.compile(
    r"<td>\s*<code>[^<]+</code>\s*</td>\s*<td>\s*</td>\s*<td>\s*<div class=\"doc-md-description\">\s*</div>\s*</td>",
    re.DOTALL,
)

_REQUIRED_ROOT_FILES: tuple[str, ...] = ("objects.inv", "llms.txt", "llms-full.txt")
_TS_NAMED_EXPORT_BLOCK_PATTERN = re.compile(r"export\s+(?:type\s+)?\{([^}]+)\}", re.DOTALL)
_TS_DIRECT_EXPORT_PATTERN = re.compile(
    r"^\s*export\s+(?:async\s+)?(?:class|function|const|type|interface|enum)\s+([A-Za-z_][A-Za-z0-9_]*)",
    re.MULTILINE,
)


class DocsVerificationError(RuntimeError):
    """Raised when built documentation artifacts violate structural or hygiene gates."""

    def __init__(self, violations: list[str]) -> None:
        self.violations = violations
        super().__init__("\n".join(violations))


def verify_site_artifacts(
    site_dir: Path,
    *,
    max_page_bytes: int = 180_000,
    max_reference_bytes: int = 120_000,
    max_toc_items: int = 14,
    ts_doc_path: Path | None = None,
    package_json_path: Path | None = None,
    index_ts_path: Path | None = None,
) -> list[str]:
    """Verify built Zensical site artifacts and raise DocsVerificationError on violations."""
    violations: list[str] = []

    if not site_dir.is_dir():
        raise DocsVerificationError([f"Missing site directory: {site_dir}"])

    for required_name in _REQUIRED_ROOT_FILES:
        artifact = site_dir / required_name
        if not artifact.is_file() or artifact.stat().st_size == 0:
            violations.append(f"Missing or empty required artifact: {artifact}")

    html_files = sorted(site_dir.rglob("*.html"))
    if not html_files:
        violations.append(f"No built HTML files found under {site_dir}")

    for html_file in html_files:
        rel_posix = html_file.relative_to(site_dir).as_posix()
        if rel_posix.startswith("rustdoc/"):
            continue
        raw_bytes = html_file.read_bytes()
        byte_len = len(raw_bytes)
        text = raw_bytes.decode("utf-8", errors="replace")

        if not _is_size_exempt(rel_posix):
            limit = max_reference_bytes if rel_posix.startswith("reference/") else max_page_bytes
            if byte_len > limit:
                violations.append(f"{rel_posix}: HTML size {byte_len} bytes exceeds limit of {limit} bytes")

        violations.extend(
            f"{rel_posix}: contains banned documentation leak phrase {phrase!r}"
            for phrase in BANNED_PHRASES
            if phrase in text
        )

        if rel_posix.startswith("reference/") and _EMPTY_ATTRIBUTE_ROW_PATTERN.search(text):
            violations.append(f"{rel_posix}: contains Attributes table row with both Type and Description cells empty")

        parser = _PageArtifactParser()
        parser.feed(text)
        parser.close()

        if not _is_toc_exempt(rel_posix) and parser.max_secondary_toc_links > max_toc_items:
            violations.append(
                f"{rel_posix}: secondary 'On this page' TOC has {parser.max_secondary_toc_links} items "
                f"(limit: {max_toc_items})"
            )

        violations.extend(
            f"{rel_posix}: broken same-page anchor href='#{fragment}'"
            for fragment in parser.same_page_fragments
            if fragment not in parser.element_ids and unquote(fragment) not in parser.element_ids
        )

        if rel_posix == "index.html" and "vertebra-hero" in text and "vertebra-brand-text" not in text:
            violations.append(f"{rel_posix}: missing glowing brand text element 'vertebra-brand-text'")

    for brand_file in (site_dir / "assets" / "logo.svg", site_dir / "assets" / "favicon.svg"):
        if brand_file.is_file():
            content = brand_file.read_text(encoding="utf-8", errors="replace")
            if 'x="8" y="8" width="240" height="240"' in content or 'class="badge-bg"' in content:
                violations.append(f"{brand_file.name}: contains outer badge border rectangle")

    resolved_ts_doc = ts_doc_path if ts_doc_path is not None else Path("docs/reference/typescript-client.md")
    resolved_pkg_json = (
        package_json_path if package_json_path is not None else Path("src/ts/vertebra-client/package.json")
    )
    resolved_index_ts = index_ts_path if index_ts_path is not None else Path("src/ts/vertebra-client/src/index.ts")
    if resolved_ts_doc.is_file() and resolved_pkg_json.is_file() and resolved_index_ts.is_file():
        violations.extend(
            verify_typescript_exports(
                ts_doc_path=resolved_ts_doc,
                package_json_path=resolved_pkg_json,
                index_ts_path=resolved_index_ts,
            )
        )

    if violations:
        raise DocsVerificationError(violations)
    return violations


def verify_typescript_exports(
    *,
    ts_doc_path: Path,
    package_json_path: Path,
    index_ts_path: Path,
) -> list[str]:
    """Verify that docs/reference/typescript-client.md documents every @vertebra/client subpath and root export."""
    violations: list[str] = []
    doc_text = ts_doc_path.read_text(encoding="utf-8")

    pkg_data = decode_json(package_json_path.read_text(encoding="utf-8"))
    pkg_name = str(pkg_data.get("name", "@vertebra/client"))
    exports_map = pkg_data.get("exports", {})
    if isinstance(exports_map, dict):
        for raw_key in sorted(exports_map.keys()):
            subpath = str(raw_key)
            import_spec = pkg_name if subpath == "." else f"{pkg_name}/{subpath.removeprefix('./')}"
            if import_spec not in doc_text:
                violations.append(f"{ts_doc_path}: missing @vertebra/client subpath export {import_spec!r}")

    for symbol in _extract_typescript_exports(index_ts_path):
        if symbol not in doc_text:
            violations.append(f"{ts_doc_path}: missing @vertebra/client root export symbol {symbol!r}")

    return violations


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint for built documentation verification."""
    parser = argparse.ArgumentParser(description="Verify built Zensical site artifacts.")
    parser.add_argument("--site-dir", type=Path, default=Path("site"), help="Built site directory.")
    parser.add_argument("--max-page-bytes", type=int, default=180_000, help="Max HTML byte budget for guide pages.")
    parser.add_argument(
        "--max-reference-bytes", type=int, default=120_000, help="Max HTML byte budget for API reference pages."
    )
    parser.add_argument(
        "--max-toc-items", type=int, default=14, help="Max links allowed in the secondary On-this-page TOC."
    )
    args = parser.parse_args(argv)

    try:
        verify_site_artifacts(
            site_dir=args.site_dir,
            max_page_bytes=args.max_page_bytes,
            max_reference_bytes=args.max_reference_bytes,
            max_toc_items=args.max_toc_items,
        )
    except DocsVerificationError as exc:
        sys.stderr.write("Documentation artifact verification failed:\n")
        for item in exc.violations:
            sys.stderr.write(f"  - {item}\n")
        return 1

    sys.stdout.write(f"Verified documentation artifacts in {args.site_dir}.\n")
    return 0


def _extract_typescript_exports(index_ts_path: Path) -> list[str]:
    """Extract exported symbol names from src/ts/vertebra-client/src/index.ts and star-reexported siblings."""
    source = index_ts_path.read_text(encoding="utf-8")
    symbols: set[str] = set()

    for block_match in _TS_NAMED_EXPORT_BLOCK_PATTERN.finditer(source):
        for raw_item in block_match.group(1).split(","):
            token = raw_item.strip()
            if not token:
                continue
            if token.startswith("type "):
                token = token.removeprefix("type ").strip()
            if " as " in token:
                token = token.split(" as ", 1)[1].strip()
            if token:
                symbols.add(token)

    for direct_match in _TS_DIRECT_EXPORT_PATTERN.finditer(source):
        symbols.add(direct_match.group(1))

    for star_match in re.finditer(r'export\s+\*\s+from\s+"([^"]+)"', source):
        rel_target = star_match.group(1)
        candidate = (index_ts_path.parent / f"{rel_target}.ts").resolve()
        if candidate.is_file():
            for direct_match in _TS_DIRECT_EXPORT_PATTERN.finditer(candidate.read_text(encoding="utf-8")):
                symbols.add(direct_match.group(1))

    return sorted(symbols)


def _is_size_exempt(rel_posix: str) -> bool:
    """Return whether rel_posix is exempt from standard HTML byte budgets."""
    return rel_posix in {"changelog.html", "404.html"} or rel_posix.startswith(("changelog/", "rustdoc/"))


def _is_toc_exempt(rel_posix: str) -> bool:
    """Return whether rel_posix is exempt from the secondary TOC item cap."""
    return rel_posix == "changelog.html" or rel_posix.startswith(("changelog/", "rustdoc/"))


class _PageArtifactParser(HTMLParser):
    """Single-pass HTML parser extracting element IDs, #fragment links, and secondary TOC counts."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.element_ids: set[str] = set()
        self.same_page_fragments: list[str] = []
        self._secondary_nav_stack: list[bool] = []
        self._current_secondary_links: int = 0
        self._secondary_toc_counts: list[int] = []

    @property
    def max_secondary_toc_links(self) -> int:
        """Return the link count of the largest secondary On-this-page TOC in the page."""
        return max(self._secondary_toc_counts, default=0)

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attr_map = dict(attrs)
        raw_id = attr_map.get("id")
        if raw_id:
            self.element_ids.add(raw_id)

        if tag == "a":
            raw_name = attr_map.get("name")
            if raw_name:
                self.element_ids.add(raw_name)

        if tag == "nav":
            classes = set((attr_map.get("class") or "").split())
            in_secondary = any(self._secondary_nav_stack)
            if "md-nav--secondary" in classes and not in_secondary:
                self._current_secondary_links = 0
                self._secondary_nav_stack.append(True)
            else:
                self._secondary_nav_stack.append(in_secondary)

        if tag == "a":
            href = attr_map.get("href") or ""
            if href.startswith("#") and len(href) > 1:
                fragment = href[1:]
                if fragment != "__top":
                    self.same_page_fragments.append(fragment)
                if any(self._secondary_nav_stack):
                    self._current_secondary_links += 1

    def handle_endtag(self, tag: str) -> None:
        if tag == "nav" and self._secondary_nav_stack:
            was_secondary = self._secondary_nav_stack.pop()
            if was_secondary and not any(self._secondary_nav_stack):
                self._secondary_toc_counts.append(self._current_secondary_links)
                self._current_secondary_links = 0


if __name__ == "__main__":
    raise SystemExit(main())
