"""Unit tests for the application-agnostic OKF v0.2 bundle serializer."""

from __future__ import annotations

import hashlib
import importlib
import io
import math
import random
import stat
import subprocess
import sys
import zipfile
from pathlib import Path
from typing import Any

import pytest

import vertebra.agent as agent_pkg
from vertebra.agent.okf import (
    MAX_OKF_ENTRY_BYTES,
    OkfBundle,
    OkfDocument,
    OkfSource,
    OkfValidationError,
)


def _make_content_doc(
    *,
    status: str = "stable",
    sources: list[dict[str, str]] | None = None,
    truth_lines: list[str] | None = None,
    body_extra: str = "",
    footnotes: dict[str, str] | None = None,
) -> str:
    """Build a minimal valid OKF v0.2 content document for tests."""
    src_list = sources if sources is not None else [{"id": "s1", "resource": "src/app.py", "title": "App module"}]
    fn_map = footnotes if footnotes is not None else {"s1": "`src/app.py` — App module."}
    truth = truth_lines if truth_lines is not None else ["- Primary fact [^s1]."]

    fm_lines = ["---", "type: Guide", "title: Test Document", "description: Test summary.", f"status: {status}"]
    if src_list:
        fm_lines.append("sources:")
        for item in src_list:
            fm_lines.append(f"  - id: {item['id']}")
            fm_lines.append(f"    resource: {item['resource']}")
            if "title" in item:
                fm_lines.append(f"    title: {item['title']}")
    fm_lines.append("---")
    fm_lines.append("")
    fm_lines.append("# Test Document")
    fm_lines.append("")
    fm_lines.append("<!-- truth: start -->")
    fm_lines.extend(truth)
    fm_lines.append("<!-- truth: end -->")
    if body_extra:
        fm_lines.append("")
        fm_lines.append(body_extra)
    if fn_map:
        fm_lines.append("")
        for fn_id, fn_desc in fn_map.items():
            fm_lines.append(f"[^{fn_id}]: {fn_desc}")
    return "\n".join(fm_lines) + "\n"


def test_canonical_frontmatter_round_trip_preserves_types_and_hex_strings() -> None:
    """Pure-Python YAML frontmatter round-trips scalars, nested maps, lists, colons, and hex strings."""
    raw = """---
type: Guide
title: "Vertebra: OKF v0.2"
description: "Handles #116 colons, quotes, and hex SHAs"
status: stable
count: 42
negative: -7
ratio: 0.75
enabled: true
disabled: false
optional: null
okf_version: "0.2"
hex_leading_zero: "05d88d00"
hex_all_digits: "12345678"
hex_scientific: "1234e567"
tags:
  - okf
  - "ns:sub"
  - "true"
nested:
  meta:
    endpoint: "https://dataplex.googleapis.com:443/v1"
    retries: 3
verified:
  - by: "agent:executor"
    at: "2026-11-19T00:00:00Z"
    commit: "05d88d00"
sources:
  - id: src-core
    resource: "src/py/vertebra/agent/okf.py::OkfBundle"
    title: "Module: OKF serializer"
---

# Heading

<!-- truth: start -->
- Verified behavior [^src-core].
<!-- truth: end -->

[^src-core]: `src/py/vertebra/agent/okf.py::OkfBundle` — OKF serializer.
"""
    doc = OkfDocument.parse("knowledge/guide.md", raw)
    assert doc.frontmatter["title"] == "Vertebra: OKF v0.2"
    assert doc.frontmatter["count"] == 42
    assert doc.frontmatter["negative"] == -7
    assert doc.frontmatter["ratio"] == 0.75
    assert doc.frontmatter["enabled"] is True
    assert doc.frontmatter["disabled"] is False
    assert doc.frontmatter["optional"] is None
    assert doc.frontmatter["okf_version"] == "0.2"
    assert doc.frontmatter["hex_leading_zero"] == "05d88d00"
    assert doc.frontmatter["hex_all_digits"] == "12345678"
    assert doc.frontmatter["hex_scientific"] == "1234e567"
    assert doc.frontmatter["tags"] == ["okf", "ns:sub", "true"]
    assert doc.frontmatter["nested"] == {"meta": {"endpoint": "https://dataplex.googleapis.com:443/v1", "retries": 3}}
    assert doc.frontmatter["verified"] == [{"by": "agent:executor", "at": "2026-11-19T00:00:00Z", "commit": "05d88d00"}]
    assert doc.sources == (
        OkfSource(
            id="src-core",
            resource="src/py/vertebra/agent/okf.py::OkfBundle",
            title="Module: OKF serializer",
        ),
    )

    emitted = doc.to_markdown()
    assert 'hex_leading_zero: "05d88d00"' in emitted
    assert 'hex_all_digits: "12345678"' in emitted
    assert 'hex_scientific: "1234e567"' in emitted
    assert 'title: "Vertebra: OKF v0.2"' in emitted

    reparsed = OkfDocument.parse("knowledge/guide.md", emitted)
    assert reparsed == doc
    assert len(doc.sha256) == 64
    assert doc.sha256 == hashlib.sha256(emitted.encode("utf-8")).hexdigest()


def test_crlf_normalization_produces_canonical_lf_sha256() -> None:
    """Line endings are normalized to LF before computing the 64-char SHA-256 digest."""
    lf_doc = _make_content_doc()
    crlf_doc = lf_doc.replace("\n", "\r\n")

    parsed_lf = OkfDocument.parse("product/overview.md", lf_doc)
    parsed_crlf = OkfDocument.parse("product/overview.md", crlf_doc)

    assert parsed_lf == parsed_crlf
    assert "\r" not in parsed_crlf.to_markdown()
    assert parsed_crlf.sha256 == parsed_lf.sha256


@pytest.mark.parametrize(
    "bad_frontmatter",
    [
        "---\ntitle: Unterminated\n\n# Body\n",
        "---\n\ttitle: Tab indented\n---\n# Body\n",
        "---\n- root_list_item\n---\n# Body\n",
        '---\ntitle: "unclosed quote\n---\n# Body\n',
        "---\ntitle: dup\ntitle: again\n---\n# Body\n",
        "---\nnot_a_mapping_line\n---\n# Body\n",
    ],
)
def test_malformed_frontmatter_raises_validation_error(bad_frontmatter: str) -> None:
    """Malformed YAML frontmatter variants fail with OkfValidationError."""
    with pytest.raises(OkfValidationError):
        OkfDocument.parse("notes.md", bad_frontmatter)


@pytest.mark.parametrize("fence", ["```", "~~~", "````"])
def test_fenced_code_blocks_ignore_rules_truth_and_footnotes(fence: str) -> None:
    """Code fences protect --- delimiters, truth markers, and [^id] markers from scanning."""
    body_extra = (
        f"{fence}markdown\n"
        "---\n"
        "<!-- truth: start -->\n"
        "- Fake truth inside fence [^fake-ref]\n"
        "<!-- truth: end -->\n"
        "[^fake-def]: Should be ignored.\n"
        f"{fence}\n"
        "\nInline code `[^inline-ignored]` should also be ignored."
    )
    content = _make_content_doc(body_extra=body_extra)
    doc = OkfDocument.parse("knowledge/fenced.md", content)

    assert doc.truth_lines == ("- Primary fact [^s1].",)
    assert doc.footnotes == {"s1": "`src/app.py` — App module."}


def test_standalone_horizontal_rule_and_unclosed_truth_rejected() -> None:
    """Standalone --- outside fences and unclosed truth blocks fail validation."""
    with_rule = _make_content_doc(body_extra="Section 1\n\n---\n\nSection 2")
    with pytest.raises(OkfValidationError, match="horizontal rule"):
        OkfDocument.parse("knowledge/bad-rule.md", with_rule)

    unclosed_truth = "---\ntype: Guide\nstatus: stable\n---\n\n<!-- truth: start -->\n- Open\n"
    with pytest.raises(OkfValidationError, match="Unterminated"):
        OkfDocument.parse("knowledge/unclosed.md", unclosed_truth)


def test_reserved_files_validation_rules() -> None:
    """Root index.md accepts okf_version '0.2', while subdir index.md and log.md parse without frontmatter."""
    valid_root = '---\nokf_version: "0.2"\nbundle: demo\n---\n\n# Root Index\n'
    root_doc = OkfDocument.parse("index.md", valid_root)
    assert root_doc.frontmatter["okf_version"] == "0.2"

    subdir_idx = OkfDocument.parse("knowledge/index.md", "# Knowledge Index\n")
    assert subdir_idx.frontmatter == {}

    log_doc = OkfDocument.parse("log.md", "# Log\n\n- Entry 1\n")
    assert log_doc.frontmatter == {}


@pytest.mark.parametrize(
    "bad_root",
    [
        "---\nokf_version: 0.2\n---\n\n# Float version\n",
        "---\nbundle: demo\n---\n\n# Missing okf_version\n",
        "# No frontmatter\n",
    ],
)
def test_invalid_root_index_rejected(bad_root: str) -> None:
    """Root index.md without string okf_version '0.2' fails validation."""
    with pytest.raises(OkfValidationError):
        OkfDocument.parse("index.md", bad_root)


@pytest.mark.parametrize("reserved_path", ["knowledge/index.md", "product/index.md", "log.md"])
def test_reserved_paths_reject_frontmatter(reserved_path: str) -> None:
    """Subdirectory index.md and log.md files forbid YAML frontmatter."""
    with pytest.raises(OkfValidationError, match="must not carry YAML frontmatter"):
        OkfDocument.parse(reserved_path, "---\ntitle: Illegal\n---\n\n# Reserved\n")


@pytest.mark.parametrize(
    ("rel_path", "content"),
    [
        ("knowledge/bad-status.md", _make_content_doc(status="archived")),
        (
            "knowledge/over-budget.md",
            _make_content_doc(truth_lines=[f"- Line {i} [^s1]." for i in range(41)]),
        ),
        (
            "product/missing-footnote.md",
            _make_content_doc(
                sources=[
                    {"id": "s1", "resource": "a.py"},
                    {"id": "s2", "resource": "b.py"},
                ],
                footnotes={"s1": "a.py"},
            ),
        ),
        (
            "knowledge/undeclared-footnote.md",
            _make_content_doc(
                sources=[{"id": "s1", "resource": "a.py"}],
                footnotes={"s1": "a.py", "s2": "b.py"},
            ),
        ),
        (
            "knowledge/dangling-ref.md",
            _make_content_doc(body_extra="References [^missing] inline."),
        ),
    ],
)
def test_content_document_validation_rejects_invalid_status_budget_and_provenance(
    rel_path: str,
    content: str,
) -> None:
    """Content documents under product/ and knowledge/ enforce status, truth budget, and provenance parity."""
    with pytest.raises(OkfValidationError):
        OkfDocument.parse(rel_path, content)


def test_directory_import_excludes_specs_and_log_by_default(tmp_path: Path) -> None:
    """OkfBundle.from_directory skips specs/ and log.md by default unless opted in."""
    (tmp_path / "index.md").write_text('---\nokf_version: "0.2"\n---\n\n# Bundle\n', encoding="utf-8")
    (tmp_path / "log.md").write_text("# Log\n\n- Initialized\n", encoding="utf-8")
    (tmp_path / "specs" / "active").mkdir(parents=True)
    (tmp_path / "specs" / "active" / "spec.md").write_text("---\ntype: Spec\n---\n\n# Spec\n", encoding="utf-8")
    (tmp_path / "knowledge").mkdir()
    (tmp_path / "knowledge" / "index.md").write_text("# Knowledge\n", encoding="utf-8")
    (tmp_path / "knowledge" / "guide.md").write_text(_make_content_doc(), encoding="utf-8")

    default_bundle = OkfBundle.from_directory(tmp_path)
    assert [doc.rel_path for doc in default_bundle] == [
        "index.md",
        "knowledge/guide.md",
        "knowledge/index.md",
    ]

    full_bundle = OkfBundle.from_directory(tmp_path, exclude_specs=False, exclude_log=False)
    assert [doc.rel_path for doc in full_bundle] == [
        "index.md",
        "knowledge/guide.md",
        "knowledge/index.md",
        "log.md",
        "specs/active/spec.md",
    ]


def test_entry_size_ceiling_enforced_during_streaming_reads(tmp_path: Path) -> None:
    """Entries up to 120,832 bytes succeed while 120,833-byte entries fail during chunked streaming."""
    prefix = '---\nokf_version: "0.2"\n---\n\n# Index\n'
    exact_payload = prefix + ("a" * (MAX_OKF_ENTRY_BYTES - len(prefix.encode("utf-8"))))
    assert len(exact_payload.encode("utf-8")) == MAX_OKF_ENTRY_BYTES

    (tmp_path / "index.md").write_bytes(exact_payload.encode("utf-8"))
    bundle = OkfBundle.from_directory(tmp_path)
    assert len(bundle) == 1

    oversized_bytes = exact_payload.encode("utf-8") + b"x"
    assert len(oversized_bytes) == MAX_OKF_ENTRY_BYTES + 1
    (tmp_path / "index.md").write_bytes(oversized_bytes)
    with pytest.raises(OkfValidationError, match="120832"):
        OkfBundle.from_directory(tmp_path)

    zip_buf = io.BytesIO()
    with zipfile.ZipFile(zip_buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("index.md", oversized_bytes)
    with pytest.raises(OkfValidationError, match="120832"):
        OkfBundle.from_zip(zip_buf.getvalue())

    large_payload = prefix.encode("utf-8") + (b"z" * 1_000_000)
    bomb_buf = io.BytesIO()
    with zipfile.ZipFile(bomb_buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        info = zipfile.ZipInfo("index.md")
        info.file_size = 100
        zf.writestr(info, large_payload)
    with pytest.raises(OkfValidationError, match="120832"):
        OkfBundle.from_zip(bomb_buf.getvalue())


def test_directory_and_zip_reject_symlinks_and_path_traversal(tmp_path: Path) -> None:
    """Symlinks in directories or ZIP archives and path traversal entries are rejected."""
    bundle_dir = tmp_path / "bundle"
    bundle_dir.mkdir()
    (bundle_dir / "index.md").write_text('---\nokf_version: "0.2"\n---\n\n# Root\n', encoding="utf-8")
    outside_file = tmp_path / "secret.md"
    outside_file.write_text("# Secret\n", encoding="utf-8")
    (bundle_dir / "leak.md").symlink_to(outside_file)

    with pytest.raises(OkfValidationError, match="Symlink"):
        OkfBundle.from_directory(bundle_dir)

    symlink_zip = io.BytesIO()
    with zipfile.ZipFile(symlink_zip, mode="w") as zf:
        info = zipfile.ZipInfo("leak.md")
        info.external_attr = (stat.S_IFLNK | 0o777) << 16
        zf.writestr(info, "../secret.md")
    with pytest.raises(OkfValidationError, match="symlink"):
        OkfBundle.from_zip(symlink_zip.getvalue())


@pytest.mark.parametrize(
    "unsafe_path",
    [
        "../escape.md",
        "knowledge/../../escape.md",
        "/etc/passwd",
        "C:/escape.md",
        "D:\\escape.md",
        "knowledge\\escape.md",
        "./index.md",
        "knowledge//doc.md",
    ],
)
def test_zip_slip_and_unsafe_paths_rejected(unsafe_path: str) -> None:
    """Absolute paths, Windows drives, backslashes, and .. segments raise OkfValidationError."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, mode="w") as zf:
        zf.writestr(unsafe_path, "# Unsafe\n")
    with pytest.raises(OkfValidationError):
        OkfBundle.from_zip(buf.getvalue())

    with pytest.raises(OkfValidationError):
        OkfDocument.parse(unsafe_path, "# Unsafe\n")


def test_reproducible_zip_export_and_directory_round_trip(tmp_path: Path) -> None:
    """ZIP archives are deterministic, sorted, permission-normalized, and round-trip cleanly."""
    src_dir = tmp_path / "src_bundle"
    (src_dir / "product").mkdir(parents=True)
    (src_dir / "knowledge").mkdir(parents=True)
    (src_dir / "index.md").write_text('---\nokf_version: "0.2"\nbundle: sample\n---\n\n# Sample\n', encoding="utf-8")
    (src_dir / "product" / "index.md").write_text("# Product Index\n", encoding="utf-8")
    (src_dir / "product" / "overview.md").write_text(_make_content_doc(), encoding="utf-8")
    (src_dir / "knowledge" / "note.md").write_text(_make_content_doc(), encoding="utf-8")

    bundle = OkfBundle.from_directory(src_dir)
    reversed_bundle = OkfBundle(documents=tuple(reversed(bundle.documents)))

    zip_first = bundle.to_zip_bytes()
    zip_second = reversed_bundle.to_zip_bytes()
    assert zip_first == zip_second
    assert hashlib.sha256(zip_first).hexdigest() == hashlib.sha256(zip_second).hexdigest()

    with zipfile.ZipFile(io.BytesIO(zip_first), mode="r") as zf:
        infos = zf.infolist()
        assert [info.filename for info in infos] == [
            "index.md",
            "knowledge/note.md",
            "product/index.md",
            "product/overview.md",
        ]
        for info in infos:
            assert info.date_time == (1980, 1, 1, 0, 0, 0)
            assert info.external_attr == 0o644 << 16
            assert info.compress_type == zipfile.ZIP_DEFLATED

    zip_file_path = bundle.export_zip(tmp_path / "out" / "bundle.zip")
    from_zip_bundle = OkfBundle.from_zip(zip_file_path)
    assert from_zip_bundle == bundle
    assert from_zip_bundle.digest() == bundle.digest()

    exported_dir = tmp_path / "exported_dir"
    bundle.export_directory(exported_dir)
    reimported_dir_bundle = OkfBundle.from_directory(exported_dir)
    assert reimported_dir_bundle == bundle


def test_repository_okf_bundle_documents_round_trip(tmp_path: Path) -> None:
    """Repository OKF v0.2 bundle files parse, export to ZIP, and re-import with identical digests."""
    repo_bundles = Path(__file__).resolve().parents[5] / ".agents" / "bundles"
    assert repo_bundles.is_dir()

    bundle = OkfBundle.from_directory(repo_bundles)
    assert len(bundle.documents) >= 75

    zip_bytes = bundle.to_zip_bytes()
    reloaded = OkfBundle.from_zip(zip_bytes)
    assert reloaded == bundle
    assert reloaded.digest() == bundle.digest()

    export_dir = tmp_path / "repo_roundtrip"
    bundle.export_directory(export_dir)
    assert OkfBundle.from_directory(export_dir) == bundle


@pytest.mark.parametrize(
    ("symbol_name", "source_module"),
    [
        ("AsyncKnowledgeCatalogClient", "vertebra.agent.knowledge_catalog"),
        ("CatalogEntryRecord", "vertebra.agent.knowledge_catalog"),
        ("CatalogSyncError", "vertebra.agent.knowledge_catalog"),
        ("CatalogSyncSummary", "vertebra.agent.knowledge_catalog"),
        ("KnowledgeCatalogConfig", "vertebra.agent.knowledge_catalog"),
        ("OkfBundle", "vertebra.agent.okf"),
        ("OkfDocument", "vertebra.agent.okf"),
        ("OkfSource", "vertebra.agent.okf"),
        ("OkfValidationError", "vertebra.agent.okf"),
        ("SyncKnowledgeCatalogClient", "vertebra.agent.knowledge_catalog"),
    ],
)
def test_vertebra_agent_exports_okf_and_knowledge_catalog_symbols(
    symbol_name: str,
    source_module: str,
) -> None:
    """vertebra.agent.__all__ stays sorted and resolves all OKF and Knowledge Catalog exports."""
    assert list(agent_pkg.__all__) == sorted(agent_pkg.__all__)
    assert symbol_name in agent_pkg.__all__

    canonical_mod = importlib.import_module(source_module)
    assert getattr(agent_pkg, symbol_name) is getattr(canonical_mod, symbol_name)


def test_vertebra_agent_lazy_imports_avoid_eager_okf_and_catalog_loads() -> None:
    """from vertebra.agent import ... resolves all nine exports lazily without eager submodule loads."""
    probe = (
        "import sys\n"
        "import vertebra.agent as agent_pkg\n"
        "assert 'vertebra.agent.okf' not in sys.modules\n"
        "assert 'vertebra.agent.knowledge_catalog' not in sys.modules\n"
        "from vertebra.agent import OkfBundle, OkfDocument, OkfSource, OkfValidationError\n"
        "assert 'vertebra.agent.okf' in sys.modules\n"
        "assert 'vertebra.agent.knowledge_catalog' not in sys.modules\n"
        "from vertebra.agent import (\n"
        "    AsyncKnowledgeCatalogClient,\n"
        "    CatalogEntryRecord,\n"
        "    CatalogSyncSummary,\n"
        "    KnowledgeCatalogConfig,\n"
        "    SyncKnowledgeCatalogClient,\n"
        ")\n"
        "assert 'vertebra.agent.knowledge_catalog' in sys.modules\n"
        "expected = {\n"
        "    'AsyncKnowledgeCatalogClient',\n"
        "    'CatalogEntryRecord',\n"
        "    'CatalogSyncSummary',\n"
        "    'KnowledgeCatalogConfig',\n"
        "    'OkfBundle',\n"
        "    'OkfDocument',\n"
        "    'OkfSource',\n"
        "    'OkfValidationError',\n"
        "    'SyncKnowledgeCatalogClient',\n"
        "}\n"
        "assert expected.issubset(set(agent_pkg.__all__))\n"
        "assert all(callable(obj) or isinstance(obj, type) for obj in (\n"
        "    OkfBundle, OkfDocument, OkfSource, OkfValidationError,\n"
        "    AsyncKnowledgeCatalogClient, CatalogEntryRecord, CatalogSyncSummary,\n"
        "    KnowledgeCatalogConfig, SyncKnowledgeCatalogClient,\n"
        "))\n"
    )
    completed = subprocess.run(
        [sys.executable, "-c", probe],
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert completed.returncode == 0


def _round_trip_frontmatter(frontmatter: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    """Emit `frontmatter` through `OkfDocument.to_markdown` and parse the emitted text back."""
    emitted = OkfDocument(rel_path="notes/scalars.md", frontmatter=frontmatter, body="\n# Scalars\n").to_markdown()
    return emitted, OkfDocument.parse("notes/scalars.md", emitted).frontmatter


def _parse_frontmatter(yaml_text: str) -> dict[str, Any]:
    """Parse a frontmatter YAML snippet through the public `OkfDocument.parse` seam."""
    return OkfDocument.parse("notes/blocks.md", f"---\n{yaml_text}---\n\n# Blocks\n").frontmatter


def _same_scalar(left: object, right: object) -> bool:
    """Compare scalars by type and value, distinguishing `-0.0` from `0.0`."""
    if type(left) is not type(right):
        return False
    if isinstance(left, float) and isinstance(right, float):
        return left == right and math.copysign(1.0, left) == math.copysign(1.0, right)
    return left == right


@pytest.mark.parametrize(
    "value",
    [1e20, 1e-07, 1.5e300, 5e-324, -0.0, 0.1, 1.0, 1e16, 123456789.125, -2.5e-10, 1.7976931348623157e308],
)
def test_finite_floats_round_trip_as_the_same_float(value: float) -> None:
    """Every finite float emitted into frontmatter parses back as the identical float."""
    emitted, parsed = _round_trip_frontmatter({"value": value})

    assert _same_scalar(parsed["value"], value), emitted
    yaml = pytest.importorskip("yaml")
    assert _same_scalar(yaml.safe_load(emitted.split("---\n")[1])["value"], value), emitted


@pytest.mark.parametrize(
    ("raw", "expected"),
    [("1e+20", 1e20), ("1e-07", 1e-07), ("1E5", 1e5), ("-3e2", -300.0), ("1.0e+20", 1e20), ("2e0", 2.0)],
)
def test_parser_accepts_exponent_only_floats(raw: str, expected: float) -> None:
    """Unquoted exponent-only scalars such as `1e+20` parse as floats."""
    assert _same_scalar(_parse_frontmatter(f"value: {raw}\n")["value"], expected)


@pytest.mark.parametrize(
    "text",
    [
        "1_000",
        "2024-01-01",
        "2024-1-1",
        "2024-01-01 10:00:00",
        "1.",
        "1.e3",
        "0o17",
        "0b101",
        "1:30",
        "190:20:30",
        "+1_000.5",
        "1e5",
        "=",
        "<<",
    ],
)
def test_strings_other_yaml_readers_would_retype_are_quoted(text: str) -> None:
    """Strings that standard YAML reads as numbers, dates, or merge keys are emitted double-quoted."""
    emitted, parsed = _round_trip_frontmatter({"value": text})

    assert f'value: "{text}"' in emitted
    assert _same_scalar(parsed["value"], text)
    yaml = pytest.importorskip("yaml")
    assert _same_scalar(yaml.safe_load(emitted.split("---\n")[1])["value"], text)


_TRICKY_STRINGS: tuple[str, ...] = (
    "1_000",
    "2024-01-01",
    "1.",
    "0o17",
    "1:30",
    "1e+20",
    "true",
    "No",
    "~",
    ".inf",
    "0x1F",
    "- item",
    "key: value",
    "a # b",
    " padded ",
    "",
    "plain words",
    "line\nbreak",
    'quote"d',
    "it's",
    "caf\u00e9",
    "[x]",
    "{y}",
    "|",
    ">",
    "1.2.3",
    "v0-2",
)


def _random_scalar(rng: random.Random) -> object:
    """Draw one frontmatter scalar across floats, integers, booleans, null, and tricky strings."""
    kind = rng.randrange(7)
    if kind == 0:
        return rng.uniform(-1.0, 1.0) * 10.0 ** rng.randint(-320, 300)
    if kind == 1:
        return float(rng.randint(-10, 10)) * 10.0 ** rng.randint(-30, 30)
    if kind == 2:
        return rng.randint(-(10**18), 10**18)
    if kind == 3:
        return rng.choice([True, False, None])
    if kind == 4:
        return rng.choice(_TRICKY_STRINGS)
    if kind == 5:
        return repr(rng.uniform(-1.0, 1.0) * 10.0 ** rng.randint(-30, 30))
    alphabet = "abz019_-.:+eE xo#'\"\\,~"
    return "".join(rng.choice(alphabet) for _ in range(rng.randint(0, 12)))


@pytest.mark.parametrize("seed", [11, 23, 37, 41, 59, 67, 73, 89])
def test_mixed_scalar_frontmatter_round_trip_is_type_exact_for_seeded_samples(seed: int) -> None:
    """Seeded mixed scalars survive emit -> parse with identical types and values, and emission is a fixed point."""
    rng = random.Random(seed)
    frontmatter: dict[str, Any] = {f"k{idx}": _random_scalar(rng) for idx in range(60)}
    frontmatter["nested"] = {"items": [_random_scalar(rng) for _ in range(20)]}

    emitted, parsed = _round_trip_frontmatter(frontmatter)

    assert list(parsed) == list(frontmatter)
    mismatches = [
        (key, frontmatter[key], parsed[key])
        for key in frontmatter
        if key != "nested" and not _same_scalar(parsed[key], frontmatter[key])
    ]
    assert mismatches == []
    nested_pairs = zip(parsed["nested"]["items"], frontmatter["nested"]["items"], strict=True)
    assert [pair for pair in nested_pairs if not _same_scalar(*pair)] == []
    assert _round_trip_frontmatter(parsed)[0] == emitted

    yaml = pytest.importorskip("yaml")
    foreign = yaml.safe_load(emitted.split("---\n")[1])
    foreign_mismatches = [
        (key, frontmatter[key], foreign[key])
        for key in frontmatter
        if key != "nested" and not _same_scalar(foreign[key], frontmatter[key])
    ]
    assert foreign_mismatches == []


_BLOCK_SCALAR_CASES: tuple[tuple[str, str, object], ...] = (
    ("folded-clip", "description: >\n  folded text\n  continues here\ntitle: T\n", "folded text continues here\n"),
    ("folded-strip", "description: >-\n  folded text\n  continues here\n", "folded text continues here"),
    ("folded-keep", "description: >+\n  folded text\n\n\ntitle: T\n", "folded text\n\n\n"),
    ("folded-paragraphs", "description: >\n  one\n  two\n\n  three\n", "one two\nthree\n"),
    ("folded-more-indented", "description: >\n  one\n    code\n  two\n", "one\n  code\ntwo\n"),
    ("literal-clip", "description: |\n  line one\n  line two\n\ntitle: T\n", "line one\nline two\n"),
    ("literal-strip", "description: |-\n  line one\n  line two\n", "line one\nline two"),
    ("literal-keep", "description: |+\n  line one\n\ntitle: T\n", "line one\n\n"),
    (
        "literal-syntax-chars",
        "description: |\n  # not a comment\n  key: value\n  - dash\n",
        "# not a comment\nkey: value\n- dash\n",
    ),
    ("literal-inner-blank", "description: |\n  a\n\n  b\n", "a\n\nb\n"),
    ("literal-indent-indicator", "description: |2\n    indented\n  flush\n", "  indented\nflush\n"),
    ("literal-indicator-chomp", "description: |2-\n    indented\n", "  indented"),
    ("header-comment", "description: > # folded\n  text\n", "text\n"),
    ("empty-block", "description: |\ntitle: T\n", ""),
)


@pytest.mark.parametrize(
    ("yaml_text", "expected"),
    [(case[1], case[2]) for case in _BLOCK_SCALAR_CASES],
    ids=[case[0] for case in _BLOCK_SCALAR_CASES],
)
def test_block_scalars_parse_with_chomping_and_folding(yaml_text: str, expected: str) -> None:
    """Literal and folded block scalars honor clip, strip, and keep chomping like a standard YAML reader."""
    parsed = _parse_frontmatter(yaml_text)

    assert parsed["description"] == expected
    if "title" in yaml_text:
        assert parsed["title"] == "T"
    yaml = pytest.importorskip("yaml")
    assert yaml.safe_load(yaml_text)["description"] == expected


@pytest.mark.parametrize(
    ("yaml_text", "expected"),
    [
        ("matrix:\n  - - a\n    - b\n  - - c\n", {"matrix": [["a", "b"], ["c"]]}),
        ("matrix:\n- - 1\n  - 2\n- - 3\n", {"matrix": [[1, 2], [3]]}),
        ("deep:\n  - - - x\n", {"deep": [[["x"]]]}),
        ("rows:\n  - - k: v\n      j: w\n    - z\n", {"rows": [[{"k": "v", "j": "w"}, "z"]]}),
        (
            "notes:\n  - |\n    first\n    second\n  - >-\n    folded\n    item\n",
            {"notes": ["first\nsecond\n", "folded item"]},
        ),
        (
            "items:\n  - name: a\n    text: >\n      folded\n      value\n    kind: b\n",
            {"items": [{"name": "a", "text": "folded value\n", "kind": "b"}]},
        ),
        ("outer:\n  inner: |\n    deep\n  other: 1\n", {"outer": {"inner": "deep\n", "other": 1}}),
    ],
    ids=[
        "nested-seq",
        "nested-seq-flush",
        "triple-nested",
        "nested-seq-mapping",
        "seq-blocks",
        "seq-map-block",
        "map-block",
    ],
)
def test_nested_inline_sequences_and_block_scalars_reach_a_fixed_point(
    yaml_text: str,
    expected: dict[str, Any],
) -> None:
    """Nested `- - x` sequences and block scalars parse like standard YAML, and parse -> emit -> parse is stable."""
    parsed = _parse_frontmatter(yaml_text)
    assert parsed == expected
    yaml = pytest.importorskip("yaml")
    assert yaml.safe_load(yaml_text) == expected

    first = OkfDocument.parse("notes/blocks.md", f"---\n{yaml_text}---\n\n# Blocks\n")
    second = OkfDocument.parse("notes/blocks.md", first.to_markdown())
    assert second.frontmatter == first.frontmatter
    assert second.to_markdown() == first.to_markdown()
    assert second.sha256 == first.sha256
    assert all("\n" not in line or line == "" for line in first.to_markdown().split("\n"))


@pytest.mark.parametrize(
    "bad_yaml",
    ["description: |\n    deep\n  shallow\n", "description: |0\n  text\n", "description: |\n\ttabbed\n"],
    ids=["dedent-inside-block", "zero-indent-indicator", "tab-indented-block"],
)
def test_malformed_block_scalars_raise_validation_error(bad_yaml: str) -> None:
    """Block scalars with inconsistent indentation or invalid indicators are rejected."""
    with pytest.raises(OkfValidationError):
        _parse_frontmatter(bad_yaml)


def _zip_with_members(members: list[tuple[str, bytes]]) -> bytes:
    """Build an in-memory ZIP archive from `(name, payload)` members."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, payload in members:
            archive.writestr(name, payload)
    return buffer.getvalue()


_INDEX_MD: bytes = b'---\nokf_version: "0.2"\n---\n\n# Index\n'


@pytest.mark.parametrize("asset_name", ["assets/diagram.png", "notes.txt", "knowledge/data.md.bak"])
def test_from_zip_skips_oversized_non_markdown_members(asset_name: str) -> None:
    """A non-Markdown member above the per-entry ceiling is skipped instead of rejecting the archive."""
    archive = _zip_with_members([("index.md", _INDEX_MD), (asset_name, b"\x00" * (MAX_OKF_ENTRY_BYTES + 1))])

    bundle = OkfBundle.from_zip(archive)

    assert [doc.rel_path for doc in bundle] == ["index.md"]


@pytest.mark.parametrize("unsafe_name", ["../escape.bin", "assets/../../escape.bin", "C:/escape.bin"])
def test_from_zip_still_rejects_unsafe_non_markdown_member_paths(unsafe_name: str) -> None:
    """Path-safety checks apply to skipped non-Markdown members too."""
    archive = _zip_with_members([("index.md", _INDEX_MD), (unsafe_name, b"x")])

    with pytest.raises(OkfValidationError):
        OkfBundle.from_zip(archive)


def test_from_zip_still_rejects_non_markdown_symlink_members() -> None:
    """Symlink checks apply to skipped non-Markdown members too."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, mode="w") as archive:
        archive.writestr("index.md", _INDEX_MD)
        link = zipfile.ZipInfo("assets/link.bin")
        link.create_system = 3
        link.external_attr = (stat.S_IFLNK | 0o777) << 16
        archive.writestr(link, "/etc/passwd")

    with pytest.raises(OkfValidationError, match="symlink"):
        OkfBundle.from_zip(buffer.getvalue())


@pytest.mark.parametrize("source_kind", ["directory", "zip"])
def test_oversized_reserved_log_error_names_file_and_both_sizes(tmp_path: Path, source_kind: str) -> None:
    """An oversized `log.md` keeps the per-entry ceiling and the error names the file, its size, and the limit."""
    oversized = b"# Bundle Activity Log\n" + b"x" * MAX_OKF_ENTRY_BYTES
    expected = rf"'log\.md'.*\b{len(oversized)} bytes.*\b{MAX_OKF_ENTRY_BYTES} bytes"

    with pytest.raises(OkfValidationError, match=expected):
        if source_kind == "directory":
            (tmp_path / "index.md").write_bytes(_INDEX_MD)
            (tmp_path / "log.md").write_bytes(oversized)
            OkfBundle.from_directory(tmp_path, exclude_log=False)
        else:
            OkfBundle.from_zip(_zip_with_members([("index.md", _INDEX_MD), ("log.md", oversized)]))
