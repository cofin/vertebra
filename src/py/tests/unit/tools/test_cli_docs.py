"""Behavioral unit tests for the Vertebra Click CLI reference Markdown generator."""

import importlib
import importlib.util
import sys
from pathlib import Path

import pytest
from tools.docs.generate_cli_docs import (
    ensure_native_stub_module,
    render_cli_reference,
)
from tools.docs.generate_cli_docs import (
    main as cli_docs_main,
)

__all__ = (
    "test_cli_docs_main_writes_and_checks_staleness",
    "test_ensure_native_stub_module_supports_mandatory_utils_imports_without_compiled_extension",
    "test_render_cli_reference_includes_command_heading",
    "test_render_cli_reference_renders_all_vtb_commands_arguments_and_options_cleanly",
)


def test_render_cli_reference_renders_all_vtb_commands_arguments_and_options_cleanly() -> None:
    """Verify render_cli_reference emits vtb arguments, options, and headings without Sentinel or Rich leaks."""
    markdown = render_cli_reference()

    assert markdown.startswith("# CLI Reference\n")
    assert "## Global Usage" in markdown

    assert "| `app` |" in markdown
    assert "| `packages` |" in markdown
    assert "| `--host` |" in markdown
    assert "| `--port` |" in markdown
    assert "| `--workers` |" in markdown

    assert "Sentinel.UNSET" not in markdown
    assert "--help" not in markdown
    assert "[dim]" not in markdown
    assert "[/]" not in markdown

    h2_headings = [line for line in markdown.splitlines() if line.startswith("## ")]
    h3_headings = [line for line in markdown.splitlines() if line.startswith("### ")]
    assert 2 <= len(h2_headings) <= 6
    assert len(h2_headings) + len(h3_headings) <= 12


@pytest.mark.parametrize(
    "heading",
    [
        "#### `vtb run`",
        "#### `vtb dev`",
        "#### `vtb init`",
        "#### `vtb build`",
        "#### `vtb js add`",
        "#### `vtb js remove`",
        "#### `vtb js list`",
        "#### `vtb js install`",
        "#### `vtb js build`",
        "#### `vtb js generate-types`",
        "#### `vtb js exec`",
        "#### `vtb ai models list`",
        "#### `vtb ai models pull`",
        "#### `vtb ai models stage`",
        "#### `vtb ai models warm`",
        "#### `vtb ai mcp bridge`",
        "#### `vtb ai mcp serve`",
        "#### `vtb workers run`",
    ],
    ids=[
        "vtb-run",
        "vtb-dev",
        "vtb-init",
        "vtb-build",
        "vtb-js-add",
        "vtb-js-remove",
        "vtb-js-list",
        "vtb-js-install",
        "vtb-js-build",
        "vtb-js-generate-types",
        "vtb-js-exec",
        "vtb-ai-models-list",
        "vtb-ai-models-pull",
        "vtb-ai-models-stage",
        "vtb-ai-models-warm",
        "vtb-ai-mcp-bridge",
        "vtb-ai-mcp-serve",
        "vtb-workers-run",
    ],
)
def test_render_cli_reference_includes_command_heading(heading: str) -> None:
    """Verify render_cli_reference emits each expected vtb leaf command heading."""
    markdown = render_cli_reference()
    assert heading in markdown


def test_cli_docs_main_writes_and_checks_staleness(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """Verify generate_cli_docs main writes output and detects missing or stale files under --check."""
    output_file = tmp_path / "cli.md"

    missing_rc = cli_docs_main(["--check", "--output", str(output_file)])
    assert missing_rc == 1
    assert "missing" in capsys.readouterr().err.lower()

    write_rc = cli_docs_main(["--output", str(output_file)])
    assert write_rc == 0
    assert output_file.is_file()

    clean_rc = cli_docs_main(["--check", "--output", str(output_file)])
    assert clean_rc == 0

    output_file.write_text("# stale\n", encoding="utf-8")
    stale_rc = cli_docs_main(["--check", "--output", str(output_file)])
    assert stale_rc == 1
    assert "out of date" in capsys.readouterr().err.lower()


@pytest.mark.parametrize(
    "mod_name",
    ["vertebra.utils.text", "vertebra.utils.humanize", "vertebra.utils.uuids"],
    ids=["text", "humanize", "uuids"],
)
def test_ensure_native_stub_module_supports_mandatory_utils_imports_without_compiled_extension(
    monkeypatch: pytest.MonkeyPatch,
    mod_name: str,
) -> None:
    """Verify ensure_native_stub_module satisfies vertebra.utils.{text,humanize,uuids} and hides dunder attributes."""
    orig_find_spec = importlib.util.find_spec

    def _fake_find_spec(name: str, package: str | None = None) -> object:
        if name in ("vertebra._vertebra", "vertebra._bundler"):
            return None
        return orig_find_spec(name, package)

    monkeypatch.setattr(importlib.util, "find_spec", _fake_find_spec)
    monkeypatch.delitem(sys.modules, "vertebra._vertebra", raising=False)
    monkeypatch.delitem(sys.modules, "vertebra._bundler", raising=False)
    monkeypatch.delitem(sys.modules, "vertebra.utils.text", raising=False)
    monkeypatch.delitem(sys.modules, "vertebra.utils.humanize", raising=False)
    monkeypatch.delitem(sys.modules, "vertebra.utils.uuids", raising=False)

    ensure_native_stub_module()
    stub_mod = sys.modules["vertebra._vertebra"]
    assert not hasattr(stub_mod, "__path__")
    assert getattr(stub_mod, "AsyncObjectStore", None) is None

    mod = importlib.import_module(mod_name)
    assert mod is not None

    markdown = render_cli_reference()
    assert "#### `vtb run`" in markdown
