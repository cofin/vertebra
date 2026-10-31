"""Behavioral unit tests for the Vertebra Click CLI reference Markdown generator."""

from pathlib import Path

import pytest
from tools.docs.generate_cli_docs import main as cli_docs_main
from tools.docs.generate_cli_docs import render_cli_reference

__all__ = (
    "test_cli_docs_main_writes_and_checks_staleness",
    "test_render_cli_reference_renders_all_vtb_commands_arguments_and_options_cleanly",
)


def test_render_cli_reference_renders_all_vtb_commands_arguments_and_options_cleanly() -> None:
    """Verify render_cli_reference emits all 14 vtb leaf commands without Sentinel or Rich leaks."""
    markdown = render_cli_reference()

    assert markdown.startswith("# CLI Reference\n")
    assert "## Global Usage" in markdown

    expected_commands = (
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
        "#### `vtb mcp bridge`",
        "#### `vtb mcp serve`",
        "#### `vtb workers run`",
    )
    for heading in expected_commands:
        assert heading in markdown

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
