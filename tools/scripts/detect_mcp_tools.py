#!/usr/bin/env python3
"""Auto-detect available MCP tools for Gemini agent system."""

import json
import os
import subprocess
from datetime import UTC, datetime
from pathlib import Path

KNOWN_TOOLS = {
    "crash": {"crash", "mcp__crash__crash"},
    "sequential_thinking": {
        "sequentialthinking",
        "sequential-thinking",
        "mcp__sequential-thinking__sequentialthinking",
    },
    "context7": {"context7", "mcp__context7__query-docs", "mcp__context7__resolve-library-id"},
    "pal_thinkdeep": {"pal_thinkdeep", "mcp__pal__thinkdeep"},
    "pal_planner": {"pal_planner", "mcp__pal__planner"},
    "pal_consensus": {"pal_consensus", "mcp__pal__consensus"},
    "pal_analyze": {"pal_analyze", "mcp__pal__analyze"},
    "pal_debug": {"pal_debug", "mcp__pal__debug"},
    "web_search": {"web_search", "web.search_query", "search_query", "web.run", "web"},
    "web_fetch": {"web_fetch", "web.open", "web.openurl", "web.run"},
}


def _normalize_token(token: str) -> str:
    return "".join(ch.lower() for ch in token if ch.isalnum() or ch in {"_", "-"}).strip()


def _flatten(values: object) -> set[str]:
    normalized = set()
    if not isinstance(values, (list, tuple, set)):
        values = [values]
    for value in values:
        for part in str(value).replace(",", " ").split():
            token = _normalize_token(part)
            if token:
                normalized.add(token)
    return normalized


def _tokens_from_payload(payload: object) -> set[str]:
    if isinstance(payload, dict):
        if "tools" in payload:
            return _tokens_from_payload(payload["tools"])
        if "available_tools" in payload:
            return _tokens_from_payload(payload["available_tools"])
        if "mcp_tools" in payload:
            return _tokens_from_payload(payload["mcp_tools"])
        return _flatten([key for key, value in payload.items() if value])
    if isinstance(payload, list):
        return _flatten([str(item) for item in payload])
    if isinstance(payload, str):
        return _flatten([payload])
    return set()


def _detect_from_env() -> tuple[set[str], str | None]:
    for key in ("MCP_TOOLS_JSON", "GEMINI_MCP_TOOLS_JSON", "CODEX_MCP_TOOLS_JSON"):
        raw = os.getenv(key)
        if raw:
            try:
                return _tokens_from_payload(json.loads(raw)), f"env:{key}"
            except json.JSONDecodeError:
                return _flatten([raw]), f"env:{key}"
    for key in ("MCP_TOOLS", "GEMINI_MCP_TOOLS", "CODEX_MCP_TOOLS"):
        raw = os.getenv(key)
        if raw:
            return _flatten([raw]), f"env:{key}"
    return set(), None


def _run_command(command: list[str]) -> tuple[str, str] | None:
    try:
        result = subprocess.run(command, check=False, capture_output=True, text=True, timeout=3)
    except (FileNotFoundError, subprocess.SubprocessError):
        return None
    if result.returncode != 0:
        return None
    output = result.stdout.strip()
    return output, " ".join(command)


def _detect_from_cli() -> tuple[set[str], str | None]:
    candidates = [
        ["gemini", "mcp", "list", "--json"],
        ["gemini", "tools", "list", "--json"],
        ["gemini", "mcp", "list"],
        ["gemini", "tools", "list"],
    ]
    for command in candidates:
        result = _run_command(command)
        if not result:
            continue
        output, label = result
        if not output:
            continue
        try:
            return _tokens_from_payload(json.loads(output)), f"cli:{label}"
        except json.JSONDecodeError:
            return _flatten(output.splitlines()), f"cli:{label}"
    return set(), None


def _resolve_known_tools(tokens: set[str]) -> dict[str, bool]:
    resolved = {}
    for tool, aliases in KNOWN_TOOLS.items():
        normalized_aliases = {_normalize_token(alias) for alias in aliases}
        resolved[tool] = any(alias and any(alias in token for token in tokens) for alias in normalized_aliases)
    return resolved


def detect_mcp_tools() -> tuple[dict[str, bool], str | None]:
    tokens, source = _detect_from_env()
    if not tokens:
        tokens, source = _detect_from_cli()
    if not tokens:
        source = "unknown"
    return _resolve_known_tools(tokens), source


def main() -> None:
    tools, source = detect_mcp_tools()

    output_path = Path(".gemini/mcp-tools.txt")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w") as f:
        f.write("# Available MCP Tools\n\n")
        f.write(f"Detected: {datetime.now(UTC).isoformat()}\n")
        f.write(f"Source: {source}\n\n")
        for tool, available in sorted(tools.items()):
            status = "✓" if available else "✗"
            f.write(f"{status} {tool.replace('_', ' ').title()}\n")

        f.write("\n# Usage Examples\n\n")
        f.write("## mcp__pal__thinkdeep (for deep analysis)\n")
        f.write("```python\n")
        f.write(
            "mcp__pal__thinkdeep(\
"
        )
        f.write('    step="Analyzing...",\n')
        f.write("    step_number=1,\n")
        f.write("    total_steps=5,\n")
        f.write("    next_step_required=True,\n")
        f.write('    findings="...",\n')
        f.write('    model="gemini-3-pro-preview"  # MANDATORY\n')
        f.write(")\n```\n")


if __name__ == "__main__":
    main()
