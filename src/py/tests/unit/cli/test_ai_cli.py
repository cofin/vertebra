"""Unit tests for the consolidated vtb ai (models + mcp) CLI command group."""

from __future__ import annotations

import hashlib
import sys
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest
from click.testing import CliRunner

from vertebra.agent.mcp.models import MCPConfig
from vertebra.agent.mcp.service import MCPCore
from vertebra.agent.plugin import AgentPlugin, AgentPluginConfig
from vertebra.cli import cli
from vertebra.cli.ai import (
    ModelBundleManifest,
    ai,
    extract_mcp_headers_from_jsonrpc,
    mcp,
    models,
    run_mcp_stdio_bridge,
)
from vertebra.framework._app import App
from vertebra.utils.serializers import decode_json


@pytest.fixture
def runner() -> CliRunner:
    """Return a Click CLI test runner."""
    return CliRunner()


def test_vtb_ai_help_and_mcp_removed_from_root(runner: CliRunner) -> None:
    """Verify vtb ai, vtb ai models, and vtb ai mcp succeed and top-level vtb mcp is removed."""
    root_help = runner.invoke(cli, ["--help"])
    assert root_help.exit_code == 0, root_help.output
    assert "ai" in root_help.output
    assert "mcp" not in cli.commands

    ai_help = runner.invoke(cli, ["ai", "--help"])
    assert ai_help.exit_code == 0, ai_help.output
    assert "models" in ai_help.output
    assert "mcp" in ai_help.output


@pytest.mark.parametrize(
    ("args", "subcmd"),
    [
        (["ai", "models", "--help"], "list"),
        (["ai", "models", "--help"], "pull"),
        (["ai", "models", "--help"], "stage"),
        (["ai", "models", "--help"], "warm"),
        (["ai", "mcp", "--help"], "serve"),
        (["ai", "mcp", "--help"], "bridge"),
    ],
    ids=["models-list", "models-pull", "models-stage", "models-warm", "mcp-serve", "mcp-bridge"],
)
def test_vtb_ai_subcommand_help_lists_expected_command(runner: CliRunner, args: list[str], subcmd: str) -> None:
    """Verify vtb ai models and vtb ai mcp --help outputs list each expected subcommand."""
    help_result = runner.invoke(cli, args)
    assert help_result.exit_code == 0, help_result.output
    assert subcmd in help_result.output


def test_vtb_ai_models_list_json_and_provider_filter(runner: CliRunner, tmp_path: Path) -> None:
    """Verify vtb ai models list --json outputs the built-in ModelSpec catalog and provider filter."""
    result = runner.invoke(cli, ["ai", "models", "list", "--json", "--cache-dir", str(tmp_path)])
    assert result.exit_code == 0, result.output
    entries = decode_json(result.output.encode("utf-8"))
    assert isinstance(entries, list)

    gemma_only = runner.invoke(
        cli,
        ["ai", "models", "list", "--provider", "gemma", "--json", "--cache-dir", str(tmp_path)],
    )
    assert gemma_only.exit_code == 0, gemma_only.output
    gemma_entries = decode_json(gemma_only.output.encode("utf-8"))
    assert {item["name"] for item in gemma_entries} == {"embeddinggemma-2-270m", "embeddinggemma-300m"}


@pytest.mark.parametrize(
    "expected_model",
    [
        "gemini-embedding-001",
        "gemini-embedding-2",
        "text-embedding-005",
        "embeddinggemma-2-270m",
        "embeddinggemma-300m",
        "BAAI/bge-small-en-v1.5",
        "nomic-ai/nomic-embed-text-v1.5",
        "minishlab/potion-base-8M",
    ],
    ids=[
        "gemini-embedding-001",
        "gemini-embedding-2",
        "text-embedding-005",
        "embeddinggemma-2-270m",
        "embeddinggemma-300m",
        "bge-small-en-v1.5",
        "nomic-embed-text-v1.5",
        "potion-base-8M",
    ],
)
def test_vtb_ai_models_list_includes_builtin_model(runner: CliRunner, tmp_path: Path, expected_model: str) -> None:
    """Verify vtb ai models list --json includes each built-in ModelSpec entry."""
    result = runner.invoke(cli, ["ai", "models", "list", "--json", "--cache-dir", str(tmp_path)])
    assert result.exit_code == 0, result.output
    entries = decode_json(result.output.encode("utf-8"))
    assert isinstance(entries, list)
    names = {item["name"] for item in entries}
    assert expected_model in names


def test_vtb_ai_models_pull_stage_and_warm_shm_roundtrip(
    runner: CliRunner,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify vtb ai models pull, stage, and warm --shm verify SHA-256 digests and populate /dev/shm."""
    mirror_dir = tmp_path / "mirror"
    (mirror_dir / "onnx").mkdir(parents=True)
    tok_bytes = b'{"version":"1.0","model":{"vocab":{"hello":1}}}'
    onnx_bytes = b"ONNX-QUANTIZED-WEIGHTS-PAYLOAD-270M"
    (mirror_dir / "tokenizer.json").write_bytes(tok_bytes)
    (mirror_dir / "onnx" / "model_quantized.onnx").write_bytes(onnx_bytes)

    cache_dir = tmp_path / "cache"
    staging_dir = tmp_path / "staging"
    shm_dir = tmp_path / "shm"
    shm_dir.mkdir(parents=True)
    monkeypatch.setenv("VERTEBRA_SHM_DIR", str(shm_dir))

    pull_res = runner.invoke(
        cli,
        [
            "ai",
            "models",
            "pull",
            "embeddinggemma-2-270m",
            "--quant",
            "int8",
            "--cache-dir",
            str(cache_dir),
            "--source",
            mirror_dir.as_uri(),
        ],
    )
    assert pull_res.exit_code == 0, pull_res.output
    local_manifest_path = cache_dir / "embeddinggemma-2-270m" / "manifest.json"
    assert local_manifest_path.is_file()
    manifest = decode_json(local_manifest_path.read_bytes(), type=ModelBundleManifest)
    assert manifest.model == "embeddinggemma-2-270m"
    assert manifest.quantization == "int8"
    assert len(manifest.files) == 2
    by_path = {entry.path: entry for entry in manifest.files}
    assert by_path["tokenizer.json"].sha256 == hashlib.sha256(tok_bytes).hexdigest()
    assert by_path["onnx/model_quantized.onnx"].sha256 == hashlib.sha256(onnx_bytes).hexdigest()

    stage_res = runner.invoke(
        cli,
        [
            "ai",
            "models",
            "stage",
            "embeddinggemma-2-270m",
            "--to",
            staging_dir.as_uri(),
            "--quant",
            "int8",
            "--cache-dir",
            str(cache_dir),
        ],
    )
    assert stage_res.exit_code == 0, stage_res.output
    assert (staging_dir / "manifest.json").is_file()
    assert (staging_dir / "onnx" / "model_quantized.onnx").read_bytes() == onnx_bytes

    warm_cache_dir = tmp_path / "warm_cache"
    warm_res = runner.invoke(
        cli,
        [
            "ai",
            "models",
            "warm",
            "embeddinggemma-2-270m",
            "--from",
            staging_dir.as_uri(),
            "--shm",
            "--cache-dir",
            str(warm_cache_dir),
        ],
    )
    assert warm_res.exit_code == 0, warm_res.output
    shm_model_dir = shm_dir / "vtb-model-embeddinggemma-2-270m"
    assert (shm_model_dir / "manifest.json").is_file()
    assert (shm_model_dir / "onnx" / "model_quantized.onnx").read_bytes() == onnx_bytes

    list_res = runner.invoke(
        cli,
        [
            "ai",
            "models",
            "list",
            "--provider",
            "gemma",
            "--json",
            "--cache-dir",
            str(warm_cache_dir),
        ],
    )
    assert list_res.exit_code == 0, list_res.output
    listed = {item["name"]: item for item in decode_json(list_res.output.encode("utf-8"))}
    assert listed["embeddinggemma-2-270m"]["cached"] is True
    assert listed["embeddinggemma-2-270m"]["shm_warmed"] is True


def test_vtb_ai_models_warm_rejects_corrupted_sha256(
    runner: CliRunner,
    tmp_path: Path,
) -> None:
    """Verify vtb ai models warm fails when a staged artifact violates its manifest SHA-256 digest."""
    staging_dir = tmp_path / "corrupt_staging"
    staging_dir.mkdir(parents=True)
    bad_manifest = (
        '{"model":"minishlab/potion-base-8M","provider":"static","quantization":"fp32","dimensions":256,'
        '"files":[{"path":"tokenizer.json","size":5,"sha256":"0000000000000000000000000000000000000000000000000000000000000000"}]}'
    )
    (staging_dir / "manifest.json").write_text(bad_manifest, encoding="utf-8")
    (staging_dir / "tokenizer.json").write_bytes(b"hello")

    res = runner.invoke(
        cli,
        [
            "ai",
            "models",
            "warm",
            "minishlab/potion-base-8M",
            "--from",
            staging_dir.as_uri(),
            "--cache-dir",
            str(tmp_path / "cache"),
        ],
    )
    assert res.exit_code != 0
    assert "sha-256" in res.output.lower() or "digest" in res.output.lower()


def test_vtb_ai_mcp_serve_and_bridge_commands(
    runner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify vtb ai mcp serve and vtb ai mcp bridge invoke the MCP entrypoints."""
    assert ai.commands["models"] is models
    assert ai.commands["mcp"] is mcp

    headers = extract_mcp_headers_from_jsonrpc(
        b'{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"search"}}'
    )
    assert headers["Mcp-Method"] == "tools/call"
    assert headers["Mcp-Name"] == "search"
    assert callable(run_mcp_stdio_bridge)

    bridge_calls: dict[str, Any] = {}

    def fake_bridge(*, url: str, bearer_token: str | None, timeout_s: float = 30.0) -> int:
        bridge_calls["url"] = url
        bridge_calls["bearer_token"] = bearer_token
        bridge_calls["timeout_s"] = timeout_s
        return 0

    monkeypatch.setattr("vertebra.cli.ai.run_mcp_stdio_bridge", fake_bridge)
    res_bridge = runner.invoke(
        cli,
        [
            "ai",
            "mcp",
            "bridge",
            "--url",
            "http://127.0.0.1:8000/mcp",
            "--token",
            "tok-123",
            "--timeout",
            "12",
        ],
    )
    assert res_bridge.exit_code == 0, res_bridge.output
    assert bridge_calls == {
        "url": "http://127.0.0.1:8000/mcp",
        "bearer_token": "tok-123",
        "timeout_s": 12.0,
    }

    app_core = MCPCore(config=MCPConfig(server_name="test-mcp"))
    plugin = AgentPlugin(AgentPluginConfig(mcp=MCPConfig(server_name="test-mcp")))
    plugin.mcp_service = app_core
    serve_mod = ModuleType("vertebra_test_ai_serve_app")
    serve_mod.__dict__["app"] = App(plugins=[plugin])
    monkeypatch.setitem(sys.modules, serve_mod.__name__, serve_mod)

    serve_called: list[MCPCore] = []

    async def fake_stdio_server(svc: MCPCore) -> int:
        serve_called.append(svc)
        return 0

    monkeypatch.setattr("vertebra.cli.ai.run_stdio_server", fake_stdio_server)
    res_serve = runner.invoke(cli, ["ai", "mcp", "serve", "--app", f"{serve_mod.__name__}:app"])
    assert res_serve.exit_code == 0, res_serve.output
    assert serve_called == [app_core]
