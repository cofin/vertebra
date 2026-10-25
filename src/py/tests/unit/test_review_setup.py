"""Unit tests for review infrastructure setup validator, router, and ledger checker."""

import stat
from pathlib import Path

import pytest
from tools.scripts.review_setup import (
    LedgerValidator,
    ReviewManifest,
    ReviewRouter,
    ReviewSetupValidator,
)


def build_minimal_review_tree(tmp_path: Path) -> Path:
    """Build a minimal valid review infrastructure tree for isolated testing."""
    repo = tmp_path / "repo"
    repo.mkdir()

    areas_toml = (
        "schema_version = 1\n\n"
        "[agents]\n"
        'model = "pro"\n'
        'common_preload = ["review-core", "flow-memory-keeper"]\n'
        'valid_tools = ["read_file", "edit_file", "write_file", "code_search", "run_command", "search_web", "moma_search", "skill_search", "send_message", "view_file", "grep_search", "find_by_name", "list_dir"]\n'
        'write_tools = ["edit_file", "write_file"]\n'
        'reviewer_required_tools = ["read_file", "view_file", "grep_search", "find_by_name", "list_dir", "code_search", "run_command", "search_web", "moma_search", "skill_search", "send_message"]\n'
        'specialist_extra_tools = ["edit_file", "write_file"]\n'
        "reviewer_forbidden_commands = ['\\bmake\\s+lint\\b']\n\n"
        "[policy]\n"
        "max_review_skill_lines = 200\n"
        "max_truth_lines = 40\n"
        'okf_reserved = ["index.md", "log.md"]\n'
        'privacy_scan = ["AGENTS.md", ".agents/**/*.md"]\n'
        "forbidden_patterns = ['/(?:home|Users)/[^/\\\\s`]+/']\n"
        'forbidden_terms_env = "VERTEBRA_REVIEW_FORBIDDEN_TERMS"\n'
        'forbidden_terms_file = ".agents/scratch/forbidden-terms.txt"\n'
        'prohibition_line_markers = ["never use", "forbidden"]\n\n'
        "[areas.runtime-ipc]\n"
        'prefix = "RT"\n'
        'title = "Rust runtime and IPC"\n'
        'paths = ["src/rs/**"]\n'
        'knowledge = [".agents/bundles/knowledge/patterns/rust/sample.md"]\n'
        'focused_commands = ["cargo test"]\n'
        "[[areas.runtime-ipc.shards]]\n"
        'name = "rings"\n'
        'paths = ["src/rs/channel.rs"]\n'
    )
    areas_path = repo / ".agents/skills/review-core/references/areas.toml"
    areas_path.parent.mkdir(parents=True, exist_ok=True)
    areas_path.write_text(areas_toml, encoding="utf-8")

    chk_text = (
        "## RT: Rust runtime and IPC\n"
        "- **RT-01**: Safety check. Verify: grep.\n"
        "- **RT-02**: Bounds check. Verify: grep.\n"
    )
    chk_path = repo / ".agents/skills/review-core/references/checklist.md"
    chk_path.write_text(chk_text, encoding="utf-8")

    fp_text = (
        "### FP-RT-01: SeqCst wait\n"
        "- **Applies to**: RT-01\n"
        "- **Scope**: src/rs/channel.rs\n"
        "- **Do not flag**: SeqCst ordering.\n"
        "- **Why**: Primary source at [.agents/bundles/knowledge/patterns/rust/sample.md](../../../bundles/knowledge/patterns/rust/sample.md).\n"
    )
    fp_path = repo / ".agents/skills/review-core/references/false-positive-suppressions.md"
    fp_path.write_text(fp_text, encoding="utf-8")

    core_skill = (
        "---\n"
        "name: review-core\n"
        "description: Core review skill.\n"
        "---\n\n"
        "<!-- truth: start -->\n"
        "- Truth line\n"
        "<!-- truth: end -->\n\n"
        "# Core Skill\n"
    )
    (repo / ".agents/skills/review-core/SKILL.md").write_text(core_skill, encoding="utf-8")

    flow_skill = "---\nname: flow-memory-keeper\ndescription: Memory keeper.\n---\n\n# Flow Memory Keeper\n"
    (repo / ".agents/skills/flow-memory-keeper").mkdir(parents=True, exist_ok=True)
    (repo / ".agents/skills/flow-memory-keeper/SKILL.md").write_text(flow_skill, encoding="utf-8")

    knowledge_file = repo / ".agents/bundles/knowledge/patterns/rust/sample.md"
    knowledge_file.parent.mkdir(parents=True, exist_ok=True)
    knowledge_file.write_text(
        "---\n"
        "type: Pattern\n"
        "title: Sample Pattern\n"
        "description: Pattern description.\n"
        "status: stable\n"
        "tags: [rust, shm]\n"
        "---\n\n"
        "# Sample Pattern\n",
        encoding="utf-8",
    )

    bundles_index = repo / ".agents/bundles/index.md"
    bundles_index.write_text(
        "---\n"
        'okf_version: "0.2"\n'
        "type: Index\n"
        "title: OKF Root Index\n"
        "description: Root index.\n"
        "---\n\n"
        "# OKF Root Index\n",
        encoding="utf-8",
    )

    skill_text = (
        "---\n"
        "name: review-runtime-ipc\n"
        "description: Review runtime IPC.\n"
        "---\n\n"
        "<!-- truth: start -->\n"
        "- Ordering truth\n"
        "<!-- truth: end -->\n\n"
        "# Runtime and IPC Review\n\n"
        "## Scope\n\n"
        "## Rulebook\n"
        "[Sample](../../bundles/knowledge/patterns/rust/sample.md)\n\n"
        "## Review Process\n\n"
        "## Suppressions Index\n"
        "- FP-RT-01: SeqCst wait\n\n"
        "## Focused Commands\n"
    )
    skill_path = repo / ".agents/skills/review-runtime-ipc/SKILL.md"
    skill_path.parent.mkdir(parents=True, exist_ok=True)
    skill_path.write_text(skill_text, encoding="utf-8")

    tools_yaml = (
        "  - read_file\n"
        "  - view_file\n"
        "  - grep_search\n"
        "  - find_by_name\n"
        "  - list_dir\n"
        "  - code_search\n"
        "  - run_command\n"
        "  - search_web\n"
        "  - moma_search\n"
        "  - skill_search\n"
        "  - send_message\n"
    )

    reviewer_text = (
        "---\n"
        "name: runtime-ipc-reviewer\n"
        "description: Reviews runtime IPC.\n"
        "model: pro\n"
        "mainAgent: false\n"
        "subagent: true\n"
        "commandExecutionPolicy: auto\n"
        "tools:\n"
        f"{tools_yaml}"
        "preloadSkills:\n"
        "  - review-core\n"
        "  - review-runtime-ipc\n"
        "  - flow-memory-keeper\n"
        "---\n\n"
        "# Reviewer\n\n"
        "## Scope\n\n"
        "## Review Process\n\n"
        "## Allowed Commands\n"
        "cargo test\n\n"
        "## Output\n"
    )
    rev_path = repo / ".agents/agents/runtime-ipc-reviewer.md"
    rev_path.parent.mkdir(parents=True, exist_ok=True)
    rev_path.write_text(reviewer_text, encoding="utf-8")

    specialist_tools = tools_yaml + "  - edit_file\n  - write_file\n"
    specialist_text = (
        "---\n"
        "name: runtime-ipc-specialist\n"
        "description: Implements runtime IPC.\n"
        "model: pro\n"
        "mainAgent: false\n"
        "subagent: true\n"
        "commandExecutionPolicy: auto\n"
        "tools:\n"
        f"{specialist_tools}"
        "preloadSkills:\n"
        "  - review-core\n"
        "  - review-runtime-ipc\n"
        "  - flow-memory-keeper\n"
        "---\n\n"
        "# Specialist\n\n"
        "## Scope\n\n"
        "## Implementation Process\n\n"
        "## Verification\n\n"
        "## Flow Integration\n"
    )
    spec_path = repo / ".agents/agents/runtime-ipc-specialist.md"
    spec_path.write_text(specialist_text, encoding="utf-8")

    index_text = (
        "# Skills Index\n\n"
        "| Area | Skill | Reviewer | Specialist |\n"
        "| --- | --- | --- | --- |\n"
        "| runtime-ipc | [review-runtime-ipc](review-runtime-ipc/SKILL.md) | [reviewer](../agents/runtime-ipc-reviewer.md) | [specialist](../agents/runtime-ipc-specialist.md) |\n"
    )
    (repo / ".agents/skills/index.md").write_text(index_text, encoding="utf-8")

    gitignore_text = ".agents/*\n!.agents/bundles/**\n!.agents/skills/**\n!.agents/agents/\n!.agents/agents/**\n"
    (repo / ".gitignore").write_text(gitignore_text, encoding="utf-8")

    (repo / "src/rs").mkdir(parents=True, exist_ok=True)
    (repo / "src/rs/channel.rs").write_text("// rust channel\n", encoding="utf-8")

    return repo


def test_manifest_catches_orphan_source_file(tmp_path: Path) -> None:
    """Verify manifest rule catches untracked source files not owned by any area."""
    repo = build_minimal_review_tree(tmp_path)
    orphan = repo / "src/rs/orphan.rs"
    orphan.write_text("// orphan\n", encoding="utf-8")

    areas_toml = (
        "schema_version = 1\n\n"
        "[agents]\n"
        'model = "pro"\n'
        "common_preload = []\n"
        "valid_tools = []\n"
        "write_tools = []\n"
        "reviewer_required_tools = []\n"
        "specialist_extra_tools = []\n"
        "reviewer_forbidden_commands = []\n\n"
        "[policy]\n"
        "max_review_skill_lines = 200\n"
        "max_truth_lines = 40\n\n"
        "[areas.runtime-ipc]\n"
        'prefix = "RT"\n'
        'title = "Rust runtime and IPC"\n'
        'paths = ["src/rs/channel.rs"]\n'
    )
    (repo / ".agents/skills/review-core/references/areas.toml").write_text(areas_toml, encoding="utf-8")

    validator = ReviewSetupValidator(repo)
    issues = validator.validate(frozenset({"manifest"}))
    assert any("not owned by any non-cross-cutting area" in i.message for i in issues)


def test_checklist_catches_duplicate_and_misordered_ids(tmp_path: Path) -> None:
    """Verify checklist rule detects duplicate IDs and misordered sequences."""
    repo = build_minimal_review_tree(tmp_path)
    bad_checklist = (
        "## RT: Rust runtime and IPC\n"
        "- **RT-02**: Bounds check. Verify: grep.\n"
        "- **RT-01**: Safety check. Verify: grep.\n"
    )
    chk_path = repo / ".agents/skills/review-core/references/checklist.md"
    chk_path.write_text(bad_checklist, encoding="utf-8")

    validator = ReviewSetupValidator(repo)
    issues = validator.validate(frozenset({"checklist"}))
    assert any("not strictly increasing" in i.message for i in issues)


def test_suppressions_catches_retired_or_unknown_checklist_id(tmp_path: Path) -> None:
    """Verify suppressions rule catches references to unknown checklist IDs."""
    repo = build_minimal_review_tree(tmp_path)
    bad_fp = (
        "### FP-RT-01: SeqCst wait\n"
        "- **Applies to**: RT-99\n"
        "- **Scope**: src/rs/channel.rs\n"
        "- **Do not flag**: SeqCst ordering.\n"
        "- **Why**: Primary source.\n"
    )
    fp_path = repo / ".agents/skills/review-core/references/false-positive-suppressions.md"
    fp_path.write_text(bad_fp, encoding="utf-8")

    validator = ReviewSetupValidator(repo)
    issues = validator.validate(frozenset({"suppressions"}))
    assert any("references unknown checklist ID 'RT-99'" in i.message for i in issues)


def test_skills_catches_truth_budget_and_missing_fp_index(tmp_path: Path) -> None:
    """Verify skills rule detects excessive truth block lines and missing FP index entries."""
    repo = build_minimal_review_tree(tmp_path)
    long_truth = "\n".join(f"- Truth line {i}" for i in range(50))
    skill_text = (
        "---\n"
        "name: review-runtime-ipc\n"
        "description: Review runtime IPC.\n"
        "---\n\n"
        f"<!-- truth: start -->\n{long_truth}\n<!-- truth: end -->\n\n"
        "# Runtime and IPC Review\n\n"
        "## Scope\n\n"
        "## Rulebook\n\n"
        "## Review Process\n\n"
        "## Suppressions Index\n\n"
        "## Focused Commands\n"
    )
    (repo / ".agents/skills/review-runtime-ipc/SKILL.md").write_text(skill_text, encoding="utf-8")

    validator = ReviewSetupValidator(repo)
    issues = validator.validate(frozenset({"skills"}))
    assert any("truth block line count" in i.message for i in issues)
    assert any("missing from skill Suppressions Index" in i.message for i in issues)


def test_agents_catches_write_tool_on_reviewer_and_forbidden_command(
    tmp_path: Path,
) -> None:
    """Verify agents rule rejects write tools on reviewers and forbidden commands."""
    repo = build_minimal_review_tree(tmp_path)
    tools_with_write = (
        "  - read_file\n"
        "  - view_file\n"
        "  - grep_search\n"
        "  - find_by_name\n"
        "  - list_dir\n"
        "  - code_search\n"
        "  - run_command\n"
        "  - search_web\n"
        "  - moma_search\n"
        "  - skill_search\n"
        "  - send_message\n"
        "  - write_file\n"
    )
    bad_reviewer = (
        "---\n"
        "name: runtime-ipc-reviewer\n"
        "description: Reviews runtime IPC.\n"
        "model: pro\n"
        "mainAgent: false\n"
        "subagent: true\n"
        "commandExecutionPolicy: auto\n"
        "tools:\n"
        f"{tools_with_write}"
        "preloadSkills:\n"
        "  - review-core\n"
        "  - review-runtime-ipc\n"
        "  - flow-memory-keeper\n"
        "---\n\n"
        "# Reviewer\n\n"
        "## Scope\n\n"
        "## Review Process\n\n"
        "## Allowed Commands\n"
        "make lint\n\n"
        "## Output\n"
    )
    (repo / ".agents/agents/runtime-ipc-reviewer.md").write_text(bad_reviewer, encoding="utf-8")

    validator = ReviewSetupValidator(repo)
    issues = validator.validate(frozenset({"agents"}))
    assert any("reviewer must not declare write tools" in i.message for i in issues)
    assert any("Allowed Commands contains forbidden command pattern" in i.message for i in issues)


def test_agents_rejects_unregistered_lazy_mcp_tools(tmp_path: Path) -> None:
    """Verify agents rule rejects lazy MCP multiplexer tools absent from the subagent tool registry."""
    repo = build_minimal_review_tree(tmp_path)
    rev_path = repo / ".agents/agents/runtime-ipc-reviewer.md"
    rev_text = rev_path.read_text(encoding="utf-8").replace(
        "  - send_message\n",
        "  - call_mcp_tool\n  - list_resources\n  - read_resource\n  - send_message\n",
    )
    rev_path.write_text(rev_text, encoding="utf-8")

    validator = ReviewSetupValidator(repo)
    issues = validator.validate(frozenset({"agents"}))
    for bad_tool in ("call_mcp_tool", "list_resources", "read_resource"):
        assert any(f"declared tool '{bad_tool}' is not in valid_tools" in i.message for i in issues)


def test_index_catches_missing_area_row(tmp_path: Path) -> None:
    """Verify skills index rule detects missing area links."""
    repo = build_minimal_review_tree(tmp_path)
    (repo / ".agents/skills/index.md").write_text("# Empty Index\n", encoding="utf-8")

    validator = ReviewSetupValidator(repo)
    issues = validator.validate(frozenset({"index"}))
    assert any("skills index missing entry for area 'runtime-ipc'" in i.message for i in issues)


def test_files_catches_executable_bit_and_symlink(tmp_path: Path) -> None:
    """Verify files rule flags executable bits on review markdown files."""
    repo = build_minimal_review_tree(tmp_path)
    target = repo / ".agents/agents/runtime-ipc-reviewer.md"
    current_mode = target.stat().st_mode
    target.chmod(current_mode | stat.S_IXUSR)

    validator = ReviewSetupValidator(repo)
    issues = validator.validate(frozenset({"files"}))
    assert any("file must not be executable" in i.message for i in issues)


def test_bundles_catches_completed_spec_and_broken_source(tmp_path: Path) -> None:
    """Verify bundles rule catches completed specs resident in specs directory."""
    repo = build_minimal_review_tree(tmp_path)
    spec_dir = repo / ".agents/bundles/specs/completed-flow"
    spec_dir.mkdir(parents=True, exist_ok=True)
    (spec_dir / "spec.md").write_text(
        "---\ntype: Spec\ntitle: Completed Flow\ndescription: Flow done.\nstate: completed\n---\n\n# Completed Flow\n",
        encoding="utf-8",
    )

    validator = ReviewSetupValidator(repo)
    issues = validator.validate(frozenset({"bundles"}))
    assert any("resident spec has 'state: completed'" in i.message for i in issues)


def test_privacy_catches_home_path_and_env_term_without_echoing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify privacy rule detects home paths and forbidden terms without leaking the term."""
    repo = build_minimal_review_tree(tmp_path)
    secret_token = "secret_canary_token_xyz"
    monkeypatch.setenv("VERTEBRA_REVIEW_FORBIDDEN_TERMS", secret_token)

    leak_file = repo / ".agents/skills/review-runtime-ipc/SKILL.md"
    content = leak_file.read_text(encoding="utf-8")
    content += f"\nLeaked token: {secret_token}\n"
    leak_file.write_text(content, encoding="utf-8")

    validator = ReviewSetupValidator(repo)
    issues = validator.validate(frozenset({"privacy"}))
    privacy_issues = [i for i in issues if i.rule == "privacy"]
    assert any("private term #1" in i.message for i in privacy_issues)
    for issue in privacy_issues:
        assert secret_token not in issue.message


def test_router_assigns_shards_and_cross_cutting(tmp_path: Path) -> None:
    """Verify router maps changed paths to area shards and includes cross-cutting areas."""
    repo = build_minimal_review_tree(tmp_path)
    manifest = ReviewManifest.from_toml(repo / ".agents/skills/review-core/references/areas.toml")
    router = ReviewRouter(manifest, repo)

    routed = router.route(["src/rs/channel.rs"])
    assert "runtime-ipc:rings" in routed
    assert "src/rs/channel.rs" in routed["runtime-ipc:rings"]


def test_ledger_rejects_unverified_accepted_critical(tmp_path: Path) -> None:
    """Verify ledger validator rejects accepted critical findings without verified confidence."""
    repo = build_minimal_review_tree(tmp_path)
    manifest = ReviewManifest.from_toml(repo / ".agents/skills/review-core/references/areas.toml")
    ledger_path = repo / "test-ledger.md"
    ledger_content = (
        "### RT-01 · critical · src/rs/channel.rs:L1-L1\n"
        "- **Finding**: Critical defect.\n"
        "- **Evidence**: Verified code citation.\n"
        "- **Fix**: Update code.\n"
        "- **Confidence**: likely\n"
        "- **Status**: accepted\n"
    )
    ledger_path.write_text(ledger_content, encoding="utf-8")

    validator = LedgerValidator(repo, manifest)
    issues = validator.validate_file(ledger_path)
    assert any("accepted critical finding requires 'Confidence: verified'" in i.message for i in issues)


def test_real_repo_review_setup_passes() -> None:
    """Verify real repository passes the full default review setup validator suite."""
    repo_root = Path.cwd()
    validator = ReviewSetupValidator(repo_root)
    issues = validator.validate()
    assert issues == []


def test_real_repo_bundles_and_privacy_pass() -> None:
    """Verify real repository bundles and privacy rules pass cleanly."""
    repo_root = Path.cwd()
    validator = ReviewSetupValidator(repo_root)
    issues = validator.validate(frozenset({"bundles", "privacy"}))
    assert issues == []


def test_python_imports_catches_private_typing_and_raw_import_module(tmp_path: Path) -> None:
    """Verify python_imports rule rejects vertebra._typing, raw importlib.import_module, and PY-07 prefix violations."""
    repo = build_minimal_review_tree(tmp_path)
    pkg_dir = repo / "src/py/vertebra/sample"
    pkg_dir.mkdir(parents=True, exist_ok=True)
    bad_file = pkg_dir / "mod.py"
    bad_file.write_text(
        "import importlib\n"
        "from vertebra._typing import MissingDependencyError\n\n"
        "class VertebraSampleConfig:\n"
        "    pass\n\n"
        "class PostgresVtbDialect:\n"
        "    pass\n\n"
        "class NativeSampleBroker:\n"
        "    pass\n\n"
        "def load() -> object:\n"
        "    return importlib.import_module('json')\n",
        encoding="utf-8",
    )

    validator = ReviewSetupValidator(repo)
    issues = validator.validate(frozenset({"python_imports"}))
    assert any("vertebra._typing" in i.message for i in issues)
    assert any("importlib.import_module" in i.message for i in issues)
    assert any("VertebraSampleConfig" in i.message for i in issues)
    assert any("PostgresVtbDialect" in i.message for i in issues)
    assert any("NativeSampleBroker" in i.message for i in issues)
