"""Manifest-driven structural validator, router, and finding ledger checker.

This module provides validation of review capability skills, paired subagents,
shared checklists, false-positive catalogs, OKF knowledge bundles, privacy
hygiene, and finding ledgers according to vertebra review infrastructure specs.
"""

import ast
import fnmatch
import os
import re
import sys
import tomllib
from collections.abc import Sequence
from pathlib import Path

import msgspec

__all__: list[str] = [
    "AgentPolicy",
    "AreaShard",
    "AreaSpec",
    "LedgerFinding",
    "LedgerValidator",
    "MarkdownDocument",
    "ReviewIssue",
    "ReviewManifest",
    "ReviewRouter",
    "ReviewSetupValidator",
    "ValidationPolicy",
    "main",
]


def _strip_leading_dot_slash(p: str) -> str:
    """Safely strip leading ./ or / without removing leading dot from hidden directory names."""
    if p.startswith("./"):
        return p[2:]
    if p.startswith("/"):
        return p[1:]
    return p


class ReviewIssue(msgspec.Struct, frozen=True, gc=False):
    """Represents a validation failure or structural issue."""

    rule: str
    path: str
    line: int
    message: str

    def format(self) -> str:
        """Format the issue as a standardized error line."""
        return f"ERROR [{self.rule}] {self.path}:{self.line}: {self.message}"


class AreaShard(msgspec.Struct, frozen=True, gc=False):
    """Specification for a partitioned review shard within an area."""

    name: str
    paths: list[str] = []


class AreaSpec(msgspec.Struct, frozen=True, gc=False):
    """Domain area specification from areas.toml."""

    prefix: str
    paths: list[str] = []
    title: str = ""
    knowledge: list[str] = []
    focused_commands: list[str] = []
    shards: list[AreaShard] = []
    cross_cutting: bool = False


class AgentPolicy(msgspec.Struct, frozen=True, gc=False):
    """Tooling and model policies for paired reviewer/specialist agents."""

    model: str = "pro"
    common_preload: list[str] = []
    valid_tools: list[str] = []
    write_tools: list[str] = []
    reviewer_required_tools: list[str] = []
    specialist_extra_tools: list[str] = []
    reviewer_forbidden_commands: list[str] = []


class ValidationPolicy(msgspec.Struct, frozen=True, gc=False):
    """Budgets and privacy scanning policies."""

    max_review_skill_lines: int = 200
    max_truth_lines: int = 40
    okf_reserved: list[str] = []
    privacy_scan: list[str] = []
    forbidden_patterns: list[str] = []
    forbidden_terms_env: str = "VERTEBRA_REVIEW_FORBIDDEN_TERMS"
    forbidden_terms_file: str = ".agents/scratch/forbidden-terms.txt"
    prohibition_line_markers: list[str] = []


class ReviewManifest(msgspec.Struct, frozen=True, gc=False):
    """Parsed manifest model representing areas.toml."""

    schema_version: int
    agents: AgentPolicy
    policy: ValidationPolicy
    areas: dict[str, AreaSpec]

    @classmethod
    def from_toml(cls, path: Path) -> "ReviewManifest":
        """Load and deserialize a review manifest from a TOML file."""
        with path.open("rb") as f:
            raw = tomllib.load(f)
        return msgspec.convert(raw, cls)


class MarkdownDocument(msgspec.Struct, frozen=True, gc=False):
    """Parsed markdown file with frontmatter, headings, links, and truth block."""

    path: Path
    frontmatter: dict[str, object]
    body: str
    truth_lines: list[str]
    truth_span: tuple[int, int] | None
    headings: list[tuple[int, str, int]]
    links: list[tuple[str, int]]
    total_lines: int

    @classmethod
    def parse(cls, path: Path) -> "MarkdownDocument":
        """Parse frontmatter and structural blocks from a markdown document."""
        content = path.read_text(encoding="utf-8")
        lines = content.splitlines()
        total_lines = len(lines)

        frontmatter: dict[str, object] = {}
        body_start_line = 0

        if lines and lines[0].strip() == "---":
            yaml_lines: list[str] = []
            idx = 1
            while idx < len(lines):
                line = lines[idx]
                if line.strip() == "---":
                    body_start_line = idx + 1
                    break
                yaml_lines.append(line)
                idx += 1
            frontmatter = cls._parse_simple_yaml(yaml_lines)

        body_lines = lines[body_start_line:]
        body = "\n".join(body_lines)

        truth_lines: list[str] = []
        truth_span: tuple[int, int] | None = None
        in_truth = False
        truth_start = 0

        for line_idx, line in enumerate(lines, start=1):
            stripped = line.strip()
            if "<!-- truth: start -->" in stripped:
                in_truth = True
                truth_start = line_idx
                continue
            if "<!-- truth: end -->" in stripped:
                in_truth = False
                truth_span = (truth_start, line_idx)
                continue
            if in_truth:
                truth_lines.append(line)

        headings: list[tuple[int, str, int]] = []
        links: list[tuple[str, int]] = []
        heading_re = re.compile(r"^(#{1,6})\s+(.+)$")
        link_re = re.compile(r"\[([^\]]+)\]\(([^)]+)\)")

        in_code_fence = False
        fence_char = ""

        for line_idx, line in enumerate(lines, start=1):
            stripped = line.strip()
            if stripped.startswith("```") or stripped.startswith("~~~~"):
                current_fence = stripped[:3]
                if not in_code_fence:
                    in_code_fence = True
                    fence_char = current_fence
                elif current_fence == fence_char:
                    in_code_fence = False
                continue

            if in_code_fence:
                continue

            match = heading_re.match(line)
            if match:
                level = len(match.group(1))
                text = match.group(2).strip()
                headings.append((level, text, line_idx))

            for match in link_re.finditer(line):
                target = match.group(2).strip()
                if not target.startswith(("http://", "https://", "mailto:", "#")):
                    links.append((target, line_idx))

        return cls(
            path=path,
            frontmatter=frontmatter,
            body=body,
            truth_lines=truth_lines,
            truth_span=truth_span,
            headings=headings,
            links=links,
            total_lines=total_lines,
        )

    @classmethod
    def _parse_simple_yaml(cls, lines: list[str]) -> dict[str, object]:
        """Parse YAML scalars, booleans, nested mappings, and lists without external dependencies."""
        result: dict[str, object] = {}
        current_top_key: str | None = None
        current_list: list[object] | None = None
        current_dict: dict[str, object] | None = None
        current_list_item_dict: dict[str, object] | None = None

        for line in lines:
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue

            indent = len(line) - len(line.lstrip(" "))

            if indent == 0:
                if current_top_key is not None:
                    if current_list is not None:
                        result[current_top_key] = current_list
                    elif current_dict is not None:
                        result[current_top_key] = current_dict
                current_top_key = None
                current_list = None
                current_dict = None
                current_list_item_dict = None

                if ":" in stripped:
                    key, rest = stripped.split(":", 1)
                    key = key.strip()
                    rest = rest.strip()
                    if not rest:
                        current_top_key = key
                    elif rest.startswith("[") and rest.endswith("]"):
                        inner = rest[1:-1].strip()
                        if not inner:
                            result[key] = []
                        else:
                            result[key] = [cls._parse_scalar(item.strip()) for item in inner.split(",")]
                    else:
                        result[key] = cls._parse_scalar(rest)
                continue

            if current_top_key is None:
                continue

            if stripped.startswith("- "):
                if current_list is None:
                    current_list = []
                item_str = stripped[2:].strip()
                if ":" in item_str and not (
                    (item_str.startswith('"') and item_str.endswith('"'))
                    or (item_str.startswith("'") and item_str.endswith("'"))
                ):
                    ik, iv = item_str.split(":", 1)
                    current_list_item_dict = {ik.strip(): cls._parse_scalar(iv.strip())}
                    current_list.append(current_list_item_dict)
                else:
                    current_list_item_dict = None
                    current_list.append(cls._parse_scalar(item_str))
                continue

            if ":" in stripped:
                ik, iv = stripped.split(":", 1)
                ik = ik.strip()
                iv = iv.strip()
                parsed_val = cls._parse_scalar(iv)
                if current_list_item_dict is not None:
                    current_list_item_dict[ik] = parsed_val
                else:
                    if current_dict is None:
                        current_dict = {}
                    current_dict[ik] = parsed_val

        if current_top_key is not None:
            if current_list is not None:
                result[current_top_key] = current_list
            elif current_dict is not None:
                result[current_top_key] = current_dict
            else:
                result[current_top_key] = []

        return result

    @classmethod
    def _parse_scalar(cls, val: str) -> object:
        """Parse primitive scalar types from string."""
        if (val.startswith('"') and val.endswith('"')) or (val.startswith("'") and val.endswith("'")):
            return val[1:-1]
        lower = val.lower()
        if lower == "true":
            return True
        if lower == "false":
            return False
        if lower in ("null", "~"):
            return None
        try:
            return int(val)
        except ValueError:
            pass
        try:
            return float(val)
        except ValueError:
            pass
        return val


class ReviewRouter:
    """Routes changed file paths to corresponding review areas and shards."""

    def __init__(self, manifest: ReviewManifest, repo_root: Path) -> None:
        self._manifest = manifest
        self._repo_root = repo_root

    def route(self, paths: Sequence[str]) -> dict[str, list[str]]:
        """Map paths to area or area:shard keys."""
        routed: dict[str, list[str]] = {}
        any_code_matched = False

        normalized_paths = [_strip_leading_dot_slash(p.strip()) for p in paths if p.strip()]

        for area_name, area in self._manifest.areas.items():
            if area.cross_cutting:
                continue
            matched_area_paths: list[str] = []
            for path in normalized_paths:
                if self._matches_any(path, area.paths):
                    matched_area_paths.append(path)
                    any_code_matched = True

            if not matched_area_paths:
                continue

            if not area.shards:
                routed.setdefault(area_name, []).extend(matched_area_paths)
            else:
                assigned_to_shard: set[str] = set()
                for shard in area.shards:
                    shard_key = f"{area_name}:{shard.name}"
                    for path in matched_area_paths:
                        if self._matches_any(path, shard.paths):
                            routed.setdefault(shard_key, []).append(path)
                            assigned_to_shard.add(path)
                unsharded = [p for p in matched_area_paths if p not in assigned_to_shard]
                if unsharded:
                    routed.setdefault(area_name, []).extend(unsharded)

        if any_code_matched:
            for area_name, area in self._manifest.areas.items():
                if area.cross_cutting:
                    routed[area_name] = list(normalized_paths)

        return routed

    @staticmethod
    def _matches_any(path: str, patterns: Sequence[str]) -> bool:
        """Check if path matches any glob pattern."""
        for pat in patterns:
            cleaned_pat = _strip_leading_dot_slash(pat)
            if fnmatch.fnmatch(path, cleaned_pat):
                return True
            if cleaned_pat.endswith("/**") and (path.startswith(cleaned_pat[:-3] + "/") or path == cleaned_pat[:-3]):
                return True
        return False


class LedgerFinding(msgspec.Struct, frozen=True, gc=False):
    """Parsed finding from a markdown review ledger."""

    checklist_id: str
    severity: str
    path: str
    line_start: int | None
    line_end: int | None
    finding: str
    evidence: str
    fix: str
    confidence: str
    status: str
    line: int


class LedgerValidator:
    """Validates structural correctness of findings in review ledgers."""

    def __init__(self, repo_root: Path, manifest: ReviewManifest | None = None) -> None:
        self._repo_root = repo_root
        self._manifest = manifest

    def validate_file(self, ledger_path: Path) -> list[ReviewIssue]:
        """Validate a finding ledger file against the grammar specification."""
        issues: list[ReviewIssue] = []
        if not ledger_path.exists():
            return [ReviewIssue("ledger", str(ledger_path), 1, "ledger file does not exist")]

        content = ledger_path.read_text(encoding="utf-8")
        lines = content.splitlines()

        heading_re = re.compile(
            r"^###\s+(?P<id>[A-Z]+-\d{2}|[A-Z]+-NEW)\s+·\s+(?P<sev>critical|important|minor)\s+·\s+(?P<path>[^:\s]+)(?::L(?P<start>\d+)(?:-L(?P<end>\d+))?)?$"
        )

        known_checklist_ids = self._load_checklist_ids()
        known_fps = self._load_fp_ids()

        idx = 0
        while idx < len(lines):
            line = lines[idx]
            line_no = idx + 1
            if line.startswith("### "):
                match = heading_re.match(line)
                if not match:
                    issues.append(
                        ReviewIssue(
                            "ledger",
                            str(ledger_path.relative_to(self._repo_root)),
                            line_no,
                            f"malformed finding heading: '{line}'",
                        )
                    )
                    idx += 1
                    continue

                cid = match.group("id")
                sev = match.group("sev")
                target_path = match.group("path")
                start_str = match.group("start")
                end_str = match.group("end")

                prefix = cid.split("-")[0]
                if cid.endswith("-NEW"):
                    if self._manifest and prefix not in {a.prefix for a in self._manifest.areas.values()}:
                        issues.append(
                            ReviewIssue(
                                "ledger",
                                str(ledger_path.relative_to(self._repo_root)),
                                line_no,
                                f"unknown prefix in new checklist id: {cid}",
                            )
                        )
                elif known_checklist_ids and cid not in known_checklist_ids:
                    issues.append(
                        ReviewIssue(
                            "ledger",
                            str(ledger_path.relative_to(self._repo_root)),
                            line_no,
                            f"unknown checklist id: {cid}",
                        )
                    )

                full_target = self._repo_root / target_path
                if not full_target.exists():
                    issues.append(
                        ReviewIssue(
                            "ledger",
                            str(ledger_path.relative_to(self._repo_root)),
                            line_no,
                            f"target path does not exist: {target_path}",
                        )
                    )
                elif start_str is not None:
                    target_lines = len(full_target.read_text(encoding="utf-8").splitlines())
                    start_val = int(start_str)
                    if start_val < 1 or start_val > max(1, target_lines):
                        issues.append(
                            ReviewIssue(
                                "ledger",
                                str(ledger_path.relative_to(self._repo_root)),
                                line_no,
                                f"line start {start_val} out of bounds for {target_path} ({target_lines} lines)",
                            )
                        )
                    if end_str is not None:
                        end_val = int(end_str)
                        if end_val < start_val or end_val > max(1, target_lines):
                            issues.append(
                                ReviewIssue(
                                    "ledger",
                                    str(ledger_path.relative_to(self._repo_root)),
                                    line_no,
                                    f"line end {end_val} invalid for {target_path} ({target_lines} lines)",
                                )
                            )

                bullet_fields: dict[str, str] = {}
                sub_idx = idx + 1
                while sub_idx < len(lines):
                    sub_line = lines[sub_idx].strip()
                    if sub_line.startswith("### ") or sub_line.startswith("## "):
                        break
                    for field_name in ("Finding", "Evidence", "Fix", "Confidence", "Status"):
                        marker = f"- **{field_name}**:"
                        if sub_line.startswith(marker):
                            val = sub_line[len(marker) :].strip()
                            bullet_fields[field_name] = val
                    sub_idx += 1

                for required_field in ("Finding", "Evidence", "Fix", "Confidence", "Status"):
                    if required_field not in bullet_fields:
                        issues.append(
                            ReviewIssue(
                                "ledger",
                                str(ledger_path.relative_to(self._repo_root)),
                                line_no,
                                f"finding missing required field: {required_field}",
                            )
                        )

                conf = bullet_fields.get("Confidence", "")
                if conf and conf not in ("verified", "likely"):
                    issues.append(
                        ReviewIssue(
                            "ledger",
                            str(ledger_path.relative_to(self._repo_root)),
                            line_no,
                            f"invalid confidence '{conf}', must be 'verified' or 'likely'",
                        )
                    )

                stat = bullet_fields.get("Status", "")
                if stat:
                    valid_status = False
                    if stat in ("open", "accepted") or stat.startswith(("duplicate:", "wontfix:", "fixed:")):
                        valid_status = True
                    elif stat.startswith("fp:"):
                        fp_val = stat[3:].strip()
                        valid_status = True
                        if known_fps and fp_val not in known_fps:
                            issues.append(
                                ReviewIssue(
                                    "ledger",
                                    str(ledger_path.relative_to(self._repo_root)),
                                    line_no,
                                    f"unknown false-positive suppression id: {fp_val}",
                                )
                            )
                    if not valid_status:
                        issues.append(
                            ReviewIssue(
                                "ledger",
                                str(ledger_path.relative_to(self._repo_root)),
                                line_no,
                                f"invalid status '{stat}'",
                            )
                        )

                if stat == "accepted" and sev in ("critical", "important") and conf != "verified":
                    issues.append(
                        ReviewIssue(
                            "ledger",
                            str(ledger_path.relative_to(self._repo_root)),
                            line_no,
                            f"accepted {sev} finding requires 'Confidence: verified'",
                        )
                    )

            idx += 1

        return issues

    def _load_checklist_ids(self) -> set[str]:
        """Collect existing checklist IDs from checklist.md if present."""
        chk = self._repo_root / ".agents/skills/review-core/references/checklist.md"
        if not chk.exists():
            return set()
        ids: set[str] = set()
        chk_re = re.compile(r"^-\s+(?:\*\*|~~\*\*)(?P<id>[A-Z]+-\d{2})(?:\*\*|~~\*\*)")
        for line in chk.read_text(encoding="utf-8").splitlines():
            m = chk_re.match(line)
            if m:
                ids.add(m.group("id"))
        return ids

    def _load_fp_ids(self) -> set[str]:
        """Collect existing suppression IDs from false-positive-suppressions.md."""
        fp_path = self._repo_root / ".agents/skills/review-core/references/false-positive-suppressions.md"
        if not fp_path.exists():
            return set()
        fps: set[str] = set()
        fp_re = re.compile(r"^###\s+(?P<id>FP-[A-Z]+-\d{2}):")
        for line in fp_path.read_text(encoding="utf-8").splitlines():
            m = fp_re.match(line)
            if m:
                fps.add(m.group("id"))
        return fps


class ReviewSetupValidator:
    """Full-suite structural validator for review capability setup and rules."""

    DEFAULT_RULES: frozenset[str] = frozenset(
        {
            "manifest",
            "checklist",
            "suppressions",
            "skills",
            "agents",
            "index",
            "files",
            "bundles",
            "privacy",
            "python_imports",
        }
    )

    def __init__(self, repo_root: Path) -> None:
        self.repo_root = repo_root
        self._manifest_path = repo_root / ".agents/skills/review-core/references/areas.toml"
        self._manifest: ReviewManifest | None = None
        if self._manifest_path.exists():
            try:
                self._manifest = ReviewManifest.from_toml(self._manifest_path)
            except Exception:
                self._manifest = None

    def validate(self, rules: frozenset[str] | None = None) -> list[ReviewIssue]:
        """Execute selected validator rules and collect issues."""
        active_rules = rules if rules is not None else self.DEFAULT_RULES
        issues: list[ReviewIssue] = []

        if "manifest" in active_rules:
            issues.extend(self._check_manifest())

        if "checklist" in active_rules:
            issues.extend(self._check_checklist())

        if "suppressions" in active_rules:
            issues.extend(self._check_suppressions())

        if "skills" in active_rules:
            issues.extend(self._check_skills())

        if "agents" in active_rules:
            issues.extend(self._check_agents())

        if "index" in active_rules:
            issues.extend(self._check_index())

        if "files" in active_rules:
            issues.extend(self._check_files())

        if "bundles" in active_rules:
            issues.extend(self._check_bundles())

        if "privacy" in active_rules:
            issues.extend(self._check_privacy())

        if "python_imports" in active_rules:
            issues.extend(self._check_python_imports())

        return issues

    def _check_manifest(self) -> list[ReviewIssue]:
        """Validate areas.toml schema, prefixes, path coverage, and knowledge links."""
        issues: list[ReviewIssue] = []
        rel_path = str(self._manifest_path.relative_to(self.repo_root))

        if not self._manifest_path.exists():
            return [ReviewIssue("manifest", rel_path, 1, "manifest file does not exist")]

        if self._manifest is None:
            return [ReviewIssue("manifest", rel_path, 1, "failed to parse areas.toml")]

        manifest = self._manifest
        if manifest.schema_version != 1:
            issues.append(
                ReviewIssue(
                    "manifest",
                    rel_path,
                    1,
                    f"schema_version must be 1, found {manifest.schema_version}",
                )
            )

        seen_prefixes: set[str] = set()
        area_slug_re = re.compile(r"^[a-z0-9-]+$")
        prefix_re = re.compile(r"^[A-Z]{2,5}$")

        tracked_files = self._get_tracked_files()

        for area_name, area in manifest.areas.items():
            if not area_slug_re.match(area_name):
                issues.append(
                    ReviewIssue(
                        "manifest",
                        rel_path,
                        1,
                        f"area name '{area_name}' must be a lowercase slug",
                    )
                )

            if not prefix_re.match(area.prefix):
                issues.append(
                    ReviewIssue(
                        "manifest",
                        rel_path,
                        1,
                        f"prefix '{area.prefix}' for area '{area_name}' must match [A-Z]{{2,5}}",
                    )
                )

            if area.prefix in seen_prefixes:
                issues.append(
                    ReviewIssue(
                        "manifest",
                        rel_path,
                        1,
                        f"duplicate prefix '{area.prefix}' in area '{area_name}'",
                    )
                )
            seen_prefixes.add(area.prefix)

            if not area.cross_cutting:
                area_matches = False
                for p in area.paths:
                    if any(ReviewRouter._matches_any(tf, [p]) for tf in tracked_files):
                        area_matches = True
                        break
                if not area_matches and tracked_files:
                    issues.append(
                        ReviewIssue(
                            "manifest",
                            rel_path,
                            1,
                            f"area '{area_name}' paths match no tracked files",
                        )
                    )

            for shard in area.shards:
                shard_matches = False
                for p in shard.paths:
                    if any(ReviewRouter._matches_any(tf, [p]) for tf in tracked_files):
                        shard_matches = True
                        break
                if not shard_matches and tracked_files:
                    issues.append(
                        ReviewIssue(
                            "manifest",
                            rel_path,
                            1,
                            f"shard '{shard.name}' in area '{area_name}' paths match no tracked files",
                        )
                    )

            for kp in area.knowledge:
                kp_cleaned = _strip_leading_dot_slash(kp)
                if not (self.repo_root / kp_cleaned).exists():
                    issues.append(
                        ReviewIssue(
                            "manifest",
                            rel_path,
                            1,
                            f"knowledge path '{kp}' for area '{area_name}' does not exist",
                        )
                    )

        if tracked_files:
            orphan_source_files: list[str] = []
            non_cross_cutting_areas = [a for a in manifest.areas.values() if not a.cross_cutting]
            for tf in tracked_files:
                if (
                    tf.startswith("src/rs/")
                    or tf.startswith("src/py/vertebra/")
                    or tf.startswith("src/ts/vertebra-client/src/")
                ):
                    owned = any(ReviewRouter._matches_any(tf, area.paths) for area in non_cross_cutting_areas)
                    if not owned:
                        orphan_source_files.append(tf)

            for orphan in orphan_source_files[:10]:
                issues.append(
                    ReviewIssue(
                        "manifest",
                        rel_path,
                        1,
                        f"tracked source file '{orphan}' not owned by any non-cross-cutting area",
                    )
                )

        return issues

    def _check_checklist(self) -> list[ReviewIssue]:
        """Validate checklist sections, ID uniqueness, ordering, and syntax."""
        issues: list[ReviewIssue] = []
        chk_path = self.repo_root / ".agents/skills/review-core/references/checklist.md"
        rel_path = str(chk_path.relative_to(self.repo_root))

        if not chk_path.exists():
            return [ReviewIssue("checklist", rel_path, 1, "checklist file does not exist")]

        content = chk_path.read_text(encoding="utf-8")
        lines = content.splitlines()

        section_re = re.compile(r"^##\s+(?P<prefix>[A-Z]{2,5}):\s+.+$")
        item_re = re.compile(r"^-\s+\*\*(?P<id>(?P<prefix>[A-Z]{2,5})-(?P<num>\d{2}))\*\*:\s+.+Verify:\s+.+$")
        retired_re = re.compile(
            r"^-\s+~~\*\*(?P<id>(?P<prefix>[A-Z]{2,5})-(?P<num>\d{2}))\*\*~~:\s+retired(?:\s+\(.+\))?$"
        )

        seen_ids: set[str] = set()
        seen_section_prefixes: set[str] = set()
        current_section_prefix: str | None = None
        last_num_in_section: int = 0

        manifest_prefixes = {a.prefix for a in self._manifest.areas.values()} if self._manifest else set()

        for line_no, line in enumerate(lines, start=1):
            sec_match = section_re.match(line)
            if sec_match:
                prefix = sec_match.group("prefix")
                current_section_prefix = prefix
                seen_section_prefixes.add(prefix)
                last_num_in_section = 0
                if manifest_prefixes and prefix not in manifest_prefixes:
                    issues.append(
                        ReviewIssue(
                            "checklist",
                            rel_path,
                            line_no,
                            f"section prefix '{prefix}' not declared in manifest",
                        )
                    )
                continue

            item_match = item_re.match(line) or retired_re.match(line)
            if item_match:
                cid = item_match.group("id")
                prefix = item_match.group("prefix")
                num = int(item_match.group("num"))

                if current_section_prefix is None or prefix != current_section_prefix:
                    issues.append(
                        ReviewIssue(
                            "checklist",
                            rel_path,
                            line_no,
                            f"item '{cid}' prefix does not match section '{current_section_prefix}'",
                        )
                    )

                if cid in seen_ids:
                    issues.append(
                        ReviewIssue(
                            "checklist",
                            rel_path,
                            line_no,
                            f"duplicate checklist ID '{cid}'",
                        )
                    )
                seen_ids.add(cid)

                if num <= last_num_in_section:
                    issues.append(
                        ReviewIssue(
                            "checklist",
                            rel_path,
                            line_no,
                            f"checklist ID '{cid}' not strictly increasing (was {last_num_in_section})",
                        )
                    )
                last_num_in_section = num

        if manifest_prefixes:
            for p in manifest_prefixes:
                if p not in seen_section_prefixes:
                    issues.append(
                        ReviewIssue(
                            "checklist",
                            rel_path,
                            1,
                            f"manifest prefix '{p}' has no section in checklist.md",
                        )
                    )

        return issues

    def _check_suppressions(self) -> list[ReviewIssue]:
        """Validate false-positive suppression catalog headings, IDs, and links."""
        issues: list[ReviewIssue] = []
        fp_path = self.repo_root / ".agents/skills/review-core/references/false-positive-suppressions.md"
        rel_path = str(fp_path.relative_to(self.repo_root))

        if not fp_path.exists():
            return [ReviewIssue("suppressions", rel_path, 1, "suppressions file does not exist")]

        content = fp_path.read_text(encoding="utf-8")
        lines = content.splitlines()

        heading_re = re.compile(r"^###\s+FP-(?P<prefix>[A-Z]{2,5})-(?P<num>\d{2}):\s+.+$")

        manifest_prefixes = {a.prefix for a in self._manifest.areas.values()} if self._manifest else set()
        valid_checklist_ids, retired_ids = self._get_checklist_ids_and_retired()
        tracked_files = self._get_tracked_files()

        seen_fps: set[str] = set()
        idx = 0
        while idx < len(lines):
            line = lines[idx]
            line_no = idx + 1
            if line.startswith("### "):
                m = heading_re.match(line)
                if not m:
                    issues.append(
                        ReviewIssue(
                            "suppressions",
                            rel_path,
                            line_no,
                            f"malformed suppression heading: '{line}'",
                        )
                    )
                    idx += 1
                    continue

                prefix = m.group("prefix")
                num_str = m.group("num")
                fp_id = f"FP-{prefix}-{num_str}"

                if fp_id in seen_fps:
                    issues.append(
                        ReviewIssue(
                            "suppressions",
                            rel_path,
                            line_no,
                            f"duplicate suppression ID '{fp_id}'",
                        )
                    )
                seen_fps.add(fp_id)

                if manifest_prefixes and prefix not in manifest_prefixes:
                    issues.append(
                        ReviewIssue(
                            "suppressions",
                            rel_path,
                            line_no,
                            f"unknown prefix '{prefix}' in suppression ID '{fp_id}'",
                        )
                    )

                entry_lines: dict[str, str] = {}
                sub_idx = idx + 1
                while sub_idx < len(lines):
                    sub_line = lines[sub_idx].strip()
                    if sub_line.startswith("### ") or sub_line.startswith("---"):
                        break
                    for tag in ("Applies to", "Scope", "Do not flag", "Why"):
                        marker = f"- **{tag}**:"
                        if sub_line.startswith(marker):
                            entry_lines[tag] = sub_line[len(marker) :].strip()
                    sub_idx += 1

                for required_tag in ("Applies to", "Scope", "Do not flag", "Why"):
                    if required_tag not in entry_lines:
                        issues.append(
                            ReviewIssue(
                                "suppressions",
                                rel_path,
                                line_no,
                                f"suppression '{fp_id}' missing '{required_tag}' bullet",
                            )
                        )

                applies_to_val = entry_lines.get("Applies to", "")
                if applies_to_val:
                    for cid_raw in applies_to_val.split(","):
                        cid = cid_raw.strip()
                        if not cid:
                            continue
                        if valid_checklist_ids and cid not in valid_checklist_ids:
                            issues.append(
                                ReviewIssue(
                                    "suppressions",
                                    rel_path,
                                    line_no,
                                    f"suppression '{fp_id}' references unknown checklist ID '{cid}'",
                                )
                            )
                        if cid in retired_ids:
                            issues.append(
                                ReviewIssue(
                                    "suppressions",
                                    rel_path,
                                    line_no,
                                    f"suppression '{fp_id}' references retired checklist ID '{cid}'",
                                )
                            )

                scope_val = entry_lines.get("Scope", "")
                if scope_val and tracked_files:
                    scopes = [s.strip().strip("`").strip("'") for s in scope_val.split(",") if s.strip()]
                    for sc in scopes:
                        if not any(ReviewRouter._matches_any(tf, [sc]) for tf in tracked_files):
                            issues.append(
                                ReviewIssue(
                                    "suppressions",
                                    rel_path,
                                    line_no,
                                    f"suppression '{fp_id}' Scope '{sc}' matches no tracked files",
                                )
                            )

                why_val = entry_lines.get("Why", "")
                if why_val:
                    link_match = re.search(r"\[([^\]]+)\]\(([^)]+)\)", why_val)
                    if link_match:
                        target = link_match.group(2).strip()
                        resolved = (fp_path.parent / target).resolve()
                        if not resolved.exists():
                            issues.append(
                                ReviewIssue(
                                    "suppressions",
                                    rel_path,
                                    line_no,
                                    f"suppression '{fp_id}' Why link '{target}' does not resolve",
                                )
                            )

            idx += 1

        return issues

    def _check_skills(self) -> list[ReviewIssue]:
        """Validate review capability skills, line budgets, and FP parity."""
        issues: list[ReviewIssue] = []
        skills_root = self.repo_root / ".agents/skills"
        if not skills_root.exists():
            return issues

        manifest = self._manifest
        if manifest is None:
            return issues

        max_lines = manifest.policy.max_review_skill_lines
        max_truth = manifest.policy.max_truth_lines

        existing_fp_ids = self._get_suppression_ids()
        active_cids, retired_cids = self._get_checklist_ids_and_retired()
        all_cids = active_cids | retired_cids

        for area_name, area in manifest.areas.items():
            skill_dir = skills_root / f"review-{area_name}"
            skill_file = skill_dir / "SKILL.md"
            rel_file = str(skill_file.relative_to(self.repo_root))

            if not skill_file.exists():
                issues.append(
                    ReviewIssue(
                        "skills",
                        rel_file,
                        1,
                        f"missing skill file for area '{area_name}'",
                    )
                )
                continue

            doc = MarkdownDocument.parse(skill_file)

            if doc.frontmatter.get("name") != f"review-{area_name}":
                issues.append(
                    ReviewIssue(
                        "skills",
                        rel_file,
                        1,
                        f"frontmatter name must be 'review-{area_name}'",
                    )
                )

            desc = doc.frontmatter.get("description")
            if not desc or not str(desc).strip():
                issues.append(
                    ReviewIssue(
                        "skills",
                        rel_file,
                        1,
                        "frontmatter description must not be empty",
                    )
                )

            if doc.total_lines > max_lines:
                issues.append(
                    ReviewIssue(
                        "skills",
                        rel_file,
                        doc.total_lines,
                        f"skill line count {doc.total_lines} exceeds budget {max_lines}",
                    )
                )

            if not doc.truth_lines or doc.truth_span is None:
                issues.append(
                    ReviewIssue(
                        "skills",
                        rel_file,
                        1,
                        "missing <!-- truth: start --> .. <!-- truth: end --> block",
                    )
                )
            else:
                truth_count = len(doc.truth_lines)
                if truth_count > max_truth:
                    issues.append(
                        ReviewIssue(
                            "skills",
                            rel_file,
                            doc.truth_span[0],
                            f"truth block line count {truth_count} exceeds budget {max_truth}",
                        )
                    )

            skill_fp_indexed: set[str] = set()
            in_suppressions_index = False
            skill_text = skill_file.read_text(encoding="utf-8")
            for line_no, line in enumerate(skill_text.splitlines(), start=1):
                if line.startswith("## Suppressions Index"):
                    in_suppressions_index = True
                    continue
                if in_suppressions_index and line.startswith("## "):
                    in_suppressions_index = False
                if in_suppressions_index:
                    m = re.search(r"\b(FP-[A-Z]+-\d{2})\b", line)
                    if m:
                        skill_fp_indexed.add(m.group(1))

                for match in re.finditer(r"\b([A-Z]{2,5}-\d{2})\b", line):
                    start = match.start()
                    if start >= 3 and line[start - 3 : start] == "FP-":
                        continue
                    cid = match.group(1)
                    if all_cids and cid not in all_cids:
                        issues.append(
                            ReviewIssue(
                                "skills",
                                rel_file,
                                line_no,
                                f"unknown checklist ID {cid}",
                            )
                        )

            area_fps = {fpid for fpid in existing_fp_ids if fpid.startswith(f"FP-{area.prefix}-")}
            for fp in area_fps:
                if fp not in skill_fp_indexed:
                    issues.append(
                        ReviewIssue(
                            "skills",
                            rel_file,
                            1,
                            f"suppression '{fp}' missing from skill Suppressions Index",
                        )
                    )
            for fp in skill_fp_indexed:
                if fp not in existing_fp_ids:
                    issues.append(
                        ReviewIssue(
                            "skills",
                            rel_file,
                            1,
                            f"skill indexes unknown suppression '{fp}'",
                        )
                    )

            for target, line_no in doc.links:
                target_clean = target.split("#")[0]
                if target_clean:
                    resolved = (skill_file.parent / target_clean).resolve()
                    if not resolved.exists():
                        issues.append(
                            ReviewIssue(
                                "skills",
                                rel_file,
                                line_no,
                                f"relative link '{target}' does not resolve",
                            )
                        )

        for child in skills_root.iterdir():
            if not child.is_dir():
                continue
            if child.name.startswith("review-"):
                if child.name == "review-core":
                    continue
                area_stem = child.name[len("review-") :]
                if area_stem not in manifest.areas:
                    issues.append(
                        ReviewIssue(
                            "skills",
                            str(child.relative_to(self.repo_root)),
                            1,
                            f"review skill directory '{child.name}' has no area in areas.toml",
                        )
                    )
                continue

            domain_skill_file = child / "SKILL.md"
            rel_domain_file = str(domain_skill_file.relative_to(self.repo_root))
            if not domain_skill_file.exists():
                issues.append(
                    ReviewIssue(
                        "skills",
                        rel_domain_file,
                        1,
                        f"missing SKILL.md in skill directory '{child.name}'",
                    )
                )
                continue

            domain_doc = MarkdownDocument.parse(domain_skill_file)
            if domain_doc.frontmatter.get("name") != child.name:
                issues.append(
                    ReviewIssue(
                        "skills",
                        rel_domain_file,
                        1,
                        f"frontmatter name must be '{child.name}'",
                    )
                )
            domain_desc = domain_doc.frontmatter.get("description")
            if not domain_desc or not str(domain_desc).strip():
                issues.append(
                    ReviewIssue(
                        "skills",
                        rel_domain_file,
                        1,
                        "frontmatter description must not be empty",
                    )
                )
            if domain_doc.total_lines > max_lines:
                issues.append(
                    ReviewIssue(
                        "skills",
                        rel_domain_file,
                        domain_doc.total_lines,
                        f"skill line count {domain_doc.total_lines} exceeds budget {max_lines}",
                    )
                )
            if not domain_doc.truth_lines or domain_doc.truth_span is None:
                issues.append(
                    ReviewIssue(
                        "skills",
                        rel_domain_file,
                        1,
                        "missing <!-- truth: start --> .. <!-- truth: end --> block",
                    )
                )
            elif len(domain_doc.truth_lines) > max_truth:
                issues.append(
                    ReviewIssue(
                        "skills",
                        rel_domain_file,
                        domain_doc.truth_span[0],
                        f"truth block line count {len(domain_doc.truth_lines)} exceeds budget {max_truth}",
                    )
                )
            for target, line_no in domain_doc.links:
                target_clean = target.split("#")[0]
                if target_clean:
                    resolved = (domain_skill_file.parent / target_clean).resolve()
                    if not resolved.exists():
                        issues.append(
                            ReviewIssue(
                                "skills",
                                rel_domain_file,
                                line_no,
                                f"relative link '{target}' does not resolve",
                            )
                        )

        return issues

    def _check_agents(self) -> list[ReviewIssue]:
        """Validate custom agent definitions, tools, commands, and preloads."""
        issues: list[ReviewIssue] = []
        agents_dir = self.repo_root / ".agents/agents"
        if not agents_dir.exists():
            return issues

        manifest = self._manifest
        if manifest is None:
            return issues

        agent_policy = manifest.agents
        valid_tools = set(agent_policy.valid_tools)
        write_tools = set(agent_policy.write_tools)
        req_tools = set(agent_policy.reviewer_required_tools)
        extra_spec = set(agent_policy.specialist_extra_tools)
        forbidden_cmds = [re.compile(pat) for pat in agent_policy.reviewer_forbidden_commands]
        prohibition_markers = manifest.policy.prohibition_line_markers

        expected_files: set[str] = set()
        for area_name in manifest.areas:
            expected_files.add(f"{area_name}-reviewer.md")
            expected_files.add(f"{area_name}-specialist.md")

        actual_files = {f.name for f in agents_dir.iterdir() if f.is_file()}

        for extra in actual_files - expected_files:
            issues.append(
                ReviewIssue(
                    "agents",
                    f".agents/agents/{extra}",
                    1,
                    f"unexpected agent file in .agents/agents/: '{extra}'",
                )
            )

        for expected in expected_files:
            fpath = agents_dir / expected
            rel_file = str(fpath.relative_to(self.repo_root))
            if not fpath.exists():
                issues.append(
                    ReviewIssue(
                        "agents",
                        rel_file,
                        1,
                        f"missing required agent definition '{expected}'",
                    )
                )
                continue

            doc = MarkdownDocument.parse(fpath)
            stem = fpath.stem
            is_reviewer = stem.endswith("-reviewer")
            area_name = stem.rsplit("-", 1)[0]

            if doc.frontmatter.get("name") != stem:
                issues.append(
                    ReviewIssue(
                        "agents",
                        rel_file,
                        1,
                        f"frontmatter name '{doc.frontmatter.get('name')}' must match stem '{stem}'",
                    )
                )

            desc = doc.frontmatter.get("description")
            if not desc or not str(desc).strip():
                issues.append(
                    ReviewIssue(
                        "agents",
                        rel_file,
                        1,
                        "description must be non-empty",
                    )
                )

            if doc.frontmatter.get("model") not in {agent_policy.model, "inherit"}:
                issues.append(
                    ReviewIssue(
                        "agents",
                        rel_file,
                        1,
                        f"model must be '{agent_policy.model}' or 'inherit'",
                    )
                )

            if doc.frontmatter.get("mainAgent") is not False:
                issues.append(ReviewIssue("agents", rel_file, 1, "mainAgent must be false"))

            if doc.frontmatter.get("subagent") is not True:
                issues.append(ReviewIssue("agents", rel_file, 1, "subagent must be true"))

            if doc.frontmatter.get("commandExecutionPolicy") not in {"auto", "off", "sandbox"}:
                issues.append(
                    ReviewIssue(
                        "agents",
                        rel_file,
                        1,
                        "commandExecutionPolicy must be 'auto', 'off', or 'sandbox'",
                    )
                )

            raw_tools = doc.frontmatter.get("tools")
            tools_list = list(raw_tools) if isinstance(raw_tools, list) else []
            tools_set = set(tools_list)

            for t in tools_set:
                if t not in valid_tools:
                    issues.append(
                        ReviewIssue(
                            "agents",
                            rel_file,
                            1,
                            f"declared tool '{t}' is not in valid_tools",
                        )
                    )

            if is_reviewer:
                overlap = tools_set.intersection(write_tools)
                if overlap:
                    issues.append(
                        ReviewIssue(
                            "agents",
                            rel_file,
                            1,
                            f"reviewer must not declare write tools: {', '.join(overlap)}",
                        )
                    )
                missing_req = req_tools - tools_set
                if missing_req:
                    issues.append(
                        ReviewIssue(
                            "agents",
                            rel_file,
                            1,
                            f"reviewer missing required tools: {', '.join(missing_req)}",
                        )
                    )
            else:
                missing_spec = (req_tools | extra_spec) - tools_set
                if missing_spec:
                    issues.append(
                        ReviewIssue(
                            "agents",
                            rel_file,
                            1,
                            f"specialist missing required tools: {', '.join(missing_spec)}",
                        )
                    )

            raw_preloads = doc.frontmatter.get("preloadSkills")
            preloads = list(raw_preloads) if isinstance(raw_preloads, list) else []
            required_preloads = set(agent_policy.common_preload) | {f"review-{area_name}"}
            optional_preloads = (
                {area_name} if (self.repo_root / f".agents/skills/{area_name}/SKILL.md").exists() else set()
            )
            allowed_preloads = required_preloads | optional_preloads
            preload_set = set(preloads)
            if not required_preloads.issubset(preload_set) or not preload_set.issubset(allowed_preloads):
                issues.append(
                    ReviewIssue(
                        "agents",
                        rel_file,
                        1,
                        f"preloadSkills must include {sorted(required_preloads)} (and optionally {sorted(optional_preloads)})",
                    )
                )

            for p in preloads:
                skill_path = self.repo_root / f".agents/skills/{p}/SKILL.md"
                if not skill_path.exists():
                    issues.append(
                        ReviewIssue(
                            "agents",
                            rel_file,
                            1,
                            f"preloaded skill '{p}' does not exist on disk",
                        )
                    )

            body_headings = {h[1] for h in doc.headings}
            if is_reviewer:
                required_sections = {
                    "Scope",
                    "Review Process",
                    "Allowed Commands",
                    "Output",
                }
            else:
                required_sections = {
                    "Scope",
                    "Implementation Process",
                    "Verification",
                    "Flow Integration",
                }

            for sec in required_sections:
                if sec not in body_headings:
                    issues.append(
                        ReviewIssue(
                            "agents",
                            rel_file,
                            1,
                            f"missing required section '## {sec}'",
                        )
                    )

            if is_reviewer:
                in_allowed = False
                in_prohibition = False
                allowed_heading_re = re.compile(r"^#{1,2}\s+Allowed Commands\s*$")
                any_section_re = re.compile(r"^#{1,2}\s+")
                for line_no, line in enumerate(fpath.read_text(encoding="utf-8").splitlines(), start=1):
                    if allowed_heading_re.match(line):
                        in_allowed = True
                        in_prohibition = False
                        continue
                    if in_allowed and any_section_re.match(line):
                        in_allowed = False
                        in_prohibition = False
                    if in_allowed:
                        lower_line = line.lower()
                        if any(marker in lower_line for marker in prohibition_markers):
                            in_prohibition = True
                            continue
                        if not in_prohibition:
                            for forbidden_re in forbidden_cmds:
                                if forbidden_re.search(line):
                                    issues.append(
                                        ReviewIssue(
                                            "agents",
                                            rel_file,
                                            line_no,
                                            f"reviewer Allowed Commands contains forbidden command pattern: '{line.strip()}'",
                                        )
                                    )

            for target, line_no in doc.links:
                target_clean = target.split("#")[0]
                if target_clean:
                    resolved = (fpath.parent / target_clean).resolve()
                    if not resolved.exists():
                        issues.append(
                            ReviewIssue(
                                "agents",
                                rel_file,
                                line_no,
                                f"relative link '{target}' does not resolve",
                            )
                        )

        return issues

    def _check_index(self) -> list[ReviewIssue]:
        """Validate .agents/skills/index.md area table rows and links."""
        issues: list[ReviewIssue] = []
        index_path = self.repo_root / ".agents/skills/index.md"
        rel_path = str(index_path.relative_to(self.repo_root))

        if not index_path.exists():
            return [ReviewIssue("index", rel_path, 1, "skills index.md does not exist")]

        content = index_path.read_text(encoding="utf-8")
        manifest = self._manifest
        if manifest is None:
            return issues

        doc = MarkdownDocument.parse(index_path)
        for target, line_no in doc.links:
            target_clean = target.split("#")[0]
            if target_clean:
                resolved = (index_path.parent / target_clean).resolve()
                if not resolved.exists():
                    issues.append(
                        ReviewIssue(
                            "index",
                            rel_path,
                            line_no,
                            f"link target does not exist: {target}",
                        )
                    )

        for area_name in manifest.areas:
            pattern = re.compile(rf"\breview-{area_name}\b")
            if not pattern.search(content):
                issues.append(
                    ReviewIssue(
                        "index",
                        rel_path,
                        1,
                        f"skills index missing entry for area '{area_name}'",
                    )
                )

        return issues

    def _check_files(self) -> list[ReviewIssue]:
        """Validate absence of symlinks, executable bits, and gitignore status."""
        issues: list[ReviewIssue] = []
        agents_dir = self.repo_root / ".agents/agents"
        skills_dir = self.repo_root / ".agents/skills"

        for base_dir in (agents_dir, skills_dir):
            if not base_dir.exists():
                continue
            for path in base_dir.rglob("*"):
                rel_path = str(path.relative_to(self.repo_root))
                if path.is_symlink():
                    issues.append(
                        ReviewIssue(
                            "files",
                            rel_path,
                            1,
                            f"symlink forbidden: {rel_path}",
                        )
                    )
                elif path.is_file():
                    mode = path.stat().st_mode
                    if mode & 0o111:
                        issues.append(
                            ReviewIssue(
                                "files",
                                rel_path,
                                1,
                                f"file must not be executable: {rel_path}",
                            )
                        )

        if agents_dir.exists():
            gitignore_path = self.repo_root / ".gitignore"
            if gitignore_path.exists():
                gi_content = gitignore_path.read_text(encoding="utf-8")
                if "!.agents/agents/" not in gi_content:
                    issues.append(
                        ReviewIssue(
                            "files",
                            ".gitignore",
                            1,
                            ".gitignore missing allow-list entry '!.agents/agents/'",
                        )
                    )

        return issues

    def _check_bundles(self) -> list[ReviewIssue]:
        """Validate OKF v0.2 frontmatter in knowledge bundle and absence of completed specs."""
        issues: list[ReviewIssue] = []
        bundles_root = self.repo_root / ".agents/bundles"
        if not bundles_root.exists():
            return issues

        index_file = bundles_root / "index.md"
        if index_file.exists():
            doc = MarkdownDocument.parse(index_file)
            if doc.frontmatter.get("okf_version") != "0.2":
                issues.append(
                    ReviewIssue(
                        "bundles",
                        str(index_file.relative_to(self.repo_root)),
                        1,
                        "bundles root index.md missing 'okf_version: \"0.2\"'",
                    )
                )

        specs_dir = bundles_root / "specs"
        if specs_dir.exists():
            for spec_file in specs_dir.rglob("spec.md"):
                doc = MarkdownDocument.parse(spec_file)
                state_val = doc.frontmatter.get("state") or doc.frontmatter.get("status")
                if state_val == "completed":
                    issues.append(
                        ReviewIssue(
                            "bundles",
                            str(spec_file.relative_to(self.repo_root)),
                            1,
                            "resident spec has 'state: completed' (specs must be archived on completion)",
                        )
                    )

        tag_slug_re = re.compile(r"^[a-z0-9-]+$")
        reserved_names = {"index.md", "log.md"}

        for subdir_name in ("knowledge", "product"):
            target_dir = bundles_root / subdir_name
            if not target_dir.exists():
                continue
            for md_path in target_dir.rglob("*.md"):
                if md_path.name in reserved_names:
                    continue
                rel_file = str(md_path.relative_to(self.repo_root))
                doc = MarkdownDocument.parse(md_path)

                doc_type = doc.frontmatter.get("type")
                if not doc_type or not str(doc_type).strip():
                    issues.append(ReviewIssue("bundles", rel_file, 1, "missing frontmatter 'type'"))

                title = doc.frontmatter.get("title")
                if not title or not str(title).strip():
                    issues.append(ReviewIssue("bundles", rel_file, 1, "missing frontmatter 'title'"))

                desc = doc.frontmatter.get("description")
                if not desc or not str(desc).strip():
                    issues.append(ReviewIssue("bundles", rel_file, 1, "missing frontmatter 'description'"))

                status = doc.frontmatter.get("status")
                if status not in ("draft", "stable", "deprecated"):
                    issues.append(
                        ReviewIssue(
                            "bundles",
                            rel_file,
                            1,
                            f"invalid status '{status}', must be draft, stable, or deprecated",
                        )
                    )

                tags = doc.frontmatter.get("tags")
                if isinstance(tags, list):
                    for tag in tags:
                        if not tag_slug_re.match(str(tag)):
                            issues.append(
                                ReviewIssue(
                                    "bundles",
                                    rel_file,
                                    1,
                                    f"tag '{tag}' must be a lowercase slug",
                                )
                            )

                sources = doc.frontmatter.get("sources")
                if isinstance(sources, list):
                    for src_entry in sources:
                        res_val: str | None = None
                        if isinstance(src_entry, dict):
                            raw_res = src_entry.get("resource")
                            if isinstance(raw_res, str):
                                res_val = raw_res
                        elif isinstance(src_entry, str) and src_entry.startswith("resource:"):
                            res_val = src_entry.split(":", 1)[1].strip()
                        if res_val:
                            file_part = res_val.split("::", 1)[0].strip()
                            if file_part and not (self.repo_root / file_part).exists():
                                issues.append(
                                    ReviewIssue(
                                        "bundles",
                                        rel_file,
                                        1,
                                        f"source resource '{res_val}' does not exist",
                                    )
                                )

                for target, line_no in doc.links:
                    target_clean = target.split("#")[0]
                    if target_clean and target_clean.endswith(".md"):
                        resolved = (md_path.parent / target_clean).resolve()
                        if not resolved.exists():
                            issues.append(
                                ReviewIssue(
                                    "bundles",
                                    rel_file,
                                    line_no,
                                    f"relative link '{target}' does not resolve",
                                )
                            )

        return issues

    def _check_privacy(self) -> list[ReviewIssue]:
        """Scan documentation and skills for forbidden patterns and confidential terms."""
        issues: list[ReviewIssue] = []
        manifest = self._manifest
        policy = manifest.policy if manifest else ValidationPolicy()

        patterns: list[tuple[re.Pattern[str], str]] = [(re.compile(p), p) for p in policy.forbidden_patterns]
        prohibition_markers = policy.prohibition_line_markers

        private_terms: list[str] = []
        env_val = os.environ.get(policy.forbidden_terms_env, "")
        if env_val:
            private_terms.extend(t.strip() for t in env_val.split(",") if t.strip())

        terms_file = self.repo_root / policy.forbidden_terms_file
        if terms_file.exists():
            for line in terms_file.read_text(encoding="utf-8").splitlines():
                t = line.strip()
                if t and not t.startswith("#"):
                    private_terms.append(t)

        term_regexes: list[tuple[int, re.Pattern[str]]] = []
        for idx, term in enumerate(private_terms, start=1):
            esc = re.escape(term)
            term_regexes.append((idx, re.compile(rf"\b{esc}\b", re.IGNORECASE)))

        scan_files: set[Path] = set()
        globs = policy.privacy_scan or [
            "AGENTS.md",
            ".agents/skills/**/*.md",
            ".agents/agents/*.md",
            ".agents/bundles/**/*.md",
            "docs/**/*.md",
        ]

        for g in globs:
            cleaned = _strip_leading_dot_slash(g)
            if "**" in cleaned:
                prefix, suffix = cleaned.split("**", 1)
                base = self.repo_root / prefix.rstrip("/")
                pat = suffix.lstrip("/") or "*"
                if base.exists():
                    scan_files.update(base.rglob(pat))
            elif "*" in cleaned:
                scan_files.update(self.repo_root.glob(cleaned))
            else:
                p = self.repo_root / cleaned
                if p.exists():
                    scan_files.add(p)

        for fpath in scan_files:
            if not fpath.is_file():
                continue
            rel_file = str(fpath.relative_to(self.repo_root))
            content = fpath.read_text(encoding="utf-8", errors="replace")
            for line_no, line in enumerate(content.splitlines(), start=1):
                lower = line.lower()
                has_prohibition = any(m in lower for m in prohibition_markers)

                if not has_prohibition:
                    for regex, pat_str in patterns:
                        if regex.search(line):
                            issues.append(
                                ReviewIssue(
                                    "privacy",
                                    rel_file,
                                    line_no,
                                    f"line matches forbidden pattern: {pat_str}",
                                )
                            )

                for term_idx, term_re in term_regexes:
                    if term_re.search(line):
                        issues.append(
                            ReviewIssue(
                                "privacy",
                                rel_file,
                                line_no,
                                f"private term #{term_idx}",
                            )
                        )

        return issues

    def _check_python_imports(self) -> list[ReviewIssue]:
        """Validate PY-07 domain naming rules and PY-08 typing/module_loader import rules."""
        issues: list[ReviewIssue] = []
        py_root = self.repo_root / "src/py"
        if not py_root.exists():
            return issues

        allowed_private_typing = {
            "src/py/vertebra/typing.py",
            "src/py/tests/unit/framework/test_typing.py",
        }
        allowed_import_module = {
            "src/py/vertebra/utils/module_loader.py",
            "src/py/vertebra/build/packaging/_onefile_extract.py",
        }
        allowed_vertebra_classes = {
            "VertebraAppConfig",
            "VertebraConfig",
            "VertebraEnv",
            "VertebraError",
            "VertebraExtensionGroup",
            "VertebraGroup",
            "VertebraPlugin",
        }

        py_files = sorted(list(py_root.rglob("*.py")) + list(py_root.rglob("*.pyi")))
        for py_file in py_files:
            rel_file = py_file.relative_to(self.repo_root).as_posix()
            try:
                source = py_file.read_text(encoding="utf-8")
                tree = ast.parse(source, filename=rel_file)
            except SyntaxError:
                continue

            in_vertebra_pkg = rel_file.startswith("src/py/vertebra/")
            if in_vertebra_pkg:
                for stmt in tree.body:
                    if (
                        isinstance(stmt, ast.Assign)
                        and len(stmt.targets) == 1
                        and isinstance(stmt.targets[0], ast.Name)
                        and isinstance(stmt.value, ast.Name)
                    ):
                        alias_name = stmt.targets[0].id
                        if alias_name.startswith(("Vertebra", "Native")) or "Vtb" in alias_name:
                            issues.append(
                                ReviewIssue(
                                    "python_imports",
                                    rel_file,
                                    stmt.lineno,
                                    f"top-level alias '{alias_name} = {stmt.value.id}' violates domain naming rules (PY-07)",
                                )
                            )

            for node in ast.walk(tree):
                if in_vertebra_pkg and isinstance(node, ast.ClassDef):
                    cls_name = node.name
                    if cls_name.startswith("Vertebra") and cls_name not in allowed_vertebra_classes:
                        issues.append(
                            ReviewIssue(
                                "python_imports",
                                rel_file,
                                node.lineno,
                                f"class '{cls_name}' uses forbidden 'Vertebra*' prefix; use a clean domain name (PY-07)",
                            )
                        )
                    elif "Vtb" in cls_name:
                        issues.append(
                            ReviewIssue(
                                "python_imports",
                                rel_file,
                                node.lineno,
                                f"class '{cls_name}' uses forbidden '*Vtb*' token; use a clean domain name (PY-07)",
                            )
                        )
                    elif cls_name.startswith("Native"):
                        issues.append(
                            ReviewIssue(
                                "python_imports",
                                rel_file,
                                node.lineno,
                                f"class '{cls_name}' uses forbidden 'Native*' implementation prefix; use a clean domain name (PY-07)",
                            )
                        )
                elif isinstance(node, ast.Import):
                    for alias in node.names:
                        if alias.name == "vertebra._typing" and rel_file not in allowed_private_typing:
                            issues.append(
                                ReviewIssue(
                                    "python_imports",
                                    rel_file,
                                    node.lineno,
                                    "import from 'vertebra.typing' instead of private 'vertebra._typing' (PY-08)",
                                )
                            )
                elif isinstance(node, ast.ImportFrom):
                    mod_name = node.module or ""
                    if (
                        mod_name == "vertebra._typing" or (in_vertebra_pkg and node.level > 0 and mod_name == "_typing")
                    ) and rel_file not in allowed_private_typing:
                        issues.append(
                            ReviewIssue(
                                "python_imports",
                                rel_file,
                                node.lineno,
                                "import from 'vertebra.typing' instead of private 'vertebra._typing' (PY-08)",
                            )
                        )
                    if (
                        in_vertebra_pkg
                        and mod_name == "importlib"
                        and any(a.name == "import_module" for a in node.names)
                        and rel_file not in allowed_import_module
                    ):
                        issues.append(
                            ReviewIssue(
                                "python_imports",
                                rel_file,
                                node.lineno,
                                "use vertebra.utils.module_loader.{import_optional,ensure_module} instead of importlib.import_module (PY-08)",
                            )
                        )
                elif (
                    in_vertebra_pkg
                    and isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Attribute)
                    and node.func.attr == "import_module"
                    and isinstance(node.func.value, ast.Name)
                    and node.func.value.id == "importlib"
                    and rel_file not in allowed_import_module
                ):
                    issues.append(
                        ReviewIssue(
                            "python_imports",
                            rel_file,
                            node.lineno,
                            "use vertebra.utils.module_loader.{import_optional,ensure_module} instead of importlib.import_module (PY-08)",
                        )
                    )

        return issues

    def _get_tracked_files(self) -> list[str]:
        """Collect tracked source files under repo root."""
        try:
            import subprocess

            res = subprocess.run(
                ["git", "ls-files"],
                cwd=self.repo_root,
                capture_output=True,
                text=True,
                check=False,
            )
            if res.returncode == 0 and res.stdout.strip():
                return [line.strip() for line in res.stdout.splitlines() if line.strip()]
        except Exception:
            pass

        ignored_parts = {
            "target",
            "node_modules",
            ".venv",
            "__pycache__",
            ".git",
            "build",
            "dist",
        }
        src_files: list[str] = []
        for base in ("src", ".agents", "tools", "docs"):
            p = self.repo_root / base
            if p.exists():
                for f in p.rglob("*"):
                    if f.is_file() and not any(part in f.parts for part in ignored_parts):
                        src_files.append(str(f.relative_to(self.repo_root)))
        return src_files

    def _get_checklist_ids_and_retired(self) -> tuple[set[str], set[str]]:
        """Extract checklist IDs and retired IDs from checklist.md."""
        chk_path = self.repo_root / ".agents/skills/review-core/references/checklist.md"
        if not chk_path.exists():
            return set(), set()
        active_ids: set[str] = set()
        retired_ids: set[str] = set()

        item_re = re.compile(r"^-\s+\*\*(?P<id>[A-Z]{2,5}-\d{2})\*\*:")
        retired_re = re.compile(r"^-\s+~~\*\*(?P<id>[A-Z]{2,5}-\d{2})\*\*~~:")

        for line in chk_path.read_text(encoding="utf-8").splitlines():
            m_act = item_re.match(line)
            if m_act:
                active_ids.add(m_act.group("id"))
                continue
            m_ret = retired_re.match(line)
            if m_ret:
                retired_ids.add(m_ret.group("id"))

        return active_ids, retired_ids

    def _get_suppression_ids(self) -> set[str]:
        """Extract suppression IDs from false-positive-suppressions.md."""
        fp_path = self.repo_root / ".agents/skills/review-core/references/false-positive-suppressions.md"
        if not fp_path.exists():
            return set()
        fps: set[str] = set()
        fp_re = re.compile(r"^###\s+(?P<id>FP-[A-Z]{2,5}-\d{2}):")
        for line in fp_path.read_text(encoding="utf-8").splitlines():
            m = fp_re.match(line)
            if m:
                fps.add(m.group("id"))
        return fps


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entrypoint supporting validate, route, and ledger commands."""
    args = list(argv if argv is not None else sys.argv[1:])
    if not args or args[0] in ("-h", "--help"):
        sys.stdout.write("Usage: python -m tools.scripts.review_setup <validate|route|ledger> [options]\n")
        return 0

    subcommand = args[0]
    repo_root = Path.cwd()

    if subcommand == "validate":
        rules: frozenset[str] | None = None
        idx = 1
        while idx < len(args):
            if args[idx] == "--rules" and idx + 1 < len(args):
                rules = frozenset(args[idx + 1].split(","))
                idx += 2
            elif args[idx] == "--repo-root" and idx + 1 < len(args):
                repo_root = Path(args[idx + 1]).resolve()
                idx += 2
            else:
                idx += 1

        validator = ReviewSetupValidator(repo_root)
        issues = validator.validate(rules)
        for issue in issues:
            sys.stderr.write(issue.format() + "\n")
        return 1 if issues else 0

    if subcommand == "route":
        manifest_path = repo_root / ".agents/skills/review-core/references/areas.toml"
        if not manifest_path.exists():
            sys.stderr.write(f"ERROR: manifest not found at {manifest_path}\n")
            return 1
        manifest = ReviewManifest.from_toml(manifest_path)
        router = ReviewRouter(manifest, repo_root)

        paths_to_route: list[str] = []
        if "--files" in args:
            for line in sys.stdin:
                stripped = line.strip()
                if stripped:
                    paths_to_route.append(stripped)
        else:
            base_ref = "origin/main"
            idx = 1
            while idx < len(args):
                if args[idx] == "--base" and idx + 1 < len(args):
                    base_ref = args[idx + 1]
                    idx += 2
                else:
                    idx += 1
            import subprocess

            res = subprocess.run(
                ["git", "diff", "--name-only", base_ref],
                capture_output=True,
                text=True,
                check=False,
            )
            paths_to_route = [p.strip() for p in res.stdout.splitlines() if p.strip()]

        routed = router.route(paths_to_route)
        for key, files in sorted(routed.items()):
            sys.stdout.write(f"[{key}]\n")
            for f in sorted(files):
                sys.stdout.write(f"  {f}\n")
        return 0

    if subcommand == "ledger":
        if len(args) < 2:
            sys.stderr.write("Usage: python -m tools.scripts.review_setup ledger <path>\n")
            return 1
        ledger_path = Path(args[1]).resolve()
        manifest_path = repo_root / ".agents/skills/review-core/references/areas.toml"
        manifest_obj: ReviewManifest | None = (
            ReviewManifest.from_toml(manifest_path) if manifest_path.exists() else None
        )
        ledger_validator = LedgerValidator(repo_root, manifest_obj)
        issues = ledger_validator.validate_file(ledger_path)
        for issue in issues:
            sys.stderr.write(issue.format() + "\n")
        return 1 if issues else 0

    sys.stderr.write(f"Unknown subcommand: {subcommand}\n")
    return 1


if __name__ == "__main__":
    sys.exit(main())
