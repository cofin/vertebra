"""Application-agnostic Open Knowledge Format (OKF v0.2) bundle serializer.

Provides pure-Python canonical YAML frontmatter parsing and emission,
fence-aware Markdown truth-block and footnote extraction, OKF v0.2 validation,
canonical SHA-256 content digests, and safe directory and reproducible ZIP
archive import/export with zero external YAML dependencies.
"""

from __future__ import annotations

import hashlib
import io
import os
import re
import stat
import zipfile
from collections.abc import Iterator, Mapping
from pathlib import Path
from typing import IO, Any, Final

import msgspec

from vertebra.agent._yaml import OkfValidationError, _dump_yaml_frontmatter, _parse_yaml_frontmatter
from vertebra.utils.serializers import encode_json_bytes

__all__ = (
    "DEFAULT_MAX_TRUTH_LINES",
    "MAX_OKF_ENTRY_BYTES",
    "OKF_RESERVED_FILENAMES",
    "VALID_OKF_STATUSES",
    "OkfBundle",
    "OkfDocument",
    "OkfSource",
    "OkfValidationError",
)

MAX_OKF_ENTRY_BYTES: Final[int] = 120_832
DEFAULT_MAX_TRUTH_LINES: Final[int] = 40
OKF_RESERVED_FILENAMES: Final[frozenset[str]] = frozenset({"index.md", "log.md"})
VALID_OKF_STATUSES: Final[frozenset[str]] = frozenset({"draft", "stable", "deprecated"})

_REPRODUCIBLE_ZIP_DATE: Final[tuple[int, int, int, int, int, int]] = (1980, 1, 1, 0, 0, 0)
_ZIP_FILE_ATTR: Final[int] = 0o644 << 16
_STREAM_CHUNK_BYTES: Final[int] = 16_384

_DRIVE_PREFIX_RE: Final[re.Pattern[str]] = re.compile(r"^[A-Za-z]:")
_FOOTNOTE_DEF_RE: Final[re.Pattern[str]] = re.compile(r"^\[\^([^\]]+)\]:\s*(.*)$")
_FOOTNOTE_REF_RE: Final[re.Pattern[str]] = re.compile(r"\[\^([^\]]+)\](?!:)")
_INLINE_CODE_RE: Final[re.Pattern[str]] = re.compile(r"`+[^`]*`+")


class OkfSource(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Provenance source descriptor declared in OKF v0.2 frontmatter."""

    id: str
    resource: str
    title: str = ""


class OkfDocument(msgspec.Struct, frozen=True, kw_only=True):
    """Parsed and validated OKF v0.2 Markdown document with canonical digest."""

    rel_path: str
    frontmatter: dict[str, Any] = msgspec.field(default_factory=dict)
    body: str = ""
    truth_lines: tuple[str, ...] = ()
    sources: tuple[OkfSource, ...] = ()
    footnotes: dict[str, str] = msgspec.field(default_factory=dict)
    sha256: str = ""

    @classmethod
    def parse(
        cls,
        rel_path: str,
        content: str | bytes,
        *,
        max_truth_lines: int = DEFAULT_MAX_TRUTH_LINES,
    ) -> OkfDocument:
        """Parse, normalize, validate, and digest a single OKF v0.2 Markdown document."""
        normalized_path = _normalize_rel_path(rel_path)
        text = cls._decode_utf8_text(content, rel_path=normalized_path)
        normalized_text = cls._normalize_line_endings(text)

        frontmatter, body, has_frontmatter = cls._split_frontmatter(normalized_text, rel_path=normalized_path)
        truth_lines, footnotes, inline_refs = cls._scan_markdown_body(body, rel_path=normalized_path)
        sources = cls._parse_sources(frontmatter, rel_path=normalized_path)

        cls._validate_okf_document(
            rel_path=normalized_path,
            frontmatter=frontmatter,
            has_frontmatter=has_frontmatter,
            truth_lines=truth_lines,
            sources=sources,
            footnotes=footnotes,
            inline_refs=inline_refs,
            max_truth_lines=max_truth_lines,
        )

        canonical_text = cls._render_markdown(frontmatter, body)
        digest = hashlib.sha256(canonical_text.encode("utf-8")).hexdigest()

        return cls(
            rel_path=normalized_path,
            frontmatter=frontmatter,
            body=body,
            truth_lines=truth_lines,
            sources=sources,
            footnotes=footnotes,
            sha256=digest,
        )

    def to_markdown(self) -> str:
        """Render the document as canonical UTF-8 Markdown with normalized line endings."""
        return self._render_markdown(self.frontmatter, self.body)

    def to_bytes(self) -> bytes:
        """Encode the canonical Markdown representation as UTF-8 bytes."""
        return self.to_markdown().encode("utf-8")

    @staticmethod
    def _decode_utf8_text(content: str | bytes, *, rel_path: str) -> str:
        if isinstance(content, str):
            raw_bytes = content.encode("utf-8")
            if len(raw_bytes) > MAX_OKF_ENTRY_BYTES:
                raise _entry_too_large_error(rel_path, len(raw_bytes), MAX_OKF_ENTRY_BYTES)
            return content
        if len(content) > MAX_OKF_ENTRY_BYTES:
            raise _entry_too_large_error(rel_path, len(content), MAX_OKF_ENTRY_BYTES)
        try:
            return content.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise OkfValidationError(f"OKF entry '{rel_path}' is not valid UTF-8: {exc}") from exc

    @staticmethod
    def _normalize_line_endings(text: str) -> str:
        normalized = text.replace("\r\n", "\n").replace("\r", "\n")
        if normalized.startswith("\ufeff"):
            return normalized[1:]
        return normalized

    @staticmethod
    def _split_frontmatter(text: str, *, rel_path: str) -> tuple[dict[str, Any], str, bool]:
        lines = text.split("\n")
        if not lines or lines[0].strip() != "---":
            return {}, text, False

        close_idx: int | None = None
        for idx in range(1, len(lines)):
            if lines[idx].strip() == "---":
                close_idx = idx
                break
        if close_idx is None:
            raise OkfValidationError(f"Unterminated YAML frontmatter in '{rel_path}'")

        yaml_lines = lines[1:close_idx]
        frontmatter = _parse_yaml_frontmatter(yaml_lines, rel_path=rel_path)
        body = "\n".join(lines[close_idx + 1 :])
        return frontmatter, body, True

    @staticmethod
    def _render_markdown(frontmatter: Mapping[str, Any], body: str) -> str:
        if not frontmatter:
            return body
        yaml_text = _dump_yaml_frontmatter(frontmatter)
        return f"---\n{yaml_text}\n---\n{body}"

    @classmethod
    def _scan_markdown_body(
        cls,
        body: str,
        *,
        rel_path: str,
    ) -> tuple[tuple[str, ...], dict[str, str], set[str]]:
        truth_lines: list[str] = []
        footnotes: dict[str, str] = {}
        inline_refs: set[str] = set()

        in_fence = False
        fence_char = ""
        fence_len = 0
        in_truth = False

        for line_no, line in enumerate(body.split("\n"), start=1):
            stripped = line.strip()
            if not in_fence:
                opened = cls._fence_opening(stripped)
                if opened is not None:
                    in_fence = True
                    fence_char, fence_len = opened
                    if in_truth:
                        truth_lines.append(line)
                    continue
            else:
                if cls._is_fence_closing(stripped, fence_char, fence_len):
                    in_fence = False
                    fence_char = ""
                    fence_len = 0
                if in_truth:
                    truth_lines.append(line)
                continue

            if stripped == "---":
                raise OkfValidationError(
                    f"Standalone '---' horizontal rule is forbidden in bundle markdown body ({rel_path}:{line_no})"
                )

            if stripped == "<!-- truth: start -->":
                if in_truth:
                    raise OkfValidationError(f"Nested truth block start marker in '{rel_path}:{line_no}'")
                in_truth = True
                continue
            if stripped == "<!-- truth: end -->":
                if not in_truth:
                    raise OkfValidationError(f"Unexpected truth block end marker in '{rel_path}:{line_no}'")
                in_truth = False
                continue
            if in_truth:
                truth_lines.append(line)

            fn_match = _FOOTNOTE_DEF_RE.match(line)
            if fn_match is not None:
                fn_id = fn_match.group(1).strip()
                fn_text = fn_match.group(2).strip()
                if not fn_id:
                    raise OkfValidationError(f"Empty footnote identifier in '{rel_path}:{line_no}'")
                if fn_id in footnotes:
                    raise OkfValidationError(f"Duplicate footnote definition '[^{fn_id}]:' in '{rel_path}:{line_no}'")
                footnotes[fn_id] = fn_text
                continue

            scrubbed_line = _INLINE_CODE_RE.sub("", line)
            for ref_match in _FOOTNOTE_REF_RE.finditer(scrubbed_line):
                inline_refs.add(ref_match.group(1).strip())

        if in_truth:
            raise OkfValidationError(f"Unterminated '<!-- truth: start -->' block in '{rel_path}'")

        return tuple(truth_lines), footnotes, inline_refs

    @staticmethod
    def _parse_sources(frontmatter: Mapping[str, Any], *, rel_path: str) -> tuple[OkfSource, ...]:
        if "sources" not in frontmatter or frontmatter["sources"] is None:
            return ()
        raw_sources = frontmatter["sources"]
        if not isinstance(raw_sources, list):
            raise OkfValidationError(f"Frontmatter 'sources' must be a list in '{rel_path}'")

        parsed: list[OkfSource] = []
        seen_ids: set[str] = set()
        for item in raw_sources:
            if not isinstance(item, Mapping):
                raise OkfValidationError(f"Each entry in 'sources' must be a mapping in '{rel_path}'")
            src_id = item.get("id")
            resource = item.get("resource")
            title = item.get("title", "")
            if not isinstance(src_id, str) or not src_id.strip():
                raise OkfValidationError(f"Source entry missing non-empty string 'id' in '{rel_path}'")
            if not isinstance(resource, str) or not resource.strip():
                raise OkfValidationError(f"Source '{src_id}' missing non-empty string 'resource' in '{rel_path}'")
            if title is None:
                title = ""
            if not isinstance(title, str):
                raise OkfValidationError(f"Source '{src_id}' 'title' must be a string in '{rel_path}'")
            if src_id in seen_ids:
                raise OkfValidationError(f"Duplicate source id {src_id!r} in '{rel_path}'")
            seen_ids.add(src_id)
            parsed.append(OkfSource(id=src_id, resource=resource, title=title))
        return tuple(parsed)

    @staticmethod
    def _validate_okf_document(
        *,
        rel_path: str,
        frontmatter: Mapping[str, Any],
        has_frontmatter: bool,
        truth_lines: tuple[str, ...],
        sources: tuple[OkfSource, ...],
        footnotes: Mapping[str, str],
        inline_refs: set[str],
        max_truth_lines: int,
    ) -> None:
        if len(truth_lines) > max_truth_lines:
            raise OkfValidationError(
                f"Truth block line count ({len(truth_lines)}) exceeds max_truth_lines ({max_truth_lines}) in '{rel_path}'"
            )

        filename = rel_path.rsplit("/", 1)[-1]
        if rel_path == "index.md":
            if frontmatter.get("okf_version") != "0.2":
                raise OkfValidationError("Root 'index.md' must declare frontmatter okf_version == '0.2'")
            return

        if filename in OKF_RESERVED_FILENAMES:
            if has_frontmatter:
                raise OkfValidationError(f"Reserved OKF file '{rel_path}' must not carry YAML frontmatter")
            return

        is_content_doc = rel_path.startswith(("product/", "knowledge/"))
        if is_content_doc:
            if not has_frontmatter:
                raise OkfValidationError(f"Content document '{rel_path}' must include YAML frontmatter")
            status = frontmatter.get("status")
            if not isinstance(status, str) or status not in VALID_OKF_STATUSES:
                raise OkfValidationError(
                    f"Invalid OKF status {status!r} in '{rel_path}'; expected one of {sorted(VALID_OKF_STATUSES)}"
                )

        source_ids = {source.id for source in sources}
        footnote_ids = set(footnotes.keys())

        if is_content_doc or source_ids or footnote_ids:
            missing_footnotes = sorted(source_ids - footnote_ids)
            if missing_footnotes:
                raise OkfValidationError(f"Sources missing footnote definitions in '{rel_path}': {missing_footnotes}")
            undeclared_footnotes = sorted(footnote_ids - source_ids)
            if undeclared_footnotes:
                raise OkfValidationError(
                    f"Footnote definitions missing from frontmatter sources in '{rel_path}': {undeclared_footnotes}"
                )
            dangling_refs = sorted(inline_refs - footnote_ids)
            if dangling_refs:
                raise OkfValidationError(f"Dangling footnote references in '{rel_path}': {dangling_refs}")

    @staticmethod
    def _fence_opening(stripped: str) -> tuple[str, int] | None:
        for fence_char in ("`", "~"):
            if stripped.startswith(fence_char * 3):
                count = 0
                for ch in stripped:
                    if ch == fence_char:
                        count += 1
                    else:
                        break
                if fence_char == "`" and "`" in stripped[count:]:
                    return None
                return fence_char, count
        return None

    @staticmethod
    def _is_fence_closing(stripped: str, fence_char: str, fence_len: int) -> bool:
        if not stripped or len(stripped) < fence_len:
            return False
        return all(ch == fence_char for ch in stripped)


class OkfBundle(msgspec.Struct, frozen=True, kw_only=True):
    """Collection of validated OKF v0.2 documents with directory and ZIP serialization."""

    documents: tuple[OkfDocument, ...] = ()

    def __post_init__(self) -> None:
        seen: set[str] = set()
        for doc in self.documents:
            normalized = _normalize_rel_path(doc.rel_path)
            if normalized != doc.rel_path:
                raise OkfValidationError(f"Non-canonical document path: {doc.rel_path!r}")
            if normalized in seen:
                raise OkfValidationError(f"Duplicate document path in bundle: {normalized!r}")
            seen.add(normalized)

    def __len__(self) -> int:
        return len(self.documents)

    def __iter__(self) -> Iterator[OkfDocument]:
        return iter(self.documents)

    def get(self, rel_path: str) -> OkfDocument | None:
        """Return the document matching a relative POSIX path, or ``None``."""
        normalized = _normalize_rel_path(rel_path)
        for doc in self.documents:
            if doc.rel_path == normalized:
                return doc
        return None

    def digest(self) -> str:
        """Return the deterministic 64-char SHA-256 digest across sorted bundle entries."""
        manifest = [[doc.rel_path, doc.sha256] for doc in sorted(self.documents, key=lambda item: item.rel_path)]
        return hashlib.sha256(encode_json_bytes(manifest)).hexdigest()

    @classmethod
    def from_directory(
        cls,
        root: str | Path,
        *,
        exclude_specs: bool = True,
        exclude_log: bool = True,
        max_truth_lines: int = DEFAULT_MAX_TRUTH_LINES,
    ) -> OkfBundle:
        """Load and validate an OKF v0.2 bundle from a filesystem directory.

        Every loaded Markdown entry, including a reserved `log.md` loaded with `exclude_log=False`,
        must fit `MAX_OKF_ENTRY_BYTES`; the error names the entry, its size, and the limit.
        """
        root_path = Path(root)
        if root_path.is_symlink():
            raise OkfValidationError(f"OKF bundle root must not be a symlink: {root_path}")
        if not root_path.is_dir():
            raise OkfValidationError(f"OKF bundle root is not a directory: {root_path}")

        resolved_root = root_path.resolve()
        loaded: list[OkfDocument] = []
        cls._collect_directory_documents(
            current_dir=root_path,
            root_path=root_path,
            resolved_root=resolved_root,
            exclude_specs=exclude_specs,
            exclude_log=exclude_log,
            max_truth_lines=max_truth_lines,
            out=loaded,
        )
        loaded.sort(key=lambda doc: doc.rel_path)
        return cls(documents=tuple(loaded))

    def export_directory(self, root: str | Path) -> tuple[Path, ...]:
        """Write canonical bundle documents to a directory tree with safe relative paths."""
        root_path = Path(root)
        if root_path.is_symlink():
            raise OkfValidationError(f"OKF export target must not be a symlink: {root_path}")
        root_path.mkdir(parents=True, exist_ok=True)
        resolved_root = root_path.resolve()

        written: list[Path] = []
        for doc in sorted(self.documents, key=lambda item: item.rel_path):
            rel_path = _normalize_rel_path(doc.rel_path)
            target = root_path.joinpath(*rel_path.split("/"))
            self._ensure_safe_export_target(target, root_path=root_path, resolved_root=resolved_root)
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(doc.to_bytes())
            written.append(target)
        return tuple(written)

    @classmethod
    def from_zip(
        cls,
        source: str | Path | bytes | bytearray | memoryview | IO[bytes],
        *,
        exclude_specs: bool = False,
        exclude_log: bool = False,
        max_truth_lines: int = DEFAULT_MAX_TRUTH_LINES,
    ) -> OkfBundle:
        """Load and validate an OKF v0.2 bundle from a ZIP archive path, buffer, or stream.

        Symlink, path-safety, and duplicate-path checks apply to every archive member. Members that
        are not Markdown, or that `exclude_specs` / `exclude_log` filter out, are skipped before the
        `MAX_OKF_ENTRY_BYTES` check and are never decompressed.
        """
        loaded: list[OkfDocument] = []
        seen_paths: set[str] = set()

        try:
            with cls._open_zip_reader(source) as archive:
                for info in archive.infolist():
                    if stat.S_ISLNK(info.external_attr >> 16):
                        raise OkfValidationError(f"ZIP entry must not be a symlink: {info.filename!r}")
                    if info.is_dir():
                        dir_name = info.filename[:-1] if info.filename.endswith("/") else info.filename
                        if not dir_name and info.filename == "/":
                            raise OkfValidationError(f"Absolute ZIP directory entry is not allowed: {info.filename!r}")
                        if dir_name:
                            _normalize_rel_path(dir_name)
                        continue

                    rel_path = _normalize_rel_path(info.filename)
                    if rel_path in seen_paths:
                        raise OkfValidationError(f"Duplicate ZIP entry path: {rel_path!r}")
                    seen_paths.add(rel_path)

                    if exclude_specs and cls._is_specs_path(rel_path):
                        continue
                    if exclude_log and cls._is_log_path(rel_path):
                        continue
                    if not rel_path.endswith(".md"):
                        continue

                    if info.file_size > MAX_OKF_ENTRY_BYTES:
                        raise _entry_too_large_error(rel_path, info.file_size, MAX_OKF_ENTRY_BYTES)

                    with archive.open(info, "r") as member_stream:
                        raw_bytes = cls._read_stream_bounded(
                            member_stream,
                            rel_path=rel_path,
                            max_bytes=MAX_OKF_ENTRY_BYTES,
                        )

                    loaded.append(
                        OkfDocument.parse(
                            rel_path,
                            raw_bytes,
                            max_truth_lines=max_truth_lines,
                        )
                    )
        except zipfile.BadZipFile as exc:
            raise OkfValidationError(f"Invalid OKF ZIP archive: {exc}") from exc

        loaded.sort(key=lambda doc: doc.rel_path)
        return cls(documents=tuple(loaded))

    def to_zip_bytes(self) -> bytes:
        """Serialize the bundle into a reproducible DEFLATE-compressed ZIP byte buffer."""
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as archive:
            for doc in sorted(self.documents, key=lambda item: item.rel_path):
                rel_path = _normalize_rel_path(doc.rel_path)
                info = zipfile.ZipInfo(filename=rel_path, date_time=_REPRODUCIBLE_ZIP_DATE)
                info.compress_type = zipfile.ZIP_DEFLATED
                info.create_system = 3
                info.external_attr = _ZIP_FILE_ATTR
                archive.writestr(info, doc.to_bytes())
        return buffer.getvalue()

    def export_zip(self, destination: str | Path) -> Path:
        """Write the reproducible ZIP archive to ``destination`` and return the path."""
        dest_path = Path(destination)
        if dest_path.is_symlink():
            raise OkfValidationError(f"OKF ZIP export destination must not be a symlink: {dest_path}")
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        dest_path.write_bytes(self.to_zip_bytes())
        return dest_path

    @classmethod
    def _collect_directory_documents(
        cls,
        *,
        current_dir: Path,
        root_path: Path,
        resolved_root: Path,
        exclude_specs: bool,
        exclude_log: bool,
        max_truth_lines: int,
        out: list[OkfDocument],
    ) -> None:
        entries = sorted(current_dir.iterdir(), key=lambda item: item.name)
        for entry in entries:
            if entry.is_symlink():
                raise OkfValidationError(f"Symlink is not allowed in OKF bundle: {entry}")
            rel_posix = entry.relative_to(root_path).as_posix()
            if entry.is_dir():
                if exclude_specs and cls._is_specs_path(rel_posix):
                    continue
                cls._collect_directory_documents(
                    current_dir=entry,
                    root_path=root_path,
                    resolved_root=resolved_root,
                    exclude_specs=exclude_specs,
                    exclude_log=exclude_log,
                    max_truth_lines=max_truth_lines,
                    out=out,
                )
                continue

            if not entry.is_file():
                continue
            if not entry.resolve().is_relative_to(resolved_root):
                raise OkfValidationError(f"Entry escapes OKF bundle root: {entry}")
            if exclude_specs and cls._is_specs_path(rel_posix):
                continue
            if exclude_log and cls._is_log_path(rel_posix):
                continue
            if not rel_posix.endswith(".md"):
                continue

            rel_path = _normalize_rel_path(rel_posix)
            with cls._open_file_nofollow(entry) as handle:
                on_disk_bytes = os.fstat(handle.fileno()).st_size
                if on_disk_bytes > MAX_OKF_ENTRY_BYTES:
                    raise _entry_too_large_error(rel_path, on_disk_bytes, MAX_OKF_ENTRY_BYTES)
                raw_bytes = cls._read_stream_bounded(handle, rel_path=rel_path, max_bytes=MAX_OKF_ENTRY_BYTES)
            out.append(OkfDocument.parse(rel_path, raw_bytes, max_truth_lines=max_truth_lines))

    @staticmethod
    def _ensure_safe_export_target(target: Path, *, root_path: Path, resolved_root: Path) -> None:
        current = target
        while current != root_path and current != current.parent:
            if current.is_symlink():
                raise OkfValidationError(f"Refusing to export through symlink path: {current}")
            current = current.parent
        if not target.resolve().is_relative_to(resolved_root):
            raise OkfValidationError(f"Export path escapes destination root: {target}")

    @classmethod
    def _open_zip_reader(
        cls,
        source: str | Path | bytes | bytearray | memoryview | IO[bytes],
    ) -> zipfile.ZipFile:
        if isinstance(source, (str, Path)):
            zip_path = Path(source)
            if zip_path.is_symlink():
                raise OkfValidationError(f"OKF ZIP archive must not be a symlink: {zip_path}")
            if not zip_path.is_file():
                raise OkfValidationError(f"OKF ZIP archive does not exist: {zip_path}")
            return zipfile.ZipFile(cls._open_file_nofollow(zip_path), mode="r")
        if isinstance(source, (bytes, bytearray, memoryview)):
            return zipfile.ZipFile(io.BytesIO(source), mode="r")
        return zipfile.ZipFile(source, mode="r")

    @staticmethod
    def _is_specs_path(rel_path: str) -> bool:
        return rel_path == "specs" or rel_path.startswith("specs/")

    @staticmethod
    def _is_log_path(rel_path: str) -> bool:
        return rel_path == "log.md" or rel_path.endswith("/log.md")

    @staticmethod
    def _read_stream_bounded(
        stream: IO[bytes],
        *,
        rel_path: str,
        max_bytes: int = MAX_OKF_ENTRY_BYTES,
        chunk_size: int = _STREAM_CHUNK_BYTES,
    ) -> bytes:
        chunks: list[bytes] = []
        total = 0
        while True:
            chunk = stream.read(chunk_size)
            if not chunk:
                break
            total += len(chunk)
            if total > max_bytes:
                raise OkfValidationError(
                    f"OKF entry '{rel_path}' streamed more than the maximum entry size of {max_bytes} bytes"
                )
            chunks.append(chunk)
        return b"".join(chunks)

    @staticmethod
    def _open_file_nofollow(path: Path) -> IO[bytes]:
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        try:
            fd = os.open(path, flags)
        except OSError as exc:
            raise OkfValidationError(f"Unable to safely open OKF entry '{path}': {exc}") from exc
        return os.fdopen(fd, "rb")


def _entry_too_large_error(rel_path: str, actual_bytes: int, max_bytes: int) -> OkfValidationError:
    """Build the size-ceiling error naming the entry, its size, and the per-entry limit."""
    return OkfValidationError(
        f"OKF entry '{rel_path}' is {actual_bytes} bytes, exceeding the maximum entry size of {max_bytes} bytes"
    )


def _normalize_rel_path(raw_path: str) -> str:
    if not raw_path or "\x00" in raw_path:
        raise OkfValidationError(f"Invalid OKF entry path: {raw_path!r}")
    if raw_path.startswith(("/", "\\")):
        raise OkfValidationError(f"Absolute path is not allowed in OKF bundle: {raw_path!r}")
    if _DRIVE_PREFIX_RE.match(raw_path):
        raise OkfValidationError(f"Windows drive prefix is not allowed in OKF bundle: {raw_path!r}")
    if "\\" in raw_path:
        raise OkfValidationError(f"Backslash path separator is not allowed in OKF bundle: {raw_path!r}")

    segments = raw_path.split("/")
    for segment in segments:
        if segment in ("", ".", ".."):
            raise OkfValidationError(f"Unsafe or malformed path segment in OKF bundle: {raw_path!r}")
    return raw_path
