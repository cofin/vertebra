"""SQL Server `@P1` placeholder rewriter."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from typing import Any

from vertebra.db.driver._common import MSSQL_DIALECT_TAG
from vertebra.utils.module_loader import import_optional

__all__ = (
    "MSSQL_DIALECT_TAG",
    "MSSQLCore",
    "rewrite_mssql_placeholders",
)


class MSSQLCore:
    """T-SQL placeholder translation."""

    @staticmethod
    def rewrite_placeholders(sql: str) -> tuple[str, int]:
        """Rewrite `?` and `$1`/`$2` placeholders to T-SQL `@P1, @P2, ...` outside quotes, brackets, and comments."""
        native = import_optional("vertebra._vertebra")
        if native is not None and hasattr(native, "rewrite_mssql_placeholders"):
            rewritten, count = native.rewrite_mssql_placeholders(sql)
            return str(rewritten), int(count)

        out: list[str] = []
        state = "NORMAL"
        i = 0
        n = len(sql)
        seq_index = 0
        max_param = 0

        while i < n:
            ch = sql[i]
            if state == "NORMAL":
                if ch == "'":
                    out.append(ch)
                    state = "SINGLE_QUOTE"
                    i += 1
                elif ch == '"':
                    out.append(ch)
                    state = "DOUBLE_QUOTE"
                    i += 1
                elif ch == "[":
                    out.append(ch)
                    state = "BRACKET"
                    i += 1
                elif ch == "-" and i + 1 < n and sql[i + 1] == "-":
                    out.append("--")
                    state = "LINE_COMMENT"
                    i += 2
                elif ch == "/" and i + 1 < n and sql[i + 1] == "*":
                    out.append("/*")
                    state = "BLOCK_COMMENT"
                    i += 2
                elif ch == "?":
                    seq_index += 1
                    max_param = max(max_param, seq_index)
                    out.append(f"@P{seq_index}")
                    i += 1
                elif ch == "$" and i + 1 < n and sql[i + 1].isdigit() and sql[i + 1] != "0":
                    j = i + 1
                    while j < n and sql[j].isdigit():
                        j += 1
                    num = int(sql[i + 1 : j])
                    seq_index = max(seq_index, num)
                    max_param = max(max_param, num)
                    out.append(f"@P{num}")
                    i = j
                else:
                    out.append(ch)
                    i += 1
            elif state == "SINGLE_QUOTE":
                out.append(ch)
                if ch == "'":
                    if i + 1 < n and sql[i + 1] == "'":
                        out.append("'")
                        i += 2
                    else:
                        state = "NORMAL"
                        i += 1
                else:
                    i += 1
            elif state == "DOUBLE_QUOTE":
                out.append(ch)
                if ch == '"':
                    if i + 1 < n and sql[i + 1] == '"':
                        out.append('"')
                        i += 2
                    else:
                        state = "NORMAL"
                        i += 1
                else:
                    i += 1
            elif state == "BRACKET":
                out.append(ch)
                if ch == "]":
                    if i + 1 < n and sql[i + 1] == "]":
                        out.append("]")
                        i += 2
                    else:
                        state = "NORMAL"
                        i += 1
                else:
                    i += 1
            elif state == "LINE_COMMENT":
                out.append(ch)
                i += 1
                if ch == "\n":
                    state = "NORMAL"
            elif state == "BLOCK_COMMENT":
                if ch == "*" and i + 1 < n and sql[i + 1] == "/":
                    out.append("*/")
                    state = "NORMAL"
                    i += 2
                else:
                    out.append(ch)
                    i += 1

        return "".join(out), max_param


rewrite_mssql_placeholders = MSSQLCore.rewrite_placeholders
