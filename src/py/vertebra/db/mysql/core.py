"""MySQL and MariaDB DSN parsing and protocol helpers."""

from __future__ import annotations

import urllib.parse
from typing import Any

__all__ = ("MySQLCore",)


class MySQLCore:
    """Protocol and DSN helpers for MySQL and MariaDB drivers."""

    @staticmethod
    def parse_dsn_params(
        dsn: str,
        *,
        default_charset: str = "utf8mb4",
        default_autocommit: bool = False,
        iam_password: str | None = None,
    ) -> dict[str, Any]:
        """Parse a MySQL or MariaDB DSN connection string into connection kwargs."""
        parsed = urllib.parse.urlparse(dsn)
        host = parsed.hostname or "localhost"
        port = parsed.port or 3306
        user = parsed.username or ""
        password = parsed.password or iam_password or ""
        database = parsed.path.lstrip("/") if parsed.path else ""
        query = urllib.parse.parse_qs(parsed.query)
        charset = query.get("charset", [default_charset])[0]
        autocommit_val = query.get("autocommit", [str(default_autocommit).lower()])[0]
        autocommit = autocommit_val.lower() in ("true", "1", "yes")
        return {
            "host": host,
            "port": port,
            "user": user,
            "password": password,
            "database": database,
            "charset": charset,
            "autocommit": autocommit,
        }
