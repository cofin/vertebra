"""DuckDB analytical SQL inspection and GCS secret helpers."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from vertebra.db.driver._common import DUCKDB_DIALECT_TAG
from vertebra.typing import ImproperConfigurationError
from vertebra.utils.text import validate_identifier

if TYPE_CHECKING:
    from collections.abc import Sequence

    from vertebra.storage import GCSConfig

__all__ = (
    "DUCKDB_DIALECT_TAG",
    "DuckDBCore",
)


class DuckDBCore:
    """Protocol and SQL helpers for DuckDB analytical execution."""

    @staticmethod
    def is_write_statement(sql: str) -> bool:
        """Return True if the SQL statement mutates state and requires the single-writer lock."""
        head = sql.lstrip().split(None, 1)
        if not head:
            return False
        verb = head[0].upper()
        return verb in {
            "INSERT",
            "UPDATE",
            "DELETE",
            "CREATE",
            "DROP",
            "ALTER",
            "COPY",
            "MERGE",
            "TRUNCATE",
            "ATTACH",
        }

    @staticmethod
    def build_gcs_secret_sql(
        secret_name: str = "vertebra_gcs",
        config: GCSConfig | Any | None = None,
        *,
        bearer_token: str | None = None,
        scope: str | Sequence[str] | None = None,
    ) -> str:
        """Build a DuckDB `httpfs` GCS bearer-token `CREATE OR REPLACE SECRET` statement.

        Resolves an OAuth2 bearer token via `GCSConfig.resolve_adc_token()` (including optional
        `target_service_account` impersonation) when `bearer_token` is omitted. DuckDB `CREATE SECRET`
        DDL does not accept prepared parameters, so the token is rendered as a single-quoted SQL
        literal with embedded quotes doubled, and tokens containing NUL, CR, or LF characters are
        rejected before rendering.

        Args:
            secret_name: Valid SQL identifier for the DuckDB secret.
            config: Optional `GCSConfig` from which to resolve an ADC bearer token.
            bearer_token: Explicit OAuth2 bearer token overriding `config`.
            scope: Optional GCS URI prefix or sequence of URI prefixes restricting the secret scope.

        Returns:
            Rendered `CREATE OR REPLACE SECRET` SQL statement.

        Raises:
            ImproperConfigurationError: When neither `bearer_token` nor `config` yields a non-blank bearer token.
            ValueError: When `secret_name` is not a valid SQL identifier or the token/scope contains NUL, CR, or LF.
        """
        validate_identifier(secret_name, "DuckDB secret name", allow_dots=False)
        token = bearer_token
        if token is None and config is not None:
            resolve_fn = getattr(config, "resolve_adc_token", None)
            if callable(resolve_fn):
                resolved = resolve_fn()
                if isinstance(resolved, str):
                    token = resolved
        if token is None or not token.strip():
            msg = "DuckDB GCS secret requires a non-empty bearer token from `bearer_token` or `GCSConfig` credentials"
            raise ImproperConfigurationError(msg)
        if any(char in token for char in ("\x00", "\r", "\n")):
            msg = "DuckDB GCS bearer token must not contain NUL, CR, or LF characters"
            raise ValueError(msg)
        escaped = token.replace("'", "''")
        if scope is None:
            return f"CREATE OR REPLACE SECRET {secret_name} (TYPE gcs, BEARER_TOKEN '{escaped}')"
        if isinstance(scope, str):
            if any(char in scope for char in ("\x00", "\r", "\n")):
                msg = "DuckDB GCS scope must not contain NUL, CR, or LF characters"
                raise ValueError(msg)
            escaped_scope = scope.replace("'", "''")
            return (
                f"CREATE OR REPLACE SECRET {secret_name} (TYPE gcs, BEARER_TOKEN '{escaped}', SCOPE '{escaped_scope}')"
            )
        scope_items: list[str] = []
        for item in scope:
            item_str = str(item)
            if any(char in item_str for char in ("\x00", "\r", "\n")):
                msg = "DuckDB GCS scope must not contain NUL, CR, or LF characters"
                raise ValueError(msg)
            escaped_item = item_str.replace("'", "''")
            scope_items.append(f"'{escaped_item}'")
        if not scope_items:
            return f"CREATE OR REPLACE SECRET {secret_name} (TYPE gcs, BEARER_TOKEN '{escaped}')"
        if len(scope_items) == 1:
            return (
                f"CREATE OR REPLACE SECRET {secret_name} (TYPE gcs, BEARER_TOKEN '{escaped}', SCOPE {scope_items[0]})"
            )
        return f"CREATE OR REPLACE SECRET {secret_name} (TYPE gcs, BEARER_TOKEN '{escaped}', SCOPE [{', '.join(scope_items)}])"
