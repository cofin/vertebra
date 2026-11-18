"""Django Oracle DatabaseWrapper over vertebra.db.dbapi with mTLS Wallet and LeaseBroker."""

from __future__ import annotations

from functools import cached_property
from typing import TYPE_CHECKING, Any, cast

from vertebra.db.adapters.oracle import OracleConfig
from vertebra.db.dbapi import SyncDBAPIConnection, SyncDBAPIModule
from vertebra.extensions.django.db.backends import DatabaseWrapperMixin
from vertebra.utils.module_loader import ensure_module

if TYPE_CHECKING:
    from collections.abc import Mapping

    from django.db.backends.base.base import BaseDatabaseWrapper
    from django.db.backends.base.introspection import BaseDatabaseIntrospection, TableInfo
    from django.db.backends.base.operations import BaseDatabaseOperations
    from django.db.backends.oracle.client import DatabaseClient as OracleDatabaseClient
    from django.db.backends.oracle.creation import DatabaseCreation as OracleDatabaseCreation
    from django.db.backends.oracle.features import DatabaseFeatures as OracleDatabaseFeatures
    from django.db.backends.oracle.schema import DatabaseSchemaEditor as OracleDatabaseSchemaEditor
    from django.db.backends.oracle.validation import DatabaseValidation as OracleDatabaseValidation
    from django.db.backends.utils import strip_quotes, truncate_name

    from vertebra.db.dbapi import SyncDBAPICursor
else:
    BaseDatabaseWrapper = ensure_module(
        "django.db.backends.base.base", package="django", extra="django"
    ).BaseDatabaseWrapper
    _introspection_mod = ensure_module("django.db.backends.base.introspection", package="django", extra="django")
    BaseDatabaseIntrospection = _introspection_mod.BaseDatabaseIntrospection
    TableInfo = _introspection_mod.TableInfo
    BaseDatabaseOperations = ensure_module(
        "django.db.backends.base.operations", package="django", extra="django"
    ).BaseDatabaseOperations
    _utils_mod = ensure_module("django.db.backends.utils", package="django", extra="django")
    strip_quotes = _utils_mod.strip_quotes
    truncate_name = _utils_mod.truncate_name
    OracleDatabaseClient = ensure_module(
        "django.db.backends.oracle.client", package="django", extra="django"
    ).DatabaseClient
    OracleDatabaseCreation = ensure_module(
        "django.db.backends.oracle.creation", package="django", extra="django"
    ).DatabaseCreation
    OracleDatabaseFeatures = ensure_module(
        "django.db.backends.oracle.features", package="django", extra="django"
    ).DatabaseFeatures
    OracleDatabaseSchemaEditor = ensure_module(
        "django.db.backends.oracle.schema", package="django", extra="django"
    ).DatabaseSchemaEditor
    OracleDatabaseValidation = ensure_module(
        "django.db.backends.oracle.validation", package="django", extra="django"
    ).DatabaseValidation

__all__ = ("DatabaseWrapper",)


class _OracleOperations(BaseDatabaseOperations):
    """Driver-free Oracle SQL operations helper."""

    integer_field_ranges: dict[str, tuple[int, int]] = {
        "SmallIntegerField": (-99999999999, 99999999999),
        "IntegerField": (-99999999999, 99999999999),
        "BigIntegerField": (-9999999999999999999, 9999999999999999999),
        "PositiveBigIntegerField": (0, 9999999999999999999),
        "PositiveSmallIntegerField": (0, 99999999999),
        "PositiveIntegerField": (0, 99999999999),
        "SmallAutoField": (-99999, 99999),
        "AutoField": (-99999999999, 99999999999),
        "BigAutoField": (-9999999999999999999, 9999999999999999999),
    }
    set_operators: dict[str, str] = {**BaseDatabaseOperations.set_operators, "difference": "MINUS"}
    cast_char_field_without_max_length: str = "NVARCHAR2(2000)"
    cast_data_types: dict[str, str] = {
        "AutoField": "NUMBER(11)",
        "BigAutoField": "NUMBER(19)",
        "SmallAutoField": "NUMBER(5)",
        "TextField": "NVARCHAR2(2000)",
    }

    def max_name_length(self) -> int:
        """Return Oracle identifier length limit."""
        return 30

    def no_limit_value(self) -> str | None:
        """Return None so Oracle OFFSET/FETCH omits explicit unbounded limits."""
        return None

    def limit_offset_sql(self, low_mark: int, high_mark: int | None) -> str:
        """Render ANSI OFFSET/FETCH SQL clauses for Oracle."""
        offset = low_mark or 0
        fetch = (high_mark - offset) if high_mark is not None else None
        clauses = [
            f"OFFSET {offset} ROWS" if offset else None,
            f"FETCH FIRST {fetch} ROWS ONLY" if fetch else None,
        ]
        return " ".join(clause for clause in clauses if clause)

    def quote_name(self, name: str) -> str:
        """Quote and uppercase an Oracle identifier truncated to ``max_name_length``."""
        if not name.startswith('"') and not name.endswith('"'):
            name = f'"{truncate_name(name, self.max_name_length())}"'
        return name.replace("%", "%%").upper()

    def pk_default_value(self) -> str:
        """Return the SQL placeholder for Oracle identity primary key defaults."""
        return "NULL"

    def prep_for_iexact_query(self, x: str) -> str:
        """Return pattern operand unchanged for Oracle iexact lookups."""
        return x

    def _get_no_autofield_sequence_name(self, table: str) -> str:
        name_length = self.max_name_length() - 3
        return f"{truncate_name(strip_quotes(table), name_length).upper()}_SQ"


class _OracleIntrospection(BaseDatabaseIntrospection):
    """Driver-free Oracle schema introspection helper."""

    data_types_reverse: dict[Any, str] = {}

    def identifier_converter(self, name: str) -> str:
        """Normalize Oracle uppercase identifiers to lowercase for comparison."""
        return name.lower()

    def get_table_list(self, cursor: Any) -> list[TableInfo]:
        """Return table and view entries visible to the current Oracle user."""
        cursor.execute(
            "SELECT user_tables.table_name, 't' FROM user_tables UNION ALL SELECT view_name, 'v' FROM user_views"
        )
        return [TableInfo(self.identifier_converter(row[0]), row[1]) for row in cursor.fetchall()]


class DatabaseWrapper(DatabaseWrapperMixin, BaseDatabaseWrapper):
    """Django Oracle database wrapper backed by Vertebra native driver and LeaseBroker."""

    vendor: str = "oracle"
    display_name: str = "Oracle (Vertebra)"
    Database: Any = SyncDBAPIModule(paramstyle="named")
    SchemaEditorClass = OracleDatabaseSchemaEditor
    client_class = OracleDatabaseClient
    creation_class = OracleDatabaseCreation
    features_class = OracleDatabaseFeatures
    introspection_class = _OracleIntrospection
    ops_class = _OracleOperations
    validation_class = OracleDatabaseValidation

    data_types: dict[str, str] = {
        "AutoField": "NUMBER(11) GENERATED BY DEFAULT ON NULL AS IDENTITY",
        "BigAutoField": "NUMBER(19) GENERATED BY DEFAULT ON NULL AS IDENTITY",
        "BinaryField": "BLOB",
        "BooleanField": "NUMBER(1)",
        "CharField": "NVARCHAR2(%(max_length)s)",
        "DateField": "DATE",
        "DateTimeField": "TIMESTAMP",
        "DecimalField": "NUMBER(%(max_digits)s, %(decimal_places)s)",
        "DurationField": "INTERVAL DAY(9) TO SECOND(6)",
        "FileField": "NVARCHAR2(%(max_length)s)",
        "FilePathField": "NVARCHAR2(%(max_length)s)",
        "FloatField": "DOUBLE PRECISION",
        "IntegerField": "NUMBER(11)",
        "JSONField": "NCLOB",
        "BigIntegerField": "NUMBER(19)",
        "IPAddressField": "VARCHAR2(15)",
        "GenericIPAddressField": "VARCHAR2(39)",
        "PositiveBigIntegerField": "NUMBER(19)",
        "PositiveIntegerField": "NUMBER(11)",
        "PositiveSmallIntegerField": "NUMBER(11)",
        "SlugField": "NVARCHAR2(%(max_length)s)",
        "SmallAutoField": "NUMBER(5) GENERATED BY DEFAULT ON NULL AS IDENTITY",
        "SmallIntegerField": "NUMBER(11)",
        "TextField": "NCLOB",
        "TimeField": "TIMESTAMP",
        "URLField": "VARCHAR2(%(max_length)s)",
        "UUIDField": "VARCHAR2(32)",
    }

    data_type_check_constraints: dict[str, str] = {
        "BooleanField": "%(qn_column)s IN (0,1)",
        "JSONField": "%(qn_column)s IS JSON",
        "PositiveBigIntegerField": "%(qn_column)s >= 0",
        "PositiveIntegerField": "%(qn_column)s >= 0",
        "PositiveSmallIntegerField": "%(qn_column)s >= 0",
    }

    _limited_data_types: tuple[str, ...] = ("clob", "nclob", "blob")

    operators: dict[str, str] = {
        "exact": "= %s",
        "iexact": "= UPPER(%s)",
        "contains": "LIKE TRANSLATE(%s USING NCHAR_CS) ESCAPE TRANSLATE('\\' USING NCHAR_CS)",
        "icontains": "LIKE UPPER(TRANSLATE(%s USING NCHAR_CS)) ESCAPE TRANSLATE('\\' USING NCHAR_CS)",
        "gt": "> %s",
        "gte": ">= %s",
        "lt": "< %s",
        "lte": "<= %s",
        "startswith": "LIKE TRANSLATE(%s USING NCHAR_CS) ESCAPE TRANSLATE('\\' USING NCHAR_CS)",
        "endswith": "LIKE TRANSLATE(%s USING NCHAR_CS) ESCAPE TRANSLATE('\\' USING NCHAR_CS)",
        "istartswith": "LIKE UPPER(TRANSLATE(%s USING NCHAR_CS)) ESCAPE TRANSLATE('\\' USING NCHAR_CS)",
        "iendswith": "LIKE UPPER(TRANSLATE(%s USING NCHAR_CS)) ESCAPE TRANSLATE('\\' USING NCHAR_CS)",
    }

    pattern_esc: str = r"REPLACE(REPLACE(REPLACE({}, '\', '\\'), '%%', '\%%'), '_', '\_')"
    pattern_ops: dict[str, str] = {
        "contains": "LIKE TRANSLATE( '%%' || {} || '%%' USING NCHAR_CS) ESCAPE TRANSLATE('\\' USING NCHAR_CS)",
        "icontains": "LIKE TRANSLATE( '%%' || UPPER({}) || '%%' USING NCHAR_CS) ESCAPE TRANSLATE('\\' USING NCHAR_CS)",
        "startswith": "LIKE TRANSLATE( {} || '%%' USING NCHAR_CS) ESCAPE TRANSLATE('\\' USING NCHAR_CS)",
        "istartswith": "LIKE TRANSLATE( UPPER({}) || '%%' USING NCHAR_CS) ESCAPE TRANSLATE('\\' USING NCHAR_CS)",
        "endswith": "LIKE TRANSLATE( '%%' || {} USING NCHAR_CS) ESCAPE TRANSLATE('\\' USING NCHAR_CS)",
        "iendswith": "LIKE TRANSLATE( '%%' || UPPER({}) USING NCHAR_CS) ESCAPE TRANSLATE('\\' USING NCHAR_CS)",
    }

    def __init__(self, settings_dict: dict[str, Any], alias: str = "default") -> None:
        """Initialize Oracle DatabaseWrapper with settings dictionary and alias."""
        BaseDatabaseWrapper.__init__(self, settings_dict, alias=alias)
        DatabaseWrapperMixin.__init__(self, settings_dict, alias=alias)

    @property
    def is_pool(self) -> bool:
        """Return True when Django connection pooling is enabled in OPTIONS."""
        return bool(self.settings_dict.get("OPTIONS", {}).get("pool", False))

    @cached_property
    def oracle_version(self) -> tuple[int, ...]:
        """Return parsed Oracle database server version tuple."""
        with self.temporary_connection() as cursor:
            cursor.execute("SELECT VERSION FROM V$INSTANCE")
            row = cursor.fetchone()
        version_str = str(row[0]) if row and row[0] is not None else "23.0.0"
        return tuple(int(part) for part in version_str.split(".") if part.isdigit()) or (23, 0, 0)

    def get_database_version(self) -> tuple[int, ...]:
        """Return Oracle server version tuple."""
        return self.oracle_version

    def get_connection_params(self) -> dict[str, Any]:
        """Build OracleConfig locally from Django DATABASES settings."""
        host = str(self.settings_dict.get("HOST") or "localhost")
        port = int(self.settings_dict.get("PORT") or 1521)
        name = str(self.settings_dict.get("NAME") or "FREEPDB1")
        user = str(self.settings_dict.get("USER") or "system")
        password = str(self.settings_dict.get("PASSWORD") or "")
        auth = f"{user}:{password}@" if (user and password) else (f"{user}@" if user else "")
        dsn = f"oracle://{auth}{host}:{port}/{name}"

        cfg = OracleConfig(
            dsn=dsn,
            host=host,
            port=port,
            service_name=name,
            user=user,
            password=password,
            wallet_zip=self.pooler_config.wallet_zip,
            broker=self._broker if self._share_pool else None,
        )
        return {"config": cfg}

    def get_new_connection(self, conn_params: Mapping[str, Any]) -> SyncDBAPIConnection:
        """Open a native SyncDBAPIConnection using OracleConfig."""
        cfg = conn_params["config"]
        conn = SyncDBAPIConnection(cfg)
        conn.autocommit = True
        return conn

    def create_cursor(self, name: str | None = None) -> SyncDBAPICursor:
        """Create and return a DBAPI cursor adapting Django :arg0 and out-binds."""
        _ = name
        return cast("SyncDBAPICursor", self.connection.cursor())

    def _set_autocommit(self, autocommit: bool) -> None:
        """Set autocommit on the underlying connection."""
        with self.wrap_database_errors:
            self.connection.autocommit = autocommit

    def is_usable(self) -> bool:
        """Verify connection liveness with a ping."""
        if self.connection is None or getattr(self.connection, "closed", True):
            return False
        try:
            with self.connection.cursor() as cur:
                cur.execute("SELECT 1 FROM DUAL")
            return True
        except Exception:
            return False
