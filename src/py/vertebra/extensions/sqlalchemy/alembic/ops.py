"""SQLAlchemy 2.1 view and vector index DDL operations for Alembic migrations."""

from __future__ import annotations

import msgspec

__all__ = (
    "CreateVectorIndexOp",
    "CreateViewOp",
    "DropVectorIndexOp",
    "DropViewOp",
)


class CreateVectorIndexOp(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """SQLAlchemy 2.1 CreateVectorIndex DDL operation for Alembic migrations."""

    index_name: str
    table_name: str
    column_name: str
    metric: str = "cosine"
    storing: tuple[str, ...] = ()
    not_null_filtered: bool = True

    def to_sql(self, dialect_name: str = "postgresql") -> str:
        """Render the dialect-aware CREATE VECTOR INDEX statement."""
        base = dialect_name.split("+", 1)[0].lower()
        if base == "spanner":
            sp_metric = {
                "cosine": "COSINE",
                "l2": "EUCLIDEAN",
                "inner_product": "DOT_PRODUCT",
            }.get(self.metric.lower(), self.metric.upper())
            storing_clause = f" STORING ({', '.join(self.storing)})" if self.storing else ""
            where_clause = f" WHERE {self.column_name} IS NOT NULL" if self.not_null_filtered else ""
            return (
                f"CREATE VECTOR INDEX {self.index_name} ON {self.table_name} "
                f"({self.column_name}){storing_clause}{where_clause} "
                f"OPTIONS (tree_type = 'SCANN', distance_type = '{sp_metric}')"
            )
        if base == "bigquery":
            bq_metric = {
                "cosine": "COSINE",
                "l2": "EUCLIDEAN",
                "inner_product": "DOT_PRODUCT",
            }.get(self.metric.lower(), self.metric.upper())
            return (
                f"CREATE VECTOR INDEX {self.index_name} ON {self.table_name}({self.column_name}) "
                f"OPTIONS(index_type = 'IVF', distance_type = '{bq_metric}')"
            )
        if base == "oracle":
            metric_kw = {
                "cosine": "DISTANCE COSINE",
                "l2": "DISTANCE EUCLIDEAN",
                "inner_product": "DISTANCE DOT",
            }.get(self.metric.lower(), "DISTANCE COSINE")
            return (
                f"CREATE VECTOR INDEX {self.index_name} ON {self.table_name} "
                f"({self.column_name}) ORGANIZATION INMEMORY NEIGHBOR GRAPH {metric_kw}"
            )
        ops = {
            "cosine": "vector_cosine_ops",
            "l2": "vector_l2_ops",
            "inner_product": "vector_ip_ops",
        }.get(self.metric.lower(), "vector_cosine_ops")
        return f"CREATE INDEX {self.index_name} ON {self.table_name} USING hnsw ({self.column_name} {ops})"

    def reverse(self) -> DropVectorIndexOp:
        """Return the inverse DropVectorIndexOp."""
        return DropVectorIndexOp(
            index_name=self.index_name,
            table_name=self.table_name,
        )


class DropVectorIndexOp(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """SQLAlchemy 2.1 DropVectorIndex DDL operation for Alembic migrations."""

    index_name: str
    table_name: str = ""
    if_exists: bool = True

    def to_sql(self, dialect_name: str = "postgresql") -> str:
        """Render the dialect-aware DROP INDEX statement."""
        base = dialect_name.split("+", 1)[0].lower()
        if base == "oracle":
            return f"DROP INDEX {self.index_name}"
        if base == "bigquery" and self.table_name:
            exists_bq = "IF EXISTS " if self.if_exists else ""
            return f"DROP VECTOR INDEX {exists_bq}{self.index_name} ON {self.table_name}"
        if base == "spanner":
            return f"DROP VECTOR INDEX {self.index_name}"
        exists_clause = "IF EXISTS " if self.if_exists else ""
        return f"DROP INDEX {exists_clause}{self.index_name}"


class CreateViewOp(msgspec.Struct, frozen=True, gc=False):
    """SQLAlchemy 2.1 CreateView DDL operation for Alembic migrations."""

    view_name: str
    select_sql: str
    or_replace: bool = False
    materialized: bool = False

    def to_sql(self, dialect_name: str = "postgresql") -> str:
        """Render the dialect-aware CREATE VIEW DDL statement."""
        base = dialect_name.split("+", 1)[0].lower()
        if self.materialized:
            return f"CREATE MATERIALIZED VIEW {self.view_name} AS {self.select_sql}"
        replace_clause = "OR REPLACE " if self.or_replace and base != "sqlite" else ""
        return f"CREATE {replace_clause}VIEW {self.view_name} AS {self.select_sql}"

    def reverse(self) -> DropViewOp:
        """Return the inverse DropViewOp for Alembic downgrade migrations."""
        return DropViewOp(view_name=self.view_name, materialized=self.materialized)


class DropViewOp(msgspec.Struct, frozen=True, gc=False):
    """SQLAlchemy 2.1 DropView DDL operation for Alembic migrations."""

    view_name: str
    if_exists: bool = True
    materialized: bool = False

    def to_sql(self, dialect_name: str = "postgresql") -> str:
        """Render the dialect-aware DROP VIEW DDL statement."""
        base = dialect_name.split("+", 1)[0].lower()
        mat = "MATERIALIZED " if self.materialized else ""
        exists_clause = "IF EXISTS " if self.if_exists and base != "oracle" else ""
        return f"DROP {mat}VIEW {exists_clause}{self.view_name}"
