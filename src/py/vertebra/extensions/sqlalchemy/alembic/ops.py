"""SQLAlchemy 2.1 view and vector index MigrateOperation subclasses for Alembic migrations."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from alembic.autogenerate import renderers
from alembic.ddl.impl import DefaultImpl
from alembic.operations import Operations
from alembic.operations.ops import MigrateOperation
from sqlalchemy.schema import DDL

if TYPE_CHECKING:
    from alembic.autogenerate.api import AutogenContext

__all__ = (
    "CreateVectorIndexOp",
    "CreateViewOp",
    "DropVectorIndexOp",
    "DropViewOp",
)


@Operations.register_operation("create_vector_index")
class CreateVectorIndexOp(MigrateOperation):
    """Alembic MigrateOperation for dialect-aware CREATE VECTOR INDEX DDL."""

    def __init__(
        self,
        index_name: str,
        table_name: str,
        column_name: str,
        *,
        metric: str = "cosine",
        storing: tuple[str, ...] = (),
        not_null_filtered: bool = True,
    ) -> None:
        """Initialize CreateVectorIndexOp with index target, metric, and Spanner options."""
        self.index_name = index_name
        self.table_name = table_name
        self.column_name = column_name
        self.metric = metric
        self.storing = tuple(storing)
        self.not_null_filtered = not_null_filtered

    @classmethod
    def create_vector_index(
        cls,
        operations: Operations,
        index_name: str,
        table_name: str,
        column_name: str,
        *,
        metric: str = "cosine",
        storing: Sequence[str] = (),
        not_null_filtered: bool = True,
    ) -> Any:
        """Invoke CreateVectorIndexOp on the active Alembic Operations context."""
        op = cls(
            index_name=index_name,
            table_name=table_name,
            column_name=column_name,
            metric=metric,
            storing=tuple(storing),
            not_null_filtered=not_null_filtered,
        )
        return operations.invoke(op)

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
        ops_name = {
            "cosine": "vector_cosine_ops",
            "l2": "vector_l2_ops",
            "inner_product": "vector_ip_ops",
        }.get(self.metric.lower(), "vector_cosine_ops")
        return f"CREATE INDEX {self.index_name} ON {self.table_name} USING hnsw ({self.column_name} {ops_name})"

    def reverse(self) -> DropVectorIndexOp:
        """Return the inverse DropVectorIndexOp."""
        return DropVectorIndexOp(
            index_name=self.index_name,
            table_name=self.table_name,
        )

    def to_diff_tuple(self) -> tuple[Any, ...]:
        """Return an Alembic diff representation for this operation."""
        return ("create_vector_index", self.index_name, self.table_name, self.column_name)


@Operations.register_operation("drop_vector_index")
class DropVectorIndexOp(MigrateOperation):
    """Alembic MigrateOperation for dialect-aware DROP VECTOR INDEX DDL."""

    def __init__(
        self,
        index_name: str,
        table_name: str = "",
        if_exists: bool = True,
    ) -> None:
        """Initialize DropVectorIndexOp with index name, optional table name, and IF EXISTS flag."""
        self.index_name = index_name
        self.table_name = table_name
        self.if_exists = if_exists

    @classmethod
    def drop_vector_index(
        cls,
        operations: Operations,
        index_name: str,
        table_name: str = "",
        *,
        if_exists: bool = True,
    ) -> Any:
        """Invoke DropVectorIndexOp on the active Alembic Operations context."""
        op = cls(index_name=index_name, table_name=table_name, if_exists=if_exists)
        return operations.invoke(op)

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

    def to_diff_tuple(self) -> tuple[Any, ...]:
        """Return an Alembic diff representation for this operation."""
        return ("drop_vector_index", self.index_name, self.table_name)


@Operations.register_operation("create_view")
class CreateViewOp(MigrateOperation):
    """Alembic MigrateOperation for dialect-aware CREATE VIEW DDL."""

    def __init__(
        self,
        view_name: str,
        select_sql: str,
        or_replace: bool = False,
        materialized: bool = False,
    ) -> None:
        """Initialize CreateViewOp with view name, SELECT SQL, and view options."""
        self.view_name = view_name
        self.select_sql = select_sql
        self.or_replace = or_replace
        self.materialized = materialized

    @classmethod
    def create_view(
        cls,
        operations: Operations,
        view_name: str,
        select_sql: str,
        *,
        or_replace: bool = False,
        materialized: bool = False,
    ) -> Any:
        """Invoke CreateViewOp on the active Alembic Operations context."""
        op = cls(
            view_name=view_name,
            select_sql=select_sql,
            or_replace=or_replace,
            materialized=materialized,
        )
        return operations.invoke(op)

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

    def to_diff_tuple(self) -> tuple[Any, ...]:
        """Return an Alembic diff representation for this operation."""
        return ("create_view", self.view_name)


@Operations.register_operation("drop_view")
class DropViewOp(MigrateOperation):
    """Alembic MigrateOperation for dialect-aware DROP VIEW DDL."""

    def __init__(
        self,
        view_name: str,
        if_exists: bool = True,
        materialized: bool = False,
    ) -> None:
        """Initialize DropViewOp with view name, IF EXISTS flag, and materialized flag."""
        self.view_name = view_name
        self.if_exists = if_exists
        self.materialized = materialized

    @classmethod
    def drop_view(
        cls,
        operations: Operations,
        view_name: str,
        *,
        if_exists: bool = True,
        materialized: bool = False,
    ) -> Any:
        """Invoke DropViewOp on the active Alembic Operations context."""
        op = cls(view_name=view_name, if_exists=if_exists, materialized=materialized)
        return operations.invoke(op)

    def to_sql(self, dialect_name: str = "postgresql") -> str:
        """Render the dialect-aware DROP VIEW DDL statement."""
        base = dialect_name.split("+", 1)[0].lower()
        mat = "MATERIALIZED " if self.materialized else ""
        exists_clause = "IF EXISTS " if self.if_exists and base != "oracle" else ""
        return f"DROP {mat}VIEW {exists_clause}{self.view_name}"

    def to_diff_tuple(self) -> tuple[Any, ...]:
        """Return an Alembic diff representation for this operation."""
        return ("drop_view", self.view_name)


def _execute_ddl_op(operations: Operations, operation: Any) -> None:
    impl = operations.impl
    dialect_name = getattr(getattr(impl, "dialect", None), "name", "postgresql")
    sql = operation.to_sql(dialect_name)
    ddl_cls: Any = DDL
    stmt = ddl_cls(sql.replace("%", "%%"))
    if isinstance(impl, DefaultImpl):
        impl.execute(stmt)
        return
    operations.execute(stmt)


@Operations.implementation_for(CreateVectorIndexOp)
def _implement_create_vector_index(operations: Operations, operation: CreateVectorIndexOp) -> None:
    _execute_ddl_op(operations, operation)


@Operations.implementation_for(DropVectorIndexOp)
def _implement_drop_vector_index(operations: Operations, operation: DropVectorIndexOp) -> None:
    _execute_ddl_op(operations, operation)


@Operations.implementation_for(CreateViewOp)
def _implement_create_view(operations: Operations, operation: CreateViewOp) -> None:
    _execute_ddl_op(operations, operation)


@Operations.implementation_for(DropViewOp)
def _implement_drop_view(operations: Operations, operation: DropViewOp) -> None:
    _execute_ddl_op(operations, operation)


@renderers.dispatch_for(CreateVectorIndexOp)
def _render_create_vector_index(autogen_context: AutogenContext, op: CreateVectorIndexOp) -> str:
    prefix = autogen_context.opts.get("alembic_module_prefix", "op.")
    parts = [
        f"{op.index_name!r}",
        f"{op.table_name!r}",
        f"{op.column_name!r}",
        f"metric={op.metric!r}",
    ]
    if op.storing:
        parts.append(f"storing={op.storing!r}")
    if not op.not_null_filtered:
        parts.append("not_null_filtered=False")
    return f"{prefix}create_vector_index({', '.join(parts)})"


@renderers.dispatch_for(DropVectorIndexOp)
def _render_drop_vector_index(autogen_context: AutogenContext, op: DropVectorIndexOp) -> str:
    prefix = autogen_context.opts.get("alembic_module_prefix", "op.")
    parts = [f"{op.index_name!r}"]
    if op.table_name:
        parts.append(f"{op.table_name!r}")
    if not op.if_exists:
        parts.append("if_exists=False")
    return f"{prefix}drop_vector_index({', '.join(parts)})"


@renderers.dispatch_for(CreateViewOp)
def _render_create_view(autogen_context: AutogenContext, op: CreateViewOp) -> str:
    prefix = autogen_context.opts.get("alembic_module_prefix", "op.")
    parts = [f"{op.view_name!r}", f"{op.select_sql!r}"]
    if op.or_replace:
        parts.append("or_replace=True")
    if op.materialized:
        parts.append("materialized=True")
    return f"{prefix}create_view({', '.join(parts)})"


@renderers.dispatch_for(DropViewOp)
def _render_drop_view(autogen_context: AutogenContext, op: DropViewOp) -> str:
    prefix = autogen_context.opts.get("alembic_module_prefix", "op.")
    parts = [f"{op.view_name!r}"]
    if not op.if_exists:
        parts.append("if_exists=False")
    if op.materialized:
        parts.append("materialized=True")
    return f"{prefix}drop_view({', '.join(parts)})"
