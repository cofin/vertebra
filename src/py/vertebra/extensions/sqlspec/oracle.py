"""Oracle 26ai JSON/OSON sanitizer, Oracle Text CONTAINS sanitizer, and hybrid RRF SQL builder."""

from __future__ import annotations

import array
import datetime
import decimal
import re
import uuid
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Literal

from vertebra.db.oracle import OracleConfig, OraclePoolRegistry, SyncOracleDriver

if TYPE_CHECKING:
    from vertebra.db.oracle.driver import AsyncOracleDriver

__all__ = (
    "OracleConfig",
    "OracleHybridSearch",
    "OraclePoolRegistry",
    "OracleVectorConverter",
)


class OracleVectorConverter:
    """Helper converting Python sequences into Oracle VECTOR bind parameters."""

    __slots__ = ()

    @staticmethod
    def to_oracle_vector(
        vector: Sequence[float | int],
        *,
        type_code: str = "f",
        dtype: str | None = None,
    ) -> array.array[Any]:
        """Convert a sequence of floats or ints to an Oracle VECTOR representation."""
        return SyncOracleDriver.encode_vector(vector, type_code=type_code, dtype=dtype)


_ORACLE_TEXT_RESERVED_WORDS: frozenset[str] = frozenset(
    {
        "ABOUT",
        "ACCUM",
        "AND",
        "BT",
        "BTG",
        "BTI",
        "BTP",
        "EQUIV",
        "FUZZY",
        "HASPATH",
        "INPATH",
        "MDATA",
        "MINUS",
        "NEAR",
        "NOT",
        "NT",
        "NTG",
        "NTI",
        "NTP",
        "OR",
        "PT",
        "RT",
        "SDATA",
        "SQE",
        "SYN",
        "THRESHOLD",
        "TR",
        "TRSYN",
        "TT",
        "WITHIN",
    }
)

_TOKEN_RE = re.compile(r"[A-Za-z0-9]+")


class OracleHybridSearch:
    """Oracle 26ai JSON/OSON sanitizer, Oracle Text CONTAINS sanitizer, and hybrid RRF SQL builder."""

    RESERVED_WORDS: ClassVar[frozenset[str]] = _ORACLE_TEXT_RESERVED_WORDS
    __slots__ = ()

    @staticmethod
    def sanitize_json(value: Any) -> Any:
        """Recursively convert Oracle/Python types into JSON/OSON-safe primitives for MCP and ADK tools."""
        if value is None or isinstance(value, (str, int, float, bool)):
            return value
        if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
            return value.isoformat()
        if isinstance(value, decimal.Decimal):
            return int(value) if value == value.to_integral_value() else float(value)
        if isinstance(value, uuid.UUID):
            return str(value)
        if isinstance(value, array.array):
            return list(value)
        if isinstance(value, (bytes, bytearray, memoryview)):
            return bytes(value).decode("utf-8", errors="replace")
        if isinstance(value, dict):
            return {str(k): OracleHybridSearch.sanitize_json(v) for k, v in value.items()}
        if isinstance(value, (list, tuple, set, frozenset)):
            return [OracleHybridSearch.sanitize_json(item) for item in value]
        read_fn = getattr(value, "read", None)
        if callable(read_fn):
            return OracleHybridSearch.sanitize_json(read_fn())
        return str(value)

    @staticmethod
    def sanitize_text_query(
        query: str,
        *,
        mode: Literal["accum", "and", "near"] = "accum",
    ) -> str:
        """Sanitize user input into an injection-safe Oracle Text ``CONTAINS`` query expression.

        All punctuation and Oracle Text operator characters are stripped, each alphanumeric token
        is escaped inside ``{token}`` braces (neutralizing reserved keywords such as ``AND``, ``OR``,
        ``NOT``, ``NEAR``, and ``ACCUM``), and tokens are joined according to ``mode``.
        """
        tokens = _TOKEN_RE.findall(query)
        if not tokens:
            return ""
        escaped = [f"{{{tok}}}" for tok in tokens]
        if len(escaped) == 1:
            return escaped[0]
        if mode == "and":
            return " & ".join(escaped)
        if mode == "near":
            return f"NEAR(({', '.join(escaped)}))"
        return " , ".join(escaped)

    @staticmethod
    def build_rrf_sql(
        table: str,
        *,
        id_column: str = "id",
        vector_column: str = "embedding",
        text_column: str = "content",
        select_columns: tuple[str, ...] = ("id", "content"),
        vector_metric: Literal["COSINE", "DOT", "EUCLIDEAN"] = "COSINE",
        rrf_k: int = 60,
        candidate_limit: int = 50,
        query_vec: Sequence[float] | None = None,
        text_query: str = "",
        top_k: int = 10,
        approximate: bool = True,
    ) -> tuple[str, dict[str, Any]]:
        """Build an Oracle 26ai Reciprocal Rank Fusion (RRF) hybrid vector + full-text SQL query and bind dictionary.

        Isolates ``ORDER BY VECTOR_DISTANCE(...) FETCH APPROX FIRST :candidate_limit ROWS ONLY`` inside an
        inner derived table (`vec_top`) and assigns ``ROWNUM AS vec_rank`` in the outer CTE block so the
        Oracle 26ai cost-based optimizer preserves ``VECTOR INDEX HNSW SCAN`` rather than falling back to
        ``TABLE ACCESS FULL`` + ``WINDOW SORT``.
        """
        projected = ", ".join(f"base.{col}" for col in select_columns)
        fetch_clause = "FETCH APPROX FIRST" if approximate else "FETCH FIRST"
        sql = (
            "WITH vec_candidates AS ("
            "SELECT vec_top.doc_id, ROWNUM AS vec_rank FROM ("
            f"SELECT {id_column} AS doc_id, "
            f"VECTOR_DISTANCE({vector_column}, :query_vec, {vector_metric}) AS vec_distance "
            f"FROM {table} "
            f"ORDER BY VECTOR_DISTANCE({vector_column}, :query_vec, {vector_metric}) "
            f"{fetch_clause} :candidate_limit ROWS ONLY"
            ") vec_top"
            "), text_candidates AS ("
            "SELECT txt_top.doc_id, ROWNUM AS text_rank FROM ("
            f"SELECT {id_column} AS doc_id, "
            "SCORE(1) AS text_score "
            f"FROM {table} "
            f"WHERE CONTAINS({text_column}, :text_query, 1) > 0 "
            "ORDER BY SCORE(1) DESC "
            "FETCH FIRST :candidate_limit ROWS ONLY"
            ") txt_top"
            "), rrf_fused AS ("
            "SELECT COALESCE(v.doc_id, t.doc_id) AS doc_id, "
            "COALESCE(1.0 / (:rrf_k + v.vec_rank), 0.0) + "
            "COALESCE(1.0 / (:rrf_k + t.text_rank), 0.0) AS rrf_score "
            "FROM vec_candidates v "
            "FULL OUTER JOIN text_candidates t ON v.doc_id = t.doc_id"
            ") "
            f"SELECT {projected}, rrf_fused.rrf_score "
            f"FROM rrf_fused JOIN {table} base ON base.{id_column} = rrf_fused.doc_id "
            "ORDER BY rrf_fused.rrf_score DESC "
            "FETCH FIRST :top_k ROWS ONLY"
        )
        binds: dict[str, Any] = {
            "query_vec": list(query_vec) if query_vec is not None else [],
            "text_query": OracleHybridSearch.sanitize_text_query(text_query) if text_query else "",
            "top_k": int(top_k),
            "rrf_k": int(rrf_k),
            "candidate_limit": int(candidate_limit),
        }
        return sql, binds

    @classmethod
    def execute_rrf(
        cls,
        driver: SyncOracleDriver,
        table: str,
        *,
        id_column: str = "id",
        vector_column: str = "embedding",
        text_column: str = "content",
        select_columns: tuple[str, ...] = ("id", "content"),
        vector_metric: Literal["COSINE", "DOT", "EUCLIDEAN"] = "COSINE",
        rrf_k: int = 60,
        candidate_limit: int = 50,
        query_vec: Sequence[float] | None = None,
        text_query: str = "",
        top_k: int = 10,
        approximate: bool = True,
    ) -> list[dict[str, Any]]:
        """Compile and execute an RRF hybrid search returning sanitized JSON row dicts."""
        sql, binds = cls.build_rrf_sql(
            table,
            id_column=id_column,
            vector_column=vector_column,
            text_column=text_column,
            select_columns=select_columns,
            vector_metric=vector_metric,
            rrf_k=rrf_k,
            candidate_limit=candidate_limit,
            query_vec=query_vec,
            text_query=text_query,
            top_k=top_k,
            approximate=approximate,
        )
        if query_vec is not None:
            binds["query_vec"] = driver.encode_vector(query_vec)
        df = driver.execute_df(sql, binds)
        return [cls.sanitize_json(row_dict) for row_dict in df.to_dicts()]

    @classmethod
    async def execute_rrf_async(
        cls,
        driver: AsyncOracleDriver,
        table: str,
        *,
        id_column: str = "id",
        vector_column: str = "embedding",
        text_column: str = "content",
        select_columns: tuple[str, ...] = ("id", "content"),
        vector_metric: Literal["COSINE", "DOT", "EUCLIDEAN"] = "COSINE",
        rrf_k: int = 60,
        candidate_limit: int = 50,
        query_vec: Sequence[float] | None = None,
        text_query: str = "",
        top_k: int = 10,
        approximate: bool = True,
    ) -> list[dict[str, Any]]:
        """Compile and asynchronously execute an RRF hybrid search returning sanitized row dicts."""
        sql, binds = cls.build_rrf_sql(
            table,
            id_column=id_column,
            vector_column=vector_column,
            text_column=text_column,
            select_columns=select_columns,
            vector_metric=vector_metric,
            rrf_k=rrf_k,
            candidate_limit=candidate_limit,
            query_vec=query_vec,
            text_query=text_query,
            top_k=top_k,
            approximate=approximate,
        )
        if query_vec is not None:
            binds["query_vec"] = SyncOracleDriver.encode_vector(query_vec)
        df = await driver.execute_df(sql, binds)
        return [cls.sanitize_json(row_dict) for row_dict in df.to_dicts()]
