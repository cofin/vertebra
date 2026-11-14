"""Unit tests for PostgreSQL libpq option resolution and lexical bind compiler."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from vertebra._vertebra import VectorBatchView
from vertebra.db.adapters.postgres import PostgresClientOptions, PostgresTarget
from vertebra.db.adapters.postgres.binds import (
    PG_JSONB_TEXT,
    compile_postgres_binds,
    normalize_postgres_parameter,
)
from vertebra.db.adapters.postgres.config import PostgreSQLConfig
from vertebra.db.adapters.postgres.core import PostgreSQLCore
from vertebra.db.adapters.postgres.options import (
    build_native_client_options,
    parse_libpq_conninfo,
    parse_postgres_dsn,
)
from vertebra.db.adapters.postgres.types import (
    PG_UNCHANGED_TOAST,
    PgArray,
    PgComposite,
    PgJson,
    PgParameter,
    PgVector,
)
from vertebra.db.driver import ProgrammingError
from vertebra.typing import ImproperConfigurationError


def test_parse_uri_preserves_plus_and_decodes_unix_socket_percent_encoding() -> None:
    """Percent-decoding decodes %2F without converting literal '+' into spaces."""
    cfg = PostgreSQLConfig.from_url(
        "postgresql://alice+tag:sec+ret%2B1@%2Fvar%2Frun%2Fpostgresql/my+db?application_name=app+worker"
    )
    opts = build_native_client_options(cfg, env={})
    assert isinstance(opts, PostgresClientOptions)
    assert isinstance(opts.targets[0], PostgresTarget)
    assert opts.user == "alice+tag"
    assert opts.password == "sec+ret+1"
    assert opts.database == "my+db"
    assert opts.host == "localhost"
    assert opts.unix_socket == "/var/run/postgresql/.s.PGSQL.5432"
    assert opts.sslmode == "disable"
    assert opts.application_name == "app+worker"


def test_unix_socket_directory_and_exact_socket_normalization() -> None:
    """Directory paths append .s.PGSQL.<port>, while exact .s.PGSQL.<port> paths stay intact."""
    cfg_dir = PostgreSQLConfig(dsn="postgresql://postgres@/app?host=/tmp/pgsock&port=5433")
    opts_dir = build_native_client_options(cfg_dir, env={})
    assert opts_dir.host == "localhost"
    assert opts_dir.port == 5433
    assert opts_dir.unix_socket == "/tmp/pgsock/.s.PGSQL.5433"
    assert opts_dir.sslmode == "disable"

    cfg_exact = PostgreSQLConfig(dsn="postgresql://postgres@/app?host=/tmp/pgsock/.s.PGSQL.5435&port=5435")
    opts_exact = build_native_client_options(cfg_exact, env={})
    assert opts_exact.unix_socket == "/tmp/pgsock/.s.PGSQL.5435"

    with pytest.raises(ImproperConfigurationError, match="incompatible with Unix socket"):
        build_native_client_options(cfg_dir, env={}, sslmode="require")


def test_env_precedence_and_empty_user_dbname_rules(monkeypatch: pytest.MonkeyPatch) -> None:
    """Explicit overrides > DSN > PG* env vars > OS user / resolved user defaults."""
    monkeypatch.setattr("getpass.getuser", lambda: "os_alice")
    env = {
        "PGHOST": "env-db.internal",
        "PGPORT": "6432",
        "PGUSER": "env_user",
        "PGDATABASE": "env_db",
        "PGPASSWORD": "env_pw",
        "PGSSLMODE": "verify-ca",
        "PGAPPNAME": "env_app",
    }

    minimal = build_native_client_options(
        PostgreSQLConfig(dsn="postgresql:///"),
        env=env,
    )
    assert minimal.host == "env-db.internal"
    assert minimal.port == 6432
    assert minimal.user == "env_user"
    assert minimal.database == "env_db"
    assert minimal.password == "env_pw"
    assert minimal.sslmode == "verify-ca"
    assert minimal.application_name == "env_app"

    from_dsn = build_native_client_options(
        PostgreSQLConfig(dsn="postgresql://dsn_user@dsn-host:5544/"),
        env=env,
    )
    assert from_dsn.host == "dsn-host"
    assert from_dsn.port == 5544
    assert from_dsn.user == "dsn_user"
    assert from_dsn.database == "env_db"

    os_fallback = build_native_client_options(
        PostgreSQLConfig(dsn="postgresql:///"),
        env={},
    )
    assert os_fallback.user == "os_alice"
    assert os_fallback.database == "os_alice"
    assert os_fallback.sslmode == "prefer"

    with pytest.raises(ImproperConfigurationError, match="user cannot be an empty"):
        build_native_client_options(PostgreSQLConfig(dsn="postgresql://localhost/app?user="), env={})
    with pytest.raises(ImproperConfigurationError, match="dbname/database cannot be an empty"):
        build_native_client_options(PostgreSQLConfig(dsn="postgresql://u@localhost/?dbname="), env={})


def test_libpq_keyword_conninfo_multihost_and_pem_loading(tmp_path: Path) -> None:
    """Parse quoted libpq conninfo, multi-host/IPv6 targets, PEM certs, and PoolerMode."""
    ca_file = tmp_path / "ca.pem"
    cert_file = tmp_path / "client.crt"
    key_file = tmp_path / "client.key"
    ca_file.write_bytes(b"---CA---")
    cert_file.write_bytes(b"---CERT---")
    key_file.write_bytes(b"---KEY---")

    conninfo = (
        f"host='[::1],db2.internal' port=5433,5434 user='bob\\'s' dbname=prod "
        f"sslmode=verify-full sslrootcert='{ca_file}' sslcert='{cert_file}' sslkey='{key_file}'"
    )
    parsed_kv = parse_libpq_conninfo(conninfo)
    assert parsed_kv["user"] == "bob's"

    cfg = PostgreSQLConfig(dsn=conninfo, pgbouncer=True)
    assert cfg.pool_key()[:5] == ("postgresql", "[::1]", 5433, "prod", "bob's")

    uri_cfg = PostgreSQLConfig(
        dsn="postgresql://bob@[::1]:5433,db2.internal:5434/prod?sslmode=verify-full",
        pgbouncer=True,
    )
    uri_opts = build_native_client_options(
        uri_cfg,
        env={},
        sslrootcert=str(ca_file),
        sslcert=str(cert_file),
        sslkey=str(key_file),
    )
    assert len(uri_opts.targets) == 2
    assert (uri_opts.targets[0].host, uri_opts.targets[0].port) == ("::1", 5433)
    assert (uri_opts.targets[1].host, uri_opts.targets[1].port) == ("db2.internal", 5434)
    assert uri_opts.for_target(1).host == "db2.internal"
    assert uri_opts.ca_pem == b"---CA---"
    assert uri_opts.client_cert_pem == b"---CERT---"
    assert uri_opts.client_key_pem == b"---KEY---"
    assert uri_opts.to_pool_kwargs()["pooler_mode"] == "transaction"
    assert uri_opts.to_pool_kwargs()["statement_cache_size"] == 0

    with pytest.raises(FileNotFoundError):
        build_native_client_options(
            PostgreSQLConfig(dsn="postgresql://bob@localhost/prod"),
            env={},
            sslrootcert=str(tmp_path / "missing-ca.pem"),
        )


def test_invalid_utf8_percent_and_mixed_host_targets_rejected() -> None:
    """Reject invalid UTF-8 escapes, missing port parity, and mixed TCP/Unix targets."""
    with pytest.raises(ImproperConfigurationError, match="UTF-8"):
        parse_postgres_dsn("postgresql://user@localhost/bad%FFdb")

    with pytest.raises(ImproperConfigurationError, match="Cannot mix TCP hosts and Unix"):
        build_native_client_options(
            PostgreSQLConfig(dsn="postgresql://user@localhost/db?host=/var/run/postgresql,db2"),
            env={},
        )

    with pytest.raises(ImproperConfigurationError, match="Multi-host target count"):
        build_native_client_options(
            PostgreSQLConfig(dsn="postgresql://user@localhost/db?host=h1,h2,h3&port=5432,5433"),
            env={},
        )

    with pytest.raises(ImproperConfigurationError, match="channel_binding='require'"):
        build_native_client_options(
            PostgreSQLConfig(dsn="postgresql://user@localhost/db?sslmode=disable&channel_binding=require"),
            env={},
        )


@pytest.mark.parametrize(
    ("query_or_overrides", "expected_field", "expected_value"),
    [
        (
            {"dsn": "postgresql://alice@localhost/app?cloud_sql_instance=p:r:i&cloud_sql_ip_type=PUBLIC"},
            "cloud_sql_ip_type",
            "PUBLIC",
        ),
        (
            {
                "dsn": "postgresql://alice@localhost/app?alloydb_instance_uri=projects/p/locations/r/clusters/c/instances/i&alloydb_ip_type=PSC"
            },
            "alloydb_ip_type",
            "PSC",
        ),
    ],
)
def test_cloud_sql_and_alloydb_options_and_rejections(
    query_or_overrides: dict[str, Any],
    expected_field: str,
    expected_value: str,
) -> None:
    """Validate Cloud SQL/AlloyDB IP types, native kwargs projection, and Unix socket rejection."""
    cfg = PostgreSQLConfig.from_url(query_or_overrides["dsn"])
    opts = build_native_client_options(cfg, env={})
    assert getattr(opts, expected_field) == expected_value
    assert opts.to_client_kwargs()[expected_field] == expected_value
    assert opts.to_pool_kwargs()[expected_field] == expected_value


@pytest.mark.parametrize(
    ("dsn", "overrides", "match_pattern"),
    [
        (
            "postgresql://alice@/app?host=/var/run/postgresql&cloud_sql_instance=p:r:i",
            {},
            "Unix domain socket transport",
        ),
        (
            "postgresql://alice@/app?host=/var/run/postgresql&alloydb_instance_uri=projects/p/locations/r/clusters/c/instances/i",
            {},
            "Unix domain socket transport",
        ),
        (
            "postgresql://alice@localhost/app?cloud_sql_instance=p:r:i",
            {"cloud_sql_ip_type": "INTERNAL"},
            "cloud_sql_ip_type",
        ),
        (
            "postgresql://alice@localhost/app?alloydb_instance_uri=projects/p/locations/r/clusters/c/instances/i",
            {"alloydb_ip_type": "ANYCAST"},
            "alloydb_ip_type",
        ),
    ],
)
def test_cloud_sql_and_alloydb_invalid_combinations_rejected(
    dsn: str,
    overrides: dict[str, Any],
    match_pattern: str,
) -> None:
    """Reject Unix socket transport and invalid IP types for Cloud SQL / AlloyDB."""
    cfg = PostgreSQLConfig.from_url(dsn)
    with pytest.raises(ImproperConfigurationError, match=match_pattern):
        build_native_client_options(cfg, env={}, **overrides)


@pytest.mark.parametrize(
    ("sql", "params", "expected_sql", "expected_params", "expected_style"),
    [
        (
            "SELECT $1, $2::jsonb, '%% literal %s', $$body %s $1$$ FROM t",
            ["hi", {"a": 1}],
            "SELECT $1, $2::jsonb, '%% literal %s', $$body %s $1$$ FROM t",
            ("hi", {"a": 1}),
            "numeric",
        ),
        (
            "SELECT 100 %% 3, 'it\\'s ok'",
            None,
            "SELECT 100 %% 3, 'it\\'s ok'",
            (),
            "none",
        ),
        (
            "SELECT %s, 100 %% 3, $tag$%s %(x)s$tag$, E'it\\'s %s' FROM \"col %s\" WHERE doc ? 'k' AND doc ?| %s",
            ["alpha", ["a", "b"]],
            "SELECT $1, 100 % 3, $tag$%s %(x)s$tag$, E'it\\'s %s' FROM \"col %s\" WHERE doc ? 'k' AND doc ?| $2",
            ("alpha", ["a", "b"]),
            "pyformat",
        ),
        (
            "/* outer /* inner %s */ */ -- line %s\nINSERT INTO items VALUES (%(id)s, %(label)s, %(id)s)",
            {"id": 42, "label": "ok", "unused": "ignored"},
            "/* outer /* inner %s */ */ -- line %s\nINSERT INTO items VALUES ($1, $2, $1)",
            (42, "ok"),
            "named",
        ),
        (
            "SELECT foo$1, %s",
            [99],
            "SELECT foo$1, $1",
            (99,),
            "pyformat",
        ),
    ],
)
def test_compile_postgres_binds_valid_cases(
    sql: str,
    params: Any,
    expected_sql: str,
    expected_params: tuple[Any, ...],
    expected_style: str,
) -> None:
    """Verify single-pass SQL lexical compilation across literals, comments, casts, and JSONB ops."""
    compiled = PostgreSQLCore.compile_binds(sql, params)
    assert compiled.sql == expected_sql
    assert compiled.params == expected_params
    assert compiled.style == expected_style


@pytest.mark.parametrize(
    ("sql", "params"),
    [
        ("SELECT %s, %(name)s", ["a"]),
        ("SELECT %(name)s, %s", {"name": "a"}),
        ("SELECT $1, %s", [1, 2]),
        ("SELECT %s, $1", [1, 2]),
        ("SELECT %(a)s, $1", {"a": 1}),
        ("SELECT 'it\\'s %s'", ["bad"]),
        ("SELECT %s, 'it\\'s'", ["bad"]),
        ("SELECT $1", {"a": 1}),
        ("SELECT %s", {"a": 1}),
        ("SELECT %(a)s", ["a"]),
        ("SELECT %(missing)s", {"present": 1}),
        ("SELECT %s, %s", [1]),
        ("SELECT $2", [1]),
        ("SELECT $0", [1]),
        ("SELECT %s", "not-a-sequence"),
        ("SELECT 1", [1]),
    ],
)
def test_compile_postgres_binds_rejects_invalid_placeholders_and_params(
    sql: str,
    params: Any,
) -> None:
    """Verify ProgrammingError on mixed styles, ambiguous backslash quote, and shape mismatches."""
    with pytest.raises(ProgrammingError):
        compile_postgres_binds(sql, params)


def test_parameter_normalization_copies_mutable_buffers_and_ffi_views() -> None:
    """Normalize bytearray, memoryview, VectorView, VectorBatchView, PgArray, PgComposite, PgJson, PgParameter."""
    raw_ba = bytearray(b"hello")
    raw_mv = memoryview(bytearray(b"world"))
    json_payload: dict[str, Any] = {"nested": [1, 2]}
    batch = VectorBatchView.from_f32_rows([[1.0, 0.0], [0.0, 1.0]], normalize=False)
    vec = batch[0]
    arr_values: tuple[Any, ...] = (bytearray(b"elem"),)
    comp_values: tuple[Any, ...] = (bytearray(b"c"),)
    arr = PgArray(
        element_oid=17,
        dimensions=(1,),
        lower_bounds=(1,),
        values=arr_values,
    )
    comp = PgComposite(
        type_oid=16384,
        field_names=("raw",),
        field_oids=(17,),
        values=comp_values,
    )

    compiled = compile_postgres_binds(
        "INSERT INTO docs VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
        [
            raw_ba,
            raw_mv,
            PgJson(json_payload),
            vec,
            batch,
            arr,
            comp,
            PgParameter({"x": 1}, PG_JSONB_TEXT),
        ],
    )
    raw_ba[:] = b"XXXXX"
    assert isinstance(raw_mv.obj, bytearray)
    raw_mv.obj[:] = b"YYYYY"
    json_payload["nested"].append(999)

    out = PostgreSQLCore.normalize_parameters(compiled.params)
    assert type(out[0]) is bytes and out[0] == b"hello"
    assert type(out[1]) is bytes and out[1] == b"world"
    assert isinstance(out[2], PgJson) and out[2].value == {"nested": [1, 2]}
    assert isinstance(out[3], PgVector) and out[3].values == (1.0, 0.0)
    assert isinstance(out[4], tuple) and len(out[4]) == 2
    assert isinstance(out[4][0], PgVector)
    assert isinstance(out[5], PgArray) and type(out[5].values[0]) is bytes
    assert isinstance(out[6], PgComposite) and type(out[6].values[0]) is bytes
    assert isinstance(out[7], PgParameter) and out[7].type_oid == PG_JSONB_TEXT
    assert out[7].value == '{"x":1}'

    with pytest.raises(TypeError, match="PG_UNCHANGED_TOAST"):
        normalize_postgres_parameter(PG_UNCHANGED_TOAST)
