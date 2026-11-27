# vertebra-postgres

PostgreSQL Protocol 3.0 client and pool extension for [Vertebra](https://cofin.github.io/vertebra/).

This package contains the compiled `vertebra._postgres` module that Vertebra's PostgreSQL drivers run on. You don't import it directly. Install it through the `vertebra-core` extra so both packages stay on the same version:

```bash
pip install "vertebra-core[postgres]"
uv add "vertebra-core[postgres]"
```

Without it, the PostgreSQL drivers raise `MissingDependencyError`.

## Features

- Rust Protocol 3.0 client. No `libpq` required.
- Single round-trip `pipeline()`, binary `COPY`, `pgvector`, and `JSONB`.
- Logical replication change streams.
- Works behind PgBouncer, PgCat, and pgdog, with Cloud SQL and AlloyDB dialers and IAM auth.

## Usage

| | |
| --- | --- |
| SQLSpec configs (`vertebra.extensions.sqlspec`) | `PostgresSyncConfig`, `PostgresAsyncConfig` |
| Native config (`vertebra.db.adapters.postgres`) | `PostgreSQLConfig` |
| Drivers | `SyncPostgreSQLDriver`, `AsyncPostgreSQLDriver` |

## Compatibility

- Python 3.12 to 3.15, including free-threaded builds.
- Prebuilt wheels for Linux (x86_64, aarch64; glibc 2.28+), macOS (x86_64, arm64), and Windows (x64).

## Links

- [Documentation](https://cofin.github.io/vertebra/)
- [Databases guide](https://cofin.github.io/vertebra/usage/databases/)
- [Source](https://github.com/cofin/vertebra/tree/main/src/rs/vertebra-postgres)
- [Issues](https://github.com/cofin/vertebra/issues)
- [Changelog](https://cofin.github.io/vertebra/changelog/)
