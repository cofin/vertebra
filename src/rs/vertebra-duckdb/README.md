# vertebra-duckdb

Native Rust DuckDB client extension for [Vertebra](https://cofin.github.io/vertebra/).

This package contains the compiled `vertebra._duckdb` module that Vertebra's DuckDB drivers run on. You don't import it directly. Install it through the `vertebra-core` extra so both packages stay on the same version:

```bash
pip install "vertebra-core[duckdb]"
uv add "vertebra-core[duckdb]"
```

Without it, the DuckDB drivers raise `MissingDependencyError`.

## Features

- Embedded DuckDB with single-writer, multi-reader access and savepoints.
- Zero-copy Arrow results over shared memory, plus the DuckDB `Appender` for bulk loads.
- Pipelines (`SyncDuckDBPipeline`, `AsyncDuckDBPipeline`) and rotating GCS bearer secrets for `httpfs`.
- Add `vertebra-core[sqlalchemy-duckdb]` for SQLAlchemy `duckdb+vtb://` URLs.

## Usage

| | |
| --- | --- |
| SQLSpec configs (`vertebra.extensions.sqlspec`) | `DuckDBSyncConfig`, `DuckDBAsyncConfig` |
| Native config (`vertebra.db.adapters.duckdb`) | `DuckDBConfig` |
| Drivers | `SyncDuckDBDriver`, `AsyncDuckDBDriver` |

## Compatibility

- Python 3.12 to 3.15, including free-threaded 3.14t and 3.15t.
- Prebuilt wheels for Linux (x86_64, aarch64; glibc 2.28+), macOS (x86_64, arm64), and Windows (x64).

## Links

- [Documentation](https://cofin.github.io/vertebra/)
- [Databases guide](https://cofin.github.io/vertebra/usage/databases/)
- [Source](https://github.com/cofin/vertebra/tree/main/src/rs/vertebra-duckdb)
- [Issues](https://github.com/cofin/vertebra/issues)
- [Changelog](https://cofin.github.io/vertebra/changelog/)
