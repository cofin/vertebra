# vertebra-mssql

Native pure-Rust Microsoft SQL Server (TDS 7.4) client extension for [Vertebra](https://cofin.github.io/vertebra/).

This package contains the compiled `vertebra._mssql` module that Vertebra's Microsoft SQL Server drivers run on. You don't import it directly. Install it through the `vertebra-core` extra so both packages stay on the same version:

```bash
pip install "vertebra-core[mssql]"
uv add "vertebra-core[mssql]"
```

Without it, the Microsoft SQL Server drivers raise `MissingDependencyError`.

## Features

- Pure Rust TDS 7.4 protocol for SQL Server 2012 and later, with TLS-in-TDS encryption or a TLS-first `strict` mode. No ODBC driver or FreeTDS required.
- `?` and `$1` placeholders are rewritten to `sp_executesql` `@P1` parameters.
- `BulkLoad` inserts, `ATTENTION` query cancellation, pipelines, and Arrow result streaming.
- Cloud SQL for SQL Server over the `:3307` mTLS dialer.

## Usage

| | |
| --- | --- |
| SQLSpec configs (`vertebra.extensions.sqlspec`) | `MSSQLSyncConfig`, `MSSQLAsyncConfig` |
| Native config (`vertebra.db.adapters.mssql`) | `MSSQLConfig` |
| Drivers | `SyncMSSQLDriver`, `AsyncMSSQLDriver` |

## Compatibility

- Python 3.12 to 3.15, including free-threaded 3.14t and 3.15t.
- Prebuilt wheels for Linux (x86_64, aarch64; glibc 2.28+), macOS (x86_64, arm64), and Windows (x64).

## Links

- [Documentation](https://cofin.github.io/vertebra/)
- [Databases guide](https://cofin.github.io/vertebra/usage/databases/)
- [Source](https://github.com/cofin/vertebra/tree/main/src/rs/vertebra-mssql)
- [Issues](https://github.com/cofin/vertebra/issues)
- [Changelog](https://cofin.github.io/vertebra/changelog/)
