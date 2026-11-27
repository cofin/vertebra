# vertebra-mysql

MySQL 8.4/9 and MariaDB client extension for [Vertebra](https://cofin.github.io/vertebra/).

This package contains the compiled `vertebra._mysql` module that Vertebra's MySQL drivers run on. You don't import it directly. Install it through the `vertebra-core` extra so both packages stay on the same version:

```bash
pip install "vertebra-core[mysql]"
uv add "vertebra-core[mysql]"
```

Without it, the MySQL drivers raise `MissingDependencyError`.

## Features

- Rust binary protocol with server-side prepared statements. No `libmysqlclient` required.
- Query pipelines and Arrow result streaming.
- Out-of-band `KILL QUERY` cancellation.
- Cloud SQL IAM token auth on `:3306` or the `:3307` mTLS dialer.

## Usage

| | |
| --- | --- |
| SQLSpec configs (`vertebra.extensions.sqlspec`) | `MySQLSyncConfig`, `MySQLAsyncConfig` |
| Native config (`vertebra.db.adapters.mysql`) | `MySQLConfig` |
| Drivers | `SyncMySQLDriver`, `AsyncMySQLDriver` |

## Compatibility

- Python 3.12 to 3.15, including free-threaded builds.
- Prebuilt wheels for Linux (x86_64, aarch64; glibc 2.28+), macOS (x86_64, arm64), and Windows (x64).

## Links

- [Documentation](https://cofin.github.io/vertebra/)
- [Databases guide](https://cofin.github.io/vertebra/usage/databases/)
- [Source](https://github.com/cofin/vertebra/tree/main/src/rs/vertebra-mysql)
- [Issues](https://github.com/cofin/vertebra/issues)
- [Changelog](https://cofin.github.io/vertebra/changelog/)
