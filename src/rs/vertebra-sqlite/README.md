# vertebra-sqlite

Native Rust SQLite client extension for [Vertebra](https://cofin.github.io/vertebra/).

This package contains the compiled `vertebra._sqlite` module that Vertebra's SQLite drivers run on. You don't import it directly. Install it through the `vertebra-core` extra so both packages stay on the same version:

```bash
pip install "vertebra-core[sqlite]"
uv add "vertebra-core[sqlite]"
```

Without it, the SQLite drivers raise `MissingDependencyError`.

## Features

- Pure Rust driver with SQLite 3.45+ bundled. No system SQLite required.
- WAL-mode connection pooling, `mmap_size`, and binary `JSONB`.
- Online `backup_to` and `restore_from`, pipelines, and Arrow result streaming.

## Usage

| | |
| --- | --- |
| SQLSpec configs (`vertebra.extensions.sqlspec`) | `SQLiteSyncConfig`, `SQLiteAsyncConfig` |
| Native config (`vertebra.db.adapters.sqlite`) | `SQLiteConfig` |
| Drivers | `SyncSQLiteDriver`, `AsyncSQLiteDriver` |

## Compatibility

- Python 3.12 to 3.15, including free-threaded 3.14t and 3.15t.
- Prebuilt wheels for Linux (x86_64, aarch64; glibc 2.28+), macOS (x86_64, arm64), and Windows (x64).

## Links

- [Documentation](https://cofin.github.io/vertebra/)
- [Databases guide](https://cofin.github.io/vertebra/usage/databases/)
- [Source](https://github.com/cofin/vertebra/tree/main/src/rs/vertebra-sqlite)
- [Issues](https://github.com/cofin/vertebra/issues)
- [Changelog](https://cofin.github.io/vertebra/changelog/)
