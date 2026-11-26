# vertebra-spanner

Native Rust Google Cloud Spanner client extension for [Vertebra](https://cofin.github.io/vertebra/).

This package contains the compiled `vertebra._spanner` module that Vertebra's Google Cloud Spanner drivers run on. You don't import it directly. Install it through the `vertebra-core` extra so both packages stay on the same version:

```bash
pip install "vertebra-core[spanner]"
uv add "vertebra-core[spanner]"
```

Without it, the Google Cloud Spanner drivers raise `MissingDependencyError`.

## Features

- GoogleSQL and PostgreSQL-dialect databases, built on the official `google-cloud-spanner` Rust client.
- Multiplexed sessions, automatic retry of aborted transactions, and `BatchWrite`.
- Data Boost partitioned reads into Arrow with `select_to_arrow`.
- Change stream sources, and support for the Spanner Emulator.

## Usage

| | |
| --- | --- |
| SQLSpec configs (`vertebra.extensions.sqlspec`) | `SpannerSyncConfig`, `SpannerAsyncConfig`, `SpangresSyncConfig`, `SpangresAsyncConfig` |
| Native config (`vertebra.db.adapters.spanner`) | `SpannerConfig` |
| Drivers | `SyncSpannerDriver`, `AsyncSpannerDriver` |

## Compatibility

- Python 3.12 to 3.15, including free-threaded 3.14t and 3.15t.
- Prebuilt wheels for Linux (x86_64, aarch64; glibc 2.28+), macOS (x86_64, arm64), and Windows (x64).

## Links

- [Documentation](https://cofin.github.io/vertebra/)
- [Databases guide](https://cofin.github.io/vertebra/usage/databases/)
- [Source](https://github.com/cofin/vertebra/tree/main/src/rs/vertebra-spanner)
- [Issues](https://github.com/cofin/vertebra/issues)
- [Changelog](https://cofin.github.io/vertebra/changelog/)
