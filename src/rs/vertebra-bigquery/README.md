# vertebra-bigquery

Google BigQuery client extension for [Vertebra](https://cofin.github.io/vertebra/).

This package contains the compiled `vertebra._bigquery` module that Vertebra's Google BigQuery drivers run on. You don't import it directly. Install it through the `vertebra-core` extra so both packages stay on the same version:

```bash
pip install "vertebra-core[bigquery]"
uv add "vertebra-core[bigquery]"
```

Without it, the Google BigQuery drivers raise `MissingDependencyError`.

## Features

- BigQuery REST v2 queries and Storage Read/Write v1 streams, built on the official `google-cloud-bigquery` Rust client.
- `select_to_arrow` and `load_from_arrow` move Arrow data through shared memory without copying it.
- Dry-run cost ceilings and object tables.

## Usage

| | |
| --- | --- |
| SQLSpec configs (`vertebra.extensions.sqlspec`) | `BigQuerySyncConfig`, `BigQueryAsyncConfig` |
| Native config (`vertebra.db.adapters.bigquery`) | `BigQueryConfig` |
| Drivers | `SyncBigQueryDriver`, `AsyncBigQueryDriver` |

## Compatibility

- Python 3.12 to 3.15, including free-threaded builds.
- Prebuilt wheels for Linux (x86_64, aarch64; glibc 2.28+), macOS (x86_64, arm64), and Windows (x64).

## Links

- [Documentation](https://cofin.github.io/vertebra/)
- [Databases guide](https://cofin.github.io/vertebra/usage/databases/)
- [Source](https://github.com/cofin/vertebra/tree/main/src/rs/vertebra-bigquery)
- [Issues](https://github.com/cofin/vertebra/issues)
- [Changelog](https://cofin.github.io/vertebra/changelog/)
