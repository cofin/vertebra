# vertebra-valkey

Valkey 8.0+/9.0 client, Cluster, Sentinel, Pub/Sub, Streams, and Memorystore IAM extension for [Vertebra](https://cofin.github.io/vertebra/).

This package contains the compiled `vertebra._valkey` module that Vertebra's Valkey drivers run on. You don't import it directly. Install it through the `vertebra-core` extra so both packages stay on the same version:

```bash
pip install "vertebra-core[valkey]"
uv add "vertebra-core[valkey]"
```

Without it, the Valkey drivers raise `MissingDependencyError`.

## Features

- RESP3 client with client-side caching.
- Valkey 9.0 hash-field expiration (`hexpire`, `httl`) and `CLIENT CAPA redirect`.
- Cluster, Sentinel, Pub/Sub, and Streams.
- Google Cloud Memorystore IAM authentication.

## Usage

| | |
| --- | --- |
| Native config (`vertebra.db.adapters.valkey`) | `ValkeyConfig` |
| Drivers | `SyncValkeyDriver`, `AsyncValkeyDriver` |

## Compatibility

- Python 3.12 to 3.15, including free-threaded builds.
- Prebuilt wheels for Linux (x86_64, aarch64; glibc 2.28+), macOS (x86_64, arm64), and Windows (x64).

## Links

- [Documentation](https://cofin.github.io/vertebra/)
- [Databases guide](https://cofin.github.io/vertebra/usage/databases/)
- [Source](https://github.com/cofin/vertebra/tree/main/src/rs/vertebra-valkey)
- [Issues](https://github.com/cofin/vertebra/issues)
- [Changelog](https://cofin.github.io/vertebra/changelog/)
