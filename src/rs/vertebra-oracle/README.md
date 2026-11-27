# vertebra-oracle

Oracle Thin 26ai client and pool extension for [Vertebra](https://cofin.github.io/vertebra/).

This package contains the compiled `vertebra._oracle` module that Vertebra's Oracle drivers run on. You don't import it directly. Install it through the `vertebra-core` extra so both packages stay on the same version:

```bash
pip install "vertebra-core[oracle]"
uv add "vertebra-core[oracle]"
```

Without it, the Oracle drivers raise `MissingDependencyError`.

## Features

- Rust Thin protocol. No Oracle Instant Client required.
- Connection pools with DRCP support (`cclass`, `purity`, `connection_tag`).
- Autonomous Database wallet mTLS and IAM token authentication.
- 26ai `VECTOR` and `OSON` binary JSON types, Arrow reads and writes, and change notifications.

## Usage

| | |
| --- | --- |
| SQLSpec configs (`vertebra.extensions.sqlspec`) | `OracleSyncConfig`, `OracleAsyncConfig` |
| Native config (`vertebra.db.adapters.oracle`) | `OracleConfig` |
| Drivers | `SyncOracleDriver`, `AsyncOracleDriver` |

## Compatibility

- Python 3.12 to 3.15, including free-threaded builds.
- Prebuilt wheels for Linux (x86_64, aarch64; glibc 2.28+), macOS (x86_64, arm64), and Windows (x64).

## Links

- [Documentation](https://cofin.github.io/vertebra/)
- [Databases guide](https://cofin.github.io/vertebra/usage/databases/)
- [Source](https://github.com/cofin/vertebra/tree/main/src/rs/vertebra-oracle)
- [Issues](https://github.com/cofin/vertebra/issues)
- [Changelog](https://cofin.github.io/vertebra/changelog/)
