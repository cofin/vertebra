# vertebra-redis

Native Rust Redis 7.2+/8.0 client and pool extension for [Vertebra](https://cofin.github.io/vertebra/).

This package contains the compiled `vertebra._redis` module that Vertebra's Redis drivers run on. You don't import it directly. Install it through the `vertebra-core` extra so both packages stay on the same version:

```bash
pip install "vertebra-core[redis]"
uv add "vertebra-core[redis]"
```

Without it, the Redis drivers raise `MissingDependencyError`.

## Features

- RESP3 client (`HELLO 3`) with atomic pipelines.
- Client-side caching with `CLIENT TRACKING` invalidation.
- Sentinel and Cluster support.
- Backs Vertebra's Redis cache, queue, and channel backends.

## Usage

| | |
| --- | --- |
| Native config (`vertebra.db.adapters.redis`) | `RedisConfig` |
| Drivers | `SyncRedisDriver`, `AsyncRedisDriver` |

## Compatibility

- Python 3.12 to 3.15, including free-threaded 3.14t and 3.15t.
- Prebuilt wheels for Linux (x86_64, aarch64; glibc 2.28+), macOS (x86_64, arm64), and Windows (x64).

## Links

- [Documentation](https://cofin.github.io/vertebra/)
- [Databases guide](https://cofin.github.io/vertebra/usage/databases/)
- [Source](https://github.com/cofin/vertebra/tree/main/src/rs/vertebra-redis)
- [Issues](https://github.com/cofin/vertebra/issues)
- [Changelog](https://cofin.github.io/vertebra/changelog/)
