# vertebra-storage

Multi-cloud object storage extension for [Vertebra](https://cofin.github.io/vertebra/).

This package contains the compiled `vertebra._storage` module. It powers `vertebra.storage`. Install it through the `vertebra-core` extra so both packages stay on the same version:

```bash
pip install "vertebra-core[storage]"
uv add "vertebra-core[storage]"
```

## Features

- One interface for Amazon S3, Google Cloud Storage, Azure Blob Storage, Cloudflare R2, OCI Object Storage, HTTP, local files, and in-memory stores.
- `SyncObjectStore` releases the GIL during I/O. `AsyncObjectStore` completes on the event loop.
- Signed URLs, conditional writes, and streaming uploads.
- Add the `arrow` extra (`vertebra-core[storage,arrow]`) for Arrow IPC and Parquet methods.

## Usage

```python
from vertebra.storage import MemoryConfig, SyncObjectStore

store = SyncObjectStore(MemoryConfig(namespace="uploads"))
store.put_stream("reports/day.txt", [b"hello ", b"world"], if_not_exists=True)
store.rename("reports/day.txt", "reports/latest.txt", overwrite=False)
payload = store.get("reports/latest.txt")
```

## Compatibility

- Python 3.12 to 3.15, including free-threaded builds.
- Prebuilt wheels for Linux (x86_64, aarch64; glibc 2.28+), macOS (x86_64, arm64), and Windows (x64).

## Links

- [Documentation](https://cofin.github.io/vertebra/)
- [Object storage guide](https://cofin.github.io/vertebra/usage/storage/)
- [Source](https://github.com/cofin/vertebra/tree/main/src/rs/vertebra-storage)
- [Issues](https://github.com/cofin/vertebra/issues)
- [Changelog](https://cofin.github.io/vertebra/changelog/)
