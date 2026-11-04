# PyO3 Native Extension Stubs (`_vertebra`, `_bundler`, `_storage`)

Low-level Rust/PyO3 classes and functions declared in PEP 561 `.pyi` stub files (`src/py/vertebra/_vertebra.pyi`, `_bundler.pyi`, `_storage.pyi`) and compiled with `gil_used = false` for free-threaded Python `3.14t`/`3.15t`. Most applications interact with these primitives through the higher-level [`vertebra`](vertebra.md), [`vertebra.server`](server.md), [`vertebra.db`](db.md), and [`vertebra.storage`](storage.md) facades.

| Native Module | Key Stub Classes | Backing Rust Crate |
| --- | --- | --- |
| **`vertebra._vertebra`** | `Request`, `Headers`, `Response`, `RequestBatch`, `AsgiScope`, `RsgiScope`, `WsgiEnviron` | `vertebra-py`, `vertebra-core`, `vertebra-http`, `vertebra-db` |
| **`vertebra._bundler`** | `BundleConfig`, `BuildResult`, `Bundler`, `SSRPool`, `IsolatedDeclarationsGenerator` | `vertebra-bundler` (Oxc, Rolldown, QuickJS) |
| **`vertebra._storage`** | `StorageBuffer`, `ObjectMeta`, `SyncObjectStore`, `AsyncObjectStore` | `vertebra-storage` (Rust `object_store` + Tokio) |

## `vertebra._vertebra` — HTTP/IPC Transport, Request Batches & Protocol Drivers

::: vertebra._vertebra.Request
    options:
      heading_level: 3

::: vertebra._vertebra.Headers
    options:
      heading_level: 3

::: vertebra._vertebra.Response
    options:
      heading_level: 3

::: vertebra._vertebra.RequestBatch
    options:
      heading_level: 3

::: vertebra._vertebra.AsgiScope
    options:
      heading_level: 3

::: vertebra._vertebra.RsgiScope
    options:
      heading_level: 4
      members: false

::: vertebra._vertebra.WsgiEnviron
    options:
      heading_level: 3

## `vertebra._bundler` — Embedded Rolldown/Oxc Bundler & QuickJS SSR Pool

::: vertebra._bundler.BundleConfig
    options:
      heading_level: 4
      members: false

::: vertebra._bundler.BuildResult
    options:
      heading_level: 4
      members: false

::: vertebra._bundler.Bundler
    options:
      heading_level: 3

::: vertebra._bundler.SSRPool
    options:
      heading_level: 3

::: vertebra._bundler.IsolatedDeclarationsGenerator
    options:
      heading_level: 3

## `vertebra._storage` — Rust `object_store` & Cloud Presigning Primitives

::: vertebra._storage.StorageBuffer
    options:
      heading_level: 4

::: vertebra._storage.ObjectMeta
    options:
      heading_level: 4
      members: false

::: vertebra._storage.SyncObjectStore
    options:
      heading_level: 3
      members:
        - put
        - get
        - head
        - delete
        - list
        - copy
        - presign_get
        - presign_put

::: vertebra._storage.AsyncObjectStore
    options:
      heading_level: 3
      members:
        - put
        - get
        - head
        - delete
        - list
        - copy
        - presign_get
        - presign_put
