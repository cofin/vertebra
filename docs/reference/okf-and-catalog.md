# OKF v0.2 & Google Cloud Knowledge Catalog

`vertebra.agent.okf` parses Open Knowledge Format (OKF v0.2) bundles in Python with no `PyYAML` dependency. It validates truth blocks and footnotes, computes SHA-256 digests, and writes reproducible ZIP archives.

`vertebra.agent.knowledge_catalog` connects to Google Cloud Dataplex Universal Catalog (`dataplex.googleapis.com/v1`) over `GcpTokenCache` and HTTP/2. Sync and async clients resolve `context-set` entry groups, upload changed `OkfBundle` entries in batches of up to 250 by digest, and search entries via `locations/global:searchEntries`. See [Agent Runtime](agent.md) for MCP, A2A, and workflow APIs.

## Quick Example

```python
from pathlib import Path

from vertebra.agent import (
    AsyncKnowledgeCatalogClient,
    KnowledgeCatalogConfig,
    OkfBundle,
)

bundle = OkfBundle.from_directory(Path(".agents/bundles"))
archive_bytes = bundle.to_zip_bytes()

client = AsyncKnowledgeCatalogClient(
    KnowledgeCatalogConfig(
        project_id="my-gcp-project",
        location="us-central1",
        entry_group_id="okf-context",
    )
)
await client.resolve_context_set(entry_group_id="okf-context")
summary = await client.sync_bundle(bundle, entry_group_id="okf-context")
matches = await client.search_entries(
    "protocol harness",
    entry_group_id="okf-context",
)
```

## OKF v0.2 Bundle Serializer

::: vertebra.agent.okf.OkfSource
    options:
      heading_level: 4
      members: false

::: vertebra.agent.okf.OkfDocument
    options:
      heading_level: 4
      members:
        - parse
        - to_markdown
        - to_bytes

::: vertebra.agent.okf.OkfBundle
    options:
      heading_level: 3
      members:
        - from_directory
        - from_zip
        - export_directory
        - export_zip
        - to_zip_bytes
        - digest
        - get

::: vertebra.agent.okf.OkfValidationError
    options:
      heading_level: 4
      members: false

## Knowledge Catalog Clients

::: vertebra.agent.knowledge_catalog.KnowledgeCatalogConfig
    options:
      heading_level: 4
      members: false

::: vertebra.agent.knowledge_catalog.CatalogEntryRecord
    options:
      heading_level: 4
      members:
        - from_dataplex_dict

::: vertebra.agent.knowledge_catalog.CatalogSyncSummary
    options:
      heading_level: 4
      members: false

::: vertebra.agent.knowledge_catalog.CatalogSyncError
    options:
      heading_level: 4
      members: false

::: vertebra.agent.knowledge_catalog.SyncKnowledgeCatalogClient
    options:
      heading_level: 3
      members:
        - get_entry_group
        - list_entry_groups
        - create_entry_group
        - resolve_context_set
        - get_entry
        - list_entries
        - create_entry
        - update_entry
        - delete_entry
        - sync_bundle
        - search_entries

::: vertebra.agent.knowledge_catalog.AsyncKnowledgeCatalogClient
    options:
      heading_level: 3
      members:
        - get_entry_group
        - list_entry_groups
        - create_entry_group
        - resolve_context_set
        - get_entry
        - list_entries
        - create_entry
        - update_entry
        - delete_entry
        - sync_bundle
        - search_entries
