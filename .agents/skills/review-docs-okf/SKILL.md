---
name: review-docs-okf
description: Use when reviewing OKF v0.2 knowledge chapters, bounded truth blocks, relative link integrity, privacy sanitization, or workflow sync.
---

<!-- truth: start -->
- **Frontmatter**: Non-reserved OKF files require type, title, description, and status (`DOC-01`).
- **Truth blocks**: Truth blocks are bounded (<= 40 lines) and match live implementation signatures (`DOC-02`).
- **Links**: All relative document links and resource sources resolve on disk (`DOC-03`).
- **Synchronized**: Architecture or command updates touch knowledge chapters in the same change (`DOC-04`, `DOC-08`).
- **Privacy**: Zero private home paths, company tokens, or prohibited terms in public docs (`DOC-06`).
- **Suppressions**: Respect `FP-DOC-01` in the shared catalog.
<!-- truth: end -->

# Docs and OKF Review (`review-docs-okf`)

## Scope

- Paths: `[areas.docs-okf]` in [`areas.toml`](../review-core/references/areas.toml).
- Checklist: `DOC-01`..`DOC-09` in [`checklist.md`](../review-core/references/checklist.md).
- Ledger and finding grammar: [`ledger-format.md`](../review-core/references/ledger-format.md).

## Rulebook

- [Workflow guide](../../bundles/knowledge/workflow.md)
- [Knowledge index](../../bundles/knowledge/index.md)
- OKF frontmatter reference: [`references/okf-frontmatter.md`](references/okf-frontmatter.md)

## Review Process

1. **Frontmatter schemas**: Check that all OKF v0.2 markdown files contain type, title, description, and status (`DOC-01`).
2. **Truth block accuracy**: Ensure truth blocks compile against live codebase interfaces (`DOC-02`).
3. **Link integrity**: Audit all relative links and resource paths for existence on disk (`DOC-03`).
4. **Knowledge currency**: Require architecture, protocol, and command modifications to update docs in lockstep (`DOC-04`).
5. **Privacy hygiene**: Verify files are free of machine-specific home paths and internal identifiers (`DOC-06`).
6. **Lazy specs**: Ensure specs directories contain no completed specs (`DOC-07`).

## Suppressions Index

- `FP-DOC-01`: KaTeX mathematical notation

## Focused Commands

```bash
uv run python -m tools.scripts.review_setup validate --rules bundles,privacy
```
