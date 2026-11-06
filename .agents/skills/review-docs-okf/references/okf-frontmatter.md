# OKF v0.2 Frontmatter Reference

This guide summarizes frontmatter, bounded truth block, and provenance requirements across OKF v0.2 bundle files (`.agents/bundles/`).

---

## 1. Concept Chapter Frontmatter (`product/**` & `knowledge/**`)

Every non-reserved `.md` chapter declares:

1. **`type`**: Semantic category (`Architecture`, `Pattern`, `Domain`, `Testing`, `Gotcha`, `Guide`, `Concept`, `Decision`, `Research`, `Spec`, `Task`).
2. **`title`**: Non-empty title matching document H1.
3. **`description`**: Crisp one- or two-sentence summary of scope and invariants.
4. **`status`**: Document maturity only — `draft`, `stable`, or `deprecated` (workflow execution state belongs in `state:` on `Spec`, `Task`, and `Research`).
5. **`tags`**: List of lowercase alphanumeric slugs separated by hyphens (`[rust, shm]`).
6. **`generated`**: Producer metadata (`by: flow/0.26.0`, `at: "YYYY-MM-DDTHH:MM:SSZ"`).
7. **`verified`**: Verification list (`- by: <actor>`, `at: "YYYY-MM-DDTHH:MM:SSZ"`, `commit: <git_sha>`).
8. **`sources`**: Structured provenance list (`- id: <slug>`, `resource: <repo/path[::Symbol]>`, `title: "<summary>"`) paired with matching `[^<slug>]:` body footnotes whose local repository paths exist on disk.

## 2. Bounded Truth Blocks & Reserved Files

- **Truth Block**: Every non-reserved chapter under `product/**` and `knowledge/**` begins its body with a bounded `<!-- truth: start --> ... <!-- truth: end -->` block containing `<= 40` lines of machine-checkable invariants.
- **Reserved Files**: `index.md` and `log.md` are reserved filenames (`bundles/index.md` declares `okf_version: "0.2"` and `bundle: "vertebra"`; `log.md` carries no frontmatter). Reserved files never declare concept-only keys (`type`, `status`, or `sources`).
