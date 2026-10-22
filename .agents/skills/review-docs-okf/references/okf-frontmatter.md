# OKF v0.2 Frontmatter Reference

This guide summarizes requirements for frontmatter across OKF v0.2 knowledge chapters.

---

## Required Frontmatter Fields

1. **`type`**: Semantic category (`Architecture`, `Pattern`, `Domain`, `Testing`, `Gotcha`, `Guide`, `Index`).
2. **`title`**: Non-empty title matching document H1.
3. **`description`**: One or two sentence summary of scope and invariants.
4. **`status`**: Must be one of `draft`, `stable`, or `deprecated`.
5. **`tags`**: List of lowercase alphanumeric slugs separated by hyphens (`[rust, shm]`).
