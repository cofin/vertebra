# Flow Directory Structure

The `.agent` directory serves as the root for all Flow methodology artifacts, providing a structured environment for project management, documentation, and archival.

## Root Directory (`.agent/`)

- `prds.md`: The central registry of all Product Requirements Documents (PRDs). This is the primary index for active work.
- `product.md`: High-level product definition and vision.
- `product-guidelines.md`: Product principles, design guidelines, and constraints.
- `tech-stack.md`: Defined technology stack and architectural decisions.
- `workflow.md`: The operational workflow and process definition.
- `index.md`: The main entry point for the project context, linking to key documents.
- `setup-state.json`: State file tracking the initialization progress.

## Specifications (`.agent/specs/`)

This directory contains the active PRDs, each in its own dedicated folder.

- **Structure per PRD:**
  - `metadata.json`: Machine-readable metadata (ID, status, timestamps).
  - `index.md`: Context index linking to the spec and plan.
  - `spec.md`: The detailed Product Requirements Document.
  - `plan.md`: The implementation plan with trackable tasks.

## Archive (`.agent/archive/`)

Contains completed, abandoned, or superseded work artifacts. This preserves the project's history without cluttering the active workspace.

- **Legacy Migration:** `legacy_migration_source/` holds the original specs that were migrated to the new structure.
- **Historical PRDs:** Previous iterations (e.g., `PRD-1-rust-ipc`, `PRD-2-python-bindings`) are stored here.

## Templates (`.agent/templates/`)

Standardized templates for creating new PRDs and research documents.

- `prd.md`: Template for new PRDs.
- `tasks.md`: Template for implementation plans.
- `recovery.md`: Template for context recovery notes.

## Code Style Guides (`.agent/code-styleguides/`)

Language and framework-specific style guides referenced by the project.

- `python.md`, `rust.md`, `typescript.md`: Language standards.
- `litestar.md`, `orm.md`, `sqlspec.md`: Framework-specific conventions.
