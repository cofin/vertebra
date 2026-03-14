# Flow Spec: Type Bridge & Codegen
## Goal
Implement IR extraction and TypeScript/Zod emission.

## Workflow Requirements
- **GitHub Issues**: Task 0 is to open a focused GitHub issue for this feature.
- **Pull Requests**: All changes MUST be submitted via GitHub Pull Requests.

## Plan
### Phase 1: Inspection
- [ ] Task 0: Open GitHub issue for "Type Bridge and Codegen IR extraction".
- [ ] Task 1: Implement `src/py/vertebra/core/codegen/extractor.py`.
- [ ] Task 2: Handle `msgspec.Struct` and `dataclass` introspection.

### Phase 2: Emission
- [ ] Task 3: Implement `src/py/vertebra/core/codegen/emitters/` for TS and Zod.
- [ ] Task 4: Add route manifest generation for frontend consumption.
