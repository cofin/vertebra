# Recovery Guide for Codebase Consolidation

## Quick Resume

**Last Phase:** PRD Complete - Pending Multi-Model Consensus
**Current Status:** Planning Complete
**PRD Location:** `specs/active/codebase-consolidation/`

## Intelligence Context

**Complexity:** Complex (12 tasks across 4 phases)
**Stack:** Python 3.12+ / TypeScript (Bun) / Rust 2021

**Commands:**
- Test: `make test-all`
- Lint: `make lint-all`
- Build: `make build`

**Documentation:**
- System: Sphinx (docs/)
- Location: `docs/`
- Skills: N/A

**Similar implementations:**
- `_internal/serialization.py` - MessagePackCodec pattern
- `broker/client.ts` - Canonical type definitions
- `vertebra-py/src/lib.rs` - Binding wrapper pattern

**Patterns being followed:**
- Centralized serialization codec
- Abstract base class for pools
- Hub-and-spoke FFI bindings

## Files Created

```
specs/active/codebase-consolidation/
├── prd.md            (3,242 words) - Main PRD document
├── tasks.md          (12 tasks) - Implementation checklist
├── recovery.md       (This file) - Resume guide
└── research/
    └── plan.md       (2,545 words) - Research findings
```

## State Summary

### Completed
- [x] Phase 0: Project Discovery
- [x] Phase 1: Complexity Assessment
- [x] Phase 2: Pattern Recognition
- [x] Phase 3: Create Workspace
- [x] Phase 4: Intelligent Analysis
- [x] Phase 5: Research Document (2,545 words)
- [x] Phase 6: PRD Document (3,242 words)
- [x] Phase 7: Task Breakdown (12 tasks)
- [x] Phase 8: Recovery Guide

### Pending
- [ ] Phase 9: Multi-Model Consensus (Gemini 3 Pro + GPT 5.2)
- [ ] Phase 10: Git Verification

## Key Findings Summary

### Python Issues
1. **filesystem/__init__.py** - 300+ lines of useless wrapper classes
2. **storage/ vs filesystem/** - Overlapping modules that should merge
3. **MessagePack** - Inconsistent serialization across 3+ files
4. **resources/ipc.py** - Confusing name, should be `broker.py`

### TypeScript Issues
1. **metrics.ts + unified-metrics.ts** - Duplicate files
2. **PoolStats types** - Defined in 3 places with different names
3. **Pool adapters** - 150+ lines of duplicate stats tracking
4. **"Pool" naming** - Means 3 different things

### Rust Issues
1. **vertebra-js** - Missing Channel abstraction (Python has it)
2. **Wrapper duplication** - Py and JS both have boilerplate wrappers

### Explicitly NOT Consolidating
1. **SQLSpec dialect folders** - KEEP separate. Each dialect has meaningful differences (ParameterStyle, transactional DDL support, Arrow export flags). Structure follows SQLSpec/Advanced Alchemy patterns.

## Consolidation Summary

| Change | Type | Impact |
|--------|------|--------|
| Delete filesystem wrappers | Python | -300 lines |
| Merge metrics files | TypeScript | -50 lines |
| Create BasePoolAdapter | TypeScript | -150 lines |
| Add ChannelHandle | Rust | +150 lines |
| **Net** | | **-350 lines** |

## Next Steps

### Immediate: Multi-Model Consensus

Run consensus review with Gemini 3 Pro and GPT 5.2 to validate:
1. Are the consolidation priorities correct?
2. Are there any risks missed?
3. Should SQLSpec dialect consolidation be included?
4. Buffer tracking in JS: implement or document?

### After Approval: Implementation

Start with Phase 1 tasks (low risk):
1. Task 1.1: Delete filesystem wrappers
2. Task 1.2: Merge metrics files
3. Task 1.3: Standardize MessagePack
4. Task 1.4: Unify type definitions

Then proceed to Phase 2-4 as documented in `tasks.md`.

## Commands to Verify State

```bash
# Check no source changes were made during planning
git status --porcelain src/

# Verify PRD workspace exists
ls -la specs/active/codebase-consolidation/

# Count words in documents
wc -w specs/active/codebase-consolidation/prd.md
wc -w specs/active/codebase-consolidation/research/plan.md
```

## How to Resume

If picking up this work later:

1. Read `specs/active/codebase-consolidation/prd.md` for full context
2. Check `tasks.md` for implementation checklist
3. Start with Phase 1 tasks (any order within phase)
4. Run `make test-all` after each task
5. Update task checkboxes as you complete them

## Contact

**PRD Author:** Claude (AI Assistant)
**Created:** 2026-01-19
**Last Updated:** 2026-01-19
