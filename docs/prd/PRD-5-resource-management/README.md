# PRD-5: Cooperative Resource Management

This directory contains the complete planning documentation for Vertebra's Cooperative Resource Management system.

## Quick Navigation

- **Main PRD**: `prd.md` - Complete technical specification (6,800+ words)
- **Task List**: `tasks.md` - 108 actionable tasks across 6 phases
- **Research**: `research/plan.md` - Technology analysis and decisions (4,200+ words)
- **Recovery Guide**: `recovery.md` - Session resumption and troubleshooting

## What is PRD-5?

Cooperative Resource Management enables shared connection pools across Python workers, reducing database connections by 70-90%.

**Architecture**:
```
┌─────────────────────────────────────┐
│     Vertebra Resource Broker        │
│   (Rust process, manages pools)     │
├─────────────────────────────────────┤
│  PostgreSQL Pool (20 conns)         │
│  MySQL Pool (10 conns)              │
│  Redis Pool (5 conns)               │
└─────────────────────────────────────┘
         │ IPC transport (UDS / named pipes)
    ┌────┴────┬────┴────┬────┴────┐
    │Worker 1 │Worker 2 │Worker N │
    └─────────┴─────────┴─────────┘
```

## Implementation Status

Updated 2026-01-18: Broker scaffolding + Python client tests are in place.
Check `tasks.md` for detailed task list and mark items as complete.

**Phase Overview**:
1. Foundation (Weeks 1-3) - Rust broker + single pool
2. Python Client (Weeks 4-5) - IPC client + multi-pool
3. Fair Scheduling (Weeks 6-7) - WFQ scheduler
4. Production Readiness (Weeks 8-10) - Observability, security, deployment
5. Integration & Testing (Weeks 11-12) - Performance testing, PRD-4 integration
6. Production Rollout (Weeks 13-14) - Gradual rollout + documentation

## Key Files

```
PRD-5-resource-management/
├── README.md           # This file
├── prd.md              # Main PRD document
├── tasks.md            # Task breakdown
├── recovery.md         # Recovery guide
└── research/
    └── plan.md         # Research findings
```

## Getting Started

1. Read `prd.md` for complete specification
2. Review `tasks.md` for implementation tasks
3. Check `research/plan.md` for technology decisions
4. Use `recovery.md` for troubleshooting and resumption

## Success Metrics

- **Performance**: P99 latency < 10ms
- **Efficiency**: 70-90% reduction in database connections
- **Reliability**: 99.95% uptime
- **Fairness**: < 10% deviation in per-worker allocation

---

**Status**: Planning Complete, Ready for Implementation
**Last Updated**: 2026-01-17
