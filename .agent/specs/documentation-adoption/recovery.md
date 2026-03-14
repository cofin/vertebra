# Recovery Guide: Documentation & Adoption

## Quick Resume

**Last phase**: Not Started
**Current status**: PRD Complete, Ready for Implementation
**Spec location**: `.agent/specs/documentation-adoption/`

## Overview

This is a comprehensive 18-week PRD covering all documentation and adoption work for Vertebra 1.0:

1. **Integration Tests** (Weeks 1-5): 90%+ coverage across Python/TypeScript/Rust boundaries
2. **Performance Benchmarks** (Weeks 6-7): Compare against Granian/Uvicorn/Hypercorn
3. **Example Applications** (Weeks 8-11): 6 production-quality examples
4. **Migration Guides** (Weeks 12-13): From litestar-vite and ASGI servers
5. **API Documentation** (Weeks 14-15): Python (Sphinx), TypeScript (TypeDoc), Rust (rustdoc)
6. **Developer Guides** (Weeks 16-17): Getting Started, Type Bridge, Performance, Deployment
7. **Polish** (Week 18): Final integration and release prep

## Success Metrics

| Metric | Target |
|--------|--------|
| Integration test coverage | ≥90% |
| Test execution time | <5 min |
| Performance vs Granian | +10% RPS |
| Example applications | 6 complete |
| API documentation | 100% coverage |
| New user onboarding | <10 min setup |

## Dependencies

All previous PRDs (0-5) must be functionally complete before starting.

## Key Deliverables

### Integration Tests
- Python ↔ TypeScript (HTTP contract, serialization, type bridge)
- TypeScript ↔ Rust (type generation, FFI, pools)
- Python ↔ Rust (PyO3, shared memory, GIL management)
- End-to-End with real databases (pytest-databases)
- WebSocket tests (Playwright)

### Example Applications
1. Todo API (SQLSpec + Litestar)
2. Blog Platform (SQLAlchemy + Advanced Alchemy)
3. AI Tools Server (MCP)
4. Realtime Chat (WebSocket)
5. Data Pipeline (Pool Sharing)
6. gRPC Microservice

### Benchmark Scenarios
- Simple JSON response (target: 50K+ RPS)
- Database query (target: 10K+ RPS)
- WebSocket concurrent connections (target: 10K+)
- Static file serving

## Next Steps

1. Start Phase 1: Configure pytest-databases, set up CI matrix builds
2. Create integration test fixtures in `src/py/tests/integration/conftest.py`
