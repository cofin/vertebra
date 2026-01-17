# PRD-4: Bun-Backed Python Drivers - Session Recovery Guide

**Created**: 2026-01-17
**Workspace**: `/home/cody/code/c/vertebra/docs/prd/PRD-4-bun-drivers/`

---

## Quick Start

To resume work on PRD-4 after interruption:

```bash
# Navigate to workspace
cd /home/cody/code/c/vertebra/docs/prd/PRD-4-bun-drivers/

# Read main PRD
cat ../PRD-4-bun-drivers.md

# Review task list
cat tasks.md

# Check research notes
cat research/plan.md
```

---

## Document Overview

### Main PRD Document

**Location**: `/home/cody/code/c/vertebra/docs/prd/PRD-4-bun-drivers.md`

**Size**: 3,200+ words

**Sections**:
1. Executive Summary
2. Problem Statement
3. Scope (In Scope / Out of Scope)
4. Architecture (System Overview, Design Philosophy)
5. Database Drivers (DB-API 2.0, PostgreSQL, MySQL, SQLite, SQLSpec)
6. File System Layer (fsspec Integration, VertebraFileSystem)
7. Cloud Storage Clients (S3Client, GCSClient)
8. IPC Protocol and Serialization (MessagePack)
9. Implementation Plan (5 Phases, 8 weeks)
10. Testing Strategy (5 Test Levels)
11. Performance Targets (Benchmarks)
12. Documentation and Examples
13. Dependencies
14. Success Metrics
15. Risks and Mitigations
16. Open Questions
17. Future Enhancements
18. References
19. Appendix (File Structure, Glossary)

### Research Documentation

**Location**: `/home/cody/code/c/vertebra/docs/prd/PRD-4-bun-drivers/research/plan.md`

**Size**: 2,847 words

**Key Topics**:
- DB-API 2.0 specification research
- AsyncPG connection pooling best practices
- fsspec filesystem interface research
- Async MySQL/SQLite driver analysis
- Cloud storage streaming (S3, GCS)
- Serialization format analysis (MessagePack vs Protocol Buffers)
- Architecture decision rationale
- Performance expectations

### Task Breakdown

**Location**: `/home/cody/code/c/vertebra/docs/prd/PRD-4-bun-drivers/tasks.md`

**Structure**:
- Phase 1: Foundation Layer (Weeks 1-2) - 6 task groups
- Phase 2: Database Drivers (Weeks 3-5) - 9 task groups
- Phase 3: File System Layer (Weeks 4-6) - 8 task groups
- Phase 4: Cloud Storage Clients (Weeks 6-7) - 6 task groups
- Phase 5: Testing and Optimization (Weeks 7-8) - 8 task groups
- Phase 6: Documentation (Weeks 7-8) - 7 task groups
- Quality Gates
- Release Checklist

---

## Key Decisions Made

### 1. Architecture: Thin Python Wrapper

**Decision**: Python drivers are thin wrappers that serialize requests to Bun server

**Rationale**:
- Maximizes performance by running all I/O in Bun
- Simpler Python code (just API compliance and type conversion)
- Easier to maintain (single source of truth for pooling logic)
- Better resource management (Bun manages connections)

**Trade-offs**:
- Network overhead for IPC (mitigated by MessagePack)
- Dependency on Bun server (mitigated by retry logic)

### 2. Serialization: MessagePack

**Decision**: Use MessagePack for IPC serialization (Phase 1)

**Rationale**:
- Fast binary format
- Schema-less (flexible for evolving protocol)
- Well-supported in Python and TypeScript
- Good balance of size and speed

**Alternative**: Protocol Buffers (evaluate in Phase 5 if performance critical)

### 3. Standards Compliance

**Decisions**:
- Full DB-API 2.0 compliance (non-negotiable)
- Full fsspec compatibility
- asyncpg-compatible async interface
- SQLSpec adapter support

**Rationale**: Standards compliance ensures compatibility with existing Python ecosystem

### 4. Implementation Priorities

**Priority Order**:
1. PostgreSQL driver (most common database)
2. MySQL driver
3. SQLite driver (simpler, no pooling)
4. File system layer (can parallel with Phase 2)
5. Cloud storage clients (builds on filesystem patterns)

---

## Critical Dependencies

### External Dependencies

**PRD-2: Python Bindings**:
- Required for IPC communication layer
- Must provide efficient binary serialization
- Need socket/pipe communication primitives

**PRD-3: Bun Server**:
- Must expose connection pooling endpoints
- Need database driver integration (postgres, mysql2, better-sqlite3)
- Must support AWS/GCP SDKs for cloud storage

### Python Packages

```toml
[project.dependencies]
msgpack = "^1.0.7"           # IPC serialization
typing-extensions = "^4.9"   # Type hints

[project.optional-dependencies]
drivers = ["vertebra[bindings]"]
filesystem = ["fsspec >=2024.1"]
storage = ["vertebra[filesystem]"]
```

### Bun Packages

```json
{
  "dependencies": {
    "postgres": "^3.4",
    "mysql2": "^3.6",
    "better-sqlite3": "^9.2",
    "@aws-sdk/client-s3": "^3.478",
    "@google-cloud/storage": "^7.7",
    "@msgpack/msgpack": "^3.0"
  }
}
```

---

## Performance Targets

### Benchmark Goals (vs Native Drivers)

| Operation | Native Driver | Target | Expected Improvement |
|-----------|--------------|--------|---------------------|
| **PostgreSQL** |
| Simple SELECT | asyncpg | 2-3x faster | Reduced GIL overhead |
| Bulk INSERT | asyncpg | 2-5x faster | Better pooling |
| Large result | asyncpg | 1.5-2x faster | Streaming + binary |
| **MySQL** |
| Simple SELECT | aiomysql | 2-3x faster | Reduced GIL overhead |
| Bulk INSERT | aiomysql | 2-5x faster | Better pooling |
| **SQLite** |
| Simple SELECT | aiosqlite | 1.5-2x faster | Bun I/O |
| Bulk INSERT | aiosqlite | 2-3x faster | Bun I/O |
| **S3** |
| Upload 100MB | boto3 | 1.5-2x faster | Streaming + Bun I/O |
| Download 100MB | boto3 | 1.5-2x faster | Streaming + Bun I/O |

### Resource Usage Targets

- Memory overhead: <10% vs native drivers
- CPU overhead: <5% for IPC serialization
- Latency: <1ms added for small queries (<1KB result)
- Latency: <5ms added for large queries (>1MB result)

---

## Testing Strategy Summary

### Test Levels

1. **Unit Tests** (Fast, no external dependencies)
   - Mock Bun server responses
   - Test Python wrapper logic
   - Coverage >90%

2. **Integration Tests** (Requires Bun server + databases)
   - Real PostgreSQL, MySQL, SQLite databases
   - MinIO/LocalStack for S3
   - GCS emulator

3. **Compliance Tests** (Standards conformance)
   - DB-API 2.0 (PEP 249)
   - fsspec contract
   - SQLSpec adapter

4. **Performance Tests** (Benchmarks)
   - vs asyncpg, aiomysql, aiosqlite
   - Metrics: QPS, latency (p50/p95/p99), memory

5. **Stress Tests** (Reliability)
   - 1000+ concurrent connections
   - Connection pool exhaustion
   - Network failures
   - Long-running tests (memory leaks)

### Test Infrastructure

**Docker Compose** (`tests/docker-compose.yml`):
- PostgreSQL 16
- MySQL 8
- MinIO (S3-compatible)
- GCS emulator

**CI/CD Pipeline**:
1. Unit tests (parallel)
2. Integration tests (parallel across databases)
3. Compliance tests
4. Performance benchmarks (track regressions)
5. Stress tests (nightly only)

---

## Key Technical Challenges

### 1. IPC Serialization Performance

**Challenge**: Network overhead could negate performance gains

**Mitigation**:
- Use efficient binary format (MessagePack)
- Batch operations where possible
- Stream large results in chunks
- Benchmark early (Phase 1)

### 2. Connection Pool Correctness

**Challenge**: Connection pool edge cases are hard to test exhaustively

**Mitigation**:
- Reference asyncpg implementation
- Comprehensive stress tests
- Connection lifecycle logging
- Gradual rollout

### 3. Type Conversion Completeness

**Challenge**: PostgreSQL/MySQL have many type variations

**Mitigation**:
- Comprehensive test suite for all types
- Fuzzing type conversions
- Reference existing driver implementations
- Handle unknown types gracefully

### 4. Large Result Set Streaming

**Challenge**: Memory management for large results

**Mitigation**:
- Chunk results at configurable threshold
- Support incremental iteration
- Test with multi-GB result sets
- Profile memory usage

---

## Implementation Sequence

### Phase 1 (Weeks 1-2): Foundation

**Focus**: Get basic IPC working

**Key Deliverables**:
- MessagePack serialization operational
- Base driver interfaces defined
- Exception hierarchy complete
- Type conversion utilities ready

**Validation**: Can send/receive messages between Python and Bun

### Phase 2 (Weeks 3-5): Database Drivers

**Focus**: PostgreSQL first, then MySQL, then SQLite

**Key Deliverables**:
- PostgreSQL driver (DB-API 2.0 + async)
- MySQL driver (same pattern)
- SQLite driver (simpler)
- SQLSpec adapters

**Validation**: DB-API 2.0 compliance tests pass, performance benchmarks show improvement

### Phase 3 (Weeks 4-6): File System

**Focus**: fsspec compatibility (parallel with Phase 2)

**Key Deliverables**:
- VertebraFileSystem base class
- Local, S3, GCS backends
- Streaming support

**Validation**: Works with pandas, dask; fsspec compliance tests pass

### Phase 4 (Weeks 6-7): Cloud Storage

**Focus**: High-level clients for better UX

**Key Deliverables**:
- S3Client (streaming, multipart, progress)
- GCSClient (same interface)

**Validation**: Can upload/download large files efficiently

### Phase 5 (Weeks 7-8): Testing & Optimization

**Focus**: Comprehensive testing and performance tuning

**Key Deliverables**:
- Full test suite (>90% coverage)
- Performance benchmarks
- Stress tests
- Optimizations

**Validation**: All quality gates passed, performance targets met

---

## Next Steps

### To Start Implementation

1. **Set up development environment**:
   ```bash
   cd /home/cody/code/c/vertebra
   make install  # Install dependencies
   ```

2. **Create directory structure**:
   ```bash
   mkdir -p src/py/vertebra/drivers
   mkdir -p src/py/vertebra/fs
   mkdir -p src/py/vertebra/storage
   mkdir -p src/py/vertebra/_internal
   mkdir -p src/ts/vertebra/drivers
   mkdir -p src/ts/vertebra/fs
   mkdir -p src/ts/vertebra/storage
   mkdir -p src/ts/vertebra/protocol
   mkdir -p tests/{unit,integration,compliance,performance,stress}
   ```

3. **Start with Phase 1, Task 1.1** (IPC Protocol Definition):
   - Create `src/ts/vertebra/protocol/messages.ts`
   - Define TypeScript interfaces
   - Document protocol specification

4. **Review reference implementations**:
   - AsyncPG: https://github.com/magicstack/asyncpg
   - fsspec: https://filesystem-spec.readthedocs.io/
   - PEP 249: https://peps.python.org/pep-0249/

### If Resuming After Interruption

1. **Check current status**:
   ```bash
   cd /home/cody/code/c/vertebra/docs/prd/PRD-4-bun-drivers
   cat tasks.md | grep "\[x\]"  # See completed tasks
   ```

2. **Review completed work**:
   ```bash
   git log --oneline --grep="PRD-4"  # See recent commits
   find src/py/vertebra -name "*.py" -mtime -7  # Recently modified files
   ```

3. **Continue from last task**: Refer to tasks.md for next unchecked task

---

## Resources and References

### Standards Documentation

- [PEP 249: Python Database API 2.0](https://peps.python.org/pep-0249/)
- [fsspec Documentation](https://filesystem-spec.readthedocs.io/)
- [MessagePack Specification](https://msgpack.org/)

### Implementation References

- [asyncpg GitHub](https://github.com/magicstack/asyncpg)
- [asyncpg Usage Guide](https://github.com/magicstack/asyncpg/blob/master/docs/usage.rst)
- [aiomysql Documentation](https://aiomysql.readthedocs.io/)
- [aiosqlite Documentation](https://aiosqlite.omnilib.dev/)
- [aiobotocore Documentation](https://aiobotocore.readthedocs.io/)

### Best Practices Articles

- [Node.js Connection Pooling for PostgreSQL/MySQL](https://oneuptime.com/blog/post/2026-01-06-nodejs-connection-pooling-postgresql-mysql/view)
- [S3 Streaming Best Practices](https://github.com/aio-libs/aiobotocore)
- [fsspec Custom Backends](https://filesystem-spec.readthedocs.io/en/latest/_sources/async.rst)

---

## Contact and Support

**Project**: Vertebra
**Component**: PRD-4 (Bun-Backed Python Drivers)
**Owner**: Vertebra Core Team
**Dependencies**: PRD-2 (Python Bindings), PRD-3 (Bun Server)

**Key Questions to Ask**:
1. Has PRD-2 (Python Bindings) been completed?
2. Has PRD-3 (Bun Server) exposed required endpoints?
3. Which phase are we currently implementing?
4. What tests are currently passing/failing?
5. Are performance targets being met?

---

## Workspace Files

```
/home/cody/code/c/vertebra/docs/prd/PRD-4-bun-drivers/
├── research/
│   └── plan.md           # Research findings (2,847 words)
├── tmp/
│   └── (working notes)
├── recovery.md           # This file
└── tasks.md              # Implementation task list

/home/cody/code/c/vertebra/docs/prd/
└── PRD-4-bun-drivers.md  # Main PRD document (3,200+ words)
```

---

**Last Updated**: 2026-01-17
**Recovery Instructions Version**: 1.0
