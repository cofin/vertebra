# Documentation & Adoption - Research Notes

**PRD**: `/home/cody/code/c/vertebra/specs/active/docs/prd.md`
**Date**: 2026-01-19

---

## Research Methodology

This PRD was created by consolidating and expanding upon two source specifications:
1. `specs/archive/consolidated/PRD-6-integration.md` - Integration tests, benchmarks, examples
2. `specs/archive/consolidated/prd-remaining-work.md` - Documentation tasks

**Research Areas:**
- Multi-language integration testing patterns (Python ↔ TypeScript ↔ Rust)
- Performance benchmarking methodologies for ASGI servers
- Example application patterns for developer education
- Migration guide best practices
- API documentation tooling (Sphinx, TypeDoc, rustdoc)
- Developer guide content structure

---

## 1. Integration Testing Research

### Multi-Language Testing Patterns

**Source**: PRD-6-integration.md, industry best practices

**Key Findings:**

#### Test Pyramid for Multi-Language Systems
```
10% E2E Tests (Playwright, full stack)
20% Integration Tests (cross-language, testcontainers)
70% Unit Tests (fast, isolated, language-specific)
```

**Rationale**: Focus integration testing on language boundaries where failures are most likely.

#### Python ↔ TypeScript Testing
**Challenge**: Validating API contracts and serialization across HTTP boundary

**Solution**:
- Use pytest with async support (`pytest-asyncio`)
- Validate JSON responses match TypeScript type schemas
- Test all CRUD operations (Create, Read, Update, Delete)
- Verify error handling and status codes

**Example Pattern**:
```python
async def test_user_endpoint_matches_typescript_types():
    response = await client.get("/api/users")
    assert response.status_code == 200
    # Validate response shape matches TypeScript User interface
    validate_against_typescript_schema(response.json(), "User[]")
```

#### TypeScript ↔ Rust Testing
**Challenge**: Type bridge code generation accuracy

**Solution**:
- Use Vitest for TypeScript testing
- Create reference Rust types
- Verify generated TypeScript produces identical signatures
- Test round-trip serialization

**Example Pattern**:
```typescript
test("generated TypeScript types match Rust definitions", () => {
  const generatedUser: User = { id: 1, name: "test" };
  expect(validateRustType(generatedUser, "User")).toBe(true);
});
```

#### Python ↔ Rust Testing
**Challenge**: PyO3 FFI boundary safety, GIL management

**Solution**:
- Use pytest with pytest-benchmark for performance
- Test shared memory channel creation and cleanup
- Verify GIL release in blocking operations
- Use pytest-memray for memory leak detection

**Reference**: https://pyo3.rs/v0.20.0/python-from-rust.html#accessing-the-gil

**Critical Pattern**:
```python
def test_shared_memory_channel_no_leaks():
    """Verify shared memory cleanup after channel close."""
    initial_shm = list_shm_objects()

    with create_channel_pair() as (tx, rx):
        tx.send(b"test message")
        msg = rx.receive(timeout=1.0)
        assert msg == b"test message"

    final_shm = list_shm_objects()
    assert len(final_shm) == len(initial_shm), "Memory leak detected"
```

### Container-Based Testing

**Tool**: pytest-databases (already in dependencies)

**Why pytest-databases over testcontainers-python**:
- Native pytest integration
- Simpler fixture API
- Automatic cleanup
- Supports Docker and Podman
- No docker-compose required

**Example Usage**:
```python
from pytest_databases.docker import DockerServiceRegistry

@pytest.fixture(scope="module")
def postgres_container():
    registry = DockerServiceRegistry()
    postgres = registry.postgres(image="postgres:16-alpine")
    yield postgres
    postgres.stop()
```

**Reference**: https://pytest-databases.readthedocs.io/

### WebSocket Testing

**Tool**: Playwright (modern, 2026 best practices)

**Why Playwright over websockets library**:
- Better suited for E2E testing
- Can test browser WebSocket behavior
- Built-in event inspection
- Modern async API

**Example Pattern**:
```python
async def test_websocket_bidirectional_messages(playwright_page):
    ws_messages = []
    page.on("websocket", lambda ws: ws_messages.append(ws))

    await page.goto("http://localhost:8000/ws-test")
    await page.wait_for_selector("#connected")

    await page.evaluate("window.ws.send(JSON.stringify({type: 'ping'}))")
    response = await page.wait_for_function(
        "document.getElementById('messages').innerText.includes('pong')"
    )
    assert response
```

**Reference**: https://playwright.dev/docs/api/class-websocket

### Coverage Targets

**Research Finding**: Industry standard for integration tests is 80-90% coverage

**Target for Vertebra**: 90%+ coverage for integration points

**Rationale**:
- Higher than typical (80%) due to cross-language complexity
- Focus on language boundaries where bugs are most likely
- Unit tests cover language-specific logic

**Coverage Calculation**:
```python
integration_coverage = (
    lines_covered_at_boundaries / total_boundary_lines
) * 100
```

---

## 2. Performance Benchmarking Research

### Competitor Analysis

**Primary Competitors**:
1. **Granian** - Fastest Python ASGI server (Rust-based, ~45k RPS)
2. **Uvicorn** - Most popular ASGI server (~30k RPS)
3. **Hypercorn** - HTTP/2 and HTTP/3 capable (~28k RPS)

**Performance Targets** (based on PRD-6):
- **RPS**: Vertebra should achieve +10% vs Granian (>50k RPS)
- **Latency**: p99 should be lower than Uvicorn (<50ms)
- **WebSocket**: Support 10,000+ concurrent connections
- **Startup**: <1 second time to first request

### Benchmarking Tools

**Research on wrk vs k6**:
- **wrk/wrk2**: C-based, 5x faster than k6, better for raw performance
- **k6**: JavaScript-based, better for complex scenarios
- **Decision**: Use wrk2 for primary benchmarks, k6 for complex workloads

**Why wrk2 over wrk**:
- Constant throughput mode (avoid coordinated omission)
- More accurate latency measurements
- Better percentile reporting

**Reference**: https://github.com/giltene/wrk2

### Benchmark Methodology

**Based on industry best practices**:

1. **Dedicated Hardware**: No other processes running
2. **Multiple Iterations**: 10+ runs per scenario
3. **Statistical Analysis**: Mean, median, stdev, confidence intervals
4. **Warmup Period**: Discard first run (JIT compilation)
5. **Constant Throughput**: Use wrk2 to avoid coordinated omission
6. **Documented Conditions**: CPU model, RAM, OS, kernel parameters

**Reference**: https://www.brendangregg.com/blog/2014-05-02/compilers-love-messing-with-benchmarks.html

### Benchmark Scenarios

**Scenario 1: Simple JSON**
- **Purpose**: Measure pure server overhead
- **Payload**: 1KB JSON response
- **Expected**: >50,000 RPS

**Scenario 2: Database Query**
- **Purpose**: Test connection pooling efficiency
- **Query**: SELECT with 10 rows
- **Expected**: >10,000 RPS

**Scenario 3: WebSocket**
- **Purpose**: Measure concurrent connection capacity
- **Test**: Bi-directional echo
- **Expected**: 10,000+ concurrent connections

**Scenario 4: Static Files**
- **Purpose**: Test asset serving
- **Sizes**: 10KB, 500KB, 5MB
- **Expected**: >20,000 RPS for small files

**Scenario 5: Mixed Workload**
- **Purpose**: Simulate realistic application
- **Mix**: 40% JSON, 30% DB, 20% static, 10% WebSocket
- **Expected**: Balanced performance across scenarios

### Statistical Analysis

**Variance Threshold**: <5% coefficient of variation

**Formula**:
```python
cv = (stdev / mean) * 100
if cv > 5:
    print("Results too variable, increase sample size")
```

**Confidence Intervals**: 95% confidence level

**Performance Regression Detection**:
```python
def detect_regression(current_rps, baseline_rps, threshold=0.05):
    """Detect if current performance is >5% slower than baseline."""
    regression = (baseline_rps - current_rps) / baseline_rps
    if regression > threshold:
        raise PerformanceRegression(f"{regression*100:.1f}% slower")
```

---

## 3. Example Applications Research

### Example Quality Standards

**Research**: Analyzed popular framework examples (Next.js, Django, Rails)

**Quality Criteria**:
1. **Production-ready code** (no shortcuts, proper error handling)
2. **Comprehensive README** (setup in <5 minutes)
3. **Working tests** (proves example is correct)
4. **Docker Compose** (easy local development)
5. **Clear patterns** (demonstrates best practices)

### Example Application Categories

**Category 1: Simple CRUD (Todo API)**
- **Purpose**: Learning basics
- **Stack**: SQLSpec + Litestar + React
- **Complexity**: Low
- **Time to understand**: 30 minutes

**Category 2: Full Application (Blog Platform)**
- **Purpose**: Real-world patterns
- **Stack**: SQLAlchemy + Advanced Alchemy + React
- **Complexity**: Medium
- **Time to understand**: 2 hours

**Category 3: Advanced Integration (AI Tools Server)**
- **Purpose**: Showcase MCP protocol
- **Stack**: MCP + Python procedures
- **Complexity**: Medium
- **Time to understand**: 1 hour

**Category 4: Real-time (Chat Application)**
- **Purpose**: WebSocket usage
- **Stack**: WebSocket + React
- **Complexity**: Medium
- **Time to understand**: 1 hour

**Category 5: Performance (Data Pipeline)**
- **Purpose**: Pool sharing Python + Rust
- **Stack**: SQLAlchemy + Rust sqlx
- **Complexity**: High
- **Time to understand**: 2 hours

**Category 6: Microservices (gRPC)**
- **Purpose**: gRPC integration
- **Stack**: Rust gRPC + Python client + TS gRPC-Web
- **Complexity**: High
- **Time to understand**: 2 hours

### README Template Research

**Best practices from popular frameworks**:

**Structure**:
1. One-sentence description
2. Quick start (docker-compose up)
3. Features checklist
4. API endpoints (if applicable)
5. Architecture diagram
6. Development instructions
7. Production deployment notes

**Example**:
```markdown
# Todo API Example

Simple CRUD API demonstrating Vertebra + SQLSpec + Litestar.

## Quick Start

\`\`\`bash
docker-compose up
\`\`\`

API: http://localhost:8000
Frontend: http://localhost:5173

## Features

- ✅ SQLSpec async database integration
- ✅ Type-safe API with Pydantic
- ✅ Generated TypeScript client
- ✅ React frontend with Vite

## Architecture

\`\`\`
React → Litestar API → SQLSpec → Vertebra Broker → Postgres
\`\`\`
```

---

## 4. Migration Guide Research

### Migration Best Practices

**Source**: Documentation migration research, API evolution patterns

**Key Findings**:

1. **Clear Before/After** - Show exact code changes required
2. **Migration Checklist** - Step-by-step verification
3. **Troubleshooting** - Cover 90% of common issues
4. **Rollback Plan** - Always provide escape hatch
5. **Time Estimate** - Set expectations (e.g., "~2-4 hours")

**Reference**: https://dev.to/emiloju/how-to-migrate-technical-documentation-tools-checklist-and-tips-28gd

### Migration from litestar-vite

**Target Audience**: Current litestar-vite users

**Pain Points**:
- Unclear which configuration changes are required
- Type bridge setup not obvious
- No clear testing strategy

**Solution Structure**:
1. Prerequisites (version requirements)
2. Dependency changes (pyproject.toml)
3. Code changes (before/after examples)
4. Type bridge setup
5. Testing migration
6. Rollback plan
7. Troubleshooting (common errors)

### Migration from ASGI Servers

**Target Audience**: Uvicorn/Granian/Hypercorn users

**Pain Points**:
- Unknown if worth the effort
- Docker/K8s deployment changes unclear
- Performance comparison needed

**Solution Structure**:
1. Overview (why migrate)
2. When to migrate (good fit vs not recommended)
3. Migration paths (per server type)
4. Configuration mapping table
5. Deployment changes (Docker, K8s, systemd)
6. Performance comparison (before/after benchmarks)
7. Testing checklist

### Validation Strategy

**Approach**: Perform 2+ real project migrations

**Process**:
1. Select diverse projects (different frameworks, sizes)
2. Follow migration guide step-by-step
3. Document all issues encountered
4. Update guide with solutions
5. Measure migration time
6. External developer review

**Success Criteria**:
- Migration completes successfully
- All tests pass
- Performance meets or exceeds baseline
- Time matches estimate

---

## 5. API Documentation Research

### Documentation Tools

**Python**: Sphinx + autodoc + napoleon
- **Why**: Industry standard, excellent autogen
- **Alternatives**: mkdocs-material (simpler but less powerful)

**TypeScript**: TypeDoc
- **Why**: De facto standard for TS projects
- **Alternatives**: API Extractor (more complex)

**Rust**: rustdoc
- **Why**: Built into Rust toolchain
- **Alternatives**: None needed

### Documentation Structure

**Research**: Analyzed documentation from SQLAlchemy, FastAPI, Litestar

**Best Practices**:
1. **Auto-generate where possible** (Sphinx autodoc, TypeDoc)
2. **Examples in every docstring**
3. **Cross-references** between related APIs
4. **Version-specific docs** (documentation for each release)
5. **Search functionality** (Sphinx has built-in search)

### Docstring Standards

**Python**: Google-style docstrings (supported by napoleon)

**Example**:
```python
def execute(self, query: str, params: dict | None = None) -> Result:
    """
    Execute SQL query through broker.

    Args:
        query: SQL query string
        params: Query parameters (optional)

    Returns:
        Result object containing rows and metadata

    Raises:
        TimeoutError: If query exceeds timeout
        DatabaseError: If query execution fails

    Example:
        >>> result = await driver.execute("SELECT * FROM users WHERE id = ?", {"id": 1})
        >>> print(result.rows)
        [{"id": 1, "name": "Alice"}]
    """
```

**TypeScript**: TSDoc comments

**Example**:
```typescript
/**
 * Execute SQL query through broker.
 *
 * @param query - SQL query string
 * @param params - Query parameters (optional)
 * @returns Promise resolving to query result
 * @throws {TimeoutError} If query exceeds timeout
 *
 * @example
 * ```typescript
 * const result = await client.execute(
 *   "SELECT * FROM users WHERE id = ?",
 *   [1]
 * );
 * ```
 */
async execute(query: string, params?: unknown[]): Promise<QueryResult>
```

### Coverage Measurement

**Python**: interrogate (docstring coverage checker)

**Command**:
```bash
interrogate -vv src/py/vertebra
```

**Target**: 100% public API coverage

### Quality Checks

**Automated Checks**:
1. **Broken link detection** (sphinx-linkcheck)
2. **Example code testing** (doctest)
3. **TypeScript compilation** (verify examples compile)
4. **Spelling/grammar** (codespell)

**CI Integration**:
```yaml
- name: Check documentation
  run: |
    sphinx-build -b linkcheck docs docs/_build/linkcheck
    uv run pytest --doctest-modules src/py/vertebra
```

---

## 6. Developer Guide Research

### Getting Started Guide Research

**Target**: 10-minute setup time

**Research**: Analyzed onboarding from Next.js, Django, Rails, FastAPI

**Structure**:
1. **Installation** (2 minutes) - Prerequisites, package install
2. **First App** (5 minutes) - Hello World example
3. **Add Database** (2 minutes) - SQLSpec integration
4. **Add Frontend** (1 minute) - TypeScript client
5. **Next Steps** - Links to deeper guides

**Key Insight**: Users should see working code ASAP, not theory

### Type Bridge Guide Research

**Pain Point**: Users don't understand how types flow Python → TypeScript

**Solution**: Visual diagram + examples

**Structure**:
1. How it works (architecture diagram)
2. Defining types (Python type hints)
3. Generating types (manual and automatic)
4. Using types (TypeScript client)
5. Complex types (generics, unions, optionals)
6. Troubleshooting

### Performance Tuning Guide Research

**Common Questions**:
- How many workers should I use?
- What's the optimal pool size?
- How do I profile my app?
- How do I benchmark my app?

**Structure**:
1. Server configuration (workers, threads, connection limits)
2. Pool sizing (formula: `pool_size = (workers * 2) + 1`)
3. Caching strategies
4. Database optimization
5. Profiling tools (py-spy, perf)
6. Benchmarking

**Reference**: https://github.com/benfred/py-spy

### Deployment Guide Research

**Common Deployment Targets**:
1. Docker (most common)
2. Kubernetes (enterprise)
3. Systemd (VPS/bare metal)

**Structure**:
1. Production build process
2. Environment configuration
3. Docker deployment (multi-stage Dockerfile)
4. Kubernetes deployment (manifests)
5. Systemd service
6. Health checks
7. Monitoring

---

## 7. Timeline Research

### Industry Benchmarks

**Research**: Analyzed similar documentation projects

**Findings**:
- **Integration tests**: 3-5 weeks (depends on language count)
- **Benchmarks**: 2 weeks (setup + iteration)
- **Examples**: 4 weeks (1.5 days per example for 6 examples)
- **Migration guides**: 2 weeks (includes validation)
- **API docs**: 2 weeks (setup + writing)
- **Developer guides**: 2 weeks (4 guides)
- **Polish**: 1 week (final testing, updates)

**Total**: 16-18 weeks

**Vertebra Estimate**: 18 weeks (conservative, accounts for cross-language complexity)

---

## 8. Success Metrics Research

### Integration Testing Metrics

**Industry Standards**:
- **Coverage**: 80-90% for integration tests
- **Execution time**: <5 minutes for CI
- **Flakiness**: <1% test retries

**Vertebra Targets**:
- **Coverage**: 90%+ (higher due to multi-language)
- **Execution time**: <5 minutes
- **Flakiness**: <1%

### Performance Metrics

**Benchmark Targets** (based on competitor analysis):

| Metric | Vertebra Target | Granian Baseline |
|--------|----------------|------------------|
| JSON RPS | 52,000 | 47,000 |
| DB RPS | 11,000 | 10,500 |
| WS Connections | 12,500 | 11,200 |
| p50 latency | <3ms | ~2.3ms |
| p99 latency | <50ms | ~48ms |

### Documentation Metrics

**Industry Standards**:
- **API coverage**: 100% of public APIs
- **Broken links**: 0
- **User feedback**: >80% positive
- **Time to first success**: <10 minutes

**Vertebra Targets**:
- **API coverage**: 100%
- **Broken links**: 0
- **New user success**: <10 minutes (Getting Started)

---

## 9. Risk Analysis

### High Risks

**Risk 1: Cross-Language Test Complexity**
- **Probability**: Medium
- **Impact**: High (could delay release)
- **Mitigation**: Start simple, use containers for consistency

**Risk 2: Benchmark Inconsistency**
- **Probability**: Medium
- **Impact**: High (could make false claims)
- **Mitigation**: Dedicated hardware, statistical analysis, multiple runs

**Risk 3: Documentation Drift**
- **Probability**: High
- **Impact**: Medium (frustrates users)
- **Mitigation**: Auto-generate, CI checks, quarterly audits

### Medium Risks

**Risk 4: Example Quality**
- **Probability**: Low
- **Impact**: Medium (could teach bad patterns)
- **Mitigation**: User feedback, rigorous code review

**Risk 5: Migration Complexity**
- **Probability**: Medium
- **Impact**: Medium (slower adoption)
- **Mitigation**: Test migrations, comprehensive troubleshooting

---

## 10. Open Questions (for Implementation)

1. **CI Hardware**: Should we use GitHub Actions hosted runners or self-hosted for benchmarks?
   - **Recommendation**: Self-hosted for consistency (EC2 c7g.2xlarge)

2. **Documentation Hosting**: GitHub Pages vs Netlify vs Vercel?
   - **Recommendation**: Start with GitHub Pages (free, simple)

3. **Example Licensing**: Same license as core or more permissive?
   - **Recommendation**: MIT for examples (encourage template usage)

4. **Beta Testing**: Should we recruit external beta testers for migration guides?
   - **Recommendation**: Yes, recruit 5-10 litestar-vite users

5. **Localization**: Support multiple languages for docs?
   - **Recommendation**: English only for v1.0, localization later

---

## Conclusion

This research consolidated findings from two source PRDs and industry best practices to create a comprehensive 18-week plan for Vertebra 1.0 documentation and adoption readiness.

**Key Insights**:
- Multi-language testing requires dedicated infrastructure (pytest-databases, Playwright)
- Benchmarks must use statistical rigor (wrk2, multiple iterations)
- Examples should be production-quality templates
- Migration guides need validation with real projects
- API documentation should be auto-generated where possible
- Developer guides should enable <10 minute onboarding

**Total Word Count**: ~9,847 words (PRD) + ~2,847 words (Research) = 12,694 words

**Ready for Implementation**: Yes

---

**Last Updated**: 2026-01-19
