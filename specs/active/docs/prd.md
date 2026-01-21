# PRD: Documentation & Adoption - Vertebra 1.0

**Status**: Active
**Version**: 1.0
**Date**: 2026-01-19
**Priority**: P0 - Critical Path to 1.0
**Estimated Duration**: 18 weeks
**Dependencies**: PRD-0 through PRD-5 complete

---

## Executive Summary

This PRD consolidates all **Documentation & Adoption** work required to make Vertebra 1.0 accessible, verifiable, and production-ready for developers. Vertebra has completed its core technical implementation (shared-memory IPC, Python/TypeScript/Rust integration, broker architecture) but requires comprehensive testing, performance validation, example applications, migration guides, and complete documentation before 1.0 release.

**Current State:**
- ✅ Core architecture complete (Rust IPC, Python/TS bindings, broker)
- ✅ Basic functionality working (HTTP server, ASGI bridge, SQLSpec)
- ❌ No integration test suite validating cross-language boundaries
- ❌ No performance benchmarks vs competitors
- ❌ No example applications demonstrating real-world usage
- ❌ Minimal documentation (architecture docs exist, no user guides)
- ❌ No migration guides from existing solutions

**Target State:**
- ✅ 90%+ integration test coverage across Python ↔ TypeScript ↔ Rust boundaries
- ✅ Published benchmarks showing +10% RPS vs Granian, lower p99 than Uvicorn
- ✅ 6 production-quality example applications with READMEs
- ✅ Complete API reference for Python, TypeScript, Rust
- ✅ Migration guides validated with 2+ real project migrations
- ✅ Getting Started guide enabling 10-minute setup
- ✅ Deployment guide covering Docker/Kubernetes/systemd

**Success Criteria:**
1. Integration tests pass on Linux/macOS/Windows with <5 min execution time
2. Benchmarks demonstrate measurable performance superiority
3. All examples build and run without errors
4. 100% public API documentation coverage
5. New users can go from zero to working app in <10 minutes
6. Zero broken links in documentation

---

## Problem Statement

### Current Pain Points

**For Developers Evaluating Vertebra:**
- No proof that cross-language integration works reliably
- No performance data to justify adoption
- No working examples to learn from
- Incomplete API documentation requiring code reading
- No clear migration path from existing tools

**For Production Deployments:**
- Unknown reliability at scale (no integration tests)
- Unknown performance characteristics (no benchmarks)
- No deployment best practices documented
- No monitoring/observability guidance

**For Contributors:**
- Unclear testing patterns for multi-language codebase
- No contribution guidelines for documentation
- No quality gates for examples

### Goals and Objectives

| Goal | Target Metric | Measurement |
|------|---------------|-------------|
| **Validate Reliability** | 90%+ integration coverage | Code coverage tools |
| **Prove Performance** | +10% RPS vs Granian | wrk2 benchmarks |
| **Enable Learning** | 6 working examples | CI tests passing |
| **Complete Documentation** | 100% public API | Doc coverage tools |
| **Facilitate Migration** | 2+ validated migrations | Real project tests |
| **Fast Onboarding** | <10 min setup time | User testing |

---

## Deliverables

### 1. Integration Test Suite

**Location**: `/home/cody/code/c/vertebra/src/py/tests/integration/`

**Architecture:**
```
Integration Tests (90%+ coverage)
├── Python ↔ TypeScript (HTTP contract, serialization, type bridge)
├── TypeScript ↔ Rust (type generation, FFI, pools)
├── Python ↔ Rust (PyO3, shared memory, GIL management)
├── End-to-End (full request lifecycle with real databases)
├── WebSocket (Playwright, bi-directional communication)
└── Resource Pools (connection lifecycle, leak detection)
```

**Technical Approach:**

**Test Infrastructure:**
- **pytest-databases** for container management (already in dependencies)
- **pytest-xdist** for parallel execution (already in dependencies)
- **pytest-timeout** for hanging test detection (already in dependencies)
- **Playwright** for WebSocket testing (NEW dependency)
- **CI matrix**: Linux/macOS/Windows × Python 3.12/3.13

**Test Categories:**

#### 1.1 Python ↔ TypeScript Integration
**Focus**: HTTP API contracts, JSON serialization, type bridge validation

**Key Tests:**
```python
# /home/cody/code/c/vertebra/src/py/tests/integration/test_api_contract.py
async def test_sqlspec_endpoint_matches_types():
    """Validate API endpoint response matches generated TypeScript types."""
    response = await client.get("/api/users")
    assert response.status_code == 200
    # Validate shape matches TS User[] type
    validate_against_typescript_schema(response.json(), "User[]")

async def test_type_bridge_datetime_serialization():
    """Verify datetime serialization Python -> TS matches ISO 8601."""
    user = {"created_at": datetime(2026, 1, 19, 12, 0, 0, tzinfo=timezone.utc)}
    response = await client.post("/api/users", json=user)
    assert response.json()["created_at"] == "2026-01-19T12:00:00Z"
```

**Files:**
- `/home/cody/code/c/vertebra/src/py/tests/integration/test_api_contract.py`
- `/home/cody/code/c/vertebra/src/py/tests/integration/test_type_bridge.py`
- `/home/cody/code/c/vertebra/src/py/tests/integration/test_serialization.py`

#### 1.2 TypeScript ↔ Rust Integration
**Focus**: Type bridge code generation, FFI safety, pool sharing

**Key Tests:**
```typescript
// /home/cody/code/c/vertebra/src/ts/tests/integration/type-bridge.test.ts
test("generated TypeScript types match Rust definitions", () => {
  const generatedUser: User = { id: 1, name: "test" };
  expect(validateRustType(generatedUser, "User")).toBe(true);
});

test("channel handles round-trip messages correctly", async () => {
  const channel = createChannelPair();
  channel.send({ type: "query", sql: "SELECT 1" });
  const response = await channel.receive();
  expect(response.rows).toEqual([[1]]);
  channel.close();
});
```

**Files:**
- `/home/cody/code/c/vertebra/src/ts/tests/integration/type-bridge.test.ts`
- `/home/cody/code/c/vertebra/src/ts/tests/integration/channel-ffi.test.ts`
- `/home/cody/code/c/vertebra/src/ts/tests/integration/pool-sharing.test.ts`

#### 1.3 Python ↔ Rust Integration
**Focus**: PyO3 FFI, shared memory safety, GIL management

**Key Tests:**
```python
# /home/cody/code/c/vertebra/src/py/tests/integration/test_pyo3_ffi.py
def test_shared_memory_channel_no_leaks():
    """Verify shared memory cleanup after channel close."""
    initial_shm = list_shm_objects()

    with create_channel_pair() as (tx, rx):
        tx.send(b"test message")
        msg = rx.receive(timeout=1.0)
        assert msg == b"test message"

    final_shm = list_shm_objects()
    assert len(final_shm) == len(initial_shm), "Memory leak detected"

async def test_pool_concurrent_access():
    """Test Python and Rust accessing same pool concurrently."""
    async with broker_pool.acquire() as conn:
        # Python query
        py_result = await conn.execute("SELECT 1")

        # Rust query via FFI
        rs_result = await rust_pool_query(pool_id, "SELECT 2")

        assert py_result == [[1]]
        assert rs_result == [[2]]
```

**Files:**
- `/home/cody/code/c/vertebra/src/py/tests/integration/test_pyo3_ffi.py`
- `/home/cody/code/c/vertebra/src/py/tests/integration/test_pool_sharing.py`
- `/home/cody/code/c/vertebra/src/py/tests/integration/test_memory_safety.py`

#### 1.4 End-to-End Tests
**Focus**: Full request lifecycle with real databases

**Key Tests:**
```python
# /home/cody/code/c/vertebra/src/py/tests/integration/test_e2e_database.py
@pytest.mark.integration
async def test_full_stack_crud_postgres(postgres_container):
    """End-to-end CRUD operations through entire stack."""
    # Create via HTTP -> ASGI -> Broker -> Postgres
    response = await client.post("/api/users", json={"name": "Alice"})
    user_id = response.json()["id"]

    # Read via HTTP
    response = await client.get(f"/api/users/{user_id}")
    assert response.json()["name"] == "Alice"

    # Update via HTTP
    response = await client.patch(f"/api/users/{user_id}", json={"name": "Bob"})
    assert response.json()["name"] == "Bob"

    # Delete via HTTP
    response = await client.delete(f"/api/users/{user_id}")
    assert response.status_code == 204

    # Verify deletion
    response = await client.get(f"/api/users/{user_id}")
    assert response.status_code == 404
```

**Container Management** (using pytest-databases):
```python
# /home/cody/code/c/vertebra/src/py/tests/integration/conftest.py
from pytest_databases.docker import DockerServiceRegistry

@pytest.fixture(scope="module")
def postgres_container():
    """Postgres 16 container for integration tests."""
    registry = DockerServiceRegistry()
    postgres = registry.postgres(image="postgres:16-alpine")
    yield postgres
    postgres.stop()

@pytest.fixture(scope="module")
def redis_container():
    """Redis 7 container for integration tests."""
    registry = DockerServiceRegistry()
    redis = registry.redis(image="redis:7-alpine")
    yield redis
    redis.stop()
```

**Files:**
- `/home/cody/code/c/vertebra/src/py/tests/integration/conftest.py`
- `/home/cody/code/c/vertebra/src/py/tests/integration/test_e2e_database.py`
- `/home/cody/code/c/vertebra/src/py/tests/integration/test_e2e_auth.py`
- `/home/cody/code/c/vertebra/src/py/tests/integration/test_e2e_file_upload.py`

#### 1.5 WebSocket Tests
**Focus**: Bi-directional communication, connection lifecycle

**Key Tests (using Playwright):**
```python
# /home/cody/code/c/vertebra/src/py/tests/integration/test_websocket.py
async def test_websocket_bidirectional_messages(playwright_page):
    """Test WebSocket message exchange with Playwright."""
    ws_messages = []

    page.on("websocket", lambda ws: ws_messages.append(ws))

    await page.goto("http://localhost:8000/ws-test")
    await page.wait_for_selector("#connected")

    # Send message from client
    await page.evaluate("window.ws.send(JSON.stringify({type: 'ping'}))")

    # Wait for server response
    response = await page.wait_for_function(
        "document.getElementById('messages').innerText.includes('pong')"
    )
    assert response

async def test_websocket_reconnect_on_disconnect():
    """Verify reconnection logic works correctly."""
    # Initial connection
    ws = await websockets.connect("ws://localhost:8000/ws")
    await ws.send("test")
    assert await ws.recv() == "test"

    # Force disconnect
    await ws.close()

    # Should auto-reconnect
    await asyncio.sleep(1)
    ws = await websockets.connect("ws://localhost:8000/ws")
    await ws.send("test2")
    assert await ws.recv() == "test2"
```

**Files:**
- `/home/cody/code/c/vertebra/src/py/tests/integration/test_websocket.py`
- `/home/cody/code/c/vertebra/src/py/tests/integration/test_websocket_concurrent.py`

#### 1.6 Resource Pool Tests
**Focus**: Connection lifecycle, leak detection, health checks

**Key Tests:**
```python
# /home/cody/code/c/vertebra/src/py/tests/integration/test_pool_lifecycle.py
async def test_pool_exhaustion_recovery():
    """Verify pool handles exhaustion gracefully."""
    pool = BrokerPool(max_connections=2)

    # Acquire all connections
    conn1 = await pool.acquire()
    conn2 = await pool.acquire()

    # Third should timeout
    with pytest.raises(PoolExhaustedError):
        async with asyncio.timeout(1.0):
            await pool.acquire()

    # Release and verify recovery
    await pool.release(conn1)
    conn3 = await pool.acquire()
    assert conn3 is not None

async def test_pool_health_check_removes_dead_connections():
    """Verify health checks remove unhealthy connections."""
    pool = BrokerPool(health_check_interval=1.0)

    # Acquire and corrupt connection
    conn = await pool.acquire()
    await simulate_connection_failure(conn)
    await pool.release(conn)

    # Wait for health check
    await asyncio.sleep(2.0)

    # Next acquire should get fresh connection
    new_conn = await pool.acquire()
    assert new_conn is not conn
    assert await new_conn.ping() == "pong"
```

**Files:**
- `/home/cody/code/c/vertebra/src/py/tests/integration/test_pool_lifecycle.py`
- `/home/cody/code/c/vertebra/src/py/tests/integration/test_pool_health.py`
- `/home/cody/code/c/vertebra/src/py/tests/integration/test_pool_memory_leaks.py`

**CI Integration:**
```yaml
# .github/workflows/integration-tests.yml
name: Integration Tests

on: [push, pull_request]

jobs:
  integration:
    strategy:
      matrix:
        os: [ubuntu-latest, macos-latest, windows-latest]
        python-version: ["3.12", "3.13"]

    runs-on: ${{ matrix.os }}

    steps:
      - uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: ${{ matrix.python-version }}

      - name: Install dependencies
        run: |
          pip install uv
          uv sync --group test

      - name: Install Playwright
        run: |
          uv run playwright install chromium

      - name: Run integration tests
        run: |
          uv run pytest src/py/tests/integration/ \
            -m integration \
            --integration \
            --cov=vertebra \
            --cov-report=xml \
            -v

      - name: Upload coverage
        uses: codecov/codecov-action@v4
        with:
          files: ./coverage.xml
```

**Acceptance Criteria:**
- [ ] All integration tests pass on Linux/macOS/Windows
- [ ] 90%+ code coverage for integration points
- [ ] Tests complete in <5 minutes
- [ ] Zero memory leaks detected (verified with pytest-memray)
- [ ] CI runs integration tests on every commit
- [ ] Coverage reports published to Codecov

**Estimated Effort:** 3 weeks (Weeks 3-5)

---

### 2. Performance Benchmarks

**Location**: `/home/cody/code/c/vertebra/benchmarks/`

**Goal**: Establish Vertebra as highest-performing Python ASGI server through empirical evidence

**Comparison Targets:**
- **Granian** (current fastest Python ASGI server, Rust-based)
- **Uvicorn** (most popular ASGI server)
- **Hypercorn** (HTTP/2 and HTTP/3 capable)

**Benchmark Scenarios:**

#### 2.1 Simple JSON Response
**Purpose**: Measure pure server overhead

```python
# /home/cody/code/c/vertebra/benchmarks/apps/simple_json.py
from litestar import Litestar, get

@get("/json")
async def json_endpoint() -> dict[str, str]:
    return {"message": "Hello, World!"}

app = Litestar(route_handlers=[json_endpoint])
```

**Benchmark Script:**
```bash
# /home/cody/code/c/vertebra/benchmarks/scripts/bench_json.sh
#!/bin/bash

SERVER=$1
DURATION=30
CONNECTIONS=100
RATE=10000

case $SERVER in
  vertebra)
    vertebra serve benchmarks.apps.simple_json:app --port 8000 &
    ;;
  granian)
    granian --interface asgi benchmarks.apps.simple_json:app --port 8000 &
    ;;
  uvicorn)
    uvicorn benchmarks.apps.simple_json:app --port 8000 &
    ;;
  hypercorn)
    hypercorn benchmarks.apps.simple_json:app --bind 0.0.0.0:8000 &
    ;;
esac

SERVER_PID=$!
sleep 2  # Warmup

# Run wrk2 benchmark
wrk2 -t4 -c$CONNECTIONS -d${DURATION}s -R$RATE \
  --latency http://localhost:8000/json \
  > results/${SERVER}_json.txt

kill $SERVER_PID
```

**Expected Results:**
- **Target**: Vertebra > 50,000 RPS (+10% vs Granian ~45k RPS)
- **Latency**: p50 <3ms, p95 <10ms, p99 <50ms

#### 2.2 Database Query Benchmark
**Purpose**: Test connection pooling efficiency

```python
# /home/cody/code/c/vertebra/benchmarks/apps/database.py
from litestar import Litestar, get
from vertebra.extensions.litestar.sqlspec import VertebraSQLSpecPlugin
from vertebra.extensions.sqlspec.postgres import VertebraPostgresAsyncConfig

config = VertebraPostgresAsyncConfig(pool="bench_pg")

@get("/users")
async def get_users(session: AsyncSession) -> list[dict]:
    result = await session.execute("SELECT id, name FROM users LIMIT 10")
    return [{"id": r[0], "name": r[1]} for r in result.rows]

app = Litestar(
    route_handlers=[get_users],
    plugins=[VertebraSQLSpecPlugin(configs=[config])]
)
```

**Setup:**
```sql
-- benchmarks/fixtures/schema.sql
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(255) NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

INSERT INTO users (name, email)
SELECT
    'User ' || generate_series,
    'user' || generate_series || '@example.com'
FROM generate_series(1, 100000);
```

**Expected Results:**
- **Target**: Vertebra > 10,000 RPS (+5% vs Granian)
- **Pool Efficiency**: <10ms acquire time at 95th percentile

#### 2.3 WebSocket Benchmark
**Purpose**: Measure concurrent connection capacity

```python
# /home/cody/code/c/vertebra/benchmarks/apps/websocket.py
from litestar import Litestar, websocket_listener
from litestar.connection import WebSocket

@websocket_listener("/ws")
async def websocket_handler(socket: WebSocket) -> None:
    await socket.accept()
    async for message in socket.iter_data():
        await socket.send_json({"echo": message})

app = Litestar(route_handlers=[websocket_handler])
```

**Benchmark Script:**
```python
# /home/cody/code/c/vertebra/benchmarks/scripts/bench_websocket.py
import asyncio
import websockets
import time

async def client(client_id: int, duration: int):
    uri = "ws://localhost:8000/ws"
    messages_sent = 0

    async with websockets.connect(uri) as ws:
        start = time.time()
        while time.time() - start < duration:
            await ws.send(f"Message {messages_sent}")
            await ws.recv()
            messages_sent += 1

    return messages_sent

async def main():
    concurrent_clients = 1000
    duration = 30

    tasks = [client(i, duration) for i in range(concurrent_clients)]
    results = await asyncio.gather(*tasks)

    total_messages = sum(results)
    print(f"Total messages: {total_messages}")
    print(f"Messages/sec: {total_messages / duration:.2f}")

if __name__ == "__main__":
    asyncio.run(main())
```

**Expected Results:**
- **Target**: 10,000+ concurrent WebSocket connections
- **Throughput**: 50,000+ messages/sec aggregate

#### 2.4 Static File Serving
**Purpose**: Test asset delivery efficiency

```python
# /home/cody/code/c/vertebra/benchmarks/apps/static_files.py
from litestar import Litestar
from litestar.static_files import create_static_files_router

app = Litestar(
    route_handlers=[
        create_static_files_router(
            path="/static",
            directories=["benchmarks/fixtures/static"]
        )
    ]
)
```

**Test Files:**
```bash
# benchmarks/fixtures/static/
├── small.txt    (10KB)
├── medium.txt   (500KB)
└── large.txt    (5MB)
```

**Expected Results:**
- **Small files**: >20,000 RPS
- **Large files**: Limited by network bandwidth

#### 2.5 Mixed Workload
**Purpose**: Simulate realistic application

```bash
# /home/cody/code/c/vertebra/benchmarks/scripts/bench_mixed.lua
-- wrk2 Lua script for mixed workload
request = function()
    local paths = {"/json", "/users", "/static/small.txt"}
    local path = paths[math.random(#paths)]
    return wrk.format("GET", path)
end
```

**Methodology:**

**Hardware Requirements:**
- AWS EC2 c7g.2xlarge (8 vCPU, 16GB RAM) or equivalent
- Amazon Linux 2023
- Fixed CPU frequency (disable turbo boost)
- No other processes running

**Statistical Analysis:**
```python
# /home/cody/code/c/vertebra/benchmarks/scripts/analyze.py
import json
import statistics

def analyze_results(server: str, scenario: str):
    """Statistical analysis of benchmark results."""
    runs = []
    for i in range(10):
        with open(f"results/{server}_{scenario}_run{i}.json") as f:
            runs.append(json.load(f))

    rps_values = [r["requests_per_sec"] for r in runs]

    return {
        "mean": statistics.mean(rps_values),
        "median": statistics.median(rps_values),
        "stdev": statistics.stdev(rps_values),
        "min": min(rps_values),
        "max": max(rps_values),
        "variance_pct": (statistics.stdev(rps_values) / statistics.mean(rps_values)) * 100
    }
```

**Reporting:**
```markdown
# Vertebra Benchmark Results

## Test Environment
- **Hardware**: AWS EC2 c7g.2xlarge (8 vCPU, 16GB RAM)
- **OS**: Amazon Linux 2023
- **Python**: 3.12.7
- **Date**: 2026-01-19

## Results Summary

| Scenario | Vertebra | Granian | Uvicorn | Hypercorn |
|----------|----------|---------|---------|-----------|
| JSON (RPS) | 52,341 | 47,612 | 31,245 | 28,732 |
| DB Query (RPS) | 11,234 | 10,678 | 8,421 | 7,892 |
| WebSocket (conns) | 12,500 | 11,200 | 8,500 | 7,800 |
| Static 10KB (RPS) | 24,567 | 22,341 | 18,234 | 16,789 |

## Latency Distribution (JSON endpoint)

| Percentile | Vertebra | Granian | Uvicorn |
|------------|----------|---------|---------|
| p50 | 2.1ms | 2.3ms | 3.2ms |
| p95 | 8.7ms | 9.4ms | 12.3ms |
| p99 | 42.3ms | 48.7ms | 67.8ms |

**Winner**: Vertebra is **10% faster** than Granian on JSON endpoints.
```

**Files:**
- `/home/cody/code/c/vertebra/benchmarks/README.md`
- `/home/cody/code/c/vertebra/benchmarks/apps/simple_json.py`
- `/home/cody/code/c/vertebra/benchmarks/apps/database.py`
- `/home/cody/code/c/vertebra/benchmarks/apps/websocket.py`
- `/home/cody/code/c/vertebra/benchmarks/apps/static_files.py`
- `/home/cody/code/c/vertebra/benchmarks/scripts/bench_json.sh`
- `/home/cody/code/c/vertebra/benchmarks/scripts/bench_db.sh`
- `/home/cody/code/c/vertebra/benchmarks/scripts/bench_websocket.py`
- `/home/cody/code/c/vertebra/benchmarks/scripts/bench_static.sh`
- `/home/cody/code/c/vertebra/benchmarks/scripts/analyze.py`
- `/home/cody/code/c/vertebra/benchmarks/scripts/generate_report.py`
- `/home/cody/code/c/vertebra/benchmarks/results/.gitkeep`

**Acceptance Criteria:**
- [ ] Vertebra achieves +10% RPS vs Granian on JSON endpoint
- [ ] p99 latency lower than Uvicorn
- [ ] Handles 10,000+ concurrent WebSocket connections
- [ ] Benchmark methodology fully documented
- [ ] Reproducible results (<5% variance across 10 runs)
- [ ] Comparison charts generated

**Estimated Effort:** 2 weeks (Weeks 6-7)

---

### 3. Example Applications

**Location**: `/home/cody/code/c/vertebra/examples/`

**Goal**: Provide 6 production-quality reference applications

#### 3.1 Todo API (SQLSpec + Litestar)
**Purpose**: Simple CRUD API demonstrating SQLSpec integration

**Features:**
- SQLSpec async configuration
- CRUD operations (Create, Read, Update, Delete)
- Input validation with Pydantic
- Type-safe API with generated TypeScript client
- Docker Compose setup

**Structure:**
```
examples/todo-api/
├── backend/
│   ├── app.py              # Litestar app
│   ├── models.py           # Pydantic models
│   ├── routes.py           # API endpoints
│   └── config.py           # SQLSpec config
├── frontend/
│   ├── src/
│   │   ├── api/            # Generated TypeScript client
│   │   ├── components/     # React components
│   │   └── main.tsx
│   ├── vite.config.ts
│   └── package.json
├── docker-compose.yml      # Postgres + API + Frontend
├── README.md
└── .env.example
```

**Key Code:**
```python
# examples/todo-api/backend/app.py
from litestar import Litestar, post, get, patch, delete
from litestar.datastructures import State
from vertebra.extensions.litestar.sqlspec import VertebraSQLSpecPlugin
from vertebra.extensions.sqlspec.postgres import VertebraPostgresAsyncConfig

from .models import Todo, TodoCreate, TodoUpdate

config = VertebraPostgresAsyncConfig(pool="todo_db")

@post("/todos")
async def create_todo(data: TodoCreate, session: AsyncSession) -> Todo:
    result = await session.insert("todos").values(**data.model_dump()).returning("*")
    return Todo.model_validate(result.one())

@get("/todos")
async def list_todos(session: AsyncSession) -> list[Todo]:
    result = await session.select("*").from_("todos").order_by("created_at DESC")
    return [Todo.model_validate(row) for row in result.rows]

@patch("/todos/{todo_id:int}")
async def update_todo(todo_id: int, data: TodoUpdate, session: AsyncSession) -> Todo:
    result = await session.update("todos").set(**data.model_dump(exclude_unset=True)).where(id=todo_id).returning("*")
    return Todo.model_validate(result.one())

@delete("/todos/{todo_id:int}", status_code=204)
async def delete_todo(todo_id: int, session: AsyncSession) -> None:
    await session.delete("todos").where(id=todo_id)

app = Litestar(
    route_handlers=[create_todo, list_todos, update_todo, delete_todo],
    plugins=[VertebraSQLSpecPlugin(configs=[config])]
)
```

**README Template:**
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
- ✅ Docker Compose setup

## API Endpoints

- `POST /todos` - Create todo
- `GET /todos` - List todos
- `PATCH /todos/{id}` - Update todo
- `DELETE /todos/{id}` - Delete todo

## Architecture

\`\`\`
React Frontend → Litestar API → SQLSpec → Vertebra Broker → Postgres
\`\`\`

## Development

\`\`\`bash
# Backend
cd backend
uv sync
uv run vertebra serve app:app

# Frontend
cd frontend
bun install
bun dev
\`\`\`
```

**Files:**
- `/home/cody/code/c/vertebra/examples/todo-api/` (complete directory)

#### 3.2 Blog Platform (SQLAlchemy + Advanced Alchemy)
**Purpose**: Demonstrate Advanced Alchemy Repository pattern

**Features:**
- SQLAlchemy ORM models
- Advanced Alchemy Repository/Service patterns
- File uploads with storage pool
- User authentication
- Alembic migrations

**Structure:**
```
examples/blog-platform/
├── backend/
│   ├── app.py
│   ├── models/             # SQLAlchemy models
│   │   ├── user.py
│   │   ├── post.py
│   │   └── comment.py
│   ├── repositories/       # AA repositories
│   ├── services/           # AA services
│   ├── routes/
│   └── migrations/         # Alembic
├── docker-compose.yml
└── README.md
```

**Key Code:**
```python
# examples/blog-platform/backend/repositories/post.py
from advanced_alchemy.repository import SQLAlchemyAsyncRepository
from advanced_alchemy.service import SQLAlchemyAsyncRepositoryService

from ..models import Post

class PostRepository(SQLAlchemyAsyncRepository[Post]):
    model_type = Post

class PostService(SQLAlchemyAsyncRepositoryService[Post]):
    repository_type = PostRepository

    async def create_with_image(self, data: dict, image: UploadFile) -> Post:
        # Upload image to storage pool
        storage_path = await self.storage.save(image)
        data["image_url"] = storage_path
        return await self.create(data)
```

**Files:**
- `/home/cody/code/c/vertebra/examples/blog-platform/` (complete directory)

#### 3.3 AI Tools Server (MCP)
**Purpose**: Demonstrate MCP protocol integration

**Features:**
- 5-10 useful AI tools
- MCP server with all transports (Stdio/SSE/WebSocket)
- Claude Desktop integration
- Tool discovery and execution

**Tools Implemented:**
1. **Database Query Tool**: Execute SQL safely
2. **File Search Tool**: Search codebase
3. **API Call Tool**: Make HTTP requests
4. **Data Analysis Tool**: Analyze CSV/JSON
5. **Code Execution Tool**: Run Python snippets in sandbox

**Structure:**
```
examples/ai-tools/
├── server/
│   ├── tools/
│   │   ├── database.py
│   │   ├── file_search.py
│   │   ├── api_call.py
│   │   ├── data_analysis.py
│   │   └── code_exec.py
│   └── app.py
├── claude_desktop_config.json
└── README.md
```

**Key Code:**
```python
# examples/ai-tools/server/tools/database.py
from vertebra.mcp import mcp_tool

@mcp_tool(
    name="query_database",
    description="Execute read-only SQL query against database",
    schema={
        "type": "object",
        "properties": {
            "sql": {"type": "string", "description": "SELECT query to execute"},
            "database": {"type": "string", "enum": ["main", "analytics"]}
        },
        "required": ["sql"]
    }
)
async def query_database(sql: str, database: str = "main") -> list[dict]:
    """Execute SQL query and return results."""
    if not sql.strip().upper().startswith("SELECT"):
        raise ValueError("Only SELECT queries allowed")

    async with get_session(database) as session:
        result = await session.execute(sql)
        return [dict(row) for row in result.rows]
```

**Claude Desktop Config:**
```json
{
  "mcpServers": {
    "vertebra-tools": {
      "command": "uv",
      "args": ["run", "vertebra", "mcp", "--transport", "stdio"],
      "cwd": "/path/to/examples/ai-tools"
    }
  }
}
```

**Files:**
- `/home/cody/code/c/vertebra/examples/ai-tools/` (complete directory)

#### 3.4 Realtime Chat (WebSocket)
**Purpose**: Real-time collaborative application

**Features:**
- Multi-user chat rooms
- Presence tracking
- Message persistence
- Typing indicators
- Reconnection handling

**Files:**
- `/home/cody/code/c/vertebra/examples/realtime-chat/` (complete directory)

#### 3.5 Data Pipeline (Resource Pool Sharing)
**Purpose**: Demonstrate Python + Rust accessing shared pools

**Features:**
- Shared PostgreSQL pool
- Python SQLAlchemy integration
- Rust sqlx integration
- Performance comparison

**Files:**
- `/home/cody/code/c/vertebra/examples/data-pipeline/` (complete directory)

#### 3.6 gRPC Microservice
**Purpose**: Advanced gRPC integration

**Features:**
- Protocol Buffers definitions
- gRPC server in Rust
- Python client
- TypeScript gRPC-Web client
- Streaming RPC

**Files:**
- `/home/cody/code/c/vertebra/examples/grpc-service/` (complete directory)

**Example Quality Standards:**
- Production-quality code (no shortcuts)
- Comprehensive README with setup instructions
- Working tests for each example
- Docker Compose for easy local development
- Clear explanation of patterns demonstrated

**Acceptance Criteria:**
- [ ] All 6 examples build without errors
- [ ] Each demonstrates unique Vertebra capability
- [ ] READMEs are complete and accurate
- [ ] Examples tested on macOS, Linux, Windows
- [ ] Code quality passes linting standards
- [ ] Each example has working tests

**Estimated Effort:** 4 weeks (Weeks 8-11)

---

### 4. Migration Guides

**Location**: `/home/cody/code/c/vertebra/docs/migration/`

**Goal**: Enable smooth migration from existing solutions

#### 4.1 Migration from litestar-vite

**Target Audience**: Developers using litestar-vite plugin

**Guide Structure:**

**File**: `/home/cody/code/c/vertebra/docs/migration/from-litestar-vite.md`

```markdown
# Migrating from litestar-vite to Vertebra

## Overview

Vertebra provides superior performance and type safety compared to litestar-vite through:
- Shared-memory broker (no socket overhead)
- Automatic TypeScript type generation from Python
- Unified resource pooling

**Migration Time**: ~2-4 hours for typical application

## Prerequisites

- Python 3.12+
- Bun (for drivers and frontend build)
- Existing litestar-vite application

## Step 1: Update Dependencies

\`\`\`toml
# pyproject.toml

[project.dependencies]
# Remove
- litestar-vite

# Add
+ vertebra[litestar]
\`\`\`

\`\`\`bash
uv sync
\`\`\`

## Step 2: Update Litestar Configuration

**Before:**
\`\`\`python
from litestar import Litestar
from litestar_vite import VitePlugin

app = Litestar(
    plugins=[VitePlugin()],
    route_handlers=[...],
)
\`\`\`

**After:**
\`\`\`python
from litestar import Litestar
from vertebra.extensions.litestar import VertebraPlugin

app = Litestar(
    plugins=[VertebraPlugin()],
    route_handlers=[...],
)
\`\`\`

## Step 3: Enable Type Bridge

\`\`\`python
# config.py
from vertebra.extensions.litestar import VertebraPlugin

plugin = VertebraPlugin(
    type_bridge_enabled=True,
    vite_config="vite.config.ts"
)
\`\`\`

## Step 4: Update Frontend Imports

\`\`\`typescript
// Before
import { apiClient } from "./api/client";

// After (uses generated types)
import { api } from "./api/generated";

// Now type-safe!
const users = await api.users.list(); // Type: User[]
\`\`\`

## Step 5: Test Migration

\`\`\`bash
# Run development server
uv run vertebra serve app:app

# Run tests
uv run pytest

# Build production
uv run vertebra build
\`\`\`

## Troubleshooting

### Issue: Type generation fails

**Symptom**: No TypeScript types generated

**Solution**:
\`\`\`bash
# Manually trigger generation
uv run vertebra types generate
\`\`\`

### Issue: Vite proxy not working

**Symptom**: API calls fail with CORS errors

**Solution**:
\`\`\`typescript
// vite.config.ts
export default defineConfig({
  server: {
    proxy: {
      "/api": "http://localhost:8000"
    }
  }
})
\`\`\`

## Rollback Plan

If migration issues occur:

\`\`\`bash
# Revert pyproject.toml
git checkout pyproject.toml

# Reinstall
uv sync

# Restart with litestar-vite
\`\`\`

## Next Steps

- [ ] Enable resource pooling for database connections
- [ ] Set up production deployment
- [ ] Configure monitoring

## Getting Help

- Documentation: https://vertebra.dev/docs
- GitHub Discussions: https://github.com/cofin/vertebra/discussions
- Discord: https://discord.gg/vertebra
\`\`\`
```

**Files:**
- `/home/cody/code/c/vertebra/docs/migration/from-litestar-vite.md`

#### 4.2 Migration from Standalone ASGI Servers

**Target Audience**: Developers using Uvicorn, Granian, or Hypercorn

**File**: `/home/cody/code/c/vertebra/docs/migration/from-asgi-servers.md`

```markdown
# Migrating from Standalone ASGI Servers to Vertebra

## Overview

Vertebra provides:
- **10% higher throughput** than Granian
- **Lower p99 latency** than Uvicorn
- Integrated type bridge for frontends
- Shared resource pooling

## When to Migrate

✅ **Good fit:**
- You have or plan to add a TypeScript frontend
- You want better performance
- You need shared resource pools

❌ **Not recommended:**
- Pure API server with no frontend
- Serverless deployments (Lambda/Cloud Functions)

## Migration Paths

### From Uvicorn

**Before:**
\`\`\`bash
uvicorn app:app --host 0.0.0.0 --port 8000 --workers 4
\`\`\`

**After:**
\`\`\`bash
vertebra serve app:app --host 0.0.0.0 --port 8000 --workers 4
\`\`\`

**Configuration Changes:**

| Uvicorn Flag | Vertebra Equivalent | Notes |
|--------------|---------------------|-------|
| `--reload` | `--reload` | Same |
| `--workers N` | `--workers N` | Same |
| `--log-level` | `--log-level` | Same |
| `--ssl-keyfile` | `--ssl-keyfile` | Same |
| `--ssl-certfile` | `--ssl-certfile` | Same |

### From Granian

**Before:**
\`\`\`bash
granian --interface asgi app:app --port 8000 --workers 4
\`\`\`

**After:**
\`\`\`bash
vertebra serve app:app --port 8000 --workers 4
\`\`\`

**Key Differences:**
- Vertebra uses Bun runtime (similar performance to Granian's Rust)
- Vertebra adds type bridge and resource pooling
- Vertebra has integrated frontend build support

## Deployment Changes

### Docker

**Before (Uvicorn):**
\`\`\`dockerfile
FROM python:3.12
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
CMD ["uvicorn", "app:app", "--host", "0.0.0.0"]
\`\`\`

**After (Vertebra):**
\`\`\`dockerfile
FROM python:3.12
WORKDIR /app

# Install Bun
RUN curl -fsSL https://bun.sh/install | bash
ENV PATH="/root/.bun/bin:$PATH"

COPY pyproject.toml .
RUN pip install uv && uv sync

COPY . .
CMD ["vertebra", "serve", "app:app", "--host", "0.0.0.0"]
\`\`\`

### Systemd Service

**Before:**
\`\`\`ini
[Service]
ExecStart=/usr/bin/uvicorn app:app --workers 4
\`\`\`

**After:**
\`\`\`ini
[Service]
ExecStart=/usr/local/bin/vertebra serve app:app --workers 4
\`\`\`

## Performance Comparison

Run benchmarks before and after:

\`\`\`bash
# Before migration (with Uvicorn)
wrk2 -t4 -c100 -d30s -R10000 http://localhost:8000/api/users

# After migration (with Vertebra)
wrk2 -t4 -c100 -d30s -R10000 http://localhost:8000/api/users
\`\`\`

**Expected Improvements:**
- +10-15% RPS on simple endpoints
- -20% p99 latency
- Better memory efficiency

## Testing Checklist

- [ ] All routes accessible
- [ ] WebSocket connections working (if applicable)
- [ ] Static files serving correctly
- [ ] Database connections functional
- [ ] Performance benchmarks run
- [ ] Production build successful
- [ ] Monitoring/logging configured

## Rollback Plan

\`\`\`bash
# Revert to Uvicorn
pip uninstall vertebra
pip install uvicorn[standard]
uvicorn app:app
\`\`\`

## Next Steps

1. Add TypeScript frontend (optional)
2. Enable type bridge for API type safety
3. Configure resource pooling
4. Set up production monitoring
\`\`\`
```

**Files:**
- `/home/cody/code/c/vertebra/docs/migration/from-asgi-servers.md`

**Validation:**
- Migrate 2+ real projects using guides
- Collect feedback from test migrations
- Update troubleshooting based on actual issues

**Acceptance Criteria:**
- [ ] 2+ real project migrations completed successfully
- [ ] Migration guides tested by external developers
- [ ] Clear before/after code examples
- [ ] Troubleshooting covers 90% of common issues
- [ ] Average migration time documented
- [ ] Zero data loss scenarios

**Estimated Effort:** 2 weeks (Weeks 12-13)

---

### 5. API Documentation

**Location**: `/home/cody/code/c/vertebra/docs/api/`

**Goal**: 100% public API documentation coverage

#### 5.1 Python API Documentation

**Tool**: Sphinx with autodoc + napoleon extensions

**Setup:**
```bash
# Install docs dependencies
uv add --group docs sphinx sphinx-autobuild sphinx-rtd-theme
```

**Configuration:**
```python
# /home/cody/code/c/vertebra/docs/conf.py
project = "Vertebra"
copyright = "2026, Vertebra Contributors"
author = "Cody Fincher"
release = "1.0.0"

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.intersphinx",
    "sphinx.ext.viewcode",
    "sphinx.ext.autosummary",
]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

html_theme = "sphinx_rtd_theme"
html_static_path = ["_static"]

# Autodoc settings
autodoc_default_options = {
    "members": True,
    "member-order": "bysource",
    "special-members": "__init__",
    "undoc-members": True,
    "exclude-members": "__weakref__"
}

# Napoleon settings (Google/NumPy style docstrings)
napoleon_google_docstring = True
napoleon_numpy_docstring = True
napoleon_include_init_with_doc = True

# Intersphinx mappings
intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "litestar": ("https://docs.litestar.dev/latest/", None),
}
```

**Documentation Structure:**
```
docs/
├── conf.py
├── index.rst
├── api/
│   ├── index.rst
│   ├── extensions/
│   │   ├── sqlspec.rst
│   │   ├── sqlalchemy.rst
│   │   ├── advanced_alchemy.rst
│   │   └── litestar.rst
│   ├── resources/
│   │   └── broker.rst
│   ├── server/
│   │   ├── worker.rst
│   │   └── supervisor.rst
│   └── mcp/
│       └── tools.rst
├── guides/
│   ├── getting-started.rst
│   ├── type-bridge.rst
│   ├── performance.rst
│   └── deployment.rst
├── migration/
│   ├── from-litestar-vite.rst
│   └── from-asgi-servers.rst
└── _static/
    └── custom.css
```

**Example Documentation:**
```python
# /home/cody/code/c/vertebra/src/py/vertebra/extensions/sqlspec/_driver.py
class VertebraProxyAsyncDriver:
    """
    Async driver that routes database operations through Vertebra broker.

    This driver provides automatic connection pooling, health checks, and
    performance monitoring through the shared-memory broker architecture.

    Args:
        pool_name: Name of the pool configured in broker
        timeout: Operation timeout in seconds (default: 30.0)
        auto_commit: Enable auto-commit mode (default: False)

    Example:
        >>> from vertebra.extensions.sqlspec.postgres import VertebraPostgresAsyncConfig
        >>> config = VertebraPostgresAsyncConfig(pool="main")
        >>> async with config.provide_session() as session:
        ...     result = await session.execute("SELECT 1")
        ...     print(result.rows)
        [[1]]

    See Also:
        :class:`VertebraProxySyncDriver` for synchronous operations
        :class:`vertebra.resources.broker.BrokerClient` for low-level IPC

    Note:
        All database operations are routed through the broker process,
        enabling connection pooling across Python workers.

    Attributes:
        pool_name (str): Configured pool name
        timeout (float): Default operation timeout
        is_connected (bool): Connection status
    """

    def __init__(
        self,
        pool_name: str,
        timeout: float = 30.0,
        auto_commit: bool = False
    ) -> None:
        """Initialize driver with broker pool configuration."""
        self.pool_name = pool_name
        self.timeout = timeout
        self.auto_commit = auto_commit
        self._client: BrokerClient | None = None

    async def execute(self, query: str, params: dict | None = None) -> Result:
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
        # Implementation...
```

**Build Process:**
```bash
# /home/cody/code/c/vertebra/Makefile
docs:
	cd docs && sphinx-build -b html . _build/html

docs-live:
	cd docs && sphinx-autobuild . _build/html --open-browser

docs-clean:
	rm -rf docs/_build
```

**Files:**
- `/home/cody/code/c/vertebra/docs/conf.py`
- `/home/cody/code/c/vertebra/docs/index.rst`
- `/home/cody/code/c/vertebra/docs/api/**/*.rst`

#### 5.2 TypeScript API Documentation

**Tool**: TypeDoc

**Setup:**
```json
// /home/cody/code/c/vertebra/src/ts/package.json
{
  "devDependencies": {
    "typedoc": "^0.25.0",
    "typedoc-plugin-markdown": "^3.17.0"
  },
  "scripts": {
    "docs": "typedoc --out ../../docs/_build/ts-api src/index.ts"
  }
}
```

**Configuration:**
```json
// /home/cody/code/c/vertebra/src/ts/typedoc.json
{
  "entryPoints": ["src/index.ts"],
  "out": "../../docs/_build/ts-api",
  "plugin": ["typedoc-plugin-markdown"],
  "readme": "README.md",
  "includeVersion": true,
  "categorizeByGroup": true,
  "categoryOrder": [
    "Broker",
    "Server",
    "MCP",
    "Drivers",
    "*"
  ]
}
```

**Example Documentation:**
```typescript
/**
 * Type-safe broker client for database operations.
 *
 * @example
 * ```typescript
 * const client = new BrokerClient("main");
 * const result = await client.execute("SELECT * FROM users");
 * console.log(result.rows);
 * ```
 *
 * @see {@link BrokerPool} for connection pool management
 */
export class BrokerClient {
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
  async execute(query: string, params?: unknown[]): Promise<QueryResult> {
    // Implementation...
  }
}
```

**Files:**
- `/home/cody/code/c/vertebra/src/ts/typedoc.json`
- `/home/cody/code/c/vertebra/src/ts/README.md`

#### 5.3 Unified Documentation Site

**Integration Strategy:**

```
docs/_build/
├── html/                 # Python docs (Sphinx)
│   ├── index.html
│   ├── api/
│   └── guides/
├── ts-api/              # TypeScript docs (TypeDoc)
│   └── index.html
└── rust-api/            # Rust docs (rustdoc)
    └── index.html
```

**Landing Page:**
```html
<!-- /home/cody/code/c/vertebra/docs/_templates/index.html -->
<!DOCTYPE html>
<html>
<head>
    <title>Vertebra Documentation</title>
    <link rel="stylesheet" href="_static/custom.css">
</head>
<body>
    <header>
        <h1>Vertebra Documentation</h1>
        <p>Python-first application backbone with shared-memory IPC</p>
    </header>

    <nav>
        <a href="guides/getting-started.html">Getting Started</a>
        <a href="api/index.html">Python API</a>
        <a href="ts-api/index.html">TypeScript API</a>
        <a href="rust-api/index.html">Rust API</a>
    </nav>

    <main>
        <section>
            <h2>Quick Start</h2>
            <pre><code>pip install vertebra
vertebra serve app:app</code></pre>
        </section>

        <section>
            <h2>Language Documentation</h2>
            <div class="cards">
                <div class="card">
                    <h3>Python</h3>
                    <p>Litestar integration, SQLSpec, SQLAlchemy</p>
                    <a href="api/index.html">View Docs →</a>
                </div>
                <div class="card">
                    <h3>TypeScript</h3>
                    <p>Broker client, MCP server, drivers</p>
                    <a href="ts-api/index.html">View Docs →</a>
                </div>
                <div class="card">
                    <h3>Rust</h3>
                    <p>Core IPC, ring buffers, FFI bindings</p>
                    <a href="rust-api/index.html">View Docs →</a>
                </div>
            </div>
        </section>
    </main>
</body>
</html>
```

**Quality Checks:**
```python
# /home/cody/code/c/vertebra/scripts/check_docs.py
"""Documentation quality checks."""
import re
from pathlib import Path
from typing import List

def check_broken_links(docs_dir: Path) -> List[str]:
    """Find broken internal links in documentation."""
    broken = []
    for rst_file in docs_dir.rglob("*.rst"):
        content = rst_file.read_text()
        # Find :doc: and :ref: links
        links = re.findall(r":(?:doc|ref):`([^`]+)`", content)
        for link in links:
            target = docs_dir / f"{link}.rst"
            if not target.exists():
                broken.append(f"{rst_file}: broken link to {link}")
    return broken

def check_missing_docstrings(src_dir: Path) -> List[str]:
    """Find public classes/functions without docstrings."""
    missing = []
    for py_file in src_dir.rglob("*.py"):
        # Parse AST and check docstrings
        # ... implementation
    return missing

if __name__ == "__main__":
    docs_dir = Path("docs")
    src_dir = Path("src/py/vertebra")

    broken_links = check_broken_links(docs_dir)
    if broken_links:
        print("Broken links found:")
        for link in broken_links:
            print(f"  {link}")
        exit(1)

    print("✓ All documentation links valid")
```

**CI Integration:**
```yaml
# .github/workflows/docs.yml
name: Documentation

on:
  push:
    branches: [main]
  pull_request:

jobs:
  build:
    runs-on: ubuntu-latest

    steps:
      - uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.12"

      - name: Install dependencies
        run: |
          pip install uv
          uv sync --group docs

      - name: Build Python docs
        run: make docs

      - name: Build TypeScript docs
        run: |
          cd src/ts
          bun install
          bun run docs

      - name: Check for broken links
        run: uv run python scripts/check_docs.py

      - name: Deploy to GitHub Pages
        if: github.ref == 'refs/heads/main'
        uses: peaceiris/actions-gh-pages@v3
        with:
          github_token: ${{ secrets.GITHUB_TOKEN }}
          publish_dir: ./docs/_build/html
```

**Acceptance Criteria:**
- [ ] 100% of public APIs documented
- [ ] Every API has usage example
- [ ] Zero broken links
- [ ] Documentation searchable
- [ ] Available in HTML format
- [ ] Mobile-friendly
- [ ] Version selector for different releases

**Estimated Effort:** 2 weeks (Weeks 14-15)

---

### 6. Developer Guides

**Location**: `/home/cody/code/c/vertebra/docs/guides/`

**Goal**: Enable developers to use Vertebra effectively

#### 6.1 Getting Started Guide

**File**: `/home/cody/code/c/vertebra/docs/guides/getting-started.md`

**Target**: New developer can go from zero to working app in <10 minutes

**Content Outline:**

```markdown
# Getting Started with Vertebra

## Installation (2 minutes)

### Prerequisites
- Python 3.12+
- Bun (for drivers and build tooling)

### Install Python Package
\`\`\`bash
pip install vertebra[litestar]
\`\`\`

### Install Bun
\`\`\`bash
curl -fsSL https://bun.sh/install | bash
\`\`\`

## Create Your First App (5 minutes)

### 1. Create Application
\`\`\`python
# app.py
from litestar import Litestar, get

@get("/")
async def hello() -> dict[str, str]:
    return {"message": "Hello from Vertebra!"}

app = Litestar(route_handlers=[hello])
\`\`\`

### 2. Run Development Server
\`\`\`bash
vertebra serve app:app --reload
\`\`\`

Visit: http://localhost:8000

### 3. Add Database Integration
\`\`\`python
# app.py (enhanced)
from litestar import Litestar, get
from vertebra.extensions.litestar.sqlspec import VertebraSQLSpecPlugin
from vertebra.extensions.sqlspec.postgres import VertebraPostgresAsyncConfig

config = VertebraPostgresAsyncConfig(
    pool="main",
    connection_string="postgresql://localhost/mydb"
)

@get("/users")
async def get_users(session: AsyncSession) -> list[dict]:
    result = await session.execute("SELECT * FROM users")
    return [dict(row) for row in result.rows]

app = Litestar(
    route_handlers=[get_users],
    plugins=[VertebraSQLSpecPlugin(configs=[config])]
)
\`\`\`

### 4. Add TypeScript Frontend
\`\`\`bash
# Initialize frontend
cd frontend
bun init -y
bun add vite react @types/react
\`\`\`

\`\`\`typescript
// frontend/src/main.tsx
import { api } from "./api/generated"; // Auto-generated types!

const users = await api.users.list(); // Type: User[]
console.log(users);
\`\`\`

## Next Steps

- [ ] Read [Type Bridge Guide](type-bridge.md)
- [ ] Explore [Example Applications](../examples/)
- [ ] Review [Performance Tuning](performance.md)
- [ ] Configure [Production Deployment](deployment.md)

## Troubleshooting

**Issue**: `ModuleNotFoundError: No module named 'vertebra'`

**Solution**: Ensure you're in activated virtual environment:
\`\`\`bash
python -m venv .venv
source .venv/bin/activate  # or .venv\Scripts\activate on Windows
pip install vertebra
\`\`\`

**Issue**: `Error: Bun not found`

**Solution**: Install Bun:
\`\`\`bash
curl -fsSL https://bun.sh/install | bash
\`\`\`
```

**Files:**
- `/home/cody/code/c/vertebra/docs/guides/getting-started.md`

#### 6.2 Type Bridge Guide

**File**: `/home/cody/code/c/vertebra/docs/guides/type-bridge.md`

**Content**: How type bridge works, defining types, using generated types

#### 6.3 Performance Tuning Guide

**File**: `/home/cody/code/c/vertebra/docs/guides/performance.md`

**Content**: Server configuration, pool sizing, caching, profiling

#### 6.4 Deployment Guide

**File**: `/home/cody/code/c/vertebra/docs/guides/deployment.md`

**Content Outline:**

```markdown
# Deploying Vertebra to Production

## Docker Deployment

### Multi-Stage Dockerfile
\`\`\`dockerfile
# Build stage
FROM python:3.12-slim AS builder
WORKDIR /app

# Install Bun
RUN curl -fsSL https://bun.sh/install | bash
ENV PATH="/root/.bun/bin:$PATH"

# Install Python dependencies
COPY pyproject.toml .
RUN pip install uv && uv sync --no-dev

# Build frontend
COPY frontend/ ./frontend/
RUN cd frontend && bun install && bun build

# Runtime stage
FROM python:3.12-slim
WORKDIR /app

# Install Bun runtime
RUN curl -fsSL https://bun.sh/install | bash
ENV PATH="/root/.bun/bin:$PATH"

# Copy app and dependencies
COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /app/frontend/dist /app/static
COPY src/ ./src/

ENV PATH="/app/.venv/bin:$PATH"

EXPOSE 8000
CMD ["vertebra", "serve", "app:app", "--host", "0.0.0.0", "--workers", "4"]
\`\`\`

### Docker Compose (Development)
\`\`\`yaml
version: "3.9"

services:
  app:
    build: .
    ports:
      - "8000:8000"
    environment:
      - DATABASE_URL=postgresql://postgres:password@db:5432/myapp
      - REDIS_URL=redis://redis:6379/0
    depends_on:
      - db
      - redis

  db:
    image: postgres:16-alpine
    environment:
      - POSTGRES_PASSWORD=password
      - POSTGRES_DB=myapp
    volumes:
      - postgres_data:/var/lib/postgresql/data

  redis:
    image: redis:7-alpine
    volumes:
      - redis_data:/data

volumes:
  postgres_data:
  redis_data:
\`\`\`

## Kubernetes Deployment

### Deployment
\`\`\`yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: vertebra-app
spec:
  replicas: 3
  selector:
    matchLabels:
      app: vertebra
  template:
    metadata:
      labels:
        app: vertebra
    spec:
      containers:
      - name: app
        image: myregistry/vertebra-app:latest
        ports:
        - containerPort: 8000
        env:
        - name: DATABASE_URL
          valueFrom:
            secretKeyRef:
              name: db-credentials
              key: url
        resources:
          requests:
            memory: "512Mi"
            cpu: "500m"
          limits:
            memory: "1Gi"
            cpu: "1000m"
        livenessProbe:
          httpGet:
            path: /health
            port: 8000
          initialDelaySeconds: 30
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /ready
            port: 8000
          initialDelaySeconds: 5
          periodSeconds: 5
\`\`\`

## Systemd Service

\`\`\`ini
[Unit]
Description=Vertebra Application
After=network.target

[Service]
Type=notify
User=www-data
Group=www-data
WorkingDirectory=/var/www/myapp
Environment="PATH=/var/www/myapp/.venv/bin:/usr/local/bin:/usr/bin"
ExecStart=/var/www/myapp/.venv/bin/vertebra serve app:app --workers 4
Restart=always

[Install]
WantedBy=multi-user.target
\`\`\`

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `VERTEBRA_HOST` | Server host | `0.0.0.0` |
| `VERTEBRA_PORT` | Server port | `8000` |
| `VERTEBRA_WORKERS` | Worker processes | `4` |
| `VERTEBRA_LOG_LEVEL` | Log level | `info` |
| `DATABASE_URL` | Database connection string | Required |

## Monitoring

### Health Checks
\`\`\`python
# app.py
from litestar import get

@get("/health")
async def health() -> dict[str, str]:
    return {"status": "healthy"}

@get("/ready")
async def ready() -> dict[str, bool]:
    # Check database connectivity
    db_ready = await check_database()
    return {"ready": db_ready}
\`\`\`

### Prometheus Metrics
\`\`\`python
from litestar import Litestar
from litestar.contrib.prometheus import PrometheusConfig, PrometheusController

app = Litestar(
    route_handlers=[...],
    middleware=[PrometheusConfig().middleware]
)
\`\`\`

Access metrics: http://localhost:8000/metrics

## Performance Tuning

### Worker Configuration
\`\`\`bash
# CPU-bound workload
vertebra serve app:app --workers $(nproc)

# I/O-bound workload
vertebra serve app:app --workers $(($(nproc) * 2))
\`\`\`

### Connection Pooling
\`\`\`python
# config.py
config = VertebraPostgresAsyncConfig(
    pool="main",
    min_pool_size=10,
    max_pool_size=50,
    pool_timeout=30.0
)
\`\`\`
```

**Files:**
- `/home/cody/code/c/vertebra/docs/guides/deployment.md`

**Acceptance Criteria:**
- [ ] Getting Started guide enables 10-minute setup
- [ ] All guides have working code examples
- [ ] Guides cover common use cases
- [ ] Troubleshooting sections address 90% of issues
- [ ] Deployment guide tested on AWS, GCP, Azure

**Estimated Effort:** 2 weeks (Weeks 16-17)

---

## Timeline and Phases

**Total Duration**: 18 weeks

### Phase 1: Test Infrastructure (Weeks 1-2)
**Deliverables:**
- pytest-databases integration
- CI/CD pipeline for multi-platform testing
- Coverage baseline established

**Milestone**: Test infrastructure ready

### Phase 2: Integration Tests (Weeks 3-5)
**Deliverables:**
- Python ↔ TypeScript tests (90%+ coverage)
- TypeScript ↔ Rust tests
- Python ↔ Rust tests
- E2E tests with containers
- WebSocket tests

**Milestone**: 90%+ integration coverage achieved

### Phase 3: Performance Benchmarks (Weeks 6-7)
**Deliverables:**
- Benchmark suite for JSON/DB/WebSocket/Static
- Comparative benchmarks vs Granian/Uvicorn/Hypercorn
- Statistical analysis tooling
- Published benchmark results

**Milestone**: Benchmarks prove +10% RPS superiority

### Phase 4: Example Applications (Weeks 8-11)
**Deliverables:**
- Todo API (SQLSpec + Litestar)
- Blog Platform (SQLAlchemy + AA)
- AI Tools Server (MCP)
- Realtime Chat (WebSocket)
- Data Pipeline (Pool Sharing)
- gRPC Microservice

**Milestone**: 6 production-quality examples complete

### Phase 5: Migration Guides (Weeks 12-13)
**Deliverables:**
- litestar-vite migration guide
- ASGI server migration guide
- 2+ validated migrations

**Milestone**: Migration guides validated

### Phase 6: API Documentation (Weeks 14-15)
**Deliverables:**
- Python API docs (Sphinx)
- TypeScript API docs (TypeDoc)
- Rust API docs (rustdoc)
- Unified documentation site

**Milestone**: 100% API coverage, zero broken links

### Phase 7: Developer Guides (Weeks 16-17)
**Deliverables:**
- Getting Started guide
- Type Bridge guide
- Performance Tuning guide
- Deployment guide

**Milestone**: Guides tested by new developers

### Phase 8: Integration and Polish (Week 18)
**Deliverables:**
- Final testing across all deliverables
- Documentation review and updates
- Release notes preparation

**Milestone**: Release readiness achieved

---

## Success Metrics

| Metric | Target | Measurement |
|--------|--------|-------------|
| Integration test coverage | ≥90% | Codecov |
| Test execution time | <5 min | CI logs |
| Performance vs Granian | +10% RPS | wrk2 benchmarks |
| Latency improvement | Lower p99 than Uvicorn | wrk2 percentiles |
| Example applications | 6 complete | CI passing |
| API documentation | 100% coverage | Doc coverage tools |
| Migration validations | 2+ projects | Real migrations |
| Documentation quality | 0 broken links | Automated checks |
| New user onboarding | <10 min setup | User testing |

---

## Risks and Mitigation

### High Risks

**Risk 1: Cross-Language Test Complexity**
- **Impact**: Could delay release or ship with insufficient coverage
- **Mitigation**: Start simple, use testcontainers for consistency, invest in infrastructure early
- **Contingency**: Focus on critical paths (80/20 rule)

**Risk 2: Benchmark Consistency**
- **Impact**: Could make false performance claims
- **Mitigation**: Dedicated hardware, multiple iterations, statistical analysis, document conditions
- **Contingency**: Increase sample size, wider confidence intervals

**Risk 3: Documentation Maintenance**
- **Impact**: Outdated docs frustrate users
- **Mitigation**: Auto-generate where possible, CI checks for examples, quarterly audits
- **Contingency**: Focus on auto-generated API docs

### Medium Risks

**Risk 4: Example Quality**
- **Impact**: Could lead to anti-patterns in user code
- **Mitigation**: User feedback, real project patterns, rigorous code review
- **Contingency**: Fewer but higher-quality examples (4 instead of 6)

**Risk 5: Migration Complexity**
- **Impact**: Slower adoption
- **Mitigation**: Test with varied projects, comprehensive troubleshooting, community support
- **Contingency**: Professional migration support services

---

## Dependencies

### Internal Dependencies
- ✅ PRD-0: Project setup complete
- ✅ PRD-1: Rust server complete
- ✅ PRD-2: Python integration complete
- ✅ PRD-3: TypeScript integration complete
- ✅ PRD-4: Bun-backed drivers complete
- ✅ PRD-5: Resource management complete

**This PRD cannot begin until all previous PRDs are functionally complete.**

### External Dependencies
- GitHub Actions (CI/CD)
- Codecov (coverage reporting)
- AWS EC2 (benchmark server - can use alternatives)
- Documentation hosting (GitHub Pages/Netlify/Vercel)

**No blocking external dependencies** - all tools have open-source alternatives

---

## Acceptance Criteria for 1.0 Release

### Integration Testing
- [ ] All tests pass on Linux/macOS/Windows
- [ ] 90%+ code coverage for integration points
- [ ] Tests complete in <5 minutes
- [ ] Zero memory leaks detected
- [ ] CI runs on every commit

### Performance Benchmarks
- [ ] +10% RPS vs Granian on JSON endpoint
- [ ] Lower p99 latency than Uvicorn
- [ ] 10,000+ concurrent WebSocket connections
- [ ] Startup time <1 second
- [ ] Reproducible results (<5% variance)

### Example Applications
- [ ] 6 examples complete and working
- [ ] Each has comprehensive README
- [ ] All examples tested in CI
- [ ] Docker Compose setup for each

### Migration Guides
- [ ] 2+ test migrations completed
- [ ] Guides peer-reviewed
- [ ] Troubleshooting covers common issues
- [ ] Before/after code examples

### API Documentation
- [ ] 100% public API documented
- [ ] Zero broken links
- [ ] Search functionality working
- [ ] Mobile-responsive
- [ ] Version selector implemented

### Developer Guides
- [ ] Getting Started guide (10-minute setup)
- [ ] Type Bridge guide complete
- [ ] Performance Tuning guide complete
- [ ] Deployment guide complete
- [ ] All code examples verified

### Overall Release Criteria
- [ ] All acceptance criteria above met
- [ ] Changelog comprehensive
- [ ] Release notes prepared
- [ ] Community support channels established

---

## Task Breakdown

### Phase 1: Test Infrastructure (Weeks 1-2)
1. Configure pytest-databases for container management
2. Set up GitHub Actions matrix builds
3. Configure coverage reporting to Codecov
4. Create integration test fixtures
5. Document test patterns

### Phase 2: Integration Tests (Weeks 3-5)
6. Implement Python ↔ TypeScript tests
7. Implement TypeScript ↔ Rust tests
8. Implement Python ↔ Rust tests
9. Implement E2E database tests
10. Implement WebSocket tests
11. Implement resource pool tests
12. Configure CI integration test runs

### Phase 3: Performance Benchmarks (Weeks 6-7)
13. Create benchmark applications
14. Write benchmark scripts (JSON/DB/WS/Static)
15. Set up benchmark infrastructure
16. Run comparative benchmarks
17. Implement statistical analysis
18. Generate benchmark reports

### Phase 4: Example Applications (Weeks 8-11)
19. Create Todo API example
20. Create Blog Platform example
21. Create AI Tools Server example
22. Create Realtime Chat example
23. Create Data Pipeline example
24. Create gRPC Microservice example
25. Write READMEs for all examples
26. Test examples in CI

### Phase 5: Migration Guides (Weeks 12-13)
27. Write litestar-vite migration guide
28. Write ASGI server migration guide
29. Perform 2+ test migrations
30. Update troubleshooting sections

### Phase 6: API Documentation (Weeks 14-15)
31. Configure Sphinx for Python docs
32. Write docstrings for all public APIs
33. Configure TypeDoc for TypeScript docs
34. Build Rust docs with rustdoc
35. Create unified documentation site
36. Implement search functionality
37. Set up documentation deployment

### Phase 7: Developer Guides (Weeks 16-17)
38. Write Getting Started guide
39. Write Type Bridge guide
40. Write Performance Tuning guide
41. Write Deployment guide
42. Test guides with new developers

### Phase 8: Integration and Polish (Week 18)
43. Final testing across all deliverables
44. Documentation review
45. Fix broken links
46. Prepare release notes
47. Set up community support channels

---

## File Paths Reference

### Python Files to Create
- `/home/cody/code/c/vertebra/src/py/tests/integration/conftest.py`
- `/home/cody/code/c/vertebra/src/py/tests/integration/test_api_contract.py`
- `/home/cody/code/c/vertebra/src/py/tests/integration/test_type_bridge.py`
- `/home/cody/code/c/vertebra/src/py/tests/integration/test_pyo3_ffi.py`
- `/home/cody/code/c/vertebra/src/py/tests/integration/test_e2e_database.py`
- `/home/cody/code/c/vertebra/src/py/tests/integration/test_websocket.py`
- `/home/cody/code/c/vertebra/src/py/tests/integration/test_pool_lifecycle.py`

### TypeScript Files to Create
- `/home/cody/code/c/vertebra/src/ts/tests/integration/type-bridge.test.ts`
- `/home/cody/code/c/vertebra/src/ts/tests/integration/channel-ffi.test.ts`
- `/home/cody/code/c/vertebra/src/ts/tests/integration/pool-sharing.test.ts`
- `/home/cody/code/c/vertebra/src/ts/typedoc.json`

### Benchmark Files to Create
- `/home/cody/code/c/vertebra/benchmarks/README.md`
- `/home/cody/code/c/vertebra/benchmarks/apps/simple_json.py`
- `/home/cody/code/c/vertebra/benchmarks/apps/database.py`
- `/home/cody/code/c/vertebra/benchmarks/apps/websocket.py`
- `/home/cody/code/c/vertebra/benchmarks/scripts/bench_json.sh`
- `/home/cody/code/c/vertebra/benchmarks/scripts/analyze.py`

### Example Application Directories to Create
- `/home/cody/code/c/vertebra/examples/todo-api/`
- `/home/cody/code/c/vertebra/examples/blog-platform/`
- `/home/cody/code/c/vertebra/examples/ai-tools/`
- `/home/cody/code/c/vertebra/examples/realtime-chat/`
- `/home/cody/code/c/vertebra/examples/data-pipeline/`
- `/home/cody/code/c/vertebra/examples/grpc-service/`

### Documentation Files to Create
- `/home/cody/code/c/vertebra/docs/conf.py`
- `/home/cody/code/c/vertebra/docs/index.rst`
- `/home/cody/code/c/vertebra/docs/migration/from-litestar-vite.md`
- `/home/cody/code/c/vertebra/docs/migration/from-asgi-servers.md`
- `/home/cody/code/c/vertebra/docs/guides/getting-started.md`
- `/home/cody/code/c/vertebra/docs/guides/type-bridge.md`
- `/home/cody/code/c/vertebra/docs/guides/performance.md`
- `/home/cody/code/c/vertebra/docs/guides/deployment.md`

### CI/CD Files to Create
- `/home/cody/code/c/vertebra/.github/workflows/integration-tests.yml`
- `/home/cody/code/c/vertebra/.github/workflows/docs.yml`
- `/home/cody/code/c/vertebra/.github/workflows/benchmarks.yml`

---

## Related Specifications

This PRD consolidates and supersedes:
- `specs/archive/consolidated/PRD-6-integration.md` (integration tests, benchmarks)
- Documentation sections from `specs/archive/consolidated/prd-remaining-work.md`

---

**Document Version:** 1.0
**Last Updated:** 2026-01-19
**Word Count:** 9,847 words
**Author:** Claude (Sonnet 4.5)
**Status:** Ready for Implementation
