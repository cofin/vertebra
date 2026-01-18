# PRD-5 Session Recovery Guide

**Document Version**: 1.0
**Last Updated**: 2026-01-18
**Purpose**: Enable session resumption and context recovery for PRD-5 implementation

---

## Quick Start

If you're resuming work on PRD-5 after a break, start here:

### What is PRD-5?

Cooperative Resource Management for Vertebra - a system that enables shared connection pools across Python workers, reducing database connections by 70-90%.

### Where are we?

Check the project status:

```bash
# View task completion
cat /home/cody/code/c/vertebra/docs/prd/PRD-5-resource-management/tasks.md

# Check current implementation
ls -la /home/cody/code/c/vertebra/src/rs/vertebra-broker/  # Rust broker
ls -la /home/cody/code/c/vertebra/src/py/vertebra/resources/  # Python client

# Review recent commits
cd /home/cody/code/c/vertebra
git log --oneline -10
```

### What's next?

1. **Read the PRD**: `/home/cody/code/c/vertebra/docs/prd/PRD-5-resource-management.md`
2. **Check current phase**: Review tasks.md to see what's completed
3. **Pick a task**: Choose from the next uncompleted milestone
4. **Implement**: Follow the technical specs in the PRD
5. **Test**: Run tests from tasks.md checklist
6. **Commit**: Use clear commit messages referencing task IDs

---

## Document Locations

All PRD-5 documentation is organized under:

```
/home/cody/code/c/vertebra/docs/prd/PRD-5-resource-management/
├── prd.md              # Main PRD (6,800+ words)
├── tasks.md            # Task breakdown (108 tasks)
├── recovery.md         # This file
└── research/
    └── plan.md         # Research findings (4,200+ words)
```

**Main PRD**: `/home/cody/code/c/vertebra/docs/prd/PRD-5-resource-management.md`
- Complete technical specification
- Architecture diagrams
- API designs
- Implementation roadmap

**Tasks**: `/home/cody/code/c/vertebra/docs/prd/PRD-5-resource-management/tasks.md`
- 108 actionable tasks across 6 phases
- Dependencies and effort estimates
- Assignee guidelines (RD = Rust Dev, PD = Python Dev)

**Research**: `/home/cody/code/c/vertebra/docs/prd/PRD-5-resource-management/research/plan.md`
- Technology evaluation (deadpool, shmipc, WFQ)
- Performance benchmarks
- Security considerations
- Lessons from production systems

---

## Implementation Status Tracking

### How to mark tasks complete

Edit `/home/cody/code/c/vertebra/docs/prd/PRD-5-resource-management/tasks.md`:

```markdown
**P1-M1-001**: Create vertebra-broker crate structure
- [x] Initialize cargo project: `cargo new --lib vertebra-broker`  # ← Change [ ] to [x]
- [x] Set up module structure (lib.rs, pool_manager.rs, etc.)
- [x] Configure Cargo.toml with dependencies
- **Effort**: S | **Assignee**: RD | **Dependencies**: None
```

### Phase Checklist

Use this checklist to track high-level progress:

#### Phase 1: Foundation (Weeks 1-3)
- [ ] Milestone 1.1: Rust Broker Skeleton (6 tasks)
- [ ] Milestone 1.2: Single Pool Implementation (7 tasks)

#### Phase 2: Python Client (Weeks 4-5)
- [ ] Milestone 2.1: Python IPC Client (7 tasks)
- [ ] Milestone 2.2: Multi-Pool Support (5 tasks)

#### Phase 3: Fair Scheduling (Weeks 6-7)
- [ ] Milestone 3.1: Basic Scheduler (5 tasks)
- [ ] Milestone 3.2: Weighted Fair Queuing (6 tasks)

#### Phase 4: Production Readiness (Weeks 8-10)
- [ ] Milestone 4.1: Observability (5 tasks)
- [ ] Milestone 4.2: Security & Resilience (7 tasks)
- [ ] Milestone 4.3: Deployment (5 tasks)

#### Phase 5: Integration & Testing (Weeks 11-12)
- [ ] Milestone 5.1: Performance Testing (5 tasks)
- [ ] Milestone 5.2: PRD-4 Integration (3 tasks)
- [ ] Milestone 5.3: Production Pilot (4 tasks)

#### Phase 6: Production Rollout (Weeks 13-14)
- [ ] Milestone 6.1: Gradual Rollout (5 tasks)
- [ ] Milestone 6.2: Documentation & Training (5 tasks)

---

## Key Technical Decisions

Refer back to these when implementing:

### Architecture Decisions

1. **IPC Mechanism**: shmipc for MVP
   - Rationale: Balance of performance (< 1ms) and simplicity
   - Migration path: iceoryx2 if needed for > 5000 workers
   - Location in PRD: Section 2.2, Section 3 (Research doc)

2. **Connection Pooling**: deadpool for all DB types
   - Rationale: Multi-protocol support, simple API, mature
   - Location in PRD: Section 2.1.1, Section 2 (Research doc)

3. **Scheduling Algorithm**: WFQ + DRR + Starvation Prevention
   - Rationale: Fair, prevents starvation, proven in production
   - Location in PRD: Section 2.3, Section 4 (Research doc)

4. **Serialization**: bincode
   - Rationale: Zero-copy, efficient, excellent Rust support
   - Location in PRD: Section 2.2.2, Section 6 (Research doc)

### API Design Principles

**Python API** (user-facing):
```python
from vertebra.resources import get_pool

async with get_pool("postgres").acquire() as conn:
    result = await conn.execute("SELECT * FROM users")
```

**Rust Broker** (internal):
```rust
let handle = pool_manager.acquire(pool_name, worker_id).await?;
// ... use handle ...
pool_manager.release(handle).await?;
```

### Configuration Format

**Broker Config** (`/etc/vertebra/broker.toml`):
```toml
[broker]
ipc_socket_path = "/var/run/vertebra/broker.sock"
ipc_named_pipe = "\\\\.\\pipe\\vertebra-broker" # Windows

[[pools.postgres]]
name = "main"
dsn = "${POSTGRES_DSN}"
max_connections = 20
```

**Python Config**:
```python
from vertebra.resources import configure_client

configure_client(
    broker_socket="/var/run/vertebra/broker.sock",
    broker_named_pipe="\\\\.\\pipe\\vertebra-broker",  # Windows
    default_timeout=5.0
)
```

---

## Development Workflow

### Setup Development Environment

```bash
# Navigate to project
cd /home/cody/code/c/vertebra

# Check Python version
python --version  # Should be 3.12+

# Check Rust toolchain
rustc --version   # Should be 1.75+

# Install dependencies (if needed)
make install
```

### Running Tests

```bash
# Rust tests
cd /home/cody/code/c/vertebra/src/rs/vertebra-broker
cargo test

# Python tests
cd /home/cody/code/c/vertebra
make test

# Integration tests
cd /home/cody/code/c/vertebra
make test-all
```

### Development Commands

```bash
# Format code
make fmt

# Lint code
make lint

# Type check
make type-check

# Build broker
cd src/rs/vertebra-broker
cargo build --release

# Run broker locally
./target/release/vertebra-broker --config config/dev.toml
```

---

## Common Questions

### Q: Where do I start if I'm new to the codebase?

**A**: Start with Phase 1, Milestone 1.1 (Broker Skeleton):

1. Read PRD Section 2.1.1 (Rust Resource Broker)
2. Create `src/rs/vertebra-broker/` directory
3. Follow tasks P1-M1-001 through P1-M1-006
4. Refer to research doc Section 2 for technology details

### Q: How do I test IPC communication locally?

**A**:

1. Build broker: `cargo build --release`
2. Run broker: `./target/release/vertebra-broker --config test.toml`
3. In Python: Use `IPCClient` to connect to `/var/run/vertebra/broker.sock`
4. Send `AcquireRequest`, verify `AcquireResponse`

### Q: What are the performance targets?

**A**: From PRD Section 9.1:

- P50 latency: < 2ms
- P90 latency: < 5ms
- P99 latency: < 10ms
- Connection reduction: 70-90%
- Error rate: < 0.1%

### Q: How do I debug connection issues?

**A**:

1. Check broker logs: `journalctl -u vertebra-broker -f`
2. Verify IPC endpoint exists: `ls -la /var/run/vertebra/broker.sock` (Unix) or check named pipe on Windows
3. Check metrics: `curl http://localhost:9090/metrics | grep pool_`
4. Enable trace logging: Set `VERTEBRA_LOG_LEVEL=trace`

### Q: What if a dependency from PRD-4 isn't ready?

**A**: PRD-5 is designed to work standalone. PRD-4 (Bun drivers) is optional:

- Phase 1-4: No PRD-4 dependency
- Phase 5, Milestone 5.2: PRD-4 integration (can skip if not ready)
- See PRD Section 11.2 for details

---

## Critical Code Snippets

### IPC Message Definition (Rust)

```rust
// src/rs/vertebra-broker/src/protocol.rs

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum IpcMessage {
    AcquireRequest {
        pool_name: String,
        worker_id: u64,
        timeout: f64,
    },
    AcquireResponse {
        connection_handle: Option<u64>,
        error: Option<String>,
    },
    ReleaseRequest {
        connection_handle: u64,
    },
    ReleaseResponse {
        success: bool,
    },
}
```

### Connection Wrapper (Python)

```python
# src/py/vertebra/resources/connection.py

class Connection:
    """Wrapper around connection handle with auto-release."""

    def __init__(self, handle: int, ipc_client: IPCClient, pool_name: str):
        self._handle = handle
        self._ipc_client = ipc_client
        self._pool_name = pool_name
        self._released = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if not self._released:
            await self.release()

    async def release(self):
        await self._ipc_client.release(self._handle)
        self._released = True

    async def execute(self, query, *args):
        return await self._ipc_client.execute(self._handle, query, args)
```

### Scheduler Interface (Rust)

```rust
// src/rs/vertebra-broker/src/scheduler.rs

#[async_trait]
pub trait Scheduler: Send + Sync {
    async fn enqueue(&mut self, worker_id: WorkerId, request: AcquireRequest);
    async fn dequeue(&mut self) -> Option<(WorkerId, AcquireRequest)>;
    fn get_stats(&self) -> SchedulerStats;
}

pub struct WeightedFairScheduler {
    queues: HashMap<WorkerId, WorkerQueue>,
    quantum: u32,
}

struct WorkerQueue {
    worker_id: WorkerId,
    pending: VecDeque<AcquireRequest>,
    weight: u32,
    deficit: u32,
    consecutive_misses: u32,
}
```

---

## Troubleshooting

### Broker won't start

**Symptoms**: Broker exits immediately, no error message

**Solutions**:
1. Check config file: `vertebra-broker --config /path/to/config.toml`
2. Verify socket path writable: `ls -la /var/run/vertebra/`
3. Check logs: `journalctl -u vertebra-broker -n 50`
4. Run in foreground: `RUST_LOG=debug vertebra-broker ...`

### Python client can't connect

**Symptoms**: `ConnectionError: No such file or directory`

**Solutions**:
1. Verify broker running: `systemctl status vertebra-broker`
2. Check IPC endpoint exists: `ls -la /var/run/vertebra/broker.sock` (Unix) or check named pipe on Windows
3. Verify permissions: Socket should be mode 0600 or 0660
4. Check client config: `broker_socket` path matches broker config

### Pool exhaustion

**Symptoms**: `PoolTimeoutError` or `PoolExhaustedError`

**Solutions**:
1. Check metrics: `curl localhost:9090/metrics | grep pool_utilization`
2. Increase `max_connections` in config
3. Investigate slow queries (holding connections too long)
4. Check for connection leaks: `grep "leak" /var/log/vertebra/broker.log`

### High latency

**Symptoms**: P99 latency > 10ms

**Solutions**:
1. Check scheduler queue length: `pool_queue_length` metric
2. Verify IPC performance: Run benchmark from P5-M1-002
3. Profile broker: Use `perf` or `flamegraph`
4. Consider migration to iceoryx2 if shmipc insufficient

---

## Metrics Reference

### Key Metrics to Monitor

**Pool Health**:
- `pool_connections_total{state="idle|leased"}`: Current connection state
- `pool_utilization_ratio`: % of connections in use (target: < 0.9)

**Performance**:
- `pool_acquisition_duration_seconds{quantile="0.99"}`: P99 latency (target: < 0.01)
- `pool_queue_length`: Pending requests (target: < 10)

**Errors**:
- `pool_connection_errors_total`: Total errors (target: < 0.1% of requests)
- `pool_health_checks_total{result="failure"}`: Failed health checks

**Fairness**:
- `scheduler_worker_requests_total`: Requests per worker (should be balanced)
- `scheduler_worker_starvation_events`: Starvation count (target: 0)

### Accessing Metrics

```bash
# All metrics
curl http://localhost:9090/metrics

# Filter for specific pool
curl http://localhost:9090/metrics | grep 'pool="postgres"'

# Health check
curl http://localhost:9090/health
```

---

## External Resources

### Research Sources

All sources are documented in `/home/cody/code/c/vertebra/docs/prd/PRD-5-resource-management/research/plan.md`, Section 16.

**Key References**:

1. **Connection Pooling**: [deadpool documentation](https://mozilla-services.github.io/merino/rustdoc/deadpool/index.html)
2. **Zero-Copy IPC**: [shmipc on crates.io](https://crates.io/crates/shmipc)
3. **Fair Scheduling**: [Wikipedia - Fair Queuing](https://en.wikipedia.org/wiki/Fair_queuing)
4. **Python Async**: [Aiomultiprocess Guide](https://towardsdatascience.com/supercharge-your-python-asyncio-with-aiomultiprocess-a-comprehensive-guide-571ee0e2f416/)

### Code Examples

**Rust Pool Example**: PRD Section 3.1 (PostgreSQL Pools)
**Python API Example**: PRD Section 2.1.2 (Python Client Library)
**Scheduler Algorithm**: PRD Section 2.3.2 (Anti-Starvation Mechanism)

---

## Next Session Checklist

Before ending a session, update this section:

**Session Date**: _____________
**Completed Tasks**: _____________
**In Progress**: _____________
**Blockers**: _____________
**Next Steps**: _____________

### For Next Session

1. Review completed tasks in tasks.md
2. Check git status and recent commits
3. Pick next task from current milestone
4. Read relevant PRD section
5. Implement, test, commit

---

## Contact & Support

**Project Repository**: `/home/cody/code/c/vertebra`
**Documentation**: `/home/cody/code/c/vertebra/docs/prd/PRD-5-resource-management/`

**For Questions**:
1. Check this recovery guide
2. Review PRD main document
3. Consult research document for technical details
4. Review tasks.md for implementation specifics

---

**Document Version**: 1.0
**Last Updated**: 2026-01-17

**Quick Command Reference**:
```bash
# View PRD
cat /home/cody/code/c/vertebra/docs/prd/PRD-5-resource-management.md

# View tasks
cat /home/cody/code/c/vertebra/docs/prd/PRD-5-resource-management/tasks.md

# View research
cat /home/cody/code/c/vertebra/docs/prd/PRD-5-resource-management/research/plan.md

# View this guide
cat /home/cody/code/c/vertebra/docs/prd/PRD-5-resource-management/recovery.md
```
