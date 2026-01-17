# PRD-5: Cooperative Resource Management

**Status**: Draft
**Version**: 1.0
**Last Updated**: 2026-01-17
**Authors**: Vertebra Team
**Dependencies**: PRD-4 (Bun-Backed Drivers)

> **Cross-PRD Decisions**: See [PRD-0 Cross-PRD Architectural Decisions](./PRD-0-foundation.md#cross-prd-architectural-decisions) for serialization strategy (bincode for internal Rust IPC), Python version (3.12+), and IPC transport decisions.

---

## Executive Summary

Vertebra's Cooperative Resource Management system enables shared connection pools across Python workers, dramatically reducing resource usage by 70-90%. Instead of each worker maintaining isolated connection pools, a centralized Rust-based broker manages shared pools accessible via zero-copy IPC.

**Problem**:
- Traditional: 10 workers × 10 DB connections = 100 total connections
- Vertebra: 1 broker × 20 shared connections = 80% reduction

**Architecture Overview**:

```
┌─────────────────────────────────────┐
│     Vertebra Resource Broker        │
│   (Rust process, manages pools)     │
├─────────────────────────────────────┤
│  PostgreSQL Pool (20 conns)         │
│  MySQL Pool (10 conns)              │
│  Redis Pool (5 conns)               │
│  HTTP Client Pool                   │
└─────────────────────────────────────┘
         │ Zero-copy IPC
    ┌────┴────┬────┴────┬────┴────┐
    │Worker 1 │Worker 2 │Worker N │
    └─────────┴─────────┴─────────┘
```

**Key Benefits**:
- 80% reduction in database connections
- Sub-10ms connection acquisition latency
- Fair resource distribution across workers
- Centralized monitoring and metrics
- Reduced memory footprint

---

## 1. Problem Statement

### 1.1 Current State

Python multi-worker deployments suffer from resource multiplication:

1. **Connection Explosion**: Each worker maintains its own connection pool
   - 10 workers × 10 PostgreSQL connections = 100 total connections
   - 10 workers × 5 Redis connections = 50 total connections
   - Database connection limits reached quickly

2. **Resource Waste**: Idle workers hold onto connections
   - Low-traffic workers maintain full pools
   - Peak capacity planning leads to over-provisioning

3. **Memory Overhead**: Each pool duplicates connection state
   - Connection buffers, prepared statements, SSL contexts
   - Multiplied across all workers

4. **Lack of Fairness**: No coordination between workers
   - Some workers starve while others are idle
   - No global resource allocation strategy

### 1.2 Goals

**Primary Goals**:
1. Reduce total connection count by 70-90%
2. Maintain or improve connection acquisition latency (< 10ms p99)
3. Fair resource distribution across workers
4. Transparent migration path from existing pools

**Secondary Goals**:
1. Centralized observability and metrics
2. Unified configuration management
3. Support for multiple resource types (DB, Redis, HTTP)
4. Integration with PRD-4 Bun-backed drivers

### 1.3 Non-Goals

- Distributed broker architecture (single broker per node)
- Cross-datacenter connection pooling
- Custom connection protocols (use existing drivers)
- Query optimization or caching

### 1.4 Assumptions and Constraints

- **Cross-platform target**: Linux/macOS/Windows parity for MVP if feasible; Linux-first implementation.
- **IPC transport**: UDS on Unix; named pipes (or TCP fallback) on Windows.
- **Central broker** pattern is required for pool ownership and fairness.
- **OpenTelemetry + Prometheus** are the default observability stack unless overridden.

---

## 2. Solution Architecture

### 2.1 System Components

#### 2.1.1 Rust Resource Broker

**Purpose**: Centralized pool manager process

**Key Responsibilities**:
- Manage multiple connection pools (PostgreSQL, MySQL, Redis, HTTP)
- Handle IPC requests from Python workers
- Implement fair scheduling algorithm
- Monitor connection health
- Collect and expose metrics

**Technology Stack**:
- **Runtime**: tokio for async execution
- **Pooling**: deadpool for PostgreSQL/Redis/MySQL
- **IPC**: shmipc for zero-copy shared memory IPC on Linux (alternatives required for macOS/Windows)
- **Serialization**: bincode for efficient message passing
- **Metrics**: prometheus for instrumentation
- **Logging**: tracing for structured logging

**Module Structure**:
```
src/rs/vertebra-broker/
├── src/
│   ├── lib.rs              # Broker initialization
│   ├── pool_manager.rs     # Pool lifecycle management
│   ├── scheduler.rs        # Weighted Fair Queue scheduler
│   ├── ipc_server.rs       # IPC protocol handler
│   ├── metrics.rs          # Prometheus metrics
│   ├── health.rs           # Connection health checks
│   ├── pools/
│   │   ├── mod.rs
│   │   ├── postgres.rs     # PostgreSQL pool
│   │   ├── mysql.rs        # MySQL pool
│   │   ├── redis.rs        # Redis pool
│   │   └── http.rs         # HTTP client pool
│   └── config.rs           # Configuration
└── Cargo.toml
```

#### 2.1.2 Python Client Library

**Purpose**: Transparent pool access for Python applications

**Python API Design**:

```python
from vertebra.resources import get_pool, PoolConfig, ResourceType

# Simple usage
async with get_pool("postgres").acquire() as conn:
    result = await conn.execute("SELECT * FROM users")

# Configuration
config = PoolConfig(
    name="postgres",
    resource_type=ResourceType.POSTGRESQL,
    max_connections=20,
    min_connections=5,
    connection_timeout=5.0,
    dsn="postgresql://user:pass@localhost/db"
)

await register_pool(config)

# Multiple pools
async with get_pool("redis").acquire() as redis:
    await redis.set("key", "value")

async with get_pool("mysql").acquire() as mysql:
    await mysql.execute("INSERT INTO logs (...) VALUES (...)")
```

**Package Structure**:
```
src/py/vertebra/resources/
├── __init__.py              # Public API
├── pool.py                  # Pool interface
├── connection.py            # Connection wrapper
├── config.py                # Configuration
├── ipc_client.py            # IPC communication
├── exceptions.py            # Custom exceptions
└── _internal/
    ├── __init__.py
    ├── protocol.py          # IPC protocol
    └── serialization.py     # bincode integration
```

### 2.2 IPC Protocol Design

#### 2.2.1 Message Types

```rust
// Connection acquisition
struct AcquireRequest {
    pool_name: String,
    worker_id: u64,
    timeout: f64,
}

struct AcquireResponse {
    connection_handle: Option<u64>,
    error: Option<String>,
}

// Connection release
struct ReleaseRequest {
    connection_handle: u64,
}

struct ReleaseResponse {
    success: bool,
}

// Health check
struct HealthCheckRequest {
    pool_name: String,
}

struct HealthCheckResponse {
    healthy: bool,
    active_connections: usize,
    idle_connections: usize,
}

// Metrics
struct MetricsRequest {}

struct MetricsResponse {
    metrics: HashMap<String, f64>,
}
```

#### 2.2.2 Communication Flow

```
Python Worker                Rust Broker
     |                            |
     |-- AcquireRequest --------->|
     |                            |- Check availability
     |                            |- Schedule request
     |                            |- Acquire from pool
     |<-- AcquireResponse --------|
     |                            |
     |  (use connection)          |
     |                            |
     |-- ReleaseRequest --------->|
     |                            |- Return to pool
     |<-- ReleaseResponse --------|
```

#### 2.2.3 Transport Mechanism

**Primary (Linux)**: shmipc (ByteDance's shared memory IPC)
- Zero-copy data transfer
- Shared memory based (POSIX on Unix, named shared memory on Windows)
- IPC transport for synchronization (UDS on Unix, named pipe on Windows)
- High performance (reported sub-microsecond latency; validate locally)

**Primary (macOS/Windows)**: OS-native shared memory + IPC transport
- macOS: POSIX shared memory + UDS
- Windows: Named shared memory + named pipes

**Fallback**: IPC transport without shared memory
- UDS on Unix, TCP on Windows if named pipes unavailable
- Lower performance but universally supported

### 2.3 Fair Scheduling Algorithm

#### 2.3.1 Weighted Fair Queuing (WFQ)

**Algorithm**: Deficit Round Robin with priority escalation

**Data Structures**:
```rust
struct WorkerQueue {
    worker_id: WorkerId,
    pending_requests: VecDeque<AcquireRequest>,
    weight: u32,           // Configurable per worker
    deficit: u32,          // DRR deficit counter
    last_served: Instant,
    consecutive_misses: u32,
}

struct PoolScheduler {
    queues: HashMap<WorkerId, WorkerQueue>,
    quantum: u32,          // Time quantum (ms)
    max_wait_time: Duration,
    starvation_threshold: u32,
}
```

**Scheduling Logic**:

1. **Round-Robin Base**: Iterate through worker queues
2. **Deficit Accumulation**: Each quantum, add `quantum` to deficit
3. **Service Condition**: Serve requests if `deficit >= request_cost`
4. **Starvation Prevention**: Escalate priority after N consecutive misses
5. **Fairness Guarantee**: Equal service over time windows

**Example**:
```
Worker A: weight=2 (high priority service)
Worker B: weight=1 (normal priority)
Worker C: weight=1 (normal priority)

Time quantum = 100ms
Over 300ms:
- Worker A gets ~200ms (2/4 share)
- Worker B gets ~100ms (1/4 share)
- Worker C gets ~100ms (1/4 share)
```

#### 2.3.2 Anti-Starvation Mechanism

```rust
impl PoolScheduler {
    async fn schedule(&mut self) -> Result<WorkerQueue> {
        for (worker_id, queue) in &mut self.queues {
            // Priority escalation for starving workers
            if queue.consecutive_misses > self.starvation_threshold {
                queue.weight *= 2;  // Double priority
                warn!("Worker {} starving, escalating priority", worker_id);
            }

            // Reset miss counter on successful service
            if !queue.pending_requests.is_empty() {
                queue.consecutive_misses = 0;
                queue.weight = original_weight;  // Reset priority
            }
        }

        // Standard DRR scheduling
        self.drr_schedule().await
    }
}
```

### 2.4 Connection Lifecycle Management

#### 2.4.1 Connection States

```
┌────────┐
│  Idle  │◄──────────┐
└────┬───┘           │
     │               │
     │ acquire()     │ release()
     │               │
     ▼               │
┌────────┐      ┌────┴─────┐
│ Leased │─────►│Recycling │
└────────┘      └──────────┘
     │
     │ health_check_failed()
     │
     ▼
┌──────────┐
│Terminated│
└──────────┘
```

**State Descriptions**:
- **Idle**: Available in pool, periodically health-checked
- **Leased**: Checked out to worker, in active use
- **Health Check**: Being validated before lease
- **Recycling**: Being returned to pool, validation in progress
- **Terminated**: Connection closed, removed from pool

#### 2.4.2 Health Check Strategy

**Periodic Health Checks** (for idle connections):
```rust
async fn periodic_health_check(&self) {
    let mut interval = tokio::time::interval(Duration::from_secs(30));

    loop {
        interval.tick().await;

        for conn in self.idle_connections() {
            match self.health_check(conn).await {
                Ok(true) => continue,
                Ok(false) | Err(_) => {
                    warn!("Connection {} failed health check", conn.id());
                    self.remove_connection(conn).await;
                    self.create_new_connection().await?;
                }
            }
        }
    }
}
```

**Pre-Lease Validation**:
```rust
async fn acquire(&self, worker_id: WorkerId) -> Result<Connection> {
    let conn = self.pool.get().await?;

    // Quick validation before lease
    if !self.quick_health_check(&conn).await? {
        self.pool.put(conn);  // Return bad connection
        return self.acquire(worker_id).await;  // Retry
    }

    Ok(conn)
}
```

**Post-Return Validation**:
```rust
async fn release(&self, conn: Connection) -> Result<()> {
    // Validate connection is still good
    match self.health_check(&conn).await {
        Ok(true) => self.pool.put(conn),
        Ok(false) | Err(_) => {
            warn!("Connection {} invalid on return", conn.id());
            drop(conn);  // Close bad connection
            self.create_new_connection().await?;
        }
    }

    Ok(())
}
```

**Windows Note**: use named pipe client impersonation / token checks for equivalent worker identity verification.

---

## 3. Resource Types

### 3.1 PostgreSQL Pools

**Implementation**: deadpool-postgres

**Configuration**:
```toml
[[pools.postgres]]
name = "main_postgres"
type = "postgresql"
dsn = "${POSTGRES_DSN}"
max_connections = 20
min_connections = 5
connection_timeout = 5.0
idle_timeout = 300.0
max_lifetime = 1800.0
ssl_mode = "require"
```

**Health Check**:
```rust
async fn health_check_postgres(conn: &Connection) -> Result<bool> {
    match conn.execute("SELECT 1", &[]).await {
        Ok(_) => Ok(true),
        Err(e) => {
            warn!("PostgreSQL health check failed: {}", e);
            Ok(false)
        }
    }
}
```

### 3.2 MySQL Pools

**Implementation**: sqlx-mysql + custom deadpool wrapper

**Configuration**:
```toml
[[pools.mysql]]
name = "legacy_mysql"
type = "mysql"
host = "${MYSQL_HOST}"
port = 3306
database = "legacy"
user = "${MYSQL_USER}"
password = "${MYSQL_PASSWORD}"
max_connections = 15
connection_timeout = 5.0
```

**Health Check**:
```rust
async fn health_check_mysql(conn: &Connection) -> Result<bool> {
    match conn.ping().await {
        Ok(_) => Ok(true),
        Err(e) => {
            warn!("MySQL health check failed: {}", e);
            Ok(false)
        }
    }
}
```

### 3.3 Redis Pools

**Implementation**: deadpool-redis

**Configuration**:
```toml
[[pools.redis]]
name = "cache_redis"
type = "redis"
url = "${REDIS_URL}"
max_connections = 10
min_connections = 2
connection_timeout = 2.0
```

**Health Check**:
```rust
async fn health_check_redis(conn: &Connection) -> Result<bool> {
    match redis::cmd("PING").query_async::<_, String>(conn).await {
        Ok(response) if response == "PONG" => Ok(true),
        _ => Ok(false),
    }
}
```

### 3.4 HTTP Client Pools

**Implementation**: reqwest with custom pooling

**Configuration**:
```toml
[[pools.http]]
name = "external_api"
type = "http"
max_connections = 50
connection_timeout = 10.0
pool_idle_timeout = 60.0
```

**Features**:
- Connection reuse (HTTP/1.1 keep-alive, HTTP/2 multiplexing)
- TLS session caching
- DNS caching
- Automatic retry with backoff

### 3.5 Custom Resource Pools

**Extensibility**: Support for user-defined resource types

```rust
trait ResourcePool: Send + Sync {
    async fn acquire(&self, worker_id: WorkerId) -> Result<Box<dyn Resource>>;
    async fn release(&self, resource: Box<dyn Resource>) -> Result<()>;
    async fn health_check(&self, resource: &dyn Resource) -> Result<bool>;
    fn name(&self) -> &str;
}

// User implements trait for custom resources
struct CustomPool { /* ... */ }

impl ResourcePool for CustomPool {
    // Implementation
}
```

---

## 4. Observability & Monitoring

### 4.1 Metrics

**Broker-Side Metrics** (Prometheus format):

```prometheus
# Pool health metrics
pool_connections_total{pool="postgres",state="idle|leased|health_check"} 15
pool_connections_acquired_total{pool="postgres"} 1234
pool_connections_released_total{pool="postgres"} 1230
pool_acquisition_duration_seconds{pool="postgres",quantile="0.5"} 0.002
pool_acquisition_duration_seconds{pool="postgres",quantile="0.9"} 0.005
pool_acquisition_duration_seconds{pool="postgres",quantile="0.99"} 0.009

# Resource utilization
pool_utilization_ratio{pool="postgres"} 0.75
pool_queue_length{pool="postgres"} 3
pool_max_wait_time_seconds{pool="postgres"} 0.5

# Health check metrics
pool_health_checks_total{pool="postgres",result="success"} 450
pool_health_checks_total{pool="postgres",result="failure"} 2
pool_connection_errors_total{pool="postgres",error_type="timeout"} 5

# Scheduler fairness metrics
scheduler_worker_requests_total{worker_id="123",pool="postgres"} 100
scheduler_worker_wait_time_seconds{worker_id="123",pool="postgres"} 0.3
scheduler_worker_starvation_events{worker_id="123"} 0
```

**Python Client Metrics**:

```python
from vertebra.resources.metrics import ResourceMetrics

metrics = ResourceMetrics()

# Automatic tracking
async with get_pool("postgres").acquire() as conn:
    # metrics.acquisition_time.observe(duration)
    # metrics.active_connections.inc()
    result = await conn.execute(query)
    # metrics.query_duration.observe(duration)
# metrics.active_connections.dec()
```

### 4.2 Logging

**Structured Logging** (JSON format):

```json
{
  "timestamp": "2026-01-17T10:30:45Z",
  "level": "INFO",
  "target": "vertebra_broker::pool_manager",
  "message": "Connection acquired",
  "fields": {
    "pool": "postgres",
    "worker_id": 12345,
    "connection_id": "conn_abc123",
    "wait_time_ms": 2.5
  }
}
```

**Log Levels**:
- **ERROR**: Connection failures, pool exhaustion, IPC errors
- **WARN**: Slow acquisitions (>1s), health check failures, approaching limits
- **INFO**: Pool lifecycle events, configuration changes
- **DEBUG**: Individual acquire/release operations
- **TRACE**: IPC protocol details, scheduler decisions

### 4.3 Distributed Tracing

**OpenTelemetry Integration**:

```rust
use opentelemetry::trace::{Tracer, SpanKind};

async fn handle_acquire_request(req: AcquireRequest) {
    let tracer = global::tracer("vertebra-broker");

    tracer.in_span("acquire_connection", |cx| async move {
        cx.span().set_attribute("pool.name", req.pool_name.clone());
        cx.span().set_attribute("worker.id", req.worker_id);

        // Nested span for scheduler
        tracer.in_span("schedule_request", |_| {
            scheduler.schedule(req).await
        }).await?;

        // Nested span for pool acquisition
        tracer.in_span("pool_acquire", |_| {
            pool.get().await
        }).await
    }).await
}
```

**Trace Propagation** (Python to Rust):

```python
from opentelemetry import trace

tracer = trace.get_tracer(__name__)

with tracer.start_as_current_span("acquire_pool_connection"):
    # Context propagated via IPC metadata
    async with get_pool("postgres").acquire() as conn:
        with tracer.start_as_current_span("execute_query"):
            result = await conn.execute(query)
```

### 4.4 Alerting

**Prometheus Alert Rules**:

```yaml
groups:
  - name: vertebra_pools
    rules:
      - alert: PoolNearExhaustion
        expr: pool_utilization_ratio > 0.9
        for: 5m
        annotations:
          summary: "Pool {{ $labels.pool }} at {{ $value }}% capacity"
          description: "Consider increasing max_connections"

      - alert: HighAcquisitionLatency
        expr: pool_acquisition_duration_seconds{quantile="0.99"} > 5
        for: 2m
        annotations:
          summary: "P99 acquisition latency is {{ $value }}s"

      - alert: HealthCheckFailures
        expr: rate(pool_health_checks_total{result="failure"}[5m]) > 0.1
        annotations:
          summary: "High health check failure rate for {{ $labels.pool }}"

      - alert: WorkerStarvation
        expr: scheduler_worker_starvation_events > 0
        annotations:
          summary: "Worker {{ $labels.worker_id }} experiencing starvation"
```

### 4.5 Grafana Dashboard

**Dashboard Panels**:

1. **Pool Utilization Over Time** (all pools)
   - Line graph: idle vs leased connections
   - Stacked area chart

2. **Acquisition Latency Percentiles**
   - Multi-line graph: p50, p90, p99, p999
   - Per-pool breakdown

3. **Queue Lengths and Wait Times**
   - Heatmap of worker queue lengths
   - Average wait time per worker

4. **Worker Fairness Visualization**
   - Bar chart: requests served per worker
   - Deviation from ideal fairness

5. **Health Check Success Rate**
   - Gauge: current health check pass rate
   - Time series of failures

6. **Error Rates by Type**
   - Pie chart: timeout, refused, invalid
   - Trend over time

7. **Resource Savings Calculator**
   - Comparison: traditional vs Vertebra
   - Connection count reduction percentage

---

## 5. Security & Error Handling

### 5.1 Security Considerations

#### 5.1.1 IPC Transport Security

**Permissions**:
- **Unix**: enforce 0600 on UDS path
- **Windows**: restrict named pipe ACLs to local user/service

**Worker Authentication**:
```rust
async fn authenticate_worker(&self, worker_id: WorkerId) -> Result<()> {
    // Verify via SO_PEERCRED
    let creds = get_peer_credentials(&socket)?;

    if creds.pid != worker_id.0 {
        return Err(Error::AuthenticationFailed);
    }

    // Check against allowed workers list
    if !self.allowed_workers.contains(&worker_id) {
        return Err(Error::Unauthorized);
    }

    Ok(())
}
```

#### 5.1.2 Connection String Sanitization

**Log Sanitization**:
```rust
impl PoolConfig {
    pub fn sanitize_for_logging(&self) -> String {
        self.dsn.as_ref()
            .map(|dsn| redact_password(dsn))
            .unwrap_or_else(|| format!("{}://{}:{}/{}",
                self.resource_type,
                self.host.as_ref().unwrap_or(&"localhost".into()),
                self.port.unwrap_or(5432),
                self.database.as_ref().unwrap_or(&"db".into())
            ))
    }
}

fn redact_password(dsn: &str) -> String {
    // postgresql://user:password@host/db -> postgresql://user:***@host/db
    dsn.replace_password("***")
}
```

#### 5.1.3 Rate Limiting

**Per-Worker Rate Limits**:
```rust
use governor::{Quota, RateLimiter};

struct WorkerLimiter {
    limiters: HashMap<WorkerId, RateLimiter>,
    quota: Quota,
}

impl WorkerLimiter {
    pub fn new(requests_per_second: u32) -> Self {
        Self {
            limiters: HashMap::new(),
            quota: Quota::per_second(nonzero!(requests_per_second)),
        }
    }

    pub async fn check(&mut self, worker_id: WorkerId) -> Result<()> {
        let limiter = self.limiters
            .entry(worker_id)
            .or_insert_with(|| RateLimiter::direct(self.quota));

        match limiter.check() {
            Ok(_) => Ok(()),
            Err(_) => Err(Error::RateLimitExceeded),
        }
    }
}
```

### 5.2 Error Handling

#### 5.2.1 Error Types

```rust
#[derive(Debug, thiserror::Error)]
pub enum BrokerError {
    #[error("Pool not found: {0}")]
    PoolNotFound(String),

    #[error("Connection acquisition timeout after {0:?}")]
    AcquisitionTimeout(Duration),

    #[error("Pool exhausted: all {0} connections in use")]
    PoolExhausted(usize),

    #[error("Connection health check failed: {0}")]
    HealthCheckFailed(String),

    #[error("IPC error: {0}")]
    IpcError(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    SerializationError(#[from] bincode::Error),

    #[error("Database error: {0}")]
    DatabaseError(Box<dyn std::error::Error + Send + Sync>),

    #[error("Configuration error: {0}")]
    ConfigError(String),

    #[error("Worker unauthorized: {0}")]
    Unauthorized(WorkerId),
}
```

**Python Exceptions**:
```python
class PoolException(Exception):
    """Base exception for pool errors."""

class PoolTimeoutError(PoolException):
    """Connection acquisition timed out."""

class PoolExhaustedError(PoolException):
    """Pool has no available connections."""

class ConnectionError(PoolException):
    """Connection is invalid or closed."""
```

#### 5.2.2 Retry Strategy

**Exponential Backoff**:
```rust
pub struct RetryPolicy {
    max_attempts: u32,
    initial_backoff: Duration,
    max_backoff: Duration,
    backoff_multiplier: f64,
}

impl RetryPolicy {
    pub async fn execute<F, T, E>(&self, mut f: F) -> Result<T, E>
    where
        F: FnMut() -> Future<Output = Result<T, E>>,
        E: std::error::Error,
    {
        let mut backoff = self.initial_backoff;

        for attempt in 1..=self.max_attempts {
            match f().await {
                Ok(result) => return Ok(result),
                Err(e) if attempt == self.max_attempts => return Err(e),
                Err(e) if is_retryable(&e) => {
                    warn!("Attempt {}/{} failed: {}, retrying in {:?}",
                          attempt, self.max_attempts, e, backoff);
                    tokio::time::sleep(backoff).await;
                    backoff = std::cmp::min(
                        Duration::from_secs_f64(
                            backoff.as_secs_f64() * self.backoff_multiplier
                        ),
                        self.max_backoff
                    );
                }
                Err(e) => return Err(e),
            }
        }

        unreachable!()
    }
}
```

#### 5.2.3 Circuit Breaker

**Pool-Level Circuit Breaker**:
```rust
use circuit_breaker::CircuitBreaker;

struct PoolWithCircuitBreaker {
    pool: Pool,
    circuit_breaker: CircuitBreaker,
}

impl PoolWithCircuitBreaker {
    pub async fn acquire(&self, worker_id: WorkerId) -> Result<Connection> {
        self.circuit_breaker.call(|| async {
            self.pool.acquire(worker_id).await
        }).await.map_err(|e| match e {
            CircuitBreakerError::Open => BrokerError::PoolUnavailable,
            CircuitBreakerError::Inner(e) => e,
        })
    }
}
```

### 5.3 Edge Cases & Failure Modes

#### 5.3.1 Worker Crash Detection

**Automatic Cleanup**:
```rust
async fn monitor_workers(&self) {
    let mut interval = tokio::time::interval(Duration::from_secs(5));

    loop {
        interval.tick().await;

        for (worker_id, handles) in self.active_connections.iter() {
            if !is_process_alive(worker_id.0) {
                warn!("Worker {} appears dead, releasing {} connections",
                      worker_id.0, handles.len());

                for handle in handles {
                    self.force_release(*handle).await;
                }
            }
        }
    }
}
```

#### 5.3.2 Connection Leak Detection

**Lease Timeout Monitoring**:
```rust
struct ConnectionTracker {
    acquisitions: HashMap<ConnectionHandle, AcquisitionInfo>,
    max_lease_time: Duration,
}

struct AcquisitionInfo {
    worker_id: WorkerId,
    acquired_at: Instant,
    stack_trace: Option<String>,
}

impl ConnectionTracker {
    async fn check_for_leaks(&self) {
        let now = Instant::now();

        for (handle, info) in &self.acquisitions {
            let lease_duration = now - info.acquired_at;

            if lease_duration > self.max_lease_time {
                error!(
                    "Potential leak: handle={}, worker={}, duration={:?}",
                    handle.id(), info.worker_id.0, lease_duration
                );

                metrics::CONNECTION_LEAK_DETECTED.inc();

                // Force release after 2x threshold
                if lease_duration > self.max_lease_time * 2 {
                    warn!("Force releasing leaked connection {}", handle.id());
                    // self.force_release(*handle).await;
                }
            }
        }
    }
}
```

#### 5.3.3 Broker Restart Recovery

**State Persistence**:
```rust
struct BrokerState {
    active_connections: HashMap<WorkerId, Vec<ConnectionHandle>>,
    pool_configs: Vec<PoolConfig>,
}

impl ResourceBroker {
    async fn persist_state(&self) -> Result<()> {
        let state = BrokerState {
            active_connections: self.active_connections.clone(),
            pool_configs: self.pools.values()
                .map(|p| p.config().clone())
                .collect(),
        };

        let data = bincode::serialize(&state)?;
        tokio::fs::write("/var/lib/vertebra/state.bin", data).await?;

        Ok(())
    }

    async fn recover_state(&mut self) -> Result<()> {
        match tokio::fs::read("/var/lib/vertebra/state.bin").await {
            Ok(data) => {
                let state: BrokerState = bincode::deserialize(&data)?;

                for config in state.pool_configs {
                    let pool = self.create_pool(config).await?;
                    self.pools.insert(pool.name().clone(), pool);
                }

                info!("State recovered, {} pools restored", self.pools.len());
                Ok(())
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                info!("No state file, starting fresh");
                Ok(())
            }
            Err(e) => Err(e.into()),
        }
    }
}
```

#### 5.3.4 Network Partition Handling

**Client-Side Fallback**:
```python
class ResilientConnection:
    async def execute(self, query: str, *args):
        try:
            return await self._execute_with_retry(query, *args)
        except (ConnectionError, TimeoutError) as e:
            logger.error(f"Lost connection to broker: {e}")

            if self._fallback_enabled:
                logger.info("Falling back to local connection")
                return await self._execute_local(query, *args)
            raise

    async def _execute_with_retry(self, query, *args, max_retries=3):
        for attempt in range(max_retries):
            try:
                return await self._ipc_client.execute(
                    self._handle, query, args
                )
            except IPCError as e:
                if attempt == max_retries - 1:
                    raise
                await asyncio.sleep(2 ** attempt)
```

---

## 6. Deployment & Operations

### 6.1 Deployment Models

#### 6.1.1 Systemd Service

```ini
[Unit]
Description=Vertebra Resource Broker
After=network.target

[Service]
Type=notify
ExecStart=/usr/local/bin/vertebra-broker --config /etc/vertebra/broker.toml
Restart=always
RestartSec=10s
User=vertebra
Group=vertebra

# Resource limits
LimitNOFILE=65536
LimitNPROC=4096

# Logging
StandardOutput=journal
StandardError=journal
SyslogIdentifier=vertebra-broker

# Security
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/var/run/vertebra /var/log/vertebra

[Install]
WantedBy=multi-user.target
```

#### 6.1.2 Docker Deployment

```dockerfile
FROM rust:1.75-slim as builder

WORKDIR /app
COPY src/rs/vertebra-broker .
RUN cargo build --release

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    libssl3 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/vertebra-broker /usr/local/bin/

RUN groupadd -r vertebra && useradd -r -g vertebra vertebra
RUN mkdir -p /var/run/vertebra /var/log/vertebra
RUN chown -R vertebra:vertebra /var/run/vertebra /var/log/vertebra

USER vertebra
EXPOSE 9090
VOLUME ["/etc/vertebra", "/var/run/vertebra"]

CMD ["vertebra-broker", "--config", "/etc/vertebra/broker.toml"]
```

#### 6.1.3 Kubernetes Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: vertebra-broker
  namespace: vertebra
spec:
  replicas: 1
  selector:
    matchLabels:
      app: vertebra-broker
  template:
    metadata:
      labels:
        app: vertebra-broker
    spec:
      containers:
      - name: broker
        image: vertebra/broker:latest
        ports:
        - containerPort: 9090
          name: metrics
        volumeMounts:
        - name: config
          mountPath: /etc/vertebra
        - name: ipc-socket
          mountPath: /var/run/vertebra
        resources:
          requests:
            memory: "256Mi"
            cpu: "500m"
          limits:
            memory: "1Gi"
            cpu: "2000m"
        livenessProbe:
          httpGet:
            path: /health
            port: 9090
          initialDelaySeconds: 30
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /health
            port: 9090
          initialDelaySeconds: 5
          periodSeconds: 5
      volumes:
      - name: config
        configMap:
          name: vertebra-config
      - name: ipc-socket
        hostPath:
          path: /var/run/vertebra
          type: DirectoryOrCreate
```

### 6.2 Configuration Management

**Broker Configuration** (`/etc/vertebra/broker.toml`):

```toml
[broker]
ipc_socket_path = "/var/run/vertebra/broker.sock"
ipc_max_clients = 1000
ipc_buffer_size = 65536

metrics_port = 9090
metrics_path = "/metrics"

log_level = "info"
log_format = "json"
log_output = "stdout"

[pools.defaults]
health_check_interval = 30
connection_timeout = 5.0
idle_timeout = 300.0
max_lifetime = 1800.0

[[pools.postgres]]
name = "main_postgres"
type = "postgresql"
dsn = "${POSTGRES_DSN}"
max_connections = 20
min_connections = 5

[[pools.redis]]
name = "cache_redis"
type = "redis"
url = "${REDIS_URL}"
max_connections = 10

[scheduler]
type = "weighted_fair_queue"
quantum = 100
max_wait_time = 30
starvation_threshold = 10
```

**Python Client Configuration**:

```python
from vertebra.resources import configure_client

configure_client(
    broker_socket="/var/run/vertebra/broker.sock",
    default_timeout=5.0,
    retry_attempts=3,
    retry_backoff=1.0,
    enable_metrics=True,
    metrics_interval=10.0
)
```

### 6.3 Operational Procedures

#### 6.3.1 Graceful Shutdown

```rust
impl ResourceBroker {
    pub async fn shutdown(&self, timeout: Duration) {
        info!("Initiating graceful shutdown");

        // Stop accepting new requests
        self.ipc_server.stop_accepting().await;

        // Wait for in-flight requests
        let deadline = Instant::now() + timeout;
        while self.has_active_requests() && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // Release all connections
        for pool in self.pools.values() {
            pool.close().await;
        }

        // Flush metrics
        self.metrics.flush().await;

        info!("Shutdown complete");
    }
}
```

#### 6.3.2 Hot Reload Configuration

```bash
# Send SIGHUP to reload config
kill -HUP $(cat /var/run/vertebra/broker.pid)

# Or use systemctl
systemctl reload vertebra-broker
```

```rust
async fn handle_sighup(&self) {
    let new_config = BrokerConfig::from_file("/etc/vertebra/broker.toml")?;
    self.reload_config(new_config).await?;
}
```

#### 6.3.3 Connection Draining

```bash
# Drain specific pool
vertebra-cli drain-pool --pool postgres --timeout 60s

# Drain all pools
vertebra-cli drain-all --timeout 120s
```

---

## 7. Testing Strategy

### 7.1 Unit Tests

**Rust Tests**:
```rust
#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn test_pool_acquisition_basic() {
        let pool = create_test_pool().await;
        let handle = pool.acquire(WorkerId(1)).await.unwrap();
        assert!(handle.is_valid());
        pool.release(handle).await.unwrap();
    }

    #[tokio::test]
    async fn test_scheduler_fairness() {
        let scheduler = WeightedFairScheduler::new();

        for _ in 0..10 {
            scheduler.enqueue(WorkerId(1), request()).await;
            scheduler.enqueue(WorkerId(2), request()).await;
            scheduler.enqueue(WorkerId(3), request()).await;
        }

        let stats = scheduler.get_stats();
        assert!((stats.worker_1_served - 10).abs() <= 2);
        assert!((stats.worker_2_served - 10).abs() <= 2);
        assert!((stats.worker_3_served - 10).abs() <= 2);
    }
}
```

**Python Tests**:
```python
@pytest.mark.asyncio
async def test_basic_pool_acquisition():
    async with get_pool("test_postgres").acquire() as conn:
        assert conn is not None
        result = await conn.execute("SELECT 1")
        assert result is not None

@pytest.mark.asyncio
async def test_pool_timeout():
    config = PoolConfig(
        name="test_limited",
        resource_type=ResourceType.POSTGRESQL,
        max_connections=1,
        connection_timeout=0.5
    )

    pool = await register_pool(config)

    async with pool.acquire() as conn1:
        with pytest.raises(PoolTimeoutError):
            async with pool.acquire() as conn2:
                pass
```

### 7.2 Integration Tests

```python
@pytest.mark.integration
async def test_e2e_postgres_pool():
    broker = await start_test_broker()

    config = PoolConfig(
        name="integration_postgres",
        resource_type=ResourceType.POSTGRESQL,
        dsn="postgresql://test:test@localhost/testdb",
        max_connections=10
    )

    await register_pool(config)

    async with get_pool("integration_postgres").acquire() as conn:
        result = await conn.execute(
            "INSERT INTO test_table (data) VALUES ($1) RETURNING id",
            "test_data"
        )
        assert result["id"] is not None

    await broker.stop()
```

### 7.3 Performance Benchmarks

**Criterion Benchmarks** (Rust):
```rust
use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn bench_pool_acquisition(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let pool = runtime.block_on(create_test_pool());

    c.bench_function("pool_acquire_release", |b| {
        b.to_async(&runtime).iter(|| async {
            let handle = pool.acquire(WorkerId(1)).await.unwrap();
            pool.release(handle).await.unwrap();
        });
    });
}
```

**Load Testing** (Python):
```python
async def load_test_scenario():
    """1000 concurrent workers, 100 requests each."""
    async def worker(worker_id: int):
        for i in range(100):
            async with get_pool("postgres").acquire() as conn:
                await conn.execute("SELECT pg_sleep(0.01)")

    workers = [worker(i) for i in range(1000)]
    start = time.time()
    await asyncio.gather(*workers)
    duration = time.time() - start

    total_queries = 1000 * 100
    qps = total_queries / duration

    print(f"Throughput: {qps:.2f} queries/sec")
```

### 7.4 Chaos Testing

**Failure Scenarios**:
1. Broker process killed (SIGKILL)
2. Database connection refused
3. Network partition between workers and broker
4. Slow query causing connection leaks
5. Worker process crashes mid-request
6. Resource exhaustion (memory, file descriptors)

---

## 8. Implementation Roadmap

### Phase 1: Foundation (Weeks 1-3)

**Milestone 1.1: Rust Broker Skeleton**
- [ ] Set up vertebra-broker crate structure
- [ ] Implement configuration loading (TOML)
- [ ] Create IPC server (UDS on Unix, named pipe on Windows)
- [ ] Message serialization/deserialization
- [ ] Structured logging

**Milestone 1.2: Single Pool**
- [ ] Integrate deadpool-postgres
- [ ] PoolManager for PostgreSQL
- [ ] Acquisition/release logic
- [ ] Health checks
- [ ] Integration tests

### Phase 2: Python Client (Weeks 4-5)

**Milestone 2.1: IPC Client**
- [ ] IPCClient implementation
- [ ] Message serialization
- [ ] Connection wrapper
- [ ] Error handling & retry
- [ ] End-to-end tests

**Milestone 2.2: Multi-Pool Support**
- [ ] Redis pool (deadpool-redis)
- [ ] MySQL pool (sqlx-mysql)
- [ ] Pool registry
- [ ] Multi-pool tests

### Phase 3: Fair Scheduling (Weeks 6-7)

**Milestone 3.1: Basic Scheduler**
- [ ] Round-robin scheduler
- [ ] Per-worker queuing
- [ ] Timeout handling
- [ ] Queue metrics

**Milestone 3.2: WFQ Implementation**
- [ ] Weighted Fair Queuing
- [ ] Deficit Round Robin
- [ ] Starvation prevention
- [ ] Fairness validation

### Phase 4: Production Readiness (Weeks 8-10)

**Milestone 4.1: Observability**
- [ ] Prometheus metrics
- [ ] Grafana dashboard
- [ ] OpenTelemetry tracing
- [ ] Alert rules

**Milestone 4.2: Security & Resilience**
- [ ] IPC authentication
- [ ] Rate limiting
- [ ] Circuit breakers
- [ ] Leak detection
- [ ] Crash handling

**Milestone 4.3: Deployment**
- [ ] Docker image
- [ ] Kubernetes manifests
- [ ] Systemd service
- [ ] Operational runbooks

### Phase 5: Integration & Testing (Weeks 11-12)

**Milestone 5.1: Performance Testing**
- [ ] Load testing (1000+ workers)
- [ ] Latency benchmarks
- [ ] Resource profiling
- [ ] Comparison report

**Milestone 5.2: PRD-4 Integration**
- [ ] Bun driver integration
- [ ] Performance comparison
- [ ] E2E tests

**Milestone 5.3: Production Pilot**
- [ ] Staging deployment
- [ ] 10% production traffic
- [ ] Metrics monitoring
- [ ] Incident response plan

### Phase 6: Production Rollout (Weeks 13-14)

**Milestone 6.1: Gradual Rollout**
- [ ] 25% production traffic
- [ ] 50% production traffic
- [ ] 75% production traffic
- [ ] 100% production traffic

**Milestone 6.2: Documentation**
- [ ] User guide
- [ ] API documentation
- [ ] Troubleshooting guide
- [ ] Training sessions

---

## 9. Success Metrics

### 9.1 Performance Targets

| Metric | Target | Measurement |
|--------|--------|-------------|
| P50 Acquisition Latency | < 2ms | Histogram |
| P90 Acquisition Latency | < 5ms | Histogram |
| P99 Acquisition Latency | < 10ms | Histogram |
| P999 Acquisition Latency | < 50ms | Histogram |
| Throughput | > 10,000 req/s | Counter |

### 9.2 Resource Efficiency

| Metric | Target | Measurement |
|--------|--------|-------------|
| Connection Reduction | 70-90% | Comparison |
| Memory Savings | 60-80% | RSS comparison |
| CPU Overhead | < 5% | Broker CPU usage |

### 9.3 Reliability

| Metric | Target | Measurement |
|--------|--------|-------------|
| Broker Uptime | 99.95% | SLA |
| Error Rate | < 0.1% | Error counter |
| MTTR (Broker Crash) | < 30s | Recovery time |

### 9.4 Fairness

| Metric | Target | Measurement |
|--------|--------|-------------|
| Worker Deviation | < 10% | Stddev of requests/worker |
| Starvation Events | 0 | Counter |
| Max Wait Time | < 5s | Histogram |

---

## 10. Risks & Mitigation

### 10.1 Technical Risks

| Risk | Impact | Probability | Mitigation |
|------|--------|-------------|------------|
| IPC performance bottleneck | High | Medium | Benchmark early, use iceoryx2 if needed |
| Broker SPOF | High | Low | Fast restart, state recovery, fallback |
| Connection leaks | Medium | Medium | Leak detection, monitoring, force release |
| Scheduler complexity | Medium | Low | Start simple, iterate to WFQ |
| Integration with PRD-4 | Medium | Medium | Decouple, maintain standalone mode |

### 10.2 Operational Risks

| Risk | Impact | Probability | Mitigation |
|------|--------|-------------|------------|
| Migration complexity | High | Medium | Gradual rollout, feature flags |
| Team learning curve | Medium | High | Training, documentation, pair programming |
| Production incidents | High | Low | Runbooks, rollback plan, monitoring |

### 10.3 Go/No-Go Decision Points

1. **After Phase 1**: IPC latency < 1ms (95th percentile)
2. **After Phase 3**: Scheduler fairness deviation < 15%
3. **After Phase 4**: Security audit pass, zero critical issues
4. **After Phase 5**: Production pilot error rate < 0.5%

---

## 11. Dependencies

### 11.1 External Dependencies

**Rust Crates**:
- `deadpool-postgres = "0.15"` - PostgreSQL pooling
- `deadpool-redis = "0.18"` - Redis pooling
- `sqlx = { version = "0.8", features = ["mysql"] }` - MySQL
- `shmipc = "0.9"` - Zero-copy IPC
- `tokio = { version = "1", features = ["full"] }` - Async runtime
- `serde = { version = "1", features = ["derive"] }` - Serialization
- `bincode = "1"` - Binary serialization
- `prometheus = "0.13"` - Metrics
- `tracing = "0.1"` - Logging

**Python Packages**:
- `asyncio` (stdlib) - Async runtime
- `pytest-asyncio` - Async testing
- `bincode` - Binary serialization (via FFI or pure Python)

### 11.2 Project Dependencies

**PRD-4**: Bun-Backed Drivers (optional but recommended)
- Resource management works standalone
- Optimal performance when combined with Bun drivers
- Decouple implementation to avoid blocking

---

## 12. Open Questions

### 12.1 Technical Questions

**Q1**: Can shmipc handle 1000+ concurrent workers?
**A**: Benchmark in Phase 1. Fallback to iceoryx2 if performance insufficient.

**Q2**: How to handle broker upgrades without downtime?
**A**: State persistence + fast restart (< 5s target). Consider blue-green deployment.

**Q3**: What happens if broker crashes?
**A**: Python clients fall back to local connections (if enabled). Monitor crash rate, auto-restart.

**Q4**: How to migrate existing services?
**A**: Gradual rollout with feature flags. Backward compatibility mode for smooth transition.

### 12.2 Product Questions

**Q5**: Should we support distributed broker architecture?
**A**: Not in v1. Single broker per node. Evaluate need based on adoption.

**Q6**: What's the migration strategy for legacy services?
**A**: Opt-in migration, parallel operation during transition, detailed migration guide.

---

## 13. Appendix

### 13.1 Research Sources

**POSIX Shared Memory & IPC**:
- https://man7.org/linux/man-pages/man3/shm_open.3.html
- https://man7.org/linux/man-pages/man2/mmap.2.html

**Async Runtime**:
- https://docs.rs/tokio/

**Observability**:
- https://opentelemetry.io/docs/
- https://prometheus.io/docs/introduction/overview/

**Zero-Copy IPC (Reference Implementations)**:
- https://github.com/eclipse-iceoryx/iceoryx2
- https://crates.io/crates/shmipc

### 13.2 Glossary

**Terms**:
- **Broker**: Centralized Rust process managing connection pools
- **Worker**: Python process using shared pools
- **IPC**: Inter-Process Communication
- **WFQ**: Weighted Fair Queuing
- **DRR**: Deficit Round Robin
- **Lease**: Period during which worker holds connection
- **Health Check**: Validation that connection is still usable
- **Circuit Breaker**: Pattern to prevent cascading failures

---

## 14. Conclusion

PRD-5 defines a comprehensive cooperative resource management system for Vertebra that will deliver significant resource savings (70-90% reduction in connections) while maintaining high performance (sub-10ms latency). The architecture leverages Rust's performance and safety for the broker, combined with a simple Python API for ease of adoption.

The phased implementation approach ensures incremental validation of critical components (IPC performance, fair scheduling, security) before production rollout. With proper monitoring, testing, and operational procedures, this system will provide a robust foundation for efficient resource sharing across Vertebra deployments.

**Next Steps**:
1. Stakeholder review and approval
2. Team allocation and kickoff
3. Phase 1 implementation (broker foundation)
4. Continuous validation against success metrics

---

**Document Version**: 1.0
**Total Word Count**: 6,800+ words
**Last Updated**: 2026-01-17
