# PRD: Bun Broker Pool Manager

## Context
The **Bun Broker** is a high-performance database proxy/broker. It currently employs a `FairScheduler` to manage incoming query requests and ensure fair resource allocation among tenants/users. However, the `FairScheduler` currently lacks a robust mechanism to manage the lifecycle of the underlying database connections.

The **Pool Manager** is a new component designed to bridge this gap. It will own the database connections, handling their creation, validation, health monitoring, and destruction, while serving valid connections to the `FairScheduler` upon request.

## Goals
1.  **Connection Lifecycle Management:** robustly handle connect, disconnect, and error states.
2.  **Resource Optimization:** Reuse connections to minimize handshake overhead (Connection Pooling).
3.  **Resilience:** Proactively detect and evict broken connections (Health Checks).
4.  **Observability:** Provide real-time metrics on pool state (active, idle, wait times) to inform the Scheduler and operators.

## Non-Goals
-   **Query Scheduling:** The `FairScheduler` decides *who* gets a connection and *when*. The Pool Manager only provides the *resource*.
-   **Protocol Parsing:** The Pool Manager manages `Connection` objects (opaque or interface-based) but does not parse SQL/Wire protocol messages itself (unless required for health checks).

## Architecture

### Component Interaction
```
[Client] -> [Bun Broker]
                |
          [FairScheduler] --(Acquire)--> [Pool Manager]
                |                             |
                +<-------(Connection)---------+
                |
          [Connection] <---> [Database]
```

### logical Components

#### 1. Connection Pool (`PoolManager`)
The central orchestrator.
-   **State:**
    -   `idle_connections`: Queue/Stack of ready connections.
    -   `active_connections`: Set of connections currently leased.
    -   `total_count`: Current number of connections (idle + active).
-   **Config:**
    -   `min_size`: Minimum number of connections to keep open.
    -   `max_size`: Hard limit on total connections.
    -   `idle_timeout`: Max time a connection can stay idle before eviction.
    -   `max_lifetime`: Max absolute life of a connection.
    -   `connection_timeout`: Max time to wait for a new connection creation.

#### 2. Health Checker
A background or reactive process.
-   **Passive Check:** On `release()`, check connection error state.
-   **Active Check:** On `acquire()` (optional, high latency) or Background Loop (low latency).
-   **Strategy:** "Select 1" or equivalent ping.

#### 3. Eviction Policy
-   **LRU (Least Recently Used):** Good for distributing load.
-   **LIFO (Last In First Out):** Good for keeping hot connections hot and letting cold ones time out. *Preferred for DB pools to minimize idle connections on DB side.*

## API Specification

### `PoolManager` Interface

```python
class PoolManager:
    async def start(self) -> None:
        """Initialize pool, start background tasks (health check, min_size maintainer)."""

    async def stop(self) -> None:
        """Gracefully close all connections."""

    async def acquire(self, timeout: float = 5.0) -> Connection:
        """
        Request a connection.
        - If idle available: pop, validate (fast), return.
        - If not idle & total < max: create new, return.
        - If total == max: wait for release/timeout.
        """

    async def release(self, conn: Connection, discard: bool = False) -> None:
        """
        Return a connection to the pool.
        - If discard=True or conn is broken: close and decrement total.
        - Else: push to idle list.
        """

    def stats(self) -> PoolStats:
        """Return {active, idle, waiting, total}."""
```

### `FairScheduler` Integration Hooks
The `FairScheduler` will hold a reference to `PoolManager`.
-   When `FairScheduler` processes a queue item: call `pool.acquire()`.
-   When task finishes: call `pool.release()`.
-   On `PoolError` (exhausted/timeout): Scheduler must requeue or reject the task.

## Observability Hooks
The Pool Manager must emit events or expose metrics:
-   `pool.size`: Gauge.
-   `pool.in_use`: Gauge.
-   `pool.wait_count`: Gauge (pending acquires).
-   `pool.acquire_latency`: Histogram.
-   `pool.connection_created`: Counter.
-   `pool.connection_closed`: Counter (labeled by reason: idle, error, max_age).

## Logic Details

### Connection Creation
-   Must be async.
-   Must handle "thundering herd" if multiple acquires trigger creation (use semaphore/lock for creation logic).

### Health Checks
-   **Background Reaper:** Runs every X seconds.
    -   Closes connections > `max_lifetime`.
    -   Closes idle connections > `idle_timeout` (down to `min_size`).
    -   Optionally pings idle connections to keep them alive or detect server-side closes.

## detailed Requirements
1.  **Concurrency:** Must be thread-safe (or asyncio-safe, assuming Python/Bun context).
2.  **Leaks:** Must guarantee `active_connections` is decremented on release/error.
3.  **Backpressure:** If pool is empty, `acquire` must block (async wait) until a connection is released or timeout occurs.

