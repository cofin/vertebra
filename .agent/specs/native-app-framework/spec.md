# Native Vertebra App Framework

## Overview

A high-performance native application framework for Vertebra that bypasses ASGI overhead, providing real-time state synchronization via opcode-based field deltas over SPSC rings. The framework follows Litestar-aligned patterns (Router, Route, Controller, Guards, Events) while leveraging Rust and Bun for extreme performance optimization.

**Key Goals:**
- Sub-100μs IPC latency for state updates
- >50K req/s throughput
- Real-time state sync across Python workers
- Universal dependency injection across all layers
- Clean, type-safe API with `Inject[T]` pattern

## Research Reference

Multi-model consensus (Gemini 3 Pro + Flash) achieved 9-10/10 confidence on:
- Opcode-based Field Deltas over SPSC Rings
- AOT DI Compilation for hot paths
- Hybrid Rust/Bun/Python architecture split

## Functional Requirements

### Must Have

#### Native App & Routing
- [ ] `VertebraApp` class as the native application entry point (not ASGI)
- [ ] `Router` for grouping routes with shared middleware/guards
- [ ] `Route` decorators (`@get`, `@post`, `@put`, `@delete`, `@patch`, `@websocket`)
- [ ] `Controller` base class for grouping related routes
- [ ] Path parameters with type coercion (`/users/{user_id:int}`)
- [ ] Rust-based router using `matchit` for O(1) route matching

#### State Synchronization
- [ ] `NativeState` with opcode-based field deltas (not full snapshots)
- [ ] SPSC ring buffers for state updates between workers
- [ ] Bun Broker as central state hub with LWW (Last-Writer-Wins) resolution
- [ ] Local shadow state in Python workers for nanosecond reads
- [ ] Immutable replace only for nested objects (no deep mutation tracking)

#### Dependency Injection
- [ ] `Inject[T]` type annotation for handler parameters (clean consumption)
- [ ] Simplified provider registration (avoid verbose `@provide` decorators)
- [ ] AOT compilation of dependency graphs at startup
- [ ] Universal DI across: Controllers, Guards, Middleware, Events, CLI
- [ ] `VertebraContext` abstraction for all entry points

#### Guards & Middleware
- [ ] Guard system with `requires()` decorator pattern
- [ ] Native guards in Rust for performance (JWT validation, rate limiting)
- [ ] Python guards for complex business logic
- [ ] Middleware chain with before/after hooks
- [ ] Guard result caching within request scope

#### Events & Lifecycle
- [ ] Application lifecycle hooks (`on_startup`, `on_shutdown`)
- [ ] Event system for pub/sub within the application
- [ ] Background task support with DI access

### Should Have

#### Frontend Sync
- [ ] SSE (Server-Sent Events) for state broadcasting
- [ ] WebSocket support for bidirectional communication
- [ ] Long-polling fallback for restricted environments
- [ ] Automatic reconnection with state catch-up

#### Response Handling
- [ ] Automatic JSON serialization via msgspec
- [ ] Streaming response support (NDJSON, SSE)
- [ ] File responses with range request support
- [ ] Custom response types and serializers

#### Validation
- [ ] Request body validation via msgspec/Pydantic
- [ ] Query parameter validation
- [ ] Path parameter type coercion and validation

### Nice to Have

- [ ] OpenAPI schema generation
- [ ] GraphQL integration layer
- [ ] gRPC support via shared-memory transport
- [ ] Hot reload for development

## Non-Functional Requirements

### Performance
| Metric | Target | Notes |
|--------|--------|-------|
| IPC latency | <100μs | State sync round-trip |
| State read (local shadow) | <100ns | From worker shadow state |
| Throughput (simple JSON) | >200K req/s | No ASGI overhead baseline |
| Throughput (with state sync) | >150K req/s | Including delta propagation |
| Memory allocation rate | <1MB/s | Under sustained load |
| Route matching | O(1) | Rust matchit |
| DI resolution (compiled) | ~0 overhead | AOT removes container lookups |

**Comparison baseline:** Granian ASGI achieves ~70-100K req/s. Native should exceed this by 2-3x.

### Security
- JWT validation in Rust (no Python GIL)
- Guard execution before handler invocation
- Scoped dependency lifetimes (request, session, app)
- No shared mutable state without LWW coordination

### Reliability
- Graceful degradation if Rust/Bun unavailable
- State recovery on worker restart
- Connection pooling with backpressure

## Technical Approach

### Architecture Split

```
┌─────────────────────────────────────────────────────────────┐
│                    Native App Architecture                   │
├─────────────────────────────────────────────────────────────┤
│  RUST CORE         │  BUN IO LAYER      │  PYTHON LOGIC     │
│  ───────────────   │  ──────────────    │  ─────────────    │
│  • Router(matchit) │  • HTTP/WS server  │  • Controllers    │
│  • State LWW CRDT  │  • SSE broadcast   │  • Services       │
│  • Native Guards   │  • Long-polling    │  • Domain logic   │
│  • Dep Graph Cache │  • Static assets   │  • Inject[T]      │
│  • Ring Buffers    │  • Connection mgmt │  • Events         │
└─────────────────────────────────────────────────────────────┘
```

### State Sync: Opcode-Delta Ring

```
Python Worker A                    Bun Broker                    Python Worker B
     │                                 │                              │
     │  ┌─────────────────────┐        │                              │
     │  │ user.name = "Alice" │        │                              │
     │  └─────────────────────┘        │                              │
     │            │                    │                              │
     │   Delta: (FIELD_SET, "name",    │                              │
     │           "Alice", timestamp)   │                              │
     │  ──────────────────────────────>│                              │
     │                                 │  LWW Resolution              │
     │                                 │  Update Authoritative        │
     │                                 │  ─────────────────────────>  │
     │                                 │   Delta broadcast            │
     │                                 │                              │
     │                                 │  <─────────────────────────  │
     │                                 │   ACK                        │
     │  <──────────────────────────────│                              │
     │   ACK                           │                              │
```

**Opcodes:**
| Opcode | Name | Payload |
|--------|------|---------|
| 0x01 | FIELD_SET | (field_id, value, timestamp) |
| 0x02 | FIELD_DELETE | (field_id, timestamp) |
| 0x03 | OBJECT_REPLACE | (field_id, new_object, timestamp) |
| 0x04 | SYNC_REQUEST | (state_id) |
| 0x05 | SYNC_RESPONSE | (full_state) |

### Dependency Injection: AOT Compilation

**Registration (simplified):**
```python
from vertebra import App, provide

app = App()

# Auto-discovered from type hints
app.provide(DatabasePool)
app.provide(UserService)
app.provide(AuthService, scope="request")

# Or factory function
@app.provide
def get_config() -> AppConfig:
    return AppConfig.from_env()
```

**Consumption (clean Inject[T]):**
```python
from vertebra import Controller, get, Inject

class UserController(Controller):
    path = "/api/users"
    guards = [requires_auth]

    @get("/{user_id:int}")
    async def get_user(
        self,
        user_id: int,
        user_service: Inject[UserService],
    ) -> User:
        return await user_service.get(user_id)
```

**AOT Compilation:**
At startup, the dependency graph is analyzed and compiled into linear execution sequences, eliminating runtime container lookups for hot paths.

### Files to Modify/Create

**New Files:**
- `src/py/vertebra/app.py` - VertebraApp class
- `src/py/vertebra/routing/` - Router, Route, decorators
- `src/py/vertebra/controllers/` - Controller base class
- `src/py/vertebra/di/` - Inject, Provider, AOT compiler
- `src/py/vertebra/guards/` - Guard system
- `src/py/vertebra/events/` - Event system
- `src/py/vertebra/middleware/` - Middleware chain
- `src/rs/vertebra-core/src/router.rs` - Rust matchit wrapper
- `src/rs/vertebra-core/src/state_manager.rs` - LWW state hub
- `src/rs/vertebra-core/src/guards.rs` - Native guards
- `src/ts/src/broker/state-hub.ts` - Bun state synchronization
- `src/ts/src/server/sse.ts` - SSE streaming
- `src/ts/src/server/websocket.ts` - WebSocket handling

**Modified Files:**
- `src/py/vertebra/state.py` - Extend NativeState with opcode deltas
- `src/rs/vertebra-core/src/ring.rs` - Add state delta opcodes
- `src/ts/src/broker/serve.ts` - Add state hub service

## Acceptance Criteria

- [ ] `VertebraApp` can define routes without ASGI
- [ ] Routes resolve in O(1) time via Rust matchit
- [ ] State changes propagate to all workers in <100μs
- [ ] `Inject[T]` works in controllers, guards, middleware, events
- [ ] Guards execute before handlers with DI access
- [ ] SSE/WebSocket/long-polling deliver state updates to frontend
- [ ] Multi-writer state uses LWW without conflicts
- [ ] Simple JSON throughput >200K req/s (2-3x Granian ASGI baseline)
- [ ] Throughput with state sync >150K req/s
- [ ] Memory allocation rate <1MB/s under load
- [ ] All tests pass with >90% coverage

## Out of Scope

- ASGI compatibility layer (separate adapter if needed)
- GraphQL/gRPC (future PRD)
- Database migrations
- Admin UI
- OpenTelemetry integration (separate PRD)

## Dependencies

**Internal:**
- `vertebra-core` Rust crate (ring buffers, sync primitives)
- `vertebra-py` PyO3 bindings
- `vertebra-js` napi-rs bindings
- Bun broker infrastructure

**External:**
- `matchit` (Rust) - Path router
- `msgspec` (Python) - Serialization
- `dishka` patterns (inspiration only, not dependency)

## Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| AOT DI adds startup latency | Medium | Low | Lazy compilation, cache graph |
| LWW conflicts in high-contention | Low | Medium | Vector clocks for detection, merge strategies |
| Rust FFI overhead negates gains | Low | High | Profile early, batch operations, minimize crossings |
| API divergence from Litestar | Medium | Medium | Document differences, provide migration guide |
| State sync race conditions | Medium | High | Extensive integration tests, formal verification |

### Recovery Strategy

**Rollback Trigger:**
- Performance regression >20% vs ASGI baseline
- Critical bugs in state synchronization
- DI compilation failures in production

**Rollback Steps:**
1. Revert to ASGI adapter mode
2. Disable native routing, use Litestar directly
3. Fall back to per-worker state (no sync)

**Safe Checkpoints:**
- After each phase completion
- Full integration test suite must pass
- Performance benchmarks must meet targets

## API Examples

### Basic App

```python
from vertebra import App, Router, Controller, get, post, Inject
from vertebra.guards import requires

app = App()

# Register providers
app.provide(DatabasePool)
app.provide(UserService)

# Define controller
class UserController(Controller):
    path = "/users"
    guards = [requires("authenticated")]

    @get("/")
    async def list_users(
        self,
        service: Inject[UserService],
    ) -> list[User]:
        return await service.list_all()

    @get("/{user_id:int}")
    async def get_user(
        self,
        user_id: int,
        service: Inject[UserService],
    ) -> User:
        return await service.get(user_id)

    @post("/")
    async def create_user(
        self,
        data: CreateUserRequest,
        service: Inject[UserService],
    ) -> User:
        return await service.create(data)

# Register controller
app.register(UserController)

# Or use router for grouping
api = Router("/api/v1")
api.register(UserController)
api.register(ProductController)
app.register(api)
```

### State Synchronization

```python
from vertebra import App, NativeState
from vertebra.state import sync_field

class GameState(NativeState):
    players: dict[str, Player] = {}
    score: int = 0

    # Immutable replace for nested objects
    def add_player(self, player: Player) -> None:
        self.players = {**self.players, player.id: player}

app = App()
game = app.state(GameState)

@app.get("/score")
async def get_score() -> int:
    return game.score  # Nanosecond read from shadow state

@app.post("/score")
async def increment_score() -> int:
    game.score += 1  # Delta sent via opcode ring
    return game.score
```

### Guards with DI

```python
from vertebra import Guard, Inject
from vertebra.guards import requires

class RoleGuard(Guard):
    def __init__(self, required_role: str):
        self.required_role = required_role

    async def __call__(
        self,
        user: Inject[CurrentUser],
        role_service: Inject[RoleService],
    ) -> bool:
        return await role_service.has_role(user, self.required_role)

requires_admin = requires(RoleGuard("admin"))

class AdminController(Controller):
    path = "/admin"
    guards = [requires_admin]

    @get("/dashboard")
    async def dashboard(self) -> DashboardData:
        ...
```
