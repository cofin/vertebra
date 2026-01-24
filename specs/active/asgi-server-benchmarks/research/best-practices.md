# Framework & Server Best Practices for Benchmarking

## Benchmark Categories

The Python web ecosystem has three distinct layers that must be benchmarked separately:

### Category 1: Raw ASGI Servers
**What it tests**: Pure HTTP parsing, connection handling, event loop efficiency
**Same app on different servers** - isolates server performance

| Server | Type | Key Characteristics |
|--------|------|---------------------|
| Uvicorn (h11) | Pure Python | Baseline, most compatible |
| Uvicorn (httptools) | Cython | ~30% faster than h11 |
| Granian | Rust + PyO3 | Single interpreter, threaded workers |
| Hypercorn | Python | HTTP/2, HTTP/3 support |
| Daphne | Python | Django Channels reference |
| Socketify | C++ (uSockets) | Claims fastest, custom event loop |

### Category 2: Integrated Runtimes
**What it tests**: Full stack performance where server and framework are inseparable
**Each has its own runtime** - can't run on other servers

| Runtime | Architecture | Key Characteristics |
|---------|--------------|---------------------|
| Vertebra | Bun HTTP + Rust IPC + Python ASGI | Shared-memory transport |
| Robyn | Rust (Actix-web + Tokio) + PyO3 | Native interface, not ASGI |
| Sanic (standalone) | Python + uvloop | Built-in optimized server |
| Spikard | Rust (Tower-HTTP) + PyO3 | Zero-copy bindings |

### Category 3: Framework Overhead
**What it tests**: Framework routing, middleware, serialization overhead
**Same server (Uvicorn httptools)** - isolates framework performance

| Framework | Overhead Source | Key Characteristics |
|-----------|-----------------|---------------------|
| Raw ASGI | Baseline | No framework, pure ASGI callable |
| Starlette | Routing, middleware | Foundation for FastAPI |
| FastAPI | Pydantic validation | Adds ~30-50% overhead vs Starlette |
| BlackSheep | Cython routing | Optimized for speed |
| Sanic (on Uvicorn) | Full framework | Compare vs standalone |
| Litestar | Msgspec validation | Alternative to FastAPI |

---

## Server Best Practices

### Uvicorn

```bash
# Maximum performance configuration
uvicorn app:app \
    --host 127.0.0.1 \
    --port 8000 \
    --workers 1 \          # Single worker for fair comparison
    --loop uvloop \        # Cython event loop
    --http httptools \     # Cython HTTP parser
    --no-access-log \      # Disable logging overhead
    --limit-concurrency 1000
```

**Key settings**:
- `--loop uvloop`: 2-4x throughput improvement
- `--http httptools`: ~30% faster than h11
- `--no-access-log`: Eliminates I/O overhead
- Workers: Use 1 for benchmarks, `(2 * CPU) + 1` for production

### Granian

```bash
# Maximum performance configuration
granian app:app \
    --interface asgi \
    --host 127.0.0.1 \
    --port 8000 \
    --workers 1 \
    --threads 1 \
    --http 1 \             # HTTP/1.1
    --no-ws \              # Disable WebSocket
    --opt                  # Enable optimizations
```

**Key settings**:
- `--threads 1`: Single thread for fair comparison
- `--no-ws`: Disable unused features
- Runtime modes: `st` for <8 cores, `mt` for high-core systems
- Memory: ~15MB per worker

### Hypercorn

```bash
# Maximum performance configuration
hypercorn app:app \
    --bind 127.0.0.1:8000 \
    --workers 1 \
    -k uvloop \            # uvloop worker
    --access-log - \       # Disable access log (dash = no log)
    --error-log -
```

**Key settings**:
- `-k uvloop`: Required for competitive performance
- HTTP/2: `--quic-bind` for HTTP/3 testing
- Default is HTTP/1.1

### Daphne

```bash
# Maximum performance configuration
daphne app:app \
    -b 127.0.0.1 \
    -p 8000 \
    --access-log - \
    --verbosity 0
```

**Key settings**:
- Limited tuning options
- Good for Django Channels baseline
- Not competitive for raw performance

### Socketify

```bash
# Maximum performance configuration
python3 -m socketify app:app \
    --host 127.0.0.1 \
    --port 8000 \
    --workers 1 \
    --interface asgi \
    --task-factory-maxitems 100000 \
    --req-res-factory-maxitems 200000
```

**Key settings**:
- Factory pools: Pre-allocate 100K-200K objects
- Does NOT use uvloop (custom libuv integration)
- Supports WSGI, ASGI, and native interface
- `--ws none`: Disable WebSocket for HTTP benchmarks

---

## Integrated Runtime Best Practices

### Robyn

```bash
# Maximum performance configuration
python app.py \
    --host 127.0.0.1 \
    --port 8000 \
    --processes 1 \        # Single process for fair comparison
    --workers 1 \
    --log-level WARNING \
    --fast                 # Auto-optimization flag
```

**Application structure**:
```python
from robyn import Robyn

app = Robyn(__file__)

@app.get("/", const=True)  # const=True enables caching
async def hello():
    return "Hello, World!"

app.start(port=8000)
```

**Key settings**:
- `--fast`: Automatic production optimization
- `const=True`: Cache static responses
- Native interface (not ASGI) - faster than ASGI wrappers
- Uses Actix-web + Tokio under the hood

### Sanic (Standalone)

```bash
# Maximum performance configuration
sanic app:app \
    --host 127.0.0.1 \
    --port 8000 \
    --single-process \     # Single process for benchmarks
    --no-access-logs \
    --fast                 # Auto-optimization
```

**Application structure**:
```python
from sanic import Sanic, response

app = Sanic("benchmark")
app.config.ACCESS_LOG = False

@app.get("/")
async def hello(request):
    return response.text("Hello, World!")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, fast=True, single_process=True)
```

**Key settings**:
- `--fast`: Auto-tunes for performance
- `ACCESS_LOG = False`: Major performance gain
- `USE_UVLOOP = True`: Default, don't disable
- `--single-process`: Isolates benchmark from multiprocessing

### Spikard

```python
from spikard import Spikard

app = Spikard()

@app.get("/")
async def hello():
    return {"message": "Hello, World!"}

app.run(host="127.0.0.1", port=8000)
```

**Key settings**:
- Tower-HTTP runtime (Rust)
- Zero-copy PyO3 bindings (30-40% faster)
- ~17MB memory baseline
- Automatic middleware ordering

### Vertebra

```bash
# Maximum performance configuration
vertebra serve app:app \
    --host 127.0.0.1 \
    --port 8000 \
    --workers 1 \
    --slots 1024 \
    --slot-size 65536
```

**Key settings**:
- Bun handles HTTP layer
- Rust ring buffers for IPC
- Python executes ASGI
- Shared-memory transport (no sockets)

---

## Framework Best Practices

### Raw ASGI (Baseline)

```python
async def app(scope, receive, send):
    if scope["type"] == "http":
        await send({
            "type": "http.response.start",
            "status": 200,
            "headers": [[b"content-type", b"text/plain"]],
        })
        await send({
            "type": "http.response.body",
            "body": b"Hello, World!",
        })
```

**Notes**:
- Zero framework overhead
- Baseline for all comparisons

### Starlette

```python
from starlette.applications import Starlette
from starlette.responses import PlainTextResponse
from starlette.routing import Route

async def hello(request):
    return PlainTextResponse("Hello, World!")

app = Starlette(debug=False, routes=[Route("/", hello)])
```

**Key optimizations**:
- `PlainTextResponse` for text (fastest)
- `debug=False` for production behavior
- Avoid `GZipMiddleware` in benchmarks
- Pure ASGI middleware > `BaseHTTPMiddleware`

### FastAPI

```python
from fastapi import FastAPI
from fastapi.responses import PlainTextResponse

app = FastAPI()

@app.get("/", response_class=PlainTextResponse)
async def hello():
    return "Hello, World!"
```

**Key optimizations**:
- Skip Pydantic validation for benchmarks (return raw types)
- `ORJSONResponse` for JSON (20-50% faster)
- `async def` for I/O-bound (no threadpool)
- Disable assignment validation in models

### BlackSheep

```python
from blacksheep import Application, get

app = Application()

@get("/")
async def hello():
    return "Hello, World!"
```

**Key optimizations**:
- Install `httptools` for CPython
- Use Granian or Socketify for maximum speed
- PyPy runtime significantly faster
- Cython-optimized routing

### Litestar

```python
from litestar import Litestar, get

@get("/")
async def hello() -> str:
    return "Hello, World!"

app = Litestar(route_handlers=[hello])
```

**Key optimizations**:
- Msgspec for serialization (faster than Pydantic)
- `sync_to_thread=False` for truly async handlers
- Disable OpenAPI for benchmarks

---

## Benchmark Execution Matrix

### Category 1: Raw Server Performance

| Test | Servers | App |
|------|---------|-----|
| hello_c64 | All 6 servers | Raw ASGI |
| hello_c256 | All 6 servers | Raw ASGI |
| body_10k | All 6 servers | Raw ASGI |
| echo | All 6 servers | Raw ASGI |

### Category 2: Integrated Runtimes

| Test | Runtimes | Notes |
|------|----------|-------|
| hello_c64 | Vertebra, Robyn, Sanic, Spikard | Native interface |
| hello_c256 | Vertebra, Robyn, Sanic, Spikard | Native interface |
| body_10k | Vertebra, Robyn, Sanic, Spikard | Native interface |

### Category 3: Framework Overhead

| Test | Frameworks | Server |
|------|------------|--------|
| hello_c64 | Raw, Starlette, FastAPI, BlackSheep, Litestar | Uvicorn httptools |
| json_1k | Raw, Starlette, FastAPI, BlackSheep, Litestar | Uvicorn httptools |
| validation | FastAPI, Litestar | Compare validation overhead |

---

## References

- [Socketify Docs](https://docs.socketify.dev/)
- [Robyn Documentation](https://robyn.tech/)
- [BlackSheep Docs](https://www.neoteroi.dev/blacksheep/)
- [Sanic User Guide](https://sanic.dev/)
- [Starlette Docs](https://www.starlette.io/)
- [FastAPI Benchmarks](https://fastapi.tiangolo.com/benchmarks/)
- [Granian Benchmarks](https://github.com/emmett-framework/granian/tree/master/benchmarks)
- [TechEmpower Benchmarks](https://www.techempower.com/benchmarks/)
