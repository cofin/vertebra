# Research: Why Litecore ASGI and Native Lagged WSGI

## Executive Summary

- The first important finding is that `litecore` has at least three materially different "native" stories:
  - raw native batch in `tools/benchmarks/apps/litecore_http_runner.py`
  - framework-native via `Litecore(...).run()` in `tools/benchmarks/apps/litecore_framework_runner.py`
  - ASGI and WSGI adapters over the same Rust HTTP core
- Those should not be treated as one number. The repo itself records:
  - early recovery baseline around February 7-8, 2026: native `179,827`, ASGI `25,328`, WSGI `~25,000`
  - post-recovery gate on February 8, 2026: ASGI `87,460`, WSGI `198,047`, native `169,266`
  - stricter gate learnings later on February 8, 2026: ASGI `88,382`, WSGI `194,124`, native `167,608`
  - separate framework-native work later states raw native `~127k` while framework mode was only `~21k`
- ASGI stayed well below WSGI for structural reasons. Even after heavy optimization, ASGI still has to honor the ASGI protocol shape: per-request `scope`, `receive`, and `send`, coroutine scheduling, and dict-based message passing. WSGI is a single synchronous Python call with `environ` + `start_response`, and Litecore specifically built a highly optimized WSGI executor around that simpler contract.
- WSGI is faster than expected not because WSGI is intrinsically better than ASGI, but because Litecore gave WSGI a very specialized execution engine:
  - `WsgiEnviron` lazy mapping
  - `WsgiInput` instead of `BytesIO`
  - dedicated worker pool instead of `spawn_blocking`
  - lock-free response state instead of `Arc<Mutex<_>>`
- Native batch did not get an equally specialized fast path for every layer. It still pays request decomposition copies, Python object creation for `PyRequest`, batch collector wake policy, header/string conversion, and response extraction/reassembly. The repo's own remaining-work notes for the native path explicitly call out `extract_body_bytes`, request decomposition copies, batch dispatch timing, and response assembly as the follow-up bottlenecks.
- If your remembered "native ~90k" refers to the full `Litecore(...)` framework path rather than raw `App().route_batch(...)`, that gap is mostly Python framework overhead: middleware, route matching, guard execution, dependency injection, response normalization, and an event-loop-driven batch runner in `_LitecoreRunner`.

## What "Native" Means Here

`litecore` uses "native" in two different senses, and the benchmark discussion is confusing if those are not separated.

### 1. Raw native batch transport

This is the low-level Rust HTTP server exposed through:

- `litecore.server.App`
- `litecore.server.serve()`
- the benchmark target in `tools/benchmarks/apps/litecore_http_runner.py`

Its contract is a synchronous batch bridge:

- Rust receives HTTP requests
- Rust groups requests into a batch
- Python receives `Sequence[Request]`
- Python returns `Sequence[ReplyLike]`

The Python binding makes that explicit in `src/py/litecore/server/_native.py`:

- `Handler = Callable[[Sequence[Request]], Sequence[ReplyLike]]`
- `_build_bridge()` calls the handler directly and immediately normalizes the replies
- there is no await/async handling in this low-level bridge

So the raw native transport is a synchronous Python callback API over a native Rust HTTP/batching core.

### 2. Framework-native mode

This is `Litecore(...).run()` in `src/py/litecore/framework/app/_app.py`, benchmarked by `tools/benchmarks/apps/litecore_framework_runner.py`.

That path still serves through the same raw native Rust transport, but it adds the full Litecore framework on top:

- middleware
- route lookup
- guards
- dependency injection
- response normalization
- mount bridging for ASGI/WSGI plugins

The framework-native bridge uses `_LitecoreRunner`, which synchronously satisfies the raw native batch contract by running an internal event loop and calling `handle_one()` across the batch with `asyncio.gather()`.

### 3. ASGI and WSGI are adapters, not the native path

ASGI and WSGI use the same Rust HTTP core, but not the same Python contract:

- ASGI: async app with `scope`, `receive`, `send`
- WSGI: sync app with `environ`, `start_response`

So "native vs ASGI vs WSGI" is really "three different Python-side execution models over the same Rust transport layer."

## Does Native Support Both Sync And Async?

Yes, but with an important qualification.

### Raw native batch API: sync only

The low-level native API in `src/py/litecore/server/_native.py` is sync-only:

- `Handler` is typed as `Callable[[Sequence[Request]], Sequence[ReplyLike]]`
- `_build_bridge()` just does `replies = handler(requests)`
- there is no `inspect.isawaitable()` or event-loop handoff in that layer

So `App().route_batch(...)` and `serve(...)` expect a synchronous Python batch handler.

### Framework-native mode: supports both sync and async handlers

The higher-level framework-native mode does support both sync and async route handlers.

In `src/py/litecore/framework/app/_app.py`:

- `_LitecoreRunner.__call__()` runs an event loop and batches `self._app.handle_one(r)` via `asyncio.gather()`
- `_handle_one_raw()` invokes the route handler and then does:
  - `result = route.handler(**kwargs)`
  - `if inspect.isawaitable(result): result = await result`

That means a route used under `Litecore(...).run()` can be:

- synchronous
- asynchronous

and the framework-native runner will normalize both through the same native Rust transport underneath.

So the most accurate statement is:

- raw native batch transport: sync-only Python handler contract
- framework-native serving on top of native transport: supports both sync and async route handlers

## Codebase Analysis

### 1. The benchmark targets are not equivalent

The benchmark harness defines separate targets for:

- `litecore-http` → raw batch handler: `tools/benchmarks/apps/litecore_http_runner.py`
- `litecore-asgi` → ASGI adapter: `tools/benchmarks/apps/litecore_asgi_runner.py`
- `litecore-wsgi` → WSGI adapter: `tools/benchmarks/apps/litecore_wsgi_runner.py`
- `litecore-framework` → full Python framework mode: `tools/benchmarks/apps/litecore_framework_runner.py`

This matters because the raw native runner is not the same code path as the framework-native app.

### 2. Why ASGI stayed below WSGI

The repo's own perf-recovery PRD already explains the core asymmetry:

- ASGI requires:
  - per-request `scope`
  - per-request `receive` and `send`
  - coroutine scheduling on a persistent event loop
  - dict-based protocol messages
- WSGI requires:
  - one synchronous application call
  - one `environ`
  - one `start_response`
  - an iterable response body

Litecore's current ASGI implementation in `src/rs/litecore-http/src/asgi.rs` is already heavily optimized:

- callback scheduler instead of `spawn_blocking`
- persistent event loop thread
- `PyIterAwaitable`
- Rust-backed `AsgiScope` mapping
- batch scheduling support

But it still cannot remove the protocol's core tax:

- `AsgiScope` is still a Python mapping object with lazy lookups
- the app is still an async callable
- `receive()` and `send()` are still Python-callable boundaries
- messages are still shaped as ASGI dict payloads

The perf-recovery ASGI spec says this directly: native batch is faster because it has one `Python::attach` per batch, minimal Python object creation, and no asyncio machinery, while ASGI still has multiple Python calls and protocol objects per request.

Bottom line:

- ASGI is slower because Litecore is paying real ASGI protocol compliance costs, not because the Rust HTTP core is slow.
- WSGI is faster because Litecore successfully cut away most of WSGI's historical overhead while the ASGI protocol still forces more moving parts.

### 3. Why WSGI got so fast

The current WSGI implementation in `src/rs/litecore-http/src/wsgi.rs` and `src/rs/litecore-http/src/wsgi_worker.rs` is unusually specialized.

The file-level comments describe the exact wins:

- no per-request environ dict construction
- no `BytesIO` allocation
- no `spawn_blocking`
- no `Arc<Mutex<_>>`
- per-thread Python caches

The critical architectural change is that WSGI does not go through generic tokio blocking dispatch anymore. It is handed to a fixed worker pool of OS threads, and each worker calls the WSGI app inside `Python::attach`, using a Rust-backed `WsgiEnviron` mapping and lock-free response state.

That means the WSGI fast path is:

- one request struct
- one dispatch to a dedicated worker
- one synchronous Python call
- direct response start/body streaming back to Rust

Compared with ASGI, this avoids coroutine task management and extra protocol call sites.

Compared with native batch, it also avoids some of the batch-bridge marshalling complexity described below.

### 4. Why raw native batch still left performance on the table

The raw native batch path lives in `src/rs/litecore-http/src/lib.rs`.

The key hot path is:

- `handle_request()`
- queueing into the batch collector
- `dispatch_batch()`
- `build_batch_replies()`
- `build_response_from_py()`
- `extract_body_bytes()`

The repo already identified the remaining native bottlenecks in the follow-up notes after the main perf recovery:

- `extract_body_bytes`
- priority fast-path split
- batch dispatch timing
- request decomposition copies
- response assembly overhead

The native-optimization learnings from February 2, 2026, also recorded flamegraph hotspots in:

- `_http::handle_request`
- `_http::build_batch_replies`
- `PyTuple` reply creation
- `_http::extract_body_bytes`
- header parsing

Looking at the current code, those notes still make sense:

1. Request decomposition still performs real allocation work.
   - `handle_request()` converts request path and query into owned `String`s.
   - headers are cloned out of `HeaderMap`.
   - the request body is materialized into `Bytes`.

2. `build_batch_replies()` still creates one Python `PyRequest` object per request.
   - each object owns `method`, `path`, `query`, `body`, `headers`, and optional route/query-param objects.
   - headers are converted into `Vec<(String, String)>`
   - optional route matching and query parsing happen before Python dispatch when a native router is present

3. Response extraction is still generic and expensive.
   - `build_response_from_py()` accepts either raw bytes or `(status, headers, body)`
   - headers are extracted as `Vec<(String, String)>`
   - body extraction falls through `Bytes`, buffer-protocol copy, or `Vec<u8>`
   - content-length and header reconstruction happen per response

4. The batcher itself can leave some headroom on the table.
   - `batch_collector_loop()` uses `try_recv()` draining plus a wait window
   - the remaining-work notes explicitly call out batch wake policy and dispatch timing as unresolved

5. The native benchmark API shape is not as cheap as the optimized WSGI shape.
   - WSGI app: `app(environ, start_response)` returning an iterable
   - native batch app: Python loop over `Request` objects, path branching in Python, and a returned list of tuples to be parsed back into Rust

That means raw native batch is not "just the Rust HTTP server." It is still a general Python bridge with a fairly generic response contract.

### 5. Why framework-native can be much lower than raw native batch

If your "native ~90k" number refers to framework-native rather than raw `litecore-http`, the explanation is mostly in `src/py/litecore/framework/app/_app.py`.

The framework-native path does all of this on top of the raw native transport:

- request middleware chain
- guard execution
- route lookup
- path-param/query-param handling
- dependency resolution
- route invocation
- response normalization/encoding

The framework perf PRD documents the gap clearly:

- raw native around `~127k`
- framework-native around `~21k` before the dedicated optimization pass

Even after those optimizations, framework-native still has intrinsic overheads that raw WSGI does not:

1. `_LitecoreRunner.__call__()` runs an event loop and uses `asyncio.gather()` over the batch.
   - That means framework-native is still carrying async orchestration overhead even for very simple benchmark routes.

2. `_handle_one_raw()` still does guard compilation/execution and route dispatch work.

3. `_resolve_call_kwargs()` still resolves providers and builds kwargs dictionaries.

4. `_normalize_response()` still normalizes Python handler returns into `(status, headers, body)`.

5. Even though routing/DI/serialization were optimized later, the framework stack is still deeper than the plain WSGI benchmark app.

So the framework-native answer to "why isn't it closer to WSGI?" is straightforward:

- because it is not competing with WSGI on equal semantic terms
- it is competing with a much fatter Python framework surface on top of the same transport

## Library Documentation

Two official protocol references help explain why ASGI and WSGI diverge even when the underlying Rust server is identical:

- ASGI HTTP interface: the application is an async callable receiving `scope`, `receive`, and `send`.
  - Official source: https://asgi.readthedocs.io/en/latest/specs/main.html
- WSGI (PEP 3333): the application is a synchronous callable receiving `environ` and `start_response`, and returns an iterable.
  - Official source: https://peps.python.org/pep-3333/

Why that matters here:

- ASGI requires protocol objects and async scheduling even for trivial handlers.
- WSGI is a much simpler synchronous contract, and Litecore exploited that simplicity very aggressively in its optimized executor.

## Prior Art

### Performance-recovery PRD on February 7-8, 2026

The repo's most useful internal reference is:

- `.agent/archive/perf-recovery-prd_20260208/spec.md`

It records the before-and-after arc:

- baseline:
  - native `179,827`
  - ASGI `25,328`
  - WSGI `~25,000`
- later gate:
  - ASGI `87,460`
  - WSGI `198,047`
  - native `169,266`

That PRD is strong evidence that:

- the Rust core was never the bottleneck
- the gains came from protocol-bridge redesign
- WSGI ended up with the most successful bridge rewrite

### Native follow-up notes

The same PRD leaves one unresolved native item:

- strict native single-process `>=170k` remained slightly short
- recommended follow-up work:
  - profile `extract_body_bytes`
  - profile priority fast-path split
  - inspect batch dispatch timing
  - inspect request decomposition copies and response assembly

That is effectively the repo's own answer to "why didn't native go higher?"

### Framework-native follow-up PRD

The framework performance PRD later confirms that using the native transport does not automatically make the full Python framework fast:

- raw native `~127k`
- full framework-native `~21k` before targeted Python-side optimizations

That means any discussion of "native" must distinguish transport-native from framework-native.

## Risk Assessment

- The biggest analysis risk is mixing unlike benchmarks:
  - raw native batch
  - framework-native
  - ASGI adapter
  - WSGI adapter
  These do not measure the same semantic workload.
- The current codebase contains both protocol-level optimizations and framework-level optimizations from different phases. It is easy to read one branch of work and miss the other.
- Perf artifacts in the repo also show variance across dates and gates, so any exact number should be treated as a snapshot, not a timeless constant.

## Recommended Approach

### If the goal is "Why is ASGI below WSGI?"

Treat the gap as mostly structural and only partly fixable:

- ASGI will keep paying for `scope` + `receive` + `send` + coroutine scheduling
- WSGI has a cheaper protocol contract and Litecore already exploited that hard

The realistic goal is not necessarily "make ASGI equal WSGI," but:

- keep the protocol bridge lean
- add fast paths for simple non-streaming responses
- avoid feature regressions in the ASGI hot path

### If the goal is "Why is native below WSGI?"

Split the answer in two:

1. Raw native batch:
   - remaining bottlenecks are request decomposition, Python request-object creation, response extraction, and batch wake policy
   - optimize `build_batch_replies()`, `extract_body_bytes()`, and response assembly before chasing bigger architecture changes

2. Framework-native:
   - the missing performance is mostly in the Python framework stack, not the Rust transport
   - the strongest remaining opportunities are:
     - reduce event-loop overhead in `_LitecoreRunner`
     - shrink middleware and guard overhead further
     - keep native route-id dispatch as the default, not Python fallback routing
     - continue specializing DI and response encoding for common cases

### Practical conclusion

The codebase points to this hierarchy:

- fastest: highly specialized WSGI executor
- next: raw native batch when the bridge payload is simple
- then: ASGI, because protocol semantics are heavier
- slowest: full framework-native unless the framework layers are aggressively specialized

So the answer to "why aren't ASGI and native closer to WSGI?" is:

- ASGI: because the ASGI protocol itself is heavier
- native batch: because Litecore's batch bridge is still generic and marshalling-heavy
- framework-native: because the Python framework stack dominates once you move beyond the transport fast path
