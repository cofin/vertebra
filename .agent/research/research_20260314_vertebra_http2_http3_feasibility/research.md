# Research: Vertebra HTTP/2 and HTTP/3 Feasibility

## Executive Summary

- Yes, there is meaningful Rust work we can do in Vertebra right now.
- HTTP/2 is feasible without replacing the whole server, because `vertebra-server` already uses Hyper's auto connection builder and already preserves request version into the ASGI payload.
- Proper browser-grade HTTP/2 does not exist today in Vertebra because the active server has no TLS, no ALPN, and the worker bridge still buffers each request and response into a single IPC exchange.
- HTTP/3 is feasible in Rust, but it is not an incremental patch on top of the current worker bridge. It needs a separate QUIC transport layer and is a better fit after the backend contract is stream-aware.
- The useful multiplexing lesson from prior art is not "reimplement HTTP/2 frames ourselves." It is "keep stream state and backpressure at the Rust transport edge, and only send app-visible events across the language boundary."

## Codebase Analysis

### What Vertebra already has

- `vertebra-server` already uses `hyper_util::server::conn::auto::Builder`, so the Rust edge is not locked to an HTTP/1 parser:
  - `src/rs/vertebra-server/src/lib.rs:177`
- The current IPC protocol already has correlation IDs specifically for multiplexing:
  - `src/rs/vertebra-core/src/protocol/mod.rs:13`
- Worker dispatch is already concurrency-friendly:
  - `WorkerConnection` keeps a pending map keyed by request ID.
  - Request sending uses an MPMC ring plus notifier-driven wakeups.
  - Least-pending dispatch already exists.
  - `src/rs/vertebra-server/src/worker.rs:17`
  - `src/rs/vertebra-server/src/worker.rs:67`
- `vertebra-engine` already has the beginnings of a stream-aware execution model:
  - `ResponseBody` streams body frames through an `mpsc::Receiver`.
  - `ZeroCopyBody` exposes ASGI `receive()` semantics over Hyper `Incoming`.
  - `DirectExecutor` already maps `HTTP_2` and `HTTP_3` into ASGI scope strings.
  - `src/rs/vertebra-engine/src/executor.rs:23`
  - `src/rs/vertebra-engine/src/executor.rs:57`
  - `src/rs/vertebra-engine/src/executor.rs:268`
  - `src/rs/vertebra-engine/src/executor.rs:324`

### What blocks "proper" HTTP/2 today

- The active server is plain TCP only. There is no TLS acceptor, ALPN, or QUIC stack:
  - `src/rs/vertebra-server/src/lib.rs:154`
  - `src/rs/vertebra-server/Cargo.toml:6`
  - `src/rs/vertebra-engine/Cargo.toml:6`
- The worker bridge collects the full request body before dispatch:
  - `src/rs/vertebra-server/src/bridge.rs:63`
- The worker bridge also reconstructs a single buffered response body:
  - `src/rs/vertebra-server/src/bridge.rs:156`
- On the Python side, managed mode explicitly aggregates ASGI frames into one `AsgiResponse` before sending back to Rust:
  - `src/py/vertebra/core/worker.py:247`
  - `src/py/vertebra/core/worker.py:291`
- That means current multiplexing is only request correlation, not transport-stream preservation. Multiple requests can be in flight at once, but each one is still flattened into one inbound blob and one outbound blob.

## Library Documentation

### HTTP/2 in Rust

- Hyper and `hyper-util` already provide the right server-side primitives for HTTP/1.1 and HTTP/2:
  - `hyper-util::server::conn::auto::Builder`
  - Hyper's HTTP/2 server connection module
- That means the shortest path is not building an H2 state machine ourselves. It is adding TLS termination plus ALPN and then letting Hyper keep owning stream scheduling, HPACK, flow control, and connection state.

Official docs:
- https://docs.rs/hyper-util/latest/hyper_util/server/conn/auto/struct.Builder.html
- https://docs.rs/hyper/latest/hyper/server/conn/http2/index.html

### TLS and ALPN

- `tokio-rustls` is the obvious async TLS layer for the current Tokio server.
- `rustls` exposes ALPN support, which is the missing piece for real browser-facing HTTP/2 negotiation.

Official docs:
- https://docs.rs/tokio-rustls/latest/tokio_rustls/
- https://docs.rs/rustls/latest/rustls/manual/_04_features/index.html

### HTTP/3 in Rust

- Rust has viable QUIC/H3 building blocks, but they are not part of Hyper's server surface.
- `quinn` provides the QUIC transport layer.
- `h3` provides HTTP/3 on top of QUIC.
- This strongly suggests HTTP/3 should be treated as a separate transport implementation, not a tiny extension to `vertebra-server`.

Official docs:
- https://docs.rs/quinn/latest/quinn/
- https://docs.rs/h3/latest/h3/

## Prior Art

### What we can learn from Bungi

- Keep protocol state at the transport edge.
- Keep one dispatch path for all protocols.
- Treat long-lived and streaming protocols as queue-backed sessions, not single buffered messages.

Those ideas transfer well.

### What does not transfer cleanly

- Bungi's actual HTTP/2 machinery is built around native per-connection session state, per-stream state, reset handling, and upgrade handling.
- Vertebra currently loses that granularity as soon as Hyper yields a request object and the bridge turns it into one `AsgiRequest`.

So the transferable lesson is architectural:
- keep multiplexing and backpressure in Rust
- send only app-level events to Python
- avoid rebuilding protocol logic in Python or in the IPC bridge

## Risk Assessment

### Low-risk path

- Add TLS termination to `vertebra-server`.
- Add ALPN negotiation for `h2` vs `http/1.1`.
- Preserve negotiated scheme/TLS metadata in the ASGI scope.
- Keep the current buffered worker bridge for an initial H2-compatible mode.

Outcome:
- browsers and clients can use HTTP/2
- concurrency per TCP connection improves
- but request/response streaming still does not fully exploit H2

### Medium-risk path

- Move the "real" server path toward `vertebra-engine` semantics:
  - stream request bodies from Hyper into ASGI `receive()`
  - stream response frames back to Hyper incrementally
- If the worker model remains, redesign the IPC protocol around stream frames instead of one `AsgiRequest` / one `AsgiResponse`.

Outcome:
- this is where HTTP/2 becomes architecturally meaningful rather than just transport-compatible

### High-risk path

- Add native HTTP/3 before the backend contract is stream-aware.

Risk:
- QUIC complexity lands before the worker boundary can express reset, cancellation, trailers, or incremental backpressure
- most of the protocol advantage gets flattened away

## Recommended Approach

### 1. Do in Rust now

- Add `tokio-rustls` + `rustls` to `vertebra-server`.
- Build an HTTPS listener with ALPN for `h2` and `http/1.1`.
- Carry `scheme`, TLS, and negotiated protocol metadata into the scope instead of hardcoding `scheme = "http"`.
- Add HTTP/2 tests for:
  - parallel requests on one connection
  - streamed request body
  - streamed response body
  - cancellation / client disconnect behavior

### 2. Learn the multiplexing work, but only the right layer

What to learn:
- per-connection state stays in Rust
- stream IDs and flow control stay in Rust
- Python sees request/response events, not frame machinery

What not to copy:
- hand-rolled HTTP/2 parsing when Hyper already provides it
- pushing transport-stream concerns across the current whole-message worker bridge

### 3. Decide between two backend shapes

Option A: buffered worker H2
- least disruptive
- enough if the goal is compatibility, browser reuse, and some connection-level wins

Option B: stream-aware backend
- more work
- required if the goal is "proper multiplexing" and a serious HTTP/3 future

### 4. Defer HTTP/3 until after the backend contract is fixed

- Once the backend can express streamed bodies and cancellation, adding an H3 transport becomes much more coherent.
- Before that point, the lowest-risk H3 story is still edge termination or a sidecar.

## Bottom Line

Rust can absolutely move Vertebra forward here.

The immediate value is:
- real TLS + ALPN
- real HTTP/2 negotiation
- better metadata plumbing
- keeping request concurrency and transport state in Rust

The bigger lesson from the multiplexing work is also usable:
- not by porting Bungi's frame machinery,
- but by making Vertebra's Rust edge own streams and backpressure while Python stays at the ASGI event layer.

If we do that, HTTP/2 is practical now and HTTP/3 becomes realistic later. If we do not, HTTP/2 can still be exposed, but it will remain mostly a transport wrapper around a buffered request/response bridge.
