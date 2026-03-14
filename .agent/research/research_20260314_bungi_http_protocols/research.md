# Research: Bungi HTTP/2, h2c, Negotiation, and HTTP/3/QUIC Options

## Executive Summary

- `bungi` already has real cleartext HTTP/2 support in Zig:
  - prior-knowledge `h2c` via the HTTP/2 client preface
  - HTTP/1.1 `Upgrade: h2c`
  - stream multiplexing via `nghttp2`
- It does not have real HTTP/2-over-TLS auto-negotiation today. TLS config exists in the FFI and CLI, but the server still creates a non-SSL uSockets context and never performs ALPN-driven protocol selection.
- The current `h2c` path is still prototype-grade, not production-grade. The most important gaps are:
  - likely broken response handling for the initial upgraded request on stream 1
  - fragile prior-knowledge detection when the HTTP/2 preface is split across TCP reads
  - non-RFC-robust upgrade validation and case-sensitive header matching
  - incomplete HTTP/2 response support for streaming bodies and trailers
- If the goal is to improve protocol support, the next step is not HTTP/3 first. The next step is:
  1. finish correctness on the existing `h2c` implementation
  2. wire real TLS
  3. add ALPN-based `h2` / `http/1.1` negotiation
- If the goal is HTTP/3/QUIC, there are viable options in 2026, but they are effectively a new transport project. The lowest-risk near-term option is to terminate HTTP/3 at a proxy and keep `bungi` as an HTTP/2 or HTTP/1.1 backend.

## Codebase Analysis

### Current protocol model

The active Zig server is centered in:

- `src/zig/src/server.zig`
- `src/zig/src/http.zig`
- `src/zig/src/h2.zig`
- `src/zig/src/usockets.zig`
- `src/zig/src/ffi.zig`

The connection state machine is simple:

- every socket starts as `unknown`
- on first bytes:
  - if the full HTTP/2 client preface is present, switch to `h2c`
  - otherwise switch to HTTP/1.1
- while in HTTP/1.1:
  - parse requests with `hparse`
  - detect `Upgrade: h2c`
  - detect `Upgrade: websocket`
- while in `h2c`:
  - feed frames into `nghttp2`
  - enqueue completed streams back into the shared batch queue

This is structurally sound for a prototype because it reuses one batch queue and one Python wakeup model across HTTP/1.1, h2c, and WebSocket.

### What works today

`bungi` does already implement:

- raw HTTP/1.1 request parsing in `http.zig`
- h2c prior knowledge detection through the HTTP/2 preface in `http.zig`
- h2c upgrade handling in `server.zig`
- HTTP/2 session and stream state in `h2.zig`
- HTTP/2 multiplexing tests in `src/py/tests/integration/server/test_h2_multiplexing.py`

Branch history also shows this work was deliberate and fairly deep:

- `20aea4f` `feat(h2c): implement HTTP/1.1 Upgrade: h2c mechanism`
- `fb2426c` `feat(h2): HTTP/2 stream multiplexing with flow control`
- `3355dc2` `feat(h2): static nghttp2 build, enable HTTP/2 by default, fix H2 header lifetime bug`

### What is missing or incomplete

#### 1. TLS is not actually wired through the server path

The public FFI exposes TLS configuration in `bungi_server_set_tls()`, but `bungi_server_listen()` never passes TLS state into `server.start()`, and `server.start()` still creates the socket context with `ssl=0` and empty options.

That means:

- the CLI TLS flags are not a real HTTP transport feature yet
- there is no HTTP/2-over-TLS path
- there is no ALPN negotiation path
- `scheme=https` and TLS metadata are not coming from a real negotiated SSL transport in the active server

This is the biggest architectural gap because proper HTTP/2 auto-negotiation depends on TLS+ALPN, not `h2c`.

#### 2. The `h2c` upgrade path has correctness risk on stream 1

The upgrade path in `server.zig`:

- sends `101 Switching Protocols`
- enqueues the initial HTTP/1.1 request as stream 1
- calls `H2Session.initUpgrade()`

But `H2Session.initUpgrade()` passes `null` stream user data to `nghttp2_session_upgrade2(...)`, while later response handling expects stream user data to exist for the response start/body buffering path.

That suggests the server may dispatch the upgraded request into Python but fail to send the corresponding HTTP/2 response on stream 1.

The existing integration test only verifies:

- the `101`
- that the app ran

It does not verify the stream-1 HTTP/2 response body itself.

#### 3. Prior-knowledge detection is too eager

`isH2Preface()` only succeeds when the first read already contains the full 24-byte preface. If the preface arrives split across packets, the server falls back to HTTP/1.1 permanently.

That makes direct h2c prior knowledge brittle on real networks.

#### 4. Upgrade validation is weaker than the WebSocket validation path

The WebSocket upgrade helper is more robust than the h2c helper:

- it validates method and multiple headers
- it handles tokenized `Connection` values
- it is case-insensitive where needed

The h2c helper only checks `Upgrade: h2c` and later assumes `HTTP2-Settings` exists. It does not robustly validate the `Connection` tokens and uses case-sensitive header lookup.

This is fixable and should be aligned with the WebSocket approach.

#### 5. HTTP/2 response support is incomplete

`HTTP/2` request intake is substantially ahead of the response path.

Current limitations:

- `more_body=true` for HTTP/2 is a TODO and effectively ignored
- HTTP/2 trailers are not implemented in the exported API
- file sending falls back to generic read/write behavior
- graceful shutdown and GOAWAY behavior are not surfaced as a transport policy

So the server can speak HTTP/2, but it does not yet offer full ASGI-style streaming parity on that transport.

#### 6. Negotiated protocol details do not consistently reach Python

The lower-level Zig Python bridge still hardcodes HTTP/1.1-like defaults in places, which means protocol metadata can be flattened away on some execution paths.

What is missing in practice:

- a reliable `http_version == "2"` path everywhere
- `scheme == "https"` from real TLS state
- negotiated ALPN visibility
- a clean transport extension for protocol metadata

## Library Documentation

### RFC direction

The standards direction is clear:

- RFC 9113 marks the `h2c` token as cleartext HTTP/2 and marks the HTTP/1.1 Upgrade-based startup mechanism obsolete.
- RFC 9113 also keeps HTTP/2 priority intentionally unimportant for new implementations, which lines up with `bungi` leaving stream priority unimplemented.
- RFC 9114 defines HTTP/3 over QUIC, not over TCP, and deployments typically advertise it via `Alt-Svc` while negotiation uses `h3` ALPN identifiers on the QUIC/TLS side.

That means:

- `h2c` is fine for internal or dev scenarios, but it is the wrong primary negotiation strategy for a public server
- real public negotiation should be TLS + ALPN
- HTTP/3 is not an incremental extension of the current TCP/uSockets path

### nghttp2

`bungi` already chose a sensible HTTP/2 core dependency:

- `nghttp2` is mature
- `build.zig` already vendors and statically builds it
- the build already includes `nghttp2_alpn.c`

That last point matters: the dependency set already contains the helper needed for ALPN selection, but the server path does not use it because TLS is not wired.

The same project also ships `nghttpx`, which is relevant because it can terminate HTTP/3, HTTP/2, and HTTP/1.1 on the frontend and proxy to backends.

### uSockets

The current transport abstraction is still effectively TCP-only in this codebase.

The Zig wrapper only uses the non-SSL convenience path and does not expose a negotiated ALPN flow. The upstream uSockets project also documents QUIC support as work in progress, which is not a good foundation for betting `bungi`'s protocol roadmap on native HTTP/3 inside this server today.

### HTTP/3 / QUIC options in 2026

There are several realistic options, but they are not equivalent.

#### Option 1: `ngtcp2` + `nghttp3`

This is the closest conceptual fit to the current stack.

- `ngtcp2` is the QUIC transport library
- `nghttp3` is the HTTP/3 layer
- both sit naturally next to an existing `nghttp2`-based design

This is the most coherent native path if the plan is "stay in Zig/C land and own the transport."

#### Option 2: `quiche`

`quiche` is a mature QUIC + HTTP/3 stack from Cloudflare with a C API. It is viable, but it would pull `bungi` further toward a Rust-owned transport surface and away from the current uSockets/TCP design.

#### Option 3: `MsQuic`

`MsQuic` is a mature cross-platform QUIC transport. It is transport-only, so pairing it with HTTP/3 semantics is a larger integration job.

#### Option 4: Python-first prototyping with `aioquic` or `qh3`

If the main goal is to explore HTTP/3 semantics quickly from the Python host side rather than to replace the Zig transport immediately:

- `aioquic` is the established Python QUIC/HTTP/3 implementation
- `qh3` exists as a maintained fork, but its own project page says server-side code is maintained without active new feature development

So for Python-first experimentation:

- `aioquic` is the stronger prototyping choice
- `qh3` is acceptable for compatibility or maintenance contexts, but it is not where I would place the main future bet

## Prior Art

### What landed vs what stayed branch-only

`main` contains the early protocol pivot:

- `77eb7dc` `feat(zig): ASGI backend with Zig networking core (#5)`
- `a4b424a` `feat(asgi): HTTP/2 multiplexing, TLS client certs, and test fixes (#8)`

But the fuller protocol stack lives on `feat/asgi-wsgi-2`, which is still ahead of `main`.

That branch contains:

- explicit h2c upgrade
- HTTP/2 multiplexing and flow control
- WSGI support
- WebSocket lifecycle and fast path
- SSE
- pub/sub
- HMR
- static nghttp2 vendoring

So the branch history says:

- the protocol direction was ambitious and technically serious
- the code already explored most of the right ideas
- the remaining gaps are in transport completion and negotiation correctness, not in whether the architecture ever thought about them

## Risk Assessment

### Low to medium risk

- Fixing h2c correctness
- Hardening header parsing and upgrade validation
- Making prior-knowledge detection incremental
- Surfacing better protocol metadata into Python scopes

These are straightforward improvements to the existing design.

### Medium risk

- Wiring real TLS into the Zig server path
- Adding ALPN-driven `h2` vs `http/1.1` negotiation

This is still tractable because the code already has:

- TLS-shaped config types
- uSockets SSL-capable C APIs
- `nghttp2` vendored

But it requires a real refactor, not just toggling a flag.

### High risk

- Native HTTP/3/QUIC inside `bungi`

Why it is high risk:

- QUIC is UDP-based, not TCP-based
- connection lifecycle and flow control are different
- TLS 1.3 and ALPN are embedded into the QUIC handshake model
- current batching, wakeup, and per-connection socket assumptions are TCP-centric

This is a new transport subsystem, not "more HTTP/2 work."

## Recommended Approach

### 1. Finish HTTP/2 correctness before adding new protocols

Do this first:

- fix stream-1 state on h2c upgrade
- add an integration test that verifies the actual upgraded stream-1 response body
- make prior-knowledge preface detection incremental
- make h2c upgrade validation case-insensitive and token-aware
- surface `http_version`, `scheme`, and protocol metadata cleanly into Python
- implement real HTTP/2 streaming for `more_body=true`
- define the trailer story for HTTP/2

Without these, more negotiation paths will only multiply edge cases.

### 2. Make ALPN the real auto-negotiation path

If "auto-negotiation" is a priority, this is the correct target:

- HTTPS listener
- ALPN advertises `h2` and `http/1.1`
- on TLS connections:
  - select `h2` when offered
  - otherwise fall back to HTTP/1.1
- keep `h2c` available only for:
  - internal deployments
  - local development
  - explicit cleartext gRPC-style usage

I would not market or architect `h2c` Upgrade as the primary modern negotiation path.

### 3. Use a proxy for HTTP/3 first

If you want HTTP/3 soon, the practical path is:

- terminate HTTP/3 at `nghttpx` or another edge proxy
- proxy to `bungi` over HTTP/2 or HTTP/1.1

Why this is the right first move:

- you get real H3/QUIC at the edge
- you preserve the current Python/Zig application architecture
- you avoid rewriting the transport layer before the application semantics are finished

### 4. If native HTTP/3 becomes a real goal, choose a deliberate stack

If you later decide to bring QUIC into-process:

- best native fit: `ngtcp2` + `nghttp3`
- strongest Rust-leaning option: `quiche`
- transport-focused option: `MsQuic` plus an HTTP/3 layer
- fastest Python-hosted experiment: `aioquic`

I would not recommend:

- hand-rolling QUIC
- waiting on upstream uSockets QUIC to define the whole roadmap

## Bottom Line

`bungi` is closest to a good HTTP/2 server than to an HTTP/3 server.

The architectural move that makes the most sense is:

- finish the existing `h2c` / h2 implementation
- wire real TLS
- add ALPN-based negotiation
- keep h2c as secondary
- use a proxy for HTTP/3 first

That gets you a credible modern transport story without taking on a full QUIC rewrite too early.

## Sources

- RFC 9113 HTTP/2: https://datatracker.ietf.org/doc/html/rfc9113
- RFC 9114 HTTP/3: https://datatracker.ietf.org/doc/html/rfc9114
- nghttp2 project: https://github.com/nghttp2/nghttp2
- ngtcp2 project: https://github.com/ngtcp2/ngtcp2
- nghttp3 project: https://github.com/ngtcp2/nghttp3
- uSockets project: https://github.com/uNetworking/uSockets
- aioquic project: https://github.com/aiortc/aioquic
- qh3 project page: https://pypi.org/project/qh3/
- quiche project: https://github.com/cloudflare/quiche
- MsQuic project: https://github.com/microsoft/msquic
