# Flow Spec: Native HTTP Server - H2 & TLS
## Goal
Implement Hyper-based HTTP/2 server with ALPN and TLS.

## Workflow Requirements
- **GitHub Issues**: Task 0 is to open a focused GitHub issue for this feature.
- **Pull Requests**: All changes MUST be submitted via GitHub Pull Requests.

## Plan
### Phase 1: Hyper Server
- [ ] Task 0: Open GitHub issue for "Hyper-based HTTP/2 Server Implementation".
- [ ] Task 1: Implement `src/rs/vertebra-http/src/server.rs`. Set up Hyper connection handler.
- [ ] Task 2: Add connection-level state tracking.

### Phase 2: Security
- [ ] Task 3: Implement `src/rs/vertebra-http/src/tls.rs` using `rustls`.
- [ ] Task 4: Add ALPN negotiation for `h2` and `http/1.1`.
