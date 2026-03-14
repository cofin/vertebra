# Flow Spec: HTTP/3 & QUIC Transport
## Goal
Implement native HTTP/3 support via ngtcp2/nghttp3.

## Workflow Requirements
- **GitHub Issues**: Task 0 is to open a focused GitHub issue for this feature.
- **Pull Requests**: All changes MUST be submitted via GitHub Pull Requests.

## Plan
### Phase 1: UDP Listener
- [ ] Task 0: Open GitHub issue for "QUIC Transport and HTTP/3 support".
- [ ] Task 1: Implement `src/rs/vertebra-http/src/h3/server.rs` with `tokio::net::UdpSocket`.
- [ ] Task 2: Add QUIC packet dispatch logic.

### Phase 2: H3 Protocol
- [ ] Task 3: Implement `src/rs/vertebra-http/src/h3/transport.rs` wrapping `ngtcp2`.
- [ ] Task 4: Implement H3 stream handling and translation to `BatchNode`.
