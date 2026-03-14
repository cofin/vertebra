# Flow Spec: Data Plane Bypass
## Goal
Implement SHM-to-Socket direct streaming for bodies.

## Workflow Requirements
- **GitHub Issues**: Task 0 is to open a focused GitHub issue for this feature.
- **Pull Requests**: All changes MUST be submitted via GitHub Pull Requests.

## Plan
### Phase 1: Body Handles
- [ ] Task 0: Open GitHub issue for "Data Plane Bypass and Direct SHM-to-Socket streaming".
- [ ] Task 1: Extend `ResponseInfo` to support "Body Handles" (pointers to SHM chunks).
- [ ] Task 2: Implement Rust-side reading from these handles.

### Phase 2: Socket Streaming
- [ ] Task 3: Update the Rust server to stream from SHM directly to the socket via zero-copy.
- [ ] Task 4: Expose Python-side `ShmResponse` types.
