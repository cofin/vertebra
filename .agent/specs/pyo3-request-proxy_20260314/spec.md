# Flow Spec: PyO3 Request Proxy
## Goal
Implement zero-object Python proxy class in Rust.

## Workflow Requirements
- **GitHub Issues**: Task 0 is to open a focused GitHub issue for this feature.
- **Pull Requests**: All changes MUST be submitted via GitHub Pull Requests.

## Plan
### Phase 1: Request Proxy
- [ ] Task 0: Open GitHub issue for "Zero-Object Request Proxy in PyO3".
- [ ] Task 1: Implement `src/rs/vertebra-py/src/request.rs`. `VertebraRequest` Python class.
- [ ] Task 2: Implement lazy memory mapping for `.method`, `.path`, `.body`.

### Phase 2: Headers Proxy
- [ ] Task 3: Implement `src/rs/vertebra-py/src/headers.rs`. `VertebraHeaders` lazy dict-like object.
- [ ] Task 4: Implement `.headers` getter on the request proxy.
