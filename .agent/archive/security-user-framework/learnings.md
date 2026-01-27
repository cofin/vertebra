# Knowledge Capture: Security & User Framework

## Patterns Discovered
- **MPMC Control Rings:** Using a Multi-Producer Multi-Consumer (MPMC) ring for the "control" channel allows multiple Python workers (or clients) to register with a single Broker thread without lock contention on the Broker side, as the ring handles the concurrency.
- **Opcode-based Dispatch:** Using integer opcodes (matched between Rust/TS/Python) for high-frequency security operations (hashing, signing) avoids the overhead of JSON/MsgPack serialization for the method name itself.
- **Lazy User Hydration:** The `LazyUserProxy` pattern allows `request.user` to be available immediately with claims from the token (JWT), while deferring the database fetch until a missing attribute is accessed.
- **Broker-Side Token Rotation:** Implementing refresh token rotation entirely within the Broker (Bun) keeps the authentication logic centralized and fast. The Broker transparently issues new cookies (`Set-Cookie`) when valid refresh tokens are presented, without the Python worker needing to implement rotation logic.

## Lessons Learned
- **Test Timeouts:** Integration tests involving `subprocess` and network calls (Bun server) are prone to timeouts if not carefully managed. Ensuring correct environment variables (`PYTHONPATH`, `VERTEBRA_CONFIG`) and absolute paths is critical.
- **Shared Memory Cleanup:** Named shared memory regions must be explicitly unlinked. We added `ShmMpmcRing.unlink` to ensure tests and server restarts don't fail with "File exists" errors.
- **Dependency Management:** Adding dependencies (like `jose` in Bun) requires ensuring the test environment (CI or local `make test`) installs them.
- **Bun Spawn & FD Inheritance:** Bun's `spawn` implementation (via `posix_spawn` or similar) respects `O_CLOEXEC` strictly. To pass specific FDs (like eventfds for IPC) to a child process, they must be explicitly included in the `stdio` array (e.g., `stdio: ["inherit", "inherit", "inherit", fd]`), which dups them to the child. Passing them merely as CLI args causes `OSError: Bad file descriptor` in the child.
- **PYTHONPATH Relative Resolution:** When spawning Python from TypeScript (via `ProcessManager`), resolving `src/py` relative to `process.cwd()` is fragile if the runner CWD varies (e.g., `src/ts` during dev vs root during prod). Use robust existence checks or fallback to `import.meta.dir` relative paths.

## Decisions Made
- **Bcrypt Default:** Switched to `bcrypt` for password hashing in the Broker as it's a standard, widely supported algorithm, though Argon2 is also available.
- **Symmetric JWT:** Added support for symmetric key JWT verification in the Broker to support simple shared-secret setups (like in tests) alongside asymmetric (JWKS) flows.
- **Refresh Token Strategy:** Implemented rotation for Symmetric JWTs only. External providers (OAuth2/IAP) manage their own rotation. Broker injects `Set-Cookie` headers for updated tokens.

## Reusable Components
- `SecurityClient` (Python): A standalone client for accessing Broker security functions via IPC.
- `AuthValidator` (TS): A reusable class for validating JWT/IAP tokens in the Bun server.
- `LazyUserProxy` (Python): A generic proxy for lazy-loading models.

## Gotchas & Warnings
- **Bun FFI Symbols:** When adding new Rust functions to `vertebra-broker`, you must rebuild the Rust library (`make rs-build`) *and* ensure the Bun FFI definition in `shm.ts` matches exactly, or you'll get runtime symbol lookup errors.
- **Pytest Asyncio vs Anyio:** Use `@pytest.mark.anyio` for async tests in this project, as `pytest-asyncio` markers might be ignored or cause warnings if `anyio` is the configured backend.
