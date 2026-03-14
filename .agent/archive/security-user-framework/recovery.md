# Recovery Guide: Security & User Framework

## Quick Resume

**Last phase**: Phase 2 Complete, Phase 3 In Progress
**Current status**: Integration tests failing (claims not propagating)
**Spec location**: `.agent/specs/security-user-framework/`

## State Summary

### Completed
- [x] Phase 1: Bun IPC Security Hooks (HASH_PASSWORD, VERIFY_PASSWORD, GENERATE_RANDOM, CSRF_SIGN)
- [x] Phase 2: Core User Framework (VertebraUserMixin, AnonymousUser, LazyUserProxy, contextvars state)

### In Progress
- [~] Phase 3: Broker-Centric Authentication
  - [x] JWT and Google IAP header validation in Bun Broker
  - [x] JWKS caching for Google/GitHub
  - [x] HTTP-only cookie extraction and CSRF validation
  - [x] Fixed Supervisor import mismatch
  - [~] Integration tests written but failing with timeouts
  - [ ] Refresh Token rotation flow

### Blocking Issues

1. **Integration tests timing out**: Tests in `test_broker_auth.py` fail with timeouts. Server starts but claims are not propagating or validation is failing.

2. **SHM "File exists" errors**: On re-runs, shared memory objects persist. Clean with:
   ```bash
   rm -rf /dev/shm/test-* /dev/shm/*vertebra*
   ```

## Key Files

| File | Purpose |
|------|---------|
| `src/py/vertebra/auth/models.py` | VertebraUserMixin, AnonymousUser |
| `src/py/vertebra/auth/state.py` | contextvars global state |
| `src/py/vertebra/auth/proxy.py` | LazyUserProxy implementation |
| `src/ts/src/broker/auth.ts` | AuthValidator (JWT/IAP validation) |
| `src/ts/src/broker/security.ts` | Password hashing, CSRF, random handlers |
| `src/py/tests/integration/test_broker_auth.py` | Failing integration tests |

## Checkpoints

- Phase 1 complete: `23a05d2`
- Phase 2 complete: `654f611`

## Next Steps

1. Debug integration test failures:
   ```bash
   uv run pytest src/py/tests/integration/test_broker_auth.py -v -s --timeout=60
   ```

2. Check if claims are reaching Python workers:
   - Add logging to `AuthMetadata` extraction
   - Verify Broker is sending `X-Auth-Claims` header

3. Once Phase 3 passes, implement Refresh Token rotation

## Learnings Applied

- Use uuid-based unique names for shared memory in tests
- Clean `/dev/shm/` before test runs
- Use `@pytest.mark.anyio` for async tests
- Ensure `PYTHONPATH` and `VERTEBRA_CONFIG` environment variables are set
