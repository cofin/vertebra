## Tasks

### Phase 1: Capability Classification (Python)
- [x] 1. Add multipart capability detection for `RequestEncodingType.MULTI_PART`, string media types (`"multipart/form-data"`), and `UploadFile` (including optional/list/nested dependencies).
- [ ] 2. Ensure classifier resolves handler configuration across app/router/controller/handler layers and emits explicit fallback reasons.
  Note: classifier inspects handler + params, but does not emit explicit fallback reason output yet.

### Phase 2: Bun Runtime Safety Guard
- [x] 3. Add runtime guard in `AsgiBridge` to force ASGI fallback (or return 415/413) when `Content-Type` is multipart or body size exceeds native thresholds.
- [ ] 4. Clarify and document that multipart requests use the ASGI streaming path (`handleStream()`), preserving backpressure semantics.
  Note: runtime behavior exists in `AsgiBridge`, docs not updated.

### Phase 3: Observability
- [ ] 5. Emit structured startup logs for route classification with fields: `route_path`, `http_method`, `handler_id`, `classification`, `fallback_reason`.
- [ ] 6. Add runtime metric counter `vertebra_multipart_fallback_total` and include it in server metrics output.

### Phase 4: Tests
- [ ] 7. Unit tests for classifier: string media types, nested DI, optional/list `UploadFile`.
- [ ] 8. Unit tests for multi-method routes where one method implies multipart and another does not.
- [ ] 9. Integration test: multipart request never emits `direct.http` metadata and is handled via ASGI.
- [ ] 10. Integration test: Bun runtime guard forces ASGI fallback (or error) when multipart `Content-Type` is present on a native route.
- [ ] 11. Negative test: misclassified multipart routes fail fast with clear error, not large buffering.
  Note: coverage exists in `src/py/tests/integration/litestar/test_compatibility_matrix.py` and `src/ts/tests/unit/asgi-bridge.test.ts`.

### Phase 5: Documentation and Skills
- [ ] 12. Update `docs/arch.md` with multipart fallback rationale and constraints.
- [ ] 13. Update `docs/architecture/native-integration.md` with capability matrix and fallback reasons.
- [ ] 14. Update `docs/progress.md` with Phase 1 status and Phase 2 roadmap notes.
- [ ] 15. Record patterns for skill extraction in `~/.claude/skills/vertebra/` and `~/.claude/skills/ipc/`.

### Phase 6: Validation
- [ ] 16. Run `make test` and `make js-test` (optionally `make test-all`) to validate behavior.
