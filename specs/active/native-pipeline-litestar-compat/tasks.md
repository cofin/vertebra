## Tasks

### Phase 1: Compatibility Inventory
- [ ] 1. Build a route/component matrix for sqlstack, fullstack-spa, fullstack-inertia, and dma accelerator (documented in tests or reports)
- [ ] 2. Capture the missing failure log and reproduce the current failing scenario in a controlled test

### Phase 2: Native Pipeline Core
- [ ] 3. Enhance route reflection to include handler objects, route metadata, and ASGI/static route detection
- [ ] 4. Replace handler lookup in worker startup with reflection-derived handler objects (avoid app.route_handlers search)
- [ ] 5. Add a compatibility classifier that marks routes native-supported or ASGI-fallback
- [ ] 6. Update router insertion so only native-supported routes are inserted into ShmRouter

### Phase 3: Request/Response Semantics
- [ ] 7. Expand parameter conversion (uuid, bool, float) for native-supported routes
- [ ] 8. Introduce native response handling (status codes, headers, JSON/text) and explicit fallback for redirects/streams/files

### Phase 4: Static Files Strategy
- [ ] 9. Implement Bun static asset mapping for Vite output (hybrid approach) and document fallback for uploads/static routers
- [ ] 10. Add configuration surface for enabling/disabling Bun static interception

### Phase 5: Testing
- [ ] 11. Add controller-based routing integration tests for native pipeline
- [ ] 12. Add DI/guard compatibility tests including Provide and fallback for advanced DI
- [ ] 13. Add multipart and WebSocket fallback tests
- [ ] 14. Add compatibility matrix tests and local-only tests for external apps

### Phase 6: Documentation and Skills
- [ ] 15. Update docs to describe native pipeline scope, compatibility matrix, and fallback policy
- [ ] 16. Note new patterns for future updates to `~/.codex/skills/vertebra/SKILL.md`

### Phase 7: Validation
- [ ] 17. Run `make test` and relevant integration subsets; confirm no regressions
