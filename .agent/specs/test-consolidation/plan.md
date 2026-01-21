## Tasks

### Phase 1: Discovery and Mapping
- [x] 1. Inventory all Vertebra Python tests and classify unit vs integration
- [x] 2. Draft final file move map and confirm marker assignments
- [x] 3. Verify pytest discovery and testpaths assumptions

### Phase 2: Vertebra Test Restructure
- [x] 4. Create `unit/` and `integration/` directories with domain subfolders
- [x] 5. Move test files into new structure and update imports
- [x] 6. Apply `@pytest.mark.unit` / `@pytest.mark.integration` markers to all tests

### Phase 3: Fixture Wiring and Cleanup (Vertebra)
- [x] 7. Remove `pytest-databases` from Vertebra dependencies and lockfile
- [x] 8. Replace `tests.database` stub with real fixture registration
- [x] 9. Validate DB fixtures (`postgres_service`, `mysql_service`, `redis_service`) in pytest
- [x] 10. Confirm xdist grouping and `--integration` selection works post-migration

### Phase 4: Documentation and Skills
- [x] 11. Update Vertebra docs/specs with new test taxonomy and run commands
- [x] 12. Cross-link Vertebra `quality` PRD and note shared memory cleanup alignment
- [x] 13. Update `vertebra` skill (and optionally `testing` skill) with new conventions

### Phase 5: Validation
- [x] 14. Run unit-only test pass (Vertebra)
- [x] 15. Run integration tests with `--integration` (Vertebra)
- [x] 16. Run full quality checks (`make test-all` or equivalent) after migration
