## [2026-01-25 20:45] - Task 7.3: Legacy Removal & Adapter Modernization
- **Files changed:** `src/py/vertebra/app.py`, `src/py/vertebra/__init__.py`, `src/py/vertebra/asgi.py`
- **Learning:** Renamed `App` to `Vertex` to distinguish the native app container from `Vertebra` (plugin container) and `Litestar` (ASGI app).
- **Pattern:** Created `VertebraAdapter` (`src/py/vertebra/asgi.py`) to bridge `Vertex` apps to the ASGI protocol, enabling compatibility with standard servers and fallback paths.
- **Gotcha:** `Vertex.register` didn't support bare handlers; updated it to delegate to `Router` logic for better ergonomics.