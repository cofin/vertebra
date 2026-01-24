● Let me walk through how this all works end-to-end.

  ---
  How the Build System Works

  Part 1: Vertebra Framework Distribution (We Publish)

  Goal: User runs pip install vertebra and gets everything working.

  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                         CI Pipeline (GitHub Actions)                        │
  ├─────────────────────────────────────────────────────────────────────────────┤
  │                                                                             │
  │  1. cibuildwheel builds platform-specific wheels:                          │
  │                                                                             │
  │     ┌──────────────┐    ┌──────────────┐    ┌──────────────┐               │
  │     │ Linux x64    │    │ Linux ARM64  │    │ macOS ARM64  │  ...          │
  │     │ + PyO3 ext   │    │ + PyO3 ext   │    │ + PyO3 ext   │               │
  │     │ + Bun binary │    │ + Bun binary │    │ + Bun binary │               │
  │     └──────────────┘    └──────────────┘    └──────────────┘               │
  │                                                                             │
  │  2. Sign macOS binaries (rcodesign + Apple notarization)                   │
  │                                                                             │
  │  3. Publish all wheels to PyPI                                             │
  │                                                                             │
  └─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                              User's Machine                                 │
  ├─────────────────────────────────────────────────────────────────────────────┤
  │                                                                             │
  │  $ pip install vertebra                                                    │
  │                                                                             │
  │  pip auto-selects best wheel for platform:                                 │
  │  - vertebra-1.0.0-cp312-cp312-manylinux_2_17_x86_64.whl (Linux)           │
  │  - vertebra-1.0.0-cp312-cp312-macosx_11_0_arm64.whl (Apple Silicon)       │
  │                                                                             │
  │  Installed in site-packages:                                               │
  │  └── vertebra/                                                             │
  │      ├── __init__.py                                                       │
  │      ├── _vertebra.cpython-312-*.so  (PyO3 Rust extension)                │
  │      └── bin/vertebra-engine         (Bun binary, ready to run)           │
  │                                                                             │
  │  $ vertebra serve app:app   ← Works immediately!                           │
  │                                                                             │
  └─────────────────────────────────────────────────────────────────────────────┘

  ---
  Part 2: User Builds Their App (They Run vertebra build)

  Goal: User has a cool-web-app/ project and wants a standalone binary.

  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                          User's Project                                     │
  ├─────────────────────────────────────────────────────────────────────────────┤
  │                                                                             │
  │  cool-web-app/                                                             │
  │  ├── pyproject.toml         # dependencies + [tool.vertebra] config        │
  │  ├── src/py/cool_web_app/                                                  │
  │  │   ├── app.py             # Vertebra app with pools configured           │
  │  │   └── procedures/        # Business logic                               │
  │  └── src/ts/                # Optional TypeScript                          │
  │                                                                             │
  └─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      │ $ vertebra build --one-file \
                                      │     --profile modern \
                                      │     --target linux-x64 \
                                      │     --output dist/myapp
                                      ▼
  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                         vertebra build Process                              │
  ├─────────────────────────────────────────────────────────────────────────────┤
  │                                                                             │
  │  Step 1: Resolve dependencies                                              │
  │  ┌─────────────────────────────────────────────────────────────────────┐   │
  │  │ Read pyproject.toml → Resolve ALL dependencies (including transitive)│   │
  │  │ Download wheels for target platform (linux-x64)                      │   │
  │  └─────────────────────────────────────────────────────────────────────┘   │
  │                                                                             │
  │  Step 2: Download python-build-standalone                                  │
  │  ┌─────────────────────────────────────────────────────────────────────┐   │
  │  │ Profile: modern → x86_64_v3-unknown-linux-gnu                        │   │
  │  │ From: github.com/astral-sh/python-build-standalone/releases          │   │
  │  │ Cached locally for future builds                                     │   │
  │  └─────────────────────────────────────────────────────────────────────┘   │
  │                                                                             │
  │  Step 3: Compile Bun engine for target                                     │
  │  ┌─────────────────────────────────────────────────────────────────────┐   │
  │  │ Bun.build({ compile: true, target: "bun-linux-x64" })               │   │
  │  │ Embeds: broker, server, TypeScript procedures                        │   │
  │  └─────────────────────────────────────────────────────────────────────┘   │
  │                                                                             │
  │  Step 4: Bundle everything with pyembed                                    │
  │  ┌─────────────────────────────────────────────────────────────────────┐   │
  │  │ Combines into single executable:                                     │   │
  │  │ - Python interpreter (from step 2)                                   │   │
  │  │ - Standard library (frozen)                                          │   │
  │  │ - Site-packages (all deps from step 1)                              │   │
  │  │ - User's app code (cool_web_app/)                                   │   │
  │  │ - Bun engine binary (from step 3)                                   │   │
  │  │ - SSL certificates (certifi)                                        │   │
  │  └─────────────────────────────────────────────────────────────────────┘   │
  │                                                                             │
  └─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                              Output                                         │
  ├─────────────────────────────────────────────────────────────────────────────┤
  │                                                                             │
  │  dist/myapp  (single file, ~80-150MB)                                      │
  │                                                                             │
  │  Can be copied to ANY Linux x64 machine and run:                           │
  │                                                                             │
  │  $ scp dist/myapp server:/opt/myapp                                        │
  │  $ ssh server "/opt/myapp serve"                                           │
  │                                                                             │
  │  ✓ No Python installation required                                         │
  │  ✓ No pip install required                                                 │
  │  ✓ No network access required                                              │
  │  ✓ No dependencies to manage                                               │
  │                                                                             │
  └─────────────────────────────────────────────────────────────────────────────┘

  ---
  What's Inside the Standalone Binary

  dist/myapp (executable, ~100MB)
  │
  ├─ HEADER (pyembed bootstrap)
  │  └─ Rust code that initializes embedded Python
  │
  ├─ PYTHON RUNTIME
  │  ├─ libpython3.12.so (interpreter)
  │  ├─ Standard library (frozen bytecode)
  │  └─ SSL certificates
  │
  ├─ SITE-PACKAGES (all dependencies)
  │  ├─ vertebra/
  │  ├─ msgspec/
  │  ├─ cffi/
  │  ├─ httpx/
  │  └─ (all transitive deps)
  │
  ├─ USER APPLICATION
  │  └─ cool_web_app/
  │      ├─ app.py
  │      └─ procedures/
  │
  └─ BUN ENGINE (compiled binary)
     └─ Handles HTTP, broker, pools

  ---
  Runtime Flow

  User runs: ./myapp serve

  ┌──────────────────┐
  │  pyembed loader  │ ← Rust entry point
  └────────┬─────────┘
           │ Initializes embedded Python
           ▼
  ┌──────────────────┐
  │  Rust Supervisor │ ← Manages process lifecycle
  └────────┬─────────┘
           │ py.allow_threads() - releases GIL
           │
      ┌────┴────┐
      │         │
      ▼         ▼
  ┌────────┐ ┌────────────┐
  │  Bun   │ │  Python    │
  │ Engine │ │  Workers   │
  └────┬───┘ └─────┬──────┘
       │           │
       └─────┬─────┘
             │ Shared memory IPC
             ▼
      ┌──────────────┐
      │   Broker     │ ← Owns DB pools, Redis, storage
      └──────────────┘

  ---
  Profile Comparison

# Maximum compatibility (runs on any x64 Linux)

  vertebra build --one-file --profile baseline --target linux-x64

# Maximum performance (requires 2013+ CPU with AVX2)

  vertebra build --one-file --profile modern --target linux-x64

# Apple Silicon Mac

  vertebra build --one-file --target darwin-arm64

# Build for all targets (CI use case)

  vertebra build --one-file --all-targets --output dist/

# Creates: dist/myapp-linux-x64, dist/myapp-linux-arm64, dist/myapp-darwin-arm64, etc

  Does this clarify how it all fits together?
