 ---
  What Happens When You Run ./myapp serve

  Phase 1: Binary Bootstrap (Rust/pyembed)

  User types: ./myapp serve
                  │
                  ▼
  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                        OS Loads Executable                                  │
  ├─────────────────────────────────────────────────────────────────────────────┤
  │                                                                             │
  │  The binary is a Rust executable compiled with pyembed.                    │
  │                                                                             │
  │  1. OS loads the ELF/Mach-O/PE binary into memory                          │
  │  2. Rust main() starts executing                                           │
  │                                                                             │
  │  fn main() {                                                               │
  │      // pyembed has packed everything INTO the binary                      │
  │      // No extraction to /tmp needed for most resources                    │
  │                                                                             │
  │      let config = OxidizedPythonInterpreterConfig::default();              │
  │      let interp = MainPythonInterpreter::new(config)?;                     │
  │                                                                             │
  │      interp.with_gil(|py| {                                                │
  │          // Now we have a Python interpreter running                       │
  │          // from INSIDE the binary                                         │
  │      });                                                                   │
  │  }                                                                         │
  │                                                                             │
  └─────────────────────────────────────────────────────────────────────────────┘

  Phase 2: Python Interpreter Initialization

  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                    pyembed Initializes Python                               │
  ├─────────────────────────────────────────────────────────────────────────────┤
  │                                                                             │
  │  What pyembed does internally:                                             │
  │                                                                             │
  │  ┌─────────────────────────────────────────────────────────────────────┐   │
  │  │ 1. MEMORY-MAPPED RESOURCES                                          │   │
  │  │                                                                      │   │
  │  │    The binary contains:                                             │   │
  │  │    - Python bytecode (.pyc) for stdlib + deps                       │   │
  │  │    - Native extensions (.so/.dylib) for C modules                   │   │
  │  │    - Resource files (certificates, etc.)                            │   │
  │  │                                                                      │   │
  │  │    pyembed memory-maps these directly from the executable.          │   │
  │  │    No extraction to disk needed for pure Python modules.            │   │
  │  └─────────────────────────────────────────────────────────────────────┘   │
  │                                                                             │
  │  ┌─────────────────────────────────────────────────────────────────────┐   │
  │  │ 2. CUSTOM IMPORT HOOK (OxidizedFinder)                              │   │
  │  │                                                                      │   │
  │  │    Python's import system is redirected:                            │   │
  │  │                                                                      │   │
  │  │    import msgspec                                                   │   │
  │  │           │                                                         │   │
  │  │           ▼                                                         │   │
  │  │    OxidizedFinder.find_module("msgspec")                           │   │
  │  │           │                                                         │   │
  │  │           ▼                                                         │   │
  │  │    Looks up in embedded index → Returns memory-mapped bytecode     │   │
  │  │           │                                                         │   │
  │  │           ▼                                                         │   │
  │  │    Module loaded WITHOUT touching filesystem                        │   │
  │  └─────────────────────────────────────────────────────────────────────┘   │
  │                                                                             │
  │  ┌─────────────────────────────────────────────────────────────────────┐   │
  │  │ 3. NATIVE EXTENSIONS (Special Handling)                             │   │
  │  │                                                                      │   │
  │  │    For C extensions (cffi, msgspec's C parts, etc.):                │   │
  │  │                                                                      │   │
  │  │    - Extracted to temp directory on first run                       │   │
  │  │    - dlopen() requires filesystem path                              │   │
  │  │    - Cached for subsequent runs                                     │   │
  │  │                                                                      │   │
  │  │    Location: $TMPDIR/pyembed-XXXX/ or configurable                  │   │
  │  └─────────────────────────────────────────────────────────────────────┘   │
  │                                                                             │
  └─────────────────────────────────────────────────────────────────────────────┘

  Phase 3: Vertebra CLI Executes

  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                      Python CLI Starts                                      │
  ├─────────────────────────────────────────────────────────────────────────────┤
  │                                                                             │
  │  # Rust calls into Python                                                  │
  │  py.run("from cool_web_app.app import app; app.cli()", None, None)         │
  │                                                                             │
  │  # Which executes:                                                         │
  │  @click.command()                                                          │
  │  def serve():                                                              │
  │      from vertebra.supervisor import Supervisor                            │
  │                                                                             │
  │      supervisor = Supervisor()                                             │
  │      supervisor.start(                                                     │
  │          engine_path=find_embedded_engine(),  # Bun binary location        │
  │          python_executable=sys.executable,     # Points to SELF            │
  │          config=app.get_supervisor_config(),                               │
  │          workers=4,                                                        │
  │      )                                                                     │
  │      supervisor.wait_for_shutdown()                                        │
  │                                                                             │
  │  Note: sys.executable points to the running binary itself!                 │
  │  ./myapp IS the Python interpreter in one-file mode.                       │
  │                                                                             │
  └─────────────────────────────────────────────────────────────────────────────┘

  Phase 4: Rust Supervisor Spawns Processes

  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                    Supervisor.start() - Rust Side                           │
  ├─────────────────────────────────────────────────────────────────────────────┤
  │                                                                             │
  │  impl Supervisor {                                                         │
  │      fn start(&mut self, py: Python, ...) -> PyResult<()> {                │
  │                                                                             │
  │          // CRITICAL: Release the GIL before spawning                      │
  │          py.allow_threads(|| {                                             │
  │                                                                             │
  │              // 1. Extract Bun engine to temp (if not cached)              │
  │              let engine_path = self.extract_engine()?;                     │
  │                                                                             │
  │              // 2. Start Bun broker/engine process                         │
  │              self.bun_process = Some(                                      │
  │                  Command::new(&engine_path)                                │
  │                      .env("VERTEBRA_MODE", "embedded")                     │
  │                      .spawn()?                                             │
  │              );                                                            │
  │                                                                             │
  │              // 3. Start Python workers (re-exec self!)                    │
  │              let self_exe = std::env::current_exe()?;                      │
  │              for i in 0..worker_count {                                    │
  │                  self.workers.push(                                        │
  │                      Command::new(&self_exe)  // ./myapp                   │
  │                          .arg("--worker")      // Special mode             │
  │                          .arg("--worker-id")                               │
  │                          .arg(i.to_string())                               │
  │                          .spawn()?                                         │
  │                  );                                                        │
  │              }                                                             │
  │                                                                             │
  │              Ok(())                                                        │
  │          })                                                                │
  │      }                                                                     │
  │  }                                                                         │
  │                                                                             │
  └─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      │
            ┌─────────────────────────┼─────────────────────────┐
            │                         │                         │
            ▼                         ▼                         ▼
  ┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐
  │  Bun Engine      │    │  ./myapp         │    │  ./myapp         │
  │  (extracted)     │    │  --worker        │    │  --worker        │
  │                  │    │  --worker-id 0   │    │  --worker-id 1   │
  │  PID: 1001       │    │  PID: 1002       │    │  PID: 1003       │
  └──────────────────┘    └──────────────────┘    └──────────────────┘

  Phase 5: Worker Process Startup

  ┌─────────────────────────────────────────────────────────────────────────────┐
  │              Worker Process (./myapp --worker --worker-id 0)                │
  ├─────────────────────────────────────────────────────────────────────────────┤
  │                                                                             │
  │  Same binary, different entry point:                                       │
  │                                                                             │
  │  fn main() {                                                               │
  │      let args: Vec<String> = std::env::args().collect();                   │
  │                                                                             │
  │      if args.contains(&"--worker".to_string()) {                           │
  │          // Worker mode: run ASGI handler                                  │
  │          run_worker_mode(args);                                            │
  │      } else {                                                              │
  │          // Normal mode: run CLI                                           │
  │          run_cli_mode();                                                   │
  │      }                                                                     │
  │  }                                                                         │
  │                                                                             │
  │  fn run_worker_mode() {                                                    │
  │      let interp = MainPythonInterpreter::new(config)?;                     │
  │      interp.with_gil(|py| {                                                │
  │          py.run(r#"                                                        │
  │              from vertebra.server.worker import run_worker                 │
  │              run_worker()  # Connects to broker via shared memory          │
  │          "#, None, None)                                                   │
  │      });                                                                   │
  │  }                                                                         │
  │                                                                             │
  └─────────────────────────────────────────────────────────────────────────────┘

  Phase 6: Shared Memory IPC Setup

  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                    Shared Memory Ring Buffers                               │
  ├─────────────────────────────────────────────────────────────────────────────┤
  │                                                                             │
  │  The Bun broker creates shared memory regions:                             │
  │                                                                             │
  │  /dev/shm/vertebra-broker-ctrl    (control ring - locked with fcntl)       │
  │  /dev/shm/vertebra-broker-data-0  (data ring for worker 0)                 │
  │  /dev/shm/vertebra-broker-data-1  (data ring for worker 1)                 │
  │                                                                             │
  │  ┌─────────────┐         ┌─────────────────────┐         ┌─────────────┐   │
  │  │   Worker 0  │◄───────►│   Shared Memory     │◄───────►│  Bun Broker │   │
  │  │  (Python)   │         │   Ring Buffer       │         │  (owns DB   │   │
  │  └─────────────┘         │                     │         │   pools)    │   │
  │                          │  ┌───────────────┐  │         └─────────────┘   │
  │  ┌─────────────┐         │  │ Request Ring  │  │                           │
  │  │   Worker 1  │◄───────►│  │ Response Ring │  │                           │
  │  │  (Python)   │         │  └───────────────┘  │                           │
  │  └─────────────┘         └─────────────────────┘                           │
  │                                                                             │
  │  Communication is ZERO-COPY:                                               │
  │  - Small results: MessagePack directly in ring buffer                      │
  │  - Large results (>1MB): Arrow IPC with shared memory backing              │
  │                                                                             │
  └─────────────────────────────────────────────────────────────────────────────┘

  Phase 7: Request Flow (Steady State)

  HTTP Request arrives
          │
          ▼
  ┌───────────────────┐
  │    Bun Server     │  Port 8000
  │   (HTTP handling) │
  └────────┬──────────┘
           │ Routes to ASGI bridge
           ▼
  ┌───────────────────┐
  │   ASGI Bridge     │  In Bun process
  │  (Bun → Python)   │
  └────────┬──────────┘
           │ Writes to shared memory ring
           ▼
  ┌───────────────────────────────────────────────────────┐
  │              Shared Memory Ring Buffer                │
  │  ┌─────────────────────────────────────────────────┐  │
  │  │ { "type": "http.request",                       │  │
  │  │   "path": "/api/users",                         │  │
  │  │   "method": "GET",                              │  │
  │  │   "headers": [...] }                            │  │
  │  └─────────────────────────────────────────────────┘  │
  └───────────────────────────────────────────────────────┘
           │ Worker reads from ring
           ▼
  ┌───────────────────┐
  │  Python Worker    │
  │  (ASGI app)       │
  │                   │
  │  async def app(): │
  │    # Your code!   │
  │    users = await  │
  │      db.fetch()   │◄──┐
  └────────┬──────────┘   │
           │              │
           │   ┌──────────┴─────────────────────────────┐
           │   │  DB query goes through broker:         │
           │   │                                        │
           │   │  Worker → SHM Ring → Broker → Postgres │
           │   │  Worker ← SHM Ring ← Broker ← Postgres │
           │   │                                        │
           │   │  Broker owns the connection pool,      │
           │   │  worker just sends query + params      │
           │   └────────────────────────────────────────┘
           │
           ▼ Response written to ring
  ┌───────────────────────────────────────────────────────┐
  │              Shared Memory Ring Buffer                │
  │  ┌─────────────────────────────────────────────────┐  │
  │  │ { "type": "http.response",                      │  │
  │  │   "status": 200,                                │  │
  │  │   "body": [{"id": 1, "name": "Alice"}, ...] }  │  │
  │  └─────────────────────────────────────────────────┘  │
  └───────────────────────────────────────────────────────┘
           │
           ▼
  ┌───────────────────┐
  │    Bun Server     │
  │  Sends HTTP resp  │
  └───────────────────┘
           │
           ▼
      HTTP Response

  ---
  Summary: The Key Points
  Aspect: No Python install needed
  How It Works: Python interpreter is embedded in the binary via pyembed
  ────────────────────────────────────────
  Aspect: No pip install needed
  How It Works: All dependencies are memory-mapped from inside the binary
  ────────────────────────────────────────
  Aspect: No extraction (mostly)
  How It Works: Pure Python modules load directly from binary; only native .so files extract
    to temp
  ────────────────────────────────────────
  Aspect: Workers are re-exec
  How It Works: ./myapp --worker runs the same binary in worker mode
  ────────────────────────────────────────
  Aspect: Zero-copy IPC
  How It Works: Shared memory rings, no sockets in data plane
  ────────────────────────────────────────
  Aspect: GIL not a bottleneck
  How It Works: Rust supervisor releases GIL; each worker has its own GIL
  ────────────────────────────────────────
  Aspect: Bun owns pools
  How It Works: DB connections, Redis, storage all managed by Bun process
  The magic is that one file contains everything: the Rust bootstrap, Python interpreter, all
   Python code (bytecode), all dependencies, and the Bun engine. When you run it, pyembed
  bootstraps Python from within the binary, and the supervisor orchestrates the multi-process
   architecture.
