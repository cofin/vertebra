# Research: Litecore + Vertebra IPC Integration

## 1. The Opportunity
*   **Litecore** runs the Python Application in-process (or in threads).
*   **Vertebra IPC** runs a separate "Pool Manager" process (Postgres/Redis/Arrow).
*   **Integration Point:** If Litecore can speak to the Vertebra Ring Buffer *natively*, it can bypass the kernel TCP stack entirely for DB queries, even if the DB Pool is in another process (e.g. a centralized pool for multiple Litecore instances).

## 2. Integration Modes

### Mode A: "Sidecar" (Loose Coupling)
*   **Setup:** Litecore starts. Vertebra Pool Manager starts separately.
*   **Mechanism:** Python Application uses `vertebra.extensions.sqlspec` to talk to the Pool Manager.
*   **Flow:** Python (in Litecore) -> PyObject -> Shared Memory -> Rust Pool Manager.
*   **Pros:** Clean separation. Works today.
*   **Cons:** Python is still the "Driver". We are doing `Python -> Rust (IPC)` serialization.

### Mode B: "Native Bridge" (Tight Coupling)
*   **Setup:** Litecore detects `vertebra-ipc` availability.
*   **Mechanism:** Litecore's **Rust Runtime** talks directly to the Shared Memory Ring.
*   **Flow:**
    1.  Request arrives at Litecore (HTTP).
    2.  Litecore (Rust) parses request.
    3.  **Fast Path:** If the request matches a "Data Route" (e.g. `/api/users`), Litecore sends the query request *directly* to the Vertebra Ring from Rust.
    4.  **Zero-Python:** The Vertebra Manager executes SQL, writes Arrow to SHM.
    5.  Litecore reads Arrow from SHM, serializes to JSON (or sends Arrow Stream), and responds.
*   **Efficiency:** Python is completely bypassed for simple CRUD. 100% Rust path.

### Mode C: "The Data Injection"
*   **Setup:** Normal Python App running in Litecore.
*   **Mechanism:** Litecore injects a pre-connected `VertebraConnection` into the request scope (RSGI).
*   **Flow:**
    1.  Litecore (Rust) initializes the IPC Ring connection at startup.
    2.  Request arrives.
    3.  Litecore passes the *Rust struct* `Arc<ShmRing>` to Python as a `PyCapsule`.
    4.  Python `vertebra` adapter sees the capsule and uses it directly.
*   **Efficiency:** Avoids "connecting" overhead in Python. The connection is persistent at the server level.

## 3. Recommended Strategy: "The Data Injection" (Mode C)
This offers the best balance of DX and Performance.
1.  **Optional Dependency:** `litecore` crate does *not* depend on `vertebra-ipc`.
2.  **Plugin Trait:** Litecore exposes a `StatePlugin` trait.
3.  **Vertebra Plugin:** A small crate `litecore-vertebra` implements `StatePlugin`.
    *   On Startup: Connects to Ring.
    *   On Request: Injects the Ring handle into `request.extensions` (RSGI) or `scope['extensions']` (ASGI).
4.  **Python Usage:**
    ```python
    def handler(req):
        # Zero setup cost. The ring is already hot.
        conn = req.extensions['vertebra_ipc']
        return conn.execute("SELECT * FROM users")
    ```

## 4. Updates Required
*   **Litecore PRD:** Add "Plugin System" or "Extension Injection" to the architecture.
*   **Vertebra IPC PRD:** Add "Litecore Plugin" to the roadmap.
