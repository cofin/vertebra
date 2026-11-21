# Chapters

Chapters run in the order below. Each chapter ends by sampling the adapter benchmark matrix for the engines it touched. All work lands on the branch `feat/integrated-benchmarks`.

| Chapter | Title | Depends on | Planning | State |
| --- | --- | --- | --- | --- |
| [embedded-cursors](embedded-cursors/spec.md) | Native cursor in the SQLite and DuckDB extensions and their native drivers | — | complete | completed |
| [wrapper-layers](wrapper-layers/spec.md) | DB-API module, SQLAlchemy dialect, and SQLSpec adapter base on the native cursor | embedded-cursors | complete | completed |
| [network-engines](network-engines/spec.md) | Native cursor for PostgreSQL, MySQL, SQL Server, Oracle, Spanner, and BigQuery, per-connection leases, and the Oracle relay switch | embedded-cursors, wrapper-layers | complete | completed |
| [single-call-paths](single-call-paths/spec.md) | One native call per query: single-call methods, completed awaitables, the SQLSpec cache-hit executor, and bind fast paths | network-engines | complete | completed |
| [wire-parity](wire-parity/spec.md) | PostgreSQL and Oracle wire-level parity | single-call-paths | empirical_dependent | completed |
| [cleanup-and-checks](cleanup-and-checks/spec.md) | Removal audit, benchmark comparison check, test tree repair, and knowledge records | embedded-cursors, wrapper-layers, network-engines | complete | completed |

## Chapter frontmatter values

| Chapter | `depends_on_chapters` | `planning_status` | `replan_trigger` | Task id prefix |
| --- | --- | --- | --- | --- |
| `embedded-cursors` | `[]` | `complete` | `null` | completed |
| `wrapper-layers` | `[embedded-cursors]` | `complete` | `null` | completed |
| `network-engines` | `[embedded-cursors, wrapper-layers]` | `complete` | `null` | completed |
| `single-call-paths` | `[network-engines]` | `complete` | `null` | completed |
| `wire-parity` | `[single-call-paths]` | `empirical_dependent` | The PostgreSQL async pooled row or the Oracle relay-off row has a median ratio above `1.0` in the closing capture of `single-call-paths` | completed |
| `cleanup-and-checks` | `[embedded-cursors, wrapper-layers, network-engines]` | `complete` | `null` | completed |

## Hand-offs between chapters

- `cleanup-and-checks` runs in two parts. Its test tree repair and benchmark tasks (`t13` to `t20`) run first and finish before any `single-call-paths` task starts. Its acceptance run (`t07`) and the knowledge tasks that describe the single-call work wait on `single:t27` at task level.
- `embedded-cursors` binds the DB-API exception names to the `vertebra._vertebra` classes and delivers the cursor contract in root spec section 2 for SQLite and DuckDB. It adds `cursor()` to all four embedded drivers.
- `wrapper-layers` moves the DB-API module, the SQLAlchemy asyncio adapter, and the SQLSpec bases onto the native cursor.
- `network-engines` delivers the cursor for the six network engines, takes leases at checkout, and deletes the old wrapper path.
- `single-call-paths` adds the single-call methods, the completed awaitable, the bind fast paths, the SQLSpec cache-hit executor, and the bound DB-API cursor factory. Its last task captures every acceptance row and states, per row, whether the `wire-parity` trigger fired.
- `wire-parity` is planned from that capture. When no row misses, it closes with no task run.
- `cleanup-and-checks` ends with a second removal audit over what `single-call-paths` supersedes, then the acceptance run, which waits on `single:t27`, the audit, and `wire-parity`.
