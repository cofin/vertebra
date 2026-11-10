---
type: Guide
flow_id: native-postgres-driver
title: Native PostgreSQL public contracts
description: Public interface and ownership contracts for the complete native PostgreSQL driver plan.
status: stable
created_at: "2026-11-05T00:00:00Z"
updated_at: "2026-11-07T16:40:00Z"
generated:
  by: flow-revise
  at: "2026-11-07T16:40:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-07T16:40:00Z"
    commit: dd432f18
  - by: agent:flow-setup
    at: "2026-11-07T04:15:00Z"
    commit: 9af810f5
sources:
  - id: pg-config
    resource: src/py/vertebra/db/adapters/postgres/config.py::PostgreSQLConfig
    title: PostgreSQL DSN, TLS, Cloud SQL, AlloyDB, and IAM configuration
  - id: pg-driver
    resource: src/py/vertebra/db/adapters/postgres/driver.py::SyncPostgreSQLDriver
    title: Public SyncPostgreSQLDriver and AsyncPostgreSQLDriver adapter surface
  - id: arrow-cursor
    resource: src/py/vertebra/db/arrow.py::ArrowCursor
    title: Public ArrowCursor result wrapper over VTBARROW ShmCapsule
tags:
  - postgres
  - interfaces
  - ownership
  - contracts
---

# Native PostgreSQL public contracts

<!-- truth: start -->
- **Owned Primitive FFI Boundary**: `SyncPostgresClient` and `AsyncPostgresClient` accept owned primitive options across `vertebra._postgres` and never extract Rust `Arc` or `PyRef` handles from sibling `cdylib` extensions [^pg-config].
- **Lossless Value & `VTBARROW` Contracts**: Frozen `msgspec` adapter records (`PgInterval`, `PgArray`, `PgComposite`, `PgTemporal`, `PgBitString`) and standard Arrow structural schemas preserve full PostgreSQL fidelity without Tokio-thread Python hydration [^pg-driver] [^arrow-cursor].
<!-- truth: end -->

All features ship in one complete-driver PR across `ch01`–`ch31`. These contracts govern every chapter specification and task worksheet.

## Connection constructors

`SyncPostgresClient` and `AsyncPostgresClient` share this keyword-only constructor:

```python
def __init__(
    self,
    *,
    user: str,
    database: str,
    host: str = "localhost",
    port: int = 5432,
    password: str | None = None,
    unix_socket: str | None = None,
    sslmode: str = "verify-full",
    ca_pem: bytes | None = None,
    client_cert_pem: bytes | None = None,
    client_key_pem: bytes | None = None,
    server_name: str | None = None,
    channel_binding: str = "prefer",
    connect_timeout: float = 10.0,
    application_name: str = "vertebra",
    session_gucs: Mapping[str, str] | None = None,
    keepalives: bool = True,
    keepalives_idle: int | None = None,
    keepalives_interval: int | None = None,
    keepalives_count: int | None = None,
    cloud_sql_instance: str | None = None,
    cloud_sql_ip_type: str = "PRIVATE",
    alloydb_instance_uri: str | None = None,
    alloydb_ip_type: str = "PRIVATE",
    enable_iam_auth: bool = False,
    cloud_auth_options: Mapping[str, str] | None = None,
    iam_auth_options: Mapping[str, str] | None = None,
) -> None: ...
```

The native boundary receives owned primitive options, never another extension's
Rust cache/PyRef/Arc. Constructors validate/copy without I/O. PEM file DSN options
are resolved by the adapter before native construction, with file errors preserved;
the physical network deadline begins when connect is reserved. Explicit typed
non-None TLS/timeout options override DSN query options, then URI components, then
native defaults. Existing application_name/session_gucs config values remain
authoritative. The sole URL/libpq parser is adapter-owned.

Unix socket is an exact PostgreSQL socket file path. It excludes cloud endpoints;
the adapter converts a DSN host directory to `.s.PGSQL.<port>`. Unix transport has
no TLS negotiation and requires sslmode disable; the adapter chooses disable for
an implicit Unix TLS default and rejects explicit incompatible TLS requests.
Unsupported platforms fail clearly. A direct TCP host may be IPv4/IPv6/DNS and
is not a URI or Unix directory.

Cloud resource options exclude each other and Unix transport; IP types are
PUBLIC/PRIVATE/PSC, binding is disable/prefer/require, TLS modes are the documented
five spellings. Cloud peer identity/TLS1.3 is mandatory independently of direct
sslmode. Cloud Admin credentials come from cloud_auth_options; login credentials
come from iam_auth_options. A missing login map derives the same credential source
with the required login scopes. Explicit static login tokens are designated only
through iam_auth_options; opaque Admin tokens are never assumed to be login grants.

Direct-host IAM remains supported. With enable_iam_auth and no cloud resource,
resolve a fresh login token before ordinary PostgreSQL startup and use it as the
password; default scope is sqlservice.login. An explicit iam_auth_options scopes
value is authoritative, including alloydb.login for direct AlloyDB endpoints. IAM
mode supersedes a DSN password; it does not fetch proxy certificates or change the
configured direct TLS policy. Test cache expiry/refresh, rejected grants and failed
authentication without same-stream retry.

## Pools and sessions

Both native pool constructors take the complete connection signature plus:

```python
pooler_mode: str = "direct"
protocol_level_pooler_cache: bool = False
statement_cache_size: int = 256
min_size: int = 1
max_size: int = 10
acquire_timeout: float = 30.0
reset_timeout: float = 10.0
close_timeout: float = 30.0
```

Pool modes are direct/session/transaction/statement/auto; explicit unsupported
transaction features in statement mode fail before emission. `acquire()` returns
the corresponding client owning a real PooledConnection. `client.close()` resets
and releases pooled ownership and terminates direct ownership. Pool `close()`
drains boundedly. `validate_configuration(**connection_options)` compares canonical
native configuration and raises InterfaceError on mismatch without network access;
there is no caller-supplied pool key. Pool configuration includes credentials, TLS,
GUCs, cloud resource/network/IAM identity and preparation policy. A shared-driver
cache cannot mix options; share_pool=False creates private ownership.

Ordinary Python driver calls acquire transient leases. Explicit transactions,
pipelines and portals pin them. A driver owns ContextVar scope with thread/task
identity checks; inherited concurrent scopes cannot concurrently use one socket.
Nested scopes use savepoints. Shared registry pool resources remain separate from
individual transaction scopes. No generic database engine dispatch is added.

## Query and pipeline results

Native `query(sql, parameters=())` executes one extended statement and returns the
adapter-owned immutable PostgresQueryResult. `query_multi(sql)` returns all simple
query results in order and accepts no parameters. Both Python drivers expose an
explicit query_multi counterpart; usual execute/select methods are single-statement.
PostgresQueryResult contains ordered PostgresColumn metadata, tuple rows, affected
count, command tag and I/T/E status. The driver translates to its existing public
tuple/count/dict/hydration shapes. PostgresColumn fields are name/type_oid/format/
type_modifier/table_oid/attribute_number.

The raw adapter accepts native $N positional bindings and existing pyformat %s or
%(name)s through its owned SQL-aware lexer. Raw mappings require named placeholders;
never bind mapping.values(). SQLSpec prepared objects use their existing compiler
copy to numeric transport, preserving original config identity and provenance.
Question marks inside raw PostgreSQL operators remain operators.

A pipeline context begins a transaction or savepoint before batch emission. It
retains ownership while results are converted by the caller and commits/releases
only on clean exit. The batch itself emits one Sync and one logical flush, including
cold type discovery via text formats. Rust tracks completed/error/skipped outcomes;
Python raises the first canonical DatabaseError after recovery and attaches ordered
outcomes/failing index, preserving shared exception identity. No fabricated SQLSTATE
is assigned to skipped commands and no failed batch is replayed.

## Values, Arrow and CDC

Adapter-owned immutable records preserve intervals, dimensioned arrays, composites,
typed parameters, JSON null, and temporal infinity/range. Ambiguous non-ISO timestamp
display text retains exact text plus formatting/timezone context in PgTemporal.
Every unresolved display typed bind raises DataError before text, binary, pipeline
or COPY submission, even under matching context. Callers explicitly resolve the
instant or intentionally bind raw text. Binary temporal values retain full raw
components independently of display settings.

Arrow uses exact numeric UTF8 plus OID/typmod metadata, typed nested arrays/composites,
typed vector columns, and lossless temporal/interval storage described in `ch19:t01`.
It never passes Python row materialization through its native fast path.
IPC chunking preserves every byte and one EOS before the producer-owned SHM capsule
handoff; public cursors use vertebra.db.arrow.ArrowCursor.

Logical replication uses pre-existing caller-owned slot/publication, pgoutput v1,
tail/resume delivery and no automatic initial snapshot/provision/drop. Checkpoints
contain source identity, acknowledged commit-end LSN and signed-int64 next event ID.
The adapter-owned store saves before flush/apply feedback advances; default memory
storage is explicitly nondurable. Only contiguous emitted commit boundaries may
be acknowledged. Consumers own durable application and initial snapshot alignment.


## Authentication and lexical defaults

With usable TLS binding, Prefer selects PLUS if advertised and uses GS2 y
(ChannelBinding::unrequested) for plain-only advertisement. Disable or genuinely
unavailable binding uses n (unsupported). Require fails before credentials when
PLUS/binding is absent; no failed exchange is replayed as a weaker mode.

Designated static iam_auth_options require access_token (or bearer_token), actual
expires_at_unix_ms as canonical unsigned decimal milliseconds, and declared scopes
covering the requested login scopes. Reject missing/expired/inside-skew lifetime or
unknown scope before transmission. Construct the native static cache with that
actual expiry; never fabricate TTL. Scoped provider failures use exact core
GoogleCloudAuthError; server SQLSTATE28 remains DatabaseError.

DSN defaults at bridge invocation: explicit user > nonempty PGUSER > getpass.getuser;
explicit dbname > nonempty PGDATABASE > resolved user; explicit empty user/dbname
fails. Empty URI path is absent. Password explicit > PGPASSWORD if set > None;
host explicit > nonempty PGHOST > localhost; port explicit > nonempty PGPORT >5432.
OS username failure is ImproperConfigurationError. Strict UTF8 percent decoding
preserves literal +; keyword grammar supports quoted/backslash values.

Pyformat emulation rejects ambiguous ordinary-string backslash-quote forms and
requires explicit E strings; it never guesses changes made by earlier queued user
functions. Native numeric SQL without emulation remains server-parsed: pass SQL and ordered values unchanged, validate sequence shape/resource limits, and leave SQL syntax/index coverage/count errors to the server. Do not scan numeric-only SQL using a guessed session lexical setting. Numeric
tokens require an identifier boundary, so foo$1 is not a bind. SQLSpec retains its
own prepared-object compiler boundary.

Cold ordinary queries use a text-format extended group without implicit BEGIN.
Required custom parameter metadata is resolved before Parse. Owned cold text-result
normalization runs only after original ReadyForQuery with its captured codec context.
Catalog Query never sits between unnamed Parse and Bind; any split named preflight
pins/re-Parses correctly. Recoverable result enrichment uses exact-text fallback,
with a metadata-only savepoint inside ambient T. Irrecoverable after-completion
errors keep the canonical source class/SQLSTATE and attach .completed_result; the
original statement is never replayed.

## Dedicated event ownership and checkpoints

Both native listener and replication handles expose read-only closed:bool. The
driver registry pairs each handle with its dedicated AsyncPostgresClient, prunes
closed pairs and closes clients in finally. Both driver facades provide async
_open_notification_listener(channels,*,queue_capacity=1024,reconnect=False), async
_replication_identity(slot,publication), and async
_open_replication_stream(config:PostgresChangeConfig,*,start_lsn=None,reconnect=False).
The last maps config.slot/publication/queue_capacity to native arguments. Dedicated
direct config is required; no query-pool lease or original-loop consumer task is owned
by this registry. Identity preflight issues no START_REPLICATION.

`PostgresReplicationIdentity` (defined in `vertebra.db.adapters.postgres.types`) and `PostgresCheckpoint` (defined alongside `PostgresChangeConfig`, `PostgresChangeSource`, `PostgresCheckpointStore`, `MemoryPostgresCheckpointStore`, `PostgresCheckpointConflictError`, and `PostgresReplicationGapError` in `vertebra.db.adapters.postgres.change_stream`) share `identity_key`:
tuple[system_identifier:str,database:str,slot:str,publication_fingerprint:str]. The
checkpoint additionally stores commit_end_lsn:u64 and next_event_id in1..2**63-1.
PostgresChangeConfig adds positive finite checkpoint_timeout=10.0.

```python
class PostgresCheckpointStore(Protocol):
    async def load(self, identity_key: tuple[str, str, str, str]) -> PostgresCheckpoint | None: ...
    async def save(self, checkpoint: PostgresCheckpoint, *, expected: PostgresCheckpoint | None) -> None: ...
```

Save atomically compares expected value, validates identity and monotone LSN/counter,
and returns only under its documented durability guarantee. Equal LSN requires equal
counter. Memory storage (`MemoryPostgresCheckpointStore` in `vertebra.db.adapters.postgres.change_stream`) is explicitly nondurable. Conflict raises adapter-owned
PostgresCheckpointConflictError(OperationalError) and terminates ownership rather
than silently adopting another consumer's checkpoint. Other terminal gap errors use
PostgresReplicationGapError(OperationalError) with identity_mismatch, wal_unavailable,
checkpoint_uncertain or retries_exhausted reason and original cause/SQLSTATE.

The source obtains identity before load, disables raw native auto-reconnect and owns
retry. One tracked shielded acknowledgement task closes the yield gate, saves CAS,
then sends native feedback. Caller cancellation does not retract a possibly durable
save. Successful save+ack resumes the same generation and preserves one pending next
record. Any uncertain store/native outcome closes the old generation, reloads durable
state and resumes from its matching LSN/counter together. No old record is yielded
during reconciliation. Aclose joins/reconciles within its absolute deadline and
fails closed if state remains uncertain. No ack occurs on yield/close.

## Arrow selection result

PostgresArrowResult (defined in `vertebra.db.adapters.postgres.types`) is an adapter-owned immutable record with
buffer:bytes|core.ArrowCursor, row_count:int, columns:tuple[str,...], used_shm:bool.
Selection uses inline IPC at/below threshold and SHM above it; streaming yields SHM
per batch. The generic ArrowCursor gains optional nonnegative row_count metadata,
checking conflicts with known rows/native count without any engine import. Native
select_to_arrow accepts max_result_bytes=268435456; callers may raise its validated
limit or stream. Builder/value limits and overflow errors are explicit in the owning
worksheets. No generic str(value) substitutes for a type-specific encoding.


## Exact PostgreSQL value records

This is the complete adapter-owned Python value contract. Implement the records in
`vertebra.db.adapters.postgres.types` with `msgspec.Struct`, `frozen=True`,
`gc=False`, and the field order/defaults below. Tuple fields and byte fields are
owned immutable values. Ordinary JSON containers retain Python JSON mutability;
parameter extraction validates and copies them before detached work. No wrapper
imports or eagerly requires the optional native wheel. Constructors validate
shape, discriminants and integer bounds without I/O; integer fields reject bool. Invalid Python construction
raises ValueError/TypeError; the native boundary maps incompatible target-OID,
shape/range or encoding errors to canonical DataError before submission.

```python
from __future__ import annotations

from datetime import date, datetime, time
from decimal import Decimal
from typing import Literal
from uuid import UUID

import msgspec

type TemporalKind = Literal["date", "time", "timestamp", "timestamptz"]
type TemporalSpecial = Literal[
    "positive_infinity", "negative_infinity", "out_of_range",
    "end_of_day", "wire_text",
]
type PostgresJsonValue = (
    None | bool | int | float | str
    | list[PostgresJsonValue] | dict[str, PostgresJsonValue]
)

class PostgresColumn(msgspec.Struct, frozen=True, gc=False):
    name: str
    type_oid: int
    format: Literal[0, 1]
    type_modifier: int
    table_oid: int
    attribute_number: int

class PostgresQueryResult(msgspec.Struct, frozen=True, gc=False):
    columns: tuple[PostgresColumn, ...]
    rows: tuple[tuple[PostgresValue, ...], ...]
    affected_rows: int
    command_tag: str
    transaction_status: Literal["I", "T", "E"]

class PgInterval(msgspec.Struct, frozen=True, gc=False):
    months: int
    days: int
    microseconds: int

class PgArray(msgspec.Struct, frozen=True, gc=False):
    values: tuple[PostgresValue, ...]
    dimensions: tuple[int, ...]
    lower_bounds: tuple[int, ...]
    element_oid: int

class PgComposite(msgspec.Struct, frozen=True, gc=False):
    type_oid: int
    field_names: tuple[str, ...]
    field_oids: tuple[int, ...]
    values: tuple[PostgresValue, ...]

class PgTemporal(msgspec.Struct, frozen=True, gc=False):
    kind: TemporalKind
    value: int | None
    special: TemporalSpecial | None
    wire_text: str | None = None
    date_style: str | None = None
    time_zone: str | None = None

class PgJson(msgspec.Struct, frozen=True, gc=False):
    value: PostgresJsonValue

class PgBitString(msgspec.Struct, frozen=True, gc=False):
    data: bytes
    bit_length: int

class PgVector(msgspec.Struct, frozen=True, gc=False):
    values: tuple[float, ...]
    kind: Literal["vector", "halfvec"]

class PgParameter(msgspec.Struct, frozen=True, gc=False):
    value: PostgresValue
    type_oid: int

class PostgresErrorInfo(msgspec.Struct, frozen=True, gc=False):
    sqlstate: str | None
    message: str
    severity: str | None
    diagnostics: tuple[tuple[str, str], ...]

class PostgresPipelineOperation(msgspec.Struct, frozen=True, gc=False):
    sql: str
    parameters: tuple[PostgresValue | PgParameter, ...] = ()
    parameter_oids: tuple[int, ...] | None = None
    kind: Literal["execute", "select"] = "execute"

class PostgresPipelineOutcome(msgspec.Struct, frozen=True, gc=False):
    index: int
    status: Literal["result", "error", "skipped"]
    result: PostgresQueryResult | None = None
    error: PostgresErrorInfo | None = None
    failed_index: int | None = None

type PostgresValue = (
    None | bool | int | float | str | bytes | Decimal | UUID
    | date | time | datetime
    | PgInterval | PgArray | PgComposite | PgTemporal | PgJson
    | PgBitString | PgVector
    | list[PostgresValue] | tuple[PostgresValue, ...]
    | dict[str, PostgresJsonValue]
)
```

`PgParameter` is an outer parameter marker, not a SQL value nested inside an array,
composite, JSON document or another PgParameter. Parameter APIs accept
`PostgresValue | PgParameter`; row values use `PostgresValue`. Plain list/tuple/dict
cases are binding conveniences or ordinary JSON projection, not alternative
representations for decoded PostgreSQL arrays/composites/vectors/bitstrings.

### Column and result invariants

- OIDs are unsigned 32-bit values; result type OIDs, declared type OIDs, composite field
  OIDs and array element OIDs are 1..2**32-1. `table_oid=0` means no originating
  table. Only a parameter-OID inference vector may contain 0.
- `type_modifier` is signed 32-bit; `attribute_number` is signed 16-bit and 0 means no source
  attribute. The format is the integer 0/text or 1/binary, not bool.
- Names and columns retain order, duplicates and empty labels. Every row has exactly
  the column count. SQL NULL is None, distinct from empty bytes/string and from a
  non-NULL array/composite whose elements/fields are NULL.
- `affected_rows` is unsigned 64-bit; DDL/empty commands use 0 when their command tag has
  no affected count. `command_tag` is exact server text. `transaction_status` is the
  actual final I/T/E state for the operation's scope, not an assumed commit result.
- QueryResult stores tuple rows; the established driver hook explicitly returns
  `[_project_public_row(row) for row in result.rows], tuple(c.name for c in result.columns)`. The private projection maps only a top-level PgJson(None) to None for the established high-level API; explicit rich native/query_multi results remain lossless.

### Intervals, arrays and composites

PgInterval uses independent signed 32-bit months, signed 32-bit days and signed 64-bit
microseconds. Mixed signs remain independent; no timedelta conversion loses
months. PostgreSQL interval infinity sentinel triples are preserved exactly when
the server supports them. The record does not invent a separate inconsistent
special flag.

PgArray values are flat row-major values. Rank is 0..6; dimensions and lower_bounds
have the same rank. Rank 0 is the canonical empty array, with both tuples empty and
no values. For nonempty arrays dimensions are positive signed 32-bit counts, their
checked product equals len(values), and each signed 32-bit lower bound plus its extent
has a signed 32-bit upper bound. Preserve non-one and negative lower bounds, element
OID and NULLs. Ordinary rectangular sequences may bind with inferred/declared
built-in element metadata and one-based bounds; ragged, untyped-empty or all-NULL
ambiguous inputs require an explicit target. Decoded arrays always use PgArray.

PgComposite preserves type_oid, ordered field names, ordered positive field OIDs
and values with exactly equal lengths. A known named composite uses ordered
non-dropped catalog attributes. An anonymous binary record uses type_oid 2249 and,
when no real names are available, positional names f1,f2,... without guessing
names from unrelated RowDescription. Nested values use the shared codecs. A SQL
NULL composite is None; a non-NULL composite with NULL fields is PgComposite.
Unknown text records without field metadata remain text in their exact outer
Column OID until discovered; they are never guessed CSV records.

### Temporal units and discriminants

The epoch is PostgreSQL 2000-01-01. `date` value is signed 32-bit days; `time` value is
signed 64-bit microseconds since midnight; `timestamp` value is signed 64-bit microseconds
from a naive 2000-01-01; `timestamptz` value is signed 64-bit microseconds from the UTC
instant 2000-01-01T00:00:00Z. No float epochs or nanosecond widening is involved.

For raw temporal variants value is required and wire_text/date_style/time_zone
must all be None. A finite explicit raw wrapper may use special=None. Native
projection uses date/time/datetime where representable, UTC-aware datetime for
timestamptz, and these exact tags otherwise:

| special | Exact invariant |
| --- | --- |
| positive_infinity | date uses value=2**31-1; timestamp/timestamptz use 2**63-1; time is invalid |
| negative_infinity | date uses value=-2**31; timestamp/timestamptz use -2**63; time is invalid |
| out_of_range | finite date/timestamp/timestamptz raw integer outside Python's supported range; no infinity sentinel |
| end_of_day | kind=time and value=86_400_000_000, exactly 24:00:00 |
| wire_text | kind=timestamptz, value=None; exact nonempty wire_text and captured nonempty DateStyle/time_zone are required |

Finite time values must lie 0..86_400_000_000 inclusive. Invalid finite raw server
ranges fail rather than being relabeled out_of_range. Wire-text and raw-value
payloads are mutually exclusive. Kind determines the actual temporal OID; explicit
PgParameter overrides must agree with it.

The wire_text variant preserves abbreviation-only non-ISO timestamptz output when
an unambiguous instant cannot be recovered within a vetted timezone library's
range. Preserve the exact original output and current ParameterStatus DateStyle
and TimeZone; no global abbreviation guess or extrapolation is permitted. Every
unresolved TemporalText/PgTemporal(special="wire_text") typed bind raises canonical
DataError before submission in text, binary, pipeline and COPY paths, even with
matching captured DateStyle/TimeZone and in the first pipeline operation. The
independent timezone_abbreviations input configuration makes matching output
settings insufficient proof of an invertible instant. Callers explicitly resolve
the instant into a raw temporal value or intentionally bind chosen raw text.
Binary temporal values remain fully supported and independent of display settings.

### JSON and typed NULL

SQL NULL uses None. `PgParameter(None,3802)` is typed SQL NULL; `PgJson(None)` or
`PgParameter(PgJson(None),3802)` is JSON null. PgJson selects JSONB by default;
explicit target OID 114 selects JSON and 3802 selects JSONB. Its value must be a
cycle-free JSON value with string object keys and finite floats; Decimal/UUID/custom
objects require an explicit caller JSON representation. Arbitrary integer values
are not rounded through f64. Text/byte payloads never silently double as decoded
JSON strings: PgJson("null") is the JSON string "null", not JSON null. Decoded JSON uses ordinary Python JSON values except top-level JSON null, which is PgJson(None) in rich native results. SQL NULL remains None and inner JSON null remains None inside its container. The established high-level driver and pipeline convenience projections map the top-level marker to None; raw query/query_multi records preserve it.

PgParameter.type_oid is a positive unsigned 32-bit OID. A per-call parameter_oids vector
has exactly the parameter count;0 requests inference. Conflicting positive vector
OID and wrapper OID is DataError. None without a type follows server inference;
ambiguous NULL retains the canonical server error rather than inventing a type.

### Bitstrings and vectors

PgBitString has a signed 32-bit nonnegative bit_length, and len(data) equals
ceil(bit_length/8) exactly. Bits are MSB-first within each byte; unused low bits in
the last byte are zero. `(b"",0)` is valid. For example `(b"\xa0",3)` represents 101.
Nonzero unused padding, extra/missing bytes, negative/overflow lengths and mutable
buffers stored as data are invalid. Decode bit/varbit values to this record, not
bytes, tuples or unspecified views. A bytea result remains bytes. Binding the
wrapper without a more specific target uses varbit OID 1562; an explicit bit target
OID 1560 retains its server typmod rules.

PgVector.kind is exactly vector or halfvec. Dimension is 1..16000 inclusive, the
pgvector 0.8.1 value limit; lower ANN-index dimension limits are not the wire value
limit. Values must be finite real numbers, not bool. Validate finiteness both before
and after the required narrowing. For vector, encode/decoded values use IEEE 754
binary32. For halfvec, input first narrows to binary32 as in the shared CPU helper and
server halfvec text conversion; encode then uses half 2.7.1 from_f32 ties-to-even
and rejects quantized infinity; decoded values are the exact binary16 values widened to Python float.
Decoded vector values are likewise exact binary32 values widened to Python float.
Signed zero and finite subnormals are preserved. Kind determines the extension
type name; TypeCatalog resolves its actual OID rather than hard-coding one.

Plain mutable buffer conveniences at binding time are copied to owned bytes by native extraction; canonical public row values and wrapper data fields are bytes.

All decoded vector/halfvec values use PgVector and all decoded SQL bitstrings use
PgBitString, including inside arrays/composites/COPY/pipeline results. Ordinary
numeric sequences are vector binding conveniences only after the target OID is
known as vector/halfvec; they never cause a result to have an ambiguous plain-list
representation. An extension OID unavailable to the current catalog requests text
and preserves owned text plus Column OID, with no guessed binary codec.

When post-result metadata enrichment encounters an unrecoverable transport, protocol
or cleanup error, the canonical source exception may carry `.completed_result`
with the original PostgresQueryResult (original columns, fallback text/values,
command tag/count and original ReadyForQuery status). Catalog results do not replace
it. Recoverable enrichment errors retain text/OID fallback and, inside an ambient
transaction, roll back only the enrichment savepoint. No completed SQL is replayed.

### Pipeline outcome and exception invariants

Operation SQL is one extended statement. parameters and parameter_oids have equal
length when the latter is supplied; operation.kind selects Python projection and
does not authorize a different wire protocol. Outcome.index is nonnegative and
ordered. A result outcome has only result set; an error outcome has only error set;
a skipped outcome has only failed_index set, referring to an earlier failed op.
No error/skipped outcome fabricates SQLSTATE or pretends to be successful row data.

The first failure raises the existing canonical shared DatabaseError subclass,
with immutable ordered `.outcomes` and integer `.failing_index`. ErrorInfo is owned
diagnostic data, not a competing exception class. Results conversion remains
inside the context transaction/savepoint until clean exit, including cardinality
checks. A caught server error still marks the scope failed and forces rollback.

Primary limits: [pgvector 0.8.1 vector/halfvec reference](https://github.com/pgvector/pgvector/blob/v0.8.1/README.md#reference),
[vector dimension definition](https://github.com/pgvector/pgvector/blob/v0.8.1/src/vector.h),
[halfvec dimension definition](https://github.com/pgvector/pgvector/blob/v0.8.1/src/halfvec.h).


The existing high-level PostgreSQLConfig adapter retains libpq's sslmode=prefer
default when no typed/DSN/PGSSLMODE override exists; the new raw native client/pool
constructor default remains verify-full. Implicit Unix configuration selects
disable. These defaults are tested at the adapter bridge, without weakening any
explicit verification mode. [libpq default](https://www.postgresql.org/docs/18/libpq-connect.html#LIBPQ-CONNECT-SSLMODE).


## Final activation, event and command ownership

Permanent native facade definitions live in `_sync.py::SyncPostgreSQLDriver` and
`_async.py::AsyncPostgreSQLDriver`. The canonical `driver.py` barrel and package,
config, registry, SQLSpec and SQLAlchemy paths select both together in `ch27:t01` and
`ch27:t02`; before that chapter the existing public pair remains coherent. Final-module
test imports switch to canonical public imports at activation. `ch27:t03` then `ch27:t04`
remove the private unreachable legacy async and sync bodies, preserving static validity
at every commit. The delivered barrel only imports the final classes and declares `__all__`;
no temporary module, fallback, duplicate body or compatibility alias survives.

Both driver facades structurally satisfy adapter-owned event Protocols (`PostgresNotificationDriver` and `PostgresReplicationDriver` in `vertebra.db.adapters.postgres.events`) with real
async hooks. PostgresNotificationDriver includes readonly config, async
_publish_notification(channel:str,payload:str)->None and async listener opening (`PostgresNotificationSource` in `vertebra.db.adapters.postgres.notify` wires this signature in `ch21:t03` and verifies canonical public driver imports in `ch27:t02`).
Sources uniformly await publish, without mode inspection or int/awaitable unions.
The async facade awaits its own numeric pg_notify execute in the same task and may
join its ambient transaction. The sync facade rejects publication from its active
Sync scope with InterfaceError before offload/I/O; otherwise asyncio.to_thread runs
normal numeric execute. Both explicitly forward timeout=10.0. A cancelled await
cannot withdraw already-running Sync SQL; native bounded cleanup still owns its
lease. Direct synchronous execute remains the supported Sync transaction route.
PostgresReplicationDriver provides readonly config and the async identity/open
hooks documented above. Dedicated handles remain paired with their own direct
AsyncPostgresClient, independent of the query pool, and close/prune deterministically.

Explicit driver.cancel uses a driver-private active-operation registry, separate
from ContextVars. Prefer the caller's active operation; otherwise cancel exactly
one active child/worker operation belonging to this driver. Idle is a no-op;
multiple candidates raise exact InterfaceError before a CancelRequest. Another
driver sharing the pool is never a candidate. Claim selection under a short lock,
release it before I/O, and retain the selected client/lease until the bounded native
generation-fenced claim finishes. A possibly sent cancel always retires the physical
session; an ambient transaction remains failed until rollback. Task.cancel remains
per-operation and cleanup outlives the Python future.

Before any pipeline frames/flush, `ch15:t02`'s private bounded SQL-head scanner validates
the entire batch. It skips leading empty semicolons, whitespace and nested/line
comments, recognizes complete unquoted identifiers, and rejects transaction aliases
BEGIN/COMMIT/END/ROLLBACK/ABORT/SAVEPOINT/RELEASE, START/PREPARE TRANSACTION and
SET [LOCAL|SESSION] TRANSACTION/SET SESSION CHARACTERISTICS. Ordinary SQL PREPARE
name AS and marker text inside quotes/bodies remain allowed. COPY is canonical
NotSupportedError; transaction control is InterfaceError, without synthetic SQLSTATE.
The first validation failure carries failing_index and immutable outcomes=() for
an entirely unsubmitted batch; native/Python scope failure persists even if caught.
Extended Parse rejects multiple nonempty statements; the guard never splits SQL.

AlloyDB transport exposes shared TransportStream::connect_alloydb_tls(options)
and finish_alloydb_metadata(stream,options,absolute_deadline). Existing connect
composes the same verified TLS1.3/identity and metadata stages under one deadline.
PostgreSQL obtains a ScopedLoginSnapshot, checks ensure_usable immediately after
verified TLS and before token-bearing metadata, then checks again before any IAM
password/SASL response. Expired/inside-skew snapshots drop the stream with zero
metadata bytes and exact GoogleCloudAuthError, never a replay or invented lifetime.
Legacy raw-token/non-PG behavior retains the shared composed transport path.

Both clients expose query(sql,parameters=(),*,parameter_oids=None,timeout=None)
and query_multi(sql,*,timeout=None); async methods return their existing Awaitable
result shapes. None adds no command deadline; positive finite seconds is validated
before reservation (`ch17:t01`–`t02`). One absolute deadline starts at native operation reservation
before worker scheduling and covers required parameter discovery, writes/flush,
reads, all simple-query results and active result enrichment. Pool acquisition,
connection/reset/close budgets remain separate; CPU decoding and Python projection
are not preemptible, so timeout is no total wall-time promise. Existing Rust query
entry points delegate to deadline-aware implementations with None.

Client expiry claims only the active generation, preserves OperationalError with
sqlstate/vendor_code None, and uses independent cleanup capped by the validated
connect_timeout across cancel transport and original drain/retirement together.
Real server 57014 received first remains QueryCancelledError; a later cleanup 57014
does not replace the primary client expiry. Never replay SQL. Expired optional
metadata work after original ReadyForQuery is skipped in favor of exact owned
text/OID/context fallback; fatal active enrichment attaches the original completed
result through AfterCompletion. High-level single/multi calls forward timeout;
execute_many applies it separately per command. No pipeline/COPY/portal timeout or
global command default is added.

[^pg-config]: `src/py/vertebra/db/adapters/postgres/config.py::PostgreSQLConfig` — PostgreSQL DSN, TLS, Cloud SQL, AlloyDB, and IAM configuration.
[^pg-driver]: `src/py/vertebra/db/adapters/postgres/driver.py::SyncPostgreSQLDriver` — Public SyncPostgreSQLDriver and AsyncPostgreSQLDriver adapter surface.
[^arrow-cursor]: `src/py/vertebra/db/arrow.py::ArrowCursor` — Public ArrowCursor result wrapper over VTBARROW ShmCapsule.
