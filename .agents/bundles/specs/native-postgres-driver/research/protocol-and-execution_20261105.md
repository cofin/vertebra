---
type: Research
flow_id: native-postgres-driver
title: "PostgreSQL protocol and execution constraints"
description: "Primary sources, repository facts, and read-only wire probes for the complete one-PR native PostgreSQL driver."
status: stable
state: promoted
promoted_to: native-postgres-driver
created_at: "2026-11-05T00:00:00Z"
updated_at: "2026-11-07T04:15:00Z"
generated:
  by: flow-setup
  at: "2026-11-07T04:15:00Z"
verified:
  - by: agent:flow-setup
    at: "2026-11-07T04:15:00Z"
    commit: cdb978d3
sources:
  - id: pg-sasl
    resource: https://raw.githubusercontent.com/rust-postgres/rust-postgres/postgres-protocol-v0.6.12/postgres-protocol/src/authentication/sasl.rs
    title: postgres-protocol 0.6.12 SCRAM-SHA-256 and channel-binding implementation
  - id: pg-frontend
    resource: https://raw.githubusercontent.com/rust-postgres/rust-postgres/postgres-protocol-v0.6.12/postgres-protocol/src/message/frontend.rs
    title: postgres-protocol 0.6.12 frontend frame encoders
  - id: pg-backend
    resource: https://raw.githubusercontent.com/rust-postgres/rust-postgres/postgres-protocol-v0.6.12/postgres-protocol/src/message/backend.rs
    title: postgres-protocol 0.6.12 backend message parser
  - id: tokio-pg-query
    resource: https://raw.githubusercontent.com/rust-postgres/rust-postgres/tokio-postgres-v0.7.18/tokio-postgres/src/query.rs
    title: tokio-postgres per-query Sync emission constraint
  - id: tokio-pg-rustls
    resource: https://raw.githubusercontent.com/jbg/tokio-postgres-rustls/v0.14.0/src/lib.rs
    title: tokio-postgres-rustls 0.14.0 private digest visibility
  - id: pg18-overview
    resource: https://www.postgresql.org/docs/18/protocol-overview.html
    title: PostgreSQL 18 Protocol 3.0 overview
  - id: pg18-flow
    resource: https://www.postgresql.org/docs/18/protocol-flow.html
    title: PostgreSQL 18 extended query, pipeline, and COPY protocol flow
  - id: pg18-repl
    resource: https://www.postgresql.org/docs/18/protocol-replication.html
    title: PostgreSQL 18 logical replication wire protocol
  - id: pgbouncer-cfg
    resource: https://www.pgbouncer.org/config.html
    title: PgBouncer transaction-mode prepared statement and cancellation configuration
  - id: pg18-datetime
    resource: https://www.postgresql.org/docs/18/datetime-config-files.html
    title: PostgreSQL 18 timezone_abbreviations input configuration
tags:
  - postgres
  - protocol
  - scram
  - arrow
  - replication
---

# PostgreSQL Protocol and Execution Constraints

<!-- truth: start -->
- **Owned Protocol 3.0 State Machine**: `vertebra-postgres` uses `postgres-protocol =0.6.12` encoders/parsers [^pg-frontend] [^pg-backend] [^pg18-overview] rather than `tokio-postgres` query methods (which append `Sync` per statement) [^tokio-pg-query], and computes `tls-server-end-point` digests via `x509-parser =0.18.1` for `SCRAM-SHA-256-PLUS` [^pg-sasl] [^tokio-pg-rustls].
- **Single-`Sync` Cold Pipelines, Portals, & `COPY`**: Cold pipelines emit `Parse`/`Describe`/`Bind`(text)/`Describe`/`Execute` sequences terminated by one `Sync`, portals pin their lease/transaction, and `COPY` enforces explicit `CopyFail` / `CancelRequest` drain recovery [^pg18-flow] [^pgbouncer-cfg].
- **Lossless Values & `pgoutput` v1**: Unresolved temporal text is output-only (`DataError` on typed rebind) [^pg18-datetime], and logical replication streams `pgoutput` v1 committed transactions with durable compare-and-save LSN acknowledgments [^pg18-repl].
<!-- truth: end -->

Verified 2026-11-05 against the local native-postgres-driver draft and native-data-platform crate survey plus primary sources. Research only; no production edits or runtime validation. Local facts below describe the inspected checkout. Recommendations are design inferences.

## Protocol and delivery boundary

The complete native PostgreSQL driver ships in one PR. Small task/commit boundaries
do not reduce its authentication, query, data-plane, event or adapter acceptance.
Use postgres-protocol under an owned state machine; pinned tokio-postgres query
methods insert their own Sync and cannot supply the required one-Sync batch seam.
[Pinned query source](https://raw.githubusercontent.com/rust-postgres/rust-postgres/tokio-postgres-v0.7.18/tokio-postgres/src/query.rs).

## Exact authentication APIs and policy

Use `frontend::startup_message(parameters, &mut BytesMut)`, `ssl_request`, `password_message`; `backend::Message::parse(&mut BytesMut) -> io::Result<Option<Message>>`; `authentication::md5_hash`; `sasl::{SCRAM_SHA_256, SCRAM_SHA_256_PLUS, ChannelBinding, ScramSha256}`. Construct `ScramSha256::new(password, binding)`; encode `message()` using `sasl_initial_response`; pass Continue bytes to `update()`; send `message()` through `sasl_response`; validate Final through `finish()`. AuthenticationOk and ReadyForQuery still follow. Crate helpers validate embedded NULs and message count/size conversions. Reuse them instead of extending the local unchecked handwritten encoders. [Frontend encoders](https://raw.githubusercontent.com/rust-postgres/rust-postgres/postgres-protocol-v0.6.12/postgres-protocol/src/message/frontend.rs), [backend parser](https://raw.githubusercontent.com/rust-postgres/rust-postgres/postgres-protocol-v0.6.12/postgres-protocol/src/message/backend.rs).

The vetted SCRAM implementation caps iterations at100000 before PBKDF2; that private cap is not configurable. It validates nonce prefix, final signature and method state. Its numeric parser accepts u32 digits without an explicit nonzero check: reject iteration0 at the integration boundary before update, without replacing the cryptography. Treat malformed packets, invalid state, excessive iterations and failed final verification as connection failures. Do not retry authentication on that same stream. [Pinned SCRAM implementation](https://raw.githubusercontent.com/rust-postgres/rust-postgres/postgres-protocol-v0.6.12/postgres-protocol/src/authentication/sasl.rs).

Full authentication uses disable/prefer/require channel-binding policy. Prefer
chooses advertised PLUS when usable binding exists; when binding is usable but only
plain SCRAM is advertised, use upstream ChannelBinding::unrequested (GS2 y). Disable
or genuinely unavailable binding uses unsupported (GS2 n). Require fails before
credentials unless PLUS and binding are available. Validate actual GS2 bytes and
downgrade rejection. [RFC5802 section6](https://datatracker.ietf.org/doc/html/rfc5802#section-6),
[pinned binding implementation](https://raw.githubusercontent.com/rust-postgres/rust-postgres/postgres-protocol-v0.6.12/postgres-protocol/src/authentication/sasl.rs).

## PLUS is not solved by the TLS adapter dependency

PostgreSQL uses tls-server-end-point certificate binding, not TLS exporter bytes. Hash DER certificate bytes using the certificate signature hash, substituting SHA256 for MD5/SHA1. [PostgreSQL SASL](https://www.postgresql.org/docs/18/sasl-authentication.html), [RFC5929 section4.1](https://datatracker.ietf.org/doc/html/rfc5929#section-4.1).

Verified tokio-postgres-rustls0.14.0 exposes no public certificate-to-binding-byte helper: its digest function is pub(super) inside a private module, wrapper stream construction is private, and TlsStream returns a tokio-postgres ChannelBinding with pub(crate) bytes and no getter. Adding this crate does not supply binding bytes to an owned postgres-protocol handshake. [Pinned adapter](https://raw.githubusercontent.com/jbg/tokio-postgres-rustls/v0.14.0/src/lib.rs), [opaque binding type](https://raw.githubusercontent.com/rust-postgres/rust-postgres/tokio-postgres-v0.7.18/tokio-postgres/src/tls.rs).

postgres-native-tls forwards native_tls::TlsStream::tls_server_end_point, but introduces a different TLS stack from the rustls/ring requirement. No reusable public rustls helper was found in these checked crates. The PLUS followup must adopt a vetted public helper/upstream seam or explicitly scope and test a certificate-binding adapter. Local TransportStream::TlsTcp exposes the rustls session but contains no existing binding implementation. [Native TLS adapter](https://raw.githubusercontent.com/rust-postgres/rust-postgres/postgres-native-tls-v0.5.3/postgres-native-tls/src/lib.rs).

## Full-driver implementation constraints

**Versioning.** Protocol3.0 supports PG14–18. PG18 protocol3.2 changes cancellation secrets to variable length;3.1 is reserved. Pinned frontend startup is3.0, cancel_request takes i32 PID/key, and backend parsing lacks NegotiateProtocolVersion. Explicitly promise3.0 first;3.2 needs more framing. [Protocol versions](https://www.postgresql.org/docs/18/protocol-overview.html).

**Pipeline recovery.** First extended-query error makes the server discard later frames until Sync: later statements have synthesized skipped/aborted outcomes, not individual server errors. ReadyForQuery transaction status determines reuse/rollback. Earlier CommandComplete does not prove commit if the implicit transaction later fails. Portals live only through their transaction. Cancellation requires a second connection, has no success response, races query completion, and requires draining the original request before reuse. Prevent a delayed cancel from reaching a new borrower. [Protocol flow](https://www.postgresql.org/docs/18/protocol-flow.html).

**Flush and backpressure.** Assert one logical batch flush and one wire Sync independently; one flush does not mean one write/TCP packet/TLS record. Read while writes block and bound memory; a write-all-then-read implementation can deadlock. Define COPY/simple-query-in-pipeline behavior. [libpq pipeline guidance](https://www.postgresql.org/docs/18/libpq-pipeline-mode.html).

**Unknown OIDs.** Local encode_bind_frame asks for binary results universally. Text fallback requires metadata before Bind plus per-column result formats; arbitrary binary bytes cannot be decoded as UTF8. Resolve extension, array-element and composite-attribute OIDs. First-use pipelines need an explicit discovery phase or cached metadata. Pinned frontend::bind supports result-format iterators.

**Arrays.** Pinned types exposes scalar, temporal and array codecs but no numeric/interval codec. Arrays carry dimensions, lower bounds, element OID, nulls and zero-dimensional empties. Preserve these semantics or explicitly bound support. [Pinned type codecs](https://raw.githubusercontent.com/rust-postgres/rust-postgres/postgres-protocol-v0.6.12/postgres-protocol/src/types/mod.rs).

**Numeric/interval.** Numeric is16-bit count/weight/sign/display-scale plus base10000 digits: retain precision/scale, NaN/infinities, and specify Decimal-to-Arrow overflow policy. Interval is independent i64 microseconds/i32 days/i32 months; timedelta loses months. Choose a lossless Python representation and Arrow month-day-nanosecond policy, including mixed signs and infinities. [Numeric send/recv](https://raw.githubusercontent.com/postgres/postgres/REL_18_STABLE/src/backend/utils/adt/numeric.c), [interval send/recv](https://raw.githubusercontent.com/postgres/postgres/REL_18_STABLE/src/backend/utils/adt/timestamp.c).

**COPY/Arrow.** COPY binary fields use exact type binary send formats, without widening. Streaming frame boundaries need not match row boundaries. Local whole-buffer decode allocates Vecs and ignores flags. Cover fragmentation, critical flags, extension length, field-count/trailer/truncation, CopyFail and server errors. Inference: row-oriented big-endian DataRow values need decoding/column building; zero-copy describes the SHM ownership handoff, not the entire network-to-Arrow path. [COPY format](https://www.postgresql.org/docs/18/sql-copy.html).

**pgvector.** Existing local broad framing is correct, but dimensions allow u16::MAX versus upstream signed16-bit/max16000; decoders allow zero/nonfinite data; local half conversion rounds midpoints upward rather than ties-to-even and may turn finite overflow into Inf. Compare golden bytes against real server send/recv, including midpoint/subnormal/overflow/dimension cases. [vector codec](https://raw.githubusercontent.com/pgvector/pgvector/master/src/vector.c), [half codec](https://raw.githubusercontent.com/pgvector/pgvector/master/src/halfvec.c), [half conversion](https://raw.githubusercontent.com/pgvector/pgvector/master/src/halfutils.h), [vector limit](https://raw.githubusercontent.com/pgvector/pgvector/master/src/vector.h), [half limit](https://raw.githubusercontent.com/pgvector/pgvector/master/src/halfvec.h).

**PgBouncer.** Transaction-mode named protocol statements work when max_prepared_statements>0; SQL PREPARE/EXECUTE are different. Local capacity helper has a capability override, but PostgresPool construction ignores it. Specify mode plus verified/configured capability and reset/reconnect invalidation. PgBouncer1.26 adds ParameterStatus pgbouncer.version, pgbouncer.pool_mode, pgbouncer.max_prepared_statements; older servers need configured fallback. Test transaction mode with support both enabled and disabled. [Configuration](https://www.pgbouncer.org/config.html#max_prepared_statements), [changelog](https://www.pgbouncer.org/changelog.html).

**Replication.** Separate from NOTIFY: replication=database, simple-query START_REPLICATION, CopyBothResponse, CopyData WAL/keepalive and standby-status feedback with persisted acknowledged LSN. Pinned backend Message lacks CopyBothResponse; add framing explicitly. [Replication protocol](https://www.postgresql.org/docs/18/protocol-replication.html).

A bounded baseline is pgoutput proto_version1, streaming/two_phase off across PG14–18. Versions2/3/4 add in-progress/two-phase/parallel streaming. Scope relation/type cache, NULL/unchanged TOAST/key/old/new tuples, transaction ordering, UPDATE/DELETE/TRUNCATE, slot ownership, snapshot/bootstrap, reconnect/resume, duplicate delivery, backpressure and durable acknowledgment policy. Idle NOTIFY needs a receive pump but is not a durable change source. [Options](https://www.postgresql.org/docs/18/protocol-logical-replication.html), [message formats](https://www.postgresql.org/docs/18/protocol-logicalrep-message-formats.html).

**Dependency correction.** Pinned postgres-protocol0.6.12 uses edition2024/MSRV1.85, sha2 0.11/hmac0.13/md-5 0.11/rand0.10 with no default features. The survey's sha2 0.10 baseline does not imply digest-version unification. tokio-postgres0.7.18 uses protocol0.6.12/types0.2.14. Inspect actual lockfile resolution. [Protocol manifest](https://raw.githubusercontent.com/rust-postgres/rust-postgres/postgres-protocol-v0.6.12/postgres-protocol/Cargo.toml), [Client manifest](https://raw.githubusercontent.com/rust-postgres/rust-postgres/tokio-postgres-v0.7.18/tokio-postgres/Cargo.toml).


## Complete driver in one PR: scope and concrete baseline

Authentication including PLUS/cloud integration, query/codecs/cache/pooling, COPY/Arrow, notifications/replication, pipeline/cancellation and Python cutover share one complete acceptance boundary. Small tasks are commit/review boundaries.

### Lossless Python and Arrow values

Keep ordinary bool/int/float/str/bytes/UUID, JSON and representable Python date/time/datetime behavior. Own PostgreSQL-specific frozen msgspec records in the adapter types module; Rust carries a typed native value enum until Python completion hydration. Do not run Python conversion on a Tokio I/O task. The following are proposed fixed defaults, rather than a new configurable codec-policy registry:

| PostgreSQL family | Python value | Stable default Arrow storage |
| --- | --- | --- |
| numeric | Decimal constructed from exact coefficient/scale or exact decimal spelling; preserve display scale and canonical NaN/positive/negative infinity | UTF8 exact decimal spelling with PostgreSQL OID/typmod metadata for every numeric column, including constrained numeric. No late Decimal128-to-string schema change and no float conversion. |
| interval | PgInterval(months:i32, days:i32, microseconds:i64); preserve mixed signs and raw components. Recognize the server's reserved infinity triples. | Struct of the same three integer components; infinity retains the reserved raw tuple. No unconditional microseconds-to-nanoseconds multiplication. |
| array | PgArray(values:tuple of flattened nullable values, dimensions:tuple[int,...], lower_bounds:tuple[int,...], element_oid:int). Empty is zero dimensions, empty bounds and values. | Struct(dimensions:List<Int32>, lower_bounds:List<Int32>, values:List<element Arrow type>); preserve parent SQL NULL separately from empty array and null elements. Store element OID/type identity in field metadata. |
| composite | PgComposite(type_oid:int, field_names:tuple[str,...], field_oids:tuple[int,...], values:tuple[Any,...]); preserve order. Named fields come from pg_attribute excluding dropped fields; anonymous records use documented ordinal names f1,f2,... where no names exist on the wire. | Struct of recursively typed fields with ordered OID/name metadata. Parent NULL differs from a present record with all-null children. Anonymous heterogeneous shapes use a documented UTF8 PostgreSQL record fallback when no fixed schema exists. |
| date/timestamp/timestamptz outside Python range, infinity, or time24 | PgTemporal(kind, value raw PostgreSQL epoch integer, special None/positive-infinity/negative-infinity). Representable finite values continue as ordinary Python values. | Stable Struct(kind:Int8 tagging finite/infinity, raw:Int32 for date or Int64 for timestamp); metadata defines date/timestamp type and PostgreSQL2000 epoch. Time remains raw Int64 microseconds since midnight, including86400000000. Use this stable lossless storage for the entire affected temporal field rather than promote after first infinity. |
| vector/halfvec | Typed vector record or existing vector view with f32/quantized-f16 elements; reject zero dimensions, nonfinite elements and dimension mismatch. | FixedSizeList<Float32> when a known fixed dimension exists, otherwise List<Float32>. Half values can be exactly widened to f32; metadata retains halfvec type. |
| bit/varbit used by pgvector | Packed bytes plus exact bit length, not a boolean list inferred from byte length | Struct(bytes:Binary, bit_length:Int32), with zero unused low padding bits and BIT/VARBIT identity metadata. |
| unknown OID | PostgreSQL text fallback with OID metadata retained in the result description | UTF8 with OID/type metadata; never pretend opaque binary is text. |

These fallbacks use standard Arrow structural/storage types, so IPC readers can read them without a custom extension registration. They trade some semantic convenience for complete roundtripping. Optional explicit conversions into conventional Arrow date/timestamp/decimal/MonthDayNano schemas must be checked and reject overflow, unrepresentable infinity or loss of array bounds; they are not required for the baseline. Arrow's decimal precision and temporal units do not cover all PostgreSQL values; structs/UTF8 keep stable schemas without silent coercion. [Arrow columnar format](https://arrow.apache.org/docs/format/Columnar.html), [PostgreSQL numeric limits](https://www.postgresql.org/docs/18/datatype-numeric.html), [PostgreSQL date/time ranges](https://www.postgresql.org/docs/18/datatype-datetime.html).

Array decode validates0–6 dimensions, equal dimension/bound lengths, checked element-count products, nullable element lengths and recursive codec depth. Accept plain rectangular nested sequences for conventional one-based array binds, while PgArray binds preserve explicit bounds. [PostgreSQL array semantics](https://www.postgresql.org/docs/18/arrays.html), [MAXDIM and row-major ordering](https://raw.githubusercontent.com/postgres/postgres/REL_18_STABLE/src/include/utils/array.h). Binary composite fields carry their own OIDs and nullable lengths; do not guess fields from comma splitting. [record_send/recv](https://raw.githubusercontent.com/postgres/postgres/REL_18_STABLE/src/backend/utils/adt/rowtypes.c).

Use vetted half::f16 conversion, not the current handwritten rounding routine; check finite input and finite result after quantization, then use to_be_bytes. Include ties-to-even/subnormal/overflow fixtures compared with server halfvec bytes. Bit length0 is valid for unconstrained varbit even though the current pgvector helper rejects it; enforce actual target typmod separately and validate zero padding. [half API](https://docs.rs/half/2.7.1/half/struct.f16.html), [PostgreSQL bit implementation](https://raw.githubusercontent.com/postgres/postgres/REL_18_STABLE/src/backend/utils/adt/varbit.c). Pin pgvector integration fixtures to0.8.1 for PG14 compatibility: its bit helper supports PostgreSQL13+, whereas current master requires15+. [Pinned pgvector compatibility](https://raw.githubusercontent.com/pgvector/pgvector/v0.8.1/src/bitutils.h).

### Cold pipeline metadata without violating one Sync

Use a cold-statement text execution path, not a hidden prepare round trip inside results(): for each operation append Parse, Describe(statement), Bind, Describe(portal), Execute in execution order; append one final Sync and submit/flush the whole batch once. Cold Bind uses text parameters and requests text results. ParameterDescription/RowDescription arrive before corresponding DataRow frames, so the receiving state machine can decode known OIDs and retain unknown values as text. Cache successful descriptions and type registry entries for subsequent operations; known cached statements choose binary inputs and per-column binary outputs, with text output for unknown columns. This supports dependency ordering, including a later statement referring to earlier DDL, without pre-parsing all statements first.

There is a real protocol constraint here: an entirely unknown result schema cannot choose per-column formats before the server describes it in that same batch. Do not claim cold first-use output is universally binary. The charter requires binary codecs and a single-Sync pipeline, not binary output for every cold query. Normal prepare/query paths may discover catalog types before execution; pipeline unknown-OID fallback remains text, with no hidden replay or metadata query while the batch is in flight. A registered/known composite or pgvector OID has typed text decoding on cold output; a type created within the same batch can remain the explicitly documented unknown-OID text fallback for that batch.

Keep public SyncPipeline/AsyncPipeline signatures and list results: execute entries are affected counts; select entries are list[dict] as current conformance expects. Preserve typed DatabaseError subclasses on failure. A suitable adapter-owned PostgresPipelineError can carry the failing DatabaseError as cause, failing index and ordered outcome records for completed/failed/skipped statements; exception type selection and naming remain the root planner's API decision. A completed wire command before a later failure can have its effects rolled back; separate execution status from transaction commit status. Never return a misleading all-success list after an error. No catch-and-replay of a failed batch, including statements with side effects.

The protocol skips remaining commands after the first failure until Sync. The queue must account for Parse/Bind/Describe failures before Execute, EmptyQueryResponse, PortalSuspended where permitted, CloseComplete and interleaved async messages. Drain to exactly the ReadyForQuery count implied by emitted Syncs; one complete batch emits one. Explicit transactions stay pinned and can remain failed; preserve caller rollback responsibility, while owned implicit transactions finish at Sync. [Server extended/pipeline flow](https://www.postgresql.org/docs/18/protocol-flow.html), [libpq ordered/aborted outcomes](https://www.postgresql.org/docs/18/libpq-pipeline-mode.html).

Text decoding must honor captured ParameterStatus DateStyle, IntervalStyle, TimeZone and client_encoding, including changes during a session. ISO-only parsers or silently overriding existing session_gucs are not equivalent behavior. PostgreSQL has four date output styles, date-order variants, four interval output styles, BC/large years and named or numeric zone outputs. No checked Rust dependency provides a complete PostgreSQL text decoder for all these forms; explicit lexical adapters based on the server output grammar are implementation scope and need multiple small worksheets if necessary. Do not assert that chrono alone covers PostgreSQL's full year range, or resolve historical LMT/ambiguous abbreviations through a global abbreviation map. Binary codecs remain independent of those textual styles. [Server formatting implementation](https://raw.githubusercontent.com/postgres/postgres/REL_18_STABLE/src/backend/utils/adt/datetime.c).

### Portal streaming and COPY ownership

A row-limited portal must pin its physical connection and transaction for its entire iterator lifetime. If the caller already owns a transaction, borrow it without committing/rolling it back. Otherwise create an explicit owned transaction before Bind/Execute. Fetch with repeated Execute(max_rows)+Sync while ReadyForQuery reports transaction status T; retain statement/portal handles and lease. Exhaustion closes the portal and commits only an owned successful read transaction. Early close/aclose, generator error, cancellation and finalization must close the portal and drain; roll back only an owned transaction, or leave an externally owned failed transaction for its caller. A destructor can enqueue cleanup but cannot block or release an unsynchronized lease. Streams reject overlapping operations on their pinned connection. Arrow SHM ownership can outlive the socket lease after complete serialization, but an active stream cannot release the socket early.

COPY owns an exclusive connection mode. For COPY FROM, send CopyFail on producer/encoder failure, stop production when server ErrorResponse arrives, then finish the correct simple/extended recovery boundary. In extended COPY FROM, do not rely on the Sync sent immediately after Execute: the server ignores it while COPY-in is active; send a fresh Sync after CopyDone/CopyFail or a detected server error as needed by the tracked state. Success requires CommandComplete and ReadyForQuery. For COPY TO, early consumer exit sends CancelRequest or deliberately closes/discards the physical connection; CopyFail is not a valid COPY-out abort. Drain CopyData/CopyDone/CommandComplete/ReadyForQuery after cancellation before pool reuse. Preserve the original producer/server failure when cleanup also fails. Handle notices/status messages in COPY mode and incremental PGCOPY parsing independently from CopyData frames. [COPY protocol](https://www.postgresql.org/docs/18/protocol-flow.html#PROTOCOL-COPY).

### pgoutput v1 complete baseline, slots and bootstrap

Use a dedicated direct/session replication connection with startup replication=database and pgoutput proto_version1, streaming off and two_phase off. This reads committed transactions across PG14–18; it does not need in-progress transaction or prepared-transaction streaming to implement the charter. Use the same transport/TLS/auth stack and an explicit CopyBoth parser extension. Implement Begin/Commit/Origin/Relation/Type/Insert/Update/Delete/Truncate/Message, updating relation metadata when Relation is resent. Tuple cells have four states: SQL NULL, unchanged TOAST, text, binary. Unchanged TOAST is a distinct tagged value, never NULL or an empty string. Preserve old/key/new tuple distinctions and truncate CASCADE/RESTART IDENTITY flags. Reject unnegotiated higher-version messages rather than silently skipping their payload. [pgoutput versions/options](https://www.postgresql.org/docs/18/protocol-logical-replication.html), [message layouts](https://www.postgresql.org/docs/18/protocol-logicalrep-message-formats.html).

Slot/publication policy can be explicit and conservative without deferring CDC: pre-existing persistent slot/publication are the default; verify database, pgoutput plugin, slot activity and usable WAL state. A source can offer explicit provision/bootstrap and drop_owned_slot operations; never silently replace an unrelated slot or recreate an invalidated one at the newest LSN. A failed missing-WAL/invalidated-slot resume is a resnapshot-required error. Publication DDL/filter/projection semantics come from PostgreSQL, not client-side guesses. [Slot state view](https://www.postgresql.org/docs/18/view-pg-replication-slots.html), [publication definition](https://www.postgresql.org/docs/18/sql-createpublication.html).

This charter tails/resumes pre-existing caller-owned slots/publications. Consistent
initial snapshot alignment, slot/publication provisioning and disposal remain caller
responsibilities. No automatic provision/bootstrap/drop operation is implemented.
[Snapshot lifetime and slot protocol](https://www.postgresql.org/docs/14/protocol-replication.html),
[logical replication limits](https://www.postgresql.org/docs/18/logical-replication-restrictions.html).

### CDC delivery, acknowledgment and reconnect

Preserve AsyncChangeSource.stream() compatibility and add adapter-owned explicit acknowledge(commit_end_lsn). Default delivery is at least once with manual acknowledgment. Yield ordered begin/DML/commit events through a bounded queue; v1 committed output permits this without materializing an entire transaction in client memory. Consumers requiring atomic application stage changes until commit. Mark commit boundaries explicitly so zero-change progress is acknowledgeable. Only completely emitted contiguous transactions can be acknowledged; reject future, partial or out-of-order acknowledgment that would skip unprocessed work.

The caller durably records its downstream effects and resume checkpoint before acknowledging. The source sends flush/apply positions only up to that acknowledged commit-end boundary; never acknowledge a received WAL position or queue insertion. Keep write/receive LSN separate from durable flush/apply LSN. A timed status writer must continue independently of application backpressure; answer requested keepalive promptly and reconnect from the last durable acknowledgment after disconnect. On restart accept the caller's persisted checkpoint containing system identity, database/slot identity and last acknowledged commit-end LSN, validate it against the current slot, and fail rather than advance through a detected gap. Slot crash recovery can replay recent transactions, so stable dedup identity belongs in event payload (systemid, slot, begin final_lsn, ordinal), not a promise of exactly-once delivery. [Logical slot replay and consumer responsibility](https://www.postgresql.org/docs/18/logicaldecoding-explanation.html), [WAL/status wire fields](https://www.postgresql.org/docs/18/protocol-replication.html).

Local ChangeEvent.event_id is an int but ChangeFeed.flush_to_parquet fixes its Arrow type to int64; do not encode (LSN<<32)|ordinal there. Retain a monotonically increasing in-range source event counter and put stable source coordinates in payload. A checkpoint-store API can be adapter-owned or supplied by the caller; existing generic AsyncChangeSource needs no new engine-specific methods. Persistence and acknowledgment ordering are correctness requirements regardless of store API.

### Transaction poolers and cancellation acceptance

Disable persistent named statements unless the pooler is session/direct or verified/configured to multiplex named protocol statements. Modern PgBouncer startup metadata can attest mode/capacity; old versions use explicit config. A no-cache transaction-mode request uses unnamed Parse/Bind/Execute in one transaction segment. Hold non-HOLD portal streaming inside an explicit transaction; route LISTEN and replication through direct/session DSNs and fail clearly if only a transaction pooler is available. Statement pooling cannot provide multi-statement transaction/stream semantics and must not be silently treated as transaction pooling. [PgBouncer feature matrix](https://www.pgbouncer.org/features.html).

Always cancel through the original target endpoint/dialer/pooler using the BackendKeyData belonging to that frontend connection. Do not send a PgBouncer synthetic key directly to PostgreSQL. Pin the operation/connection generation while cancellation is pending. Whenever CancelRequest may have been sent, retire the original physical connection before any later borrower, even if the query completed or drained successfully; a delayed cancel has no success acknowledgment. Drain under the cleanup deadline where safe, otherwise close immediately. A later operation obtains a replacement session. Repeated cancel when idle is an idempotent no-op. Tests must cover direct/session/transaction poolers, prepared cache enabled/disabled, backend rotation, pool reset/reconnect invalidation, active pg_sleep cancellation, cancellation after completion, task cancellation and cancellation connection failure, plus proof that another lease's query is unaffected. PgBouncer cancel forwarding can wait/fail, so cancel success cannot mean merely sending bytes. [PgBouncer cancellation timeout](https://www.pgbouncer.org/config.html#cancel_wait_timeout).

### PLUS completion within the same PR

No new TLS stack is needed. Pin x509-parser0.18.1 without default features; parse the TLS leaf certificate DER and require no trailing bytes. Dispatch explicit signature OIDs before PSS-specific SignatureAlgorithm::try_from, since the classifier omits DSA-SHA224/SHA256. Validate complete PSS parameter and nested DER consumption before using RsaSsaPssParams::hash_algorithm_oid; its helper discards an inner remainder. A present parameter SEQUENCE with omitted hashAlgorithm defaults to SHA1; the whole parameters field must not be absent for a signature. Map explicit RSA/ECDSA/DSA SHA2 signature OIDs to vetted Sha224/Sha256/Sha384/Sha512 and only SHA1/MD5 to SHA256, then hash the original leaf DER. MD2 is unsupported rather than substituted. PSS uses the signature hash, not MGF hash. Unknown/no-hash/multiple-hash algorithms make tls-server-end-point unavailable; require fails and prefer can use advertised plain SCRAM. This preserves the selected TLS mode's certificate-verification policy; encryption-only Require/Prefer without a supplied CA does not imply PKI verification. RFC9266 defines tls-exporter, not a universal SHA256 fallback for EdDSA; PostgreSQL uses tls-server-end-point. [Pinned X509 parser API](https://raw.githubusercontent.com/rusticata/x509-parser/x509-parser-0.18.1/src/signature_algorithm.rs), [parser manifest](https://raw.githubusercontent.com/rusticata/x509-parser/x509-parser-0.18.1/Cargo.toml), [RFC5929 selection](https://datatracker.ietf.org/doc/html/rfc5929#section-4.1), [RFC9266](https://datatracker.ietf.org/doc/html/rfc9266), [RSA/PSS identifiers](https://datatracker.ietf.org/doc/html/rfc4055), [DSA/ECDSA SHA2 identifiers](https://datatracker.ietf.org/doc/html/rfc5758), [libpq PSS extraction behavior](https://raw.githubusercontent.com/postgres/postgres/REL_18_STABLE/src/interfaces/libpq/fe-secure-openssl.c).

### Executed read-only prototype evidence

On the already-running local PostgreSQL18.3 test container, with a fresh Unix-domain connection for each raw-wire probe and no persistent schema/data changes:

- Binary send functions produced numeric1.2300 as0002000000000004000108fc; a tiny decoder reconstructed Decimal1.2300 with matching exponent. Python Decimal construction retained a much larger scale-bearing value while decimal context precision was2.
- Mixed interval1month,-2days,11045000006microseconds decoded as those independent components. An i64-max microsecond prototype exceeded i64 after multiplication by1000, proving unconditional Arrow nanosecond conversion is unsafe.
- A two-dimensional int4 array decoded dimensions(2,2), lower_bounds(0,3), elementOID23, flat values(1,None,2,3). Empty array decoded zero dimensions. Binary records included each field OID and nullable length.
- Date infinity was7fffffff; timestamp infinity7fffffffffffffff; interval infinity was the all-max microseconds/day/month tuple; time24:00 was86400000000microseconds.
- A cold batch containing two Parse/DescribeS/Bind-text/DescribeP/Execute sequences and one final Sync was submitted once and returned two completions, one ReadyForQuery, Decimal text1.2300 and the exact non-default-bound array text. This proves no preflight is required for cold text results. The prototype used unbuffered socket submission; a production Rust flush-counter test is still required.
- SELECT1; SELECT1/0; SELECT3 in one extended batch returned one success, one error and one ReadyForQuery. The third statement produced no result. A subsequent SELECT4 on that connection succeeded after draining.
- Execute(max_rows2)+Sync outside BEGIN returned PortalSuspended then ReadyI; resuming the portal failed. Inside BEGIN, repeated Execute+Sync yielded2,2,1 rows with ReadyT; Close and ROLLBACK restored ReadyI.
- COPY(SELECT1/0) TO STDOUT BINARY returned Error/ReadyI and a subsequent SELECT1 succeeded.
- CancelRequest on a separate Unix socket interrupted this probe's own pg_sleep(10) with SQLSTATE57014/ReadyI; the cancel socket closed without a response and the original connection successfully ran SELECT1 afterward.

These are prototype observations on PG18.3, not acceptance for PG14–18, TLS/PLUS, PgBouncer, logical replication, compiled wheels or production Rust performance. Each implementation task still needs its focused native tests, followed by complete conformance/package/benchmark gates in the same PR.


## Reviewed query and acknowledgment recovery

An ordinary cold extended query uses one group with text parameters/results when
metadata is unavailable. Resolve required parameter type metadata before Parse;
resolve owned text-result metadata after its ReadyForQuery. A simple Query destroys
the unnamed prepared statement, and an idle Sync can release a transaction-pooler
backend. No Parse/Describe/Sync -> catalog Query -> Bind path may rely on the old
unnamed statement. Named prepared preflight pins the required backend and re-Parses
after intervening catalog Query. No implicit BEGIN wraps every ordinary statement:
transaction-prohibited commands must remain usable.
[PostgreSQL extended protocol](https://www.postgresql.org/docs/18/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY).

The high-level change source owns reconnection and opens its native stream with
reconnect disabled. Atomic durable compare-and-save precedes native flush/apply;
any uncertain save/ack failure closes the old generation, reloads the checkpoint,
and resumes from its matching LSN and event counter. A delivery barrier prevents
old-generation events during reconciliation. This closes save-success/native-ack
failure and cancellation races without relabeling replayed records.

Designated static login credentials carry real expires_at_unix_ms and declared
scopes. Unknown/expired/inside-skew lifetime or missing grant fails before password
transmission; the provider constructs a cache with the actual expiry. Existing
unscoped consumers retain their legacy behavior.

Repository substrate facts: shared PostgresPool is SHM admission without sockets;
Connection is a lifecycle trait; shared task abort alone sends no CancelRequest;
ShmArrowStreamWriter accepts encoded IPC and pads append calls. The native engine
supplies real pool/session ownership and exact IPC framing. Optional extensions
reuse Rust source primitives but do not extract another cdylib's Rust Arc/PyRef.

Planning verification is YAML/state/graph/link/shell syntax plus independent review.
Production source, native test suites, live cloud and full-version performance
acceptance are not implied by research or read-only protocol prototypes.


## Unresolved temporal display is output-only

PostgreSQL's TimeZone supplies recognized IANA abbreviations, and the independent
timezone_abbreviations setting configures additional accepted datetime input
abbreviations. These input definitions may be changed by administrator-controlled
configuration; DateStyle and TimeZone alone do not prove an unresolved display is
invertible. The plan therefore preserves exact PgTemporal(special="wire_text")
output/context but rejects all such typed binds with DataError before text, binary,
pipeline or COPY submission. Callers explicitly resolve the instant or intentionally
bind raw text. This is a conservative design inference from the input-configuration
contract, not runtime acceptance. [PostgreSQL 18 datetime configuration](https://www.postgresql.org/docs/18/datetime-config-files.html).

[^pg-sasl]: postgres-protocol 0.6.12 SCRAM-SHA-256 and channel-binding implementation (https://raw.githubusercontent.com/rust-postgres/rust-postgres/postgres-protocol-v0.6.12/postgres-protocol/src/authentication/sasl.rs).
[^pg-frontend]: postgres-protocol 0.6.12 frontend frame encoders (https://raw.githubusercontent.com/rust-postgres/rust-postgres/postgres-protocol-v0.6.12/postgres-protocol/src/message/frontend.rs).
[^pg-backend]: postgres-protocol 0.6.12 backend message parser (https://raw.githubusercontent.com/rust-postgres/rust-postgres/postgres-protocol-v0.6.12/postgres-protocol/src/message/backend.rs).
[^tokio-pg-query]: tokio-postgres per-query Sync emission constraint (https://raw.githubusercontent.com/rust-postgres/rust-postgres/tokio-postgres-v0.7.18/tokio-postgres/src/query.rs).
[^tokio-pg-rustls]: tokio-postgres-rustls 0.14.0 private digest visibility (https://raw.githubusercontent.com/jbg/tokio-postgres-rustls/v0.14.0/src/lib.rs).
[^pg18-overview]: PostgreSQL 18 Protocol 3.0 overview (https://www.postgresql.org/docs/18/protocol-overview.html).
[^pg18-flow]: PostgreSQL 18 extended query, pipeline, and COPY protocol flow (https://www.postgresql.org/docs/18/protocol-flow.html).
[^pg18-repl]: PostgreSQL 18 logical replication wire protocol (https://www.postgresql.org/docs/18/protocol-replication.html).
[^pgbouncer-cfg]: PgBouncer transaction-mode prepared statement and cancellation configuration (https://www.pgbouncer.org/config.html).
[^pg18-datetime]: PostgreSQL 18 timezone_abbreviations input configuration (https://www.postgresql.org/docs/18/datetime-config-files.html).
