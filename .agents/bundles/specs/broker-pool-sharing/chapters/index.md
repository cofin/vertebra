# Chapters: Shared Pools for Any SQLSpec, Advanced Alchemy, or SQLAlchemy Config

`9` dependency-ordered chapters (`31` vertical-slice tasks) delivering broker-owned shared pools with safe SHM segment ownership, authenticated attach, cross-extension native engine registration, quarantine of uncertain connections, a fail-loud SQLSpec proxy, the framework-neutral DB-API package, and crash reclamation.

* [ch01 — SHM Segment Ownership](ch01-shm-segment-ownership/spec.md) — [planned] (`3` tasks) Atomic create-or-attach with owner liveness ([#95](https://github.com/cofin/vertebra/issues/95)), an owned-segment registry behind panic and signal cleanup ([#96](https://github.com/cofin/vertebra/issues/96)), and fail-loud Python attach.
* [ch02 — Pool Partitions and dbrpc Frames](ch02-pool-partitions-and-dbrpc/spec.md) — [planned] (`4` tasks) Stable XXH3 statement and bind keys, per-`pool_key` partitions with heartbeats, versioned `dbrpc` frames, and removal of in-memory transports.
* [ch03 — Engine Registry Capsule](ch03-engine-registry-capsule/spec.md) — [planned] (`2` tasks) Replaces the per-cdylib static registry with `BrokerEngineCapsule` and `_vertebra.BrokerEngineRegistry`, plus Python discovery at broker startup.
* [ch04 — Engine Backend Registration](ch04-engine-backend-registration/spec.md) — [planned] (`4` tasks) MySQL, Oracle, SQL Server, and DuckDB broker capsules.
* [ch05 — Lease Safety and Broker Host](ch05-lease-safety-and-host/spec.md) — [planned] (`4` tasks) Quarantine state ([#98](https://github.com/cofin/vertebra/issues/98)), authenticated attach ([#97](https://github.com/cofin/vertebra/issues/97)), `BrokerHost` and `vtb broker serve`, session affinity and streaming.
* [ch06 — SQLSpec Proxy](ch06-sqlspec-proxy/spec.md) — [planned] (`3` tasks) Typed provider seams without signature duck-typing or suppressed errors, and adapter-base proxy drivers.
* [ch07 — DB-API and SQLAlchemy Pool](ch07-dbapi-and-sqlalchemy-pool/spec.md) — [planned] (`4` tasks) `vertebra.db.dbapi`, the SQLAlchemy async adapter, and `BrokerPool`.
* [ch08 — Reclamation and Acceptance](ch08-reclamation-and-acceptance/spec.md) — [planned] (`4` tasks) PostgreSQL broker capsule, crash reclamation and disposal, multi-process acceptance, and pool metrics.
* [ch09 — Docs and OKF](ch09-docs-and-okf/spec.md) — [planned] (`3` tasks) User docs, changelog, the broker OKF chapter, and the limitations record.
