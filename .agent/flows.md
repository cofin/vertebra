# Flow Registry

This file tracks all flows (features, bugs, refactors) for the project.
Each flow has its own detailed spec and plan in its respective folder.

---

## [x] Flow: server-foundation
*Link: [./archive/server-foundation/](./archive/server-foundation/)*
*Beads: Closed*

## [x] Flow: ipc-worker-bridge
*Link: [./archive/ipc-worker-bridge/](./archive/ipc-worker-bridge/)*
*Beads: Closed*

## [x] Flow: python-asgi-adapter
*Link: [./archive/python-asgi-adapter/](./archive/python-asgi-adapter/)*
*Beads: Closed*

## [x] Flow: rust-broker-resources
*Link: [./archive/rust-broker-resources/](./archive/rust-broker-resources/)*
*Beads: Closed*

## [x] Flow: frontend-native-bridge
*Link: [./archive/frontend-native-bridge/](./archive/frontend-native-bridge/)*
*Beads: Closed*

## [x] Flow: cleanup-and-packaging
*Link: [./archive/cleanup-and-packaging/](./archive/cleanup-and-packaging/)*
*Beads: Closed*

## [x] Flow: rust-arrow-encoder
*Link: [./archive/rust-arrow-encoder/](./archive/rust-arrow-encoder/)*
*Beads: Closed (Abandoned)*

## [x] Flow: advanced-networking
*Link: [./archive/advanced-networking/](./archive/advanced-networking/)*
*Beads: Closed*

## [x] Flow: packaging-distribution
*Link: [./archive/packaging-distribution/](./archive/packaging-distribution/)*
*Beads: Closed*

## [x] Flow: rust-native-router
*Link: [./archive/rust-native-router/](./archive/rust-native-router/)*
*Beads: Closed*

## [x] Flow: rust-broker-introspection
*Link: [./archive/rust-broker-introspection/](./archive/rust-broker-introspection/)*
*Beads: Closed*

## [x] Flow: rust-multipart-streaming
*Link: [./archive/rust-multipart-streaming/](./archive/rust-multipart-streaming/)*
*Beads: Closed*

---

## [x] Flow: vertebra-v2-rewrite
*Link: [./specs/vertebra-v2-rewrite/](./specs/vertebra-v2-rewrite/)*
*Status: SUPERSEDED by PRD: Vertebra 0.1.0 Ghost Framework Rewrite*

---

## [ ] PRD: Vertebra 0.1.0 — Ghost Framework Rewrite
*Link: [./specs/vertebra-010-rewrite/](./specs/vertebra-010-rewrite/)*
*Beads: vertebra-w4i*
*Status: PLANNED — 9 chapters, all specs complete*

### Chapters:
- [ ] **Ch1: wipe-slate**: [./specs/wipe-slate/](./specs/wipe-slate/) - *Beads: vertebra-w4i.1*
- [ ] **Ch2: scaffold-base**: [./specs/scaffold-base/](./specs/scaffold-base/) - *Beads: vertebra-w4i.2* - blocked by Ch1
- [ ] **Ch3: project-buildout**: [./specs/project-buildout/](./specs/project-buildout/) - *Beads: vertebra-w4i.3* - blocked by Ch2
- [ ] **Ch4: native-ipc**: [./specs/native-ipc/](./specs/native-ipc/) - *Beads: vertebra-w4i.4* - blocked by Ch2
- [ ] **Ch5: native-http**: [./specs/native-http/](./specs/native-http/) - *Beads: vertebra-w4i.5* - blocked by Ch4
- [ ] **Ch6: native-ffi**: [./specs/native-ffi/](./specs/native-ffi/) - *Beads: vertebra-w4i.6* - blocked by Ch4, Ch5
- [ ] **Ch7: wsgi-adapter**: [./specs/wsgi-adapter/](./specs/wsgi-adapter/) - *Beads: vertebra-w4i.7* - blocked by Ch6
- [ ] **Ch8: asgi-adapter**: [./specs/asgi-adapter/](./specs/asgi-adapter/) - *Beads: vertebra-w4i.8* - blocked by Ch6
- [ ] **Ch9: frontend-integration**: [./specs/frontend-integration/](./specs/frontend-integration/) - *Beads: vertebra-w4i.9* - blocked by Ch3, Ch6

---

## Superseded / Absorbed (by Ghost Framework Rewrite)

- [x] **vertebra-next**: [./specs/vertebra-next/](./specs/vertebra-next/) — *absorbed into PRD*
- [x] **async-ipc-pipelining**: [./specs/async-ipc-pipelining/](./specs/async-ipc-pipelining/) — *absorbed into Ch4*
- [x] **fix-benchmarks-ipc**: [./specs/fix-benchmarks-ipc/](./specs/fix-benchmarks-ipc/) — *superseded by clean slate*
- [x] **federated-brokers**: [./specs/federated-brokers/](./specs/federated-brokers/) — *deferred*
- [x] **optional-dependency-handling**: [./specs/optional-dependency-handling/](./specs/optional-dependency-handling/) — *deferred*
- [x] **documentation-adoption**: [./specs/documentation-adoption/](./specs/documentation-adoption/) — *deferred*


## Recently Archived
- [x] **v2-alignment**: [./archive/v2-alignment/](./archive/v2-alignment/)
- [x] **fix-benchmarks-ipc**: [./archive/fix-benchmarks-ipc/](./archive/fix-benchmarks-ipc/)
- [x] **async-eventfd**: [./archive/async-eventfd/](./archive/async-eventfd/)

## Recently Archived (Round 2)
- [x] **refactor_20260125**: [./archive/refactor_20260125/](./archive/refactor_20260125/)
- [x] **pyclient-sdk_20260125**: [./archive/pyclient-sdk_20260125/](./archive/pyclient-sdk_20260125/)
- [x] **fix-startup-module-import-and-polish-cli**: [./archive/fix-startup-module-import-and-polish-cli/](./archive/fix-startup-module-import-and-polish-cli/)
