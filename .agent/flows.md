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

## [ ] Flow: vertebra-v2-rewrite
*Link: [./specs/vertebra-v2-rewrite/](./specs/vertebra-v2-rewrite/)*
*Status: ACTIVE - Master Plan for Rust Migration*

---

## [ ] Flow: Requires Review / Integration

- [ ] **federated-brokers**: [./specs/federated-brokers/](./specs/federated-brokers/)
- [ ] **serialization-optimization**: [./specs/serialization-optimization/](./specs/serialization-optimization/)
- [ ] **pyclient-sdk_20260125**: [./specs/pyclient-sdk_20260125/](./specs/pyclient-sdk_20260125/)
- [ ] **refactor_20260125**: [./specs/refactor_20260125/](./specs/refactor_20260125/)
- [ ] **async_notifier_bridge_20260126**: [./specs/async_notifier_bridge_20260126/](./specs/async_notifier_bridge_20260126/)
- [ ] **optional-dependency-handling**: [./specs/optional-dependency-handling/](./specs/optional-dependency-handling/)
- [ ] **documentation-adoption**: [./specs/documentation-adoption/](./specs/documentation-adoption/)
- [ ] **fix-startup-module-import-and-polish-cli**: [./specs/fix-startup-module-import-and-polish-cli/](./specs/fix-startup-module-import-and-polish-cli/)
- [ ] **cli-polish**: [./specs/cli-polish/](./specs/cli-polish/)

## [ ] Flow: fix-benchmarks-ipc
*Link: [./specs/fix-benchmarks-ipc/](./specs/fix-benchmarks-ipc/)*
*Status: ACTIVE - Fix IPC return path & BlackSheep*

---

## [ ] PRD: Async IPC Pipelining
*Link: [./specs/async-ipc-pipelining/](./specs/async-ipc-pipelining/)*
*Beads: vertebra-k4k*
*Status: PLANNING*

### Chapters:
- [ ] **async-eventfd**: [./specs/async-eventfd/](./specs/async-eventfd/) - *Beads: vertebra-k4k.1*
- [ ] **correlation-protocol**: [./specs/correlation-protocol/](./specs/correlation-protocol/) - *Beads: vertebra-k4k.2*
- [ ] **lock-free-dispatch**: [./specs/lock-free-dispatch/](./specs/lock-free-dispatch/) - *Beads: vertebra-k4k.3* - blocked by correlation-protocol
- [ ] **python-concurrent**: [./specs/python-concurrent/](./specs/python-concurrent/) - *Beads: vertebra-k4k.4* - blocked by correlation-protocol
