# Recovery: Production Readiness

## Strategy
This plan does **not** remove core features. Mitigations must preserve scope.

If `vertebra-broker` integration is blocked:
- **Mitigation**: Implement broker in smaller slices (internal providers first, external providers next), but **do not** replace it with local-only pooling.

If `sqlspec` APIs shift during implementation:
- **Mitigation**: Update the adapter to the latest stable API and adjust tests; do not drop SQLSpec support.

If Bun streaming shows instability:
- **Mitigation**: Fix IPC/backpressure issues and add diagnostics. Streaming remains required for MVP.

If Protobuf IPC remains unsupported:
- **Mitigation**: Keep MessagePack for MVP and track Protobuf (`betterproto`) as a post-MVP deliverable.
