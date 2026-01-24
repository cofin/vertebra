# Recovery: ORM Extensions

## Common Issues

### IPC Protocol Mismatch
- **Symptom**: Client sends request, DB Node logs decoding error.
- **Fix**: Ensure `vertebra-core` message definitions match the Python `Msgspec` structs. Use `scripts/sync_protocol.py` if available.

### Serialization Failures
- **Symptom**: `sqlalchemy` fails to decode result row.
- **Fix**: Check `VertebraCursor` row factory. It must return `tuple` or `Row` objects, not raw Arrow batches or MsgPack dicts.

### Transaction State Desync
- **Symptom**: `COMMIT` sent but DB Node considers session idle.
- **Fix**: Verify `VertebraConnection` maintains strict 1:1 mapping with a DB Node session ID. Implement heartbeats if needed.

## Debugging
- Enable trace logging: `VERTEBRA_LOG_LEVEL=TRACE`.
- Inspect IPC dump: `vertebra-cli dump-shm`.
