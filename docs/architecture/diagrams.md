# IPC Diagrams

## Shared Memory Layout

```
┌───────────────────────────────────────────────────────────────┐
│ ShmHeader (magic, abi_version, state, generation, heads, ...) │
├───────────────────────────────────────────────────────────────┤
│ Slot[0] (len + flags + payload bytes...)                      │
├───────────────────────────────────────────────────────────────┤
│ Slot[1]                                                       │
├───────────────────────────────────────────────────────────────┤
│ ...                                                           │
└───────────────────────────────────────────────────────────────┘
```

## SPSC Flow

```
Producer                     Shared Memory                     Consumer
--------                     -------------                     --------
write payload  ───────►  slot[tail]  ───────►  read payload
advance tail  ───────►  tail atomics ───────►  advance head
wait/wake     ◄───────  wait words  ───────►  wait/wake
```

## Buffer Channel Flow

```
BufferPool                SPSC Ring (metadata)               Consumer
----------                ---------------------              --------
acquire buffer   ───►  send (index,len,flags) ───►  recv metadata
write payload    ─────────────────────────────────────────►  read buffer
```
