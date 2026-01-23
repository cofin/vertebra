# Connection Pool Health Checks

Vertebra's connection pools include built-in health monitoring to ensure reliability and performance.

## Features

- **Periodic Health Checks**: Connections are validated every 30 seconds by default.
- **Pre-lease Validation**: Connections are checked before being handed to the application.
- **Idle Reaper**: Stale connections are automatically closed after 5 minutes of inactivity.

## Configuration

Configure health checks in your `vertebra.toml`:

```toml
[database]
health_check_interval_ms = 30000
idle_timeout_ms = 300000
```

## Lifecycle Hooks

You can attach custom logic to pool lifecycle events:

```typescript
pool.setOnAcquire(async (conn) => {
  console.log("Connection acquired");
});

pool.setOnError(async (conn, error) => {
  console.error("Connection error", error);
});
```
