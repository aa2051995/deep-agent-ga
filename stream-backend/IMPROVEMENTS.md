# Stream Backend Improvements Summary

This document summarizes the improvements made to handle run lifecycle, stream subscriptions, and worker management.

## Changes Made

### 1. Worker Ack/Retry Handling (`worker/tasks.py`)

- **Run Status Updates**: Workers now properly update run status to "running" when starting, "success" on completion, and "error" on failure
- **Automatic Retry**: Added `autoretry_for=(Exception,)` with exponential backoff and max 3 retries for both `run_agent` and `resume_agent` tasks
- **Proper Error Logging**: Enhanced error logging with proper exception handling
- **Recovery Function**: Added `recover_stale_runs()` function to detect and mark runs interrupted by worker crashes

### 2. Stream Subscription Lifecycle (`app/streaming.py`)

- **Subscription Tracking**: Added `_active_subscriptions` dictionary to track all active stream subscriptions
- **Subscription IDs**: Each subscription now gets a unique ID for tracking
- **Proper Cleanup**: Subscriptions are properly registered on creation and unregistered on close
- **Active Run Detection**: Added `get_active_run_for_thread()` to check for active runs on a thread
- **Run ID Tracking**: Subscriptions can now track which run they're associated with
- **Global Cleanup**: Added `close_all_subscriptions()` for graceful shutdown

### 3. Consumer Lifecycle (`app/event_bus.py`, streaming.py)

- **Thread-Safe Close**: RabbitMQ subscriptions now use locks to prevent race conditions when closing
- **Orphan Prevention**: Consumers are properly tracked and cleaned up on errors
- **Better Error Handling**: Improved error handling in consumer close operations

### 4. Server Restart Recovery (`app/main.py`)

- **Startup Recovery**: Added `recover_interrupted_runs()` to run on app startup
- **Mark Interrupted**: All pending/running runs are marked as "interrupted" with recovery metadata
- **Lifecycle Events**: Proper lifecycle events are emitted for recovered runs
- **Graceful Shutdown**: All subscriptions are closed on app shutdown

### 5. Active Run Detection API

- **New Endpoint**: Added `GET /threads/{thread_id}/active-run` endpoint
- **UI Integration**: UI can check for active runs on reconnect
- **Response**: Returns thread_id, run_id, and has_active_run flag
- **Use Case**: Allows UI to show "continue active run" banner on reconnect

### 6. Load Balancer Compatibility (`app/main.py`)

- **Session Headers**: Added middleware to set X-Session-Id and X-Request-Id headers
- **Server Identification**: Added X-Server-Id header for load balancer routing
- **Sticky Sessions**: Headers enable proper session affinity for streaming connections
- **CORS Updates**: Exposed headers include session and request IDs

### 7. Tests (`tests/test_streaming_manager.py`)

Added comprehensive tests for:
- Subscription registration and unregistration
- Active run detection with no runs
- Active run detection with pending runs
- Closing all subscriptions

## Architecture Improvements

### Run State Machine

```
pending → running → success/error/interrupted
          ↑              ↓
          └── recovery ←┘
```

### Subscription Lifecycle

```
create → register → stream → unregister → close
                           ↓
                    (on disconnect/error)
```

### Worker Lifecycle

```
receive → update(running) → execute → update(success/error)
                                             ↓
                                    (on crash) → recovery
```

## Configuration

### Environment Variables

- `STREAM_BACKEND_CELERY_ACKS_LATE`: Enable late task acknowledgment (default: true)
- `STREAM_BACKEND_CELERY_PREFETCH_MULTIPLIER`: Worker prefetch multiplier (default: 1)
- `SERVER_ID`: Unique server identifier for load balancer headers

### Load Balancer Settings

For proper operation behind a load balancer:

1. Enable sticky sessions based on `X-Session-Id` header
2. Configure health checks on `/health` endpoint
3. Set connection timeouts for SSE streams appropriately
4. Don't buffer SSE responses (X-Accel-Buffering: no)

## Usage Examples

### Check for Active Run

```bash
curl http://localhost:8000/threads/{thread_id}/active-run
```

Response:
```json
{
  "thread_id": "...",
  "run_id": "...",
  "has_active_run": true
}
```

### Resume After Reconnect

1. UI disconnects during active run
2. UI reconnects
3. UI calls `/threads/{thread_id}/active-run`
4. If active run exists, show "Continue" banner
5. User can call `/threads/{thread_id}/runs/{run_id}/stream` to resume

## Known Limitations

- Server restart marks all runs as interrupted; in-memory state is lost
- RabbitMQ stream consumers are not persisted across restarts
- Subscription tracking is in-memory only (not distributed)

## Future Improvements

- [ ] Distribute subscription tracking via Redis
- [ ] Implement run execution state persistence
- [ ] Add WebSocket-based run state synchronization
- [ ] Implement run timeouts at worker level
