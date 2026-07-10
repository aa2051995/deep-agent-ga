# Run Lifecycle Improvements

This document describes the improvements made to handle run lifecycle, stream subscriptions, consumer management, and worker task handling in the stream backend.

## Summary of Changes

### 1. Handle Tracking for Ack/Retry Operations

**Problem**: No tracking mechanism for run handles during retry operations, making it difficult to correlate retries with specific runs.

**Solution**: Added `RunHandle` class to track ack/retry operations:

```python
@dataclass
class RunHandle:
    thread_id: str
    run_id: str
    subscription_id: str
    created_at: float = field(default_factory=time.time)
    retry_count: int = 0
    last_retry_at: float | None = None
    status: Literal["active", "completed", "failed", "cancelled"] = "active"
```

**Benefits**:
- Track retry attempts per run
- Correlate stream subscriptions with runs
- Monitor run lifecycle state
- Enable retry policies with exponential backoff

### 2. Stream Subscription Lifecycle Cleanup

**Problem**: Run streams persisted after run completion, causing resource leaks.

**Solution**: Implemented automatic cleanup in `StreamSubscriptionManager`:

```python
async def iter_events(self, managed: ManagedThreadSubscription, ...):
    try:
        while True:
            event = await managed.subscription.next_event(timeout)
            if event_filter.is_terminal(event):
                # Clean up subscription when run completes
                await self.cleanup_run_subscription(managed.subscription_id)
                return
            yield event
    finally:
        await self.unregister_subscription(managed.subscription_id)
        await managed.close()
```

**Benefits**:
- No orphan stream subscriptions
- Automatic cleanup on terminal events (completed, failed, interrupted)
- Proper resource cleanup on disconnection

### 3. Consumer Lifecycle Management

**Problem**: Orphan RabbitMQ consumers after stream completion.

**Solution**: Enhanced `RabbitMQStreamBroker` with consumer tracking:

```python
class RabbitMQStreamBroker:
    def __init__(self, ...):
        self._subscription_consumers: dict[str, list[Any]] = {}
        self._consumer_refs: dict[int, str] = {}  # subscriber_id -> stream_name
        
    async def close(self):
        # Close all consumers by stream
        for stream_name, consumers in self._subscription_consumers.items():
            for consumer in consumers:
                try:
                    await consumer.close()
                except Exception:
                    logger.exception(...)
```

**Benefits**:
- Track all consumers per stream
- Ensure cleanup on broker shutdown
- Prevent connection leaks
- Monitor consumer health

### 4. UI Auto-Reconnect and Active Run Banner

**Problem**: No UI feedback when server goes down mid-run and reconnects.

**Solution**: Added monitoring in frontend:

```typescript
useEffect(() => {
    const checkActiveRun = async () => {
        const response = await fetch(`${apiUrl}/threads/${threadId}/runs?limit=1&status=running`);
        const runs = await response.json();
        const activeRun = runs.find(r => ACTIVE_RUN_STATUSES.has(r.status));
        if (activeRun) {
            setActiveRun({ threadId, runId: activeRun.run_id });
        }
    };
    
    void checkActiveRun();
    const interval = setInterval(checkActiveRun, 5000);
    return () => clearInterval(interval);
}, [apiUrl, threadId]);
```

**Active Run Banner**: Shows when disconnected run is detected:
```tsx
{visibleActiveRun && (
    <div className="active-run-banner">
        <strong>Active run found</strong>
        <span>Can continue from saved checkpoint</span>
        <button onClick={() => continueActiveRun(visibleActiveRun)}>
            Continue streaming
        </button>
        <button onClick={() => stopActiveRun(visibleActiveRun)}>
            Stop run
        </button>
    </div>
)}
```

### 5. Load Balancer Compatibility

**Problem**: API behavior behind load balancer wasn't documented.

**Solution**: Added headers and documentation:

**Headers Added**:
- `X-Session-Id`: Sticky session identifier
- `X-Request-Id`: Request tracking
- `X-Server-Id`: Backend instance identifier

**Load Balancer Requirements**:
- **Sticky Sessions**: Required for WebSocket connections
- **Connection Draining**: 30s graceful shutdown
- **Health Checks**: `/health` endpoint for readiness
- **Extensions**: RabbitMQ Stream protocol support

### 6. Worker Task Cancellation on Shutdown

**Problem**: Running tasks not gracefully cancelled when worker shuts down.

**Solution**: Implemented signal handling in worker:

```python
import signal
import asyncio

class WorkerShutdownManager:
    def __init__(self):
        self._shutdown_event = asyncio.Event()
        self._active_tasks: dict[str, asyncio.Task] = {}
        
    def setup_signal_handlers(self):
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, self._handle_shutdown)
            
    async def _handle_shutdown(self):
        logger.info("worker.shutdown.start active_tasks=%s", len(self._active_tasks))
        self._shutdown_event.set()
        
        # Give tasks 10s to complete gracefully
        await asyncio.wait(
            self._active_tasks.values(),
            timeout=10.0,
            return_when=asyncio.ALL_COMPLETED
        )
        
        # Cancel remaining tasks
        for task_id, task in self._active_tasks.items():
            if not task.done():
                logger.warning("worker.shutdown.cancel task_id=%s", task_id)
                task.cancel()
```

**Benefits**:
- Graceful shutdown with timeout
- Task cancellation tracking
- Proper cleanup of run states
- No zombie tasks

## API Changes

### New Endpoint: GET /threads/{thread_id}/runs/active

Returns active run if one exists:

```json
{
  "thread_id": "...",
  "run_id": "...",
  "status": "running",
  "created_at": "2024-01-01T00:00:00Z"
}
```

### Enhanced Endpoint: POST /threads/{thread_id}/runs/{run_id}/resume

Added `resume_check` option to verify run status before resuming.

## Configuration Changes

### New Environment Variables

```bash
# Consumer lifecycle
STREAM_BACKEND_CONSUMER_TIMEOUT=300.0  # seconds
STREAM_BACKEND_CONSUMER_MAX_RETRIES=3

# Task cancellation
STREAM_BACKEND_SHUTDOWN_TIMEOUT=10.0  # seconds

# Load balancer headers
SERVER_ID=stream-backend-1

# RabbitMQ stream retention
STREAM_BACKEND_RABBITMQ_MAX_AGE_HOURS=12  # Stream data retention (default: 12 hours)
STREAM_BACKEND_RABBITMQ_STREAM_MAX_BYTES=104857600  # Max stream size in bytes (default: 100MB)
```

## Testing

### Unit Tests Added

1. `test_streaming_manager.py`: Subscription lifecycle tests
2. `test_celery_scheduler.py`: Worker shutdown tests
3. `test_consumer_lifecycle.py`: Consumer cleanup tests

### Integration Tests

1. Server restart during active run
2. Worker crash and recovery
3. Load balancer failover
4. Consumer timeout handling

## Monitoring

### Metrics Added

- `stream_subscriptions_active`: Active stream subscriptions
- `run_handles_active`: Active run handles
- `consumers_active`: Active RabbitMQ consumers
- `worker_shutdown_seconds`: Shutdown duration

### Logs Added

- `stream.subscription.cleanup`: When subscriptions are cleaned
- `worker.task.cancel`: When tasks are cancelled
- `consumer.orphan.detected`: When orphan consumers found

## Migration Guide

1. Update environment variables for new configuration
2. Deploy new worker code with signal handlers
3. Update frontend to use active run monitoring
4. Configure load balancer with sticky sessions
5. Monitor metrics for subscription cleanup

## Future Improvements

1. Add distributed lock for run handles
2. Implement run checkpoint recovery
3. Add consumer health monitoring
4. Support multi-region failover
