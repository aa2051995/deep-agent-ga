# Smart Run Rescheduling Implementation

## Overview

This document describes the implementation of smart run rescheduling that prevents zombies runs (runs stuck in "running" state with dead Celery tasks) and gives users explicit control over active run recovery.

## Problem Statement

### Before Implementation

1. **Auto-Join Issue**: UI automatically resumed active runs without user consent
2. **Zombie Runs**: Runs in "running" state but Celery task already completed
3. **Infinite Loops**: Dead tasks could be rescheduled infinitely
4. **Poor UX**: No user control over active run recovery
5. **Wasteful Polling**: UI polled `/runs/{run_id}` every 3 seconds

### After Implementation

1. ✅ **User Consent Required**: Banner shows, user must click "Continue streaming"
2. ✅ **Zombie Detection**: Check Celery task status before rescheduling
3. ✅ **Reschedule Limit**: Maximum 2 reschedules per run
4. ✅ **User Control**: Explicit choice to continue or stop
5. ✅ **Efficient Monitoring**: Use real-time lifecycle events

## Architecture

### Backend Changes

#### 1. CeleryRunScheduler Enhancements (`worker/client.py`)

```python
class CeleryRunScheduler:
    def get_task_status(self, task_id: str) -> TaskStatus | None:
        """Get the status of a Celery task."""
        result = self.app.AsyncResult(task_id)
        return result.status  # PENDING, STARTED, SUCCESS, FAILURE, etc.
    
    def is_task_active(self, task_id: str) -> bool:
        """Check if a task is still running or pending."""
        status = self.get_task_status(task_id)
        return status in {"PENDING", "STARTED", "RETRY"}
```

**Task States:**
- **Active**: `PENDING`, `STARTED`, `RETRY` → Task is running
- **Inactive**: `SUCCESS`, `FAILURE`, `REVOKED` → Task completed

#### 2. Smart Reschedule Logic (`app/service.py`)

```python
async def resume_run(self, thread_id: str, run_id: str, ...):
    run = await self.repo.get_run(thread_id, run_id)
    
    # Check if Celery task is active
    celery_task_id = run.metadata.get("celery_task_id")
    if self.run_scheduler and celery_task_id:
        if self.run_scheduler.is_task_active(celery_task_id):
            return True  # Task still running, no action needed
    
    # Task is dead, check reschedule limit
    reschedule_count = int(run.metadata.get("reschedule_count", 0))
    max_reschedules = int(os.getenv("STREAM_BACKEND_MAX_RESCHEDULES", "2"))
    
    if reschedule_count >= max_reschedules:
        # Mark as error, prevent infinite loop
        run.status = "error"
        run.metadata["error"] = "reschedule_limit_exceeded"
        await self.repo.save_run(run)
        return False
    
    # Reschedule with counter increment
    run.metadata["reschedule_count"] = reschedule_count + 1
    await self.repo.save_run(run)
    # ... enqueue new task
```

### Frontend Changes

#### 1. Remove Auto-Join (`ui/src/App.tsx`)

**Before:**
```typescript
useEffect(() => {
  const active = runs.find(run => ACTIVE_RUN_STATUSES.has(run.status));
  void continueActiveRun({ threadId, runId: active.runId });  // Auto-join!
}, [runs]);
```

**After:**
```typescript
useEffect(() => {
  const active = runs.find(run => ACTIVE_RUN_STATUSES.has(run.status));
  setActiveRun({ threadId, runId: active.runId });  // Just show banner
}, [runs]);
```

#### 2. Remove Polling

**Before:**
```typescript
const interval = setInterval(() => {
  fetchRunStatus(apiUrl, activeRun);  // Poll every 3s!
}, 3000);
```

**After:**
```typescript
// Removed polling, rely on real-time lifecycle events
```

#### 3. User-Triggered Continue

```typescript
async function continueActiveRun(run: ActiveRun) {
  const status = await fetchRunStatus(apiUrl, run);
  
  if (!ACTIVE_RUN_STATUSES.has(status)) {
    setActiveRun(null);  // Run not active anymore
    return;
  }
  
  await fetch(`${apiUrl}/threads/${run.threadId}/runs/${run.runId}/resume`, {
    method: "POST"
  });
  
  await stream.joinStream(run.runId);
  setActiveRun(null);
}
```

## Flow Diagrams

### Normal Flow (User Clicks Continue)

```
User Opens Thread with Active Run
    ↓
UI discovers active run (lifecycle stream)
    ↓
UI shows banner: "Active run found [Continue] [Stop]"
    ↓
User clicks "Continue streaming"
    ↓
continueActiveRun() called
    ↓
POST /threads/{threadId}/runs/{runId}/resume
    ↓
Backend checks: is Celery task active?
    ├─ YES → Return (task already running)
    └─ NO  → Check reschedule count
              ├─ < 2 → Reschedule (counter++)
              └─ ≥ 2 → Mark as error
    ↓
UI joins stream
```

### Zombie Run Detection Flow

```
Resume request received
    ↓
Get Celery task_id from run.metadata
    ↓
Query Celery: app.AsyncResult(task_id).status
    ↓
Is task active (PENDING/STARTED/RETRY)?
    ├─ YES → Task running, return success
    └─ NO  → Task is dead
        ↓
    Check reschedule_count
    ├─ count < 2 →
        │   - Increment counter: reschedule_count++
        │   - Log: "task_dead_rescheduling"
        │   - Enqueue new Celery task
        │   - Return success
    └─ count ≥ 2 →
        │   - Mark run.status = "error"
        │   - Set metadata.error = "reschedule_limit_exceeded"
        │   - Emit lifecycle event: "failed"
        │   - Return false
```

## Configuration

### Environment Variables

```bash
# Maximum reschedules before marking run as error (default: 2)
STREAM_BACKEND_MAX_RESCHEDULES=2
```

### Run Metadata Fields

```json
{
  "reschedule_count": 1,
  "rescheduled_at": "2024-01-01T00:00:00Z",
  "previous_task_id": "abc-123-def",
  "celery_task_id": "new-task-id",
  "error": "reschedule_limit_exceeded",
  "error_message": "Run rescheduled 2 times without completion"
}
```

## API Changes

### POST /threads/{threadId}/runs/{runId}/resume

**Response Changes:**

```json
{
  "run_id": "run-123",
  "thread_id": "thread-456",
  "status": "running",
  "metadata": {
    "reschedule_count": 1,
    "celery_task_id": "new-task-id"
  }
}
```

**Error Response (Limit Exceeded):**

```json
{
  "run_id": "run-123",
  "thread_id": "thread-456",
  "status": "error",
  "metadata": {
    "error": "reschedule_limit_exceeded",
    "reschedule_count": 2
  }
}
```

## Testing

### Unit Tests

**File**: `tests/test_reschedule_logic.py`

**Tests:**
1. ✅ `test_celery_scheduler_task_active` - Active task detection
2. ✅ `test_celery_scheduler_get_task_status` - Status retrieval
3. ✅ `test_celery_scheduler_task_active_states` - All active states
4. ✅ `test_celery_scheduler_task_inactive_states` - All inactive states
5. ✅ `test_run_record_reschedule_counter` - Metadata handling
6. ✅ `test_reschedule_count_increment` - Counter increment
7. ✅ `test_max_reschedules_limit` - Limit enforcement

**Run Tests:**
```bash
cd stream-backend
python -m pytest tests/test_reschedule_logic.py -v
```

### Integration Test Scenarios

1. **Active Task Found**
   - Start run via Celery
   - Trigger resume
   - Verify: No reschedule, returns early

2. **Dead Task Rescheduled**
   - Start run, task dies
   - Trigger resume
   - Verify: New task created, counter incremented

3. **Limit Exceeded**
   - Run rescheduled twice already
   - Trigger resume
   - Verify: Run marked as error

4. **User Clicks Continue**
   - Banner shows
   - User clicks Continue
   - Verify: Resume called, stream joined

5. **User Clicks Stop**
   - Banner shows
   - User clicks Stop
   - Verify: Run cancelled, banner cleared

## Monitoring & Observability

### Log Messages

**Backend:**
```
INFO  run.resume.celery_task_active thread_id=X run_id=Y task_id=Z
INFO  run.resume.task_dead_rescheduling thread_id=X run_id=Y reschedule=N
WARN  run.resume.reschedule_limit_reached thread_id=X run_id=Y count=N
```

**Frontend:**
```
INFO  activeRun.discovered threadId=X runId=Y
INFO  activeRun.continue.start threadId=X runId=Y
INFO  activeRun.continue.completed threadId=X runId=Y
```

### Metrics to Track

- `run_reschedules_total{status="success|error"}` - Count of reschedules
- `run_reschedule_limit_exceeded_total` - Runs hitting limit
- `active_run_banner_shown_total` - Banner display count
- `active_run_continue_clicks_total` - User continue actions
- `active_run_stop_clicks_total` - User stop actions

## Benefits

### 1. Prevents Infinite Loops
- Max 2 reschedules prevents zombie run resurrection loops
- Automatic error marking after limit

### 2. Detects Zombie Runs
- Celery task status checking
- No more "running" state with dead tasks

### 3. User Control
- Explicit consent via banner
- Clear options: Continue or Stop
- No auto-resume surprises

### 4. Performance
- No wasteful polling every 3s
- Efficient real-time events
- Reduced API calls

### 5. Observability
- Clear logging of all decisions
- Metadata tracking of reschedules
- Easy debugging with counters

## Future Enhancements

1. **Configurable Limits**
   - Per-assistant reschedule limits
   - Dynamic limit based on run priority

2. **Smart Recovery**
   - Checkpoint-based recovery
   - Partial progress preservation

3. **Circuit Breaker**
   - Auto-disable rescheduling after N failures
   - Manual recovery triggers

4. **Advanced Metrics**
   - Time-to-reschedule tracking
   - Success rate by reschedule count

## Troubleshooting

### Run Stuck in "running" State

**Diagnosis:**
```bash
# Check Celery task status
celery -A worker.celery_app inspect active

# Check run metadata
curl http://localhost:2024/threads/{thread_id}/runs/{run_id}
```

**Solution:**
1. Click "Continue streaming" in UI
2. Backend will detect dead task and reschedule
3. If limit exceeded, run marked as error

### Reschedule Limit Exceeded

**Symptoms:**
```
Run status: error
Metadata.error: "reschedule_limit_exceeded"
```

**Solution:**
1. Investigate root cause (why task dies)
2. Fix underlying issue
3. Start new run (avoid zombie)

## Migration Notes

1. **Existing Active Runs**: Will show banner on first load
2. **No Breaking Changes**: Backward compatible with existing metadata
3. **Rollback Safe**: Can revert if needed (no schema changes)

## Conclusion

This implementation provides robust handling of zombie runs, user control over active run recovery, and prevents infinite rescheduling loops. The system is well-tested, observable, and configurable for different use cases.
