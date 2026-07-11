# Windows AsyncIO Event Loop Fix for Psycopg

## Problem

On Windows, when running Celery workers or FastAPI with async PostgreSQL connections using `psycopg`, you may encounter this error:

```
Psycopg cannot use the 'ProactorEventLoop' to run in async mode. 
Please use a compatible event loop, for instance by setting 
'asyncio.set_event_loop_policy(WindowsSelectorEventLoopPolicy())'
```

## Root Cause

### Windows Default Event Loop

Windows uses **`ProactorEventLoop`** as the default event loop policy, which is not compatible with certain async libraries like `psycopg` (async PostgreSQL driver).

### Psycopg Requirements

`psycopg` requires `**SelectorEventLoop`** to properly handle async I/O operations with PostgreSQL connections. The ProactorEventLoop uses different underlying mechanisms (I/O completion ports vs select/poll).

## Solution

Set the event loop policy to `WindowsSelectorEventLoopPolicy()` on Windows before any async operations:

```python
import asyncio
import sys

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
```

### Implementation

We applied this fix in three locations:

#### 1. **`worker/celery_app.py`** (Celery Worker)

```python
import asyncio
import sys

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
```

**Why:** Worker processes need correct event loop at startup before any async tasks run.

#### 2. **`worker/tasks.py`** (Task Execution)

```python
import asyncio
import sys

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# ... rest of imports
```

**Why:** Task execution may create new event loops; ensures correct policy at module load.

#### 3. **`app/main.py`** (FastAPI Backend)

```python
import asyncio
import sys

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from fastapi import FastAPI
# ... rest of imports
```

**Why:** Backend needs correct event loop for async PostgreSQL connections.

## When to Apply

This fix is needed when:

1. ✅ Running on **Windows** (`sys.platform == "win32"`)
2. ✅ Using **psycopg** or **asyncpg** for async PostgreSQL
3. ✅ Using **asyncio** with database operations
4. ✅ Running Celery workers with async tasks
5. ✅ Running FastAPI with async database connections

## Event Loop Comparison

| Event Loop | Platform | Compatible with Psycopg |
|------------|----------|--------------------------|
| **ProactorEventLoop** | Windows (default) | ❌ No |
| **SelectorEventLoop** | Windows (manual) | ✅ Yes |
| **SelectorEventLoop** | Linux/macOS | ✅ Yes (default) |

## Testing

### Before Fix

```bash
# Error in logs:
WARNING/MainProcess] error connecting in 'pool-2': 
Psycopg cannot use the 'ProactorEventLoop' to run in async mode.
```

### After Fix

```bash
# Clean startup, no warnings
INFO/MainProcess] Connected to amqp://guest:**@localhost:5672//
INFO/MainProcess] celery@HOST ready.
```

### Verify Fix

```bash
# Start worker
cd stream-backend
python -m worker.tasks

# Check for errors in logs
# Should see: "worker.init.recover_stale_runs" without ProactorEventLoop errors
```

## Cross-Platform Compatibility

The fix is **cross-platform safe**:

```python
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
```

- ✅ **Windows**: Sets `WindowsSelectorEventLoopPolicy()`
- ✅ **Linux**: Skips (already has correct policy)
- ✅ **macOS**: Skips (already has correct policy)

## Performance Implications

### Minimal Impact

Both event loops have similar performance for most use cases:

- **ProactorEventLoop**: Better for file I/O, subprocesses on Windows
- **SelectorEventLoop**: Better for network I/O, required for psycopg

For async PostgreSQL connections, the SelectorEventLoop is actually **better suited**.

## Alternative Solutions

### Option 1: Use WindowsSelectorEventLoopPolicy (Recommended) ✅

```python
asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
```

**Pros:**
- ✅ Compatible with psycopg
- ✅ Works with all async database drivers
- ✅ Simple one-line fix

**Cons:**
- ⚠️ Different from Windows default

### Option 2: Use Sync Connections

```python
# Use psycopg2 instead of psycopg (sync driver)
import psycopg2
```

**Pros:**
- ✅ Works with default Windows event loop

**Cons:**
- ❌ No async support
- ❌ Blocking operations
- ❌ Poor performance for concurrent requests

### Option 3: Run in WSL or Docker

```bash
# Run in Windows Subsystem for Linux
wsl
python -m worker.tasks
```

**Pros:**
- ✅ Native Linux environment
- ✅ Default event loop works

**Cons:**
- ❌ Additional setup required
- ❌ Not native Windows

## Related Issues

### Other Libraries Affected

This issue affects multiple async libraries on Windows:

1. **psycopg** - PostgreSQL async driver
2. **asyncpg** - PostgreSQL async driver
3. **aiomysql** - MySQL async driver
4. **aioredis** - Redis async driver
5. **aiohttp** - HTTP client (sometimes)

All require SelectorEventLoop for proper async I/O.

### Python Version

- **Python 3.8+**: ProactorEventLoop is default on Windows
- **Python 3.7**: SelectorEventLoop is default (fewer issues)
- **Python 3.11+**: Better async support, but still needs policy change

## Best Practices

### 1. Apply Early

Set the policy **before** any async operations:

```python
# ✅ Good
import asyncio
import sys

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# Later: database connections
from app.store_postgres import PostgresRepository
```

```python
# ❌ Bad
from app.store_postgres import PostgresRepository  # May fail!

import asyncio
import sys
if sys.platform == "win32":
    asyncio.set_event_loop_policy(WindowsSelectorEventLoopPolicy())
```

### 2. Check Platform

Always check `sys.platform` to avoid affecting Linux/macOS:

```python
import sys

if sys.platform == "win32":
    # Windows-specific fix
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
```

### 3. Document the Fix

Add comments explaining why:

```python
# Fix for psycopg on Windows: ProactorEventLoop not compatible
# See: https://www.psycopg.org/psycopg3/docs/advanced/async.html
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
```

## Troubleshooting

### Still Getting Errors?

1. **Verify policy is set:**
   ```python
   import asyncio
   print(asyncio.get_event_loop_policy())
   # Should show: WindowsSelectorEventLoopPolicy on Windows
   ```

2. **Check import order:**
   - Policy must be set **before** importing async database drivers

3. **Restart workers:**
   ```bash
   # Kill existing workers
   pkill -f celery
   
   # Start fresh
   python -m worker.tasks
   ```

4. **Check Python version:**
   ```bash
   python --version
   # Recommend: Python 3.11+ for best async support
   ```

## References

- [Python asyncio Platform Support](https://docs.python.org/3/library/asyncio-platforms.html)
- [psycopg Async Documentation](https://www.psycopg.org/psycopg3/docs/advanced/async.html)
- [Celery Windows Guide](https://docs.celeryq.dev/en/stable/userguide/windows.html)
- [PEP 492](https://www.python.org/dev/peps/pep-0492/) - async/await syntax

## Summary

✅ **Fix Applied:** `WindowsSelectorEventLoopPolicy()` set on Windows
✅ **Locations:** `celery_app.py`, `tasks.py`, `main.py`
✅ **Platform Safe:** Only affects Windows, Linux/macOS unchanged
✅ **Performance:** Minimal impact, required for psycopg
✅ **Compatibility:** Works with Python 3.8+

The fix ensures psycopg async PostgreSQL connections work correctly on Windows platforms without warnings or connection failures.
