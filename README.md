# tiny-retry

> Zero-dependency retry, exponential backoff, and circuit breaker for Python. Sync + async, decorator-friendly.

```bash
pip install tiny-retry   # coming soon
```

## Why?

Resilience patterns are universal but the libraries are heavy:

- **`tenacity`** — 4 deps, full-featured but 1500+ LOC
- **`backoff`** — sync only, no circuit breaker
- **`pybreaker`** — circuit breaker only
- **`aiobreaker`** — async circuit breaker only

**tiny-retry** ships retry + backoff + circuit breaker in one 480-line file.

## What's included

| Component | Sync | Async | Decorator |
|-----------|------|-------|-----------|
| `retry()` / `abretry()` | ✅ | ✅ | `@retry` / `@retry` |
| `CircuitBreaker` | ✅ | ✅ | `@circuit` |
| 4 jitter modes | ✅ | ✅ | — |
| Configurable exception filter | ✅ | ✅ | — |
| Callback hooks | ✅ | ✅ | — |

## Usage

### Retry with exponential backoff

```python
import tiny_retry as tr

@tr.retry_decorator(tries=5, base=0.1, max_delay=10.0, jitter="full")
def call_api():
    return requests.get("https://api.example.com/data")
```

Or call the runner directly:

```python
result = tr.retry(
    flaky_call,
    tries=3,
    base=0.5,
    multiplier=2.0,
    jitter="decorrelated",   # AWS-recommended
    retry_on=(ConnectionError, TimeoutError),
    on_retry=lambda n, exc, s: log.warning("attempt %d failed: %s, sleeping %.2fs", n, exc, s),
)
```

### Async retry

```python
@tr.abretry_decorator(tries=4, base=0.2, jitter="equal")
async def fetch_data():
    async with aiohttp.ClientSession() as s:
        return await s.get(url)
```

### Circuit breaker

```python
cb = tr.CircuitBreaker(
    failure_threshold=5,    # open after 5 consecutive failures
    recovery_time=30.0,     # stay open for 30s
    success_threshold=2,    # close after 2 successes in half-open
)

@tr.circuit_decorator(cb)
def protected_call():
    return call_external_service()
```

When OPEN, calls raise `CircuitOpenError(retry_after=N)` immediately.

## Jitter modes

| Mode | Sleep formula | Best for |
|------|---------------|----------|
| `none` | `min(base * mult^attempt, max_delay)` | Testing, deterministic |
| `full` | `random(0, cap)` | Default; spreads load |
| `equal` | `cap/2 + random(0, cap/2)` | AWS-style balance |
| `decorrelated` | `min(cap, random(base, prev * 3))` | AWS-recommended for thundering herd |

## API

| Function | Description |
|----------|-------------|
| `retry(fn, ...)` | Run with retry. Returns result, raises `RetryError`. |
| `abretry(fn, ...)` | Async version |
| `CircuitBreaker(threshold, recovery, ...)` | State machine. `.call()` / `.acall()`. |
| `CircuitOpenError` | Raised on OPEN. Has `.retry_after`. |
| `retry_decorator(...)` | `@retry(...)` for sync |
| `abretry_decorator(...)` | `@retry(...)` for async |
| `circuit_decorator(cb)` | `@circuit` for sync |
| `acircuit_decorator(cb)` | `@circuit` for async |

## Performance

```
retry() no-failure, 1 try            1.36 µs/op
retry() no-failure, 5 tries          1.10 µs/op
CircuitBreaker.call() closed         4.10 µs/op
```

`tenacity` adds ~5-10 µs per call for the same simple retry; `pybreaker` is comparable to ours on the closed path.

## Ecosystem

Part of the **tiny-*** zero-dep stack by [OpenClaw](https://github.com/hussain-alsaibai):

| Repo | What |
|------|------|
| [tiny-router](https://github.com/hussain-alsaibai/tiny-router) | HTTP routing, 76K req/s |
| [tiny-log](https://github.com/hussain-alsaibai/tiny-log) | Structured logs, 32K logs/s |
| [tiny-validator](https://github.com/hussain-alsaibai/tiny-validator) | Input validation, 247K val/s |
| [tiny-config](https://github.com/hussain-alsaibai/tiny-config) | Layered config loader |
| [tiny-cli](https://github.com/hussain-alsaibai/tiny-cli) | CLI builder with colors |
| [fast-cache](https://github.com/hussain-alsaibai/fast-cache) | LRU+TTL+SWR cache |
| [tiny-rate](https://github.com/hussain-alsaibai/tiny-rate) | Token-bucket / sliding window limiter |
| [tiny-pool](https://github.com/hussain-alsaibai/tiny-pool) | Thread / async worker pools |
| [tiny-agent](https://github.com/hussain-alsaibai/tiny-agent) | Zero-dep agent framework |
| [tiny-mcp](https://github.com/hussain-alsaibai/tiny-mcp) | Model Context Protocol server |
| [tiny-embed](https://github.com/hussain-alsaibai/tiny-embed) | Embeddings + vector search |
| [tiny-compose](https://github.com/hussain-alsaibai/tiny-compose) | Stack any decorators in any order, declaratively |
| [tiny-trace](https://github.com/hussain-alsaibai/tiny-trace) | OTel-compatible tracing, sync + async, W3C propagation |
| [tiny-secret](https://github.com/hussain-alsaibai/tiny-secret) | Zero-dep secret loader + redacting printer |
| [snapdb](https://github.com/hussain-alsaibai/snapdb) | Embedded DB (Python) |

**Total: 15 repos, ~6,400 LOC, zero deps across the entire stack.**

## License

MIT © 2026 OpenClaw (hussain-alsaibai)
