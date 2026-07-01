"""tiny-retry: Zero-dependency retry, backoff, and circuit breaker for Python.

Three building blocks in a single file:

  - retry()    : exponential backoff with jitter, sync + async, raises last error
  - CircuitBreaker : open/half-open/closed state machine
  - Decorators : @retry, @abretry, @circuit

Single file, no deps, MIT, fully typed. Honors an optional jitter strategy
("none" / "full" / "equal" / "decorrelated" — AWS-recommended).
"""

from __future__ import annotations

import asyncio
import functools
import inspect
import random
import threading
import time
from typing import Any, Awaitable, Callable, Iterable, Optional, Tuple, Type, TypeVar, Union

__version__ = "0.1.0"
__all__ = [
    "retry",
    "CircuitBreaker",
    "CircuitOpenError",
    "RetryError",
    "retry_decorator",
    "abretry_decorator",
    "circuit_decorator",
]


T = TypeVar("T")


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class RetryError(Exception):
    """Raised when all retry attempts are exhausted.

    Wraps the last underlying exception as __cause__.
    """

    def __init__(self, attempts: int, last_exc: BaseException) -> None:
        self.attempts = attempts
        self.last_exc = last_exc
        super().__init__(
            f"retry failed after {attempts} attempt(s); last error: {last_exc!r}"
        )
        self.__cause__ = last_exc


class CircuitOpenError(Exception):
    """Raised when an operation is called on an OPEN circuit.

    Attributes:
        retry_after: Seconds until the circuit transitions to half-open (float).
    """

    def __init__(self, retry_after: float, name: str = "circuit") -> None:
        self.retry_after = max(0.0, float(retry_after))
        super().__init__(
            f"{name!r} is OPEN; retry after {self.retry_after:.4f}s"
        )


# ---------------------------------------------------------------------------
# Backoff strategies
# ---------------------------------------------------------------------------


JitterMode = str  # "none" | "full" | "equal" | "decorrelated"


def _sleep_for(
    attempt: int,
    base: float,
    max_delay: float,
    multiplier: float,
    jitter: JitterMode,
) -> float:
    """Compute the next sleep duration in seconds.

    `attempt` is 0-indexed: attempt=0 is the first retry (after initial try).
    """
    if jitter == "decorrelated":
        # AWS Architecture Blog formula: sleep = min(cap, random(base, prev*3))
        # We return just the new value; the caller tracks previous sleep.
        return min(max_delay, random.uniform(base, base * 3))

    raw = base * (multiplier ** attempt)
    cap = min(raw, max_delay)

    if jitter == "none":
        return cap
    if jitter == "full":
        return random.uniform(0, cap)
    if jitter == "equal":
        half = cap / 2
        return half + random.uniform(0, half)
    raise ValueError(f"unknown jitter mode: {jitter!r}")


# ---------------------------------------------------------------------------
# Core retry runner (sync)
# ---------------------------------------------------------------------------


def _should_retry(exc: BaseException, retry_on: Tuple[Type[BaseException], ...]) -> bool:
    return isinstance(exc, retry_on)


def retry(
    fn: Callable[..., T],
    *args: Any,
    tries: int = 3,
    base: float = 0.1,
    max_delay: float = 30.0,
    multiplier: float = 2.0,
    jitter: JitterMode = "full",
    retry_on: Iterable[Type[BaseException]] = (Exception,),
    on_retry: Optional[Callable[[int, BaseException, float], None]] = None,
    **kwargs: Any,
) -> T:
    """Call fn(*args, **kwargs) with exponential-backoff retry.

    Args:
        tries:        Total attempts (>=1). 1 = no retry.
        base:         Initial backoff in seconds.
        max_delay:    Cap on the per-attempt sleep.
        multiplier:   Backoff growth factor (>=1.0).
        jitter:       "none" | "full" | "equal" | "decorrelated".
        retry_on:     Tuple of exception types to retry. Defaults to (Exception,).
        on_retry:     Optional callback(attempt_no, exception, sleep_seconds).
        **kwargs:     Forwarded to fn.

    Returns:
        fn's return value on success.

    Raises:
        RetryError:  When all attempts fail. Wraps the last exception.
        ValueError:  If tries < 1, base < 0, multiplier < 1, etc.
    """
    if tries < 1:
        raise ValueError("tries must be >= 1")
    if base < 0:
        raise ValueError("base must be >= 0")
    if max_delay < base:
        raise ValueError("max_delay must be >= base")
    if multiplier < 1.0:
        raise ValueError("multiplier must be >= 1.0")
    if jitter not in ("none", "full", "equal", "decorrelated"):
        raise ValueError(f"unknown jitter mode: {jitter!r}")

    retry_on_t = tuple(retry_on)
    last_exc: Optional[BaseException] = None
    prev_sleep = base
    for attempt in range(tries):
        try:
            return fn(*args, **kwargs)
        except BaseException as exc:  # noqa: BLE001
            if not _should_retry(exc, retry_on_t):
                raise
            last_exc = exc
            if attempt + 1 >= tries:
                # Out of attempts
                raise RetryError(attempt + 1, exc) from exc
            # Compute sleep
            if jitter == "decorrelated":
                sleep = _sleep_for(attempt, base, max_delay, multiplier, jitter)
                sleep = min(max_delay, random.uniform(base, prev_sleep * 3))
                prev_sleep = sleep
            else:
                sleep = _sleep_for(attempt, base, max_delay, multiplier, jitter)
                prev_sleep = sleep
            if on_retry is not None:
                on_retry(attempt + 1, exc, sleep)
            time.sleep(sleep)
    # Unreachable, but type-checkers love a fallthrough
    raise RetryError(tries, last_exc)  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Async retry runner
# ---------------------------------------------------------------------------


async def abretry(
    fn: Callable[..., Awaitable[T]],
    *args: Any,
    tries: int = 3,
    base: float = 0.1,
    max_delay: float = 30.0,
    multiplier: float = 2.0,
    jitter: JitterMode = "full",
    retry_on: Iterable[Type[BaseException]] = (Exception,),
    on_retry: Optional[Callable[[int, BaseException, float], None]] = None,
    **kwargs: Any,
) -> T:
    """Async version of retry()."""
    if tries < 1:
        raise ValueError("tries must be >= 1")
    if base < 0:
        raise ValueError("base must be >= 0")
    if max_delay < base:
        raise ValueError("max_delay must be >= base")
    if multiplier < 1.0:
        raise ValueError("multiplier must be >= 1.0")
    if jitter not in ("none", "full", "equal", "decorrelated"):
        raise ValueError(f"unknown jitter mode: {jitter!r}")

    retry_on_t = tuple(retry_on)
    last_exc: Optional[BaseException] = None
    prev_sleep = base
    for attempt in range(tries):
        try:
            return await fn(*args, **kwargs)
        except BaseException as exc:  # noqa: BLE001
            if not _should_retry(exc, retry_on_t):
                raise
            last_exc = exc
            if attempt + 1 >= tries:
                raise RetryError(attempt + 1, exc) from exc
            if jitter == "decorrelated":
                sleep = min(max_delay, random.uniform(base, prev_sleep * 3))
                prev_sleep = sleep
            else:
                sleep = _sleep_for(attempt, base, max_delay, multiplier, jitter)
                prev_sleep = sleep
            if on_retry is not None:
                on_retry(attempt + 1, exc, sleep)
            await asyncio.sleep(sleep)
    raise RetryError(tries, last_exc)  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Circuit Breaker
# ---------------------------------------------------------------------------


class CircuitState:
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    """Classic 3-state circuit breaker.

    Args:
        failure_threshold:  Consecutive failures before opening (>=1).
        recovery_time:      Seconds to wait before transitioning OPEN→HALF_OPEN.
        success_threshold:  Consecutive successes in HALF_OPEN before closing.
        expected_exception: Tuple of exceptions counted as failures. Other
                            exceptions propagate without affecting state.
        name:               Optional identifier for logging.

    Thread-safe. State transitions:
        CLOSED --[N consecutive failures]--> OPEN
        OPEN   --[recovery_time elapsed]--> HALF_OPEN
        HALF_OPEN --[success_threshold successes]--> CLOSED
        HALF_OPEN --[any failure]--> OPEN
    """

    __slots__ = (
        "failure_threshold",
        "recovery_time",
        "success_threshold",
        "expected_exception",
        "name",
        "_state",
        "_failures",
        "_successes",
        "_opened_at",
        "_lock",
    )

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_time: float = 30.0,
        success_threshold: int = 1,
        expected_exception: Tuple[Type[BaseException], ...] = (Exception,),
        name: str = "circuit",
    ) -> None:
        if failure_threshold < 1:
            raise ValueError("failure_threshold must be >= 1")
        if recovery_time <= 0:
            raise ValueError("recovery_time must be > 0")
        if success_threshold < 1:
            raise ValueError("success_threshold must be >= 1")
        self.failure_threshold = failure_threshold
        self.recovery_time = float(recovery_time)
        self.success_threshold = success_threshold
        self.expected_exception = tuple(expected_exception)
        self.name = name
        self._state = CircuitState.CLOSED
        self._failures = 0
        self._successes = 0
        self._opened_at = 0.0
        self._lock = threading.Lock()

    # -- state accessors -----------------------------------------------------

    @property
    def state(self) -> str:
        with self._lock:
            self._maybe_recover_locked()
            return self._state

    def _maybe_recover_locked(self) -> None:
        if self._state == CircuitState.OPEN and (
            time.monotonic() - self._opened_at >= self.recovery_time
        ):
            self._state = CircuitState.HALF_OPEN
            self._successes = 0

    def _check_call_locked(self) -> None:
        self._maybe_recover_locked()
        if self._state == CircuitState.OPEN:
            remaining = self.recovery_time - (time.monotonic() - self._opened_at)
            raise CircuitOpenError(remaining, self.name)

    def _on_success_locked(self) -> None:
        if self._state == CircuitState.HALF_OPEN:
            self._successes += 1
            if self._successes >= self.success_threshold:
                self._state = CircuitState.CLOSED
                self._failures = 0
                self._successes = 0
        else:  # CLOSED
            self._failures = 0

    def _on_failure_locked(self) -> None:
        if self._state == CircuitState.HALF_OPEN:
            self._state = CircuitState.OPEN
            self._opened_at = time.monotonic()
            self._successes = 0
        else:  # CLOSED
            self._failures += 1
            if self._failures >= self.failure_threshold:
                self._state = CircuitState.OPEN
                self._opened_at = time.monotonic()

    # -- public call helpers -------------------------------------------------

    def call(self, fn: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        """Invoke fn(*args, **kwargs) under the breaker."""
        with self._lock:
            self._check_call_locked()
        try:
            result = fn(*args, **kwargs)
        except BaseException as exc:
            with self._lock:
                if isinstance(exc, self.expected_exception):
                    self._on_failure_locked()
            raise
        with self._lock:
            self._on_success_locked()
        return result

    async def acall(self, fn: Callable[..., Awaitable[T]], *args: Any, **kwargs: Any) -> T:
        """Async version of call()."""
        with self._lock:
            self._check_call_locked()
        try:
            result = await fn(*args, **kwargs)
        except BaseException as exc:
            with self._lock:
                if isinstance(exc, self.expected_exception):
                    self._on_failure_locked()
            raise
        with self._lock:
            self._on_success_locked()
        return result

    def reset(self) -> None:
        """Force the circuit back to CLOSED."""
        with self._lock:
            self._state = CircuitState.CLOSED
            self._failures = 0
            self._successes = 0
            self._opened_at = 0.0

    def __repr__(self) -> str:
        return f"CircuitBreaker(name={self.name!r}, state={self.state})"


# ---------------------------------------------------------------------------
# Decorators
# ---------------------------------------------------------------------------


def retry_decorator(
    *,
    tries: int = 3,
    base: float = 0.1,
    max_delay: float = 30.0,
    multiplier: float = 2.0,
    jitter: JitterMode = "full",
    retry_on: Iterable[Type[BaseException]] = (Exception,),
    on_retry: Optional[Callable[[int, BaseException, float], None]] = None,
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Decorator: @retry(...) for sync functions."""
    def decorator(fn: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            return retry(
                fn,
                *args,
                tries=tries,
                base=base,
                max_delay=max_delay,
                multiplier=multiplier,
                jitter=jitter,
                retry_on=retry_on,
                on_retry=on_retry,
                **kwargs,
            )

        wrapper.__wrapped__ = fn  # type: ignore[attr-defined]
        return wrapper  # type: ignore[return-value]

    return decorator


def abretry_decorator(
    *,
    tries: int = 3,
    base: float = 0.1,
    max_delay: float = 30.0,
    multiplier: float = 2.0,
    jitter: JitterMode = "full",
    retry_on: Iterable[Type[BaseException]] = (Exception,),
    on_retry: Optional[Callable[[int, BaseException, float], None]] = None,
) -> Callable[[Callable[..., Awaitable[T]]], Callable[..., Awaitable[T]]]:
    """Decorator: @retry(...) for async functions."""
    def decorator(fn: Callable[..., Awaitable[T]]) -> Callable[..., Awaitable[T]]:
        if not inspect.iscoroutinefunction(fn):
            raise TypeError("@abretry requires an async function")

        @functools.wraps(fn)
        async def wrapper(*args: Any, **kwargs: Any) -> T:
            return await abretry(
                fn,
                *args,
                tries=tries,
                base=base,
                max_delay=max_delay,
                multiplier=multiplier,
                jitter=jitter,
                retry_on=retry_on,
                on_retry=on_retry,
                **kwargs,
            )

        wrapper.__wrapped__ = fn  # type: ignore[attr-defined]
        return wrapper

    return decorator


def circuit_decorator(
    breaker: CircuitBreaker,
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Decorator: protect a sync function with a CircuitBreaker."""
    def decorator(fn: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            return breaker.call(fn, *args, **kwargs)

        wrapper.__wrapped__ = fn  # type: ignore[attr-defined]
        wrapper.breaker = breaker  # type: ignore[attr-defined]
        return wrapper  # type: ignore[return-value]

    return decorator


def acircuit_decorator(
    breaker: CircuitBreaker,
) -> Callable[[Callable[..., Awaitable[T]]], Callable[..., Awaitable[T]]]:
    """Decorator: protect an async function with a CircuitBreaker."""
    def decorator(fn: Callable[..., Awaitable[T]]) -> Callable[..., Awaitable[T]]:
        if not inspect.iscoroutinefunction(fn):
            raise TypeError("@acircuit requires an async function")

        @functools.wraps(fn)
        async def wrapper(*args: Any, **kwargs: Any) -> T:
            return await breaker.acall(fn, *args, **kwargs)

        wrapper.__wrapped__ = fn  # type: ignore[attr-defined]
        wrapper.breaker = breaker  # type: ignore[attr-defined]
        return wrapper

    return decorator
