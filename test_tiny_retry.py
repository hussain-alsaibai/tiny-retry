"""Tests for tiny-retry. Run with `python test_tiny_retry.py`. Stdlib only."""

import asyncio
import sys
import threading
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import tiny_retry as tr


# ---------------------------------------------------------------------------
# retry() — sync
# ---------------------------------------------------------------------------


class TestRetry(unittest.TestCase):
    def test_success_first_try(self):
        calls = []

        def fn():
            calls.append(1)
            return "ok"

        self.assertEqual(tr.retry(fn, tries=3, base=0.0, max_delay=0.0), "ok")
        self.assertEqual(len(calls), 1)

    def test_eventual_success(self):
        attempts = []

        def fn():
            attempts.append(1)
            if len(attempts) < 3:
                raise ValueError("nope")
            return "ok"

        self.assertEqual(tr.retry(fn, tries=5, base=0.0, max_delay=0.0), "ok")
        self.assertEqual(len(attempts), 3)

    def test_exhausts_attempts_raises_retry_error(self):
        def fn():
            raise ValueError("always")

        with self.assertRaises(tr.RetryError) as cm:
            tr.retry(fn, tries=3, base=0.0, max_delay=0.0)
        self.assertEqual(cm.exception.attempts, 3)
        self.assertIsInstance(cm.exception.last_exc, ValueError)
        self.assertIsInstance(cm.exception.__cause__, ValueError)

    def test_does_not_retry_on_unlisted_exception(self):
        calls = []

        def fn():
            calls.append(1)
            raise KeyError("nope")

        with self.assertRaises(KeyError):
            tr.retry(fn, tries=5, base=0.0, retry_on=(ValueError,))
        self.assertEqual(len(calls), 1)

    def test_retries_only_on_listed_exception(self):
        calls = []

        def fn():
            calls.append(1)
            if len(calls) < 2:
                raise ValueError("retry me")
            return "ok"

        self.assertEqual(
            tr.retry(fn, tries=3, base=0.0, max_delay=0.0, retry_on=(ValueError,)),
            "ok",
        )

    def test_args_and_kwargs_forwarded(self):
        def fn(a, b, *, c):
            return a + b + c

        self.assertEqual(
            tr.retry(fn, 1, 2, c=3, tries=1, base=0.0), 6
        )

    def test_on_retry_callback_called(self):
        events = []

        def on_retry(n, exc, sleep):
            events.append((n, type(exc).__name__, sleep))

        def fn():
            raise RuntimeError("x")

        with self.assertRaises(tr.RetryError):
            tr.retry(
                fn, tries=3, base=0.01, max_delay=0.1,
                jitter="none", on_retry=on_retry,
            )

        # 2 retries = 2 on_retry calls
        self.assertEqual(len(events), 2)
        self.assertEqual([e[0] for e in events], [1, 2])
        self.assertEqual([e[1] for e in events], ["RuntimeError", "RuntimeError"])
        # base=0.01, multiplier=2, no jitter → 0.01, 0.02
        self.assertAlmostEqual(events[0][2], 0.01, places=3)
        self.assertAlmostEqual(events[1][2], 0.02, places=3)

    def test_invalid_args(self):
        with self.assertRaises(ValueError):
            tr.retry(lambda: 1, tries=0)
        with self.assertRaises(ValueError):
            tr.retry(lambda: 1, tries=1, base=-1)
        with self.assertRaises(ValueError):
            tr.retry(lambda: 1, tries=1, base=1, max_delay=0.5)
        with self.assertRaises(ValueError):
            tr.retry(lambda: 1, tries=1, multiplier=0.5)

    def test_jitter_modes(self):
        # Just verify they all parse and execute without error
        for jitter in ("none", "full", "equal", "decorrelated"):
            def fn():
                raise RuntimeError("x")

            with self.assertRaises(tr.RetryError):
                tr.retry(fn, tries=2, base=0.001, max_delay=0.01, jitter=jitter)

    def test_invalid_jitter(self):
        def fn():
            raise RuntimeError("x")

        with self.assertRaises(ValueError):
            tr.retry(fn, tries=1, jitter="bogus")

    def test_backoff_grows(self):
        sleeps = []

        def on_retry(n, exc, sleep):
            sleeps.append(sleep)

        def fn():
            raise RuntimeError("x")

        with self.assertRaises(tr.RetryError):
            tr.retry(
                fn, tries=4, base=0.01, max_delay=10.0, multiplier=2.0,
                jitter="none", on_retry=on_retry,
            )

        self.assertEqual(len(sleeps), 3)
        # 0.01, 0.02, 0.04
        self.assertAlmostEqual(sleeps[0], 0.01, places=3)
        self.assertAlmostEqual(sleeps[1], 0.02, places=3)
        self.assertAlmostEqual(sleeps[2], 0.04, places=3)


# ---------------------------------------------------------------------------
# abretry() — async
# ---------------------------------------------------------------------------


class TestAbRetry(unittest.TestCase):
    def test_async_success(self):
        async def fn():
            return "ok"

        async def runner():
            return await tr.abretry(fn, tries=1)

        self.assertEqual(asyncio.run(runner()), "ok")

    def test_async_eventual_success(self):
        attempts = []

        async def fn():
            attempts.append(1)
            if len(attempts) < 2:
                raise ValueError("nope")
            return "ok"

        async def runner():
            return await tr.abretry(fn, tries=3, base=0.0, max_delay=0.0)

        self.assertEqual(asyncio.run(runner()), "ok")
        self.assertEqual(len(attempts), 2)

    def test_async_exhausts(self):
        async def fn():
            raise ValueError("x")

        async def runner():
            with self.assertRaises(tr.RetryError):
                await tr.abretry(fn, tries=2, base=0.0)

        asyncio.run(runner())

    def test_async_invalid_args(self):
        async def fn():
            return 1

        async def runner():
            with self.assertRaises(ValueError):
                await tr.abretry(fn, tries=0)

        asyncio.run(runner())


# ---------------------------------------------------------------------------
# CircuitBreaker
# ---------------------------------------------------------------------------


class TestCircuitBreaker(unittest.TestCase):
    def test_initial_state_closed(self):
        cb = tr.CircuitBreaker(name="t")
        self.assertEqual(cb.state, "closed")

    def test_opens_after_threshold_failures(self):
        cb = tr.CircuitBreaker(failure_threshold=3, recovery_time=1.0, name="t")

        def boom():
            raise RuntimeError("x")

        for _ in range(3):
            with self.assertRaises(RuntimeError):
                cb.call(boom)

        self.assertEqual(cb.state, "open")

    def test_open_circuit_raises_immediately(self):
        cb = tr.CircuitBreaker(failure_threshold=1, recovery_time=10.0, name="t")

        def boom():
            raise RuntimeError("x")

        with self.assertRaises(RuntimeError):
            cb.call(boom)
        self.assertEqual(cb.state, "open")

        with self.assertRaises(tr.CircuitOpenError) as cm:
            cb.call(lambda: 1)
        self.assertGreater(cm.exception.retry_after, 0)

    def test_transitions_to_half_open(self):
        cb = tr.CircuitBreaker(failure_threshold=1, recovery_time=0.05, name="t")

        def boom():
            raise RuntimeError("x")

        with self.assertRaises(RuntimeError):
            cb.call(boom)
        time.sleep(0.07)
        self.assertEqual(cb.state, "half_open")

    def test_half_open_success_closes(self):
        cb = tr.CircuitBreaker(
            failure_threshold=1, recovery_time=0.05, success_threshold=2, name="t"
        )

        def boom():
            raise RuntimeError("x")

        with self.assertRaises(RuntimeError):
            cb.call(boom)
        time.sleep(0.07)
        self.assertEqual(cb.state, "half_open")

        cb.call(lambda: 1)
        self.assertEqual(cb.state, "half_open")  # only 1 of 2 successes
        cb.call(lambda: 1)
        self.assertEqual(cb.state, "closed")

    def test_half_open_failure_reopens(self):
        cb = tr.CircuitBreaker(failure_threshold=1, recovery_time=0.05, name="t")

        def boom():
            raise RuntimeError("x")

        with self.assertRaises(RuntimeError):
            cb.call(boom)
        time.sleep(0.07)
        self.assertEqual(cb.state, "half_open")

        with self.assertRaises(RuntimeError):
            cb.call(boom)
        self.assertEqual(cb.state, "open")

    def test_unexpected_exception_does_not_count(self):
        cb = tr.CircuitBreaker(
            failure_threshold=1, expected_exception=(ValueError,), name="t"
        )

        def boom():
            raise KeyError("not counted")

        # KeyError is not in expected_exception → should not affect state
        for _ in range(5):
            with self.assertRaises(KeyError):
                cb.call(boom)

        self.assertEqual(cb.state, "closed")

    def test_reset(self):
        cb = tr.CircuitBreaker(failure_threshold=1, recovery_time=10.0, name="t")

        def boom():
            raise RuntimeError("x")

        with self.assertRaises(RuntimeError):
            cb.call(boom)
        self.assertEqual(cb.state, "open")
        cb.reset()
        self.assertEqual(cb.state, "closed")

    def test_acall_sync(self):
        cb = tr.CircuitBreaker(name="t")

        async def fn():
            return "ok"

        async def runner():
            return await cb.acall(fn)

        self.assertEqual(asyncio.run(runner()), "ok")

    def test_acall_opens_on_failure(self):
        cb = tr.CircuitBreaker(failure_threshold=1, recovery_time=10.0, name="t")

        async def boom():
            raise RuntimeError("x")

        async def runner():
            with self.assertRaises(RuntimeError):
                await cb.acall(boom)
            self.assertEqual(cb.state, "open")
            with self.assertRaises(tr.CircuitOpenError):
                await cb.acall(boom)

        asyncio.run(runner())

    def test_invalid_construction(self):
        with self.assertRaises(ValueError):
            tr.CircuitBreaker(failure_threshold=0)
        with self.assertRaises(ValueError):
            tr.CircuitBreaker(recovery_time=0)
        with self.assertRaises(ValueError):
            tr.CircuitBreaker(success_threshold=0)

    def test_repr(self):
        cb = tr.CircuitBreaker(name="alpha")
        self.assertIn("alpha", repr(cb))


# ---------------------------------------------------------------------------
# Decorators
# ---------------------------------------------------------------------------


class TestDecorators(unittest.TestCase):
    def test_retry_decorator_sync(self):
        attempts = []

        @tr.retry_decorator(tries=3, base=0.0, max_delay=0.0)
        def f():
            attempts.append(1)
            if len(attempts) < 2:
                raise ValueError("x")
            return "ok"

        self.assertEqual(f(), "ok")

    def test_abretry_decorator_async(self):
        @tr.abretry_decorator(tries=2, base=0.0, max_delay=0.0)
        async def f():
            return "ok"

        async def runner():
            return await f()

        self.assertEqual(asyncio.run(runner()), "ok")

    def test_abretry_rejects_sync(self):
        with self.assertRaises(TypeError):
            tr.abretry_decorator()(lambda: 1)

    def test_circuit_decorator_sync(self):
        cb = tr.CircuitBreaker(failure_threshold=2, recovery_time=10.0)

        @tr.circuit_decorator(cb)
        def f():
            raise RuntimeError("x")

        with self.assertRaises(RuntimeError):
            f()
        with self.assertRaises(RuntimeError):
            f()
        with self.assertRaises(tr.CircuitOpenError):
            f()

    def test_circuit_decorator_attaches_breaker(self):
        cb = tr.CircuitBreaker(name="t")

        @tr.circuit_decorator(cb)
        def f():
            return 1

        self.assertIs(f.breaker, cb)

    def test_metadata_preserved(self):
        @tr.retry_decorator()
        def my_function():
            """Docstring here."""
            return 1

        self.assertEqual(my_function.__name__, "my_function")
        self.assertIn("Docstring", my_function.__doc__)


# ---------------------------------------------------------------------------
# Thread-safety smoke test
# ---------------------------------------------------------------------------


class TestConcurrency(unittest.TestCase):
    def test_circuit_breaker_thread_safe(self):
        cb = tr.CircuitBreaker(failure_threshold=100, recovery_time=10.0)

        def fn():
            return 1

        threads = [threading.Thread(target=lambda: cb.call(fn)) for _ in range(50)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(cb.state, "closed")


if __name__ == "__main__":
    unittest.main(verbosity=2)
