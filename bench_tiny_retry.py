"""Benchmarks for tiny-retry. Run with `python bench_tiny_retry.py`."""

import asyncio
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import tiny_retry as tr


def bench(name, fn, n=10_000):
    fn()
    t0 = time.perf_counter()
    for _ in range(n):
        fn()
    dt = (time.perf_counter() - t0) / n * 1e6
    print(f"  {name:40s} {dt:10.3f} µs/op")


def main():
    print("== tiny-retry benchmarks (n=10,000) ==")

    # retry() with no failures — just overhead
    bench("retry() no-failure, 1 try", lambda: tr.retry(lambda: 1, tries=1, base=0.0))
    bench("retry() no-failure, 5 tries", lambda: tr.retry(lambda: 1, tries=5, base=0.0))

    # CircuitBreaker.call() — closed, no failures
    cb = tr.CircuitBreaker(name="bench")
    bench("CircuitBreaker.call() closed", lambda: cb.call(lambda: 1))


if __name__ == "__main__":
    main()
