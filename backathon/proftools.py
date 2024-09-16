import collections
import contextlib
import time

perf_data: list[tuple[str, float]] = []

_enabled = False


@contextlib.contextmanager
def perf_block(name: str):
    if not _enabled:
        yield
        return
    t1 = time.perf_counter()
    try:
        yield
    finally:
        t2 = time.perf_counter()
        perf_data.append((name, t2 - t1))


def enable_perf():
    global _enabled
    _enabled = True


def print_perf_data():
    agg_times = collections.defaultdict(float)
    agg_count = collections.defaultdict(int)
    for name, t in perf_data:
        agg_times[name] += t
        agg_count[name] += 1

    if agg_times:
        max_name = max(len(n) for n in agg_times)
    else:
        max_name = 5
    print(
        "{:{}} {:5} {:10} {:10}".format("Category", max_name, "Count", "Time (s)", "C/s")
    )
    if not agg_times:
        print("No performance data captured")
    for name, t in sorted(agg_times.items()):
        count = agg_count[name]
        parts = name.split(".")
        name = "  " * (len(parts) - 1) + parts[-1]
        print(
            "{:{}} {:>5} {:>7.2f} {:>10.1f}/s".format(
                name,
                max_name,
                count,
                t,
                count / t,
            )
        )
