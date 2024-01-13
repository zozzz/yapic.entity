import asyncio
import functools
import gc
import sys
import tracemalloc


def gc_objects():
    for obj in gc.get_objects():
        yield obj, sys.getrefcount(obj)

def memleak(fn):

    @functools.wraps(fn)
    async def wrapped(*args, **kwargs):
        # warm up
        await fn(*args, **kwargs)

        only_asyncpg = tracemalloc.Filter(inclusive=True, filename_pattern="*asyncpg*")
        only_yapic = tracemalloc.Filter(inclusive=True, filename_pattern="*yapic.entity*")
        skip_tests = tracemalloc.Filter(inclusive=False, filename_pattern="*yapic.entity/tests*")
        filters = [only_asyncpg, only_yapic]

        gc.collect()
        # seen = {id(obj): obj for obj in objgraph.get_leaking_objects()}
        seen = {id(obj): (obj, refcnt) for obj, refcnt in gc_objects()}

        tracemalloc.start(5)
        snapshot1 = tracemalloc.take_snapshot().filter_traces(filters)

        totref = sys.gettotalrefcount()

        await fn(*args, **kwargs)
        await fn(*args, **kwargs)
        await fn(*args, **kwargs)

        await asyncio.sleep(2)

        gc.collect()

        print("Leaked refcount", sys.gettotalrefcount() - totref)

        snapshot2 = tracemalloc.take_snapshot().filter_traces(filters)
        result = snapshot2.compare_to(snapshot1, "traceback")
        for diff in result:
            if diff.size_diff > 0:
                print(f"New: {diff.size_diff} B ({diff.count_diff} blk) "
                      f"Total: {diff.size} B ({diff.count} blk)")
                for line in diff.traceback.format():
                    print(f"    {line}")

        # leaked = [obj for obj in objgraph.get_leaking_objects() if id(obj) not in seen]
        leaked = [(obj, refcnt) for obj, refcnt in gc_objects() if seen.get(id(obj), (obj, refcnt))[1] > refcnt]
        for value in leaked:
            print(value)

    return wrapped
