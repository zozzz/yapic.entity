import asyncio
import sys
import os
import gc

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "build", "lib.linux-x86_64-3.8-pydebug"))

from memory_profiler import profile as mprofile
import asyncpg

from yapic.entity import Registry, Entity, Serial, String, func

# XXX: python -m memory_profiler tests/memprofile/asyncpg_connection.py


def profile(fn):
    return mprofile(fn, precision=3, backend="tracemalloc")


@profile
async def connect():
    connection = await asyncpg.connect(
        user="root",
        password="root",
        database="root",
        host="127.0.0.1",
    )
    await connection.close()
    test_entity_state()


@profile
async def pool():
    pg_pool = await asyncpg.create_pool(
        host="127.0.0.1",
        user="root",
        password="root",
        database="root",
    )
    conn = await pg_pool.acquire()
    await pg_pool.release(conn)


@profile
def test_entity_state():
    registry = Registry()

    class User(Entity, registry=registry):
        id: Serial
        name: String

    user = User(id=1, name="Almafa")
    user.__state__.changes()
    user.__state__.changes_with_previous()
    user.__state__.changed_realtions()

    if user.__state__.is_dirty:
        pass

    if user.__state__.is_empty:
        pass

    user2 = User(id=2, name="Almafa")

    if user.__state__ == user2.__state__:
        pass
    gc.collect()


if __name__ == "__main__":
    asyncio.run(pool())
