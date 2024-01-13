import os

# sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "build", "lib.linux-x86_64-3.8-pydebug"))
# sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "build", "lib.linux-x86_64-cpython-311"))
from contextlib import asynccontextmanager

import asyncpg
from memleak import memleak
from memory_profiler import profile as mprofile
from yapic.entity import Registry
from yapic.entity.sql.pgsql import PostgreConnection as Connection

IN_DOCKER = int(os.getenv("IN_DOCKER", "0")) == 1
POSTGRE_HOST = "postgre" if IN_DOCKER else "127.0.0.1"
REGISTRY = Registry()
MEMRAY = int(os.getenv("MEMRAY", "0")) == 1
MY = False
# XXX: python -m memory_profiler tests/memprofile/asyncpg_connection.py


def profile(fn):
    if MEMRAY:
        return fn
    elif MY:
        return memleak(fn)
    else:
        return mprofile(fn, precision=3, backend="tracemalloc")

@asynccontextmanager
async def connect() -> Connection:
    conn = await asyncpg.connect(
        user="postgres",
        password="root",
        database="root",
        host=POSTGRE_HOST,
        connection_class=Connection,
    )

    try:
        yield conn
    finally:
        await conn.close()
