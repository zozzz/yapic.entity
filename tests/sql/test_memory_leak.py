import pytest

from yapic.entity.sql import wrap_connection, sync
from yapic.entity import Entity, Registry, Int, String, DateTime, PrimaryKey

REGISTRY = Registry()


class User(Entity, schema="memleak", registry=REGISTRY):
    id: Int = PrimaryKey()
    name: String


@pytest.yield_fixture
async def conn(pgsql):
    yield wrap_connection(pgsql, "pgsql")


async def test_init(conn, pgclean):
    result = await sync(conn, REGISTRY)
    await conn.conn.execute(result)


async def test_insert(conn):
    user = User(id=1, name="Test")
    await conn.save(user)
