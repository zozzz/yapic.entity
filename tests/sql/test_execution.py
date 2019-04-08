import pytest
from yapic.sql import wrap_connection
from yapic.entity import Entity, Serial, Int, String, ForeignKey, One

pytestmark = pytest.mark.asyncio


@pytest.yield_fixture
async def conn(pgsql):
    yield wrap_connection(pgsql, "pgsql")


class Address(Entity):
    id: Serial
    title: String


class User(Entity):
    id: Serial
    name: String
    address_id: Int = ForeignKey(Address.id)
    address: One[Address]


class User2(Entity, schema="private", name="User"):
    id: Serial
    name: String
    email: String
    address_id: Int = ForeignKey(Address.id)
    address: One[Address]


async def test_ddl(conn):
    await conn.create_entity(Address, drop=True)
    await conn.create_entity(User, drop=True)
    await conn.create_entity(User2, drop=True)


async def test_insert(conn):
    u = User(name="Jhon Doe")
    await conn.insert(u)
