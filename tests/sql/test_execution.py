import pytest
from yapic.sql import wrap_connection
from yapic.entity import Entity, Serial, Int, String, Date, DateTime, DateTimeTz, Bool, ForeignKey, One, Query, func

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

    is_active: Bool = True
    birth_date: Date
    created_time: DateTimeTz = func.now()
    updated_time: DateTimeTz


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


async def test_basic_insert_update(conn):
    u = User(name="Jhon Doe")

    assert await conn.insert(u) is True
    assert u.id == 1
    assert u.name == "Jhon Doe"
    assert u.__state__.changes() == {}

    u.name = "New Name"
    assert await conn.update(u) is True
    assert u.id == 1
    assert u.name == "New Name"
    assert u.__state__.changes() == {}

    assert await conn.delete(u) is True


async def test_select(conn):
    u = User(id=1, name="Jhon Doe")

    assert await conn.insert(u) is True

    q = Query().select_from(User).where(User.id == 1)

    u = await conn.select(q).first()
    assert u.id == 1
    assert u.name == "Jhon Doe"

    u.name = "New Name"
    await conn.update(u)
    assert u.name == "New Name"

    u2 = await conn.select(q).first()
    assert u2.id == 1
    assert u2.name == "New Name"
