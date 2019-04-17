import pytest
from datetime import datetime
from yapic.sql import wrap_connection, Entity
from yapic.entity import Field, Serial, Int, String, Date, DateTime, DateTimeTz, Bool, ForeignKey, One, Query, func

pytestmark = pytest.mark.asyncio


@pytest.yield_fixture
async def conn(pgsql):
    yield wrap_connection(pgsql, "pgsql")


class Address(Entity):
    id: Serial
    title: String


class User(Entity):
    id: Serial
    name: String = Field(size=100)
    bio: String
    fixed_char: String = Field(size=[5, 5])

    address_id: Int = ForeignKey(Address.id)
    address: One[Address]

    is_active: Bool = True
    birth_date: Date
    naive_date: DateTime = datetime(2019, 1, 1, 12, 34, 55)
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


async def test_reflect(conn):
    ent_reg = await conn.reflect()

    def test_field(ent, field, impl, *, size=None, nullable=None, default=None):
        reflected = ent_reg[ent]
        attr = getattr(reflected, field)

        assert attr._name_ == field
        assert str(attr._impl_) == impl

        if size is not None:
            if isinstance(size, list):
                assert [attr.min_size, attr.max_size] == size
            else:
                assert attr.max_size == size

        if nullable is not None:
            assert attr.nullable is nullable

        if default is not None:
            assert attr._default_ == default

    test_field("User", "id", "Int", size=4, nullable=False)
    test_field("User", "name", "String", size=100, nullable=True)
    test_field("User", "bio", "String", size=-1, nullable=True)
    test_field("User", "fixed_char", "String", size=[5, 5], nullable=True)
    test_field("User", "address_id", "Int", size=4, nullable=True)
    test_field("User", "is_active", "Bool", nullable=False, default=True)
    test_field("User", "birth_date", "Date", nullable=True)
    test_field("User", "naive_date", "DateTime", nullable=False, default=datetime(2019, 1, 1, 12, 34, 55))
    test_field("User", "created_time", "DateTimeTz", nullable=False)
    test_field("User", "updated_time", "DateTimeTz", nullable=True)
