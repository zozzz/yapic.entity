import pytest
from yapic.sql import wrap_connection, Entity, sync
from yapic.entity import (Serial, Int, String, ForeignKey, One, Many, ManyAcross, Registry, entity_deps)

pytestmark = pytest.mark.asyncio  # type: ignore


@pytest.yield_fixture  # type: ignore
async def conn(pgsql):
    yield wrap_connection(pgsql, "pgsql")


_registry = Registry()


class BaseEntity(Entity, registry=_registry, _root=True):
    pass


class Address(BaseEntity, schema="deps"):
    id: Serial
    address: String


class Tag(BaseEntity, schema="deps"):
    id: Serial
    tag: String


class Backward(BaseEntity, schema="deps"):
    id: Serial
    user_id: Int = ForeignKey("User.id")


class User(BaseEntity, schema="deps"):
    id: Serial
    name: String

    address_id: Int = ForeignKey(Address.id)
    address: One[Address]

    forward: Many["Forward"]
    backward: Many[Backward]

    tags: ManyAcross["UserTags", Tag]


class Forward(BaseEntity, schema="deps"):
    id: Serial
    user_id: Int = ForeignKey(User.id)


class UserTags(BaseEntity, name="user-tags", schema="deps"):
    user_id: Serial = ForeignKey(User.id)
    tag_id: Serial = ForeignKey(Tag.id)


async def test_creation(conn):
    await conn.conn.execute("""DROP SCHEMA IF EXISTS "deps" CASCADE""")

    assert Address.__deps__ == set()
    assert Tag.__deps__ == set()
    assert User.__deps__ == {Address}
    assert Backward.__deps__ == {User}
    assert Forward.__deps__ == {User}
    assert UserTags.__deps__ == {User, Tag}

    assert entity_deps(Address) == []
    assert entity_deps(Tag) == []
    assert entity_deps(User) == [Address]
    assert entity_deps(Forward) == [Address, User]
    assert entity_deps(UserTags) == [Address, User, Tag] or entity_deps(UserTags) == [Tag, Address, User]

    result = await sync(conn, _registry)
    await conn.conn.execute(result)


async def test_cleanup(conn):
    await conn.conn.execute("""DROP SCHEMA IF EXISTS "deps" CASCADE""")
