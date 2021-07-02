import pytest
from yapic.entity.sql import Entity, sync
from yapic.entity import (Serial, Int, String, ForeignKey, PrimaryKey, One, Many, ManyAcross, Registry, DependencyList,
                          Json, Composite, save_operations, Auto, AutoIncrement, Query)

pytestmark = pytest.mark.asyncio  # type: ignore

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
    user_id: Auto = ForeignKey("User.id")


class User(BaseEntity, schema="deps"):
    id: Serial
    name: String

    address_id: Auto = ForeignKey(Address.id)
    address: One[Address]
    caddress: One[Address] = "Address.id == User.address_id"

    forward: Many["Forward"]
    backward: Many[Backward]

    tags: ManyAcross["UserTags", Tag]
    ctags: ManyAcross["UserTags", Tag] = {
        "UserTags": "UserTags.user_id == User.id",
        Tag: "Tag.id == UserTags.tag_id",
    }


class Forward(BaseEntity, schema="deps"):
    id: Serial
    user_id: Auto = ForeignKey(User.id)


class UserTags(BaseEntity, name="user-tags", schema="deps"):
    user_id: Auto = ForeignKey(User.id) // PrimaryKey()
    tag_id: Auto = ForeignKey(Tag.id) // PrimaryKey()


async def test_creation(conn):
    await conn.execute("""DROP SCHEMA IF EXISTS "deps" CASCADE""")

    assert Address.__deps__ == {Address.id.get_ext(AutoIncrement).sequence}
    assert Tag.__deps__ == {Tag.id.get_ext(AutoIncrement).sequence}
    assert User.__deps__ == {Address, User.id.get_ext(AutoIncrement).sequence}
    assert Backward.__deps__ == {User, Backward.id.get_ext(AutoIncrement).sequence}
    assert Forward.__deps__ == {User, Forward.id.get_ext(AutoIncrement).sequence}
    assert UserTags.__deps__ == {User, Tag}

    def entity_deps(ent):
        res = DependencyList()
        res.add(ent)
        return res

    assert entity_deps(Address) == [Address.id.get_ext(AutoIncrement).sequence, Address]
    assert entity_deps(Tag) == [Tag.id.get_ext(AutoIncrement).sequence, Tag]
    assert entity_deps(User) == [
        User.id.get_ext(AutoIncrement).sequence,
        Address.id.get_ext(AutoIncrement).sequence, Address, User
    ]
    assert entity_deps(Forward) == [
        User.id.get_ext(AutoIncrement).sequence,
        Address.id.get_ext(AutoIncrement).sequence, Address, User,
        Forward.id.get_ext(AutoIncrement).sequence, Forward
    ]
    assert entity_deps(UserTags) == [
        User.id.get_ext(AutoIncrement).sequence,
        Address.id.get_ext(AutoIncrement).sequence, Address, User,
        Tag.id.get_ext(AutoIncrement).sequence, Tag, UserTags
    ]

    dl = DependencyList()
    dl.add(UserTags)
    dl.add(Tag)
    dl.add(Forward)
    dl.add(User)
    dl.add(Backward)
    dl.add(Address)
    assert dl == [
        User.id.get_ext(AutoIncrement).sequence,
        Address.id.get_ext(AutoIncrement).sequence, Address, User,
        Tag.id.get_ext(AutoIncrement).sequence, Tag, UserTags,
        Forward.id.get_ext(AutoIncrement).sequence, Forward,
        Backward.id.get_ext(AutoIncrement).sequence, Backward
    ]

    result = await sync(conn, _registry)
    await conn.execute(result)


async def test_insert(conn):
    xyz_tag = Tag(tag="xyz")
    await conn.insert(xyz_tag)

    user = User(name="JDoe")
    user.address = Address(address="XYZ st. 345")
    user.tags.append(Tag(tag="some tag"))
    user.tags.append(xyz_tag)

    # print("\n".join(map(repr, save_operations(user))))

    await conn.save(user)

    assert user.id == 1
    assert user.name == "JDoe"
    assert user.address_id == 1
    assert user.address.id == 1
    assert user.address.address == "XYZ st. 345"
    assert len(user.tags) == 2
    assert user.tags[0].id == 2
    assert user.tags[0].tag == "some tag"
    assert user.tags[1].id == 1
    assert user.tags[1].tag == "xyz"

    # automatically set foreign key on user
    addr = await conn.select(Query().select_from(Address).where(Address.id == 1)).first()
    user2 = User(name="AddrTest")
    user2.address = addr
    await conn.save(user2)
    assert user2.id == 2
    assert user2.address_id == 1

    # await conn.insert(user)


async def test_json():
    class JName(BaseEntity):
        family: String
        given: String

    class JUser(BaseEntity):
        id: Serial
        name: Json[JName]

    def entity_deps(ent):
        res = DependencyList()
        res.add(ent)
        return res

    assert entity_deps(JUser) == [JUser.id.get_ext(AutoIncrement).sequence, JName, JUser]


async def test_composite():
    class CName(BaseEntity):
        family: String
        given: String

    class CUser(BaseEntity):
        id: Serial
        name: Composite[CName]

    def entity_deps(ent):
        res = DependencyList()
        res.add(ent)
        return res

    assert entity_deps(CUser) == [CUser.id.get_ext(AutoIncrement).sequence, CName, CUser]


async def test_cleanup(conn):
    await conn.execute("""DROP SCHEMA IF EXISTS "deps" CASCADE""")
