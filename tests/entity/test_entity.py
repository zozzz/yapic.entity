import pytest
from yapic.entity import Entity, String, Int, Serial, One, Many, DontSerialize, ForeignKey
from yapic.entity._entity import EntityState
from yapic.entity._field import FieldExtension, Field
from yapic import json


def test_entity_basics():
    class PrimaryKey(FieldExtension):
        pass

    pkInst = PrimaryKey()

    class Ent(Entity):
        id: Int = pkInst
        test: String = Field()
        test_def: String = "Hello"
        test_none: String

    assert issubclass(Ent, Entity)
    assert Ent.__fields__[0]._name_ == "id"
    assert Ent.__fields__[0]._entity_ == Ent
    assert Ent.__fields__[0]._exts_[0] == pkInst
    assert pkInst.attr == Ent.__fields__[0]
    assert Ent.__fields__[1]._name_ == "test"
    assert Ent.__fields__[1]._entity_ == Ent
    assert Ent.__fields__[2]._name_ == "test_def"
    assert Ent.__fields__[2]._entity_ == Ent
    assert Ent.__fields__[3]._name_ == "test_none"
    assert Ent.__fields__[3]._entity_ == Ent

    # with pytest.raises(AttributeError):
    #     Ent.missing = ""

    ent = Ent()
    with pytest.raises(AttributeError):
        ent.missing = ""


def test_field_ext():
    class FE1(FieldExtension):
        pass

    class FE2(FieldExtension):
        pass

    class FE3(FieldExtension):
        pass

    class FE4(FieldExtension):
        pass

    class Ent2(Entity):
        f1: String = FE1()
        f2: String = Field() // FE2()
        f3: String = FE3() // FE4()

    f1 = Ent2.__fields__[0]
    assert isinstance(f1, Field)
    assert len(f1._exts_) == 1
    assert isinstance(f1._exts_[0], FE1)
    assert f1._exts_[0].attr == f1

    f2 = Ent2.__fields__[1]
    assert isinstance(f2, Field)
    assert len(f2._exts_) == 1
    assert isinstance(f2._exts_[0], FE2)
    assert f2._exts_[0].attr == f2

    f3 = Ent2.__fields__[2]
    assert isinstance(f3, Field)
    assert len(f3._exts_) == 2
    assert isinstance(f3._exts_[0], FE3)
    assert isinstance(f3._exts_[1], FE4)
    assert f3._exts_[0].attr == f3
    assert f3._exts_[1].attr == f3


def test_entity_state_common():
    class User1(Entity):
        id: Int
        name: String

    user = User1()

    assert isinstance(user.__state__, EntityState)

    with pytest.raises(AttributeError):
        user.__state__.x = 10

    with pytest.raises(AttributeError):
        user.__state__.name = "Name"

    with pytest.raises(TypeError) as excinfo:

        class MyState(EntityState):
            pass

    assert "is not an acceptable base type" in str(excinfo.value)


def test_entity_state_usage():
    class User2(Entity):
        id: Int
        name: String

    user = User2()
    assert user.id is None
    assert user.name is None

    user.id = 12
    assert user.id == 12

    user.name = "Jhon Doe"
    assert user.name == "Jhon Doe"

    changes = user.__state__.changes()
    assert changes == {"id": 12, "name": "Jhon Doe"}

    user.__state__.reset()

    assert len(user.__state__.changes()) == 0

    user.name = "New Name"
    changes = user.__state__.changes()
    assert changes == {"name": "New Name"}


def test_entity_iter():
    class User3(Entity):
        id: Int
        name: String

    u = User3(id=1, name="Test Elek")

    assert u.as_dict() == {"id": 1, "name": "Test Elek"}


class User4Addr(Entity):
    id: Serial
    addr: String


class User4(Entity):
    id: Serial
    name: String
    password: String = DontSerialize()

    address_id: Int = ForeignKey(User4Addr.id)
    address: One[User4Addr]

    many: Many["User4Many"]


class User4Many(Entity):
    id: Serial
    many: String
    parent_id: Int = ForeignKey(User4.id)


def test_entity_serialize():

    u = User4(id=2, name="User", password="secret")
    u.address = User4Addr(id=3, addr="ADDRESS 12")
    u.many.append(User4Many(id=4, parent_id=2))
    u.many.append(User4Many(id=5, parent_id=2))
    u.many.append(User4Many(id=6, parent_id=2))
    u.many.append(User4Many(id=7, parent_id=2))

    serialized = json.dumps(u)
    assert serialized == """{"id":2,"name":"User","address":{"id":3,"addr":"ADDRESS 12"},"many":[{"id":4,"parent_id":2},{"id":5,"parent_id":2},{"id":6,"parent_id":2},{"id":7,"parent_id":2}]}"""
