import pytest
from yapic.entity import Entity, String, Int
from yapic.entity._entity import EntityState
from yapic.entity._field import FieldExtension, Field


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
    assert Ent.__fields__[0].name == "id"
    assert Ent.__fields__[0].entity == Ent
    assert Ent.__fields__[0].extensions[0] == pkInst
    assert pkInst.field == Ent.__fields__[0]
    assert Ent.__fields__[1].name == "test"
    assert Ent.__fields__[1].entity == Ent
    assert Ent.__fields__[2].name == "test_def"
    assert Ent.__fields__[2].entity == Ent
    assert Ent.__fields__[3].name == "test_none"
    assert Ent.__fields__[3].entity == Ent

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

    class Ent(Entity):
        f1: String = FE1()
        f2: String = Field() // FE2()
        f3: String = FE3() // FE4()

    f1 = Ent.__fields__[0]
    assert isinstance(f1, Field)
    assert len(f1.extensions) == 1
    assert isinstance(f1.extensions[0], FE1)
    assert f1.extensions[0].field == f1

    f2 = Ent.__fields__[1]
    assert isinstance(f2, Field)
    assert len(f2.extensions) == 1
    assert isinstance(f2.extensions[0], FE2)
    assert f2.extensions[0].field == f2

    f3 = Ent.__fields__[2]
    assert isinstance(f3, Field)
    assert len(f3.extensions) == 2
    assert isinstance(f3.extensions[0], FE3)
    assert isinstance(f3.extensions[1], FE4)
    assert f3.extensions[0].field == f3
    assert f3.extensions[1].field == f3


def test_entity_state_common():
    class User(Entity):
        id: Int
        name: String

    user = User()

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
    class User(Entity):
        id: Int
        name: String

    user = User()
    assert user.id is None
    assert user.name is None

    user.id = 12
    assert user.id == 12
    # TODO: original value, lekérdezési lehetőség

    user.name = "Jhon Doe"
    assert user.name == "Jhon Doe"
    assert dict(user.__state__) == {"id": 12, "name": "Jhon Doe"}

    changes = list(user.__state__.changes)
    assert len(changes) == 2
    assert changes[0][0] is User.id
    assert changes[0][1] is None
    assert changes[0][2] == 12

    assert changes[1][0] is User.name
    assert changes[1][1] is None
    assert changes[1][2] == "Jhon Doe"

    user.__state__.reset()
    assert dict(user.__state__) == {"id": 12, "name": "Jhon Doe"}

    changes = list(user.__state__.changes)
    assert len(changes) == 0

    user.name = "New Name"
    changes = list(user.__state__.changes)
    assert len(changes) == 1
    assert changes[0][0] is User.name
    assert changes[0][1] == "Jhon Doe"
    assert changes[0][2] == "New Name"
