from yapic.entity.sql import PostgreDialect
from yapic.entity import Entity, Registry, Int, String, DateTime, PrimaryKey, Serial
from yapic.entity._entity import EntityState

dialect = PostgreDialect()


def test_entity():
    registry = Registry()

    class User(Entity, registry=registry):
        pass


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
