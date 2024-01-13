from config import REGISTRY, Connection
from yapic.entity import Composite, CreatedTime, Entity, Query, Serial, String


class BaseEntity(Entity, registry=REGISTRY, schema="memprofile", _root=True):
    @classmethod
    async def insert(cls, conn: Connection, **fields):
        inst = cls(**fields)
        await conn.save(inst)
        return inst

    @classmethod
    def query(cls):
        return Query(cls)

    @classmethod
    async def clear(cls, conn: Connection):
        await conn.execute(f""" DELETE FROM {conn.dialect.table_qname(cls)} CASCADE """)


class Name(BaseEntity):
    surname: String
    forename: String


class User(BaseEntity):
    id: Serial
    name: Composite[Name]
    email: String


class Article(BaseEntity):
    id: Serial
    title: String
    created_at: CreatedTime
