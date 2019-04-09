from yapic.entity._entity cimport EntityType, EntityBase, EntityAttribute
from yapic.entity._field cimport Field

from .._connection cimport Connection


cdef class PostgreConnection(Connection):
    async def create_entity(self, EntityType ent, *, drop=False):
        try:
            schema = ent.__meta__["schema"]
        except KeyError:
            pass
        else:
            await self.conn.execute(f"CREATE SCHEMA IF NOT EXISTS {self.dialect.quote_ident(schema)}")

        qname = self.dialect.table_qname(ent)

        if drop:
            await self.conn.execute(f"DROP TABLE IF EXISTS {qname} CASCADE")

        ddl = self.dialect.create_ddl_compiler()
        return await self.conn.execute(ddl.compile_entity(ent))

    async def insert(self, EntityBase entity):
        cdef EntityType ent = type(entity)
        cdef EntityAttribute attr

        fields = []
        values = []
        subst = []
        i = 1
        for attr, value in entity.__state__.data_for_insert():
            if isinstance(attr, Field):
                fields.append(self.dialect.quote_ident(attr._name_))
                values.append(value)
                subst.append(f"${i}")
                i += 1

        q = [
            "INSERT INTO ", self.dialect.table_qname(ent),
            " (", ", ".join(fields), ") VALUES (", ", ".join(subst), ")"]

        return await self.conn.execute("".join(q), *values)

    async def update(self, EntityBase entity):
        raise NotImplementedError()

    async def delete(self, EntityBase entity):
        raise NotImplementedError()
