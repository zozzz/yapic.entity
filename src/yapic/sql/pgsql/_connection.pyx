from yapic.entity._entity cimport EntityType

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
            await self.conn.execute(f"DROP TABLE IF EXISTS {qname}")

        ddl = self.dialect.create_ddl_compiler()
        await self.conn.execute(ddl.compile_entity(ent))

