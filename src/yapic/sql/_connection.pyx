

from yapic.entity._entity cimport EntityType, NOTSET
from yapic.entity._entity_diff cimport EntityDiff
from yapic.entity._entity_operation cimport save_operations
from yapic.entity._entity_operation import EntityOperation
from yapic.entity._query cimport Query
from yapic.entity._entity cimport EntityType, EntityBase, EntityState
from yapic.entity._registry cimport Registry, RegistryDiff

from ._query_context cimport QueryContext
from ._query_compiler cimport QueryCompiler
from ._dialect cimport Dialect
from ._entity import Entity


cdef class Connection:
    def __cinit__(self, conn, dialect):
        self.conn = conn
        self.dialect = dialect

        # self.fetch = self.conn.fetch
        # self.fetchrow = self.conn.fetchrow
        # self.fetchval = self.conn.fetchval
        # self.execute = self.conn.execute
        # self.executemany = self.conn.executemany
        # self.cursor = self.conn.cursor

    def select(self, Query q, *, prefetch=None, timeout=None):
        cdef QueryCompiler qc = self.dialect.create_query_compiler()
        sql, params = qc.compile_select(q)

        return QueryContext(
            self,
            self.conn.cursor(sql, *params, prefetch=prefetch, timeout=timeout),
            qc.select
        )

    async def create_entity(self, EntityType ent, *, drop=False):
        raise NotImplementedError()

    async def insert(self, EntityBase entity):
        raise NotImplementedError()

    async def insert_or_update(self, EntityBase entity):
        raise NotImplementedError()

    async def update(self, EntityBase entity):
        raise NotImplementedError()

    async def delete(self, EntityBase entity):
        raise NotImplementedError()

    async def save(self, EntityBase entity):
        cdef EntityBase target
        cdef EntityBase src

        for op, param in save_operations(entity):
            if op is EntityOperation.REMOVE:
                await self.delete(param)
            elif op is EntityOperation.UPDATE:
                await self.update(param)
            elif op is EntityOperation.INSERT:
                await self.insert(param)
            elif op is EntityOperation.INSERT_OR_UPDATE:
                await self.insert_or_update(param)
            elif op is EntityOperation.UPDATE_ATTR:
                target = param[0]
                src = param[2]

                val = src.__state__.get_value(param[3])
                if val is not NOTSET:
                    target.__state__.set_value(param[1], val)

    async def reflect(self, EntityType base=Entity):
        reg = Registry()
        reflect = self.dialect.create_ddl_reflect(base)
        await reflect.get_entities(self, reg)
        return reg

    def registry_diff(self, Registry a, Registry b):
        return RegistryDiff(a, b, self.__entity_diff)

    def __entity_diff(self, a, b):
        return self.dialect.entity_diff(a, b)

    async def diff(self, Registry new_reg, EntityType entity_base=Entity):
        registry = await self.reflect(entity_base)
        return self.registry_diff(registry, new_reg)


cpdef wrap_connection(conn, dialect):
    if isinstance(dialect, str):
        package = __import__(f"yapic.sql.{dialect}", fromlist=["Dialect", "Connection"])
        dialect = getattr(package, "Dialect")
        connection = getattr(package, "Connection")
    else:
        raise TypeError("Invalid dialect argument: %r" % dialect)
    return connection(conn, dialect())
