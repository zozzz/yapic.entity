

from yapic.entity._query cimport Query
from yapic.entity._entity cimport EntityType, EntityBase

from ._query_context cimport QueryContext
from ._query_compiler cimport QueryCompiler
from ._dialect cimport Dialect


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

    async def update(self, EntityBase entity):
        raise NotImplementedError()

    async def delete(self, EntityBase entity):
        raise NotImplementedError()

    async def save(self, EntityBase entity):
        raise NotImplementedError()


cpdef wrap_connection(conn, dialect):
    if isinstance(dialect, str):
        package = __import__(f"yapic.sql.{dialect}", fromlist=["Dialect", "Connection"])
        dialect = getattr(package, "Dialect")
        connection = getattr(package, "Connection")
    else:
        raise TypeError("Invalid dialect argument: %r" % dialect)
    return connection(conn, dialect())
