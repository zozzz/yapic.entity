

from yapic.entity._query cimport Query

from ._query_context cimport QueryContext


cdef class Connection:
    def __cinit__(self, conn, dialect):
        self.conn = conn
        self.dialect = dialect

    cpdef select(self, Query q, prefetch, timeout):
        qc = self.dialect.create_query_compiler()
        sql, params = qc.compile_select(q)

        return QueryContext(
            self.conn.cursor(sql, *params, prefetch=prefetch, timeout=timeout)
        )
