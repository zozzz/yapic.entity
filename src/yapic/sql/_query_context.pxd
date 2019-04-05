from ._connection cimport Connection


cdef class QueryContext:
    cdef object cursor_factory
    cdef readonly Connection conn
