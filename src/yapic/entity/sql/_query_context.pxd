from ._connection cimport Connection


cdef class QueryContext:
    cdef object cursor_factory
    cdef list columns
    cdef readonly Connection conn
    cdef dict entities
