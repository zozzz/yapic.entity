from yapic.entity._query cimport Query

from ._dialect cimport Dialect


cdef class Connection:
    cdef readonly object conn
    cdef readonly Dialect dialect

    cpdef select(self, Query q, prefetch, timeout)
