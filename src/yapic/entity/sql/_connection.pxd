
from .._entity cimport EntityBase
from ._query cimport Query
from ._dialect cimport Dialect


cdef class Connection:
    cdef readonly object conn
    cdef readonly Dialect dialect


cpdef wrap_connection(conn, dialect)
