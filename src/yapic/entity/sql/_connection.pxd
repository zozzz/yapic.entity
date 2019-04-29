from yapic.entity._query cimport Query
from yapic.entity._entity cimport EntityBase

from ._dialect cimport Dialect


cdef class Connection:
    cdef readonly object conn
    cdef readonly Dialect dialect


cpdef wrap_connection(conn, dialect)
