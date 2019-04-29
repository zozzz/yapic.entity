from inspect import iscoroutine

from yapic.entity._entity cimport EntityBase, EntityType, EntityState, EntityAttribute
from yapic.entity._field cimport Field

from .._dialect cimport Dialect

cdef class PostgreDialect(Dialect):
    pass
